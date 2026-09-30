// Copyright 2026 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package crawl

import (
	"bufio"
	"bytes"
	"context"
	"crypto/md5"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/client"
	"github.com/moby/moby/pkg/stdcopy"
	log "github.com/sirupsen/logrus"

	"github.com/internetofwater/nabu/internal/crawl/storage"
	"github.com/internetofwater/nabu/internal/opentelemetry"
	"github.com/internetofwater/nabu/pkg"
	"golang.org/x/sync/errgroup"
)

// the number of documents validated against a remote SHACL validator at once;
// sized for a large VM running a multi-process SHACL validator
const bulkShaclConcurrency = 128

// a single jsonld document read from the stdout of a bulk container
type bulkJsonldLine struct {
	path string
	line []byte
}

// The number of documents to validate at once in a bulk harvest
func bulkShaclWorkers(config *SitemapHarvestConfig) int {
	if config.exitOnShaclFailure {
		// validate one document at a time so that the harvest stops
		// at the first invalid document, the same as a serial harvest
		return 1
	}
	if _, isLocal := config.shaclValidator.(*LocalShaclValidator); isLocal {
		// local validation is CPU bound so more workers than cores would not be faster
		return runtime.GOMAXPROCS(0)
	}
	return bulkShaclConcurrency
}

// HarvestBulkSitemap processes a bulk sitemap by pulling and running Docker images specified as sitemap URLs.
func (s *Sitemap) HarvestBulkSitemap(ctx context.Context, config *SitemapHarvestConfig) (pkg.SitemapCrawlStats, error) {

	if config.workers != 1 {
		log.Warn("Bulk sitemaps do not allow for specifying workers, using default worker count")
	}

	if config.cleanupOutdatedJsonld {
		log.Warn("cleanup outdated jsonld is not configurable for bulk sitemaps; outdated jsonld is always removed after a successful bulk harvest")
	}

	ctx, span := opentelemetry.SubSpanFromCtxWithName(ctx, fmt.Sprintf("bulk_harvest_%s", s.metadata.SitemapID))
	defer span.End()

	bulkStoragePrefix := "summoned/" + s.metadata.SitemapID + "/"

	// If the storage can list the hash of every object under the prefix cheaply
	// and remove objects in batches, only documents that changed are uploaded
	// and documents no longer present are removed after a successful harvest.
	// Otherwise everything under the prefix is removed up front and re-uploaded
	hashLister, canListHashes := config.storageDestination.(storage.PrefixHashLister)
	batchRemover, canBatchRemove := config.storageDestination.(storage.BatchRemover)
	incremental := canListHashes && canBatchRemove

	var existingHashes map[storage.ObjectPath]storage.Md5Hash
	if incremental {
		var err error
		existingHashes, err = hashLister.ListHashes(ctx, bulkStoragePrefix)
		if err != nil {
			return pkg.SitemapCrawlStats{}, fmt.Errorf("failed to list pre-existing bulk data with prefix %s: %w", bulkStoragePrefix, err)
		}
		log.Infof("Found %d pre-existing jsonld documents with prefix %s; only changed documents will be uploaded", len(existingHashes), bulkStoragePrefix)
	} else if _, err := storage.DeletePrefix(bulkStoragePrefix, config.storageDestination); err != nil {
		return pkg.SitemapCrawlStats{}, fmt.Errorf("failed to delete pre-existing bulk data with prefix %s: %w", bulkStoragePrefix, err)
	}

	shaclWorkers := bulkShaclWorkers(config)

	dockerClient, err := client.NewClientWithOpts(client.WithAPIVersionNegotiation())
	if err != nil {
		return pkg.SitemapCrawlStats{}, err
	}
	defer func() { _ = dockerClient.Close() }()

	start := time.Now()

	var warningStats []pkg.ShaclInfo
	var warningMu = sync.Mutex{}

	validJsonldDocs := make(storage.Set)
	validJsonldDocsMu := sync.Mutex{}

	numNewlineSeparateJSONLDDocs := atomic.Int64{}
	numUnchangedJSONLDDocs := atomic.Int64{}
	exitedOnShaclFailure := atomic.Bool{}

	// validate a single document; only returns an error if the harvest should stop
	validateLine := func(ctx context.Context, urlLoc string, line []byte) error {
		if config.shaclValidator == nil {
			return nil
		}
		err := validate_shacl(ctx, config.shaclValidator, urlLoc, string(line))
		if err == nil {
			return nil
		}
		if shaclErr, ok := err.(ShaclValidationFailureError); ok {

			warningMu.Lock()
			warningStats = append(warningStats, pkg.ShaclInfo{
				ShaclStatus:            pkg.ShaclInvalid,
				ShaclValidationMessage: shaclErr.ShaclErrorMessage,
				Url:                    urlLoc,
			})
			warningMu.Unlock()

			// we don't always return here because it is non fatal
			// and not all integrations may be compliant with our shacl shapes yet;
			// For the time being, it is better to harvest and then have the integrator fix it
			// after the fact; in the future there could be a strict
			// validation mode wherein we fail fast upon shacl non-compliance
			// however, we do allow a flag to exit and strictly fail
			if config.exitOnShaclFailure {
				log.Errorf("Returning early on shacl failure for %s with message %s", urlLoc, shaclErr.ShaclErrorMessage)
				exitedOnShaclFailure.Store(true)
				return fmt.Errorf("exiting early for %s with shacl failure %s", urlLoc, shaclErr.ShaclErrorMessage)
			}
		} else {
			// if there is an other arbitrary issue with the shacl validation service, we mark it as a failure
			// but it is non fatal; we don't want to fail the entire bulk harvest due to an issue with the shacl validation service; thus we log the error and continue on
			msg := fmt.Sprintf("failed to communicate with shacl validation service: %v when harvesting %s", err, urlLoc)
			log.Error(msg)
			errorMessage := ShaclValidationFailureError{ShaclErrorMessage: msg, Url: urlLoc}
			errorInfo := pkg.ShaclInfo{
				ShaclStatus:            pkg.ShaclInvalid,
				ShaclValidationMessage: errorMessage.ShaclErrorMessage,
				Url:                    errorMessage.Url,
			}
			warningMu.Lock()
			warningStats = append(warningStats, errorInfo)
			warningMu.Unlock()
		}
		return nil
	}

	log.Debugf("starting bulk harvest for sitemap %s with %d container urls", s.metadata.SitemapID, len(s.URL))

	// uploads use the harvest context rather than the per-container group context,
	// so documents that were validated before a strict shacl failure are still uploaded
	harvestCtx := ctx

	var errGroupError error = nil
	for _, url := range s.URL {
		// by using an error group we can make it so that if any of the container processing fails, we can immediately stop the entire harvest and return an error
		// it is easier to keep in sync compared to channels
		// The pipeline is: container stdout reader -> shacl validation workers -> bulk upload
		group, ctx := errgroup.WithContext(ctx)

		// documents read from the container that still need to be validated
		validateChan := make(chan bulkJsonldLine, 1000)
		// documents that have been validated and need to be uploaded
		bulkUploadChan := make(chan storage.BulkStorageItem, 1000)

		group.Go(func() error {
			_, subspan := opentelemetry.SubSpanFromCtxWithName(ctx, fmt.Sprintf("bulk_upload_%s", s.metadata.SitemapID))
			err := config.storageDestination.StoreBulk(harvestCtx, bulkUploadChan)
			log.Infof("Finished uploading bulk data for %s", url.Loc)
			subspan.End()
			return err
		})

		var shaclWorkersDone sync.WaitGroup
		for range shaclWorkers {
			shaclWorkersDone.Add(1)
			group.Go(func() error {
				defer shaclWorkersDone.Done()
				for doc := range validateChan {
					if err := validateLine(ctx, url.Loc, doc.line); err != nil {
						return err
					}

					validJsonldDocsMu.Lock()
					validJsonldDocs.Add(doc.path)
					validJsonldDocsMu.Unlock()

					if existingHash, ok := existingHashes[doc.path]; ok {
						sum := md5.Sum(doc.line)
						if existingHash == hex.EncodeToString(sum[:]) {
							numUnchangedJSONLDDocs.Add(1)
							continue
						}
					}

					select {
					case bulkUploadChan <- storage.BulkStorageItem{
						Path:       doc.path,
						Data:       bytes.NewReader(doc.line),
						ByteLength: len(doc.line),
					}:
					case <-ctx.Done():
						return ctx.Err()
					}
				}
				return nil
			})
		}

		group.Go(func() error {
			shaclWorkersDone.Wait()
			close(bulkUploadChan)
			return nil
		})

		group.Go(func() error {

			defer close(validateChan)

			docker_image_name := url.Loc

			if strings.Contains(docker_image_name, "/") {

				log.Infof("Pulling docker image %s", docker_image_name)
				reader, err := dockerClient.ImagePull(ctx, docker_image_name, image.PullOptions{})
				if err != nil {
					return err
				}
				defer func() { _ = reader.Close() }()

				// read the output to completion to ensure the image is pulled
				_, err = io.ReadAll(reader)
				if err != nil {
					return err
				}
			}
			creationResp, err := dockerClient.ContainerCreate(
				ctx,
				&container.Config{Image: docker_image_name},
				&container.HostConfig{
					LogConfig: container.LogConfig{
						// disable Docker disk logging
						// this makes it so the docker daemon
						// does not log to disk and requires
						// something to attach to it to read the logs;
						// this is more efficient for bulk data which would otherwise
						// overwhelm the daemon or add overhead to log to disk
						Type: "none",
					},
				},
				nil,
				nil,
				// no container name is specified in order to
				// ensure the container name is unique
				"",
			)
			if err != nil {
				return err
			}
			log.Infof("Created container %s for image %s", creationResp.ID, docker_image_name)

			// remove the container however this goroutine exits; a background
			// context is used since ctx may already be cancelled
			defer func() {
				if err := dockerClient.ContainerRemove(context.Background(), creationResp.ID, container.RemoveOptions{Force: true}); err != nil {
					log.Errorf("failed to remove container %s: %v", creationResp.ID, err)
				}
			}()

			// attach BEFORE starting; this avoids the race where the container
			// exits and flushes stdout before we connect
			attachResp, err := dockerClient.ContainerAttach(ctx, creationResp.ID, container.AttachOptions{
				Stream: true,
				Stdout: true,
				Stderr: false,
			})
			if err != nil {
				return err
			}
			defer attachResp.Close()

			log.Infof("Starting container %s for image %s", creationResp.ID, docker_image_name)
			if err = dockerClient.ContainerStart(ctx, creationResp.ID, container.StartOptions{}); err != nil {
				return err
			}

			pipeReader, pipeWriter := io.Pipe()
			// closing the reader unblocks the demux goroutine if we stop reading early
			defer func() { _ = pipeReader.Close() }()

			waitResponseChan, errChan := dockerClient.ContainerWait(ctx, creationResp.ID, container.WaitConditionNotRunning)

			// demux the multiplexed stdout stream
			go func() {
				_, err := stdcopy.StdCopy(pipeWriter, io.Discard, attachResp.Reader)
				if err != nil {
					log.Errorf("error demuxing container attach stream: %v", err)
				}
				_ = pipeWriter.Close()
			}()
			reader := bufio.NewReader(pipeReader)
			_, processSubspan := opentelemetry.SubSpanFromCtxWithName(ctx, fmt.Sprintf("process_bulk_jsonld_%s", s.metadata.SitemapID))
			defer processSubspan.End()
			foundEOF := false
			for !foundEOF {
				line, err := reader.ReadBytes('\n')
				if err != nil {
					if err == io.EOF {
						// if we've reached EOF, we need to mark it as such,
						// so we don't iterate further; we don't want to break here
						// though since we still want to process any remaining data in the reader
						// and then exit gracefully
						foundEOF = true
					} else {
						return fmt.Errorf("error reading line from pipe in bulk sitemap harvest: %w", err)
					}
				}
				if len(bytes.TrimSpace(line)) == 0 {
					log.Warn("found a line with no data. Skipping...")
					continue
				}

				totalDocuments := numNewlineSeparateJSONLDDocs.Add(1)

				if totalDocuments%5000 == 0 {
					log.Infof("processed %d jsonld documents for %s; %d were unchanged and skipped", totalDocuments, url.Loc, numUnchangedJSONLDDocs.Load())
					processSubspan.AddEvent(fmt.Sprintf("processed %d jsonld documents", totalDocuments))
				}

				var jsonObj map[string]any
				if err := json.Unmarshal(line, &jsonObj); err != nil {
					return fmt.Errorf("error unmarshaling line as JSON-LD from container logs: %w with data %s", err, string(line))
				}

				idStr, ok := jsonObj["@id"].(string)
				if !ok {
					log.Errorf("missing or invalid @id in JSON-LD for %s", string(line))
					// this is a fatal error since there is no way to data the error to a specific identifier
					// without an id; thus we return a fatal error
					return fmt.Errorf("missing or invalid @id in JSON-LD: %s", string(line))
				}

				encodedId := base64.StdEncoding.EncodeToString([]byte(idStr))

				path := "summoned/" + s.metadata.SitemapID + "/" + encodedId + ".jsonld"

				// ReadBytes returns a new slice on each call, so the
				// line can be handed off without copying
				select {
				case validateChan <- bulkJsonldLine{path: path, line: line}:
				case <-ctx.Done():
					return ctx.Err()
				}
			}

			log.Infof("finished reading logs for container %s", url.Loc)

			select {
			case err := <-errChan:
				if err != nil && err != io.EOF {
					return err
				}
			case exitResp := <-waitResponseChan:
				if exitResp.StatusCode != 0 {
					return fmt.Errorf("container exited with status %d", exitResp.StatusCode)
				}
			}
			return nil
		})

		// if any of the goroutines in the group failed, we want to return that error and stop loop
		// from harvesting any bulk container
		log.Info("Waiting for uploads to finish")
		err := group.Wait()
		span.AddEvent("finished waiting on work group")
		if err != nil {
			log.Errorf("error in bulk harvest for %s: %v", url.Loc, err)
			errGroupError = err
			break
		}
	}

	if exitedOnShaclFailure.Load() {
		// reset count since we are exiting early and thus we have no way of knowing the actual count of sites
		numNewlineSeparateJSONLDDocs.Store(0)
	}

	// only remove outdated documents once every container has been harvested successfully;
	// otherwise we would remove documents that may still be valid
	if incremental && errGroupError == nil {
		outdated := []storage.ObjectPath{}
		for path := range existingHashes {
			if !validJsonldDocs.Contains(path) {
				outdated = append(outdated, path)
			}
		}
		if len(outdated) > 0 {
			log.Infof("Removing %d outdated jsonld documents with prefix %s", len(outdated), bulkStoragePrefix)
			if err := batchRemover.RemoveMany(ctx, outdated); err != nil {
				errGroupError = fmt.Errorf("failed to remove outdated bulk data with prefix %s: %w", bulkStoragePrefix, err)
			}
		}
	}

	log.Infof("Bulk harvest for %s read %d jsonld documents; %d were unchanged and not re-uploaded", s.metadata.SitemapID, numNewlineSeparateJSONLDDocs.Load(), numUnchangedJSONLDDocs.Load())

	firstTwentyWarnings := warningStats
	if len(warningStats) > 20 {
		firstTwentyWarnings = warningStats[:20]
	}
	stats := pkg.SitemapCrawlStats{
		SitemapSourceLink:  s.metadata.Loc,
		SitemapName:        s.metadata.SitemapID,
		SitemapDescription: s.metadata.DatasetDescription,
		WarningStats: pkg.WarningReport{
			TotalShaclFailures: len(warningStats),
			ShaclWarnings:      firstTwentyWarnings,
		},
		SecondsToComplete: time.Since(start).Seconds(),
		SuccessfulSites:   len(validJsonldDocs),
		SitesInSitemap:    int(numNewlineSeparateJSONLDDocs.Load()),
		// since bulk sitemaps are run via docker images,
		// we don't have the ability to propagate per-site crawl errors
		CrawlFailures: []pkg.UrlCrawlError{},
	}

	return stats, errGroupError
}
