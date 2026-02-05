// Copyright 2026 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package crawl

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
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

// HarvestBulkSitemap processes a bulk sitemap by pulling and running Docker images specified as sitemap URLs.
func (s *Sitemap) HarvestBulkSitemap(ctx context.Context, config *SitemapHarvestConfig) (pkg.SitemapCrawlStats, []string, error) {

	ctx, span := opentelemetry.SubSpanFromCtxWithName(ctx, fmt.Sprintf("sitemap_bulk_harvest_%s", s.sitemapId))
	defer span.End()

	group, ctx := errgroup.WithContext(ctx)
	group.SetLimit(config.workers)

	dockerClient, err := client.NewClientWithOpts(client.WithAPIVersionNegotiation())
	if err != nil {
		return pkg.SitemapCrawlStats{}, []string{}, err
	}
	defer func() { _ = dockerClient.Close() }()

	start := time.Now()

	var warningStats []pkg.ShaclInfo
	var warningMu = sync.Mutex{}

	validJsonldDocs := make(storage.Set)
	validJsonldDocsMu := sync.Mutex{}

	numNewlineSeparateJSONLDDocs := atomic.Int32{}

	for _, url := range s.URL {
		group.Go(func() error {

			docker_image_name := url.Loc

			if strings.Contains(docker_image_name, "/") {

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
			resp, err := dockerClient.ContainerCreate(
				ctx,
				&container.Config{Image: docker_image_name},
				nil, nil, nil,
				"", // don't reuse image name as container name
			)
			if err != nil {
				return err
			}

			if err = dockerClient.ContainerStart(ctx, resp.ID, container.StartOptions{}); err != nil {
				return err
			}

			waitResponseChan, errChan := dockerClient.ContainerWait(ctx, resp.ID, container.WaitConditionNotRunning)

			logReader, err := dockerClient.ContainerLogs(ctx, resp.ID, container.LogsOptions{
				ShowStdout: true,
				ShowStderr: false,
				Follow:     true,
			})
			if err != nil {
				return err
			}
			defer func() { _ = logReader.Close() }()

			piperReader, piperWriter := io.Pipe()

			// demux Docker stream → pipe (runs concurrently)
			go func() {
				_, err = stdcopy.StdCopy(piperWriter, io.Discard, logReader)
				if err != nil {
					log.Errorf("error demuxing docker logs: %v", err)
				}
				_ = piperWriter.Close()
			}()

			scanner := bufio.NewScanner(piperReader)

			scanner.Buffer(make([]byte, 1024*64), 1024*1024*10) // 10 MB lines

			for scanner.Scan() {
				line := scanner.Bytes()
				if len(bytes.TrimSpace(line)) == 0 {
					log.Warn("found a line with no data. Skipping...")
					continue
				}

				numNewlineSeparateJSONLDDocs.Add(1)

				var jsonObj map[string]any
				if err := json.Unmarshal(line, &jsonObj); err != nil {
					return fmt.Errorf("error unmarshaling line from container logs: %w", err)
				}

				idStr, ok := jsonObj["@id"].(string)
				if !ok {
					log.Errorf("missing or invalid @id in JSON-LD for %s", string(line))
					// this is a fatal error since there is no way to data the error to a specific identifier
					// without an id; thus we return a fatal error
					return fmt.Errorf("missing or invalid @id in JSON-LD: %s", string(line))
				}

				encodedId := base64.StdEncoding.EncodeToString([]byte(idStr))

				if config.grpcClient != nil && *config.grpcClient != nil {
					err = validate_shacl(ctx, *config.grpcClient, url.Loc, string(line))
					if err != nil {
						if shaclErr, ok := err.(ShaclValidationFailureError); ok {

							warningMu.Lock()
							warningStats = append(warningStats, pkg.ShaclInfo{
								ShaclStatus:            pkg.ShaclInvalid,
								ShaclValidationMessage: shaclErr.ShaclErrorMessage,
								Url:                    url.Loc,
							})
							warningMu.Unlock()

							// we don't always return here because it is non fatal
							// and not all integrations may be compliant with our shacl shapes yet;
							// For the time being, it is better to harvest and then have the integrator fix it
							// after the fact; in the future there could be a strict
							// validation mode wherein we fail fast upon shacl non-compliance
							// however, we do allow a flag to exit and strictly fail
							if config.exitOnShaclFailure {
								log.Errorf("Returning early on shacl failure for %s with message %s", url.Loc, shaclErr.ShaclErrorMessage)
								return fmt.Errorf("exiting early for %s with shacl failure %s", url.Loc, shaclErr.ShaclErrorMessage)
							}
						} else {
							return fmt.Errorf("failed to communicate with shacl validation service when harvesting %s: %w", url.Loc, err)
						}
					}
				}

				path := "summoned/" + s.sitemapId + "/" + encodedId + ".jsonld"

				validJsonldDocsMu.Lock()
				validJsonldDocs.Add(path)
				validJsonldDocsMu.Unlock()

				if err := config.storageDestination.StoreWithHash(
					path,
					bytes.NewReader(line),
					len(line),
				); err != nil {
					return err
				}
			}

			if err := scanner.Err(); err != nil {
				return err
			}

			select {
			case err := <-errChan:
				if err != nil {
					return err
				}
			case resp := <-waitResponseChan:
				if resp.StatusCode != 0 {
					return fmt.Errorf("container exited with status %d", resp.StatusCode)
				}
			}

			return nil
		})
	}

	err = group.Wait()
	stats := pkg.SitemapCrawlStats{
		SitemapSourceLink: s.sitemapUrl,
		WarningStats: pkg.WarningReport{
			TotalShaclFailures: len(warningStats),
			ShaclWarnings:      warningStats,
		},
		SecondsToComplete: time.Since(start).Seconds(),
		SuccessfulSites:   len(validJsonldDocs),
		SitesInSitemap:    int(numNewlineSeparateJSONLDDocs.Load()),
		// since bulk sitemaps are run via docker images,
		// we don't have the ability to propagate per-site crawl errors
		CrawlFailures: []pkg.UrlCrawlError{},
	}

	return stats, []string{}, err
}
