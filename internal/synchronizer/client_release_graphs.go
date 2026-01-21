// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package synchronizer

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/internetofwater/nabu/internal/common"
	"github.com/internetofwater/nabu/internal/mainstems"
	"github.com/internetofwater/nabu/internal/synchronizer/s3"
	"github.com/minio/minio-go/v7"

	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

// convert all objects in s3 with a specific prefix to nq format and stream them to a shared channel
// this allows the caller to mimic concatenating many nq files in parallel without needing to have
// the nq file ever be written to disk
func (synchronizer *SynchronizerClient) streamNqFromPrefix(prefix s3.S3Prefix, nqChan chan<- string, mainstemFile string) error {
	objects, err := synchronizer.S3Client.ObjectList(context.Background(), prefix)
	if err != nil {
		return err
	}
	if len(objects) == 0 {
		return fmt.Errorf("no objects found with prefix %s so no nq file will be created", prefix)
	}
	log.Infof("Generating nq from %d objects with prefix %s", len(objects), prefix)

	addMainstemInfo := mainstemFile != ""
	if addMainstemInfo {
		log.Infof("Adding mainstem info from %s", mainstemFile)
	} else {
		log.Info("Not adding mainstem info to nquads since no mainstem file was provided")
	}

	mainstemService, err := mainstems.NewS3FlatgeobufMainstemService(mainstemFile)
	if err != nil {
		return err
	}
	enricher := mainstems.NewJsonldEnricher(mainstemService)

	// Create errgroup with context
	g, ctx := errgroup.WithContext(context.Background())

	// Limit concurrent workers
	g.SetLimit(10) // Adjust based on your needs

	mainstemsAdded := atomic.Int32{}

	var mainstemMutex sync.Mutex

	for _, object := range objects {
		obj := object // capture loop variable
		g.Go(func() error {
			retrievedObject, err := synchronizer.S3Client.Client.GetObject(
				ctx,
				synchronizer.S3Client.DefaultBucket,
				obj.Key,
				minio.GetObjectOptions{},
			)
			if err != nil {
				return err
			}
			defer func() { _ = retrievedObject.Close() }()

			rawBytes, err := io.ReadAll(retrievedObject)
			if err != nil {
				return err
			}

			var nq string
			if strings.HasSuffix(obj.Key, ".nq") {
				nq = string(rawBytes)
			} else {
				var finalJsonLd []byte
				if mainstemFile != "" {
					mainstemMutex.Lock()
					var foundMainstem bool
					finalJsonLd, foundMainstem, err = enricher.AddMainstemInfo(rawBytes)
					mainstemMutex.Unlock()
					if foundMainstem {
						mainstemsAdded.Add(1)
					}
					if err != nil {
						return err
					}
				} else {
					finalJsonLd = rawBytes
				}
				nq, err = common.JsonldToNQ(string(finalJsonLd), synchronizer.jsonldProcessor, synchronizer.jsonldOptions)
				if err != nil {
					return err
				}
				if len(nq) == 0 {
					return fmt.Errorf("jsonld to nq conversion returned empty string for object %s with data %s", obj.Key, string(finalJsonLd))
				}
			}

			var singleFileNquad string
			if len(objects) == 1 {
				singleFileNquad = nq
			} else {
				singleFileNquad, err = common.Skolemization(nq)
				if err != nil {
					log.Errorf("Skolemization error: %s", err)
					return err
				}
			}

			graphURN, err := common.MakeURN(obj.Key)
			if err != nil {
				return err
			}

			csnq, err := common.NtToNq(singleFileNquad, graphURN)
			if err != nil {
				log.Errorf("error converting object '%s' with urn '%s' to nq: %s", obj.Key, graphURN, err)
				return err
			}

			// Send to channel, respecting context cancellation
			select {
			case <-ctx.Done():
				return ctx.Err()
			case nqChan <- csnq:
				return nil
			}
		})
	}

	// Wait for all goroutines and get first error
	err = g.Wait()
	close(nqChan)
	// only log if we actually attempted to add mainstem info
	if addMainstemInfo {
		log.Infof("Found and added mainstems to %d/%d JSON-LD objects for prefix %s", mainstemsAdded.Load(), len(objects), prefix)
	}
	return err
}

// Generate an nq file from all objects in s3 with a specific prefix
// this is accomplished by streaming the conversion of nq and uploading
// to minio concurrently. We used a buffered channel to limit the
// concurrency of the conversion process
func (synchronizer *SynchronizerClient) GenerateNqRelease(prefix s3.S3Prefix, compressGraphWithGzip bool, mainstemFile string) error {

	remote_file := strings.HasPrefix(mainstemFile, "gcs://") && strings.HasPrefix(mainstemFile, "s3://") && strings.HasPrefix(mainstemFile, "http://") && strings.HasPrefix(mainstemFile, "https://")

	if mainstemFile == "" {
		log.Warn("There was no provided mainstem file, so no mainstem info will be added to the nquad release")
		// only check for existence if the mainstem file is not remote and could be local
	} else if !remote_file {
		if _, err := os.Stat(mainstemFile); err != nil {
			if os.IsNotExist(err) {
				return fmt.Errorf("mainstem file was specified to be at %s does not exist locally", mainstemFile)
			}
			return fmt.Errorf("failed to stat mainstem file %s: %w", mainstemFile, err)
		}
	}

	if prefix == "" {
		return fmt.Errorf("prefix is empty; you must specify a prefix to generate a release graph from")
	}

	releaseNqName, err := makeReleaseNqName(prefix)
	if err != nil {
		return err
	}
	if compressGraphWithGzip {
		releaseNqName += ".gz"
	}

	const maximumNqFilesToProcessAtOnce = 30

	nqChan := make(chan string, maximumNqFilesToProcessAtOnce)
	errChan := make(chan error, 1)

	// Start processing NQ data concurrently
	go func() {
		// Don't close nqChan here - streamNqFromPrefix will close it
		errChan <- synchronizer.streamNqFromPrefix(prefix, nqChan, mainstemFile)
	}()

	pipeReader, pipeWriter := io.Pipe()

	var writerProcess errgroup.Group
	writerProcess.SetLimit(1)
	writerProcess.Go(func() error {
		hash, err := writeToPipeAndGetByteSum(compressGraphWithGzip, nqChan, pipeWriter)
		if err != nil {
			pipeWriter.CloseWithError(err)
			return err
		}

		if _, err := synchronizer.S3Client.Client.PutObject(
			context.Background(),
			synchronizer.syncBucketName,
			fmt.Sprintf("graphs/latest/%s.bytesum", releaseNqName),
			strings.NewReader(hash),
			int64(len(hash)),
			minio.PutObjectOptions{},
		); err != nil {
			pipeWriter.CloseWithError(err)
			return err
		}
		return pipeWriter.Close()
	})
	const streamObjectOfUndefinedSize = -1

	releaseNqPath := fmt.Sprintf("graphs/latest/%s", releaseNqName)
	// stream the nq data to s3
	objInfo, err := synchronizer.S3Client.Client.PutObject(
		context.Background(),
		synchronizer.syncBucketName,
		releaseNqPath,
		pipeReader,
		streamObjectOfUndefinedSize,
		minio.PutObjectOptions{},
	)
	if err != nil {
		return err
	}

	// Check for errors from the processing goroutine BEFORE checking if file is empty
	if err := <-errChan; err != nil {
		return err
	}

	if err := writerProcess.Wait(); err != nil {
		return err
	}

	if objInfo.Size == 0 {
		return fmt.Errorf("empty nq file for %s when uploading to s3", releaseNqName)
	}

	dataWasStreamed := objInfo.Size == -1
	var size int64
	if dataWasStreamed {
		info, err := synchronizer.S3Client.Client.StatObject(context.Background(), synchronizer.syncBucketName, releaseNqPath, minio.StatObjectOptions{})
		if err != nil {
			return fmt.Errorf("error getting info on nq object %s after loading it into the bucket: %w", releaseNqPath, err)
		}
		size = info.Size
	} else {
		size = objInfo.Size
	}

	log.Infof("Successfully uploaded N-Quads of size %d bytes to %s", size, releaseNqPath)

	return nil
}
