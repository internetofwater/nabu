// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package synchronizer

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/internetofwater/nabu/internal/common"
	"github.com/internetofwater/nabu/internal/synchronizer/s3"
	"github.com/minio/minio-go/v7"

	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

// convert all objects in s3 with a specific prefix to nq format and stream them to a shared channel
// this allows the caller to mimic concatenating many nq files in parallel without needing to have
// the nq file ever be written to disk
func (synchronizer *SynchronizerClient) streamNqFromPrefix(prefix s3.S3Prefix, nqChan chan<- string) error {
	objects, err := synchronizer.S3Client.ObjectList(context.Background(), prefix)
	if err != nil {
		return err
	}
	if len(objects) == 0 {
		return fmt.Errorf("no objects found with prefix %s so no nq file will be created", prefix)
	}

	log.Infof("Generating nq from %d objects with prefix %s", len(objects), prefix)

	var wg sync.WaitGroup
	errChan := make(chan error, 1)

	for _, object := range objects {
		wg.Add(1)

		go func(obj minio.ObjectInfo) {
			defer wg.Done()

			retrievedObject, err := synchronizer.S3Client.Client.GetObject(
				context.Background(),
				synchronizer.S3Client.DefaultBucket,
				obj.Key,
				minio.GetObjectOptions{},
			)
			if err != nil {
				errChan <- err
				return
			}
			defer func() { _ = retrievedObject.Close() }()

			rawBytes, err := io.ReadAll(retrievedObject)
			if err != nil {
				errChan <- err
				return
			}

			var nq string
			if strings.HasSuffix(obj.Key, ".nq") {
				nq = string(rawBytes)
			} else {
				nq, err = common.JsonldToNQ(string(rawBytes), synchronizer.jsonldProcessor, synchronizer.jsonldOptions)
				if err != nil {
					errChan <- err
					return
				}
			}

			var singleFileNquad string
			if len(objects) == 1 {
				singleFileNquad = nq
			} else {
				singleFileNquad, err = common.Skolemization(nq)
				if err != nil {
					log.Errorf("Skolemization error: %s", err)
					errChan <- err
					return
				}
			}

			graphURN, err := common.MakeURN(obj.Key)
			if err != nil {
				errChan <- err
				return
			}

			csnq, err := common.NtToNq(singleFileNquad, graphURN)
			if err != nil {
				log.Errorf("error converting object '%s' with urn '%s' to nq: %s", obj.Key, graphURN, err)
				errChan <- err
				return
			}

			// Send to channel for concurrent streaming
			nqChan <- csnq
		}(object)
	}

	// Wait for all goroutines to complete
	go func() {
		wg.Wait()
		close(errChan)
	}()

	// Return the first error encountered
	if err := <-errChan; err != nil {
		return err
	}

	return nil
}

// Generate an nq file from all objects in s3 with a specific prefix
// this is accomplished by streaming the conversion of nq and uploading
// to minio concurrently. We used a buffered channel to limit the
// concurrency of the conversion process
func (synchronizer *SynchronizerClient) GenerateNqRelease(prefix s3.S3Prefix, compressGraphWithGzip bool) error {

	releaseNqName, err := makeReleaseNqName(prefix)
	if err != nil {
		return err
	}
	if compressGraphWithGzip {
		releaseNqName += ".gz"
	}

	const maximumNqFilesToProcessAtOnce = 30

	nqChan := make(chan string, maximumNqFilesToProcessAtOnce) // Buffered channel for limiting concurrency
	errChan := make(chan error, 1)

	// Start processing NQ data concurrently
	go func() {
		defer close(nqChan)
		errChan <- synchronizer.streamNqFromPrefix(prefix, nqChan)
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

	// stream the nq data to s3
	objInfo, err := synchronizer.S3Client.Client.PutObject(
		context.Background(),
		synchronizer.syncBucketName,
		fmt.Sprintf("graphs/latest/%s", releaseNqName),
		pipeReader,
		streamObjectOfUndefinedSize,
		minio.PutObjectOptions{},
	)
	if err != nil {
		return err
	}
	if objInfo.Size == 0 {
		return fmt.Errorf("empty nq file for %s when uploading to s3", releaseNqName)
	}

	// Check for errors from the processing goroutine
	if err := <-errChan; err != nil {
		return err
	}

	if err := writerProcess.Wait(); err != nil {
		return err
	}

	log.Infof("Successfully uploaded N-Quads to %s (%d bytes)", objInfo.Key, objInfo.Size)

	return nil
}
