// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package synchronizer

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"

	"github.com/internetofwater/nabu/internal/common"
	"github.com/internetofwater/nabu/internal/synchronizer/s3"
	"github.com/minio/minio-go/v7"

	log "github.com/sirupsen/logrus"
)

// convert all objects in s3 with a specific prefix to nq and stream them to the channel
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

// Loads a single stored release graph into the graph database
func (synchronizer *SynchronizerClient) UploadNqFileToTriplestore(nqPathInS3 s3.S3Prefix) error {

	byt, err := synchronizer.S3Client.GetObjectAsBytes(nqPathInS3)
	if err != nil {
		return err
	}
	if len(byt) == 0 {
		return errors.New("empty nq file when uploading to triplestore")
	}

	// Convert JSON-LD to N-Quads if needed
	if strings.Contains(nqPathInS3, ".jsonld") {
		convertedNq, err := common.JsonldToNQ(string(byt), synchronizer.jsonldProcessor, synchronizer.jsonldOptions)
		if err != nil {
			return err
		}
		byt = []byte(convertedNq)
	}

	req, err := http.NewRequestWithContext(context.Background(), "POST", synchronizer.GraphClient.GetSparqlQueryUrl(), bytes.NewReader(byt))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/n-quads") // Corrected content type

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()

	// Read response
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode != 200 && resp.StatusCode != 204 {
		log.Errorf("GraphDB upload failed: %d %s", resp.StatusCode, string(body))
		return fmt.Errorf("GraphDB upload failed: %d", resp.StatusCode)
	}

	log.Infof("Successfully uploaded N-Quads (%d bytes)", len(byt))
	return nil
}

// Generate an nq file from all objects in s3 with a specific prefix
// this is accomplished by streaming the conversion of nq and uploading
// to minio concurrently. We used a buffered channel to limit the
// concurrency of the conversion process
func (synchronizer *SynchronizerClient) GenerateNqRelease(prefix s3.S3Prefix) error {

	releaseNqName, err := makeReleaseNqName(prefix)
	if err != nil {
		return err
	}

	const maximumNqFilesToProcessAtOnce = 30

	nqChan := make(chan string, maximumNqFilesToProcessAtOnce) // Buffered channel for limiting concurrency
	errChan := make(chan error, 1)

	// Start processing NQ data concurrently
	go func() {
		defer close(nqChan)
		errChan <- synchronizer.streamNqFromPrefix(prefix, nqChan)
	}()

	piperReader, pipeWriter := io.Pipe()

	// Concurrently upload data to S3 while receiving from the channel
	// if there is an error in the processing goroutine
	// we will close the pipe with an error and exit
	go func() {
		// once the nqChan is closed we can close the pipe
		// since there is nothing more to write
		defer func() {
			err = pipeWriter.Close()
			log.Error(err)
		}()

		for nq := range nqChan {
			_, err := pipeWriter.Write([]byte(nq))
			if err != nil {
				pipeWriter.CloseWithError(err)
				return
			}
		}
	}()

	const streamObjectOfUndefinedSize = -1

	// stream the nq data to s3
	objInfo, err := synchronizer.S3Client.Client.PutObject(
		context.Background(),
		synchronizer.syncBucketName,
		fmt.Sprintf("graphs/latest/%s", releaseNqName),
		piperReader,
		streamObjectOfUndefinedSize,
		minio.PutObjectOptions{},
	)
	if err != nil {
		return err
	}
	if objInfo.Size == 0 {
		return errors.New("empty nq file when uploading to s3")
	}

	// Check for errors from the processing goroutine
	if err := <-errChan; err != nil {
		return err
	}

	log.Infof("Successfully uploaded N-Quads to %s (%d bytes)", objInfo.Key, objInfo.Size)

	return nil
}
