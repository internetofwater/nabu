// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package synchronizer

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"nabu/internal/common"
	"nabu/internal/config"
	"nabu/internal/custom_http_trace"
	"nabu/internal/opentelemetry"
	"nabu/internal/synchronizer/s3"
	"nabu/internal/synchronizer/triplestore"
	"net/http"
	"strings"

	"github.com/minio/minio-go/v7"
	"github.com/piprate/json-gold/ld"
	log "github.com/sirupsen/logrus"
)

// Client to perform operations that synchronize the graph database with the object store
type SynchronizerClient struct {
	// the client used for communicating with the triplestore
	GraphClient *triplestore.GraphDbClient
	// the client used for communicating with s3
	S3Client *s3.MinioClientWrapper
	// default bucket in the s3 that is used for synchronization
	syncBucketName string
	// processor for JSON-LD operations; stored in this struct so we can
	// cache context mappings
	jsonldProcessor *ld.JsonLdProcessor
	// options that are applied with the processor when performing jsonld conversions
	jsonldOptions *ld.JsonLdOptions
	// number of graphs to send per POST request to the triplestore
	upsertBatchSize int
}

// Create a new SynchronizerClient by directly passing in the clients
// Mainly used for testing
func NewSynchronizerClientFromClients(graphClient *triplestore.GraphDbClient, s3Client *s3.MinioClientWrapper, bucketName string) (SynchronizerClient, error) {
	processor, options, err := common.NewJsonldProcessor(false, nil)
	if err != nil {
		return SynchronizerClient{}, err
	}

	client := SynchronizerClient{
		GraphClient:     graphClient,
		S3Client:        s3Client,
		syncBucketName:  bucketName,
		jsonldProcessor: processor,
		jsonldOptions:   options,
		upsertBatchSize: 100,
	}
	return client, nil
}

// Generate a new SynchronizerClient from a top level nabu config
func NewSynchronizerClientFromConfig(conf config.NabuConfig) (*SynchronizerClient, error) {
	graphClient, err := triplestore.NewGraphDbClient(conf.Sparql)
	if err != nil {
		return nil, err
	}
	s3Client, err := s3.NewMinioClientWrapper(conf.Minio)
	if err != nil {
		return nil, err
	}

	processor, options, err := common.NewJsonldProcessor(conf.Context.Cache, conf.ContextMaps)
	if err != nil {
		return nil, err
	}

	client := &SynchronizerClient{
		GraphClient:     graphClient,
		S3Client:        s3Client,
		syncBucketName:  conf.Minio.Bucket,
		jsonldProcessor: processor,
		jsonldOptions:   options,
		upsertBatchSize: conf.Sparql.Batch,
	}
	return client, nil
}

// Get rid of graphs with specific prefix in the triplestore that are not in the object store
// Drops are determined by mapping a prefix to the associated URN
func (synchronizer *SynchronizerClient) SyncTriplestoreGraphs(ctx context.Context, prefix string, checkAndRemoveOrphans bool) error {
	if synchronizer.upsertBatchSize == 0 {
		return fmt.Errorf("got invalid upsert batch size of 0")
	}

	ctx, span := opentelemetry.SubSpanFromCtx(ctx)
	defer span.End()

	var s3GraphsNotInTriplestore []string
	// if we want to check for orphaned graphs
	// we need to get the diff between the graph and s3
	// then drop the orphaned graphs
	if checkAndRemoveOrphans {
		graphDiff, err := synchronizer.getGraphDiff(ctx, prefix)
		if err != nil {
			log.Error(err)
			return err
		}
		for _, urn := range graphDiff.s3UrnToAssociatedObjName {
			s3GraphsNotInTriplestore = append(s3GraphsNotInTriplestore, urn)
		}

		orphanedGraphs := graphDiff.TriplestoreGraphsNotInS3
		// Don't send a drop request if there are no graphs to remove
		if len(orphanedGraphs) > 0 {
			log.Infof("Dropping %d graphs from triplestore", len(orphanedGraphs))
			// All triplestore graphs not in s3 should be removed since s3 is the source of truth
			if err := synchronizer.GraphClient.DropGraphs(orphanedGraphs); err != nil {
				log.Errorf("Drop graph issue when syncing %v\n", err)
				return err
			}
		}
		// if we don't want to remove orphaned graphs
		// just get the list of graphs that are not in s3
	} else {
		objects, err := synchronizer.S3Client.ObjectList(ctx, prefix)
		if err != nil {
			log.Error(err)
			return err
		}
		for _, obj := range objects {
			s3GraphsNotInTriplestore = append(s3GraphsNotInTriplestore, obj.Key)
		}
	}

	if err := synchronizer.batchedUpsert(ctx, s3GraphsNotInTriplestore); err != nil {
		return err
	}

	return nil
}

// Loads a single stored release graph into the graph database
func (synchronizer *SynchronizerClient) UploadNqFileToTriplestore(nqPathInS3 string) error {

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

	// Correct GraphDB REST API endpoint
	url := fmt.Sprintf("%s/statements", synchronizer.GraphClient.BaseRepositoryUrl)

	req, err := custom_http_trace.NewRequestWithContext(context.Background(), "POST", synchronizer.GraphClient.BaseSparqlQueryUrl, bytes.NewReader(byt))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/n-quads") // Corrected content type

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// Read response
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode != 200 && resp.StatusCode != 204 {
		log.Errorf("GraphDB upload failed: %d %s", resp.StatusCode, string(body))
		return fmt.Errorf("GraphDB upload failed: %d", resp.StatusCode)
	}

	log.Infof("Successfully uploaded N-Quads to %s (%d bytes)", url, len(byt))
	return nil
}

// Generate an nq file from all objects in s3 with a specific prefix
// this is accomplished by streaming the conversion of nq and uploading
// to minio concurrently. We used a buffered channel to limit the
// concurrency of the conversion process
func (synchronizer *SynchronizerClient) GenerateNqRelease(prefix string) error {

	releaseNqName, err := makeReleaseNqName(prefix)
	if err != nil {
		return err
	}

	// Create a channel to stream processed N-Quads
	nqChan := make(chan string, 30) // Buffered channel for limiting concurrency
	errChan := make(chan error, 1)

	// Start processing NQ data concurrently
	go func() {
		defer close(nqChan)
		errChan <- synchronizer.streamNqFromPrefix(prefix, nqChan)
	}()

	pr, pw := io.Pipe()

	// Concurrently upload data to S3 while receiving from the channel
	// if there is an error in the processing goroutine
	// we will close the pipe with an error and exit
	go func() {
		// once the nqChan is closed we can close the pipe
		// since there is nothing more to write
		defer pw.Close()

		for nq := range nqChan {
			_, err := pw.Write([]byte(nq))
			if err != nil {
				pw.CloseWithError(err)
				return
			}
		}
	}()

	// stream the nq data to s3
	objInfo, err := synchronizer.S3Client.Client.PutObject(
		context.Background(),
		synchronizer.syncBucketName,
		fmt.Sprintf("graphs/latest/%s", releaseNqName),
		pr,
		-1, // Unknown size; used for streaming
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
