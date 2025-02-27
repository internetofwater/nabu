package synchronizer

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"nabu/internal/common"
	"nabu/internal/custom_http_trace"
	"nabu/internal/synchronizer/s3"
	"nabu/internal/synchronizer/triplestore"
	"nabu/pkg/config"
	"net/http"
	"strings"

	"github.com/minio/minio-go/v7"
	"github.com/piprate/json-gold/ld"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
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
	}
	return client, nil
}

// Struct holding the differences between the triplestore and s3
// and a mapping of URN to object name so we can load the graphs
// based on this diff info
type GraphDiff struct {
	TriplestoreGraphsNotInS3 []string
	S3GraphsNotInTriplestore []string
	s3UrnToAssociatedObjName map[string]string
}

// Get rid of graphs with specific prefix in the triplestore that are not in the object store
// Drops are determined by mapping a prefix to the associated URN
func (synchronizer *SynchronizerClient) SyncTriplestoreGraphs(prefix string) error {

	graphDiff, err := synchronizer.getGraphDiff(prefix)
	if err != nil {
		log.Error(err)
		return err
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

	var errorGroup errgroup.Group

	graphsToInsert := make([]common.NamedGraph, len(graphDiff.S3GraphsNotInTriplestore))

	log.Infof("Upserting %d objects from S3 to triplestore", len(graphDiff.S3GraphsNotInTriplestore))
	for i, graphUrnName := range graphDiff.S3GraphsNotInTriplestore {
		graphNameInS3 := graphDiff.s3UrnToAssociatedObjName[graphUrnName]
		i := i // Capture loop variable

		errorGroup.Go(func() error {
			namedGraph, err := synchronizer.S3Client.GetObjectAndConvertToGraph(graphNameInS3, synchronizer.jsonldProcessor, synchronizer.jsonldOptions)
			if err != nil {
				return err
			}
			// by placing the named graph in the slice we can
			// append without needing to use a mutex
			graphsToInsert[i] = namedGraph
			return nil
		})
	}
	if err := errorGroup.Wait(); err != nil {
		return err
	}
	if err := synchronizer.GraphClient.UpsertNamedGraphs(graphsToInsert); err != nil {
		return err
	}

	return nil
}

// Gets all graphs in s3 with a specific prefix and loads them into the triplestore
// regardless of whether they are already in the triplestore
func (synchronizer *SynchronizerClient) CopyAllPrefixedObjToTriplestore(prefix string) error {

	objKeys, err := synchronizer.S3Client.ObjectList(prefix)
	if err != nil {
		log.Error(err)
		return err
	}

	log.Infof("%d objects found for prefix: %s:%s", len(objKeys), synchronizer.syncBucketName, prefix)

	var errorGroup errgroup.Group

	graphsToInsert := make([]common.NamedGraph, len(objKeys))

	for i, graphName := range objKeys {
		graphName := graphName // Capture loop variable
		i := i
		errorGroup.Go(func() error {
			namedGraph, err := synchronizer.S3Client.GetObjectAndConvertToGraph(graphName.Key, synchronizer.jsonldProcessor, synchronizer.jsonldOptions)
			if err != nil {
				return err
			}
			// by placing the named graph in the slice we can
			// append without needing to use a mutex
			graphsToInsert[i] = namedGraph
			return nil
		})
	}
	if err := errorGroup.Wait(); err != nil {
		return err
	}
	if err := synchronizer.GraphClient.UpsertNamedGraphs(graphsToInsert); err != nil {
		return err
	}

	return nil
}

// Generate a static file nq release and backup the old one
// this is accomplished by getting objects, converting them to nq
// and writing them to a pipe. This pipe is simultaneously read
// and written to the minio client so we don't block on either end
func (synchronizer *SynchronizerClient) GenerateNqRelease(prefix string) error {
	release_nq_name, err := makeReleaseNqName(prefix)
	if err != nil {
		return err
	}

	// Create an io.Pipe to stream data
	pr, pw := io.Pipe()

	// Start streaming NQ data into the pipe
	go func() {
		err := synchronizer.streamNqFromPrefix(prefix, pw)
		pw.CloseWithError(err) // Ensure errors propagate to the reader
	}()

	nqReleasePath := fmt.Sprintf("graphs/latest/%s", release_nq_name)
	objInfo, err := synchronizer.S3Client.Client.PutObject(
		context.Background(),
		synchronizer.syncBucketName,
		nqReleasePath,
		pr,
		-1, // Unknown size for streaming
		minio.PutObjectOptions{},
	)
	if err != nil {
		return err
	}
	if objInfo.Size == 0 {
		return errors.New("empty nq file when uploading to s3")
	}
	log.Infof("Successfully uploaded N-Quads to %s (%d bytes)", nqReleasePath, objInfo.Size)

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

	req, err := custom_http_trace.NewRequestWithContext("POST", synchronizer.GraphClient.BaseSparqlQueryUrl, bytes.NewReader(byt))
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
