package synchronizer

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"nabu/internal/common"
	"nabu/internal/custom_http_trace"
	"nabu/internal/synchronizer/s3"
	"nabu/internal/synchronizer/triplestore"
	"nabu/pkg/config"
	"net/http"
	"path"
	"slices"
	"strings"
	"sync"

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

// Create a new SynchronizerClient by directly passing in the clients
// Mainly used for testing
func newSynchronizerClient(graphClient *triplestore.GraphDbClient, s3Client *s3.MinioClientWrapper, bucketName string) (SynchronizerClient, error) {
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
	}
	return client, nil
}

// Generate a new SynchronizerClient from a top level config
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

// Get rid of graphs with specific prefix in the triplestore that are not in the object store
// Drops are determined by mapping a prefix to the associated URN
func (synchronizer *SynchronizerClient) SyncTriplestoreGraphs(s3Prefixes []string) error {

	for _, prefix := range s3Prefixes {

		// collect the objects associated with the source
		objectNamesInS3, err := synchronizer.S3Client.ObjectList(prefix)
		if err != nil {
			log.Error(err)
			return err
		}

		// collect the named graphs from graph associated with the source
		graphsInTriplestore, err := synchronizer.GraphClient.NamedGraphsAssociatedWithS3Prefix(prefix)
		if err != nil {
			log.Error(err)
			return err
		}

		// convert the object names to the URN pattern used in the graph
		// and make a map where key = URN, value = object name
		// NOTE:  since later we want to look up the object based the URN
		// we will do it this way since mapswnat you to know a key, not a value, when
		// querying them.
		// This is OK since all KV pairs involve unique keys and unique values
		var s3UrnToAssociatedObjName = map[string]string{}
		// create a list of just the names so we can diff against it
		var s3ObjGraphNames []string
		for _, objectName := range objectNamesInS3 {
			s3ObjUrn, err := common.MakeURN(objectName.Key)
			if err != nil {
				return err
			}
			s3UrnToAssociatedObjName[s3ObjUrn] = objectName.Key // key (URN)= value (object prefixpath)
			s3ObjGraphNames = append(s3ObjGraphNames, s3ObjUrn)
		}

		triplestoreGraphsNotInS3 := findMissing(graphsInTriplestore, s3ObjGraphNames)
		s3GraphsNotInTriplestore := findMissing(s3ObjGraphNames, graphsInTriplestore)

		log.Infof("Current graph items: %d  Curent object items: %d\n", len(graphsInTriplestore), len(s3ObjGraphNames))
		log.Infof("Orphaned items to remove: %d\n", len(triplestoreGraphsNotInS3))
		log.Infof("Missing items to add: %d\n", len(s3GraphsNotInTriplestore))

		log.WithFields(log.Fields{"prefix": prefix, "graph items": len(graphsInTriplestore), "object items": len(s3ObjGraphNames), "difference": len(triplestoreGraphsNotInS3),
			"missing": len(s3GraphsNotInTriplestore)}).Info("Nabu sync")

		// All triplestore graphs not in s3 should be removed since s3 is the source of truth
		if len(triplestoreGraphsNotInS3) > 0 {
			log.Infof("Dropping %d graphs from triplestore", len(triplestoreGraphsNotInS3))
			if err := synchronizer.GraphClient.DropGraphs(triplestoreGraphsNotInS3); err != nil {
				log.Errorf("Drop graph issue: %v\n", err)
				return err
			}
		}

		var errorGroup errgroup.Group
		errorGroup.SetLimit(40)

		log.Infof("Upserting %d objects from S3 to triplestore", len(s3GraphsNotInTriplestore))
		for _, graphUrnName := range s3GraphsNotInTriplestore {
			graphObjectName := s3UrnToAssociatedObjName[graphUrnName]

			// loop variable shadowing for goroutine
			graphObjectNameCopy := graphObjectName

			errorGroup.Go(func() error {
				namedGraph, err := synchronizer.S3Client.GetObjectAsNamedGraph(graphObjectNameCopy, synchronizer.jsonldProcessor, synchronizer.jsonldOptions)
				if err != nil {
					return err
				}
				return synchronizer.GraphClient.InsertNamedGraphs([]common.NamedGraph{namedGraph})
			})
		}
		if err := errorGroup.Wait(); err != nil {
			return err
		}
	}

	return nil
}

// Put data into the triplestore, associated with a graph
// If the graph already exists, it will be dropped first to prevent duplications
// THe upload process is chunking to prevent loading super large nq files all at once
//
// Takes in the raw bytes which represent rdf data and the associated
// named triplestore.
// objectName should be a full path to the object in the s3 bucket and end with the filename with the proper extension
// func (synchronizer *SynchronizerClient) upsertNamedGraphs(rawJsonldOrNqBytes []byte, objectName string) error {

// graphResourceIdentifier, err := common.MakeURN(objectName)
// if err != nil {
// 	return err
// }

// var nTriples string

// if strings.HasSuffix(objectName, ".jsonld") {
// 	nTriples, err = common.JsonldToNQ(string(rawJsonldOrNqBytes), synchronizer.jsonldProcessor, synchronizer.jsonldOptions)
// 	if err != nil {
// 		log.Errorf("JSONLD to NQ conversion error: %s", err)
// 		return err
// 	}
// } else if strings.HasSuffix(objectName, ".nq") {
// 	nTriples, _, err = common.QuadsToTripleWithCtx(string(rawJsonldOrNqBytes))
// 	if err != nil {
// 		return fmt.Errorf("nq to NTCtx error: %s when converting object %s with data %s", err, objectName, string(rawJsonldOrNqBytes))
// 	}
// } else {
// 	return fmt.Errorf("object %s is not a jsonld or nq file", objectName)
// }

// 	// If the graph is a quad already..   we need to make it triples
// 	// so we can load with "our" context.
// 	// Note: We are tossing source prov for out prov

// 	// TODO if array is too large, need to split it and load parts
// 	// Let's declare 10k lines the largest we want to send in.

// 	const maxSizeBeforeSplit = 10000

// 	tripleScanner := bufio.NewScanner(strings.NewReader(nTriples))
// 	lineCount := 0
// 	tripleArray := []string{}

// 	var errorGroup errgroup.Group

// 	for tripleScanner.Scan() {

// 		lineCount = lineCount + 1
// 		tripleArray = append(tripleArray, tripleScanner.Text())
// 		if lineCount == maxSizeBeforeSplit { // use line count, since byte len might break inside a triple statement..   it's an OK proxy
// 			errorGroup.Go(func() error {
// 				log.Debugf("Loading subgraph of %d lines", len(tripleArray))
// 				err := synchronizer.GraphClient.InsertWithNamedGraph(strings.Join(tripleArray, "\n"), graphResourceIdentifier) // convert []string to strings joined with new line to form a RDF NT set
// 				if err != nil {
// 					log.Errorf("Error uploading subgraph: %s", err)
// 					return err
// 				}
// 				return nil
// 			})
// 			tripleArray = []string{}
// 			lineCount = 0
// 		}
// 	}
// 	// We previously used a scanner which splits triples into multiple lines
// 	// If there are triples left over after finishing that loop, we still need to load
// 	// them in even if the total amount remaining is less than our max threshold for loading
// 	if len(tripleArray) > 0 {
// 		// log.Debugf("Finished reading scanner; Inserting left over subgraph of %d lines", len(tripleArray))
// 		err = synchronizer.GraphClient.InsertWithNamedGraph(strings.Join(tripleArray, "\n"), graphResourceIdentifier) // convert []string to strings joined with new line to form a RDF NT set
// 		if err != nil {
// 			return err
// 		}
// 	}

// 	// log.Debug("Waiting for subgraph load goroutines to finish")
// 	if err := errorGroup.Wait(); err != nil {
// 		return err
// 	}

// 	// log.Debugf("Finished loading graph %s", graphResourceIdentifier)

// 	return nil
// }

// Gets all graphs in s3 with a specific prefix and loads them into the triplestore
func (synchronizer *SynchronizerClient) CopyAllPrefixedObjToTriplestore(prefixes []string) error {

	for _, prefix := range prefixes {
		objKeys := []string{}

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		objectCh := synchronizer.S3Client.Client.ListObjects(ctx, synchronizer.syncBucketName, minio.ListObjectsOptions{Prefix: prefix, Recursive: true})

		for object := range objectCh {
			if object.Err != nil {
				log.Error(object.Err)
				return object.Err
			}
			objKeys = append(objKeys, object.Key)
		}

		log.Infof("%d objects found for prefix: %s:%s", len(objKeys), synchronizer.syncBucketName, prefix)

		var errorGroup errgroup.Group

		for _, graphName := range objKeys {
			graphNameCopy := graphName // Capture loop variable
			errorGroup.Go(func() error {
				namedGraph, err := synchronizer.S3Client.GetObjectAsNamedGraph(graphNameCopy, synchronizer.jsonldProcessor, synchronizer.jsonldOptions)
				if err != nil {
					return err
				}
				return synchronizer.GraphClient.InsertNamedGraphs([]common.NamedGraph{namedGraph})
			})
		}

		if err := errorGroup.Wait(); err != nil {
			return err
		}
	}

	return nil
}

// writes a new object based on an prefix, this function assumes the objects are valid when concatenated
func (synchronizer *SynchronizerClient) CopyBetweenS3PrefixesWithPipe(objectName, srcPrefix, destPrefix string) error {

	pipeReader, pipeWriter := io.Pipe()       // TeeReader of use?
	pipeTransferWorkGroup := sync.WaitGroup{} // work group for the pipe writes...
	pipeTransferWorkGroup.Add(2)              // We add 2 since there is a write to the pipe and a read from the pipe

	errChan := make(chan error, 2)

	// Write the nq files to the pipe
	go func() {
		defer pipeTransferWorkGroup.Done()
		err := getObjectsAndWriteToPipeAsNq(synchronizer, destPrefix, pipeWriter)
		if err != nil {
			log.Error(err)
			errChan <- err
			return
		}
	}()

	// read the nq files from the pipe and copy them to minio
	go func() {
		defer pipeTransferWorkGroup.Done()
		_, err := synchronizer.S3Client.Client.PutObject(context.Background(), synchronizer.syncBucketName, fmt.Sprintf("%s/%s", destPrefix, objectName), pipeReader, -1, minio.PutObjectOptions{})
		if err != nil {
			log.Error(err)
			errChan <- err
			return
		}
	}()

	pipeTransferWorkGroup.Wait()
	err := pipeWriter.Close()
	if err != nil {
		return err
	}
	err = pipeReader.Close()
	if err != nil {
		return err
	}

	// close the channel so we can read from it
	close(errChan)

	for val := range errChan {
		if val != nil {
			return val
		}
	}

	return nil
}

// Generate a static file nq release and backup the old one
func (synchronizer *SynchronizerClient) GenerateNqRelease(prefix string) error {

	prefix_parts := strings.Split(prefix, "/")
	if len(prefix_parts) < 1 {
		return fmt.Errorf("prefix %s did not contain a slash and thus is ambiguous", prefix)
	}
	// i.e. summoned/counties0 would become counties0
	prefix_path_as_filename := getTextBeforeDot(path.Base(strings.Join(prefix_parts[1:], "_")))

	var name_latest string

	if slices.Contains(prefix_parts, "summoned") && prefix_path_as_filename != "" {
		name_latest = fmt.Sprintf("%s_release.nq", prefix_path_as_filename) // ex: counties0_release.nq
	} else if slices.Contains(prefix_parts, "prov") && prefix_path_as_filename != "" {
		name_latest = fmt.Sprintf("%s_prov.nq", prefix_path_as_filename) // ex: counties0_prov.nq
	} else if slices.Contains(prefix_parts, "orgs") {
		if prefix_path_as_filename == "" {
			name_latest = "organizations.nq"
		} else {
			name_latest = fmt.Sprintf("%s_organizations.nq", prefix_path_as_filename)
		}
	} else {
		return fmt.Errorf("unable to form a release graph name from prefix %s", prefix)
	}

	// Make a release graph that will be stored in graphs/latest as {provider}_release.nq
	err := synchronizer.CopyBetweenS3PrefixesWithPipe(name_latest, prefix, "graphs/latest") // have this function return the object name and path, easy to load and remove then
	if err != nil {
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
