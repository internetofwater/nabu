package synchronizer

import (
	"context"
	"fmt"
	"io"
	"nabu/internal/common"
	"strings"
	"sync"

	"github.com/minio/minio-go/v7"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

// Struct holding the differences between the triplestore and s3
// and a mapping of URN to object name so we can load the graphs
// based on this diff info
type GraphDiff struct {
	TriplestoreGraphsNotInS3 []string
	S3GraphsNotInTriplestore []string
	s3UrnToAssociatedObjName map[string]string
}

/*
All functions in this file are private to the synchronizer package
and thus are not directly called by any CLI commands
*/

// Return the difference in graphs between the triplestore and s3 based on the prefix
// i.e. summoned/counties0 will check urn:iow:summoned:counties0 when comparing between the two
//
// This function runs two goroutines to fetch the triplestore and s3 data in parallel
func (synchronizer *SynchronizerClient) getGraphDiff(prefix string) (GraphDiff, error) {
	var (
		objectNamesInS3     []minio.ObjectInfo
		graphsInTriplestore []string
		wg                  sync.WaitGroup
	)

	// Using channels to fetch data in parallel
	objChan := make(chan []minio.ObjectInfo, 1)
	graphChan := make(chan []string, 1)
	errChan := make(chan error, 2)

	wg.Add(2)

	// Fetch object names from S3 in parallel
	go func() {
		defer wg.Done()
		objs, err := synchronizer.S3Client.ObjectList(prefix)
		if err != nil {
			errChan <- err
			return
		}
		objChan <- objs
	}()

	// Fetch named graphs from triplestore in parallel
	go func() {
		defer wg.Done()
		graphs, err := synchronizer.GraphClient.NamedGraphsAssociatedWithS3Prefix(prefix)
		if err != nil {
			errChan <- err
			return
		}
		graphChan <- graphs
	}()

	// Wait for both goroutines to finish
	wg.Wait()
	close(objChan)
	close(graphChan)
	close(errChan)

	// Collect results
	for err := range errChan {
		if err != nil {
			log.Println(err)
			return GraphDiff{}, err
		}
	}

	if objs, ok := <-objChan; ok {
		objectNamesInS3 = objs
	}
	if graphs, ok := <-graphChan; ok {
		graphsInTriplestore = graphs
	}

	// Convert object names to the URN pattern used in the graph
	s3UrnToAssociatedObjName := make(map[string]string)
	var s3ObjGraphNames []string
	for _, objectName := range objectNamesInS3 {
		s3ObjUrn, err := common.MakeURN(objectName.Key)
		if err != nil {
			return GraphDiff{}, err
		}
		s3UrnToAssociatedObjName[s3ObjUrn] = objectName.Key
		s3ObjGraphNames = append(s3ObjGraphNames, s3ObjUrn)
	}

	triplestoreGraphsNotInS3 := findMissing(graphsInTriplestore, s3ObjGraphNames)
	s3GraphsNotInTriplestore := findMissing(s3ObjGraphNames, graphsInTriplestore)

	return GraphDiff{
		TriplestoreGraphsNotInS3: triplestoreGraphsNotInS3,
		S3GraphsNotInTriplestore: s3GraphsNotInTriplestore,
		s3UrnToAssociatedObjName: s3UrnToAssociatedObjName,
	}, nil
}

// convert all objects in s3 with a specific prefix to nq and stream them to the channel
func (synchronizer *SynchronizerClient) streamNqFromPrefix(prefix string, nqChan chan<- string) error {
	objects, err := synchronizer.S3Client.ObjectList(prefix)
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
			defer retrievedObject.Close()

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

// Batch upserts objects from s3 to triplestore using upsertBatchSize as defined in the synchronizer client
func (synchronizer *SynchronizerClient) batchedUpsert(s3GraphNames []string) error {

	var errorGroup errgroup.Group

	var graphsToInsert []common.NamedGraph
	var insertMutex sync.Mutex

	log.Infof("Upserting %d objects from S3 to triplestore", len(s3GraphNames))
	for _, graphName := range s3GraphNames {

		errorGroup.Go(func() error {
			namedGraph, err := synchronizer.S3Client.GetObjectAndConvertToGraph(graphName, synchronizer.jsonldProcessor, synchronizer.jsonldOptions)
			if err != nil {
				return err
			}
			insertMutex.Lock()
			graphsToInsert = append(graphsToInsert, namedGraph)
			if len(graphsToInsert) >= synchronizer.upsertBatchSize {
				// make a copy so that we can pass in the data to upsert
				// even if we don't have the lock
				graphInsertCopy := graphsToInsert
				// flush the batch
				graphsToInsert = []common.NamedGraph{}
				// unlock before sending to allow for multiple concurrent upserts
				insertMutex.Unlock()
				if err := synchronizer.GraphClient.UpsertNamedGraphs(graphInsertCopy); err != nil {
					return err
				}
			} else {
				insertMutex.Unlock()
			}
			return nil
		})
	}
	if err := errorGroup.Wait(); err != nil {
		return err
	}

	// if there are any graphs left that werent big enough for a batch, send them at the end
	if len(graphsToInsert) > 0 {
		if err := synchronizer.GraphClient.UpsertNamedGraphs(graphsToInsert); err != nil {
			return err
		}
	}
	return nil
}
