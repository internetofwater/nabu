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
)

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

// Stream objects in S3 with a specific prefix into a writer while converting them to nquads
func (synchronizer *SynchronizerClient) streamNqFromPrefix(prefix string, w io.Writer) error {
	objects, err := synchronizer.S3Client.ObjectList(prefix)
	if err != nil {
		return err
	}
	if len(objects) == 0 {
		return fmt.Errorf("no objects found with prefix %s so no nq file will be created", prefix)
	}

	log.Infof("Generating nq from %d objects with prefix %s", len(objects), prefix)

	for _, object := range objects {
		retrievedObject, err := synchronizer.S3Client.Client.GetObject(context.Background(), synchronizer.S3Client.DefaultBucket, object.Key, minio.GetObjectOptions{})
		if err != nil {
			return err
		}
		defer retrievedObject.Close() // Close after reading

		rawBytes, err := io.ReadAll(retrievedObject)
		if err != nil {
			return err
		}

		var nq string
		if strings.HasSuffix(object.Key, ".nq") {
			nq = string(rawBytes)
		} else {
			nq, err = common.JsonldToNQ(string(rawBytes), synchronizer.jsonldProcessor, synchronizer.jsonldOptions)
			if err != nil {
				return err
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

		// Get graph URI
		graphURN, err := common.MakeURN(object.Key)
		if err != nil {
			return err
		}

		// Convert NT to NQ
		csnq, err := common.NtToNq(singleFileNquad, graphURN)
		if err != nil {
			return err
		}

		// Write directly to the pipe
		_, err = w.Write([]byte(csnq))
		if err != nil {
			return err
		}
	}

	return nil
}
