package synchronizer

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"mime"
	"nabu/internal/common"
	"nabu/internal/synchronizer/objects"
	"nabu/internal/synchronizer/triplestore"
	"nabu/pkg/config"
	"net/http"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/piprate/json-gold/ld"
	log "github.com/sirupsen/logrus"
)

// Client to perform operations that synchronize the graph database with the object store
type SynchronizerClient struct {
	// the client used for communicating with the triplestore
	GraphClient *triplestore.GraphDbClient
	// the client used for communicating with s3
	S3Client *objects.MinioClientWrapper
	// default bucket in the s3 that is used for synchronization
	syncBucketName string
	// processor for JSON-LD operations; stored in this struct so we can
	// cache context mappings
	jsonldProcessor *ld.JsonLdProcessor
	// options that are applied with the processor when performing jsonld conversions
	jsonldOptions *ld.JsonLdOptions
}

func NewSynchronizerClient(graphClient *triplestore.GraphDbClient, s3Client *objects.MinioClientWrapper, bucketName string) (SynchronizerClient, error) {
	processor, options, err := common.NewJsonldProcessor(config.NabuConfig{})
	if err != nil {
		return SynchronizerClient{}, err
	}

	return SynchronizerClient{
		GraphClient:     graphClient,
		S3Client:        s3Client,
		syncBucketName:  bucketName,
		jsonldProcessor: processor,
		jsonldOptions:   options,
	}, nil
}

// Generate a new SynchronizerClient
func NewSynchronizerClientFromConfig(conf config.NabuConfig) (*SynchronizerClient, error) {
	graphClient, err := triplestore.NewGraphDbClient(conf.Sparql)
	if err != nil {
		return nil, err
	}
	s3Client, err := objects.NewMinioClientWrapper(conf.Minio)
	if err != nil {
		return nil, err
	}

	processor, options, err := common.NewJsonldProcessor(conf)
	if err != nil {
		return nil, err
	}

	return &SynchronizerClient{
		GraphClient:     graphClient,
		S3Client:        s3Client,
		syncBucketName:  conf.Minio.Bucket,
		jsonldProcessor: processor,
		jsonldOptions:   options,
	}, nil
}

// Get rid of graphs with specific prefix in the triplestore that are not in the object store
// Drops are determined by mapping a prefix to the associated URN
func (synchronizer *SynchronizerClient) RemoveGraphsNotInS3(s3Prefixes []string) error {

	for _, prefix := range s3Prefixes {
		// collect the objects associated with the source
		objectNamesInS3, err := common.ObjectList(synchronizer.syncBucketName, synchronizer.S3Client.Client, prefix)
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
			s3ObjUrn, err := common.MakeURN(objectName)
			if err != nil {
				return err
			}
			s3UrnToAssociatedObjName[s3ObjUrn] = objectName // key (URN)= value (object prefixpath)
			s3ObjGraphNames = append(s3ObjGraphNames, s3ObjUrn)
		}

		triplestoreGraphsNotInS3 := findMissing(graphsInTriplestore, s3ObjGraphNames)
		s3GraphsNotInTriplestore := findMissing(s3ObjGraphNames, graphsInTriplestore)

		log.Infof("Current graph items: %d  Cuurent object items: %d\n", len(graphsInTriplestore), len(s3ObjGraphNames))
		log.Infof("Orphaned items to remove: %d\n", len(triplestoreGraphsNotInS3))
		log.Infof("Missing items to add: %d\n", len(s3GraphsNotInTriplestore))

		log.WithFields(log.Fields{"prefix": prefix, "graph items": len(graphsInTriplestore), "object items": len(s3ObjGraphNames), "difference": len(triplestoreGraphsNotInS3),
			"missing": len(s3GraphsNotInTriplestore)}).Info("Nabu Prune")

		// All triplestore graphs not in s3 should be removed since s3 is the source of truth
		for _, graph := range triplestoreGraphsNotInS3 {
			log.Infof("Removed graph: %s\n", graph)
			err = synchronizer.GraphClient.DropGraph(graph)
			if err != nil {
				log.Errorf("Drop graph issue: %v\n", err)
				return err
			}
		}

		for _, graphUrnName := range s3GraphsNotInTriplestore {
			graphObjectName := s3UrnToAssociatedObjName[graphUrnName]
			log.Tracef("Add graph: %s  %s \n", graphUrnName, graphObjectName)

			objBytes, err := synchronizer.S3Client.GetObjectAsBytes(graphObjectName)
			if err != nil {
				return err
			}

			err = synchronizer.upsertDataForGraph(objBytes, graphObjectName)
			if err != nil {
				return err
			}
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
func (synchronizer *SynchronizerClient) upsertDataForGraph(rawJsonldOrNqBytes []byte, objectName string) error {

	graphResourceIdentifier, err := common.MakeURN(objectName)
	if err != nil {
		return err
	}

	mimetype := mime.TypeByExtension(filepath.Ext(objectName))
	var nTriples string

	if strings.Compare(mimetype, "application/ld+json") == 0 {
		nTriples, err = common.JsonldToNQ(string(rawJsonldOrNqBytes), synchronizer.jsonldProcessor, synchronizer.jsonldOptions)
		if err != nil {
			log.Errorf("JSONLD to NQ conversion error: %s", err)
			return err
		}
	} else {
		nTriples, _, err = common.QuadsToTripleWithCtx(string(rawJsonldOrNqBytes))
		if err != nil {
			log.Errorf("nq to NTCtx error: %s", err)
			return err
		}
	}

	// drop any graph we are going to load..  we assume we are doing those due to an update
	err = synchronizer.GraphClient.DropGraph(graphResourceIdentifier)
	if err != nil {
		log.Error(err)
		return err
	}

	// If the graph is a quad already..   we need to make it triples
	// so we can load with "our" context.
	// Note: We are tossing source prov for out prov

	// TODO if array is too large, need to split it and load parts
	// Let's declare 10k lines the largest we want to send in.
	log.Infof("Loading graph %s of size: %d", graphResourceIdentifier, len(nTriples))

	const maxSizeBeforeSplit = 10000

	tripleScanner := bufio.NewScanner(strings.NewReader(nTriples))
	lineCount := 0
	tripleArray := []string{}
	// TODO PARALLELIZE
	for tripleScanner.Scan() {
		lineCount = lineCount + 1
		tripleArray = append(tripleArray, tripleScanner.Text())
		if lineCount == maxSizeBeforeSplit { // use line count, since byte len might break inside a triple statement..   it's an OK proxy
			log.Debugf("Loading subgraph of %d lines", len(tripleArray))
			err = synchronizer.GraphClient.InsertWithNamedGraph(strings.Join(tripleArray, "\n"), graphResourceIdentifier) // convert []string to strings joined with new line to form a RDF NT set
			if err != nil {
				log.Errorf("Error uploading subgraph: %s", err)
				return err
			}
			tripleArray = []string{}
			lineCount = 0
		}
	}
	// We previously used a scanner which splits triples into multiple lines
	// If there are triples left over after finishing that loop, we still need to load
	// them in even if the total amount remaining is less than our max threshold for loading
	if len(tripleArray) > 0 {
		log.Tracef("Subgraph (out of scanner) of %d lines", len(tripleArray))
		err = synchronizer.GraphClient.InsertWithNamedGraph(strings.Join(tripleArray, "\n"), graphResourceIdentifier) // convert []string to strings joined with new line to form a RDF NT set
		if err != nil {
			return err
		}
	}

	return err
}

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

		for _, graphName := range objKeys {

			objBytes, err := synchronizer.S3Client.GetObjectAsBytes(graphName)
			if err != nil {
				return err
			}

			err = synchronizer.upsertDataForGraph(objBytes, graphName)
			if err != nil {
				log.Error(err)
				return err
			}
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
		err := getObjectsAndWriteToPipe(synchronizer, destPrefix, pipeWriter)
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
func (synchronizer *SynchronizerClient) GenerateNqReleaseAndArchiveOld(prefixes []string) error {

	for _, prefix := range prefixes {
		sp := strings.Split(prefix, "/")
		srcname := strings.Join(sp[1:], "__")
		spj := strings.Join(sp, "__")

		// Here we will either make this a _release.nq or a _prov.nq based on the source string.
		name_latest := ""
		if contains(sp, "summoned") {
			name_latest = fmt.Sprintf("%s_release.nq", getTextBeforeDot(path.Base(srcname))) // ex: counties0_release.nq
		} else if contains(sp, "prov") {
			name_latest = fmt.Sprintf("%s_prov.nq", getTextBeforeDot(path.Base(srcname))) // ex: counties0_prov.nq
		} else if contains(sp, "orgs") {
			name_latest = "organizations.nq" // ex: counties0_org.nq
			fmt.Println(synchronizer.syncBucketName)
			fmt.Println(name_latest)
			err := synchronizer.CopyBetweenS3PrefixesWithPipe(name_latest, "orgs", "graphs/latest")
			if err != nil {
				return err
			}
			return err
		} else {
			return errors.New("unable to form a release graph name.  Path is not one of 'summoned', 'prov' or 'org'")
		}

		// Make a release graph that will be stored in graphs/latest as {provider}_release.nq
		err := synchronizer.CopyBetweenS3PrefixesWithPipe(name_latest, prefix, "graphs/latest") // have this function return the object name and path, easy to load and remove then
		if err != nil {
			return err
		}

		// Copy the "latest" graph just made to archive with a date
		// This means the graph in latests is a duplicate of the most recently dated version in archive/{provider}
		const timeFormat = "2000-01-02-15-04-05"
		t := time.Now()
		name := fmt.Sprintf("%s/%s/%s_%s_release.nq", "graphs/archive", srcname, getTextBeforeDot(path.Base(spj)), t.Format(timeFormat))
		latest_fullpath := fmt.Sprintf("%s/%s", "graphs/latest", name_latest)
		// TODO PARALLELIZE
		err = synchronizer.S3Client.Copy(synchronizer.syncBucketName, latest_fullpath, synchronizer.syncBucketName, strings.Replace(name, "latest", "archive", 1))
		if err != nil {
			return err
		}
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

	// Create request
	req, err := http.NewRequest("POST", synchronizer.GraphClient.BaseSparqlQueryUrl, bytes.NewReader(byt))
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
