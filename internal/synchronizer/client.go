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
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

// Client to perform operations that synchronize the graph database with the object store
type SynchronizerClient struct {
	// the client used for communicating with the triplestore
	GraphClient *triplestore.GraphDbClient
	// the client used for communicating with s3
	S3Client *objects.MinioClientWrapper
	// default bucket in the s3 that is used for synchronization
	bucketName string
}

func NewSynchronizerClient(graphClient *triplestore.GraphDbClient, s3Client *objects.MinioClientWrapper, bucketName string) SynchronizerClient {
	return SynchronizerClient{GraphClient: graphClient, S3Client: s3Client, bucketName: bucketName}
}

// Generate a new SynchronizerClient from the viper config
func NewSynchronizerClientFromViper(v1 *viper.Viper) (*SynchronizerClient, error) {
	graphClient, err := triplestore.NewGraphDbClient(v1)
	if err != nil {
		return nil, err
	}
	s3Client, err := objects.NewMinioClientWrapper(v1)
	if err != nil {
		return nil, err
	}
	bucketName, err := config.GetBucketName(v1)
	if err != nil {
		return nil, err
	}

	return &SynchronizerClient{GraphClient: graphClient, S3Client: s3Client, bucketName: bucketName}, nil
}

// Get rid of graphs with specif in the triplestore that are not in the object store
func (synchronizer *SynchronizerClient) RemoveGraphsNotInS3(prefixes []string) error {

	for _, prefix := range prefixes {
		// collect the objects associated with the source
		objectNamesInS3, err := common.ObjectList(synchronizer.bucketName, synchronizer.S3Client.Client, prefix)
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

		//compare lists, anything IN graph not in objects list should be removed
		triplestoreGraphsNotInS3 := difference(graphsInTriplestore, s3ObjGraphNames)  // return items in ga that are NOT in oag, we should remove these
		s3GraphsNotInTriplestore := findMissing(s3ObjGraphNames, graphsInTriplestore) // return items from oag we need to add

		log.Infof("Current graph items: %d  Cuurent object items: %d\n", len(graphsInTriplestore), len(s3ObjGraphNames))
		log.Infof("Orphaned items to remove: %d\n", len(triplestoreGraphsNotInS3))
		log.Infof("Missing items to add: %d\n", len(s3GraphsNotInTriplestore))

		log.WithFields(log.Fields{"prefix": prefix, "graph items": len(graphsInTriplestore), "object items": len(s3ObjGraphNames), "difference": len(triplestoreGraphsNotInS3),
			"missing": len(s3GraphsNotInTriplestore)}).Info("Nabu Prune")

		// All graphs not in s3 should be removed since s3 is the source of truth
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

			err = synchronizer.UpsertDataForGraph(objBytes, graphObjectName)
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
func (synchronizer *SynchronizerClient) UpsertDataForGraph(rawJsonldOrNqBytes []byte, objectName string) error {

	graphName, err := common.MakeURN(objectName)
	if err != nil {
		return err
	}

	mt := mime.TypeByExtension(filepath.Ext(objectName))
	nTriples := ""

	if strings.Compare(mt, "application/ld+json") == 0 {
		nTriples, err = common.JsonldToNQ(string(rawJsonldOrNqBytes))
		if err != nil {
			log.Errorf("JSONLDToNQ err: %s", err)
			return err
		}
	} else {
		nTriples, _, err = common.NQToNTCtx(string(rawJsonldOrNqBytes))
		if err != nil {
			log.Errorf("nqToNTCtx err: %s", err)
			return err
		}
	}

	// drop any graph we are going to load..  we assume we are doing those due to an update
	err = synchronizer.GraphClient.DropGraph(graphName)
	if err != nil {
		log.Error(err)
		return err
	}

	// If the graph is a quad already..   we need to make it triples
	// so we can load with "our" context.
	// Note: We are tossing source prov for out prov

	log.Tracef("Graph loading as: %s\n", graphName)

	// TODO if array is too large, need to split it and load parts
	// Let's declare 10k lines the largest we want to send in.
	log.Tracef("Graph size: %d\n", len(nTriples))

	const maxSizeBeforeSplit = 10000

	tripleScanner := bufio.NewScanner(strings.NewReader(nTriples))
	lineCount := 0
	tripleArray := []string{}
	// TODO PARALLELIZE
	for tripleScanner.Scan() {
		lineCount = lineCount + 1
		tripleArray = append(tripleArray, tripleScanner.Text())
		if lineCount == maxSizeBeforeSplit { // use line count, since byte len might break inside a triple statement..   it's an OK proxy
			log.Tracef("Subgraph of %d lines", len(tripleArray))
			err = synchronizer.GraphClient.InsertWithNamedGraph(strings.Join(tripleArray, "\n"), graphName) // convert []string to strings joined with new line to form a RDF NT set
			if err != nil {
				log.Errorf("Insert err: %s", err)
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
		err = synchronizer.GraphClient.InsertWithNamedGraph(strings.Join(tripleArray, "\n"), graphName) // convert []string to strings joined with new line to form a RDF NT set
		if err != nil {
			return err
		}
	}

	return err
}

// Gets all graphs with a specific prefix and loads them into the triplestore
func (synchronizer *SynchronizerClient) CopyAllPrefixedObjToTriplestore(prefixes []string) error {

	for _, prefix := range prefixes {
		objKeys := []string{}

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		objectCh := synchronizer.S3Client.Client.ListObjects(ctx, synchronizer.bucketName, minio.ListObjectsOptions{Prefix: prefix, Recursive: true})

		for object := range objectCh {
			if object.Err != nil {
				log.Error(object.Err)
				return object.Err
			}
			objKeys = append(objKeys, object.Key)
		}

		log.Infof("%d objects found for prefix: %s:%s", len(objKeys), synchronizer.bucketName, prefix)

		for _, graphName := range objKeys {

			objBytes, err := synchronizer.S3Client.GetObjectAsBytes(graphName)
			if err != nil {
				return err
			}

			err = synchronizer.UpsertDataForGraph(objBytes, graphName)
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

	// params for list objects calls
	doneCh := make(chan struct{}) // , N) Create a done channel to control 'ListObjectsV2' go routine.
	defer close(doneCh)           // Indicate to our routine to exit cleanly upon return.

	// Write the nq files to the pipe
	go func() {
		defer pipeTransferWorkGroup.Done()
		err := getObjectsAndWriteToPipe(synchronizer, destPrefix, pipeWriter)
		if err != nil {
			log.Error(err)
		}
	}()

	// read the nq files from the pipe and copy them to minio
	go func() {
		defer pipeTransferWorkGroup.Done()
		_, err := synchronizer.S3Client.Client.PutObject(context.Background(), synchronizer.bucketName, fmt.Sprintf("%s/%s", destPrefix, objectName), pipeReader, -1, minio.PutObjectOptions{})
		//_, err := mc.PutObject(context.Background(), bucket, fmt.Sprintf("%s/%s", prefix, name), pr, -1, minio.PutObjectOptions{})
		if err != nil {
			log.Error(err)
			return
		}
	}()

	pipeTransferWorkGroup.Wait() // wait for the pipe read writes to finish
	err := pipeWriter.Close()
	if err != nil {
		return err
	}
	err = pipeReader.Close()
	if err != nil {
		return err
	}

	return nil
}

// Generate a static file nq release and backup the old one
func (synchronizer *SynchronizerClient) GenerateNqReleaseAndArchiveOld(prefixes []string) error {

	var err error

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
			fmt.Println(synchronizer.bucketName)
			fmt.Println(name_latest)
			err = synchronizer.CopyBetweenS3PrefixesWithPipe(name_latest, "orgs", "graphs/latest") // have this function return the object name and path, easy to load and remove then
			if err != nil {
				return err
			}
			return err
		} else {
			return errors.New("unable to form a release graph name.  Path is not one of 'summoned', 'prov' or 'org'")
		}

		// Make a release graph that will be stored in graphs/latest as {provider}_release.nq
		err = synchronizer.CopyBetweenS3PrefixesWithPipe(name_latest, prefix, "graphs/latest") // have this function return the object name and path, easy to load and remove then
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
		err = synchronizer.S3Client.Copy(synchronizer.bucketName, latest_fullpath, synchronizer.bucketName, strings.Replace(name, "latest", "archive", 1))
	}

	return err
}

// Loads a single stored release graph into the graph database
func (synchronizer *SynchronizerClient) UploadNqFileToTriplestore(nqPathInS3 string) error {

	byt, err := synchronizer.S3Client.GetObjectAsBytes(nqPathInS3)
	if err != nil {
		return err
	}

	// NOTE:   commented out, but left.  Since we are loading quads, no need for a triplestore.
	// If (when) we add back in ntriples as a version, this could be used to build a graph for
	// All the triples in the bulk file to then load as triples + general context (graph)
	// Review if this graph g should b here since we are loading quads
	// I don't think it should b.   validate with all the tested triple stores
	//bn := strings.Replace(bucketName, ".", ":", -1) // convert to urn : values, buckets with . are not valid IRIs
	g, err := common.MakeURN(nqPathInS3)
	if err != nil {
		return err
	}
	url := fmt.Sprintf("%s?graph=%s", synchronizer.GraphClient.SparqlConf.Endpoint, g)

	// if the file is jsonld make it nquads before it is uploaded
	if strings.Contains(nqPathInS3, ".jsonld") {
		convertedNq, err := common.JsonldToNQ(string(byt))
		if err != nil {
			return err
		}
		byt = []byte(convertedNq)
	}

	req, err := http.NewRequest(synchronizer.GraphClient.SparqlConf.EndpointMethod, url, bytes.NewReader(byt))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", synchronizer.GraphClient.SparqlConf.ContentType) // needs to be x-nquads for blaze, n-quads for jena and graphdb

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body) // return body if you want to debugg test with it
	if err != nil {
		log.Error(string(body))
		return err
	}

	log.Println(string(body))
	log.Printf("success: %s : %d  : %s\n", nqPathInS3, len(byt), synchronizer.GraphClient.SparqlConf.Endpoint)

	return err
}
