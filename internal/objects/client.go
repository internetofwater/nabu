package objects

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"nabu/internal/common"
	"nabu/internal/graph"
	"nabu/pkg/config"
	"net/http"
	"path"
	"strings"

	"github.com/minio/minio-go/v7"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

// Wrapper to allow us to extend the minio client struct with new methods
type MinioClientWrapper struct {
	Client *minio.Client
}

// Remove is the generic object collection function
func (m *MinioClientWrapper) Remove(bucket, object string) error {
	opts := minio.RemoveObjectOptions{
		GovernanceBypass: true,
	}

	err := m.Client.RemoveObject(context.Background(), bucket, object, opts)
	if err != nil {
		log.Error(err)
		return err
	}

	return err
}

// Copy is the generic object collection function
func (m *MinioClientWrapper) Copy(srcbucket, srcobject, dstbucket, dstobject string) error {

	// Use-case 1: Simple copy object with no conditions.
	// Source object
	srcOpts := minio.CopySrcOptions{
		Bucket: srcbucket,
		Object: srcobject,
	}

	// Destination object
	dstOpts := minio.CopyDestOptions{
		Bucket: dstbucket,
		Object: dstobject,
	}

	// Copy object call
	uploadInfo, err := m.Client.CopyObject(context.Background(), dstOpts, srcOpts)
	if err != nil {
		log.Println(err)
		return err
	}

	log.Println("Successfully copied object:", uploadInfo)

	return nil
}
func (m *MinioClientWrapper) GetObjects(bucketName string, prefixes []string) ([]string, error) {
	oa := []string{}

	for _, prefix := range prefixes {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		objectCh := m.Client.ListObjects(ctx, bucketName, minio.ListObjectsOptions{Prefix: prefix, Recursive: true})

		for object := range objectCh {
			if object.Err != nil {
				log.Println(object.Err)
				return oa, object.Err
			}
			oa = append(oa, object.Key)
		}
		log.Printf("%s:%s object count: %d\n", bucketName, prefix, len(oa))
	}

	return oa, nil
}

// GetS3Bytes simply pulls the byes of an object from the store
func (m *MinioClientWrapper) GetS3Bytes(bucket, object string) ([]byte, string, error) {
	fo, err := m.Client.GetObject(context.Background(), bucket, object, minio.GetObjectOptions{})
	if err != nil {
		log.Info(err)
		return nil, "", err
	}
	defer fo.Close()

	oi, err := fo.Stat()
	if err != nil {
		log.Infof("Issue with reading an object:  %s%s", bucket, object)
	}

	dgraph := ""
	if len(oi.Metadata["X-Amz-Meta-Dgraph"]) > 0 {
		dgraph = oi.Metadata["X-Amz-Meta-Dgraph"][0]
	}

	// fmt.Printf("%s %s %s \n", urlval, sha1val, resuri)

	// TODO  set an upper byte size  limit here and return error if the size is too big
	// TODO  why was this done, return size too and let the calling function worry about it...????
	//sz := oi.Size        // what type is this...
	//if sz > 1073741824 { // if bigger than 1 GB (which is very small) move on
	//	return nil, "", errors.New("gets3bytes says file above processing size threshold")
	//}

	buf := new(bytes.Buffer)
	_, err = buf.ReadFrom(fo)
	if err != nil {
		return nil, "", err
	}

	bb := buf.Bytes() // Does a complete copy of the bytes in the buffer.

	return bb, dgraph, err
}

// BulkLoad
// This functions could be used to load stored release graphs to the graph database
func (m *MinioClientWrapper) BulkLoad(v1 *viper.Viper, bucketName string, item string) (string, error) {
	//spql, err := config.GetSparqlConfig(v1)
	//if err != nil {
	//	return "", err
	//}
	epflag := v1.GetString("flags.endpoint")
	spql, err := config.GetEndpoint(v1, epflag, "bulk")
	if err != nil {
		log.Error(err)
	}
	ep := spql.URL
	md := spql.Method
	ct := spql.Accept

	// check for the required bulk endpoint, no need to move on from here
	if spql.URL == "" {
		return "", errors.New("the configuration file lacks an endpointBulk entry")
	}

	log.Printf("Object %s:%s for %s with method %s type %s", bucketName, item, ep, md, ct)

	b, _, err := m.GetS3Bytes(bucketName, item)
	if err != nil {
		return "", err
	}

	// NOTE:   commented out, but left.  Since we are loading quads, no need for a graph.
	// If (when) we add back in ntriples as a version, this could be used to build a graph for
	// All the triples in the bulk file to then load as triples + general context (graph)
	// Review if this graph g should b here since we are loading quads
	// I don't think it should b.   validate with all the tested triple stores
	//bn := strings.Replace(bucketName, ".", ":", -1) // convert to urn : values, buckets with . are not valid IRIs
	g, err := common.MakeURN(item)
	if err != nil {
		log.Errorf("gets3Bytes %v\n", err)
		return "", err // Assume return. since on this error things are not good?
	}
	url := fmt.Sprintf("%s?graph=%s", ep, g)

	// check if JSON-LD and convert to RDF
	if strings.Contains(item, ".jsonld") {
		nb, err := common.JsonldToNQ(string(b))
		if err != nil {
			return "", err
		}
		b = []byte(nb)
	}

	req, err := http.NewRequest(md, url, bytes.NewReader(b))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", ct) // needs to be x-nquads for blaze, n-quads for jena and graphdb
	req.Header.Set("User-Agent", "EarthCube_DataBot/1.0")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {
			log.Error(err)
		}
	}(resp.Body)

	log.Println(resp)
	body, err := io.ReadAll(resp.Body) // return body if you want to debugg test with it
	if err != nil {
		log.Println(string(body))
		return string(body), err
	}

	// report
	log.Println(string(body))
	log.Printf("success: %s : %d  : %s\n", item, len(b), ep)

	return string(body), err
}

// BulkAssembly collects the objects from a bucket to load
func (m *MinioClientWrapper) BulkAssembly(v1 *viper.Viper) error {
	bucketName, _ := config.GetBucketName(v1)
	objCfg, _ := config.GetConfigForS3Objects(v1)
	pa := objCfg.Prefix

	var err error

	for p := range pa {
		name := fmt.Sprintf("%s_bulk.rdf", baseName(path.Base(pa[p])))
		err = graph.PipeCopy(v1, m.Client, name, bucketName, pa[p], "scratch") // have this function return the object name and path, easy to load and remove then
		//err = objects.MillerNG(name, bucketName, pa[p], mc) // have this function return the object name and path, easy to load and remove then
		if err != nil {
			return err
		}
	}

	for p := range pa {
		name := fmt.Sprintf("%s_bulk.rdf", baseName(path.Base(pa[p])))
		_, err := m.BulkLoad(v1, bucketName, fmt.Sprintf("/scratch/%s", name))
		if err != nil {
			log.Println(err)
		}
	}

	// TODO  remove the temporary object?
	for p := range pa {
		//name := fmt.Sprintf("%s_bulk.rdf", pa[p])
		name := fmt.Sprintf("%s_bulk.rdf", baseName(path.Base(pa[p])))
		opts := minio.RemoveObjectOptions{}
		err = m.Client.RemoveObject(context.Background(), bucketName, fmt.Sprintf("%s/%s", pa[p], name), opts)
		if err != nil {
			fmt.Println(err)
			return err
		}
	}

	return err
}

func baseName(s string) string {
	n := strings.LastIndexByte(s, '.')
	if n == -1 {
		return s
	}
	return s[:n]
}
