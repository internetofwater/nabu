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
	"nabu/internal/synchronizer/graph"
	"nabu/internal/synchronizer/objects"
	"nabu/pkg/config"
	"net/http"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/schollz/progressbar/v3"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

// Client to perform operations that synchronize the graph database with the object store
type SynchronizerClient struct {
	graphClient *graph.GraphDbClient
	s3Client    *objects.MinioClientWrapper
	bucketName  string
	objConfig   config.Objects
}

// Generate a new SynchronizerClient from the viper config
func NewSynchronizerClient(v1 *viper.Viper) (*SynchronizerClient, error) {
	graphClient, err := graph.NewGraphDbClient(v1)
	if err != nil {
		return nil, err
	}
	s3Client, err := objects.NewMinioClientWrapper(v1)
	if err != nil {
		return nil, err
	}
	bucketName, _ := config.GetBucketName(v1)
	objCfg, err := config.GetConfigForS3Objects(v1)
	if err != nil {
		return nil, err
	}
	return &SynchronizerClient{graphClient: graphClient, s3Client: s3Client, bucketName: bucketName, objConfig: objCfg}, nil
}

// Get rid of graphs in the triplestore that are not in the object store
func (synchronizer *SynchronizerClient) RemoveGraphsNotInS3() error {
	prefixes := synchronizer.objConfig.Prefixes

	for _, prefix := range prefixes {
		// collect the objects associated with the source
		oa, err := common.ObjectList(synchronizer.bucketName, synchronizer.s3Client.Client, prefix)
		if err != nil {
			log.Error(err)
			return err
		}

		// collect the named graphs from graph associated with the source
		ga, err := synchronizer.graphClient.ListNamedGraphs(prefix)
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
		var oam = map[string]string{}
		for x := range oa {
			g, err := common.MakeURN(oa[x])
			if err != nil {
				log.Errorf("MakeURN error: %v\n", err)
			}
			oam[g] = oa[x] // key (URN)= value (object prefixpath)
		}

		// make an array of just the values for use with findMissing and difference functions
		// we have in this package
		var oag []string // array of all keys
		for k := range oam {
			oag = append(oag, k)
		}

		//compare lists, anything IN graph not in objects list should be removed
		d := difference(ga, oag)  // return items in ga that are NOT in oag, we should remove these
		m := findMissing(oag, ga) // return items from oag we need to add

		fmt.Printf("Current graph items: %d  Cuurent object items: %d\n", len(ga), len(oag))
		fmt.Printf("Orphaned items to remove: %d\n", len(d))
		fmt.Printf("Missing items to add: %d\n", len(m))

		log.WithFields(log.Fields{"prefix": prefix, "graph items": len(ga), "object items": len(oag), "difference": len(d),
			"missing": len(m)}).Info("Nabu Prune")

		// For each in d will delete that graph
		if len(d) > 0 {
			bar := progressbar.Default(int64(len(d)))
			for x := range d {
				log.Infof("Removed graph: %s\n", d[x])
				_, err = synchronizer.graphClient.DropGraph(d[x])
				if err != nil {
					log.Errorf("Drop graph issue: %v\n", err)
				}
				err = bar.Add(1)
				if err != nil {
					log.Errorf("Progress bar update issue: %v\n", err)
				}
			}
		}

		//// load new ones
		//spql, err := config.GetSparqlConfig(v1)
		//if err != nil {
		//	log.Error("prune -> config.GetSparqlConfig %v\n", err)
		//}

		if len(m) > 0 {
			bar2 := progressbar.Default(int64(len(m)))
			log.Info("uploading missing %n objects", len(m))
			for x := range m {
				np := oam[m[x]]
				log.Tracef("Add graph: %s  %s \n", m[x], np)

				panic("not implemented. make sure we loop over and pipe load each")

				bytes := make([]byte, 0)
				_, err := synchronizer.PipeLoad(bytes, np)
				if err != nil {
					log.Errorf("prune -> pipeLoad %v\n", err)
				}
				err = bar2.Add(1)
				if err != nil {
					log.Errorf("Progress bar update issue: %v\n", err)
				}
			}
		}
	}
	return nil
}

// Reads from an object and loads directly into a triplestore
func (synchronizer *SynchronizerClient) PipeLoad(bytes []byte, object string) ([]byte, error) {
	// build our quad/graph from the object path

	g, err := common.MakeURN(object)
	if err != nil {
		log.Errorf("gets3Bytes %v\n", err)
		// should this just return. since on this error things are not good
	}


	// TODO, use the mimetype or suffix in general to select the path to load    or overload from the config file?
	// check the object string
	mt := mime.TypeByExtension(filepath.Ext(object))
	//log.Printf("Object: %s reads as mimetype: %s", object, mt) // application/ld+json
	nt := ""

	// if strings.Contains(object, ".jsonld") { // TODO explore why this hack is needed and the mimetype for JSON-LD is not returned
	if strings.Compare(mt, "application/ld+json") == 0 {
		//log.Info("Convert JSON-LD file to nq")
		nt, err = common.JsonldToNQ(string(bytes))
		if err != nil {
			log.Errorf("JSONLDToNQ err: %s", err)
		}
	} else {
		nt, _, err = common.NQToNTCtx(string(bytes))
		if err != nil {
			log.Errorf("nqToNTCtx err: %s", err)
		}
	}

	// drop any graph we are going to load..  we assume we are doing those due to an update...
	_, err = synchronizer.graphClient.DropGraph(g)
	if err != nil {
		log.Error(err)
	}

	// If the graph is a quad already..   we need to make it triples
	// so we can load with "our" context.
	// Note: We are tossing source prov for out prov

	log.Tracef("Graph loading as: %s\n", g)

	// TODO if array is too large, need to split it and load parts
	// Let's declare 10k lines the largest we want to send in.
	log.Tracef("Graph size: %d\n", len(nt))

	scanner := bufio.NewScanner(strings.NewReader(nt))
	lc := 0
	sg := []string{}
	for scanner.Scan() {
		lc = lc + 1
		sg = append(sg, scanner.Text())
		if lc == 10000 { // use line count, since byte len might break inside a triple statement..   it's an OK proxy
			log.Tracef("Subgraph of %d lines", len(sg))
			// TODO..  upload what we have here, modify the call code to upload these sections
			err = synchronizer.graphClient.Insert(g, strings.Join(sg, "\n"), false) // convert []string to strings joined with new line to form a RDF NT set
			if err != nil {
				log.Errorf("Insert err: %s", err)
			}
			sg = nil // clear the array
			lc = 0   // reset the counter
		}
	}
	if lc > 0 {
		log.Tracef("Subgraph (out of scanner) of %d lines", len(sg))
		err = synchronizer.graphClient.Insert(g, strings.Join(sg, "\n"), false) // convert []string to strings joined with new line to form a RDF NT set
	}

	return []byte{}, err
}

func (synchronizer *SynchronizerClient) ObjectAssembly() error {

	for _, prefix := range synchronizer.objConfig.Prefixes {
		oa := []string{}

		// NEW
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		objectCh := synchronizer.s3Client.Client.ListObjects(ctx, synchronizer.bucketName, minio.ListObjectsOptions{Prefix: prefix, Recursive: true})

		for object := range objectCh {
			if object.Err != nil {
				log.Error(object.Err)
				return object.Err
			}
			// fmt.Println(object)
			oa = append(oa, object.Key)
		}

		log.Infof("%s:%s object count: %d\n", synchronizer.bucketName, prefix, len(oa))
		bar := progressbar.Default(int64(len(oa)))
		for _, item := range oa {

			panic("not implemented, make sure pipeload takes proper bytes")
			_, err := synchronizer.PipeLoad([]byte{}, item)
			if err != nil {
				log.Error(err)
			}
			err = bar.Add(1)
			if err != nil {
				log.Error(err)
			}
			// log.Println(string(s)) // get "s" on pipeload and send to a log file
		}
	}

	return nil
}

// PipeCopy writes a new object based on an prefix, this function assumes the objects are valid when concatenated
// v1:  viper config object
// mc:  minio client pointer
// name:  name of the NEW object
// bucket:  source bucket  (and target bucket)
// prefix:  source prefix
// destprefix:   destination prefix
// sf: boolean to declare if single file or not.   If so, skip skolimization since JSON-LD library output is enough
func (synchronizer *SynchronizerClient) PipeCopy(name, prefix, destprefix string) error {
	log.Printf("PipeCopy with name: %s   bucket: %s  prefix: %s", name, synchronizer.bucketName, prefix)

	pr, pw := io.Pipe()     // TeeReader of use?
	lwg := sync.WaitGroup{} // work group for the pipe writes...
	lwg.Add(2)

	// params for list objects calls
	doneCh := make(chan struct{}) // , N) Create a done channel to control 'ListObjectsV2' go routine.
	defer close(doneCh)           // Indicate to our routine to exit cleanly upon return.
	isRecursive := true

	//log.Printf("Bulkfile name: %s_nq", name)

	go func() {
		defer lwg.Done()
		defer func(pw *io.PipeWriter) {
			err := pw.Close()
			if err != nil {
				log.Error(err)
			}
		}(pw)

		// Set and use a "single file flag" to bypass skolimaization since if it is a single file
		// the JSON-LD to RDF will correctly map blank nodes.
		// NOTE:  with a background context we can't get the len(channel) so we have to iterate it.
		// This is fast, but it means we have to do the ListObjects twice
		clen := 0
		sf := false
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		lenCh := synchronizer.s3Client.Client.ListObjects(ctx, synchronizer.bucketName, minio.ListObjectsOptions{Prefix: prefix, Recursive: isRecursive})
		for range lenCh {
			clen = clen + 1
		}
		if clen == 1 {
			sf = true
		}
		log.Printf("\nChannel/object length: %d\n", clen)
		log.Printf("Single file mode set: %t", sf)

		objectCh := synchronizer.s3Client.Client.ListObjects(context.Background(), synchronizer.bucketName, minio.ListObjectsOptions{Prefix: prefix, Recursive: isRecursive})

		// for object := range mc.ListObjects(context.Background(), bucket, minio.ListObjectsOptions{Prefix: prefix, Recursive: isRecursive}, doneCh) {
		for object := range objectCh {
			fo, err := synchronizer.s3Client.Client.GetObject(context.Background(), synchronizer.bucketName, object.Key, minio.GetObjectOptions{})
			if err != nil {
				fmt.Println(err)
			}

			var b bytes.Buffer
			bw := bufio.NewWriter(&b)

			_, err = io.Copy(bw, fo)
			if err != nil {
				log.Println(err)
			}

			jsonldString := b.String()

			nq := ""
			//log.Println("Calling JSONLDtoNQ")
			if strings.HasSuffix(object.Key, ".nq") {
				nq = jsonldString
			} else {
				nq, err = common.JsonldToNQ(jsonldString)
				if err != nil {
					log.Println(err)
					return
				}
			}

			var snq string

			if sf {
				snq = nq //  just pass through the RDF without trying to Skolemize since we ar a single fil
			} else {
				snq, err = common.Skolemization(nq, object.Key)
				if err != nil {
					return
				}
			}

			// 1) get graph URI
			ctx, err := common.MakeURN(object.Key)
			if err != nil {
				return
			}
			// 2) convert NT to NQ
			csnq, err := common.NtToNq(snq, ctx)
			if err != nil {
				return
			}

			_, err = pw.Write([]byte(csnq))
			if err != nil {
				return
			}
		}
	}()

	// go function to write to minio from pipe
	go func() {
		defer lwg.Done()
		_, err := synchronizer.s3Client.Client.PutObject(context.Background(), synchronizer.bucketName, fmt.Sprintf("%s/%s", destprefix, name), pr, -1, minio.PutObjectOptions{})
		//_, err := mc.PutObject(context.Background(), bucket, fmt.Sprintf("%s/%s", prefix, name), pr, -1, minio.PutObjectOptions{})
		if err != nil {
			log.Println(err)
			return
		}
	}()

	lwg.Wait() // wait for the pipe read writes to finish
	err := pw.Close()
	if err != nil {
		return err
	}
	err = pr.Close()
	if err != nil {
		return err
	}

	return nil
}

// // BulkAssembly collects the objects from a bucket to load
// func (m *MinioClientWrapper) BulkAssembly(v1 *viper.Viper) error {
// 	bucketName, _ := config.GetBucketName(v1)
// 	objCfg, _ := config.GetConfigForS3Objects(v1)
// 	pa := objCfg.Prefix

// 	var err error

// 	for p := range pa {
// 		name := fmt.Sprintf("%s_bulk.rdf", baseName(path.Base(pa[p])))
// 		err = graph.PipeCopy(v1, m.Client, name, bucketName, pa[p], "scratch") // have this function return the object name and path, easy to load and remove then
// 		//err = objects.MillerNG(name, bucketName, pa[p], mc) // have this function return the object name and path, easy to load and remove then
// 		if err != nil {
// 			return err
// 		}
// 	}

// 	for p := range pa {
// 		name := fmt.Sprintf("%s_bulk.rdf", baseName(path.Base(pa[p])))
// 		_, err := m.BulkLoad(v1, bucketName, fmt.Sprintf("/scratch/%s", name))
// 		if err != nil {
// 			log.Println(err)
// 		}
// 	}

// 	// TODO  remove the temporary object?
// 	for p := range pa {
// 		//name := fmt.Sprintf("%s_bulk.rdf", pa[p])
// 		name := fmt.Sprintf("%s_bulk.rdf", baseName(path.Base(pa[p])))
// 		opts := minio.RemoveObjectOptions{}
// 		err = m.Client.RemoveObject(context.Background(), bucketName, fmt.Sprintf("%s/%s", pa[p], name), opts)
// 		if err != nil {
// 			fmt.Println(err)
// 			return err
// 		}
// 	}

// 	return err
// }

func (synchronizer *SynchronizerClient) BulkRelease(v1 *viper.Viper) error {
	log.Println("Release:BulkAssembly")
	var err error

	for _, prefix := range synchronizer.objConfig.Prefixes {
		sp := strings.Split(prefix, "/")
		srcname := strings.Join(sp[1:], "__")
		spj := strings.Join(sp, "__")

		// Here we will either make this a _release.nq or a _prov.nq based on the source string.
		// TODO, should I look at the specific place in the path I expect this?
		// It is an exact match, so it should not be an issue
		name_latest := ""
		if contains(sp, "summoned") {
			name_latest = fmt.Sprintf("%s_release.nq", baseName(path.Base(srcname))) // ex: counties0_release.nq
		} else if contains(sp, "prov") {
			name_latest = fmt.Sprintf("%s_prov.nq", baseName(path.Base(srcname))) // ex: counties0_prov.nq
		} else if contains(sp, "orgs") {
			name_latest = "organizations.nq" // ex: counties0_org.nq
			fmt.Println(synchronizer.bucketName)
			fmt.Println(name_latest)
			err = synchronizer.PipeCopy(name_latest, "orgs", "graphs/latest") // have this function return the object name and path, easy to load and remove then
			if err != nil {
				return err
			}
			return err // just fully return from the function, no need for archive copies of the org graph
		} else {
			return errors.New("Unable to form a release graph name.  Path does not hold on of; summoned, prov or org")
		}

		// Make a release graph that will be stored in graphs/latest as {provider}_release.nq
		err = synchronizer.PipeCopy(name_latest, prefix, "graphs/latest") // have this function return the object name and path, easy to load and remove then
		if err != nil {
			return err
		}

		// Copy the "latest" graph just made to archive with a date
		// This means the graph in latests is a duplicate of the most recently dated version in archive/{provider}
		const layout = "2006-01-02-15-04-05"
		t := time.Now()
		// TODO  review the issue of archive and latest being hard coded.
		name := fmt.Sprintf("%s/%s/%s_%s_release.nq", "graphs/archive", srcname, baseName(path.Base(spj)), t.Format(layout))
		latest_fullpath := fmt.Sprintf("%s/%s", "graphs/latest", name_latest)
		err = synchronizer.s3Client.Copy(synchronizer.bucketName, latest_fullpath, synchronizer.bucketName, strings.Replace(name, "latest", "archive", 1))
	}

	return err
}

// Loads a stored release graph into the graph database
func (synchronizer *SynchronizerClient) BulkLoad(item string) error {

	b, _, err := synchronizer.s3Client.GetS3Bytes(synchronizer.bucketName, item)
	if err != nil {
		return err
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
		return err // Assume return. since on this error things are not good?
	}
	url := fmt.Sprintf("%s?graph=%s", synchronizer.graphClient.SparqlConf.Endpoint, g)

	// check if JSON-LD and convert to RDF
	if strings.Contains(item, ".jsonld") {
		nb, err := common.JsonldToNQ(string(b))
		if err != nil {
			return err
		}
		b = []byte(nb)
	}

	req, err := http.NewRequest(synchronizer.graphClient.SparqlConf.EndpointMethod, url, bytes.NewReader(b))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", synchronizer.graphClient.SparqlConf.ContentType) // needs to be x-nquads for blaze, n-quads for jena and graphdb
	req.Header.Set("User-Agent", "EarthCube_DataBot/1.0")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return err
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
		return err
	}

	// report
	log.Println(string(body))
	log.Printf("success: %s : %d  : %s\n", item, len(b), synchronizer.graphClient.SparqlConf.Endpoint)

	return err
}
