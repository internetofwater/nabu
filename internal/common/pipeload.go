package common

import (
	"bufio"
	"mime"
	"path/filepath"
	"strings"

	"nabu/pkg/config"

	log "github.com/sirupsen/logrus"

	"github.com/minio/minio-go/v7"
	"github.com/spf13/viper"
)

// PipeLoad reads from an object and loads directly into a triplestore
func PipeLoad(v1 *viper.Viper, mc *minio.Client, bucket, object, spql string) ([]byte, error) {
	// build our quad/graph from the object path
	//log.Info("Loading %s \n", object)

	//s2c := strings.Replace(object, "/", ":", -1)
	g, err := MakeURN(object)
	if err != nil {
		log.Errorf("gets3Bytes %v\n", err)
		// should this just return. since on this error things are not good
	}

	// TODO WARNING this needs to be addressed
	// Turn checking off while testing other parts of Nabu
	//c, err := IsGraph(spql, g)
	//if err != nil {
	//log.Println(err)
	//}
	//if c {
	//return nil, nil // our graph is loaded already..
	//}

	b, _, err := GetS3Bytes(mc, bucket, object)
	if err != nil {
		log.Errorf("gets3Bytes %v\n", err)
		// should this just return. Do we have an object?
	}

	// TODO, use the mimetype or suffix in general to select the path to load    or overload from the config file?
	// check the object string
	mt := mime.TypeByExtension(filepath.Ext(object))
	//log.Printf("Object: %s reads as mimetype: %s", object, mt) // application/ld+json
	nt := ""

	// if strings.Contains(object, ".jsonld") { // TODO explore why this hack is needed and the mimetype for JSON-LD is not returned
	if strings.Compare(mt, "application/ld+json") == 0 {
		//log.Info("Convert JSON-LD file to nq")
		nt, err = JsonldToNQ(string(b))
		if err != nil {
			log.Errorf("JSONLDToNQ err: %s", err)
		}
	} else {
		nt, _, err = NQToNTCtx(string(b))
		if err != nil {
			log.Errorf("nqToNTCtx err: %s", err)
		}
	}

	// drop any graph we are going to load..  we assume we are doing those due to an update...
	_, err = Drop(v1, g)
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

	//sprql, _ := config.GetSparqlConfig(v1)
	ep := v1.GetString("flags.endpoint")
	sprql, err := config.GetEndpoint(v1, ep, "bulk")
	if err != nil {
		log.Error(err)
	}

	scanner := bufio.NewScanner(strings.NewReader(nt))
	lc := 0
	sg := []string{}
	for scanner.Scan() {
		lc = lc + 1
		sg = append(sg, scanner.Text())
		if lc == 10000 { // use line count, since byte len might break inside a triple statement..   it's an OK proxy
			log.Tracef("Subgraph of %d lines", len(sg))
			// TODO..  upload what we have here, modify the call code to upload these sections
			_, err = Insert(g, strings.Join(sg, "\n"), spql, sprql.Username, sprql.Password, sprql.Authenticate) // convert []string to strings joined with new line to form a RDF NT set
			if err != nil {
				log.Errorf("Insert err: %s", err)
			}
			sg = nil // clear the array
			lc = 0   // reset the counter
		}
	}
	if lc > 0 {
		log.Tracef("Subgraph (out of scanner) of %d lines", len(sg))
		_, err = Insert(g, strings.Join(sg, "\n"), spql, sprql.Username, sprql.Password, sprql.Authenticate) // convert []string to strings joined with new line to form a RDF NT set
	}

	return []byte("remove me"), err
}