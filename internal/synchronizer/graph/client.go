package graph

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"nabu/internal/common"
	"net/http"
	"net/url"
	"strings"

	"nabu/pkg/config"

	"github.com/minio/minio-go/v7"
	"github.com/schollz/progressbar/v3"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"github.com/tidwall/gjson"
)

type GraphDbMethods interface {
	Insert() string
	ClearAllGraphs() error
	GraphExists(graph string) (bool, error)
	DropGraph(graph string) error
}

type GraphDbClient struct {
	password string
	username string
	Endpoint string
	GraphDbMethods
}

// Create a new client struct to connect to the triplestore
func NewGraphDbClient(v1 *viper.Viper) (*GraphDbClient, error) {
	conf, err := config.ReadSparqlConfig(v1)
	if err != nil {
		return nil, err
	}
	return &GraphDbClient{
		Endpoint: conf.Endpoint,
		password: conf.Password,
		username: conf.Username,
	}, nil

}

// Insert data into the triplestore
func (graphClient *GraphDbClient) Insert(graph, data string, auth bool) (string, error) {

	p := "INSERT DATA { "
	pab := []byte(p)
	gab := []byte(fmt.Sprintf(" graph <%s>  { ", graph))
	u := " } }"
	uab := []byte(u)
	pab = append(pab, gab...)
	pab = append(pab, []byte(data)...)
	pab = append(pab, uab...)

	req, err := http.NewRequest("POST", graphClient.Endpoint, bytes.NewBuffer(pab)) // PUT for any of the servers?
	if err != nil {
		log.Error(err)
		return "", err
	}

	req.Header.Set("Content-Type", "application/sparql-update") // graphdb  blaze and jena  alt might be application/sparql-results+xml
	req.Header.Set("Accept", "application/x-trig")              // graphdb

	if auth {
		req.SetBasicAuth(graphClient.username, graphClient.password)
	}

	client := &http.Client{}

	resp, err := client.Do(req)
	if err != nil {
		log.Error(err)
		return "", err
	}
	defer resp.Body.Close()

	log.Tracef("response Status: %s", resp.Status)
	log.Tracef("response Headers: %s", resp.Header)
	// TODO just string check for 200 or 204 rather than try to match
	if resp.Status != "200 OK" && resp.Status != "204 No Content" && resp.Status != "204 " {
		log.Infof("response Status: %s", resp.Status)
		log.Infof("response Headers: %s", resp.Header)
	}

	body, err := io.ReadAll(resp.Body)
	// log.Println(string(body))
	if err != nil {
		log.Error("response Body:", string(body))
		log.Error("response Status:", resp.Status)
		log.Error("response Headers:", resp.Header)
	}

	return resp.Status, err

}

// remove a graph from the graph database
func (graphClient *GraphDbClient) DropGraph(graph string) ([]byte, error) {

	d := fmt.Sprintf("DROP GRAPH <%s> ", graph)
	pab := []byte(d)

	//req, err := http.NewRequest("POST", spql["endpoint"], bytes.NewBuffer(pab))
	req, err := http.NewRequest("POST", graphClient.Endpoint, bytes.NewBuffer(pab))
	if err != nil {
		log.Error(err)
	}
	req.Header.Set("Content-Type", "application/sparql-update")
	// req.Header.Set("Content-Type", "application/sparql-results+xml")

	httpClient := &http.Client{}
	resp, err := httpClient.Do(req)
	if err != nil {
		log.Error(err)
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Error("response Body:", string(body))
		log.Error("response Status:", resp.Status)
		log.Error("response Headers:", resp.Header)
	}

	log.Trace(string(body))

	return body, err
}

// Remove all graphs from the graph database
func (graphClient *GraphDbClient) ClearAllGraphs() error {
	d := "CLEAR ALL"

	pab := []byte(d)

	req, err := http.NewRequest("POST", graphClient.Endpoint, bytes.NewBuffer(pab))
	if err != nil {
		log.Error(err)
		return err
	}
	req.Header.Set("Content-Type", "application/sparql-update")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		log.Error(err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Error("response Body:", string(body))
		log.Error("response Status:", resp.Status)
		log.Error("response Headers:", resp.Header)
	}

	log.Trace(string(body))

	log.Infof("All graphs were cleared")

	return err
}

// holds results from the http query
type ask struct {
	Head    string `json:"head"`
	Boolean bool   `json:"boolean"`
}

// Check if a graph exists in the graph database
func (graphClient *GraphDbClient) GraphExists(graph string) (bool, error) {
	d := fmt.Sprintf("ASK WHERE { GRAPH <%s> { ?s ?p ?o } }", graph)

	pab := []byte("")
	params := url.Values{}
	params.Add("query", d)
	req, err := http.NewRequest("GET", fmt.Sprintf("%s?%s", graphClient.Endpoint, params.Encode()), bytes.NewBuffer(pab))
	if err != nil {
		return false, err
	}
	req.Header.Set("Accept", "application/sparql-results+json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return false, err
	}

	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Println(strings.Repeat("ERROR", 5))
		log.Println("response Status:", resp.Status)
		log.Println("response Headers:", resp.Header)
		log.Println("response Body:", string(body))
		return false, err
	}

	if string(body) == "Not Acceptable\n" {
		return false, nil
	}

	ask := ask{}
	err = json.Unmarshal(body, &ask)
	if err != nil {
		return false, err
	}

	return ask.Boolean, err
}

// Get rid of graphs in the triplestore that are not in the object store
func (graphClient *GraphDbClient) ListNamedGraphs(prefix string) ([]string, error) {
	log.Debug("Getting list of named graphs")

	var ga []string

	//bucketName, err := config.GetBucketName(v1)
	//if err != nil {
	//	log.Println(err)
	//	return ga, err
	//}

	gp, err := common.MakeURNPrefix(prefix)
	if err != nil {
		log.Println(err)
		return ga, err
	}

	//d := fmt.Sprintf("SELECT DISTINCT ?g WHERE {GRAPH ?g {?s ?p ?o} FILTER regex(str(?g), \"^%s\")}", gp)

	d := "SELECT DISTINCT ?g WHERE {GRAPH ?g {?s ?p ?o} }"

	log.Printf("Pattern: %s\n", gp)
	log.Printf("SPARQL: %s\n", d)
	//log.Printf("Accept: %s\n", spql.Accept)
	//log.Printf("URL: %s\n", spql.URL)

	pab := []byte("")
	params := url.Values{}
	params.Add("query", d)
	req, err := http.NewRequest("GET", fmt.Sprintf("%s?%s", graphClient.Endpoint, params.Encode()), bytes.NewBuffer(pab))
	if err != nil {
		log.Println(err)
	}

	// These headers
	req.Header.Set("Accept", "application/sparql-results+json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		log.Println(err)
	}

	defer func() {
		err := resp.Body.Close()
		if err != nil {
			log.Printf("Error closing response body: %v", err)
		}
	}()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Println(strings.Repeat("ERROR", 5))
		log.Println("response Status:", resp.Status)
		log.Println("response Headers:", resp.Header)
		log.Println("response Body:", string(body))
	}

	// debugging calls
	//fmt.Println("response Body:", string(body))
	//err = ioutil.WriteFile("body.txt", body, 0644)
	//if err != nil {
	//	fmt.Println("An error occurred:", err)
	//	return ga, err
	//}

	result := gjson.Get(string(body), "results.bindings.#.g.value")
	result.ForEach(func(key, value gjson.Result) bool {
		ga = append(ga, value.String())
		return true // keep iterating
	})

	var gaf []string
	for _, str := range ga {
		if strings.HasPrefix(str, gp) { // check if string has prefix
			gaf = append(gaf, str) // if yes, add it to newArray
		}
	}

	return gaf, nil
}