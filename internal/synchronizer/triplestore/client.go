package triplestore

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"nabu/internal/common"
	"nabu/internal/custom_http_trace"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"nabu/pkg/config"

	log "github.com/sirupsen/logrus"
	"github.com/tidwall/gjson"
)

type TriplesAsText = string

// The set of methods that must be implemented by a triplestore to be used by nabu
type TriplestoreMethods interface {
	// Inserts data into a specified named graph.
	InsertWithNamedGraph(data TriplesAsText, graphURI string) string

	// ClearAllGraphs clears all graphs in the triplestore.
	ClearAllGraphs() error

	// Checks if a specified graph exists in the triplestore.
	GraphExists(graph string) (bool, error)

	// Removes a specified graph from the triplestore.
	DropGraph(graph string) error
}

type GraphDbClient struct {
	// Holds the configuration for how to interact with the sparql endpoint
	SparqlConf config.SparqlConfig
	// url to the host without specifying a repository
	BaseUrl string
	// url to the host specifying a repository
	BaseRepositoryUrl string
	// url to the host for issuing sparql commands
	BaseSparqlQueryUrl string
	// url to the host for the rest api base endpoint.
	// REST api metods are used for config and graphdb specific operations
	BaseRESTUrl string
	// methods that a triplestore must implement
	TriplestoreMethods
}

// Create a new client struct to connect to the triplestore
func NewGraphDbClient(config config.SparqlConfig) (*GraphDbClient, error) {

	return &GraphDbClient{
		SparqlConf:         config,
		BaseUrl:            config.Endpoint,
		BaseRepositoryUrl:  fmt.Sprintf("%s/repositories/%s", config.Endpoint, config.Repository),
		BaseRESTUrl:        fmt.Sprintf("%s/rest", config.Endpoint),
		BaseSparqlQueryUrl: fmt.Sprintf("%s/repositories/%s/statements", config.Endpoint, config.Repository),
	}, nil
}

func (graphClient *GraphDbClient) CreateRepositoryIfNotExists(ttlConfigPath string) error {
	// Open the TTL config file
	file, err := os.Open(ttlConfigPath)
	if err != nil {
		return fmt.Errorf("failed to open TTL config file: %w", err)
	}
	defer file.Close()

	// Create a buffer and multipart writer
	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)

	// Add the file part
	part, err := writer.CreateFormFile("config", filepath.Base(ttlConfigPath))
	if err != nil {
		return fmt.Errorf("failed to create form file: %w", err)
	}

	// Copy the file content into the form file part
	if _, err = io.Copy(part, file); err != nil {
		return fmt.Errorf("failed to copy file content: %w", err)
	}

	// Close the multipart writer to finalize the body
	if err = writer.Close(); err != nil {
		return fmt.Errorf("failed to close writer: %w", err)
	}

	// Create the HTTP request
	url := fmt.Sprintf("%s/repositories", graphClient.BaseRESTUrl)
	req, err := custom_http_trace.NewRequestWithContext("POST", url, body)
	if err != nil {
		return fmt.Errorf("failed to create HTTP request: %w", err)
	}
	req.Header.Set("Content-Type", writer.FormDataContentType())

	client := http.Client{}

	// Send the request
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("HTTP request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == 400 {
		bodyBytes, err := io.ReadAll(resp.Body)
		if err != nil {
			return fmt.Errorf("failed to read response body: %w", err)
		}
		bodyStr := string(bodyBytes)
		if strings.Contains(bodyStr, "already exists") {
			log.Warn("Repository already exists so skipping creation")
			return nil
		}
		return fmt.Errorf("failed to create repository, status: %d, response: %s", resp.StatusCode, bodyStr)
	} else if resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("failed to create repository, status: %d, response: %s", resp.StatusCode, string(body))
	}
	return nil
}

// Insert triples into the triplestore by listing them in the standard triple format and specifying an associated graph
func (graphClient *GraphDbClient) InsertWithNamedGraph(triples TriplesAsText, graphURI string) error {

	log.Debugf("Inserting data into graph: %s", graphURI)
	template := `
		INSERT DATA { 
			GRAPH <%s> { 
				%s
			} 
		}`
	fullReq := []byte(fmt.Sprintf(template, graphURI, triples))

	req, err := custom_http_trace.NewRequestWithContext("POST", graphClient.BaseSparqlQueryUrl, bytes.NewBuffer(fullReq)) // PUT for any of the servers?
	if err != nil {
		log.Error(err)
		return err
	}

	req.Header.Set("Content-Type", "application/sparql-update") // graphdb  blaze and jena  alt might be application/sparql-results+xml
	req.Header.Set("Accept", "application/x-trig")              // graphdb

	if graphClient.SparqlConf.Authenticate {
		req.SetBasicAuth(graphClient.SparqlConf.Username, graphClient.SparqlConf.Password)
	}

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		log.Error(err)
		return err
	}
	defer resp.Body.Close()

	// TODO just string check for 200 or 204 rather than try to match
	if resp.Status != "200 OK" && resp.Status != "204 No Content" && resp.Status != "204 " {
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return fmt.Errorf("failed reading response body; response Status: %s with error %s", resp.Status, err)
		}
		return fmt.Errorf("failed inserting data with named graph; response Status: %s with error %s after posting query %s", resp.Status, string(body), fullReq)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Error("response Body:", string(body))
		log.Error("response Status:", resp.Status)
		log.Error("response Headers:", resp.Header)
		return err
	}

	return nil

}

// Remove a graph entirely from the graph database
func (graphClient *GraphDbClient) DropGraph(graph string) error {

	d := fmt.Sprintf("DROP GRAPH <%s> ", graph)
	pab := []byte(d)

	req, err := custom_http_trace.NewRequestWithContext("POST", graphClient.BaseSparqlQueryUrl, bytes.NewBuffer(pab))
	if err != nil {
		log.Error(err)
		return err
	}
	req.Header.Set("Content-Type", "application/sparql-update")

	client := http.Client{}

	resp, err := client.Do(req)
	if err != nil {
		log.Error(err)
		return err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode != 204 {
		return fmt.Errorf("failed to drop graph, status: %d, response: %s", resp.StatusCode, string(body))
	}

	log.Trace(string(body))

	return nil
}

// Remove all triples from all graphs but keep the graphs themselves
func (graphClient *GraphDbClient) ClearAllGraphs() error {
	d := "CLEAR ALL"

	pab := []byte(d)

	req, err := custom_http_trace.NewRequestWithContext("POST", graphClient.BaseSparqlQueryUrl, bytes.NewBuffer(pab))
	if err != nil {
		log.Error(err)
		return err
	}
	req.Header.Set("Content-Type", "application/sparql-update")

	client := http.Client{}

	resp, err := client.Do(req)
	if err != nil || resp.StatusCode != 204 {
		log.Errorf("failed to clear graphs: response Status: %s with error %s", resp.Status, err)
		return err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Error("response Body:", string(body))
		log.Error("response Status:", resp.Status)
		log.Error("response Headers:", resp.Header)
		return err
	}

	log.Trace(string(body))
	log.Infof("All graphs were cleared successfully")
	return err
}

// Check if a graph exists in the graph database
func (graphClient *GraphDbClient) GraphExists(graphURN string) (bool, error) {

	// holds results from the sparql ASK query
	type ask struct {
		Head    map[string]interface{} `json:"head"`    // Map for flexible JSON object
		Boolean bool                   `json:"boolean"` // Boolean value
	}

	sparqlQuery := fmt.Sprintf("ASK WHERE { GRAPH <%s> { ?s ?p ?o } }", graphURN)

	pab := []byte("")
	params := url.Values{}
	params.Add("query", sparqlQuery)
	req, err := custom_http_trace.NewRequestWithContext("GET", fmt.Sprintf("%s?%s", graphClient.BaseRepositoryUrl, params.Encode()), bytes.NewBuffer(pab))
	if err != nil {
		return false, err
	}

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return false, err
	}

	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		return false, fmt.Errorf("failed checking if graph exists; response Status: %s with error %s after posting query %s", resp.Status, string(body), sparqlQuery)
	}

	if err != nil {
		log.Error(strings.Repeat("ERROR", 5))
		log.Error("response Status:", resp.Status)
		log.Error("response Headers:", resp.Header)
		log.Error("response Body:", string(body))
		return false, err
	}

	if string(body) == "Not Acceptable\n" {
		return false, nil
	}

	askResp := ask{}
	err = json.Unmarshal(body, &askResp)
	if err != nil {
		return false, err
	}

	return askResp.Boolean, err
}

// Get list of graphs in the triplestore
func (graphClient *GraphDbClient) NamedGraphsAssociatedWithS3Prefix(prefix string) ([]string, error) {
	log.Debug("Getting list of named graphs")

	gp, err := common.MakeURN(prefix)
	if err != nil {
		log.Error(err)
		return []string{}, err
	}

	query := "SELECT DISTINCT ?g WHERE {GRAPH ?g {?s ?p ?o} }"

	log.Printf("Pattern: %s\n", gp)
	log.Printf("SPARQL: %s\n", query)
	params := url.Values{}
	params.Add("query", query)
	req, err := custom_http_trace.NewRequestWithContext("GET", fmt.Sprintf("%s?%s", graphClient.BaseRepositoryUrl, params.Encode()), bytes.NewBuffer([]byte("")))
	if err != nil {
		log.Error(err)
		return []string{}, err
	}

	req.Header.Set("Accept", "application/sparql-results+json")

	client := &http.Client{}

	resp, err := client.Do(req)
	if err != nil {
		log.Error(err)
		return []string{}, err
	}

	defer func() {
		err := resp.Body.Close()
		if err != nil {
			log.Errorf("Error closing response body: %v", err)
		}
	}()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Error("response Status:", resp.Status)
		log.Error("response Headers:", resp.Header)
		log.Error("response Body:", string(body))
		return nil, err
	}

	var graphNames []string
	result := gjson.Get(string(body), "results.bindings.#.g.value")
	result.ForEach(func(key, value gjson.Result) bool {
		graphNames = append(graphNames, value.String())
		return true // keep iterating
	})

	var relevantGraphs []string
	for _, graph := range graphNames {
		if strings.HasPrefix(graph, gp) { // check if string has prefix
			relevantGraphs = append(relevantGraphs, graph) // if yes, add it to newArray
		}
	}

	return relevantGraphs, nil
}
