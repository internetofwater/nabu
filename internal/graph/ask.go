package graph

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	log "github.com/sirupsen/logrus"
)

// Ask holds results from SPARQL ASK
type Ask struct {
	Head    string `json:"head"`
	Boolean bool   `json:"boolean"`
}

// IsGraph return true is exists
func IsGraph(spql, g string) (bool, error) {
	d := fmt.Sprintf("ASK WHERE { GRAPH <%s> { ?s ?p ?o } }", g)

	pab := []byte("")
	params := url.Values{}
	params.Add("query", d)
	req, err := http.NewRequest("GET", fmt.Sprintf("%s?%s", spql, params.Encode()), bytes.NewBuffer(pab))
	if err != nil {
		log.Println(err)
	}
	req.Header.Set("Accept", "application/sparql-results+json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		log.Println(err)
	}

	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Println(strings.Repeat("ERROR", 5))
		log.Println("response Status:", resp.Status)
		log.Println("response Headers:", resp.Header)
		log.Println("response Body:", string(body))
	}

	ask := Ask{}
	err = json.Unmarshal(body, &ask)
	if err != nil {
		return false, err
	}

	return ask.Boolean, err
}
