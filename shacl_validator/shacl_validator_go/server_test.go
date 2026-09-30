// Copyright 2026 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package shacl_validator

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/internetofwater/nabu/shacl_validator/shapes"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestServer(t *testing.T) *httptest.Server {
	t.Helper()
	validator, err := NewGeoconnexShaclValidator()
	require.NoError(t, err)
	server := httptest.NewServer(NewHttpHandler(&validator))
	t.Cleanup(server.Close)
	return server
}

func postValidate(t *testing.T, server *httptest.Server, body string) (int, map[string]any) {
	t.Helper()
	resp, err := http.Post(server.URL+"/validate", "application/json", strings.NewReader(body))
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	var decoded map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&decoded))
	return resp.StatusCode, decoded
}

func readTestdata(t *testing.T, path ...string) string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(append([]string{"..", "testdata"}, path...)...))
	require.NoError(t, err)
	return string(data)
}

func TestHttpValidateValid(t *testing.T) {
	server := newTestServer(t)
	status, body := postValidate(t, server, readTestdata(t, "valid", "rise.jsonld"))
	require.Equal(t, http.StatusOK, status)
	assert.Equal(t, true, body["valid"])
	assert.Equal(t, "Validation Report\nConforms: True\n", body["message"])
}

func TestHttpValidateInvalid(t *testing.T) {
	server := newTestServer(t)
	status, body := postValidate(t, server, readTestdata(t, "invalid", "no_geo.jsonld"))
	require.Equal(t, http.StatusOK, status)
	assert.Equal(t, false, body["valid"])
	message := body["message"].(string)
	assert.Contains(t, message, "Conforms: False")
	assert.Contains(t, message, "Places must include geometry in WKT format")
}

// the python server test sends the jsonld as a json encoded string
// so make sure that is handled the same as sending the object directly
func TestHttpValidateJsonEncodedString(t *testing.T) {
	server := newTestServer(t)
	jsonld := `{"@context": {"ex": "http://example.org/"}, "@type": "ex:Person", "ex:name": "Alice"}`
	encoded, err := json.Marshal(jsonld)
	require.NoError(t, err)

	status, body := postValidate(t, server, string(encoded))
	require.Equal(t, http.StatusOK, status)
	assert.Equal(t, false, body["valid"])
	assert.Contains(t, body["message"], MissingPlaceOrDatasetTypeMessage)

	valid, err := json.Marshal(readTestdata(t, "valid", "rise.jsonld"))
	require.NoError(t, err)
	status, body = postValidate(t, server, string(valid))
	require.Equal(t, http.StatusOK, status)
	assert.Equal(t, true, body["valid"])
}

func TestHttpValidateEmpty(t *testing.T) {
	server := newTestServer(t)
	for _, empty := range []string{"{}", "[]", `""`, "null"} {
		status, body := postValidate(t, server, empty)
		assert.Equal(t, http.StatusBadRequest, status, empty)
		assert.Equal(t, "Missing JSON data in request", body["detail"], empty)
	}
}

func TestHttpValidateMalformed(t *testing.T) {
	server := newTestServer(t)
	status, body := postValidate(t, server, "not json")
	assert.Equal(t, http.StatusInternalServerError, status)
	assert.NotEmpty(t, body["detail"])
}

func TestHttpValidateConcurrent(t *testing.T) {
	server := newTestServer(t)
	valid := readTestdata(t, "valid", "rise.jsonld")
	invalid := readTestdata(t, "invalid", "no_geo.jsonld")

	var wg sync.WaitGroup
	for i := range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			data, expected := valid, true
			if i%2 == 0 {
				data, expected = invalid, false
			}
			status, body := postValidate(t, server, data)
			assert.Equal(t, http.StatusOK, status)
			assert.Equal(t, expected, body["valid"])
		}()
	}
	wg.Wait()
}

func TestHttpShape(t *testing.T) {
	server := newTestServer(t)
	resp, err := http.Get(server.URL + "/shape")
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Contains(t, resp.Header.Get("Content-Type"), "text/turtle")
	assert.Equal(t, "*", resp.Header.Get("Access-Control-Allow-Origin"))
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Equal(t, shapes.GeoconnexTTL, string(body))
}

func TestHttpCorsPreflight(t *testing.T) {
	server := newTestServer(t)
	req, err := http.NewRequest(http.MethodOptions, server.URL+"/validate", nil)
	require.NoError(t, err)
	req.Header.Set("Origin", "https://example.com")
	req.Header.Set("Access-Control-Request-Method", "POST")
	req.Header.Set("Access-Control-Request-Headers", "content-type")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "*", resp.Header.Get("Access-Control-Allow-Origin"))
	assert.Contains(t, resp.Header.Get("Access-Control-Allow-Methods"), "POST")
	assert.Equal(t, "content-type", resp.Header.Get("Access-Control-Allow-Headers"))
}

func TestHttpWrongMethod(t *testing.T) {
	server := newTestServer(t)
	resp, err := http.Get(server.URL + "/validate")
	require.NoError(t, err)
	_ = resp.Body.Close()
	assert.Equal(t, http.StatusMethodNotAllowed, resp.StatusCode)
}
