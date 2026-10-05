// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package common

import (
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tggo/goRDFlib/jsonld"
)

func TestMakeUrn(t *testing.T) {
	t.Run("prefix with no slashes fails", func(t *testing.T) {
		const shortPrefixWithNoSlashes = "test"
		_, err := MakeURN(shortPrefixWithNoSlashes)
		require.Error(t, err)
	})

	t.Run("prefix with 3 parts preserves all three and doesnt change order", func(t *testing.T) {
		result, err := MakeURN("test1/test2/test3")
		require.NoError(t, err)
		require.Equal(t, "urn:iow:test1:test2:test3", result)
	})

	t.Run("prefix with 4 parts preserves all four", func(t *testing.T) {
		result, err := MakeURN("test1/test2/test3/test4")
		require.NoError(t, err)
		require.Equal(t, "urn:iow:test1:test2:test3:test4", result)
	})
	t.Run("prefix with two slashes fails", func(t *testing.T) {
		const prefixWithTwoSlashes = "test1//test2"
		_, err := MakeURN(prefixWithTwoSlashes)
		require.Error(t, err)
	})
}

func TestJsonldToNquads(t *testing.T) {
	processor, options, err := NewJsonldProcessor(false)
	require.NoError(t, err)

	t.Run("invalid json returns a syntax error", func(t *testing.T) {
		_, err := JsonldToNquads([]byte("{"), "urn:test:graph", processor, options)
		var syntaxErr *json.SyntaxError
		require.ErrorAs(t, err, &syntaxErr)
	})

	t.Run("document with no triples is an error", func(t *testing.T) {
		_, err := JsonldToNquads([]byte(`{}`), "urn:test:graph", processor, options)
		require.Error(t, err)
	})

	t.Run("triples without blank nodes are unchanged and put in the graph", func(t *testing.T) {
		const doc = `{"@id": "https://example.com/1", "https://schema.org/name": "a \"quoted\" name"}`
		output, err := JsonldToNquads([]byte(doc), "urn:test:graph", processor, options)
		require.NoError(t, err)
		require.Equal(t, `<https://example.com/1> <https://schema.org/name> "a \"quoted\" name" <urn:test:graph> .`+"\n", output)
	})

	t.Run("blank nodes are replaced with an IRI that does not depend on the order of the document", func(t *testing.T) {
		const doc = `{"@id": "https://example.com/1", "https://schema.org/geo": {"https://schema.org/latitude": 1, "https://schema.org/longitude": 2}}`
		const reordered = `{"https://schema.org/geo": {"https://schema.org/longitude": 2, "https://schema.org/latitude": 1}, "@id": "https://example.com/1"}`
		output, err := JsonldToNquads([]byte(doc), "urn:test:graph", processor, options)
		require.NoError(t, err)
		require.NotContains(t, output, "_:")
		require.Contains(t, output, "<"+skolemNamespace)
		reorderedOutput, err := JsonldToNquads([]byte(reordered), "urn:test:graph", processor, options)
		require.NoError(t, err)
		require.ElementsMatch(t, strings.Split(output, "\n"), strings.Split(reorderedOutput, "\n"))
	})
}

func TestE2ESkolemizeJsonld(t *testing.T) {
	processor, options, err := NewJsonldProcessor(true)
	require.NoError(t, err)
	loader := options.DocumentLoader
	require.IsType(t, &jsonld.CachingDocumentLoader{}, loader)
	require.NotNil(t, processor)

	testJsonld, err := os.ReadFile("testdata/gage_jsonld.jsonld")
	require.NoError(t, err)
	skolemized, err := JsonldToNquads(testJsonld, "urn:test:graph", processor, options)
	require.NoError(t, err)
	require.NotEmpty(t, skolemized)
	// find a line with schema.org/longitude
	lines := strings.Split(skolemized, "\n")
	var longitudeLine string
	var latitudeLine string
	for _, line := range lines {
		if strings.Contains(line, "schema.org/longitude") {
			longitudeLine = line
		}
		if strings.Contains(line, "schema.org/latitude") {
			latitudeLine = line
		}
	}
	require.NotEmpty(t, longitudeLine)
	require.NotEmpty(t, latitudeLine)
	require.NotContains(t, longitudeLine, "_:")
	require.NotContains(t, latitudeLine, "_:")

	// lat/long contains E since the canonical representation uses scientific notation
	require.Contains(t, longitudeLine, "-1.091283306E2")
	require.Contains(t, latitudeLine, "3.712195E1")

	// wkt line
	var wktLine string
	for _, line := range lines {
		if strings.Contains(line, "POINT") {
			wktLine = line
		}
	}
	require.NotEmpty(t, wktLine)
	require.NotContains(t, wktLine, "_:")

	require.Contains(t, wktLine, "POINT (-109.1283306 37.12195)", "The WKT representation should be the same data as the lat/long values")
}
