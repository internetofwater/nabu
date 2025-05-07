// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package common

import (
	"nabu/internal/config"
	"os"
	"testing"

	"github.com/piprate/json-gold/ld"
	"github.com/stretchr/testify/require"
)

func TestCreateNewProcessor(t *testing.T) {

	t.Run("empty config returns blank processor", func(t *testing.T) {
		_, _, err := NewJsonldProcessor(false, nil)
		require.NoError(t, err)
	})

	t.Run("use full config with caching", func(t *testing.T) {
		ctxMaps := []config.ContextMap{
			{
				Prefix: "https://schema.org/",
				File:   "./assets/schemaorg-current-https.jsonld",
			},
		}

		processor, options, err := NewJsonldProcessor(true, ctxMaps)
		require.NoError(t, err)
		loader := options.DocumentLoader
		require.IsType(t, &ld.CachingDocumentLoader{}, loader)
		require.NotNil(t, processor)

		const simpleJSONLDExample = `{
			"@context": "https://json-ld.org/contexts/person.jsonld",
			"@id": "http://dbpedia.org/resource/John_Lennon",
			"name": "John Lennon",
			"born": "1940-10-09",
			"spouse": "http://dbpedia.org/resource/Cynthia_Lennon"
			}`
		nq, err := JsonldToNQ(simpleJSONLDExample, processor, options)
		require.NoError(t, err)
		require.NotEmpty(t, nq)
		birthDateLine := `<http://dbpedia.org/resource/John_Lennon> <http://schema.org/birthDate> "1940-10-09"`
		require.Contains(t, nq, birthDateLine)

		// read in a file as a string
		data, err := os.ReadFile("testdata/BPMWQX-1084.jsonld")
		require.NoError(t, err)
		require.NoError(t, err)
		nq, err = JsonldToNQ(string(data), processor, options)
		require.NoError(t, err)
		require.NotEmpty(t, nq)
	})
}
