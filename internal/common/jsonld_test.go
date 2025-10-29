// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package common

import (
	"encoding/json"
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
		ctxMaps := map[string]string{
			"https://schema.org/": "./assets/schemaorg-current-https.jsonld",
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

func TestSelfieExample(t *testing.T) {
	// https://opengeospatial.github.io/SELFIE/usgs/huc/huc12obs/070900020601
	jsonld := `    {
  "@context": [
    "https://opengeospatial.github.io/ELFIE/contexts/elfie-2/elf-index.jsonld",
    "https://opengeospatial.github.io/ELFIE/contexts/elfie-2/hy_features.jsonld"
  ],
  "@id": "https://geoconnex.us/SELFIE/usgs/huc/huc12obs/070900020601",
  "@type": "https://www.opengis.net/def/appschema/hy_features/hyf/HY_Catchment",
  "name": "Waunakee Marsh-Sixmile Creek",
  "description": "USGS Watershed Boundary Dataset Twelve Digit Hydrologic Unit Code Watershed",
  "catchmentRealization": [
    {
      "@id": "https://geoconnex.us/SELFIE/usgs/nhdplusflowline/huc12obs/070900020601",
      "@type": "https://www.opengis.net/def/appschema/hy_features/hyf/HY_HydrographicNetwork"
    },
    {
      "@id": "https://geoconnex.us/SELFIE/usgs/hucboundary/huc12obs/070900020601",
      "@type": "https://www.opengis.net/def/appschema/hy_features/hyf/HY_CatchmentDivide"
    },
    {
      "@id": "https://geoconnex.us/SELFIE/usgs/hydrometricnetwork/huc12obs/070900020601",
      "@type": "https://www.opengis.net/def/appschema/hy_features/hyf/HY_HydrometricNetwork"
    }
  ]
}`

	ctxMaps := map[string]string{
		"https://schema.org/": "./assets/schemaorg-current-https.jsonld",
	}

	processor, options, err := NewJsonldProcessor(true, ctxMaps)

	require.NoError(t, err)
	nq, err := JsonldToNQ(jsonld, processor, options)
	require.NoError(t, err)
	require.NotEmpty(t, nq)

}

func TestAddJsonldContextToEmptyJsonld(t *testing.T) {
	var jsonld = make(map[string]any)
	_, err := AddKeyToJsonLDContext(jsonld, "hyf", "https://www.opengis.net/def/schema/hy_features/hyf/")
	require.Error(t, err)

	jsonld["@context"] = map[string]any{"TEST": "https://www.opengis.net/def/schema/hy_features/hyf/"}
	_, err = AddKeyToJsonLDContext(jsonld, "hyf", "https://www.opengis.net/def/schema/hy_features/hyf/")
	require.NoError(t, err)

	jsonld["@context"] = map[string]string{"TEST": "https://www.opengis.net/def/schema/hy_features/hyf/"}
	newJsonld, err := AddKeyToJsonLDContext(jsonld, "hyf", "https://www.opengis.net/def/schema/hy_features/hyf/")
	require.NoError(t, err)
	require.Equal(t, newJsonld["@context"].(map[string]string)["hyf"], "https://www.opengis.net/def/schema/hy_features/hyf/")
}

func TestAddJsonldContextToPartialJsonld(t *testing.T) {
	var jsonld = `{
		"@context": {
			"TEST": "https://www.w3.org/ns/ldp#"
		},
		"foo": "bar"
	}`
	var serializedJson map[string]any
	err := json.Unmarshal([]byte(jsonld), &serializedJson)
	require.NoError(t, err)

	newJsonld, err := AddKeyToJsonLDContext(serializedJson, "hyf", "https://www.opengis.net/def/schema/hy_features/hyf/")
	require.NoError(t, err)
	require.Equal(t, newJsonld["@context"].(map[string]any)["hyf"], "https://www.opengis.net/def/schema/hy_features/hyf/")
	require.Equal(t, newJsonld["foo"], "bar")
}

func TestAddJsonldContextToJsonldFile(t *testing.T) {
	file, err := os.Open("testdata/mainstem1.jsonld")
	require.NoError(t, err)

	var jsonld map[string]any
	err = json.NewDecoder(file).Decode(&jsonld)
	require.NoError(t, err)

	resultJsonld, err := AddKeyToJsonLDContext(jsonld, "TEST", "https://www.opengis.net/def/schema/hy_features/hyf/")
	require.NoError(t, err)

	context, ok := resultJsonld["@context"].([]any)
	require.True(t, ok)

	require.Equal(t, context[2].(map[string]string)["TEST"], "https://www.opengis.net/def/schema/hy_features/hyf/")
}

func TestCatchBadValueForContext(t *testing.T) {
	var jsonld = make(map[string]any)
	jsonld["@context"] = 0xDEADBEEF
	_, err := AddKeyToJsonLDContext(jsonld, "hyf", "https://www.opengis.net/def/schema/hy_features/hyf/")
	require.Error(t, err)
}
