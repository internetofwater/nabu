// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package common

import (
	"nabu/internal/common/projectpath"
	"nabu/internal/config"
	"path/filepath"
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
		configPath := filepath.Join(projectpath.Root, "config/iow")
		absPath, err := filepath.Abs(configPath)
		require.NoError(t, err)
		conf, err := config.ReadNabuConfig(absPath, "nabuconfig.yaml")
		require.NoError(t, err)
		processor, options, err := NewJsonldProcessor(conf.Context.Cache, conf.ContextMaps)
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

		birthDateLine := `<http://dbpedia.org/resource/John_Lennon> <http://schema.org/birthDate> "1940-10-09"`
		require.Contains(t, nq, birthDateLine)

	})
}
