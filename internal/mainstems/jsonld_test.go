// Copyright 2026 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package mainstems

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAddMainstemToJsonld(t *testing.T) {
	const mainstem = "https://geoconnex.us/ref/mainstems/1"

	t.Run("adds the mainstem without changing the context or other values", func(t *testing.T) {
		doc := `{"@context": "https://schema.org/", "@id": "https://example.com/1", "name": "a & b", "count": 12345678901234567890}`
		enriched, err := AddMainstemToJsonld([]byte(doc), mainstem)
		require.NoError(t, err)
		require.Contains(t, string(enriched), `"@context":"https://schema.org/"`)
		require.Contains(t, string(enriched), `"name":"a & b"`)
		require.Contains(t, string(enriched), `"count":12345678901234567890`, "large numbers should not lose precision")

		var parsed map[string]any
		require.NoError(t, json.Unmarshal(enriched, &parsed))
		position := parsed[referencedPositionIRI].([]any)[0].(map[string]any)[hyfNamespace+"HY_IndirectPosition"].(map[string]any)
		require.Equal(t, mainstem, position[hyfNamespace+"linearElement"].(map[string]any)["@id"])
	})

	t.Run("documents that already reference a position are unchanged", func(t *testing.T) {
		doc := `{"@id": "https://example.com/1", "hyf:referencedPosition": []}`
		enriched, err := AddMainstemToJsonld([]byte(doc), mainstem)
		require.NoError(t, err)
		require.Equal(t, doc, string(enriched))
	})

	t.Run("documents without a single top level node cannot be enriched", func(t *testing.T) {
		_, err := AddMainstemToJsonld([]byte(`[{"@id": "https://example.com/1"}]`), mainstem)
		require.ErrorIs(t, err, ErrCannotAddMainstem)
		_, err = AddMainstemToJsonld([]byte(`{"@graph": [{"@id": "https://example.com/1"}]}`), mainstem)
		require.ErrorIs(t, err, ErrCannotAddMainstem)
	})
}
