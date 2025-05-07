// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package crawl

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOrgsJsonld(t *testing.T) {
	template, err := newOrgsJsonLD("https://example.com", "example")
	require.NoError(t, err)

	require.JSONEq(t, `{
		"@context": {
			"@vocab": "https://schema.org/"
		},
		"@id": "https://gleaner.io/id/org/example",
		"@type": "Organization",
		"url": "https://example.com",
		"name": "example",
		 "identifier": {
			"@type": "PropertyValue",
			"@id": "https://gleaner.io/genid/geoconnex",
			"propertyID": "https://registry.identifiers.org/registry/doi",
			"url": "https://gleaner.io/genid/geoconnex",
			"description": "Persistent identifier for this organization"
		}
	}`, template)

}

func TestOrgsNq(t *testing.T) {
	nq, err := NewOrgsNq("https://example.com", "example")
	require.NoError(t, err)
	const line = "<https://gleaner.io/genid/geoconnex> <https://schema.org/description> \"Persistent identifier for this organization\""
	require.Contains(t, nq, line)
}
