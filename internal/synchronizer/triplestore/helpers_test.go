// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package triplestore

import (
	"strings"
	"testing"

	"github.com/internetofwater/nabu/internal/common"

	"github.com/stretchr/testify/require"
)

// check if the two strings are the same, ignoring tabs, newlines and spaces
// we use this helper since the query strings might have different formatting
// like tabs or newlines but still both be valid sparql
func stripWhitespace(expected string, actual string) (string, string) {
	actual = strings.ReplaceAll(actual, "\t", "")
	actual = strings.ReplaceAll(actual, "\n", "")
	actual = strings.ReplaceAll(actual, " ", "")
	expected = strings.ReplaceAll(expected, "\t", "")
	expected = strings.ReplaceAll(expected, "\n", "")
	expected = strings.ReplaceAll(expected, " ", "")
	return expected, actual
}

func TestCreateBatchedUpsertQuery(t *testing.T) {

	graph := common.NamedGraph{
		GraphURI: "urn://example.com/graph",
		Triples:  "<urn://example.com/subject> <urn://example.com/predicate> <urn://example.com/object> .",
	}

	query, err := createBatchedUpsertQuery([]common.NamedGraph{graph})
	require.NoError(t, err)
	expected := `INSERT DATA { GRAPH <urn://example.com/graph> { 
				      <urn://example.com/subject> <urn://example.com/predicate> <urn://example.com/object> . 
			     }};`

	expected, query = stripWhitespace(expected, query)
	require.Equal(t, expected, query)

	graphs := []common.NamedGraph{
		{
			GraphURI: "urn://example.com/graph1",
			Triples:  "<urn://example.com/subject1> <urn://example.com/predicate1> <urn://example.com/object1> .",
		},
		{
			GraphURI: "urn://example.com/graph2",
			Triples:  "<urn://example.com/subject2> <urn://example.com/predicate2> <urn://example.com/object2> .",
		},
	}

	query, err = createBatchedUpsertQuery(graphs)
	require.NoError(t, err)
	expected = `INSERT DATA { 
					GRAPH <urn://example.com/graph1> { 
					      <urn://example.com/subject1> <urn://example.com/predicate1> <urn://example.com/object1> .
						} 
			         GRAPH <urn://example.com/graph2> { 
					       <urn://example.com/subject2> <urn://example.com/predicate2> <urn://example.com/object2> . 
						}
				};`
	expected, query = stripWhitespace(expected, query)
	require.Equal(t, expected, query)
}

func TestGraphWithoutUrnFailsBatch(t *testing.T) {
	_, err := createBatchedUpsertQuery([]common.NamedGraph{
		{
			GraphURI: "test",
			Triples:  "<summoned/test> <http://example.com/predicate> <http://example.com/object> .",
		},
	})
	require.Error(t, err)
	require.ErrorContains(t, err, "is not a valid URN")
}
