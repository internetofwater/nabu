// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package triplestore

import (
	"nabu/internal/common"
	"strings"
	"testing"

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
		GraphURI: "http://example.com/graph",
		Triples:  "<http://example.com/subject> <http://example.com/predicate> <http://example.com/object> .",
	}

	query := createBatchedUpsertQuery([]common.NamedGraph{graph})
	expected := `DROP SILENT GRAPH <http://example.com/graph>; 
				 INSERT DATA { GRAPH <http://example.com/graph> { 
				      <http://example.com/subject> <http://example.com/predicate> <http://example.com/object> . 
			     }};`

	expected, query = stripWhitespace(expected, query)
	require.Equal(t, expected, query)

	graphs := []common.NamedGraph{
		{
			GraphURI: "http://example.com/graph1",
			Triples:  "<http://example.com/subject1> <http://example.com/predicate1> <http://example.com/object1> .",
		},
		{
			GraphURI: "http://example.com/graph2",
			Triples:  "<http://example.com/subject2> <http://example.com/predicate2> <http://example.com/object2> .",
		},
	}

	query = createBatchedUpsertQuery(graphs)
	expected = `DROP SILENT GRAPH <http://example.com/graph1>; 
				DROP SILENT GRAPH <http://example.com/graph2>; 
				INSERT DATA { 
					GRAPH <http://example.com/graph1> { 
					      <http://example.com/subject1> <http://example.com/predicate1> <http://example.com/object1> .
						} 
			         GRAPH <http://example.com/graph2> { 
					       <http://example.com/subject2> <http://example.com/predicate2> <http://example.com/object2> . 
						}
				};`
	expected, query = stripWhitespace(expected, query)
	require.Equal(t, expected, query)
}
