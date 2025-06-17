// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package triplestores

import (
	"fmt"
	"strings"

	"github.com/internetofwater/nabu/internal/common"
)

/*
Create a sparql query used for inserting a batch of named graphs and
removing them if they already exist

Resulting queries will be in the form of:

	INSERT DATA {
		GRAPH <%s> {
			%s
		}
	};
	INSERT DATA {
		GRAPH <%s> {
			%s
		}
	};
*/
func createBatchedUpsertQuery(graphs []common.NamedGraph) (string, error) {

	// Create a strings.Builder to efficiently build the query string
	// and reduce memory allocations.
	var queryBuilder strings.Builder

	queryBuilder.WriteString("INSERT DATA {\n")
	for _, graph := range graphs {
		if !strings.Contains(graph.GraphURI, "urn") {
			return "", fmt.Errorf("graph %s is not a valid URN; did you pass in a s3prefix instead of an URN?", graph.GraphURI)
		}
		queryBuilder.WriteString(fmt.Sprintf("  GRAPH <%s> {\n    %s\n  }\n", graph.GraphURI, graph.Triples))
	}
	queryBuilder.WriteString("};")

	result := queryBuilder.String()
	return result, nil
}
