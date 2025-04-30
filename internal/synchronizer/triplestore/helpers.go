// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package triplestore

import (
	"fmt"
	"nabu/internal/common"
	"strings"
)

/*
Create a sparql query used for inserting a batch of named graphs and
removing them if they already exist

Resulting queries will be in the form of:

	DROP SILENT GRAPH <%s>;
	DROP SILENT GRAPH <%s>;
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
func createBatchedUpsertQuery(graphs []common.NamedGraph) string {

	// Create a strings.Builder to efficiently build the query string
	// and reduce memory allocations.
	var queryBuilder strings.Builder

	// run all drops first; chance this may speed things up rather than
	// alternating between drop and insert
	for _, graph := range graphs {
		queryBuilder.WriteString(fmt.Sprintf("DROP SILENT GRAPH <%s>;\n", graph.GraphURI))
	}

	queryBuilder.WriteString("INSERT DATA {\n")
	for _, graph := range graphs {
		queryBuilder.WriteString(fmt.Sprintf("  GRAPH <%s> {\n    %s\n  }\n", graph.GraphURI, graph.Triples))
	}
	queryBuilder.WriteString("};")

	return queryBuilder.String()
}
