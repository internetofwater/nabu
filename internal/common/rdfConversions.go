// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package common

import (
	"fmt"
	"strings"

	log "github.com/sirupsen/logrus"
	rdf "github.com/tggo/goRDFlib"
	nq "github.com/tggo/goRDFlib/nq"
	nt "github.com/tggo/goRDFlib/nt"
)

// Convert a string of N-Triples to N-Quads
func NtToNq(ntData, graphURN string) (string, error) {
	// Create a graph with the desired named graph identifier.
	// This is the equivalent of setting rdf.Context(iri) in knakk/rdf.
	graphIRI, err := rdf.NewURIRef(graphURN)
	if err != nil {
		return "", fmt.Errorf("invalid graph URN %q: %w", graphURN, err)
	}
	graph := rdf.NewGraph(rdf.WithIdentifier(graphIRI))

	// Parse N-Triples into the named graph.
	if err := nt.Parse(graph, strings.NewReader(ntData), nt.WithErrorHandler(
		func(lineNum int, line string, err error) (fixedLine string, retry bool) {
			log.Errorf("Failed converting triples to quads on line %d with data %s: %v", lineNum, line, err)
			return "", false // skip this line
		},
	)); err != nil {
		return "", fmt.Errorf("parsing N-Triples: %w", err)
	}

	// Serialize as N-Quads — the graph identifier is automatically
	// included as the 4th element of each quad.
	var buf strings.Builder
	if err := nq.Serialize(graph, &buf); err != nil {
		return "", fmt.Errorf("serializing N-Quads: %w", err)
	}
	return buf.String(), nil
}
