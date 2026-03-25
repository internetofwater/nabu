// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package common

import (
	"strings"

	"github.com/knakk/rdf"
	log "github.com/sirupsen/logrus"
)

// Convert a string of N-Triples to N-Quads
func NtToNq(nt, graphURN string) (string, error) {
	dec := rdf.NewTripleDecoder(strings.NewReader(nt), rdf.NTriples)
	triples, err := dec.DecodeAll()
	if err != nil {
		log.Errorf("Error decoding triples: %v\n", err)
		return "", err
	}

	allQuads := make([]string, len(triples))
	for i, triple := range triples {
		quad, err := makeQuad(triple, graphURN)
		if err != nil {
			return "", err
		}
		allQuads[i] = quad
	}
	return strings.Join(allQuads, ""), err
}

// makeQuad I pulled this from my ObjectEngine code in case I needed to
// use in the ntToNQ() function to add a context to each triple in turn.
// It may not be needed/used in this code
func makeQuad(t rdf.Triple, c string) (string, error) {
	newctx, err := rdf.NewIRI(c)
	if err != nil {
		return "", err
	}

	quad := rdf.Quad{
		Triple: t,
		Ctx:    rdf.Context(newctx),
	}

	return quad.Serialize(rdf.NQuads), nil
}
