// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package common

import (
	"bytes"
	"errors"
	"strings"

	"github.com/knakk/rdf"
	log "github.com/sirupsen/logrus"
)

// Representation of a triple with a graph name
// that can be inserted into a triplestore
type NamedGraph struct {
	GraphURI URN
	Triples  string
}

// Convert a string of N-Triples to N-Quads
func NtToNq(nt, ctx string) (string, error) {
	dec := rdf.NewTripleDecoder(strings.NewReader(nt), rdf.NTriples)
	triples, err := dec.DecodeAll()
	if err != nil {
		log.Errorf("Error decoding triples: %v\n", err)
		return "", err
	}

	allQuads := make([]string, len(triples))
	for i, triple := range triples {
		quad, err := makeQuad(triple, ctx)
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

// Converts nquads to ntriples plus a context (graph) string
func QuadsToTripleWithCtx(nquads string) (NamedGraph, error) {

	dec := rdf.NewQuadDecoder(strings.NewReader(nquads), rdf.NQuads)
	decodedQuads, err := dec.DecodeAll()
	if err != nil {
		log.Errorf("Error decoding quads: %v\n", err)
		return NamedGraph{}, err
	}

	// check we have triples
	if len(decodedQuads) < 1 {
		return NamedGraph{}, errors.New("no triples to generate; quads were empty")
	}

	// loop on tr and make a set of triples
	triples := make([]rdf.Triple, len(decodedQuads))
	for i, t := range decodedQuads {
		triples[i] = t.Triple
	}

	// Assume context of first triple is context of all triples  (again, a bit of a hack,
	// but likely valid as a single JSON-LD datagraph level).  This may be problematic for a "stitegraphs" where several
	// datagraph are represented in a single large JSON-LD via some collection concept.  There it is possible someone might
	// use the quad.  However, for most cases the quad is not important to us, it's local provenance, so we would still replace
	// it with our provenance (context)
	context_graph := decodedQuads[0].Ctx
	graphName := context_graph.String()

	outtriples := ""
	buf := bytes.NewBufferString(outtriples)
	encoder := rdf.NewTripleEncoder(buf, rdf.NTriples)
	err = encoder.EncodeAll(triples)
	if err != nil {
		log.Errorf("Error encoding triples: %v\n", err)
		return NamedGraph{}, err
	}
	defer func() { _ = encoder.Close() }()

	tb := bytes.NewBuffer([]byte(""))
	for k := range triples {
		tb.WriteString(triples[k].Serialize(rdf.NTriples))
	}

	return NamedGraph{GraphURI: graphName, Triples: tb.String()}, err
}
