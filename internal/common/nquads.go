// Copyright 2026 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package common

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"

	"github.com/piprate/json-gold/ld"
	log "github.com/sirupsen/logrus"
)

// the namespace of the IRIs that blank nodes are replaced with
const skolemNamespace = "https://docs.geoconnex.us/nqhash/"

// JsonldToNquads converts a JSON-LD document to N-Quads with every triple in the named graph graphURN.
// Blank nodes are skolemized so that the output of many documents can be loaded together, and triples
// that cannot be represented in N-Quads, such as those with an IRI containing a space, are dropped.
// The conversion works on the RDF dataset directly so the document is only serialized once
func JsonldToNquads(jsonld []byte, graphURN string, processor *ld.JsonLdProcessor, options *ld.JsonLdOptions) (string, error) {
	var document any
	if err := json.Unmarshal(jsonld, &document); err != nil {
		return "", err
	}
	datasetOptions := options.Copy()
	// without a format json-gold returns the dataset instead of serializing it
	datasetOptions.Format = ""
	result, err := processor.ToRDF(document, datasetOptions)
	if err != nil {
		return "", err
	}
	dataset, ok := result.(*ld.RDFDataset)
	if !ok {
		return "", fmt.Errorf("expected an RDF dataset from JSON-LD conversion but got %T", result)
	}

	quads := datasetQuads(dataset)
	if len(quads) == 0 {
		return "", fmt.Errorf("JSON-LD conversion produced no triples")
	}

	skolemIRIs := skolemIRIsForBlankNodes(quads)
	skolemize := func(node ld.Node) ld.Node {
		if blankNode, ok := node.(ld.BlankNode); ok {
			return skolemIRIs[blankNode.Attribute]
		}
		return node
	}

	graphQuads := make([]*ld.Quad, 0, len(quads))
	for _, quad := range quads {
		if !representableInNquads(quad) {
			log.Errorf("Skipping triple that cannot be represented in N-Quads in graph %s: %s %s %s", graphURN, quad.Subject.GetValue(), quad.Predicate.GetValue(), quad.Object.GetValue())
			continue
		}
		graphQuads = append(graphQuads, &ld.Quad{
			Subject:   skolemize(quad.Subject),
			Predicate: quad.Predicate,
			Object:    skolemize(quad.Object),
		})
	}

	output := ld.NewRDFDataset()
	output.Graphs = map[string][]*ld.Quad{graphURN: graphQuads}
	var buf strings.Builder
	if err := (&ld.NQuadRDFSerializer{}).SerializeTo(&buf, output); err != nil {
		return "", err
	}
	return buf.String(), nil
}

// Every quad in the dataset; the document gets a single named graph so
// any graphs within the document itself are merged into it
func datasetQuads(dataset *ld.RDFDataset) []*ld.Quad {
	var quads []*ld.Quad
	// sorted so the output is deterministic
	for _, graphName := range slices.Sorted(maps.Keys(dataset.Graphs)) {
		quads = append(quads, dataset.Graphs[graphName]...)
	}
	return quads
}

// Map each blank node to an IRI derived from a hash of the triples it appears in, so the
// same data gets the same IRI regardless of the order of the triples in the document
// reference: https://www.w3.org/TR/rdf11-concepts/#dfn-skolem-iri
func skolemIRIsForBlankNodes(quads []*ld.Quad) map[string]ld.Node {
	blankNodeToTriples := map[string][]string{}
	for _, quad := range quads {
		if subject, ok := quad.Subject.(ld.BlankNode); ok {
			blankNodeToTriples[subject.Attribute] = append(blankNodeToTriples[subject.Attribute], "S "+termKey(quad.Predicate)+" "+termKey(quad.Object))
		}
		if object, ok := quad.Object.(ld.BlankNode); ok {
			blankNodeToTriples[object.Attribute] = append(blankNodeToTriples[object.Attribute], "O "+termKey(quad.Subject)+" "+termKey(quad.Predicate))
		}
	}

	skolemIRIs := make(map[string]ld.Node, len(blankNodeToTriples))
	for blankNode, triples := range blankNodeToTriples {
		slices.Sort(triples)
		hash := sha256.Sum256([]byte(strings.Join(triples, "\n")))
		skolemIRIs[blankNode] = ld.NewIRI(skolemNamespace + hex.EncodeToString(hash[:]))
	}
	return skolemIRIs
}

// An unambiguous string for a term that is used as the input to the skolem hash
func termKey(node ld.Node) string {
	switch n := node.(type) {
	case ld.IRI:
		return "<" + n.Value + ">"
	case ld.Literal:
		return strconv.Quote(n.Value) + "^^<" + n.Datatype + ">@" + n.Language
	default:
		return node.GetValue()
	}
}

// Whether every term in the quad can be written as N-Quads. json-gold already drops quads with
// an invalid http IRI, but other IRIs, such as an unexpanded compact IRI with a space in it,
// are kept and must be checked for the characters that N-Quads does not allow in an IRI
func representableInNquads(quad *ld.Quad) bool {
	for _, node := range []ld.Node{quad.Subject, quad.Predicate, quad.Object} {
		switch n := node.(type) {
		case ld.IRI:
			if !validNquadsIRI(n.Value) {
				return false
			}
		case ld.Literal:
			if !validNquadsIRI(n.Datatype) {
				return false
			}
		}
	}
	return true
}

// https://www.w3.org/TR/n-quads/#grammar-production-IRIREF
func validNquadsIRI(iri string) bool {
	return !strings.ContainsFunc(iri, func(r rune) bool {
		return r <= 0x20 || strings.ContainsRune("<>\"{}|^`\\", r)
	})
}
