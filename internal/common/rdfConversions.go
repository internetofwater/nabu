package common

import (
	"bytes"
	"errors"
	"fmt"
	"log"
	"strings"

	"github.com/knakk/rdf"
)

// Convert a string of N-Triples to N-Quads
func NtToNq(nt, ctx string) (string, error) {
	dec := rdf.NewTripleDecoder(strings.NewReader(nt), rdf.NTriples)
	triples, err := dec.DecodeAll()
	if err != nil {
		log.Printf("Error decoding triples: %v\n", err)
		return "", err
	}

	var allQuads []string
	for _, triple := range triples {
		quad, err := makeQuad(triple, ctx)
		if err != nil {
			return "", err
		}
		allQuads = append(allQuads, quad)
	}
	return strings.Join(allQuads, ""), err
}

// makeQuad I pulled this from my ObjectEngine code in case I needed to
// use in the ntToNQ() function to add a context to each triple in turn.
// It may not be needed/used in this code
func makeQuad(t rdf.Triple, c string) (string, error) {
	newctx, err := rdf.NewIRI(c) // this should be  c
	if err != nil {
		return "", err
	}
	ctx := rdf.Context(newctx)

	quad := rdf.Quad{
		Triple: t,
		Ctx:    ctx,
	}

	buf := bytes.NewBufferString("")

	quads := quad.Serialize(rdf.NQuads)
	_, err = fmt.Fprintf(buf, "%s", quads)
	if err != nil {
		return "", err
	}

	return buf.String(), err
}

// NqToNTCtx  Converts quads to triples and return the graph name separately
func NqToNTCtx(inquads string) (string, string, error) {
	dec := rdf.NewQuadDecoder(strings.NewReader(inquads), rdf.NQuads)
	quads, err := dec.DecodeAll()
	if err != nil {
		log.Printf("Error decoding triples: %v\n", err)
		return "", "", err
	}

	// loop on tr and make a set of triples
	triples := []rdf.Triple{}
	for _, quad := range quads {
		triples = append(triples, quad.Triple)
	}

	// Assume context of first triple sis context of all triples
	// TODO..   this is stupid if not dangers, at least return []string of all the contexts
	// that were in the graph.
	ctx := quads[0].Ctx
	quadContext := ctx.String()

	outtriples := ""
	buf := bytes.NewBufferString(outtriples)
	enc := rdf.NewTripleEncoder(buf, rdf.NTriples)
	err = enc.EncodeAll(triples)
	if err != nil {
		log.Printf("Error encoding triples: %v\n", err)
		return "", "", err
	}
	err = enc.Close()
	if err != nil {
		return "", "", err
	}

	tb := bytes.NewBuffer([]byte(""))
	for _, triple := range triples {
		tb.WriteString(triple.Serialize(rdf.NTriples))
	}

	return tb.String(), quadContext, err
}

// Converts nquads to ntriples plus a context (graph) string
func QuadsToTripleWithCtx(nquads string) (string, string, error) {
	// loop on tr and make a set of triples
	triples := []rdf.Triple{}

	dec := rdf.NewQuadDecoder(strings.NewReader(nquads), rdf.NQuads)
	decodedQuads, err := dec.DecodeAll()
	if err != nil {
		log.Printf("Error decoding triples: %v\n", err)
		return "", "", err
	}

	// check we have triples
	if len(decodedQuads) < 1 {
		return "", "", errors.New("no triples to convert; quads were empty")
	}

	for _, t := range decodedQuads {
		triples = append(triples, t.Triple)
	}

	// Assume context of first triple is context of all triples  (again, a bit of a hack,
	// but likely valid as a single JSON-LD datagraph level).  This may be problematic for a "stitegraphs" where several
	// datagraph are represented in a single large JSON-LD via some collection concept.  There it is possible someone might
	// use the quad.  However, for most cases the quad is not important to us, it's local provenance, so we would still replace
	// it with our provenance (context)
	ctx := decodedQuads[0].Ctx
	graphName := ctx.String()

	outtriples := ""
	buf := bytes.NewBufferString(outtriples)
	encoder := rdf.NewTripleEncoder(buf, rdf.NTriples)
	err = encoder.EncodeAll(triples)
	if err != nil {
		log.Printf("Error encoding triples: %v\n", err)
	}
	encoder.Close()

	tb := bytes.NewBuffer([]byte(""))
	for k := range triples {
		tb.WriteString(triples[k].Serialize(rdf.NTriples))
	}

	return tb.String(), graphName, err
}
