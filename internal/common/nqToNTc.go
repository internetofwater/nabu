package common

import (
	"bytes"
	"errors"
	"strings"

	log "github.com/sirupsen/logrus"

	"github.com/knakk/rdf"
)

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
