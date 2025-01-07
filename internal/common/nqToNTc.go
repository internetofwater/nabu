package common

import (
	"bytes"
	"errors"
	"strings"

	log "github.com/sirupsen/logrus"

	"github.com/knakk/rdf"
)

// NQtoNTCtx converts nquads to ntriples plus a context (graph) string
func NQToNTCtx(inquads string) (string, string, error) {
	// loop on tr and make a set of triples
	ntr := []rdf.Triple{}
	graphName := ""

	dec := rdf.NewQuadDecoder(strings.NewReader(inquads), rdf.NQuads)
	tr, err := dec.DecodeAll()
	if err != nil {
		log.Printf("Error decoding triples: %v\n", err)
		return "", graphName, err
	}

	// check we have triples
	if len(tr) < 1 {
		return "", graphName, errors.New("no triple")
	}

	for i := range tr {
		ntr = append(ntr, tr[i].Triple)
	}

	// Assume context of first triple is context of all triples  (again, a bit of a hack,
	// but likely valid as a single JSON-LD datagraph level).  This may be problematic for a "stitegraphs" where several
	// datagraph are represented in a single large JSON-LD via some collection concept.  There it is possible someone might
	// use the quad.  However, for most cases the quad is not important to us, it's local provenance, so we would still replace
	// it with our provenance (context)
	ctx := tr[0].Ctx
	graphName = ctx.String()

	// TODO output
	outtriples := ""
	buf := bytes.NewBufferString(outtriples)
	enc := rdf.NewTripleEncoder(buf, rdf.NTriples)
	err = enc.EncodeAll(ntr)
	if err != nil {
		log.Printf("Error encoding triples: %v\n", err)
	}
	enc.Close()

	tb := bytes.NewBuffer([]byte(""))
	for k := range ntr {
		tb.WriteString(ntr[k].Serialize(rdf.NTriples))
	}

	return tb.String(), graphName, err
}
