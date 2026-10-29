// Copyright 2026 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package s2cells

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"

	"github.com/golang/geo/s2"
)

const (
	// the KnowWhereGraph S2 cell ontology; see docs/s2_vocab.ttl
	kwgOntologyNamespace = "http://stko-kwg.geog.ucsb.edu/lod/ontology/"
	// the namespace KnowWhereGraph uses for the IRIs of S2 cells, so
	// that the cells link to the same resources in KnowWhereGraph
	kwgResourceNamespace = "http://stko-kwg.geog.ucsb.edu/lod/resource/"
	// the CGS spatiotemporal ontology, which defines the spatial relations of
	// features to cells; see https://cgs-earth.github.io/ontologies/spatiotemporal/
	SpatiotemporalNamespace = "https://w3id.org/cgs/ontology/core/spatiotemporal/"
	// properties are written as full IRIs so that the document's
	// @context does not need to be changed to add them
	SpatialRelationIRI = SpatiotemporalNamespace + "spatialRelation"
	SfContainsIRI      = SpatiotemporalNamespace + "sfContains"
	SfWithinIRI        = SpatiotemporalNamespace + "sfWithin"
	SfOverlapsIRI      = SpatiotemporalNamespace + "sfOverlaps"
	SfEqualsIRI        = SpatiotemporalNamespace + "sfEquals"
	SfCrossesIRI       = SpatiotemporalNamespace + "sfCrosses"
	cellIDIRI          = kwgOntologyNamespace + "cellID"
	xsdIntegerIRI      = "http://www.w3.org/2001/XMLSchema#integer"
)

// ErrCannotAddCells is returned when the shape of a JSON-LD document
// does not have a single top level node that S2 cells can be added to
var ErrCannotAddCells = errors.New("JSON-LD document does not have a single top level node to add S2 cells to")

// CellIRI returns the KnowWhereGraph IRI of an S2 cell, i.e.
// http://stko-kwg.geog.ucsb.edu/lod/resource/s2.level11.<cell id>
func CellIRI(cell s2.CellID) string {
	return fmt.Sprintf("%ss2.level%d.%d", kwgResourceNamespace, cell.Level(), uint64(cell))
}

// The JSON-LD node describing a single S2 cell
func cellNode(cell s2.CellID) map[string]any {
	return map[string]any{
		"@id":   CellIRI(cell),
		"@type": fmt.Sprintf("%sS2Cell_Level%d", kwgOntologyNamespace, cell.Level()),
		// cell ids are unsigned 64 bit integers which cannot be represented exactly by JSON
		// numbers in most parsers, so the integer is written as a typed string literal
		cellIDIRI: map[string]any{
			"@value": strconv.FormatUint(uint64(cell), 10),
			"@type":  xsdIntegerIRI,
		},
	}
}

// AddCellsToJsonld returns the JSON-LD document linked to each of the given S2 cells
// with its spatial relation. Any values the document already has for those relations are kept
func AddCellsToJsonld(jsonld []byte, relations []CellRelation) ([]byte, error) {
	if len(relations) == 0 {
		return jsonld, nil
	}

	decoder := json.NewDecoder(bytes.NewReader(jsonld))
	// keep numbers as they were written instead of converting them to float64
	decoder.UseNumber()
	var doc map[string]any
	if err := decoder.Decode(&doc); err != nil {
		var typeErr *json.UnmarshalTypeError
		if errors.As(err, &typeErr) {
			return nil, fmt.Errorf("%w: %v", ErrCannotAddCells, err)
		}
		return nil, err
	}
	if _, isGraph := doc["@graph"]; isGraph && doc["@id"] == nil {
		return nil, fmt.Errorf("%w: documents with a @graph are not supported", ErrCannotAddCells)
	}

	cellsByRelation := map[string][]any{}
	for _, relation := range relations {
		cellsByRelation[relation.Relation] = append(cellsByRelation[relation.Relation], cellNode(relation.Cell))
	}
	for relation, cells := range cellsByRelation {
		values := []any{}
		switch existing := doc[relation].(type) {
		case nil:
		case []any:
			values = append(values, existing...)
		default:
			values = append(values, existing)
		}
		doc[relation] = append(values, cells...)
	}

	var out bytes.Buffer
	encoder := json.NewEncoder(&out)
	// urls commonly contain & which should be left as is
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(doc); err != nil {
		return nil, err
	}
	return bytes.TrimSuffix(out.Bytes(), []byte("\n")), nil
}
