// Copyright 2026 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package parquettable

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/golang/geo/s2"
	"github.com/peterstace/simplefeatures/geom"
	log "github.com/sirupsen/logrus"
)

const (
	schemaNamespace     = "https://schema.org/"
	geosparqlNamespace  = "http://www.opengis.net/ont/geosparql#"
	schemaNameIRI       = schemaNamespace + "name"
	schemaDescIRI       = schemaNamespace + "description"
	hasGeometryIRI      = geosparqlNamespace + "hasGeometry"
	asWktIRI            = geosparqlNamespace + "asWKT"
	insecureSchemaOrgNS = "http://schema.org/"
)

// A single harvested JSON-LD document along with the
// tabular fields that were extracted from it
type Feature struct {
	// the @id of the top level node in the document
	ID string
	// the value of schema:name
	Name string
	// the value of schema:description
	Description string
	// the geosparql WKT geometry converted to WKB
	Geometry []byte
	// the JSON-LD document; this is the harvested source document
	// along with any enrichments, such as the associated mainstem
	JSONLD []byte
	// the url in the sitemap that the document was harvested from
	URL string
	// the uri of the mainstem associated with the geometry, i.e.
	// https://geoconnex.us/ref/mainstems/1; this is not part of the
	// JSON-LD and is only set for sitemaps that request mainstem associations
	MainstemURI string
	// the S2 cell id of the centroid of the geometry at the leaf level as a signed
	// integer, matching BigQuery's S2_CELLIDFROMPOINT; 0 if there is no geometry
	S2CellID int64
}

// FeatureFromJsonld extracts the tabular fields from a JSON-LD document. It returns
// an error only if the document is not valid JSON; missing fields or an
// unparsable geometry are left empty since they are not required to be present
func FeatureFromJsonld(jsonld []byte, url string) (Feature, error) {
	var doc any
	if err := json.Unmarshal(jsonld, &doc); err != nil {
		return Feature{}, fmt.Errorf("JSON-LD harvested from %s is not valid JSON: %w", url, err)
	}

	feature := Feature{JSONLD: jsonld, URL: url}

	node, ctx := topLevelNode(doc)
	if node == nil {
		return feature, nil
	}

	for key, value := range node {
		switch ctx.expand(key) {
		case "@id":
			if id, ok := value.(string); ok {
				feature.ID = ctx.expandIRI(id)
			}
		case schemaNameIRI:
			feature.Name = literalString(value, ctx)
		case schemaDescIRI:
			feature.Description = literalString(value, ctx)
		case hasGeometryIRI:
			wkt := wktFromGeometryNode(value, ctx)
			if wkt == "" {
				continue
			}
			wkb, err := wktToWkb(wkt)
			if err != nil {
				log.Warnf("could not parse WKT geometry for %s; leaving its geometry empty: %v", url, err)
				continue
			}
			feature.Geometry = wkb
			feature.S2CellID = s2CellIDForWkb(wkb)
		}
	}
	return feature, nil
}

// Return the node that describes the document along with its context;
// for documents that are an array or a @graph the first node is used
func topLevelNode(doc any) (map[string]any, *jsonldContext) {
	ctx := newJsonldContext()
	switch d := doc.(type) {
	case map[string]any:
		ctx = ctx.withContext(d["@context"])
		if graph, ok := d["@graph"]; ok && d["@id"] == nil {
			node, nested := topLevelNode(graph)
			if node != nil {
				return node, ctx.merge(nested)
			}
		}
		return d, ctx
	case []any:
		for _, item := range d {
			if node, nodeCtx := topLevelNode(item); node != nil {
				return node, nodeCtx
			}
		}
	}
	return nil, ctx
}

// The subset of a JSON-LD context needed to resolve the
// small number of terms that are extracted into columns
type jsonldContext struct {
	// maps a term or prefix to an IRI or keyword
	terms map[string]string
	vocab string
}

func newJsonldContext() *jsonldContext {
	return &jsonldContext{terms: map[string]string{}}
}

// Return a copy of the context with the definitions from raw added to it
func (c *jsonldContext) withContext(raw any) *jsonldContext {
	if raw == nil {
		return c
	}
	next := &jsonldContext{terms: make(map[string]string, len(c.terms)), vocab: c.vocab}
	for k, v := range c.terms {
		next.terms[k] = v
	}
	next.add(raw)
	return next
}

func (c *jsonldContext) merge(other *jsonldContext) *jsonldContext {
	merged := c.withContext(map[string]any{})
	for k, v := range other.terms {
		merged.terms[k] = v
	}
	if other.vocab != "" {
		merged.vocab = other.vocab
	}
	return merged
}

func (c *jsonldContext) add(raw any) {
	switch ctx := raw.(type) {
	case string:
		// remote contexts are not fetched; the schema.org context is
		// common enough that it is treated as setting the vocabulary
		if strings.Contains(ctx, "schema.org") {
			c.vocab = schemaNamespace
		}
	case []any:
		for _, item := range ctx {
			c.add(item)
		}
	case map[string]any:
		for key, value := range ctx {
			switch v := value.(type) {
			case string:
				if key == "@vocab" {
					c.vocab = normalizeIRI(v)
				} else {
					c.terms[key] = v
				}
			case map[string]any:
				if id, ok := v["@id"].(string); ok {
					c.terms[key] = id
				}
			}
		}
	}
}

// Expand a property key to an absolute IRI or keyword
func (c *jsonldContext) expand(key string) string {
	if strings.HasPrefix(key, "@") {
		return key
	}
	if mapped, ok := c.terms[key]; ok {
		if strings.HasPrefix(mapped, "@") {
			return mapped
		}
		return c.expandIRI(mapped)
	}
	if strings.Contains(key, ":") {
		return c.expandIRI(key)
	}
	if c.vocab != "" {
		return c.vocab + key
	}
	return key
}

// Expand a compact IRI such as schema:name to an absolute IRI
func (c *jsonldContext) expandIRI(iri string) string {
	prefix, suffix, found := strings.Cut(iri, ":")
	if found && !strings.HasPrefix(suffix, "//") {
		if namespace, ok := c.terms[prefix]; ok && !strings.HasPrefix(namespace, "@") {
			return normalizeIRI(namespace + suffix)
		}
	}
	return normalizeIRI(iri)
}

// schema.org is commonly referenced with both http and https
func normalizeIRI(iri string) string {
	if rest, ok := strings.CutPrefix(iri, insecureSchemaOrgNS); ok {
		return schemaNamespace + rest
	}
	return iri
}

// Get a plain string from a JSON-LD value which may be a string,
// a value object, or an array of either
func literalString(value any, ctx *jsonldContext) string {
	switch v := value.(type) {
	case string:
		return v
	case map[string]any:
		for key, inner := range v {
			if ctx.expand(key) == "@value" {
				if s, ok := inner.(string); ok {
					return s
				}
			}
		}
	case []any:
		first := ""
		for _, item := range v {
			s := literalString(item, ctx)
			if s == "" {
				continue
			}
			// prefer english or untagged strings when there are several languages
			if obj, ok := item.(map[string]any); !ok || obj["@language"] == nil || strings.HasPrefix(fmt.Sprint(obj["@language"]), "en") {
				return s
			}
			if first == "" {
				first = s
			}
		}
		return first
	}
	return ""
}

// Get the WKT literal from the object of a geosparql:hasGeometry property
func wktFromGeometryNode(value any, ctx *jsonldContext) string {
	switch v := value.(type) {
	case map[string]any:
		nodeCtx := ctx.withContext(v["@context"])
		for key, inner := range v {
			if nodeCtx.expand(key) == asWktIRI {
				return literalString(inner, nodeCtx)
			}
		}
	case []any:
		for _, item := range v {
			if wkt := wktFromGeometryNode(item, ctx); wkt != "" {
				return wkt
			}
		}
	}
	return ""
}

// Convert a geosparql WKT literal to WKB. Geosparql allows the WKT to be
// prefixed with the IRI of its CRS which is removed before parsing
func wktToWkb(wkt string) ([]byte, error) {
	wkt = strings.TrimSpace(wkt)
	if strings.HasPrefix(wkt, "<") {
		if end := strings.Index(wkt, ">"); end != -1 {
			wkt = strings.TrimSpace(wkt[end+1:])
		}
	}
	geometry, err := geom.UnmarshalWKT(wkt)
	if err != nil {
		// some upstream geometries are not strictly valid, i.e. self intersecting
		// polygons; these are still useful so we keep them as is
		var noValidateErr error
		geometry, noValidateErr = geom.UnmarshalWKT(wkt, geom.NoValidate{})
		if noValidateErr != nil {
			return nil, err
		}
	}
	return geometry.AsBinary(), nil
}

// Get the leaf S2 cell id of the centroid of a WKB geometry with longitude/latitude
// coordinates; 0, which is never a valid cell id, is returned if there is no such cell
func s2CellIDForWkb(wkb []byte) int64 {
	geometry, err := geom.UnmarshalWKB(wkb, geom.NoValidate{})
	if err != nil {
		return 0
	}
	centroid, ok := geometry.Centroid().XY()
	if !ok {
		return 0
	}
	// coordinates outside of these ranges are not longitude/latitude
	if centroid.X < -180 || centroid.X > 180 || centroid.Y < -90 || centroid.Y > 90 {
		return 0
	}
	cellID := s2.CellIDFromLatLng(s2.LatLngFromDegrees(centroid.Y, centroid.X))
	return int64(cellID)
}
