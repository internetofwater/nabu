// Copyright 2026 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package s2cells

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/golang/geo/s2"
	"github.com/internetofwater/nabu/internal/common"
	"github.com/peterstace/simplefeatures/geom"
	"github.com/stretchr/testify/require"
)

func geometryFromWkt(t *testing.T, wkt string) geom.Geometry {
	t.Helper()
	g, err := geom.UnmarshalWKT(wkt, geom.NoValidate{})
	require.NoError(t, err)
	return g
}

func wkbFromWkt(t *testing.T, wkt string) []byte {
	return geometryFromWkt(t, wkt).AsBinary()
}

// expand a cell union so that it only contains cells of the target level
func expandToLevel(cells s2.CellUnion) []s2.CellID {
	expanded := []s2.CellID{}
	for _, cell := range cells {
		if cell.Level() == Level {
			expanded = append(expanded, cell)
			continue
		}
		for child := cell.ChildBeginAtLevel(Level); child != cell.ChildEndAtLevel(Level); child = child.Next() {
			expanded = append(expanded, child)
		}
	}
	slices.Sort(expanded)
	return expanded
}

// find every target level cell that intersects a region by testing each
// cell around its bounding rectangle, without condensing anything
func bruteForceCells(region s2.Region) []s2.CellID {
	candidates := (&s2.RegionCoverer{MinLevel: Level, MaxLevel: Level, MaxCells: 1 << 30}).Covering(region.RectBound())
	found := []s2.CellID{}
	for _, cell := range candidates {
		if region.IntersectsCell(s2.CellFromCellID(cell)) {
			found = append(found, cell)
		}
	}
	slices.Sort(found)
	return found
}

func TestCellsForPoint(t *testing.T) {
	cells, err := CellsForWkb(wkbFromWkt(t, "POINT(-111.68 40.44)"))
	require.NoError(t, err)
	require.Len(t, cells, 1)
	require.Equal(t, s2.CellIDFromLatLng(s2.LatLngFromDegrees(40.44, -111.68)).Parent(Level), cells[0])
}

func TestCellsForPolygonAreCondensed(t *testing.T) {
	// roughly 50km by 50km so it contains many whole level 10 cells
	const wkt = "POLYGON((-105.5 39.5, -104.9 39.5, -104.9 39.95, -105.5 39.95, -105.5 39.5))"
	cells, err := CellsForWkb(wkbFromWkt(t, wkt))
	require.NoError(t, err)
	require.True(t, cells.IsNormalized())

	coarser := 0
	for _, cell := range cells {
		require.LessOrEqual(t, cell.Level(), Level)
		if cell.Level() < Level {
			coarser++
		}
	}
	require.Positive(t, coarser, "cells inside the polygon should be condensed into their parents")

	region, err := polygonRegion(geometryFromWkt(t, wkt).MustAsPolygon())
	require.NoError(t, err)
	expected := bruteForceCells(region)
	require.Greater(t, len(expected), len(cells))
	require.Equal(t, expected, expandToLevel(cells), "the condensed cells should be exactly the level 11 cells that intersect the polygon")
}

func TestCellsForPolygonIgnoreRingOrientation(t *testing.T) {
	counterClockwise, err := CellsForWkb(wkbFromWkt(t, "POLYGON((0 0, 0.3 0, 0.3 0.3, 0 0.3, 0 0))"))
	require.NoError(t, err)
	clockwise, err := CellsForWkb(wkbFromWkt(t, "POLYGON((0 0, 0 0.3, 0.3 0.3, 0.3 0, 0 0))"))
	require.NoError(t, err)
	require.Equal(t, counterClockwise, clockwise)
	require.Less(t, len(clockwise), 200, "a clockwise ring should not be treated as covering the rest of the globe")
}

func TestCellsForPolygonWithHole(t *testing.T) {
	withoutHole, err := CellsForWkb(wkbFromWkt(t, "POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))"))
	require.NoError(t, err)
	withHole, err := CellsForWkb(wkbFromWkt(t, "POLYGON((0 0, 1 0, 1 1, 0 1, 0 0), (0.2 0.2, 0.8 0.2, 0.8 0.8, 0.2 0.8, 0.2 0.2))"))
	require.NoError(t, err)
	center := s2.CellIDFromLatLng(s2.LatLngFromDegrees(0.5, 0.5))
	require.True(t, withoutHole.ContainsCellID(center))
	require.False(t, withHole.ContainsCellID(center), "cells in the hole do not intersect the polygon")
	require.True(t, withHole.ContainsCellID(s2.CellIDFromLatLng(s2.LatLngFromDegrees(0.1, 0.1))))
}

func TestCellsForLineString(t *testing.T) {
	const wkt = "LINESTRING(-105 40, -104.5 40.2, -104.4 40.6)"
	cells, err := CellsForWkb(wkbFromWkt(t, wkt))
	require.NoError(t, err)
	region, err := regionForGeometry(geometryFromWkt(t, wkt))
	require.NoError(t, err)
	require.Equal(t, bruteForceCells(region), expandToLevel(cells))
}

func TestCellsForMultiGeometries(t *testing.T) {
	cells, err := CellsForWkb(wkbFromWkt(t, "GEOMETRYCOLLECTION(POINT(-105 40), MULTIPOINT((-104 39), (-106 41)), LINESTRING(-105.5 39.5, -105.49 39.51))"))
	require.NoError(t, err)
	for _, lngLat := range [][2]float64{{-105, 40}, {-104, 39}, {-106, 41}, {-105.5, 39.5}, {-105.49, 39.51}} {
		require.True(t, cells.ContainsCellID(s2.CellIDFromLatLng(s2.LatLngFromDegrees(lngLat[1], lngLat[0]))))
	}
}

func TestCellsForDegenerateGeometries(t *testing.T) {
	for _, wkt := range []string{
		"POINT EMPTY",
		"POLYGON EMPTY",
		"LINESTRING(1 1, 1 1)",
		// a polygon without any area only intersects the cells of its boundary
		"POLYGON((1 1, 2 2, 1 1))",
	} {
		_, err := CellsForWkb(wkbFromWkt(t, wkt))
		require.NoError(t, err, wkt)
	}
	// self intersecting polygons are common in upstream data; s2 cannot tell which side of
	// a bowtie is the inside and treats it as covering the whole world, so the size limit
	// keeps it from being associated with every cell instead of panicking or hanging
	_, err := CellsForWkb(wkbFromWkt(t, "POLYGON((0 0, 1 1, 1 0, 0 1, 0 0))"))
	require.ErrorIs(t, err, ErrGeometryTooLarge)

	cells, err := CellsForWkb(wkbFromWkt(t, "POINT EMPTY"))
	require.NoError(t, err)
	require.Empty(t, cells)

	_, err = CellsForWkb(wkbFromWkt(t, "POINT(500000 4000000)"))
	require.ErrorIs(t, err, errNotLongitudeLatitude, "projected coordinates are not supported")
}

// get the relation of a geometry to each of its cells
func relationsByCell(t *testing.T, wkt string) map[s2.CellID]string {
	t.Helper()
	relations, err := CellRelationsForWkb(wkbFromWkt(t, wkt))
	require.NoError(t, err)
	cells, err := CellsForWkb(wkbFromWkt(t, wkt))
	require.NoError(t, err)
	byCell := map[s2.CellID]string{}
	for _, relation := range relations {
		byCell[relation.Cell] = relation.Relation
	}
	require.Len(t, byCell, len(cells), "there should be a relation for every cell the geometry intersects")
	return byCell
}

func countRelations(byCell map[s2.CellID]string) map[string]int {
	counts := map[string]int{}
	for _, relation := range byCell {
		counts[relation]++
	}
	return counts
}

func TestGeometrySizeLimit(t *testing.T) {
	t.Run("a polygon the size of the contiguous US has no cells", func(t *testing.T) {
		wkb := wkbFromWkt(t, "POLYGON((-125 24, -66 24, -66 49, -125 49, -125 24))")
		_, err := CellsForWkb(wkb)
		require.ErrorIs(t, err, ErrGeometryTooLarge)
		_, err = CellRelationsForWkb(wkb)
		require.ErrorIs(t, err, ErrGeometryTooLarge)
	})

	t.Run("points spread across the US have no cells", func(t *testing.T) {
		_, err := CellsForWkb(wkbFromWkt(t, "MULTIPOINT((-122 47), (-80 25))"))
		require.ErrorIs(t, err, ErrGeometryTooLarge)
	})

	t.Run("a polygon the size of a state has cells", func(t *testing.T) {
		cells, err := CellsForWkb(wkbFromWkt(t, "POLYGON((-109 37, -102 37, -102 41, -109 41, -109 37))"))
		require.NoError(t, err)
		require.NotEmpty(t, cells)
	})
}

func TestCellRelations(t *testing.T) {
	t.Run("a point is within its cell", func(t *testing.T) {
		require.Equal(t, map[string]int{SfWithinIRI: 1}, countRelations(relationsByCell(t, "POINT(-111.68 40.44)")))
	})

	t.Run("a polygon inside a single cell is within it", func(t *testing.T) {
		center := s2.CellFromCellID(s2.CellIDFromLatLng(s2.LatLngFromDegrees(40.44, -111.68)).Parent(Level)).Center()
		latLng := s2.LatLngFromPoint(center)
		lng, lat := latLng.Lng.Degrees(), latLng.Lat.Degrees()
		const d = 0.001
		wkt := fmt.Sprintf("POLYGON((%[1]f %[2]f, %[3]f %[2]f, %[3]f %[4]f, %[1]f %[4]f, %[1]f %[2]f))", lng-d, lat-d, lng+d, lat+d)
		require.Equal(t, map[string]int{SfWithinIRI: 1}, countRelations(relationsByCell(t, wkt)))
	})

	t.Run("a polygon contains the cells inside it and overlaps the cells along its edges", func(t *testing.T) {
		const wkt = "POLYGON((-105.5 39.5, -104.9 39.5, -104.9 39.95, -105.5 39.95, -105.5 39.5))"
		polygon, err := polygonRegion(geometryFromWkt(t, wkt).MustAsPolygon())
		require.NoError(t, err)
		byCell := relationsByCell(t, wkt)
		counts := countRelations(byCell)
		require.Positive(t, counts[SfContainsIRI])
		require.Positive(t, counts[SfOverlapsIRI])
		require.Equal(t, len(byCell), counts[SfContainsIRI]+counts[SfOverlapsIRI])
		for cell, relation := range byCell {
			require.Equal(t, polygon.(*s2.Polygon).ContainsCell(s2.CellFromCellID(cell)), relation == SfContainsIRI)
		}
		corner := s2.CellIDFromLatLng(s2.LatLngFromDegrees(39.5, -105.5)).Parent(Level)
		require.Equal(t, SfOverlapsIRI, byCell[corner])
	})

	t.Run("a line through several cells crosses them", func(t *testing.T) {
		byCell := relationsByCell(t, "LINESTRING(-105 40, -104.5 40.2)")
		require.Greater(t, len(byCell), 1)
		require.Equal(t, map[string]int{SfCrossesIRI: len(byCell)}, countRelations(byCell))
	})

	t.Run("a line inside a single cell is within it", func(t *testing.T) {
		require.Equal(t, map[string]int{SfWithinIRI: 1}, countRelations(relationsByCell(t, "LINESTRING(-111.68 40.44, -111.6801 40.4401)")))
	})

	t.Run("a polygon still contains cells when the geometry also has a line", func(t *testing.T) {
		counts := countRelations(relationsByCell(t, "GEOMETRYCOLLECTION(POLYGON((-105.5 39.5, -104.9 39.5, -104.9 39.95, -105.5 39.95, -105.5 39.5)), LINESTRING(-100 40, -99.5 40.2))"))
		require.Positive(t, counts[SfContainsIRI])
		require.Positive(t, counts[SpatialRelationIRI], "edge cells of the polygon and the line are only known to intersect the geometry")
		require.Zero(t, counts[SfOverlapsIRI], "the line does not have the same dimension as the cells so the geometry cannot overlap them")
		require.Zero(t, counts[SfCrossesIRI], "the polygon has the same dimension as the cells so the geometry cannot cross them")
	})

	t.Run("points in different cells cross each of them", func(t *testing.T) {
		require.Equal(t, map[string]int{SfCrossesIRI: 2}, countRelations(relationsByCell(t, "MULTIPOINT((-105 40), (-104 39))")))
	})

	t.Run("errors are the same as for the cells", func(t *testing.T) {
		_, err := CellRelationsForWkb(wkbFromWkt(t, "POINT(500000 4000000)"))
		require.ErrorIs(t, err, errNotLongitudeLatitude)
		relations, err := CellRelationsForWkb(wkbFromWkt(t, "POINT EMPTY"))
		require.NoError(t, err)
		require.Empty(t, relations)
	})
}

func TestAddCellsToJsonld(t *testing.T) {
	cells, err := CellRelationsForWkb(wkbFromWkt(t, "POINT(-111.68 40.44)"))
	require.NoError(t, err)
	cell := cells[0].Cell

	t.Run("adds the cells without changing the context or other values", func(t *testing.T) {
		doc := `{"@context": "https://schema.org/", "@id": "https://example.com/1", "name": "a & b", "count": 12345678901234567890}`
		enriched, err := AddCellsToJsonld([]byte(doc), cells)
		require.NoError(t, err)
		require.Contains(t, string(enriched), `"@context":"https://schema.org/"`)
		require.Contains(t, string(enriched), `"name":"a & b"`)
		require.Contains(t, string(enriched), `"count":12345678901234567890`, "large numbers should not lose precision")

		var parsed map[string]any
		require.NoError(t, json.Unmarshal(enriched, &parsed))
		relations := parsed[SfWithinIRI].([]any)
		require.Len(t, relations, 1)
		node := relations[0].(map[string]any)
		require.Equal(t, CellIRI(cell), node["@id"])
		require.Equal(t, "http://stko-kwg.geog.ucsb.edu/lod/ontology/S2Cell_Level11", node["@type"])
	})

	t.Run("the cells are converted to rdf with the S2 vocabulary", func(t *testing.T) {
		enriched, err := AddCellsToJsonld([]byte(`{"@context": {"@vocab": "https://schema.org/"}, "@id": "https://example.com/1", "name": "a"}`), cells)
		require.NoError(t, err)
		processor, options, err := common.NewJsonldProcessor(false)
		require.NoError(t, err)
		nquads, err := common.JsonldToNquads(enriched, "urn:graph", processor, options)
		require.NoError(t, err)
		cellIRI := "<" + CellIRI(cell) + ">"
		require.True(t, strings.HasPrefix(CellIRI(cell), "http://stko-kwg.geog.ucsb.edu/lod/resource/s2.level11."))
		require.Contains(t, nquads, "<https://example.com/1> <https://w3id.org/cgs/ontology/core/spatiotemporal/sfWithin> "+cellIRI)
		require.Contains(t, nquads, cellIRI+" <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://stko-kwg.geog.ucsb.edu/lod/ontology/S2Cell_Level11>")
		require.Contains(t, nquads, cellIRI+` <http://stko-kwg.geog.ucsb.edu/lod/ontology/cellID> "`+strings.TrimPrefix(CellIRI(cell), "http://stko-kwg.geog.ucsb.edu/lod/resource/s2.level11.")+`"^^<http://www.w3.org/2001/XMLSchema#integer>`)
	})

	t.Run("existing spatial relations are kept", func(t *testing.T) {
		doc := `{"@id": "https://example.com/1", "` + SfWithinIRI + `": {"@id": "https://example.com/other"}}`
		enriched, err := AddCellsToJsonld([]byte(doc), cells)
		require.NoError(t, err)
		var parsed map[string]any
		require.NoError(t, json.Unmarshal(enriched, &parsed))
		require.Len(t, parsed[SfWithinIRI].([]any), 2)
	})

	t.Run("cells are grouped by their relation", func(t *testing.T) {
		relations := []CellRelation{{cell, SfContainsIRI}, {cell.Next(), SfOverlapsIRI}, {cell.Next().Next(), SfOverlapsIRI}}
		enriched, err := AddCellsToJsonld([]byte(`{"@id": "https://example.com/1"}`), relations)
		require.NoError(t, err)
		var parsed map[string]any
		require.NoError(t, json.Unmarshal(enriched, &parsed))
		require.Len(t, parsed[SfContainsIRI].([]any), 1)
		require.Len(t, parsed[SfOverlapsIRI].([]any), 2)
		require.NotContains(t, parsed, SfWithinIRI)
	})

	t.Run("no cells leaves the document unchanged", func(t *testing.T) {
		doc := `{"@id": "https://example.com/1"}`
		enriched, err := AddCellsToJsonld([]byte(doc), nil)
		require.NoError(t, err)
		require.Equal(t, doc, string(enriched))
	})

	t.Run("documents without a single top level node cannot be enriched", func(t *testing.T) {
		_, err := AddCellsToJsonld([]byte(`[{"@id": "https://example.com/1"}]`), cells)
		require.ErrorIs(t, err, ErrCannotAddCells)
		_, err = AddCellsToJsonld([]byte(`{"@graph": [{"@id": "https://example.com/1"}]}`), cells)
		require.ErrorIs(t, err, ErrCannotAddCells)
	})
}
