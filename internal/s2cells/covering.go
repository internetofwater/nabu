// Copyright 2026 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

// Package s2cells associates geometries with the S2 cells that they intersect
// and adds those associations to JSON-LD documents
package s2cells

import (
	"errors"
	"fmt"
	"math"
	"slices"

	"github.com/golang/geo/s2"
	"github.com/peterstace/simplefeatures/geom"
)

// The finest S2 cell level that features are associated with;
// level 11 cells are about 20 km² on average
const Level = 11

// The largest area in km² of the longitude/latitude bounding box of a geometry that is
// associated with cells; larger geometries, such as a polygon of an entire country, would
// add tens of thousands of cells to the document while saying little about where it is
const MaxBoundingBoxAreaKm2 = 1_000_000

// The mean radius of the earth in km, used to convert areas on the unit sphere to km²
const earthRadiusKm = 6371.0088

// ErrGeometryTooLarge is returned when the bounding box of a
// geometry is larger than MaxBoundingBoxAreaKm2
var ErrGeometryTooLarge = errors.New("geometry is too large to associate with S2 cells")

// CellsForWkb returns the S2 cells of level Level that intersect a WKB geometry with
// longitude/latitude coordinates. The cells are condensed by replacing any four sibling
// cells with their parent, so the result may also contain coarser cells; every level
// Level cell within a coarser cell intersects the geometry. An empty geometry has no cells
func CellsForWkb(wkb []byte) (cells s2.CellUnion, err error) {
	// the s2 library expects valid geometries and may panic on the
	// degenerate ones that are sometimes found in upstream data
	defer recoverFromS2Panic(&err)
	regions, err := regionsForWkb(wkb)
	if err != nil {
		return nil, err
	}
	return coveringForRegions(regions), nil
}

// CellRelation is an S2 cell that a geometry intersects and the
// IRI of the most specific spatial relation of the geometry to it
type CellRelation struct {
	Cell     s2.CellID
	Relation string
}

// CellRelationsForWkb returns the same cells as CellsForWkb along with the spatial relation
// of the geometry to each one. The relation is sfWithin if the geometry only intersects that
// cell, sfContains if a polygon of the geometry contains the cell, sfOverlaps if the geometry
// only has polygons and crosses the edge of the cell, and sfCrosses if the geometry only has
// points and lines and is partly outside the cell. A geometry that mixes polygons with points or
// lines has no single dimension to compare to the cell, so the generic spatialRelation is used
func CellRelationsForWkb(wkb []byte) (relations []CellRelation, err error) {
	defer recoverFromS2Panic(&err)
	regions, err := regionsForWkb(wkb)
	if err != nil {
		return nil, err
	}
	cells := coveringForRegions(regions)

	polygons := []*s2.Polygon{}
	for _, region := range regions {
		if polygon, ok := region.(*s2.Polygon); ok {
			polygons = append(polygons, polygon)
		}
	}
	onlyPolygons := len(polygons) == len(regions)
	noPolygons := len(polygons) == 0

	relations = make([]CellRelation, 0, len(cells))
	for _, cell := range cells {
		containsCell := slices.ContainsFunc(polygons, func(polygon *s2.Polygon) bool {
			return polygon.ContainsCell(s2.CellFromCellID(cell))
		})
		// the geometry is within a cell when it does not intersect any other cell
		withinCell := len(cells) == 1
		var relation string
		switch {
		case containsCell && withinCell:
			relation = SfEqualsIRI
		case withinCell:
			relation = SfWithinIRI
		case containsCell:
			relation = SfContainsIRI
		case onlyPolygons:
			relation = SfOverlapsIRI
		case noPolygons:
			// points and lines have a lower dimension than the cell, so a geometry of
			// them that is partly inside and partly outside the cell crosses it
			relation = SfCrossesIRI
		default:
			relation = SpatialRelationIRI
		}
		relations = append(relations, CellRelation{Cell: cell, Relation: relation})
	}
	return relations, nil
}

func recoverFromS2Panic(err *error) {
	if r := recover(); r != nil {
		*err = fmt.Errorf("failed to compute the S2 cells of the geometry: %v", r)
	}
}

// Convert each part of a WKB geometry to an s2 region, skipping empty parts
func regionsForWkb(wkb []byte) ([]s2.Region, error) {
	geometry, err := geom.UnmarshalWKB(wkb, geom.NoValidate{})
	if err != nil {
		return nil, err
	}
	regions := []s2.Region{}
	bound := s2.EmptyRect()
	for _, part := range geometry.Dump() {
		region, err := regionForGeometry(part)
		if err != nil {
			return nil, err
		}
		if region != nil {
			regions = append(regions, region)
			bound = bound.Union(region.RectBound())
		}
	}
	// checked before computing the covering since that is the expensive part for large geometries
	if areaKm2 := bound.Area() * earthRadiusKm * earthRadiusKm; areaKm2 > MaxBoundingBoxAreaKm2 {
		return nil, fmt.Errorf("%w: its bounding box is %.0f km² which is more than the limit of %d km²", ErrGeometryTooLarge, areaKm2, MaxBoundingBoxAreaKm2)
	}
	return regions, nil
}

// Get the level Level cells that intersect any of the regions, condensed into their parents
func coveringForRegions(regions []s2.Region) s2.CellUnion {
	// cells that are completely inside the geometry are kept at whatever level they are
	// found at instead of being subdivided, and the cells along the edges of the geometry
	// are subdivided down to the target level; the number of cells is not limited so the
	// result covers every intersecting cell at the target level
	coverer := &s2.RegionCoverer{MinLevel: 0, MaxLevel: Level, LevelMod: 1, MaxCells: math.MaxInt32}
	coverings := []s2.CellUnion{}
	for _, region := range regions {
		coverings = append(coverings, coverer.CellUnion(region))
	}
	return s2.CellUnionFromUnion(coverings...)
}

// Convert a point, linestring, or polygon to an s2 region; returns nil if the geometry is empty
func regionForGeometry(geometry geom.Geometry) (s2.Region, error) {
	switch geometry.Type() {
	case geom.TypePoint:
		xy, ok := geometry.MustAsPoint().XY()
		if !ok {
			return nil, nil
		}
		point, err := toS2Point(xy)
		if err != nil {
			return nil, err
		}
		return point, nil
	case geom.TypeLineString:
		points, err := toS2Points(geometry.MustAsLineString().Coordinates())
		if err != nil || len(points) == 0 {
			return nil, err
		}
		polyline := s2.Polyline(points)
		return &polyline, nil
	case geom.TypePolygon:
		return polygonRegion(geometry.MustAsPolygon())
	default:
		return nil, fmt.Errorf("unexpected geometry type %s", geometry.Type())
	}
}

// Convert a polygon to an s2 region; WKT does not define an orientation for rings
// so each ring is normalized and the s2 library determines which rings are holes
func polygonRegion(polygon geom.Polygon) (s2.Region, error) {
	if polygon.IsEmpty() {
		return nil, nil
	}
	shell, err := ringVertices(polygon.ExteriorRing())
	if err != nil {
		return nil, err
	}
	if len(shell) < 3 {
		// a polygon without any area only intersects the cells its boundary does
		polyline := s2.Polyline(shell)
		return &polyline, nil
	}
	loops := []*s2.Loop{s2.LoopFromPoints(shell)}
	for i := range polygon.NumInteriorRings() {
		hole, err := ringVertices(polygon.InteriorRingN(i))
		if err != nil {
			return nil, err
		}
		// a hole without any area does not remove anything from the polygon
		if len(hole) >= 3 {
			loops = append(loops, s2.LoopFromPoints(hole))
		}
	}
	for _, loop := range loops {
		loop.Normalize()
	}
	return s2.PolygonFromLoops(loops), nil
}

// Get the vertices of a ring in the form s2 loops expect; the closing vertex
// is implicit and s2 does not allow two adjacent vertices to be the same
func ringVertices(ring geom.LineString) ([]s2.Point, error) {
	points, err := toS2Points(ring.Coordinates())
	if err != nil {
		return nil, err
	}
	if len(points) > 1 && points[0] == points[len(points)-1] {
		points = points[:len(points)-1]
	}
	return points, nil
}

// Convert a coordinate sequence to s2 points with any repeated adjacent points removed
func toS2Points(seq geom.Sequence) ([]s2.Point, error) {
	points := make([]s2.Point, 0, seq.Length())
	for i := range seq.Length() {
		point, err := toS2Point(seq.GetXY(i))
		if err != nil {
			return nil, err
		}
		if len(points) > 0 && points[len(points)-1] == point {
			continue
		}
		points = append(points, point)
	}
	return points, nil
}

var errNotLongitudeLatitude = errors.New("geometry coordinates are not longitude/latitude")

func toS2Point(xy geom.XY) (s2.Point, error) {
	// written so that NaN coordinates are also rejected
	if !(xy.X >= -180 && xy.X <= 180 && xy.Y >= -90 && xy.Y <= 90) {
		return s2.Point{}, fmt.Errorf("%w: found (%v, %v)", errNotLongitudeLatitude, xy.X, xy.Y)
	}
	return s2.PointFromLatLng(s2.LatLngFromDegrees(xy.Y, xy.X)), nil
}
