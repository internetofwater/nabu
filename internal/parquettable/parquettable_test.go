// Copyright 2026 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package parquettable

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"

	"github.com/apache/arrow-go/v18/parquet/file"
	// "github.com/golang/geo/s2"
	"github.com/peterstace/simplefeatures/geom"
	"github.com/stretchr/testify/require"
)

const wellFormedJsonld = `{
	"@context": {"schema": "https://schema.org/", "gsp": "http://www.opengis.net/ont/geosparql#"},
	"@id": "https://geoconnex.us/ref/gages/1",
	"@type": "schema:Place",
	"schema:name": "Gage 1",
	"schema:description": "A stream gage",
	"gsp:hasGeometry": {
		"@type": "http://www.opengis.net/ont/sf#Point",
		"gsp:asWKT": {"@type": "http://www.opengis.net/ont/geosparql#wktLiteral", "@value": "POINT (-111.68 40.44)"}
	}
}`

func requireWkt(t *testing.T, expected string, wkb []byte) {
	t.Helper()
	g, err := geom.UnmarshalWKB(wkb)
	require.NoError(t, err)
	require.Equal(t, expected, g.AsText())
}

func TestFeatureFromJsonldWithPrefixes(t *testing.T) {
	feature, err := FeatureFromJsonld([]byte(wellFormedJsonld), "https://example.com/1")
	require.NoError(t, err)
	require.Equal(t, "https://geoconnex.us/ref/gages/1", feature.ID)
	require.Equal(t, "Gage 1", feature.Name)
	require.Equal(t, "A stream gage", feature.Description)
	require.Equal(t, "https://example.com/1", feature.URL)
	require.Equal(t, wellFormedJsonld, string(feature.JSONLD), "the source document must be stored losslessly")
	requireWkt(t, "POINT(-111.68 40.44)", feature.Geometry)
}

func TestFeatureFromJsonldWithVocabAndAliases(t *testing.T) {
	doc := `{
		"@context": [{"@vocab": "http://schema.org/", "id": "@id", "geo": "http://www.opengis.net/ont/geosparql#"}],
		"id": "https://geoconnex.us/ref/hu02/01",
		"name": [{"@value": "Nouvelle-Angleterre", "@language": "fr"}, {"@value": "New England", "@language": "en"}],
		"description": {"@value": "A region"},
		"geo:hasGeometry": [{"geo:asWKT": "<http://www.opengis.net/def/crs/OGC/1.3/CRS84> LINESTRING (0 0, 1 1)"}]
	}`
	feature, err := FeatureFromJsonld([]byte(doc), "")
	require.NoError(t, err)
	require.Equal(t, "https://geoconnex.us/ref/hu02/01", feature.ID)
	require.Equal(t, "New England", feature.Name)
	require.Equal(t, "A region", feature.Description)
	requireWkt(t, "LINESTRING(0 0,1 1)", feature.Geometry)
}

func TestFeatureFromJsonldWithFullIRIsAndGraph(t *testing.T) {
	doc := `{
		"@graph": [{
			"@id": "https://example.com/a",
			"https://schema.org/name": "A",
			"http://www.opengis.net/ont/geosparql#hasGeometry": {
				"http://www.opengis.net/ont/geosparql#asWKT": {"@value": "POLYGON ((0 0, 1 1, 1 0, 0 1, 0 0))"}
			}
		}]
	}`
	feature, err := FeatureFromJsonld([]byte(doc), "")
	require.NoError(t, err)
	require.Equal(t, "https://example.com/a", feature.ID)
	require.Equal(t, "A", feature.Name)
	require.Empty(t, feature.Description)
	require.NotEmpty(t, feature.Geometry, "invalid but parsable geometries such as self intersecting polygons should still be kept")
}

func TestFeatureFromJsonldMissingFields(t *testing.T) {
	feature, err := FeatureFromJsonld([]byte(`{"@context": {"gsp": "http://www.opengis.net/ont/geosparql#"}, "gsp:hasGeometry": {"gsp:asWKT": "not wkt"}}`), "")
	require.NoError(t, err, "missing fields and bad geometries should not cause an error")
	require.Empty(t, feature.ID)
	require.Empty(t, feature.Name)
	require.Nil(t, feature.Geometry)

	_, err = FeatureFromJsonld([]byte(`{"@id": `), "")
	require.Error(t, err, "invalid json should cause an error")
}

func TestWriteThenRead(t *testing.T) {
	var buf bytes.Buffer
	writer, err := NewWriter(&buf)
	require.NoError(t, err)

	withGeometry, err := FeatureFromJsonld([]byte(wellFormedJsonld), "https://example.com/1")
	require.NoError(t, err)
	withGeometry.MainstemURI = "https://geoconnex.us/ref/mainstems/1"
	withoutGeometry, err := FeatureFromJsonld([]byte(`{"@id": "https://example.com/2"}`), "https://example.com/2")
	require.NoError(t, err)

	// write enough rows to span multiple row groups
	const total = maxRowsPerRowGroup + 10
	for i := range total {
		if i%2 == 0 {
			require.NoError(t, writer.Write(withGeometry))
		} else {
			require.NoError(t, writer.Write(withoutGeometry))
		}
	}
	require.Equal(t, total, writer.Rows())
	require.NoError(t, writer.Close())

	parquetReader, err := file.NewParquetReader(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)
	require.Equal(t, 2, parquetReader.NumRowGroups())
	geoMetadata := parquetReader.MetaData().KeyValueMetadata().FindValue("geo")
	require.NotNil(t, geoMetadata)
	var geo map[string]any
	require.NoError(t, json.Unmarshal([]byte(*geoMetadata), &geo))
	require.Equal(t, ColumnGeometry, geo["primary_column"])
	require.Equal(t, []any{"Point"}, geo["columns"].(map[string]any)[ColumnGeometry].(map[string]any)["geometry_types"])
	require.NoError(t, parquetReader.Close())

	read := []Feature{}
	err = Read(context.Background(), bytes.NewReader(buf.Bytes()), func(f Feature) error {
		read = append(read, f)
		return nil
	})
	require.NoError(t, err)
	require.Len(t, read, total)
	require.Equal(t, withGeometry, read[0])
	require.Equal(t, withoutGeometry, read[1])
	require.Equal(t, withoutGeometry, read[total-1])
}

func TestReadEmptyFile(t *testing.T) {
	var buf bytes.Buffer
	writer, err := NewWriter(&buf)
	require.NoError(t, err)
	require.NoError(t, writer.Close())

	rows := 0
	err = Read(context.Background(), bytes.NewReader(buf.Bytes()), func(f Feature) error {
		rows++
		return nil
	})
	require.NoError(t, err)
	require.Zero(t, rows)
}

func TestWkbGeometryType(t *testing.T) {
	for wkt, expected := range map[string]string{
		"POINT(1 2)":          "Point",
		"POINT Z(1 2 3)":      "Point Z",
		"MULTIPOLYGON EMPTY":  "MultiPolygon",
		"LINESTRING(0 0,1 1)": "LineString",
	} {
		g, err := geom.UnmarshalWKT(wkt)
		require.NoError(t, err)
		geometryType, ok := wkbGeometryType(g.AsBinary())
		require.True(t, ok)
		require.Equal(t, expected, geometryType)
	}
	_, ok := wkbGeometryType([]byte{1})
	require.False(t, ok)
}

// TODO: S2 cell ids are disabled until the approach is finalized
// func TestS2CellID(t *testing.T) {
// 	feature, err := FeatureFromJsonld([]byte(wellFormedJsonld), "")
// 	require.NoError(t, err)
// 	cell := s2.CellID(uint64(feature.S2CellID))
// 	require.True(t, cell.IsValid())
// 	require.True(t, cell.IsLeaf())
// 	require.InDelta(t, 40.44, cell.LatLng().Lat.Degrees(), 1e-6)
// 	require.InDelta(t, -111.68, cell.LatLng().Lng.Degrees(), 1e-6)
//
// 	withoutGeometry, err := FeatureFromJsonld([]byte(`{"@id": "https://example.com/2"}`), "")
// 	require.NoError(t, err)
// 	require.Zero(t, withoutGeometry.S2CellID)
//
// 	g, err := geom.UnmarshalWKT("POINT(500000 4000000)")
// 	require.NoError(t, err)
// 	require.Zero(t, s2CellIDForWkb(g.AsBinary()), "projected coordinates are not longitude/latitude")
//
// 	// the cell of a polygon is the cell of its centroid
// 	square, err := geom.UnmarshalWKT("POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))")
// 	require.NoError(t, err)
// 	require.Equal(t, int64(s2.CellIDFromLatLng(s2.LatLngFromDegrees(1, 1))), s2CellIDForWkb(square.AsBinary()))
// }
