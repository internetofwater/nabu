// Copyright 2026 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package parquettable

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"slices"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/peterstace/simplefeatures/geom"
)

// Column names in the structured parquet file
const (
	ColumnID          = "@id"
	ColumnName        = "feature_name"
	ColumnDescription = "feature_description"
	ColumnGeometry    = "geometry"
	ColumnMainstemURI = "mainstem_uri"
	// the GeoParquet bbox covering of the geometry; it is derived from the
	// geometry when written so that readers can filter row groups by their
	// column statistics without decoding the WKB
	ColumnBbox = "bbox"
)

var structuredSchema = arrow.NewSchema([]arrow.Field{
	{Name: ColumnID, Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: ColumnName, Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: ColumnDescription, Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: ColumnGeometry, Type: arrow.BinaryTypes.Binary, Nullable: true},
	{Name: ColumnMainstemURI, Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: ColumnBbox, Type: bboxType, Nullable: true},
}, nil)

// The field names of the bbox struct are fixed by the GeoParquet spec; doubles are
// used since floats would need to be rounded outward to still cover the geometry
var bboxType = arrow.StructOf(
	arrow.Field{Name: "xmin", Type: arrow.PrimitiveTypes.Float64},
	arrow.Field{Name: "ymin", Type: arrow.PrimitiveTypes.Float64},
	arrow.Field{Name: "xmax", Type: arrow.PrimitiveTypes.Float64},
	arrow.Field{Name: "ymax", Type: arrow.PrimitiveTypes.Float64},
)

// The footer metadata key that marks the file as GeoParquet
const geoMetadataKey = "geo"

// StructuredWriter streams the tabular fields of features into a GeoParquet file. It is not safe for concurrent use
type StructuredWriter struct {
	*tableWriter
	geometryTypes map[string]struct{}
	// set if a geometry has a type that GeoParquet does not name, in
	// which case the geometry types of the file are left unspecified
	unnamedGeometryType bool
	// the bbox of every geometry in the file; empty if there are none
	fileBbox geom.Envelope
}

// NewStructuredWriter creates a writer that streams a GeoParquet file to w
func NewStructuredWriter(w io.Writer) (*StructuredWriter, error) {
	tableWriter, err := newTableWriter(w, structuredSchema,
		// many features share the same mainstem
		parquet.WithDictionaryFor(ColumnMainstemURI, true),
		// statistics on large geometries are not useful for filtering
		parquet.WithStatsFor(ColumnGeometry, false),
	)
	if err != nil {
		return nil, err
	}
	return &StructuredWriter{tableWriter: tableWriter, geometryTypes: map[string]struct{}{}}, nil
}

// Write adds the tabular fields of a feature to the file
func (w *StructuredWriter) Write(f Feature) error {
	appendNullableString(w.builder.Field(0).(*array.StringBuilder), f.ID)
	appendNullableString(w.builder.Field(1).(*array.StringBuilder), f.Name)
	appendNullableString(w.builder.Field(2).(*array.StringBuilder), f.Description)
	geometryBuilder := w.builder.Field(3).(*array.BinaryBuilder)
	if len(f.Geometry) == 0 {
		geometryBuilder.AppendNull()
	} else {
		geometryBuilder.Append(f.Geometry)
		if geometryType, ok := wkbGeometryType(f.Geometry); ok {
			w.geometryTypes[geometryType] = struct{}{}
		} else {
			w.unnamedGeometryType = true
		}
	}
	appendNullableString(w.builder.Field(4).(*array.StringBuilder), f.MainstemURI)
	w.appendBbox(f.Geometry)
	return w.rowAdded(len(f.Geometry) + len(f.Name) + len(f.Description))
}

// Append the bbox of a WKB geometry; it is null if there is no geometry or it is empty
func (w *StructuredWriter) appendBbox(wkb []byte) {
	bboxBuilder := w.builder.Field(5).(*array.StructBuilder)
	envelope, ok := wkbEnvelope(wkb)
	if !ok {
		bboxBuilder.AppendNull()
		return
	}
	minXY, maxXY, _ := envelope.MinMaxXYs()
	bboxBuilder.Append(true)
	for i, value := range []float64{minXY.X, minXY.Y, maxXY.X, maxXY.Y} {
		bboxBuilder.FieldBuilder(i).(*array.Float64Builder).Append(value)
	}
	w.fileBbox = w.fileBbox.ExpandToIncludeEnvelope(envelope)
}

// Get the envelope of a WKB geometry; ok is false if there is no
// geometry, it cannot be parsed, or it has no finite coordinates
func wkbEnvelope(wkb []byte) (envelope geom.Envelope, ok bool) {
	if len(wkb) == 0 {
		return geom.Envelope{}, false
	}
	// the geometry was already validated when converted from WKT
	geometry, err := geom.UnmarshalWKB(wkb, geom.NoValidate{})
	if err != nil {
		return geom.Envelope{}, false
	}
	envelope = geometry.Envelope()
	minXY, maxXY, ok := envelope.MinMaxXYs()
	if !ok {
		return geom.Envelope{}, false
	}
	for _, value := range []float64{minXY.X, minXY.Y, maxXY.X, maxXY.Y} {
		if math.IsNaN(value) || math.IsInf(value, 0) {
			return geom.Envelope{}, false
		}
	}
	return envelope, true
}

// SetKeyValueMetadata adds a key/value pair to the footer metadata of the file;
// it must be called before Close. The "geo" key is reserved for the GeoParquet metadata
func (w *StructuredWriter) SetKeyValueMetadata(key, value string) error {
	if key == geoMetadataKey {
		return fmt.Errorf("the %q footer metadata key is reserved for GeoParquet", geoMetadataKey)
	}
	return w.tableWriter.SetKeyValueMetadata(key, value)
}

// Close flushes any buffered features and writes the parquet footer
// with the GeoParquet metadata; it does not close the underlying writer
func (w *StructuredWriter) Close() error {
	geoMetadata, err := w.geoParquetMetadata()
	if err != nil {
		w.builder.Release()
		return err
	}
	if err := w.tableWriter.SetKeyValueMetadata(geoMetadataKey, geoMetadata); err != nil {
		w.builder.Release()
		return err
	}
	return w.tableWriter.Close()
}

// Build the metadata that marks the file as GeoParquet
// https://geoparquet.org/releases/v1.1.0/
func (w *StructuredWriter) geoParquetMetadata() (string, error) {
	// an empty list signifies that the geometry types are unknown
	geometryTypes := make([]string, 0, len(w.geometryTypes))
	if !w.unnamedGeometryType {
		for geometryType := range w.geometryTypes {
			geometryTypes = append(geometryTypes, geometryType)
		}
	}
	slices.Sort(geometryTypes)
	geometryColumn := map[string]any{
		"encoding":       "WKB",
		"geometry_types": geometryTypes,
		"covering": map[string]any{
			"bbox": map[string]any{
				"xmin": []string{ColumnBbox, "xmin"},
				"ymin": []string{ColumnBbox, "ymin"},
				"xmax": []string{ColumnBbox, "xmax"},
				"ymax": []string{ColumnBbox, "ymax"},
			},
		},
	}
	// the file bbox is optional and is left out when there are no geometries
	if minXY, maxXY, ok := w.fileBbox.MinMaxXYs(); ok {
		geometryColumn["bbox"] = []float64{minXY.X, minXY.Y, maxXY.X, maxXY.Y}
	}
	metadata := map[string]any{
		"version":        "1.1.0",
		"primary_column": ColumnGeometry,
		"columns": map[string]any{
			ColumnGeometry: geometryColumn,
		},
	}
	asJson, err := json.Marshal(metadata)
	return string(asJson), err
}

// Get the GeoParquet name for the type of a WKB geometry
func wkbGeometryType(wkb []byte) (string, bool) {
	if len(wkb) < 5 {
		return "", false
	}
	var code uint32
	if wkb[0] == 0 {
		code = binary.BigEndian.Uint32(wkb[1:5])
	} else {
		code = binary.LittleEndian.Uint32(wkb[1:5])
	}
	names := map[uint32]string{1: "Point", 2: "LineString", 3: "Polygon", 4: "MultiPoint", 5: "MultiLineString", 6: "MultiPolygon", 7: "GeometryCollection"}
	name, ok := names[code%1000]
	if !ok {
		return "", false
	}
	switch code / 1000 {
	case 0:
		return name, true
	case 1:
		return name + " Z", true
	default:
		// GeoParquet only defines names for 2D and Z geometries
		return "", false
	}
}
