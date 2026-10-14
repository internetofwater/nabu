// Copyright 2026 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

// Package parquettable stores harvested JSON-LD documents as a GeoParquet
// file with one row per document. Along with the JSON-LD document itself,
// each row has a few commonly needed fields extracted from it so that the
// file can be used as a table without needing to process the JSON-LD
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
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/peterstace/simplefeatures/geom"
)

// Column names in the parquet file
const (
	ColumnID          = "@id"
	ColumnName        = "feature_name"
	ColumnDescription = "feature_description"
	ColumnGeometry    = "geometry"
	ColumnJSONLD      = "jsonld"
	ColumnURL         = "url"
	ColumnMainstemURI = "mainstem_uri"
	// the GeoParquet bbox covering of the geometry; it is derived from the
	// geometry when written so that readers can filter row groups by their
	// column statistics without decoding the WKB
	ColumnBbox = "bbox"
	// TODO: S2 cell ids are disabled until the approach is finalized
	// ColumnS2CellID    = "s2_cell_id"
)

var schema = arrow.NewSchema([]arrow.Field{
	{Name: ColumnID, Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: ColumnName, Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: ColumnDescription, Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: ColumnGeometry, Type: arrow.BinaryTypes.Binary, Nullable: true},
	{Name: ColumnJSONLD, Type: arrow.BinaryTypes.String, Nullable: false},
	{Name: ColumnURL, Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: ColumnMainstemURI, Type: arrow.BinaryTypes.String, Nullable: true},
	// {Name: ColumnS2CellID, Type: arrow.PrimitiveTypes.Int64, Nullable: true},
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

const (
	// the max number of rows in a row group; this bounds the memory
	// needed for both writing and streaming the file back in
	maxRowsPerRowGroup = 1000
	// row groups are also flushed early if they hold large documents
	maxBytesPerRowGroup = 64 * 1024 * 1024
)

// Writer streams features into a GeoParquet file. It is not safe for concurrent use
type Writer struct {
	fileWriter    *pqarrow.FileWriter
	builder       *array.RecordBuilder
	pendingRows   int
	pendingBytes  int
	totalRows     int
	geometryTypes map[string]struct{}
	// set if a geometry has a type that GeoParquet does not name, in
	// which case the geometry types of the file are left unspecified
	unnamedGeometryType bool
	// the bbox of every geometry in the file; empty if there are none
	fileBbox geom.Envelope
}

// NewWriter creates a writer that streams a parquet file to w
func NewWriter(w io.Writer) (*Writer, error) {
	props := parquet.NewWriterProperties(
		parquet.WithCompression(compress.Codecs.Zstd),
		parquet.WithDictionaryDefault(false),
		parquet.WithDictionaryFor(ColumnURL, true),
		// many features share the same mainstem
		parquet.WithDictionaryFor(ColumnMainstemURI, true),
		// statistics on large documents and geometries are not useful for filtering
		parquet.WithStatsFor(ColumnJSONLD, false),
		parquet.WithStatsFor(ColumnGeometry, false),
		parquet.WithMaxRowGroupLength(maxRowsPerRowGroup),
		parquet.WithCreatedBy("nabu"),
	)
	// pqarrow closes writers that implement io.Closer; the caller owns w so it is hidden
	writerWithoutClose := struct{ io.Writer }{w}
	fileWriter, err := pqarrow.NewFileWriter(schema, writerWithoutClose, props, pqarrow.NewArrowWriterProperties(pqarrow.WithStoreSchema()))
	if err != nil {
		return nil, err
	}
	return &Writer{
		fileWriter:    fileWriter,
		builder:       array.NewRecordBuilder(memory.DefaultAllocator, schema),
		geometryTypes: map[string]struct{}{},
	}, nil
}

func appendNullableString(b *array.StringBuilder, s string) {
	if s == "" {
		b.AppendNull()
	} else {
		b.Append(s)
	}
}

// Write adds a feature to the file
func (w *Writer) Write(f Feature) error {
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
	w.appendBbox(f.Geometry)
	w.builder.Field(4).(*array.StringBuilder).Append(string(f.JSONLD))
	appendNullableString(w.builder.Field(5).(*array.StringBuilder), f.URL)
	appendNullableString(w.builder.Field(6).(*array.StringBuilder), f.MainstemURI)
	// s2Builder := w.builder.Field(7).(*array.Int64Builder)
	// if f.S2CellID == 0 {
	// 	s2Builder.AppendNull()
	// } else {
	// 	s2Builder.Append(f.S2CellID)
	// }

	w.pendingRows++
	w.pendingBytes += len(f.JSONLD) + len(f.Geometry)
	w.totalRows++
	if w.pendingRows >= maxRowsPerRowGroup || w.pendingBytes >= maxBytesPerRowGroup {
		return w.flush()
	}
	return nil
}

// Append the bbox of a WKB geometry; it is null if there is no geometry or it is empty
func (w *Writer) appendBbox(wkb []byte) {
	bboxBuilder := w.builder.Field(7).(*array.StructBuilder)
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

// write all buffered features as a row group
func (w *Writer) flush() error {
	if w.pendingRows == 0 {
		return nil
	}
	record := w.builder.NewRecordBatch()
	defer record.Release()
	w.pendingRows = 0
	w.pendingBytes = 0
	return w.fileWriter.Write(record)
}

// SetKeyValueMetadata adds a key/value pair to the footer metadata of the file;
// it must be called before Close. The "geo" key is reserved for the GeoParquet metadata
func (w *Writer) SetKeyValueMetadata(key, value string) error {
	if key == geoMetadataKey {
		return fmt.Errorf("the %q footer metadata key is reserved for GeoParquet", geoMetadataKey)
	}
	return w.fileWriter.AppendKeyValueMetadata(key, value)
}

// The number of features written so far
func (w *Writer) Rows() int {
	return w.totalRows
}

// Close flushes any buffered features and writes the parquet footer;
// it does not close the underlying writer
func (w *Writer) Close() error {
	defer w.builder.Release()
	if err := w.flush(); err != nil {
		return err
	}
	geoMetadata, err := w.geoParquetMetadata()
	if err != nil {
		return err
	}
	if err := w.fileWriter.AppendKeyValueMetadata(geoMetadataKey, geoMetadata); err != nil {
		return err
	}
	return w.fileWriter.Close()
}

// Build the metadata that marks the file as GeoParquet
// https://geoparquet.org/releases/v1.1.0/
func (w *Writer) geoParquetMetadata() (string, error) {
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

// ensure the error message includes the column names so that a
// schema mismatch on read is easy to debug
func schemaMismatchError(found *arrow.Schema) error {
	return fmt.Errorf("parquet file does not have the expected feature schema; expected %s but found %s", schema, found)
}
