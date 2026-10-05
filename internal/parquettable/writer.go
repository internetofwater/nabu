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
	"slices"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
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
)

var schema = arrow.NewSchema([]arrow.Field{
	{Name: ColumnID, Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: ColumnName, Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: ColumnDescription, Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: ColumnGeometry, Type: arrow.BinaryTypes.Binary, Nullable: true},
	{Name: ColumnJSONLD, Type: arrow.BinaryTypes.String, Nullable: false},
	{Name: ColumnURL, Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: ColumnMainstemURI, Type: arrow.BinaryTypes.String, Nullable: true},
}, nil)

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
	w.builder.Field(4).(*array.StringBuilder).Append(string(f.JSONLD))
	appendNullableString(w.builder.Field(5).(*array.StringBuilder), f.URL)
	appendNullableString(w.builder.Field(6).(*array.StringBuilder), f.MainstemURI)

	w.pendingRows++
	w.pendingBytes += len(f.JSONLD) + len(f.Geometry)
	w.totalRows++
	if w.pendingRows >= maxRowsPerRowGroup || w.pendingBytes >= maxBytesPerRowGroup {
		return w.flush()
	}
	return nil
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
	if err := w.fileWriter.AppendKeyValueMetadata("geo", geoMetadata); err != nil {
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
	metadata := map[string]any{
		"version":        "1.1.0",
		"primary_column": ColumnGeometry,
		"columns": map[string]any{
			ColumnGeometry: map[string]any{
				"encoding":       "WKB",
				"geometry_types": geometryTypes,
			},
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
