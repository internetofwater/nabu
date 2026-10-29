// Copyright 2026 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

// Package parquettable stores harvested JSON-LD documents as two parquet files
// with one row per document, written in the same order:
//   - a structured GeoParquet file with commonly needed fields extracted from each
//     document so that it can be used as a table without processing the JSON-LD
//   - an RDF file with each JSON-LD document, including its enrichments, for
//     converting to RDF
//
// Both files have the @id of each document so that they can be joined
package parquettable

import (
	"fmt"
	"io"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

const (
	// the max number of rows in a row group; this bounds the memory
	// needed for both writing and streaming the file back in
	maxRowsPerRowGroup = 1000
	// row groups are also flushed early if they hold large documents
	maxBytesPerRowGroup = 64 * 1024 * 1024
)

// tableWriter streams rows into a parquet file; the file specific
// writers append each row to the builder and then call rowAdded
type tableWriter struct {
	fileWriter   *pqarrow.FileWriter
	builder      *array.RecordBuilder
	schema       *arrow.Schema
	pendingRows  int
	pendingBytes int
	totalRows    int
}

func newTableWriter(w io.Writer, schema *arrow.Schema, props ...parquet.WriterProperty) (*tableWriter, error) {
	props = append([]parquet.WriterProperty{
		parquet.WithCompression(compress.Codecs.Zstd),
		parquet.WithDictionaryDefault(false),
		parquet.WithMaxRowGroupLength(maxRowsPerRowGroup),
		parquet.WithCreatedBy("nabu"),
		// arrow-go marks the root schema node as repeated by default. The parquet
		// spec says the root's repetition should be ignored, and pyarrow, duckdb, and
		// arrow-go do ignore it, but hyparquet (used by browser based viewers like the
		// portolan STAC browser) counts it as a repetition level. It then decodes
		// repetition levels that were never written and reads every page at the wrong
		// offset. Flat columns silently decode as mostly null, and the nested bbox struct
		// fails with "Offset is outside the bounds of the DataView". Marking the root as
		// required matches what pyarrow writes and is read correctly everywhere
		parquet.WithRootRepetition(parquet.Repetitions.Required),
	}, props...)
	// pqarrow closes writers that implement io.Closer; the caller owns w so it is hidden
	writerWithoutClose := struct{ io.Writer }{w}
	fileWriter, err := pqarrow.NewFileWriter(schema, writerWithoutClose, parquet.NewWriterProperties(props...), pqarrow.NewArrowWriterProperties(pqarrow.WithStoreSchema()))
	if err != nil {
		return nil, err
	}
	return &tableWriter{
		fileWriter: fileWriter,
		builder:    array.NewRecordBuilder(memory.DefaultAllocator, schema),
		schema:     schema,
	}, nil
}

func appendNullableString(b *array.StringBuilder, s string) {
	if s == "" {
		b.AppendNull()
	} else {
		b.Append(s)
	}
}

// Record that a row of roughly the given size was appended to the builder,
// writing a row group if enough rows are buffered
func (w *tableWriter) rowAdded(size int) error {
	w.pendingRows++
	w.pendingBytes += size
	w.totalRows++
	if w.pendingRows >= maxRowsPerRowGroup || w.pendingBytes >= maxBytesPerRowGroup {
		return w.flush()
	}
	return nil
}

// write all buffered rows as a row group
func (w *tableWriter) flush() error {
	if w.pendingRows == 0 {
		return nil
	}
	record := w.builder.NewRecordBatch()
	defer record.Release()
	w.pendingRows = 0
	w.pendingBytes = 0
	return w.fileWriter.Write(record)
}

// SetKeyValueMetadata adds a key/value pair to the footer metadata of the file; it must be called before Close
func (w *tableWriter) SetKeyValueMetadata(key, value string) error {
	return w.fileWriter.AppendKeyValueMetadata(key, value)
}

// The number of rows written so far
func (w *tableWriter) Rows() int {
	return w.totalRows
}

// Close flushes any buffered rows and writes the parquet footer;
// it does not close the underlying writer
func (w *tableWriter) Close() error {
	defer w.builder.Release()
	if err := w.flush(); err != nil {
		return err
	}
	return w.fileWriter.Close()
}

// ensure the error message includes the column names so that a
// schema mismatch on read is easy to debug
func schemaMismatchError(expected, found *arrow.Schema) error {
	return fmt.Errorf("parquet file does not have the expected schema; expected %s but found %s", expected, found)
}
