// Copyright 2026 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package parquettable

import (
	"bytes"
	"context"
	"io"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

// AsReaderAtSeeker returns r as a parquet.ReaderAtSeeker; if r does not support
// random access, it is read fully into memory
func AsReaderAtSeeker(r io.Reader) (parquet.ReaderAtSeeker, error) {
	if readerAtSeeker, ok := r.(parquet.ReaderAtSeeker); ok {
		return readerAtSeeker, nil
	}
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	return bytes.NewReader(data), nil
}

// ReadKeyValueMetadata returns the value of a key in the footer metadata of a parquet
// file; only the footer is read, not the rows. ok is false if the key is not set
func ReadKeyValueMetadata(r parquet.ReaderAtSeeker, key string) (value string, ok bool, err error) {
	parquetReader, err := file.NewParquetReader(r)
	if err != nil {
		return "", false, err
	}
	defer func() { _ = parquetReader.Close() }()
	found := parquetReader.MetaData().KeyValueMetadata().FindValue(key)
	if found == nil {
		return "", false, nil
	}
	return *found, true, nil
}

// Stream every row in a parquet file to fn, one row group at a time. Only the columns of
// the expected schema that are not skipped are read, in the order of the expected schema
func readTable(ctx context.Context, r parquet.ReaderAtSeeker, expected *arrow.Schema, skip func(arrow.Field) bool, fn func(record arrow.RecordBatch, row int) error) error {
	parquetReader, err := file.NewParquetReader(r)
	if err != nil {
		return err
	}
	defer func() { _ = parquetReader.Close() }()

	arrowReader, err := pqarrow.NewFileReader(parquetReader, pqarrow.ArrowReadProperties{BatchSize: maxRowsPerRowGroup}, memory.DefaultAllocator)
	if err != nil {
		return err
	}
	fileSchema, err := arrowReader.Schema()
	if err != nil {
		return err
	}
	columnIndices := make([]int, 0, len(expected.Fields()))
	for _, field := range expected.Fields() {
		if skip(field) {
			continue
		}
		indices := fileSchema.FieldIndices(field.Name)
		if len(indices) != 1 || !arrow.TypeEqual(fileSchema.Field(indices[0]).Type, field.Type) {
			return schemaMismatchError(expected, fileSchema)
		}
		columnIndices = append(columnIndices, indices[0])
	}

	recordReader, err := arrowReader.GetRecordReader(ctx, columnIndices, nil)
	if err != nil {
		return err
	}
	defer recordReader.Release()

	for recordReader.Next() {
		record := recordReader.RecordBatch()
		for i := range int(record.NumRows()) {
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := fn(record, i); err != nil {
				return err
			}
		}
	}
	return recordReader.Err()
}

// ReadStructured streams every feature in a structured parquet file to fn;
// the features only have the fields stored in the structured file
func ReadStructured(ctx context.Context, r parquet.ReaderAtSeeker, fn func(Feature) error) error {
	// the bbox is derived from the geometry so it is not read back
	skipBbox := func(field arrow.Field) bool { return field.Name == ColumnBbox }
	return readTable(ctx, r, structuredSchema, skipBbox, func(record arrow.RecordBatch, i int) error {
		geometries := record.Column(3).(*array.Binary)
		// arrow strings reference buffers that are released after the
		// batch, so every value is copied before being handed off
		feature := Feature{
			ID:          strings.Clone(record.Column(0).(*array.String).Value(i)),
			Name:        strings.Clone(record.Column(1).(*array.String).Value(i)),
			Description: strings.Clone(record.Column(2).(*array.String).Value(i)),
			MainstemURI: strings.Clone(record.Column(4).(*array.String).Value(i)),
		}
		if geometries.IsValid(i) {
			feature.Geometry = bytes.Clone(geometries.Value(i))
		}
		return fn(feature)
	})
}

// ReadRDF streams every feature in an RDF parquet file to fn;
// the features only have their ID and JSON-LD set
func ReadRDF(ctx context.Context, r parquet.ReaderAtSeeker, fn func(Feature) error) error {
	readAll := func(arrow.Field) bool { return false }
	return readTable(ctx, r, rdfSchema, readAll, func(record arrow.RecordBatch, i int) error {
		return fn(Feature{
			// arrow strings reference buffers that are released after the batch so they are copied
			ID:     strings.Clone(record.Column(0).(*array.String).Value(i)),
			JSONLD: []byte(record.Column(1).(*array.String).Value(i)),
		})
	})
}
