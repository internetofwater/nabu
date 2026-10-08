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

// Read streams every feature in a parquet file to fn, one row group at a time
func Read(ctx context.Context, r parquet.ReaderAtSeeker, fn func(Feature) error) error {
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
	columnIndices := make([]int, 0, len(schema.Fields()))
	for _, field := range schema.Fields() {
		indices := fileSchema.FieldIndices(field.Name)
		if len(indices) != 1 || !arrow.TypeEqual(fileSchema.Field(indices[0]).Type, field.Type) {
			return schemaMismatchError(fileSchema)
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
		ids := record.Column(0).(*array.String)
		names := record.Column(1).(*array.String)
		descriptions := record.Column(2).(*array.String)
		geometries := record.Column(3).(*array.Binary)
		jsonlds := record.Column(4).(*array.String)
		urls := record.Column(5).(*array.String)
		mainstemURIs := record.Column(6).(*array.String)
		// s2CellIDs := record.Column(7).(*array.Int64)
		for i := range int(record.NumRows()) {
			if err := ctx.Err(); err != nil {
				return err
			}
			// arrow strings reference buffers that are released after the
			// batch, so every value is copied before being handed off
			feature := Feature{
				ID:          strings.Clone(ids.Value(i)),
				Name:        strings.Clone(names.Value(i)),
				Description: strings.Clone(descriptions.Value(i)),
				JSONLD:      []byte(jsonlds.Value(i)),
				URL:         strings.Clone(urls.Value(i)),
				MainstemURI: strings.Clone(mainstemURIs.Value(i)),
			}
			// if s2CellIDs.IsValid(i) {
			// 	feature.S2CellID = s2CellIDs.Value(i)
			// }
			if geometries.IsValid(i) {
				feature.Geometry = bytes.Clone(geometries.Value(i))
			}
			if err := fn(feature); err != nil {
				return err
			}
		}
	}
	return recordReader.Err()
}
