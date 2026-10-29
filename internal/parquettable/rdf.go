// Copyright 2026 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package parquettable

import (
	"io"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/parquet"
)

// Column names in the RDF parquet file
const (
	ColumnRDFID  = "id"
	ColumnJSONLD = "jsonld"
)

var rdfSchema = arrow.NewSchema([]arrow.Field{
	{Name: ColumnRDFID, Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: ColumnJSONLD, Type: arrow.BinaryTypes.String, Nullable: false},
}, nil)

// RDFWriter streams the JSON-LD documents of features into a parquet file. It is not safe for concurrent use
type RDFWriter struct {
	*tableWriter
}

// NewRDFWriter creates a writer that streams a parquet file of JSON-LD documents to w
func NewRDFWriter(w io.Writer) (*RDFWriter, error) {
	tableWriter, err := newTableWriter(w, rdfSchema,
		// a bloom filter lets readers skip row groups that do not have the id they are looking for
		parquet.WithBloomFilterEnabledFor(ColumnRDFID, true),
		parquet.WithBloomFilterNDVFor(ColumnRDFID, maxRowsPerRowGroup),
		// statistics on large documents are not useful for filtering
		parquet.WithStatsFor(ColumnJSONLD, false),
	)
	if err != nil {
		return nil, err
	}
	return &RDFWriter{tableWriter: tableWriter}, nil
}

// Write adds the JSON-LD document of a feature to the file
func (w *RDFWriter) Write(f Feature) error {
	appendNullableString(w.builder.Field(0).(*array.StringBuilder), f.ID)
	w.builder.Field(1).(*array.StringBuilder).Append(string(f.JSONLD))
	return w.rowAdded(len(f.JSONLD))
}
