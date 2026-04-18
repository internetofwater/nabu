// Copyright 2026 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

use geoarrow_schema::GeometryType;

use arrow_schema::{DataType::Utf8, Field, Schema, SchemaBuilder};
use geoarrow_schema::GeoArrowType;
use geoparquet::writer::{GeoParquetRecordBatchEncoder, GeoParquetWriterOptionsBuilder};
use parquet::arrow::ArrowWriter;

const GEOMETRY_COLUMN_NAME: &str = "geometry";

/// Given an arrow schema and the output file name, generate the writer and encoder
/// for creating a geoparquet file
pub fn new_parquet_creator(
    schema: &Schema,
    file_name: &str,
) -> (GeoParquetRecordBatchEncoder, ArrowWriter<std::fs::File>) {
    let options = GeoParquetWriterOptionsBuilder::default()
        .set_primary_column(GEOMETRY_COLUMN_NAME.to_string())
        .build();

    let gpq_encoder = GeoParquetRecordBatchEncoder::try_new(schema, &options).unwrap();

    let output_file = std::fs::File::create(file_name).unwrap();
    let parquet_writer =
        ArrowWriter::try_new(output_file, gpq_encoder.target_schema(), None).unwrap();

    (gpq_encoder, parquet_writer)
}

/// Generate a schema for the geoconnex geoparquet file
/// which contains the following columns:
/// id, geometry
pub fn generate_schema() -> Schema {
    let mut schema_builder = SchemaBuilder::new();

    let geoarrow_type = GeoArrowType::Geometry(GeometryType::default());

    let geometry_field = geoarrow_type.to_field(GEOMETRY_COLUMN_NAME, false);
    schema_builder.push(geometry_field);

    let geoconnex_pid = Field::new("id", Utf8, false);
    schema_builder.push(geoconnex_pid);

    let geoconnex_sitemap = Field::new("geoconnex_sitemap", Utf8, false);
    schema_builder.push(geoconnex_sitemap);

    let feature_name = Field::new("feature_name", Utf8, true);
    schema_builder.push(feature_name);

    let feature_description = Field::new("feature_description", Utf8, true);
    schema_builder.push(feature_description);

    schema_builder.finish()
}
