// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

use core::panic;
use regex::Regex;
use std::{
    collections::HashMap,
    env,
    fmt::format,
    io::{BufRead, Read},
    sync::Arc,
};

use oxttl::NQuadsParser;

use argh::FromArgs;
use arrow_array::{self, ArrayRef, Int32Array, RecordBatch, StringArray, builder::StringBuilder};
use geo_types::Geometry;
use geoarrow_array::{GeoArrowArray, builder::GeometryBuilder};
use geoarrow_schema::GeometryType;
use wkt::TryFromWkt;

use arrow_schema::{DataType::Utf8, Field, Schema, SchemaBuilder};
use geoarrow_schema::GeoArrowType;
use geoparquet::writer::{GeoParquetRecordBatchEncoder, GeoParquetWriterOptionsBuilder};
use parquet::{arrow::ArrowWriter, format};

const GEOMETRY_COLUMN_NAME: &str = "geometry";

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

fn generate_schema() -> Schema {
    let mut schema_builder = SchemaBuilder::new();

    let geoarrow_type = GeoArrowType::Geometry(GeometryType::default());

    let geometry_field = geoarrow_type.to_field(GEOMETRY_COLUMN_NAME, false);
    schema_builder.push(geometry_field);

    let geoconnex_pid = Field::new("id", Utf8, false);
    schema_builder.push(geoconnex_pid);

    schema_builder.finish()
}

fn read_triples_into_arrays<R: BufRead>(
    triples_reader: R,
) -> Result<Vec<ArrayRef>, Box<dyn std::error::Error>> {
    let mut string_builder = StringBuilder::new();
    let mut geometry_builder = GeometryBuilder::new(GeometryType::default());

    let mut skolemization_id_to_geometry: HashMap<String, Geometry> = HashMap::new();
    let mut pid_to_skolemization_id: HashMap<String, String> = HashMap::new();

    let parser = NQuadsParser::new();
    let parsed_quads = parser.for_reader(triples_reader);

    for quad in parsed_quads {
        let quad = quad?;
        let subject = quad.subject;
        let predicate = quad.predicate;
        let object = quad.object;

        let predicate_str = predicate.as_ref();
        match predicate_str.to_string().as_str() {
            "<http://www.opengis.net/ont/geosparql#hasGeometry>" => {
                println!("Found geometry: {}", object.to_owned().to_string());
                pid_to_skolemization_id.insert(
                    subject.to_owned().to_string(),
                    object.to_owned().to_string(),
                );
            }
            "<http://www.opengis.net/ont/geosparql#asWKT>" => {
                println!("Found WKT: {}", object.to_owned().to_string());
                let object_string = object.to_owned().to_string();

                let part = object_string
                    .splitn(2, "^^")
                    .next()
                    .ok_or(format!("Invalid WKT string: {}", object_string))?;

                let part = part
                    .strip_prefix('"')
                    .ok_or(format!("Invalid WKT string: {}", object_string))?
                    .strip_suffix('"')
                    .ok_or(format!("Invalid WKT string: {}", object_string))?;


                println!("Parsed WKT: {}", part.to_string());

                let geometry = Geometry::try_from_wkt_str(&part.to_string())?;

                skolemization_id_to_geometry.insert(subject.to_owned().to_string(), geometry);
            }
            _ => (),
        }
    }

    for (pid, skolemization_id) in pid_to_skolemization_id {
        let geometry = skolemization_id_to_geometry
            .get(&skolemization_id)
            .ok_or(format!(
                "Could not find geometry for skolemization id: {}",
                skolemization_id
            ));
        let verified_geometry = match geometry {
            Ok(geometry) => geometry,
            Err(_) => {
                println!("Could not find geometry for skolemization id: {}", skolemization_id);
                continue;
            },
        };
        geometry_builder.push_geometry(Some(&verified_geometry))?;
        string_builder.append_value(&pid);
    }

    let string_array = string_builder.finish();
    let geometry_array = geometry_builder.finish();

    Ok(vec![
        geometry_array.to_array_ref(),
        Arc::new(string_array) as ArrayRef,
    ])
}

#[derive(FromArgs)]
/// Convert triples to geoparquet
struct TriplesToGeoparquetArgs {
    /// the input triples that will be converted to geoparquet
    #[argh(option, short = 'i')]
    triples: String,

    /// the output geoparquet file
    #[argh(option)]
    output: String,
}

fn main() {
    let args: TriplesToGeoparquetArgs = argh::from_env();

    let triples_file = std::fs::File::open(&args.triples).unwrap();
    let triples_reader = std::io::BufReader::new(triples_file);

    let schema = generate_schema();

    let (mut gpq_encoder, mut parquet_writer) = new_parquet_creator(&schema, &args.output);

    let arrays = match read_triples_into_arrays(triples_reader) {
        Ok(arrays) => arrays,
        Err(err) => {
            println!("{}", err);
            return;
        }
    };

    let batch = RecordBatch::try_new(Arc::new(schema), arrays).unwrap();

    for batch in [batch] {
        let encoded_batch = gpq_encoder.encode_record_batch(&batch).unwrap();
        parquet_writer.write(&encoded_batch).unwrap();
    }

    let kv_metadata = gpq_encoder.into_keyvalue().unwrap();
    parquet_writer.append_key_value_metadata(kv_metadata);
    parquet_writer.finish().unwrap();
}
