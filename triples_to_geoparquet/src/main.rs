// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

use std::{collections::HashMap, fs::{self, File}, io::{BufRead, Read}, sync::Arc};

use flate2::read::GzDecoder;
use log::{debug, error, info};
use oxrdf::Term;
use oxttl::NQuadsParser;

use argh::FromArgs;
use arrow_array::{self, ArrayRef, RecordBatch, builder::StringBuilder};
use geo_types::{Geometry, Point};
use geoarrow_array::{GeoArrowArray, builder::GeometryBuilder};
use geoarrow_schema::GeometryType;
use wkt::{ToWkt, TryFromWkt};

use arrow_schema::{DataType::Utf8, Field, Schema, SchemaBuilder};
use geoarrow_schema::GeoArrowType;
use geoparquet::writer::{GeoParquetRecordBatchEncoder, GeoParquetWriterOptionsBuilder};
use parquet::arrow::ArrowWriter;

use std::io::{BufReader};
use std::path::Path;

const GEOMETRY_COLUMN_NAME: &str = "geometry";

const UKNOWN_POINT_COORD: f64 = -1.0;

#[cfg(test)]
mod tests;

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

/// Check if two geometries are equal with some tolerance (for floating point errors, etc)
fn generally_equal(geom1: &Geometry, geom2: &Point) -> bool {
    match geom1 {
        Geometry::Point(point) => {
            let x_equal = (geom2.x() - point.x()).abs() < 0.001;
            let y_equal = (geom2.y() - point.y()).abs() < 0.001;
            x_equal && y_equal
        }
        _ => false
    }
}

fn f64_from_triple_term(data: &Term) -> Result<f64, Box<dyn std::error::Error>> {
    let binding = data.to_string();

    let literal = binding.split("^^").next().unwrap().trim_matches('"');

    let mut parts = literal.split('E');
    let base: f64 = parts.next().unwrap().parse()?;

    match parts.next() {
        Some(exp_str) => {
            let exp: i32 = exp_str.parse()?;
            Ok(base * 10_f64.powi(exp))
        }
        None => Ok(base),
    }
}

/// Given info for both the geosparql and schema geo representations of a geometry,
/// combine them into a single canonical representation for each pid and return
/// the associated hashmap
pub fn combine_geometry_representations(
    pid_to_geosparql_skolemization_id: HashMap<String, String>,
    geosparql_skolemization_id_to_geometry: HashMap<String, Geometry>,
    pid_to_schema_geo_skolemization_id: HashMap<String, String>,
    schema_geo_skolemization_id_to_geometry: HashMap<String, Point>,
) -> Result<HashMap<String, Geometry>, Box<dyn std::error::Error>> {
    let mut pid_to_canonical_geometry: HashMap<String, Geometry> = HashMap::new();

    // first we go through and get all the geosparql geometry;
    // this is the ideal canonical representation since wkt is more flexible
    // than just a point
    for (pid, geosparql_skolemization_id) in pid_to_geosparql_skolemization_id {
        match geosparql_skolemization_id_to_geometry.get(&geosparql_skolemization_id) {
            Some(geometry) => {
                pid_to_canonical_geometry.insert(pid, geometry.clone());
            }
            None => {
                return Err(format!(
                    "Could not find geometry for geosparql skolemization id {} for pid {}",
                    geosparql_skolemization_id, pid
                )
                .into());
            }
        }
    }

    // next we go through and get all the schema geo geometries
    for (pid, schema_geo_skolemization_id) in pid_to_schema_geo_skolemization_id {
        match schema_geo_skolemization_id_to_geometry.get(&schema_geo_skolemization_id) {
            Some(point_geometry) => {
                if let Some(gsp_geometry) = pid_to_canonical_geometry.get(&pid) {
                    debug!("Canonical gsp geo {}, {}", pid, gsp_geometry.to_wkt());
                    if !generally_equal(gsp_geometry, point_geometry) {
                        return Err(format!(
                                "pid {} with geosparql geometry '{}' does not match schema geo skolemization id {} with schema geo point geometry '{}'",
                                pid, gsp_geometry.to_wkt(), schema_geo_skolemization_id, point_geometry.to_wkt()
                            ).into());
                    }
                } 
                pid_to_canonical_geometry.insert(pid, Geometry::Point(point_geometry.clone()));
            }
            None => {
                return Err(format!(
                    "No geometry for schema geo skolemization id {} for pid {}",
                    schema_geo_skolemization_id, pid
                )
                .into());
            }
        }
    }

    Ok(pid_to_canonical_geometry)
}

fn read_triples_into_arrays<R: BufRead>(
    triples_reader: R,
) -> Result<Vec<ArrayRef>, Box<dyn std::error::Error>> {
    let mut string_builder = StringBuilder::new();
    let mut geometry_builder = GeometryBuilder::new(GeometryType::default());

    // there are two ways to encode geometries in nquads: either as WKT or as a schema.org latitude/longitude pair
    let mut pid_to_geoparql_skolemization_id: HashMap<String, String> = HashMap::new();
    let mut geosparql_skolemization_id_to_geometry: HashMap<String, Geometry> = HashMap::new();

    let mut pid_to_schema_geo_skolemization_id: HashMap<String, String> = HashMap::new();
    let mut schema_geo_skolemization_id_to_geometry: HashMap<String, Point> = HashMap::new();

    let parser = NQuadsParser::new();
    let parsed_quads = parser.for_reader(triples_reader);

    for quad in parsed_quads {
        let quad = quad?;
        let subject = quad.subject;
        let predicate = quad.predicate;
        let object = quad.object;

        let predicate_str = predicate.to_string();
        match predicate_str.clone().as_str() {
            "<http://www.opengis.net/ont/geosparql#hasGeometry>" => {
                debug!("Found geometry: {}", object.to_owned().to_string());
                pid_to_geoparql_skolemization_id.insert(
                    subject.to_owned().to_string(),
                    object.to_owned().to_string(),
                );
            }
            "<https://schema.org/geo>" => {
                debug!("Found geometry: {}", object.to_owned().to_string());
                pid_to_schema_geo_skolemization_id.insert(
                    subject.to_owned().to_string(),
                    object.to_owned().to_string(),
                );
            }

            point_coord_type @ ("<https://schema.org/longitude>"
            | "<https://schema.org/latitude>") => {
                match schema_geo_skolemization_id_to_geometry.get(&subject.to_owned().to_string()) {
                    Some(existing_val) => match (existing_val.x(), existing_val.y()) {
                        (UKNOWN_POINT_COORD, UKNOWN_POINT_COORD) => {
                            return Err("Found a point with unknown coords for both x/y; this is a sign that something went wrong on our end during construction".into());
                        }
                        (UKNOWN_POINT_COORD, y) => {
                            schema_geo_skolemization_id_to_geometry.insert(
                                subject.to_string(),
                                Point::new(f64_from_triple_term(&object)?, y),
                            );
                        }
                        (x, UKNOWN_POINT_COORD) => {
                            schema_geo_skolemization_id_to_geometry.insert(
                                subject.to_string(),
                                Point::new(x, f64_from_triple_term(&object)?),
                            );
                        }
                        (_, _) => {
                            return Err("Found a point with known coords for both x/y; this is a sign that something is defined multiple times in the triples or went wrong on our end during construction".into());
                        }
                    },
                    None => {
                        match point_coord_type {
                            "<https://schema.org/latitude>" => {
                                schema_geo_skolemization_id_to_geometry.insert(
                                    subject.to_string(),
                                    Point::new(UKNOWN_POINT_COORD, f64_from_triple_term(&object)?),
                                );
                            }
                            "<https://schema.org/longitude>" => {
                                schema_geo_skolemization_id_to_geometry.insert(
                                    subject.to_owned().to_string(),
                                    Point::new(f64_from_triple_term(&object)?, UKNOWN_POINT_COORD),
                                );
                            }
                            // skip other predicates unrelated to schema geo
                            _ => {}
                        }
                    }
                }
            }

            "<http://www.opengis.net/ont/geosparql#asWKT>" => {
                debug!("Found WKT: {}", object.to_owned().to_string());
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

                debug!("Parsed WKT: {}", part.to_string());

                let geometry = Geometry::try_from_wkt_str(&part.to_string())?;
                geosparql_skolemization_id_to_geometry
                    .insert(subject.to_owned().to_string(), geometry);
            }
            &_ => {}
        }
    }

    let pid_to_geometry = combine_geometry_representations(
        pid_to_geoparql_skolemization_id,
        geosparql_skolemization_id_to_geometry,
        pid_to_schema_geo_skolemization_id,
        schema_geo_skolemization_id_to_geometry,
    )?;

    for (pid, geometry) in pid_to_geometry {
        geometry_builder.push_geometry(Some(&geometry))?;
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
    /// either a file or directory of triples which can optionally be gzipped
    #[argh(option, short = 'i')]
    triples: String,

    /// the output geoparquet file
    #[argh(option)]
    output: String,

    /// log level
    #[argh(option, default = "log::Level::Info")]
    log_level: log::Level,
}


fn open_triples_reader(path: &Path) -> Box<dyn Read> {
    let file = File::open(path).unwrap();

    if path.extension().and_then(|e| e.to_str()) == Some("gz") {
        Box::new(GzDecoder::new(file))
    } else {
        Box::new(file)
    }
}

fn main() {
    let args: TriplesToGeoparquetArgs = argh::from_env();

    env_logger::Builder::new()
        .filter_level(args.log_level.to_level_filter())
        .init();

    let schema = generate_schema();
    let triples_path = Path::new(&args.triples);

    // ONE parquet writer for everything
    let (mut gpq_encoder, mut parquet_writer) = new_parquet_creator(&schema, &args.output);

    // Helper to process a single file and append to writer
    let mut process_file = |path: &Path| {
        info!("Processing {}", path.display());
        let reader = open_triples_reader(path);
        let buf_reader = BufReader::new(reader);

        let arrays = match read_triples_into_arrays(buf_reader) {
            Ok(arrays) => arrays,
            Err(err) => {
                error!("Error reading {}: {}", path.display(), err);
                return;
            }
        };

        let batch = RecordBatch::try_new(Arc::new(schema.clone()), arrays).unwrap();

        let encoded_batch = gpq_encoder.encode_record_batch(&batch).unwrap();
        parquet_writer.write(&encoded_batch).unwrap();
    };

    if triples_path.is_dir() {
        // concatenate all files in directory
        let mut found_data = false;
        for entry in fs::read_dir(triples_path).unwrap() {
            let path = entry.unwrap().path();
            let ends_with_gz_or_nq = path.extension().and_then(|e| e.to_str()) == Some("gz") || path.extension().and_then(|e| e.to_str()) == Some("nq");
            if path.is_file() && ends_with_gz_or_nq {
                process_file(&path);
                found_data = true;
            }
        }
        if !found_data {
            error!("No data found in directory '{}'", triples_path.display());
            return
        }
    } else {
        // single file
        process_file(triples_path);
    }

    // finalize once
    let kv_metadata = gpq_encoder.into_keyvalue().unwrap();
    parquet_writer.append_key_value_metadata(kv_metadata);
    parquet_writer.finish().unwrap();

    info!("Parquet file written to {}", args.output);
}
