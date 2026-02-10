// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

use std::{
    fs::{self, File},
    io::{BufRead, Read},
    sync::Arc,
};

use flate2::read::GzDecoder;
use log::{error, info};

use argh::FromArgs;
use arrow_array::{self, ArrayRef, RecordBatch, builder::StringBuilder};
use geoarrow_array::{GeoArrowArray, builder::GeometryBuilder};
use geoarrow_schema::GeometryType;
use triples_to_geoparquet::{
    parquet_lib::{generate_schema, new_parquet_creator},
    triples_lib::{combine_geometry_representations, read_triples_into_maps},
};

use std::io::BufReader;
use std::path::Path;

/// Given a reader of triples, read them into arrow arrays
fn read_triples_into_arrays<R: BufRead>(
    triples_reader: R,
) -> Result<Vec<ArrayRef>, Box<dyn std::error::Error>> {
    let hashmaps = read_triples_into_maps(triples_reader)?;

    let pid_to_geometry = combine_geometry_representations(hashmaps)?;

    let mut string_builder = StringBuilder::new();
    let mut geometry_builder = GeometryBuilder::new(GeometryType::default());

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
        let mut found_data = false;

        // convert files to a vector so we know what index
        // we are for progress logging
        let all_files: Vec<_> = fs::read_dir(triples_path)
            .unwrap()
            .map(|res| res.unwrap())
            .collect();

        for (index, entry) in all_files.iter().enumerate()  {
            let path = entry.path();
            let ends_with_gz_or_nq = path.extension().and_then(|e| e.to_str()) == Some("gz")
                || path.extension().and_then(|e| e.to_str()) == Some("nq");
            if path.is_file() && ends_with_gz_or_nq {
                info!("Processing {}, {}/{}", path.display(), index+1, all_files.len());
                found_data = true;
                process_file(&path);
            }
        }
        if !found_data {
            error!("No data found in directory '{}'", triples_path.display());
            return;
        }
    } else {
        // single file
        info!("Processing {}", triples_path.display());
        process_file(triples_path);
    }

    // finalize once
    let kv_metadata = gpq_encoder.into_keyvalue().unwrap();
    parquet_writer.append_key_value_metadata(kv_metadata);
    parquet_writer.finish().unwrap();

    info!("Finished converting to geoparquet. File written to {}", args.output);
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use crate::read_triples_into_arrays;

    #[test]
    fn test_read_triples_into_arrays() {
        let nquads = r#"<http://example.org/feature/1> <http://www.opengis.net/ont/geosparql#hasGeometry> _:geom1 .
        _:geom1 <http://www.opengis.net/ont/geosparql#asWKT> "POINT (1 2)"^^<http://www.opengis.net/ont/geosparql#wktLiteral> ."#;

        let reader = Cursor::new(nquads);

        let arrays =
            read_triples_into_arrays(reader).expect("Expected triples to be parsed successfully");

        assert_eq!(arrays.len(), 2, "Expected two columns, geometry and id");

        let geometry_array = &arrays[0];
        let id_array = &arrays[1];

        assert_eq!(geometry_array.len(), 1);
        assert_eq!(id_array.len(), 1);

        let id_array = id_array
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .expect("ID column should be a StringArray");

        assert_eq!(id_array.value(0), "<http://example.org/feature/1>");
    }

    #[test]
    fn test_invalid_wkt() {
        let nquads = r#"<http://example.org/feature/1> <http://www.opengis.net/ont/geosparql#hasGeometry> _:geom1 .
        _:geom1 <http://www.opengis.net/ont/geosparql#asWKT> "POINT (1)"<http://www.opengis.net/ont/geosparql#wktLiteral> ."#;

        let reader = Cursor::new(nquads);

        let arrays = read_triples_into_arrays(reader);

        assert!(arrays.is_err());
    }

    #[test]
    fn test_triples_with_both_gsp_and_schema_geo() {
        let nquads = r#"<http://example.org/feature/1> <http://www.opengis.net/ont/geosparql#hasGeometry> _:geom1 .
        _:geom1 <http://www.opengis.net/ont/geosparql#asWKT> "POINT (2 1)"^^<http://www.opengis.net/ont/geosparql#wktLiteral> .
        <http://example.org/feature/1> <https://schema.org/geo> _:schema1 .
        _:schema1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <https://schema.org/GeoCoordinates> .
        _:schema1 <https://schema.org/latitude> "1.0"^^<http://www.w3.org/2001/XMLSchema#double> .
        _:schema1 <https://schema.org/longitude> "2.0"^^<http://www.w3.org/2001/XMLSchema#double> .
        "#;

        let reader = Cursor::new(nquads);

        let arrays =
            read_triples_into_arrays(reader).expect("Expected triples to be parsed successfully");

        assert_eq!(arrays.len(), 2, "Expected two columns, geometry and id");

        let geometry_array = &arrays[0];

        assert_eq!(geometry_array.len(), 1);
    }

    #[test]
    fn ensure_failure_if_gsp_doesnt_match_schema_geo() {
        let nquads = r#"<http://example.org/feature/1> <http://www.opengis.net/ont/geosparql#hasGeometry> _:geom1 .
        _:geom1 <http://www.opengis.net/ont/geosparql#asWKT> "POINT (2 1)"^^<http://www.opengis.net/ont/geosparql#wktLiteral> .
        <http://example.org/feature/1> <https://schema.org/geo> _:schema1 .
        _:schema1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <https://schema.org/GeoCoordinates> .
        _:schema1 <https://schema.org/latitude> "9999999999.0"^^<http://www.w3.org/2001/XMLSchema#double> .
        _:schema1 <https://schema.org/longitude> "2.0"^^<http://www.w3.org/2001/XMLSchema#double> .
        "#;

        let reader = Cursor::new(nquads);

        let arrays = read_triples_into_arrays(reader);

        let err_msg = arrays.unwrap_err().to_string();
        assert_eq!(
            err_msg.contains("does not match schema geo skolemization id"),
            true,
            "{}",
            err_msg
        );
    }
}
