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
            let ends_with_gz_or_nq = path.extension().and_then(|e| e.to_str()) == Some("gz")
                || path.extension().and_then(|e| e.to_str()) == Some("nq");
            if path.is_file() && ends_with_gz_or_nq {
                process_file(&path);
                found_data = true;
            }
        }
        if !found_data {
            error!("No data found in directory '{}'", triples_path.display());
            return;
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
