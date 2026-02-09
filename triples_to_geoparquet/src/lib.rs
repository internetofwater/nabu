// Copyright 2026 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::HashMap,
    fs::{self, File},
    io::{BufRead, Read},
    sync::Arc,
};

use flate2::read::GzDecoder;
use log::{debug, error, info};
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

use std::io::BufReader;
use std::path::Path;

pub mod parquet_lib;

/// Given a triple term, return the value as a f64
pub fn f64_from_triple_term(data: &str) -> Result<f64, Box<dyn std::error::Error>> {
    let literal = data.split("^^").next().unwrap().trim_matches('"');

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

/// Check if two geometries are equal with some tolerance (for floating point errors, etc)
pub fn generally_equal(geom1: &Geometry, geom2: &Point) -> bool {
    match geom1 {
        Geometry::Point(point) => {
            let x_equal = (geom2.x() - point.x()).abs() < 0.001;
            let y_equal = (geom2.y() - point.y()).abs() < 0.001;
            x_equal && y_equal
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simple() {
        let data = "\"1.0\"^^<http://www.w3.org/2001/XMLSchema#double>";
        assert_eq!(f64_from_triple_term(data).unwrap(), 1.0);
    }

    #[test]
    fn exponent() {
        let data = "\"1.0E1\"^^<http://www.w3.org/2001/XMLSchema#double>";
        assert_eq!(f64_from_triple_term(data).unwrap(), 10.0);
    }
}
