// Copyright 2026 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

use std::io::BufRead;

use geo_types::{Geometry, Point};

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
    fn test_simple() {
        let data = "\"1.0\"^^<http://www.w3.org/2001/XMLSchema#double>";
        assert_eq!(f64_from_triple_term(data).unwrap(), 1.0);
    }

    #[test]
    fn test_exponent() {
        let data = "\"1.0E1\"^^<http://www.w3.org/2001/XMLSchema#double>";
        assert_eq!(f64_from_triple_term(data).unwrap(), 10.0);
    }

    #[test]
    fn test_generally_equal() {
        let geom1 = Geometry::Point(Point::new(1.0, 2.0));
        let geom2 = Point::new(1.0, 2.0);
        assert!(generally_equal(&geom1, &geom2));

        let geom1 = Geometry::Point(Point::new(1.0, 2.0));
        let geom2 = Point::new(1.0, 2.00001);
        assert!(generally_equal(&geom1, &geom2));

        let geom1 = Geometry::Point(Point::new(1.0, 2.0));
        let geom2 = Point::new(1.00001, 2.0);
        assert!(generally_equal(&geom1, &geom2));

        let geom1 = Geometry::Point(Point::new(1.0, 2.0));
        let geom2 = Point::new(1.00001, 2.00001);
        assert!(generally_equal(&geom1, &geom2));

        let geom1 = Geometry::Point(Point::new(1.0, 2.0));
        let geom2 = Point::new(1.10, 2.00002);
        assert!(!generally_equal(&geom1, &geom2));
    }
}
