// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

use shacl_validation::store::ShaclDataManager;
use srdf::{RDFFormat};
use wasm_bindgen::prelude::*;
use std::io::Cursor;

pub mod validation;

#[wasm_bindgen]
pub fn validate_triples_with_raw_shacl(shacl: &str, triples: &str) -> Result<String, String> {
    match ShaclDataManager::load(Cursor::new(shacl), RDFFormat::Turtle, None) {
        Ok(schema) => match validation::validate_triples(&schema, triples) {
            Ok(report) => Ok(report.to_string()),
            Err(err) => Err(err.to_string()),
        },
        Err(err) => Err(err.to_string()),
    }
}

