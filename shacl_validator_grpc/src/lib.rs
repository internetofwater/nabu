// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

use rudof_lib::{
    srdf::{self, SRDFGraph},
    RDFFormat, ShaclSchemaIR,
};
use shacl_validation::shacl_processor::{GraphValidation, ShaclProcessor, ShaclValidationMode};
use shacl_validation::store::graph::Graph;
use shacl_validation::validate_error::ValidateError;
use shacl_validation::validation_report::report::ValidationReport;
use sparql_service::RdfData;

// Dynamically include the proto file using a macro
pub mod shacl_validator {
    tonic::include_proto!("shacl_validator");
}

use std::sync::Arc;

use shacl_validation::store::ShaclDataManager;

use std::io::Cursor;

#[derive(Debug)]
/// An empty struct upon which to implement the necessary traits
/// for our grpc server with tokio and tonic
pub struct Validator {
    pub dataset_schema: Arc<ShaclSchemaIR>,
    pub location_schema: Arc<ShaclSchemaIR>,
}

impl Default for Validator {
    fn default() -> Self {
        // Load the dataset-oriented schema
        let dataset_schema = {
            let shacl = include_str!("../../shacl_shapes/datasetOriented.ttl");
            Arc::new(ShaclDataManager::load(Cursor::new(shacl), RDFFormat::Turtle, None).unwrap())
        };
        let location_schema = {
            let shacl = include_str!("../../shacl_shapes/locationOriented.ttl");
            Arc::new(ShaclDataManager::load(Cursor::new(shacl), RDFFormat::Turtle, None).unwrap())
        };

        Self {
            dataset_schema,
            location_schema,
        }
    }
}

impl Validator {
    /// Validate rdf triples against the location-oriented shacl shape
    pub fn validate_location_oriented(
        &self,
        triples: &str,
    ) -> Result<ValidationReport, ValidateError> {
        validate_jsonld(&self.location_schema, triples)
    }

    /// Validate rdf triples against the dataset-oriented shacl shape
    pub fn validate_dataset_oriented(
        &self,
        triples: &str,
    ) -> Result<ValidationReport, ValidateError> {
        validate_jsonld(&self.dataset_schema, triples)
    }
}

/// Validate an arbitrary string of rdf triples against a string of shacl shapes.
pub fn validate_turtle(
    shacl: &ShaclSchemaIR,
    triples: &str,
) -> Result<ValidationReport, ValidateError> {
    let srdf_graph = SRDFGraph::from_str(
        triples,
        &RDFFormat::Turtle,
        None,
        &srdf::ReaderMode::default(),
    )?;

    let data = RdfData::from_graph(srdf_graph)?;

    let graph = Graph::from_data(data);

    let endpoint_validation = GraphValidation::from_graph(graph, ShaclValidationMode::Native);

    let report = endpoint_validation.validate(shacl)?;
    Ok(report)
}

pub fn validate_n_quads(
    schema: &ShaclSchemaIR,
    quads: &str,
) -> Result<ValidationReport, ValidateError> {
    let srdf_graph = SRDFGraph::from_str(
        quads,
        &RDFFormat::NQuads,
        None,
        &srdf::ReaderMode::default(),
    )?;

    let data = Graph::from_graph(srdf_graph)?;

    let endpoint_validation = GraphValidation::from_graph(data, ShaclValidationMode::Native);
    let report = endpoint_validation.validate(schema)?;
    Ok(report)
}

pub fn validate_jsonld(
    schema: &ShaclSchemaIR,
    jsonld: &str,
) -> Result<ValidationReport, ValidateError> {
    let srdf_graph = SRDFGraph::from_str(
        jsonld,
        &RDFFormat::JsonLd,
        None,
        &srdf::ReaderMode::default(),
    )?;

    let data = Graph::from_graph(srdf_graph)?;

    let endpoint_validation = GraphValidation::from_graph(data, ShaclValidationMode::Native);
    let report = endpoint_validation.validate(schema)?;
    Ok(report)
}

#[cfg(test)]
mod tests {

    use std::io::Cursor;

    use shacl_validation::store::ShaclDataManager;
    use srdf::RDFFormat;

    use crate::validate_jsonld;

    #[test]
    pub fn test_ref_dam() {
        let location_schema = {
            let shacl = include_str!("../../shacl_shapes/locationOriented.ttl");
            ShaclDataManager::load(Cursor::new(shacl), RDFFormat::Turtle, None).unwrap()
        };
        let jsonld = include_str!("testdata/ref_dam.jsonld");
        let report = validate_jsonld(&location_schema, jsonld).unwrap();
        assert!(report.conforms(), "SHACL Validation failed:\n{}", report);
    }

    #[test]
    pub fn test_invalid_ref_dam() {
        let location_schema = {
            let shacl = include_str!("../../shacl_shapes/locationOriented.ttl");
            ShaclDataManager::load(Cursor::new(shacl), RDFFormat::Turtle, None).unwrap()
        };
        let jsonld = include_str!("testdata/invalid/ref_dam.jsonld");
        let report = validate_jsonld(&location_schema, jsonld).unwrap();
        assert!(!report.conforms(), "SHACL Validation unexpectedly succeeded:\n{}", report);
    }
}
