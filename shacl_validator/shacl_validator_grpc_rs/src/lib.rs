// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

use rudof_lib::{
    oxrdf::{NamedNode, Term},
    srdf::{self, SRDFGraph},
    RDFFormat, ShaclSchemaIR,
};
use shacl_validation::store::graph::Graph;
use shacl_validation::validate_error::ValidateError;
use shacl_validation::validation_report::report::ValidationReport;
use shacl_validation::{
    shacl_processor::{GraphValidation, ShaclProcessor, ShaclValidationMode},
    validation_report::result::ValidationResult,
};
use sparql_service::RdfData;
use srdf::{AsyncSRDF, Object};

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
    pub location_schema: Arc<ShaclSchemaIR>,
}

impl Default for Validator {
    fn default() -> Self {
        let shacl_schema = {
            let shacl = include_str!("../../shapes/locationOriented.ttl");
            Arc::new(ShaclDataManager::load(Cursor::new(shacl), RDFFormat::Turtle, None).unwrap())
        };

        Self {
            location_schema: shacl_schema,
        }
    }
}

impl Validator {
    /// Validate rdf triples against the location-oriented shacl shape
    pub async fn validate_location_oriented(
        &self,
        triples: &str,
    ) -> Result<ValidationReport, ValidateError> {
        validate_jsonld(&self.location_schema, triples).await
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

/// Create a validation report with a single error
fn new_report_with_error_msg(msg: &str) -> ValidationReport {
    let node = Object::BlankNode(msg.to_string());
    let results = vec![ValidationResult::new(
        node.clone(),
        node.clone(),
        node.clone(),
    )];
    return ValidationReport::default().with_results(results);
}

pub async fn validate_jsonld(
    schema: &ShaclSchemaIR,
    jsonld: &str,
) -> Result<ValidationReport, ValidateError> {
    let srdf_graph = SRDFGraph::from_str(
        jsonld,
        &RDFFormat::JsonLd,
        None,
        &srdf::ReaderMode::default(),
    )?;

    let rdf_type = NamedNode::new("http://www.w3.org/1999/02/22-rdf-syntax-ns#type").unwrap();
    let place_iri = Term::NamedNode(NamedNode::new("https://schema.org/Place").unwrap());
    let dataset_iri = Term::NamedNode(NamedNode::new("https://schema.org/Dataset").unwrap());

    let not_type_place = srdf_graph
        .get_subjects_for_object_predicate(&place_iri, &rdf_type)
        .await?
        .is_empty();

    let not_type_dataset = srdf_graph
        .get_subjects_for_object_predicate(&dataset_iri, &rdf_type)
        .await?
        .is_empty();

    if not_type_dataset && not_type_place {
        return Ok(new_report_with_error_msg("Not of '@type':schema:Place nor '@type':schema: Dataset"));
    }

    let data = Graph::from_graph(srdf_graph.clone())?;

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

    #[tokio::test]
    pub async fn test_all_valid_cases() {
        let location_schema = {
            let shacl = include_str!("../../shapes/locationOriented.ttl");
            ShaclDataManager::load(Cursor::new(shacl), RDFFormat::Turtle, None).unwrap()
        };
        let valid_dir = "../testdata/valid/";
        for file_result in std::fs::read_dir(valid_dir).unwrap() {
            let file = file_result.unwrap();

            let path = file.path();
            let filename = path.display().to_string();

            let jsonld = std::fs::read_to_string(&path).unwrap();
            let report = validate_jsonld(&location_schema, &jsonld).await.unwrap();
            assert!(
                report.conforms(),
                "SHACL Validation unexpectedly failed: {}\n{}",
                filename.clone(),
                report
            );
        }
    }

    #[tokio::test]
    pub async fn test_invalid_case() {
        let location_schema = {
            let shacl = include_str!("../../shapes/locationOriented.ttl");
            ShaclDataManager::load(Cursor::new(shacl), RDFFormat::Turtle, None).unwrap()
        };
        let invalid_dir = "../testdata/invalid/";
        for file_result in std::fs::read_dir(invalid_dir).unwrap() {
            let file = file_result.unwrap();

            let path = file.path();
            let filename = path.display().to_string();

            let jsonld = std::fs::read_to_string(&path).unwrap();
            let report = validate_jsonld(&location_schema, &jsonld).await.unwrap();

            assert!(
                !report.conforms(),
                "SHACL Validation unexpectedly passed: {}\n",
                filename.clone()
            );
        }
    }
}
