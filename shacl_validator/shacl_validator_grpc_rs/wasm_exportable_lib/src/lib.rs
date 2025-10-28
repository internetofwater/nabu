// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

use std::io::Cursor;

use rudof_lib::{
    RDFFormat, ShaclSchemaIR,
    oxrdf::{NamedNode, Term},
    srdf::{self, SRDFGraph},
};
use shacl_validation::validation_report::report::ValidationReport;
use shacl_validation::{
    shacl_processor::{GraphValidation, ShaclProcessor, ShaclValidationMode},
    store::ShaclDataManager,
};
use shacl_validation::{store::graph::Graph, validation_report::result::ValidationResult};
use srdf::{AsyncSRDF, Object};
use wasm_bindgen::prelude::*;

// Generate a report from an error message without needing to
// create a graph
fn new_report_with_error_msg(msg: &str) -> ValidationReport {
    let node = Object::BlankNode(msg.to_string());
    let results = vec![ValidationResult::new(
        node.clone(),
        Object::BlankNode("".to_string()),
        rudof_lib::shacl_ir::severity::CompiledSeverity::Violation,
    )];
    ValidationReport::default().with_results(results)
}

#[wasm_bindgen]
// A web assembly exportable function to validate json-ld
// against the locationOriented schema
pub async fn validate_jsonld_against_geoconnex_schema(jsonld: String) -> String {
    let shacl = include_str!("../../../shapes/geoconnex.ttl");
    let schema = ShaclDataManager::load(Cursor::new(shacl), RDFFormat::Turtle, None);
    match schema {
        Ok(schema) => match validate_jsonld(&schema, jsonld.as_str()).await {
            Ok(report) => report.to_string(),
            Err(err) => err.to_string(),
        },
        Err(err) => err.to_string(),
    }
}

#[wasm_bindgen]
pub async fn get_geoconnex_schema() -> String {
    let shacl = include_str!("../../../shapes/geoconnex.ttl");
    shacl.to_string()
}

pub async fn validate_jsonld(
    schema: &ShaclSchemaIR,
    jsonld: &str,
) -> Result<ValidationReport, Box<dyn std::error::Error>> {
    // we have to drop down to the lower level struct `SRDFGraph` because
    // we need to be able to call `get_subjects_for_object_predicate` on it
    // to validate it is either a Place or a Dataset before running shacl
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
        return Ok(new_report_with_error_msg(
            "Not of '@type':schema:Place nor '@type':schema: Dataset",
        ));
    }

    let data = Graph::from_graph(srdf_graph.clone())?;

    let endpoint_validation = GraphValidation::from_graph(data, ShaclValidationMode::Native);

    let report = endpoint_validation.validate(schema)?;
    Ok(report)
}
