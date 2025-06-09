// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

use shacl_ast::compiled::schema::SchemaIR;
use shacl_validation::shacl_processor::{GraphValidation, ShaclProcessor, ShaclValidationMode};
use shacl_validation::store::graph::Graph;
use shacl_validation::validate_error::ValidateError;
use shacl_validation::validation_report::report::ValidationReport;
use sparql_service::RdfData;
use srdf::{RDFFormat, SRDFGraph};

/// Validate an arbitrary string of rdf triples against a string of shacl shapes.
pub fn validate_triples(
    shacl: &SchemaIR<RdfData>,
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