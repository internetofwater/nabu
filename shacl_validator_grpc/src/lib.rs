// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

use shacl_validation::shacl_processor::{GraphValidation, ShaclProcessor, ShaclValidationMode};
use shacl_validation::store::graph::Graph;
use shacl_validation::store::ShaclDataManager;
use shacl_validation::validate_error::ValidateError;
use shacl_validation::validation_report::report::ValidationReport;
use srdf::{RDFFormat, SRDFGraph};
use std::io::Cursor;
use std::os::fd::FromRawFd;
use sparql_service::RdfData;
use json_ld::{JsonLdProcessor, Options, RemoteDocument, syntax::{Value, Parse}};
use json_ld::loader::ReqwestLoader;

pub async fn validate_jsonld(shacl: &str, jsonld: &str) -> Result<ValidationReport, ValidateError> {

    let mut generator = rdf_types::generator::Blank::new();

    // Create a "remote" document by parsing a file manually.
    let input = RemoteDocument::new(
        None,
        Some("application/ld+json".parse().unwrap()),
        Value::parse_str(jsonld).expect("unable to parse file").0
    );


    let loader = ReqwestLoader::new();
    let options = Options::default();

    let mut rdf = input
        .to_rdf(
            &mut generator,
            &loader
        )
        .await
        .expect("flattening failed");

    let schema = ShaclDataManager::load(Cursor::new(shacl), RDFFormat::Turtle, None)?;

    rdf.document().to_

    let validator = GraphValidation::from_graph(Graph::from_data(RdfData::from_graph(SRDFGraph::from_str(data, format, base, reader_mode))));

    let endpoint_validation = GraphValidation::from_graph(
        Graph::from_data(Cursor::new(jsonld), RDFFormat::JsonLd, None)
            .expect("Failed to create a graph from the provided JSON-LD data"),
    );

    let report = endpoint_validation.validate(&schema)?;
    Ok(report)
}

mod tests {

    #[cfg(test)]
    mod tests {

        use crate::validate_jsonld;

        #[test]
        fn test_validate_wikidata() {
            let result = validate_jsonld();
            assert!(!result.is_err());
            let report = result.unwrap();
            assert!(report.conforms());
        }
    }
}
