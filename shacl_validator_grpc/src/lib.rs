// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

use shacl_validation::shacl_processor::{GraphValidation, ShaclProcessor, ShaclValidationMode};
use shacl_validation::store::graph::Graph;
use shacl_validation::store::ShaclDataManager;
use shacl_validation::validate_error::ValidateError;
use shacl_validation::validation_report::report::ValidationReport;
use sparql_service::RdfData;
use srdf::{RDFFormat, SRDFGraph};
use std::io::Cursor;


/// Validate rdf triples against the location-oriented shacl shape
pub async fn validate_location_oriented(
    triples: &str,
) -> Result<ValidationReport, ValidateError> {
    let shacl = include_str!("../shapes/locationOriented.ttl");
    validate_triples(shacl, triples).await
}


/// Validate rdf triples against the dataset-oriented shacl shape
pub async fn validate_dataset_oriented(
    triples: &str,
) -> Result<ValidationReport, ValidateError> {
    let shacl = include_str!("../shapes/datasetOriented.ttl");
    validate_triples(shacl, triples).await
}

/// Validate an arbitrary string of rdf triples against a string of shacl shapes.
pub async fn validate_triples(
    shacl: &str,
    triples: &str,
) -> Result<ValidationReport, ValidateError> {
    if shacl.trim().is_empty() || triples.trim().is_empty() {
        return Err(ValidateError::TargetNodeBlankNode);
    }

    let schema = ShaclDataManager::load(Cursor::new(shacl), RDFFormat::Turtle, None)?;

    let srdf_graph = SRDFGraph::from_str(
        triples,
        &RDFFormat::Turtle,
        None,
        &srdf::ReaderMode::default(),
    )?;

    let data = RdfData::from_graph(srdf_graph)?;

    let graph = Graph::from_data(data);

    let endpoint_validation = GraphValidation::from_graph(graph, ShaclValidationMode::Native);

    let report = endpoint_validation.validate(&schema)?;
    Ok(report)
}

mod tests {

    #[cfg(test)]
    mod tests {

        use crate::{validate_triples, validate_location_oriented};

        #[tokio::test]
        async fn test_empty() {
            let result = validate_triples("", "").await;
            assert!(result.is_err());
        }

        #[tokio::test]
        async fn test_valid_triple() {
            // Minimal SHACL shape: ex:Person must have an ex:name property of type xsd:string
            let shacl = r#"
                @prefix sh: <http://www.w3.org/ns/shacl#> .
                @prefix ex: <http://example.org/> .
                @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

                ex:PersonShape
                    a sh:NodeShape ;
                    sh:targetClass ex:Person ;
                    sh:property [
                        sh:path ex:name ;
                        sh:datatype xsd:string ;
                    ] .
            "#;

            // Valid triple: ex:alice is a ex:Person and has an ex:name "Alice"
            let triples = r#"
                @prefix ex: <http://example.org/> .
                @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

                ex:alice a ex:Person ;
                    ex:name "Alice"^^xsd:string .
            "#;

            let result = validate_triples(shacl, triples).await;
            assert!(result.is_ok(), "Validation should succeed for valid data");
            let report = result.unwrap();
            assert!(report.conforms(), "Report should indicate conformance");
        }

        #[tokio::test]
        async fn test_location_oriented() {
            // Minimal valid RDF data for the locationOriented.ttl SHACL shape
            let triples = include_str!("testdata/locationOrientedExample.ttl");

            let result = crate::validate_location_oriented(triples).await;
            assert!(result.is_ok(), "Validation should succeed for valid location-oriented data");
            let report = result.unwrap();
            assert!(report.conforms(), "Report should indicate conformance for valid location-oriented data");
        }

        #[tokio::test]
        async fn test_invalid_location_oriented() {
            // Minimal valid RDF data for the locationOriented.ttl SHACL shape
            let triples = include_str!("testdata/locationOrientedInvalidExample.ttl");

            let result = validate_location_oriented(triples).await;
            assert!(result.is_ok(), "Validation should succeed for valid location-oriented data");
            let report = result.unwrap();
            assert!(!report.conforms(), "Report should indicate non conformance for invalid location-oriented data");
        }
        
        #[tokio::test]
        async fn test_invalid_triple() {
            // Minimal SHACL shape: ex:Person must have an ex:name property of type xsd:string
            let shacl = r#"
            @prefix sh: <http://www.w3.org/ns/shacl#> .
            @prefix ex: <http://example.org/> .
            @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

            ex:PersonShape
                a sh:NodeShape ;
                sh:targetClass ex:Person ;
                sh:property [
                    sh:path ex:name ;
                    sh:datatype xsd:string ;
                    sh:minCount 1 ;
                ] .
        "#;

            // Valid triple: ex:alice is a ex:Person and has an ex:name "Alice"
            let triples = r#"
            @prefix ex: <http://example.org/> .
            @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

            ex:alice a ex:Person ;
                ex:invalidDummyProperty "Alice"^^xsd:string .
        "#;

            let result = validate_triples(shacl, triples).await;
            assert!(result.is_ok(), "Parsing should succeed even with invalid data");
            let report = result.unwrap();
            assert!(!report.conforms(), "Report should indicate non-conformance");
        }
    }

}
