// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

use shacl_ast::compiled::schema::SchemaIR;
use shacl_validation::shacl_processor::{GraphValidation, ShaclProcessor, ShaclValidationMode};
use shacl_validation::store::graph::Graph;
use shacl_validation::validate_error::ValidateError;
use shacl_validation::validation_report::report::ValidationReport;
use sparql_service::RdfData;
use srdf::{RDFFormat, SRDFGraph};

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
    pub dataset_schema: Arc<SchemaIR<RdfData>>,
    pub location_schema: Arc<SchemaIR<RdfData>>,
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
        validate_n_quads(&self.location_schema, triples)
    }

    /// Validate rdf triples against the dataset-oriented shacl shape
    pub fn validate_dataset_oriented(
        &self,
        triples: &str,
    ) -> Result<ValidationReport, ValidateError> {
        validate_n_quads(&self.dataset_schema, triples)
    }
}

/// Validate an arbitrary string of rdf triples against a string of shacl shapes.
pub fn validate_turtle(
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

pub fn validate_n_quads(schema: &SchemaIR<RdfData>, quads: &str) -> Result<ValidationReport, ValidateError> {

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

#[cfg(test)]
mod tests {

    use std::io::Cursor;

    use shacl_validation::store::ShaclDataManager;
    use srdf::RDFFormat;

    use crate::validate_n_quads;
    use crate::validate_turtle;

    #[test]
    fn test_empty() {
        let schema = ShaclDataManager::load(Cursor::new(""), RDFFormat::Turtle, None).unwrap();
        let result = validate_turtle(&schema, "");
        assert!(result.is_ok());
    }

    #[test]
    fn test_valid_triple() {
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
        let turtle = r#"
                @prefix ex: <http://example.org/> .
                @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

                ex:alice a ex:Person ;
                    ex:name "Alice"^^xsd:string .
            "#;

        let schema = ShaclDataManager::load(Cursor::new(shacl), RDFFormat::Turtle, None).unwrap();

        let result = validate_turtle(&schema, turtle);
        assert!(result.is_ok(), "Validation should succeed for valid data");
        let report = result.unwrap();
        assert!(report.conforms(), "Report should indicate conformance");
    }

    #[test]
    fn test_full_location_oriented() {
        // Minimal valid RDF data for the locationOriented.ttl SHACL shape
        let quads = include_str!("testdata/fullLocationOrientedExample.nq");

        let validator = crate::Validator::default();

        let result = validate_n_quads(&validator.location_schema, quads);
        let report = result.unwrap();
        assert!(
            report.conforms(),
            "Report should indicate non conformance for invalid location-oriented data"
        );
    }

    #[test]
    fn test_bad_full_location_oriented_quad() {
        // Minimal valid RDF data for the locationOriented.ttl SHACL shape
        let quads = include_str!("testdata/fullLocationOrientedInvalidExample.nq");

        let validator = crate::Validator::default();

        let result = validate_n_quads(&validator.location_schema, quads);
        assert!(
            result.is_ok(),
            "Validation should succeed for valid location-oriented data"
        );
        let report = result.unwrap();
        assert!(
            !report.conforms(),
            "Report should indicate conformance for valid location-oriented data"
        );
        println!("Report: {}", report.to_string());
    }

    #[test]
    fn test_minimal_location_oriented() {
        // Minimal valid RDF data for the locationOriented.ttl SHACL shape
        let triples = include_str!("testdata/minimalLocationOrientedExample.ttl");

        let validator = crate::Validator::default();

        let result = validator.validate_location_oriented(triples);
        assert!(
            result.is_ok(),
            "Validation should succeed for valid location-oriented data"
        );
        let report = result.unwrap();
        assert!(
            report.conforms(),
            "Report should indicate conformance for valid location-oriented data"
        );
    }

    #[test]
    fn test_empty_turtle() {
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
        let turtle = "";

        let schema = ShaclDataManager::load(Cursor::new(shacl), RDFFormat::Turtle, None).unwrap();
        let result = validate_turtle(&schema, turtle);
        assert!(
            result.is_ok(),
            "Parsing should succeed even with invalid data"
        );
        let report = result.unwrap();
        // this is confusing but appears to be correct vv
        assert!(report.conforms(), "An empty triple has no nodes to validate and thus is valid");
    }

    #[test]
    fn test_invalid_turtle() {
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

        // Valid turtle: ex:alice is a ex:Person and has an ex:name "Alice"
        let turtle = r#"
            @prefix ex: <http://example.org/> .
            @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

            ex:alice a ex:Person ;
                ex:invalidDummyProperty "Alice"^^xsd:string .
        "#;

        let schema = ShaclDataManager::load(Cursor::new(shacl), RDFFormat::Turtle, None).unwrap();
        let result = validate_turtle(&schema, turtle);
        assert!(
            result.is_ok(),
            "Parsing should succeed even with invalid data"
        );
        let report = result.unwrap();
        assert!(!report.conforms(), "Report should indicate non-conformance");
    }
}
