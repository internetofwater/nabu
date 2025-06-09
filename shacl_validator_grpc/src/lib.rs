// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

use shacl_ast::compiled::schema::SchemaIR;
use shacl_validation::validate_error::ValidateError;
use shacl_validation::validation_report::report::ValidationReport;
use sparql_service::RdfData;
use srdf::{RDFFormat};

// Dynamically include the proto file using a macro
pub mod shacl_validator {
    tonic::include_proto!("shacl_validator");
}

use std::sync::Arc;
use shacl_validation::store::ShaclDataManager;
use std::io::Cursor;

use crate::validation::validate_triples;

pub mod validation;


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
            let shacl = include_str!("../shapes/datasetOriented.ttl");
            Arc::new(ShaclDataManager::load(Cursor::new(shacl), RDFFormat::Turtle, None).unwrap())
        };
        let location_schema = {
            let shacl = include_str!("../shapes/locationOriented.ttl");
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
        validate_triples(&self.location_schema, triples)
    }

    /// Validate rdf triples against the dataset-oriented shacl shape
    pub fn validate_dataset_oriented(
        &self,
        triples: &str,
    ) -> Result<ValidationReport, ValidateError> {
        validate_triples(&self.dataset_schema, triples)
    }
}



#[cfg(test)]
mod tests {

    use std::io::Cursor;
    use shacl_validation::store::ShaclDataManager;
    use srdf::RDFFormat;
    use crate::validation::validate_triples;

    #[test]
    fn test_empty() {
        let schema = ShaclDataManager::load(Cursor::new(""), RDFFormat::Turtle, None).unwrap();
        let result = validate_triples(&schema, "");
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
        let triples = r#"
                @prefix ex: <http://example.org/> .
                @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

                ex:alice a ex:Person ;
                    ex:name "Alice"^^xsd:string .
            "#;

        let schema = ShaclDataManager::load(Cursor::new(shacl), RDFFormat::Turtle, None).unwrap();

        let result = validate_triples(&schema, triples);
        assert!(result.is_ok(), "Validation should succeed for valid data");
        let report = result.unwrap();
        assert!(report.conforms(), "Report should indicate conformance");
    }

    #[test]
    fn test_location_oriented() {
        // Minimal valid RDF data for the locationOriented.ttl SHACL shape
        let triples = include_str!("testdata/locationOrientedExample.ttl");

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
    fn test_invalid_location_oriented() {
        // Minimal valid RDF data for the locationOriented.ttl SHACL shape
        let triples = include_str!("testdata/locationOrientedInvalidExample.ttl");

        let validator = crate::Validator::default();

        let result = validator.validate_location_oriented(triples);
        assert!(
            result.is_ok(),
            "Validation should succeed for valid location-oriented data"
        );
        let report = result.unwrap();
        assert!(
            !report.conforms(),
            "Report should indicate non conformance for invalid location-oriented data"
        );
    }


    #[test]
    fn test_empty_triple() {
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
        let triples = "";

        let schema = ShaclDataManager::load(Cursor::new(shacl), RDFFormat::Turtle, None).unwrap();
        let result = validate_triples(&schema, triples);
        assert!(
            result.is_ok(),
            "Parsing should succeed even with invalid data"
        );
        let report = result.unwrap();
        // this is confusing but appears to be correct vv
        assert!(report.conforms(), "An empty triple has no nodes to validate and thus is valid");
    }

    #[test]
    fn test_invalid_triple() {
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

        let schema = ShaclDataManager::load(Cursor::new(shacl), RDFFormat::Turtle, None).unwrap();
        let result = validate_triples(&schema, triples);
        assert!(
            result.is_ok(),
            "Parsing should succeed even with invalid data"
        );
        let report = result.unwrap();
        assert!(!report.conforms(), "Report should indicate non-conformance");
    }
}
