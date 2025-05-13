// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

use tonic::{transport::Server, Request, Response, Status};

use shacl_validator::{
    shacl_validator_server::{ShaclValidator, ShaclValidatorServer}, JsonldValidationRequest, ValidationReply
};

pub mod shacl_validator {
    tonic::include_proto!("shacl_validator");
}

#[derive(Debug, Default)]
pub struct Validator {}

#[tonic::async_trait]
impl ShaclValidator for Validator {
    async fn validate(
        &self,
        request: Request<JsonldValidationRequest>,
    ) -> Result<Response<ValidationReply>, Status> {
        println!("Got a request: {:?}", request);

        let validationReport = validate_wikidata().unwrap();


        let reply = ValidationReply {
            valid: true,
            message: format!("Hello {}!", request.into_inner().jsonld),
        };

        Ok(Response::new(reply))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "[::1]:50051".parse()?;
    let validator = Validator::default();

    Server::builder()
        .add_service(ShaclValidatorServer::new(validator))
        .serve(addr)
        .await?;

    Ok(())
}