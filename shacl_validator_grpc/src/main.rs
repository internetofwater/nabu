// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

use shacl_validator_grpc::{validate_dataset_oriented, validate_location_oriented};
use tonic::{transport::Server, Request, Response, Status};

use shacl_validator::{
    shacl_validator_server::{ShaclValidator, ShaclValidatorServer},
    TurtleValidationRequest, ValidationReply, MatchingShaclType
};

// Dynamically include the proto file using a macro
pub mod shacl_validator {
    tonic::include_proto!("shacl_validator");
}

#[derive(Debug, Default)]
/// An empty struct upon which to implement the necessary traits
/// for our grpc server with tokio and tonic
pub struct Validator {}

#[tonic::async_trait]
impl ShaclValidator for Validator {
    /// Validates the triples in the request using both dataset-oriented and location-oriented validation.
    /// Returns a ValidationReply with the validation result.
    async fn validate(
        &self,
        request: Request<TurtleValidationRequest>,
    ) -> Result<Response<ValidationReply>, Status> {
        let req = request.into_inner();
        let dataset_validation_report = validate_dataset_oriented(&req.triples);
        let location_validation_report = validate_location_oriented(&req.triples);

        let (dataset_result, location_result) = tokio::join!(dataset_validation_report, location_validation_report);

        match (dataset_result, location_result) {
            // If one report is successful and the other fails, return the successful one
            (Ok(report), Err(_)) => {
                let reply = ValidationReply {
                    valid: report.conforms(),
                    message: report.to_string(),
                    shacl_type: Some(MatchingShaclType::DatasetOriented as i32),
                };
                Ok(Response::new(reply))
            }
            (Err(_), Ok(report)) => {
                let reply = ValidationReply {
                    valid: report.conforms(),
                    message: report.to_string(),
                    shacl_type: Some(MatchingShaclType::LocationOriented as i32),
                };
                Ok(Response::new(reply))
            }
            // if both reports are successful, return the one that conforms
            (Ok(report1), Ok(report2)) => {
                match (report1.conforms(), report2.conforms()) {
                    (true, _) => {
                        let reply = ValidationReply {
                            valid: true,
                            message: report1.to_string(),
                            shacl_type: Some(MatchingShaclType::DatasetOriented as i32),
                        };
                        Ok(Response::new(reply))
                    }
                    (_, true) => {
                        let reply = ValidationReply {
                            valid: true,
                            message: report2.to_string(),
                            shacl_type: Some(MatchingShaclType::LocationOriented as i32),
                        };
                        Ok(Response::new(reply))
                    }
                    // if neither conform, return both reports so 
                    // the user can see what went wrong
                    (false, false) => {
                        let reply = ValidationReply {
                            valid: false,
                            message: format!(
                                "Dataset validation report:\n{}\n\nLocation validation report:\n{}",
                                report1.to_string(),
                                report2.to_string()
                            ),
                            shacl_type: None,
                        };
                        Ok(Response::new(reply))
                    }
                }
            }
            // If both reports fail, return both errors
            (Err(e1), Err(e2)) => {
                let msg = format!(
                    "Dataset validation error: {:?}; Location validation error: {:?}",
                    e1, e2
                );
                Err(Status::internal(msg))
            }
        }
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
