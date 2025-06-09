// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

use std::{fs, path::Path};

use shacl_validator::shacl_validator_server::{ShaclValidator, ShaclValidatorServer};
use shacl_validator::ValidationReply;
use shacl_validator::{
    MatchingShaclType, TurtleValidationRequest,
};
use shacl_validator_grpc::Validator;
use tokio::net::UnixListener;
use tokio::signal;
use tokio_stream::wrappers::UnixListenerStream;
use tonic::transport::Server;
use tonic::{Request, Response, Status};


// Dynamically include the proto file using a macro
pub mod shacl_validator {
    tonic::include_proto!("shacl_validator");
}

#[tonic::async_trait]
impl ShaclValidator for Validator {
    /// Validates the triples in the request using both dataset-oriented and location-oriented validation.
    /// Returns a ValidationReply with the validation result.
    async fn validate(
        &self,
        request: Request<TurtleValidationRequest>,
    ) -> Result<Response<ValidationReply>, Status> {
        println!("Received request");

        let start = std::time::Instant::now();

        let req = request.into_inner();

        let dataset_validation_report = self.validate_dataset_oriented(&req.triples);
        let location_validation_report = self.validate_location_oriented(&req.triples);

        println!("Validation took {:?}", start.elapsed());

        match (dataset_validation_report, location_validation_report) {
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
                                report1, report2
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

#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
#[tokio::main(flavor = "multi_thread")] // defaults to number of cpus on the system
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = "/tmp/shacl_validator.sock";

    // Remove the socket file if it already exists
    if Path::new(path).exists() {
        fs::remove_file(path)?;
    }

    std::fs::create_dir_all(Path::new(path).parent().unwrap())?;

    let uds = UnixListener::bind(path)?;
    let uds_stream = UnixListenerStream::new(uds);

    let validator = Validator::default();

    println!("Starting gRPC server on {}", path);

    // Run the server and listen for Ctrl+C
    let server = Server::builder()
        .add_service(ShaclValidatorServer::new(validator))
        .serve_with_incoming_shutdown(uds_stream, async {
            signal::ctrl_c()
                .await
                .expect("failed to install Ctrl+C handler");
        });

    // Make sure that the server is ran on the runtime
    let result = tokio::spawn(server).await?;

    // Clean up the socket file on shutdown
    if Path::new(path).exists() {
        println!("Cleaning up socket file at {}", path);
        fs::remove_file(path)?;
    }
    result?;
    Ok(())
}
