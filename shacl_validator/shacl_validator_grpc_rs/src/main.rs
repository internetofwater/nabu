// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

use shacl_validator::shacl_validator_server::{ShaclValidator, ShaclValidatorServer};
use shacl_validator::ValidationReply;
use shacl_validator::{JsoldValidationRequest, MatchingShaclType};
use shacl_validator_grpc::Validator;
use tokio::signal;
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
        request: Request<JsoldValidationRequest>,
    ) -> Result<Response<ValidationReply>, Status> {
        println!("Received request");

        let start = std::time::Instant::now();

        let req = request.into_inner();

        let location_validation_report = self.validate_location_oriented(&req.jsonld).await;

        println!("Validation took {:?}", start.elapsed());

        match location_validation_report {
            // If one report is successful and the other fails, return the successful one
            Ok(report) => {
                let reply = ValidationReply {
                    valid: report.conforms(),
                    message: report.to_string(),
                    shacl_type: Some(MatchingShaclType::DatasetOriented as i32),
                };
                Ok(Response::new(reply))
            }
            // If both reports fail, return both errors
            Err(report) => {
                let msg = format!("Shacl validation error: {:?}", report);
                Err(Status::internal(msg))
            }
        }
    }
}

#[tokio::main(flavor = "multi_thread")] // defaults to number of cpus on the system
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let validator = Validator::default();

    let path = "0.0.0.0:50051";
    let tcp_listener = tokio::net::TcpListener::bind(path).await.unwrap();
    let tcp_stream = tokio_stream::wrappers::TcpListenerStream::new(tcp_listener);

    println!("Starting gRPC server on {}", path);

    // Run the server and listen for Ctrl+C
    let server = Server::builder()
        .add_service(ShaclValidatorServer::new(validator))
        .serve_with_incoming_shutdown(tcp_stream, async {
            signal::ctrl_c()
                .await
                .expect("failed to install Ctrl+C handler");
        });

    // Make sure that the server is ran on the runtime
    tokio::spawn(server).await??;

    println!("Shutting down gRPC server");

    Ok(())
}
