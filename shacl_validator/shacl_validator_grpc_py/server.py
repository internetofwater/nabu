# Copyright 2025 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0

"""gRPC server for SHACL validation service.

This module implements a gRPC server that listens on a Unix domain socket
and provides SHACL validation services.
"""

import logging
import argparse
from concurrent import futures
from pathlib import Path
import grpc
from rdflib import Graph
from shacl_validator_pb2 import JsoldValidationRequest, ValidationReply, LocationOriented

from grpc import ServicerContext

# Import generated protobuf code
import shacl_validator_pb2_grpc

# Import validation logic
from lib import validate_graph

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ShaclValidator(shacl_validator_pb2_grpc.ShaclValidatorServicer):
    def Validate(
        self, request: JsoldValidationRequest, context: ServicerContext
    ) -> ValidationReply:
        
        jsonld = Graph()
        jsonld.parse(data=request.jsonld, format="json-ld")
        conforms, _, text = validate_graph(jsonld, format="location_oriented")

        return ValidationReply(
            valid=conforms,
            message=text,
            ShaclType=LocationOriented,
        )


def serve(socket_path: str = "0.0.0.0:50051"):
    """Start the gRPC server.

    Args:
        socket_path: Path to the Unix domain socket to listen on.
    """
    # Configure the server
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    # Add the Unix domain socket to the server
    success = server.add_insecure_port(socket_path)
    if not success:
        raise Exception(f"Failed to add port to server: {socket_path}")

    shacl_validator_pb2_grpc.add_ShaclValidatorServicer_to_server(
        ShaclValidator(), server
    )

    # Start the server
    server.start()
    logger.info(f"Server started, listening on unix:{socket_path}")

    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        logger.info("Server shutting down...")
        server.stop(0)
        logger.info("Server shut down successfully")


def main():
    """Main entry point for the server."""
    parser = argparse.ArgumentParser(description="SHACL Validation gRPC Server")
    parser.add_argument(
        "--socket",
        type=str,
        default="0.0.0.0:50051",
        help="Path to the grpc socket to listen on",
    )
    parser.add_argument(
        "--shacl",
        type=str,
        default=str((Path(__file__).parent.parent / "shacl_shapes" / "locationOriented.ttl").absolute()),
        help="Path to the shacl file to use for validation",
    )

    args = parser.parse_args()

    logger.info(f"Starting SHACL Validation Server on {args.socket}")
    logger.info(f"SHACL file used for validation: {args.shacl}")
    serve(socket_path=args.socket)


if __name__ == "__main__":
    main()
