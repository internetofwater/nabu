# Copyright 2025 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0

"""gRPC server for SHACL validation service.

This module implements a gRPC server that listens on a Unix domain socket
and provides SHACL validation services.
"""

import logging
from concurrent import futures
import grpc
from rdflib import Graph
from shacl_validator_pb2 import (
    JsoldValidationRequest,
    ValidationReply,
)

from grpc import ServicerContext

# Import generated protobuf code
import shacl_validator_pb2_grpc

# Import validation logic
from lib import validate_graph

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

MAX_MESSAGE_SIZE = 32 * 1024 * 1024  # 32 MB in bytes

class ShaclValidator(shacl_validator_pb2_grpc.ShaclValidatorServicer):


    def __init__(self, shacl_shape: Graph):
        self.shacl_shape = shacl_shape

    def Validate(
        self, request: JsoldValidationRequest, context: ServicerContext
    ) -> ValidationReply:
        jsonld = Graph()
        jsonld.parse(data=request.jsonld, format="json-ld")
        conforms, _, text = validate_graph(jsonld, shacl_shape=self.shacl_shape)

        return ValidationReply(
            valid=conforms,
            message=text,
        )


def serve(shacl_shape: Graph, socket_path: str = "0.0.0.0:50051"):
    """Start the gRPC server.

    Args:
        socket_path: Path to the Unix domain socket to listen on.
    """
    # Configure the server
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=10),
        options=[
            ("grpc.max_receive_message_length", MAX_MESSAGE_SIZE),
        ],
    )
    # Add the Unix domain socket to the server
    success = server.add_insecure_port(socket_path)
    if not success:
        raise Exception(f"Failed to add port to server: {socket_path}")

    shacl_validator_pb2_grpc.add_ShaclValidatorServicer_to_server(
        ShaclValidator(shacl_shape), server
    )

    # Start the server
    server.start()
    logger.info(f"Server started, listening on {socket_path}")

    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        logger.info("Server shutting down...")
        server.stop(0)
        logger.info("Server shut down successfully")
