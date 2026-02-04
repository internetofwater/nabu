# Copyright 2025 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0

"""gRPC + HTTP server for SHACL validation service (Starlette version)."""

import logging
import threading
from concurrent import futures

import grpc
from rdflib import Graph
from starlette.applications import Starlette
from starlette.responses import JSONResponse, Response
from starlette.requests import Request
from starlette.routing import Route
from starlette.middleware.cors import CORSMiddleware
from starlette.middleware import Middleware
import uvicorn

from mainstems import get_mainstem, initialize_duckdb
from shacl_validator_pb2 import JsoldValidationRequest, ValidationReply
import shacl_validator_pb2_grpc
from grpc import ServicerContext
from lib import validate_graph

# Configure logging
logger = logging.getLogger("uvicorn.error")

MAX_MESSAGE_SIZE = 32 * 1024 * 1024  # 32 MB


class ShaclValidator(shacl_validator_pb2_grpc.ShaclValidatorServicer):
    def __init__(self, shacl_shape: Graph):
        self.shacl_shape = shacl_shape

    def Validate(
        self, request: JsoldValidationRequest, context: ServicerContext
    ) -> ValidationReply:
        jsonld = Graph()
        jsonld.parse(data=request.jsonld, format="json-ld")
        try:
            conforms, _, text = validate_graph(jsonld, shacl_shape=self.shacl_shape)
        except KeyError as e:
            # https://github.com/RDFLib/pySHACL/issues/314 handle weird race condition error which raises a key error
            logger.error(f"Failed when validating due to error '{e}' with data: {request.jsonld}")
            conforms, text = False, f"Failed when validating due to internal SHACL library error '{e}'"
        return ValidationReply(valid=conforms, message=text)


def serve_grpc(shacl_shape: Graph, grpc_port: int):
    """Start gRPC server."""
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=10),
        options=[("grpc.max_receive_message_length", MAX_MESSAGE_SIZE)],
    )
    shacl_validator_pb2_grpc.add_ShaclValidatorServicer_to_server(
        ShaclValidator(shacl_shape), server
    )
    address = f"0.0.0.0:{grpc_port}"
    server.add_insecure_port(address)
    server.start()
    logger.info(
        f"gRPC server started on {address}; view protobuf file for service definition"
    )
    server.wait_for_termination()


async def validate_http(request: Request):
    """HTTP handler for SHACL validation."""
    try:
        data = await request.json()
        if not data:
            return JSONResponse(
                {"detail": "Missing JSON data in request"}, status_code=400
            )

        shacl_shape = request.app.state.shacl_shape
        graph = Graph()
        graph.parse(data=data, format="json-ld")
        conforms, _, text = validate_graph(graph, shacl_shape=shacl_shape)
        return JSONResponse({"valid": conforms, "message": text})
    except Exception as e:
        logger.exception("Validation failed")
        return JSONResponse({"detail": str(e)}, status_code=500)

async def get_shape(request: Request):
    """Return the SHACL shape as Turtle."""
    shape = request.app.state.shacl_shape.serialize(format="ttl")
    return Response(shape, media_type="text/turtle")


def serve_http(shacl_shape: Graph, port: int):
    """Start HTTP server using Starlette."""

    app = Starlette(
        debug=False,
        routes=[
            Route("/validate", validate_http, methods=["POST"]),
            Route("/shape", get_shape, methods=["GET"]),
            Route("/mainstem", get_mainstem, methods=["GET"]),
        ],
        middleware=[
            Middleware(
                CORSMiddleware,
                allow_origins=["*"],  # Allows all origins
                allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
                allow_headers=["*"],
            ),  # Allows all headers
        ],
    )
    app.add_event_handler("startup", initialize_duckdb)

    app.state.shacl_shape = shacl_shape

    logger.info(
        f"HTTP server started on 0.0.0.0:{port}; validate data by sending JSON-LD in the body of a POST to /validate"
    )
    uvicorn.run(app, host="0.0.0.0", port=port)


def serve(shacl_shape: Graph, grpc_port: int, http_port: int):
    """Launch both gRPC and HTTP servers."""
    grpc_thread = threading.Thread(
        target=serve_grpc, args=(shacl_shape, grpc_port), daemon=True
    )
    grpc_thread.start()

    # Run HTTP server in the main thread
    serve_http(shacl_shape, port=http_port)
