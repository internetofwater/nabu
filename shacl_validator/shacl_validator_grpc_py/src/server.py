# Copyright 2025 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0

"""gRPC + HTTP server for SHACL validation service (Starlette version)."""

import logging
import multiprocessing
import multiprocessing.connection
import os
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


def serve_grpc(shacl_shape: Graph, grpc_port: int, max_workers: int = 10):
    """Start gRPC server."""
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=max_workers),
        options=[
            ("grpc.max_receive_message_length", MAX_MESSAGE_SIZE),
            # allows several processes to listen on the same port;
            # the kernel spreads incoming connections across them
            ("grpc.so_reuseport", 1),
        ],
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


def _serve_grpc_process(shacl_file: str, grpc_port: int):
    """Entry point for a gRPC worker process; each process loads its own copy of the shapes."""
    logging.basicConfig(level=logging.INFO)
    shacl_shape = Graph().parse(shacl_file)
    # validation is CPU bound and holds the GIL, so extra threads
    # in a process would not validate any faster
    serve_grpc(shacl_shape, grpc_port, max_workers=1)


def serve_grpc_processes(shacl_file: str, grpc_port: int, processes: int):
    """Start several gRPC server processes listening on the same port.

    pyshacl validation is pure Python and CPU bound, so a single process can only
    use one core. Each process accepts its own connections, so clients should open
    several connections to spread requests across processes.
    Exits this process if any worker process stops so the container can be restarted.
    """
    # spawn instead of fork since gRPC does not support forking after it has been initialized
    context = multiprocessing.get_context("spawn")
    workers = [
        context.Process(
            target=_serve_grpc_process, args=(shacl_file, grpc_port), daemon=True
        )
        for _ in range(processes)
    ]
    for worker in workers:
        worker.start()
    logger.info(f"Started {processes} gRPC server processes on port {grpc_port}")

    def exit_if_any_worker_stops():
        multiprocessing.connection.wait([worker.sentinel for worker in workers])
        logger.error("A gRPC server process stopped unexpectedly; exiting")
        os._exit(1)

    threading.Thread(target=exit_if_any_worker_stops, daemon=True).start()


def grpc_processes_from_env() -> int:
    """The number of gRPC server processes; defaults to one per core."""
    return int(os.environ.get("SHACL_GRPC_PROCESSES", os.cpu_count() or 1))


def serve(shacl_shape: Graph, shacl_file: str, grpc_port: int, http_port: int):
    """Launch both gRPC and HTTP servers."""
    processes = grpc_processes_from_env()
    if processes > 1:
        serve_grpc_processes(shacl_file, grpc_port, processes)
    else:
        grpc_thread = threading.Thread(
            target=serve_grpc, args=(shacl_shape, grpc_port), daemon=True
        )
        grpc_thread.start()

    # Run HTTP server in the main thread
    serve_http(shacl_shape, port=http_port)
