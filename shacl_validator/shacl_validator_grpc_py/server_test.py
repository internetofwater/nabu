# Copyright 2025 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0

"""Integration test for SHACL validation service via HTTP and gRPC."""

import json
from pathlib import Path
import threading
import time
import pytest
import requests
import grpc
from rdflib import Graph
from shacl_validator_pb2 import JsoldValidationRequest
from shacl_validator_pb2_grpc import ShaclValidatorStub
from server import serve_grpc, serve_http  # import from your server module


@pytest.fixture(scope="session")
def shacl_shape():
    """Provide a minimal SHACL shape for testing."""
    shape = Graph()
    valid_data = Path(__file__).parent.parent / "shapes" / "geoconnex.ttl"
    shape.parse(
        data=valid_data.read_text(),
        format="ttl",
    )
    return shape


@pytest.fixture(scope="session")
def start_servers(shacl_shape):
    """Start both gRPC and HTTP servers in background threads."""
    grpc_port = 50061
    http_port = 8081

    grpc_thread = threading.Thread(
        target=serve_grpc, args=(shacl_shape, grpc_port), daemon=True
    )
    grpc_thread.start()

    http_thread = threading.Thread(
        target=serve_http, args=(shacl_shape, http_port), daemon=True
    )
    http_thread.start()

    # Give servers a moment to start
    time.sleep(2)
    yield grpc_port, http_port


def test_http_validation(start_servers):
    """Test the HTTP /validate endpoint."""
    _, http_port = start_servers
    jsonld_data = json.dumps(
        {
            "@context": {"ex": "http://example.org/"},
            "@type": "ex:Person",
            "ex:name": "Alice",
        }
    )

    response = requests.post(
        f"http://localhost:{http_port}/validate",
        json=jsonld_data,
        timeout=3,
    )
    assert response.status_code == 200
    data = response.json()
    assert "valid" in data
    assert isinstance(data["valid"], bool)
    assert "message" in data
    assert not data["valid"]


def test_grpc_validation(start_servers):
    """Test gRPC Validate call."""
    grpc_port, _ = start_servers
    channel = grpc.insecure_channel(f"localhost:{grpc_port}")
    stub = ShaclValidatorStub(channel)

    jsonld_data = json.dumps(
        {
            "@context": {"ex": "http://example.org/"},
            "@type": "ex:Person",
            "ex:name": "Alice",
        }
    )

    request = JsoldValidationRequest(jsonld=jsonld_data)
    reply = stub.Validate(request)

    assert hasattr(reply, "valid")
    assert hasattr(reply, "message")
    assert isinstance(reply.valid, bool)
    assert isinstance(reply.message, str)
    assert not reply.valid
