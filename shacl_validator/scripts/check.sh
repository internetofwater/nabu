#!/bin/sh
# Copyright 2025 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0

# grpcurl workaround with https://github.com/fullstorydev/grpcui/issues/375

cd "$(dirname "$0")"

time grpcurl -plaintext \
  -proto ../shacl_validator.proto \
  -d '{"jsonld":"{\"@context\":{\"schema\":\"https://schema.org/\",\"ex\":\"http://example.org/\"},\"@graph\":[{\"@type\":\"schema:Place\",\"@id\":\"ex:foo\",\"schema:name\":\"Test\"}]}"}' \
  -authority dummy \
  localhost:50051 \
  shacl_validator.ShaclValidator/Validate
