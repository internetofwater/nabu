#!/bin/sh
# Copyright 2025 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0

# grpcurl workaround with https://github.com/fullstorydev/grpcui/issues/375

time grpcurl -plaintext \
  -proto proto/shacl_validator.proto \
  -d '{"triples":"@prefix ex: <http://example.org/> .\nex:foo a ex:Bar ."}' \
  -authority dummy unix:///tmp/shacl_validator.sock \
  shacl_validator.ShaclValidator/Validate