#!/bin/sh
# Copyright 2025 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0


grpcurl -plaintext \
        -proto proto/shacl_validator.proto \
        -d '{"triples":"@prefix ex: <http://example.org/> .\nex:foo a ex:Bar ."}' \
        [::1]:50051 \
        shacl_validator.ShaclValidator/Validate
