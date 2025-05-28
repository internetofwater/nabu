#!/bin/sh
# Copyright 2025 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0


set -e 

docker run -d --rm --name jaeger \
  -p 16686:16686 \
  -p 4317:4317 \
  -e LOG_LEVEL=debug \
  jaegertracing/all-in-one
