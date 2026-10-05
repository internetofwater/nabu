#!/bin/sh
# Copyright 2025 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0
set -e 

# cd relative to this script and start the local test infra
cd "$(dirname "$0")" && docker compose up -d

cd ../

time go run ./cmd/nabu harvest --mainstem-metadata "$(pwd)/shacl_validator/data/reference_catchments_and_flowlines.fgb" --log-level DEBUG --sitemap-index https://geoconnex.us/sitemap.xml  --concurrent-sitemaps 10 --sitemap-workers 100 --use-otel --source bulk:gnis

open http://localhost:16686