#!/bin/sh
# Copyright 2025 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0
set -e 

# cd relative to this script and start the local test infra
cd "$(dirname "$0")" && docker compose up -d

cd ../

time go run ./cmd/nabu harvest --log-level DEBUG --sitemap-index https://geoconnex.us/sitemap.xml  --concurrent-sitemaps 1 --sitemap-workers 4 --use-otel --source usgs_monitoring-location_nwisgw_nwisgw__7 --to-disk --log-level TRACE

open http://localhost:16686