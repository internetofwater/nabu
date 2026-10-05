#!/bin/sh
# Copyright 2025 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0

set -e

# cd relative to this script and start the local test infra
cd "$(dirname "$0")" && docker compose up -d

cd ../

time go run ./cmd/nabu harvest --mainstem-metadata "$(pwd)/shacl_validator/data/reference_catchments_and_flowlines.fgb" --log-level DEBUG --sitemap-index https://geoconnex.us/sitemap.xml  --concurrent-sitemaps 10 --sitemap-workers 10 --use-otel --source wwdh:usace:access_to_water

mkdir -p /tmp/nquads

# stream the harvested parquet file to n-quads; this could instead be piped directly into a graph database
time go run "$(pwd)/cmd/nabu" nquads \
  --prefix summoned/wwdh:usace:access_to_water.parquet \
  --sitemap-index https://geoconnex.us/sitemap.xml \
  --log-level DEBUG \
  --use-otel > /tmp/nquads/wwdh_usace_access_to_water.nq
