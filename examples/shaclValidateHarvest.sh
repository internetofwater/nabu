#!/bin/sh
# Copyright 2025 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0

set -e 

# Function to clean up background processes on exit
cleanup() {
  echo "Cleaning up..."
  if [ -n "$CARGO_PID" ] && kill -0 "$CARGO_PID" 2>/dev/null; then
    echo "Killing cargo run process with PID $CARGO_PID"
    kill "$CARGO_PID"
    wait "$CARGO_PID" 2>/dev/null
  fi
}
trap cleanup EXIT INT TERM

# cd relative to this script and start the local test infra
cd "$(dirname "$0")" && docker compose up -d

# Start cargo server
cd ../shacl_validator_grpc
cargo run &
CARGO_PID=$!
cd - >/dev/null

cd ../

# export GRPC_GO_LOG_VERBOSITY_LEVEL=99
# export GRPC_GO_LOG_SEVERITY_LEVEL=info

# Run gleaner
time go run ./cmd/nabu harvest --log-level DEBUG \
  --sitemap-index https://pids.geoconnex.dev/sitemap.xml \
  --concurrent-sitemaps 100 --sitemap-workers 150 \
  --use-otel --to-disk --source ref_dams_dams__0 --validate-shacl

# Open Jaeger UI
open http://localhost:16686