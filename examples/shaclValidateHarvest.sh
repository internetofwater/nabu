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
cd ../shacl_validator/shacl_validator_grpc_rs
cargo run &
CARGO_PID=$!
cd - >/dev/null

sleep 3

cd ../

time go run ./cmd/nabu harvest --log-level DEBUG \
  --sitemap-index https://pids.geoconnex.dev/sitemap.xml \
  --concurrent-sitemaps 100 --sitemap-workers 30 \
  --use-otel --to-disk --source ref_dams_dams__0 --shacl-grpc-endpoint localhost:50051 --exit-on-shacl-failure

# Open Jaeger UI
open http://localhost:16686