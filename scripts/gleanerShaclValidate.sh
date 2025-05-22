#!/bin/sh
# Copyright 2025 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0

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

# Start MinIO container
docker run -d --rm --name minio \
  -p 9000:9000 -p 9001:9001 \
  --health-cmd="curl --silent --show-error --fail http://localhost:9000/minio/health/ready || exit 1" \
  --health-interval=10s --health-timeout=5s --health-retries=3 \
  minio/minio server /data --console-address ":9001" 2> /dev/null || echo "Minio already running so skipping start"

# Start cargo server
cd "$(dirname "$0")"
cd ../shacl_validator_grpc
cargo run &
CARGO_PID=$!
cd - >/dev/null

cd ../

export GRPC_GO_LOG_VERBOSITY_LEVEL=99
export GRPC_GO_LOG_SEVERITY_LEVEL=info

# Run gleaner
time go run cmd/gleaner/root.go --log-level DEBUG \
  --sitemap-index https://pids.geoconnex.dev/sitemap.xml \
  --concurrent-sitemaps 100 --sitemap-workers 150 \
  --use-otel --to-disk --source ref_dams_dams__0 --validate-shacl

# Open Jaeger UI
open http://localhost:16686