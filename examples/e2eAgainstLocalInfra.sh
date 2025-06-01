#!/bin/sh
# Copyright 2025 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0


set -e 

# cd relative to this script
cd "$(dirname "$0")"

# if geoconnex.us doesn't exist, then clone so we 
# can get the docker compose to spin up the local test infra
if [ ! -d "geoconnex.us" ]; then
  git clone https://github.com/internetofwater/geoconnex.us
fi
cd geoconnex.us/tests

# Find all files recursively, excluding shell scripts, and process them
# so that they are relative the localhost instead of a live remote domain
# this will allow us to harvest local jsonld
find . -type f ! -name "*.sh" | while read -r file; do
  echo "Processing $file..."
  # Use BSD sed for in-place replacement (macOS) (might not be portable on Linux)
  sed -i '' 's|https://features.geoconnex.dev/|http://127.0.0.1:5001/|g' "$file"
  sed -i '' 's|https://pids.geoconnex.dev/|http://127.0.0.1:8080/|g' "$file"
done

echo "All files processed."

docker compose up -d

cd ../../..

# yourls does not like too much concurrency; try to limit sitemap workers * concurrent sitemaps to less than 300 or so
go run ./cmd/nabu harvest --sitemap-index http://127.0.0.1:8080/sitemap.xml --log-level DEBUG --sitemap-workers 30 --concurrent-sitemaps 10 --use-otel

go run ./cmd/nabu --log-level DEBUG sync --prefix summoned/ --endpoint http://localhost:7200 --use-otel --upsert-batch-size 100 

open http://localhost:16686