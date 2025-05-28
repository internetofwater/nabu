#!/bin/sh
# Copyright 2025 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0

set -e

# Run harvest locally and open the UI for local exploration

docker run -d --rm --name minio \
  -p 9000:9000 -p 9001:9001 \
  --health-cmd="curl --silent --show-error --fail http://localhost:9000/minio/health/ready || exit 1" \
  --health-interval=10s --health-timeout=5s --health-retries=3 \
  minio/minio server /data --console-address ":9001" 2> /dev/null || echo "Minio already running so skipping start"

docker run -d \
	--rm \
	-p 7200:7200 \
	--name graphdb \
	-e "JAVA_XMS=2048m" \
	-e "JAVA_XMX=4g" \
	khaller/graphdb-free 2> /dev/null || echo "GraphDB already running so skipping start"

cd "$(dirname "$0")"

curl -X POST "localhost:7200/rest/repositories" \
  -H "Content-Type: multipart/form-data" \
  -F "config=@testdata/iow-config.ttl"

cd ../

go run ./cmd/nabu harvest --log-level DEBUG --sitemap-index https://pids.geoconnex.dev/sitemap.xml  --concurrent-sitemaps 100 --sitemap-workers 150 --use-otel

go run ./cmd/nabu --log-level DEBUG sync --prefix summoned/ --endpoint http://localhost:7200 --use-otel --upsert-batch-size 100 --trace

go tool trace trace.out