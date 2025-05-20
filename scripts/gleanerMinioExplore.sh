#!/bin/sh
# Copyright 2025 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0


# Run gleaner locally and open the UI for local exploration

docker run -d --rm --name minio \
  -p 9000:9000 -p 9001:9001 \
  --health-cmd="curl --silent --show-error --fail http://localhost:9000/minio/health/ready || exit 1" \
  --health-interval=10s --health-timeout=5s --health-retries=3 \
  minio/minio server /data --console-address ":9001" 2> /dev/null || echo "Minio already running so skipping start"

time go run cmd/gleaner/root.go --log-level DEBUG --sitemap-index https://pids.geoconnex.dev/sitemap.xml  --concurrent-sitemaps 100 --sitemap-workers 150 --use-otel --source ref_dams_dams__0

open http://localhost:9000