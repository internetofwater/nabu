#!/bin/sh
# Copyright 2025 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0
set -e 

# cd relative to this script and start the local test infra
cd "$(dirname "$0")" && docker compose up -d

cd ../

time go run ./cmd/nabu harvest --log-level DEBUG --sitemap-index https://pids.geoconnex.dev/sitemap.xml  --concurrent-sitemaps 10 --sitemap-workers 1000 --use-otel

open http://localhost:16686