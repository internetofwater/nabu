#!/bin/sh
# Copyright 2025 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0

set -e

# cd relative to this script and start the local test infra
cd "$(dirname "$0")" && docker compose up -d

cd ../

go run ./cmd/nabu harvest --log-level DEBUG --sitemap-index https://pids.geoconnex.dev/sitemap.xml  --concurrent-sitemaps 100 --sitemap-workers 150 --use-otel

go run ./cmd/nabu --log-level DEBUG sync --prefix summoned/ --endpoint http://localhost:7200 --use-otel --upsert-batch-size 100 --trace

go tool trace trace.out