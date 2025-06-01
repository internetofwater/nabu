#!/bin/sh
# Copyright 2025 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0
set -e 

# Run harvest locally and open the UI for local exploration
cd "$(dirname "$0")"

source ./startMinio.sh

cd ../

time go run ./cmd/nabu harvest --log-level DEBUG --sitemap-index https://pids.geoconnex.dev/sitemap.xml  --concurrent-sitemaps 100 --sitemap-workers 150 --use-otel

open http://localhost:9000