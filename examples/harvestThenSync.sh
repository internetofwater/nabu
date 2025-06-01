#!/bin/sh
# Copyright 2025 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0

set -e

cd "$(dirname "$0")"

# Run harvest locally and open the UI for local exploration
source ./containers/startGraphdb.sh
source ./containers/startMinio.sh
source ./containers/startJaeger.sh

cd ../

go run ./cmd/nabu harvest --log-level DEBUG --sitemap-index https://pids.geoconnex.dev/sitemap.xml  --concurrent-sitemaps 100 --sitemap-workers 150 --use-otel

go run ./cmd/nabu --log-level DEBUG sync --prefix summoned/ --endpoint http://localhost:7200 --use-otel --upsert-batch-size 100 

open http://localhost:16686