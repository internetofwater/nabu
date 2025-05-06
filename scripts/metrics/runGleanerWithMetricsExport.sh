#!/bin/sh
# Copyright 2025 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0


set -e

cd "$(dirname "$0")"
docker compose up -d 

cd ../../
time go run cmd/gleaner/root.go --log-level DEBUG --sitemap-index https://geoconnex.us/sitemap.xml  --concurrent-sitemaps 100 --sitemap-workers 150 --use-otel --to-disk

open http://localhost:3000/drilldown