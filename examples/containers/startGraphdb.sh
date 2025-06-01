#!/bin/sh
# Copyright 2025 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0


set -e

docker run -d \
	--rm \
	-p 7200:7200 \
	--name graphdb \
	-e "JAVA_XMS=2048m" \
	-e "JAVA_XMX=4g" \
	khaller/graphdb-free 2> /dev/null || echo "GraphDB already running so skipping start"

# initialize the graphdb config
curl -X POST "localhost:7200/rest/repositories" \
  -H "Content-Type: multipart/form-data" \
  -F "config=@testdata/iow-config.ttl"