#!/bin/sh
# Copyright 2025 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0
set -e 

docker run -d --rm --name minio \
  -p 9000:9000 -p 9001:9001 \
  --health-cmd="curl --silent --show-error --fail http://localhost:9000/minio/health/ready || exit 1" \
  --health-interval=10s --health-timeout=5s --health-retries=3 \
  minio/minio server /data --console-address ":9001" 2> /dev/null || echo "Minio already running so skipping start"