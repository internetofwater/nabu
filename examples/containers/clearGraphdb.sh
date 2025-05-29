#!/bin/sh
# Copyright 2025 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0

set -e

# Stop and remove any existing container named 'graphdb'
if docker ps -a --format '{{.Names}}' | grep -Eq '^graphdb$'; then
  echo "Stopping and removing existing 'graphdb' container..."
  docker stop graphdb > /dev/null
fi

# Start a new container
echo "Starting new 'graphdb' container..."
docker run -d \
  --rm \
  -p 7200:7200 \
  --name graphdb \
  -e "JAVA_XMS=2048m" \
  -e "JAVA_XMX=4g" \
  khaller/graphdb-free

# Wait for the server to be ready
echo "Waiting for GraphDB to be ready..."
until curl -s -o /dev/null http://localhost:7200; do
  sleep 1
done

# Initialize the graphdb config
echo "Initializing GraphDB repository..."
curl -s -X POST "http://localhost:7200/rest/repositories" \
  -H "Content-Type: multipart/form-data" \
  -F "config=@testdata/iow-config.ttl"

echo "GraphDB container restarted and configured."
