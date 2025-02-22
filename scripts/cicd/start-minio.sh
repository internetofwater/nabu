#!/bin/sh

# --rm
# --memory
set -e

docker run -d \
   --rm \
   -p 9000:9000 \
   -p 9001:9001 \
   --name minio \
   -e "MINIO_ROOT_USER=minioadmin" \
   -e "MINIO_ROOT_PASSWORD=minioadmin" \
   quay.io/minio/minio server /data --console-address ":9001"
sleep 3


