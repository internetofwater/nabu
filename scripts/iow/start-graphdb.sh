#!/bin/sh

set -e

docker run -d \
	-p 7200:7200 \
	--name graphdb \
	-e "JAVA_XMS=2048m" \
	-e "JAVA_XMX=4g" \
	khaller/graphdb-free

