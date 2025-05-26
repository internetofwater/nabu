#!/bin/sh
# Copyright 2025 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0

set -e

# Start both services in the background
/app/shacl_validator_grpc &
/app/nabu "$@" &

# Wait for all background processes
wait
