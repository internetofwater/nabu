#!/bin/sh
# Copyright 2025 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0

set -e

# Start the gRPC server in the background
/app/shacl_validator_grpc &

# Start nabu in the background and save its PID
/app/nabu "$@" &
nabu_pid=$!

# Wait only for nabu to finish since the gRPC server runs indefinitely
wait "$nabu_pid"
