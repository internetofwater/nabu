#!/bin/sh
# Copyright 2025 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0

set -e

if [ -z "$BINARY_NAME" ]; then
  echo "BINARY_NAME is not set!"
  exit 1
fi

#  
exec "/app/$BINARY_NAME" "$@"

