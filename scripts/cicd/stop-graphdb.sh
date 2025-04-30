#!/bin/sh
# Copyright 2025 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0



docker stop graphdb >/dev/null 2>&1
set -e
while [ "$(docker ps|grep -c graphdb)" -gt "0" ]
do
 sleep 1
done 
