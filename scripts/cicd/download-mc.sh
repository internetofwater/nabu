#!/bin/sh
# Copyright 2025 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0



set -e

mkdir -p $HOME/bin

ARCH=$(arch)
case ${ARCH} in 
  aarch64)
     curl https://dl.min.io/client/mc/release/linux-arm64/mc -o $HOME/bin/mc
     ;;
  x86_64)
     curl  https://dl.min.io/client/mc/release/linux-amd64/mc -o $HOME/bin/mc
     ;;
  *)
     echo "mc client binary for linux not supported for $ARCH architecture!" && exit 1
esac

chmod +x $HOME/bin/mc

