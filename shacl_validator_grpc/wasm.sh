#!/bin/sh
# Copyright 2025 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0


rustup target add wasm32-unknown-unknown

cargo install wasm-pack

cargo build --target wasm32-unknown-unknown --release