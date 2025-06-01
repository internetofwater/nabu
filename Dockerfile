# Copyright 2025 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0

FROM --platform=$BUILDPLATFORM golang:1.23-alpine AS go-builder

WORKDIR /app

# Explicitly download these files before the build so they can be cached
COPY go.mod .
COPY go.sum .
RUN go mod download

COPY . .

ARG TARGETOS TARGETARCH

RUN GOOS=$TARGETOS GOARCH=$TARGETARCH go mod tidy && \
    go build -o nabu ./cmd/nabu


FROM rust:slim AS rust-builder

WORKDIR /app

# Install dependencies needed for building native C libraries and openssl
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    pkg-config \
    libssl-dev \
    protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

COPY shacl_validator_grpc /app

RUN cargo build --release


FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    libssl3 \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=go-builder /app/nabu /app/nabu
COPY --from=rust-builder /app/target/release/shacl_validator_grpc /app/shacl_validator_grpc

# Rest stays the same
ADD https://schema.org/version/latest/schemaorg-current-https.jsonld /app/assets/schemaorg-current-https.jsonld
ADD https://schema.org/version/latest/schemaorg-current-http.jsonld /app/assets/schemaorg-current-http.jsonld

COPY ./docker/entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

ENTRYPOINT ["./entrypoint.sh"]

