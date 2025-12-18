# Copyright 2025 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0

FROM --platform=$BUILDPLATFORM golang:1.24-bookworm AS go-builder

WORKDIR /app

# Toolchains needed for CGO + DuckDB cross-compilation
RUN apt-get update && apt-get install -y \
    build-essential \
    gcc-aarch64-linux-gnu \
    g++-aarch64-linux-gnu \
    libc6-dev-arm64-cross \
    ca-certificates \
    git \
    && rm -rf /var/lib/apt/lists/*

COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG TARGETOS
ARG TARGETARCH

# Select correct compiler for CGO
RUN if [ "$TARGETARCH" = "arm64" ]; then \
    export CC=aarch64-linux-gnu-gcc; \
    export CXX=aarch64-linux-gnu-g++; \
    else \
    export CC=gcc; \
    export CXX=g++; \
    fi && \
    CGO_ENABLED=1 \
    GOOS=$TARGETOS \
    GOARCH=$TARGETARCH \
    go build \
    -tags=duckdb_use_bundled \
    -o nabu ./cmd/nabu


FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    libssl3 \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=go-builder /app/nabu /app/nabu

# Rest stays the same
ADD https://schema.org/version/latest/schemaorg-current-https.jsonld /app/assets/schemaorg-current-https.jsonld
ADD https://schema.org/version/latest/schemaorg-current-http.jsonld /app/assets/schemaorg-current-http.jsonld

ENTRYPOINT [ "/app/nabu" ]