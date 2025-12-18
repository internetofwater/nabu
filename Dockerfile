# Copyright 2025 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0

FROM --platform=$BUILDPLATFORM golang:1.24-alpine AS go-builder

WORKDIR /app

# Explicitly download these files before the build so they can be cached
COPY go.mod .
COPY go.sum .
RUN go mod download

COPY . .

ARG TARGETOS TARGETARCH

ARG DUCKDB_VERSION=1.4.2
RUN wget -nv https://github.com/duckdb/duckdb/releases/download/${DUCKDB_VERSION}/libduckdb-linux-${TARGETARCH}.zip -O libduckdb.zip; \
    unzip libduckdb.zip -d /tmp/libduckdb

RUN CGO_ENABLED=1 GOOS=$TARGETOS GOARCH=$TARGETARCH go mod tidy && \
    go build -o nabu ./cmd/nabu


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