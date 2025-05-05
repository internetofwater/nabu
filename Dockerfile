# Copyright 2025 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0

FROM --platform=$BUILDPLATFORM golang:1.23-alpine AS builder

WORKDIR /app

# Explicitly download these files before the build so they can be cached
COPY go.mod .
COPY go.sum .
RUN go mod download

COPY . .

ARG TARGETOS TARGETARCH BINARY_NAME

RUN GOOS=$TARGETOS GOARCH=$TARGETARCH go mod tidy && \
    go build -o ${BINARY_NAME} ./cmd/${BINARY_NAME}

FROM alpine

ARG BINARY_NAME

WORKDIR /app
COPY --from=builder /app/${BINARY_NAME} /app/${BINARY_NAME}
# Update the schema files with the latest version
ADD https://schema.org/version/latest/schemaorg-current-https.jsonld /app/assets/schemaorg-current-https.jsonld
ADD https://schema.org/version/latest/schemaorg-current-http.jsonld /app/assets/schemaorg-current-http.jsonld

COPY ./docker/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh
ENV BINARY_NAME=${BINARY_NAME}
ENTRYPOINT ["/entrypoint.sh"]



