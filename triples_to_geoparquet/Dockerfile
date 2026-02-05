# Copyright 2026 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0

FROM rust:slim AS rust-builder

WORKDIR /app

# Copy only manifest first to cache dependencies
COPY Cargo.toml Cargo.lock ./

# Create empty main.rs for initial build (to build dependencies)
RUN mkdir src && echo "fn main() {}" > src/main.rs

# Build dependencies (will be cached)
RUN cargo build --release || true

# Now copy full source and rebuild only changed code
COPY . .

RUN cargo build --release

FROM debian:bookworm-slim AS final

COPY --from=rust-builder /app/target/release/triples_to_geoparquet /usr/local/bin/triples_to_geoparquet

ENTRYPOINT [ "/usr/local/bin/triples_to_geoparquet" ]