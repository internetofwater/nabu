# Copyright 2025 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0

FROM rust:slim AS rust-builder

WORKDIR /app

COPY . .

RUN cargo build --release

ENTRYPOINT [ "/app/target/release/triples_to_geoparquet" ]


FROM debian:bookworm-slim AS final

COPY --from=rust-builder /app/target/release/triples_to_geoparquet /usr/local/bin/triples_to_geoparquet

ENTRYPOINT [ "/usr/local/bin/triples_to_geoparquet" ]