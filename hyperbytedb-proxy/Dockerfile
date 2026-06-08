# syntax=docker/dockerfile:1
FROM debian:bookworm-slim AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl ca-certificates build-essential pkg-config \
    && rm -rf /var/lib/apt/lists/*

# Install rustup but no toolchain — toolchain is selected by rust-toolchain.toml
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \
    | sh -s -- -y --default-toolchain none --profile minimal
ENV PATH="/root/.cargo/bin:${PATH}"
ENV CARGO_REGISTRIES_CRATES_IO_PROTOCOL=sparse
ENV CARGO_INCREMENTAL=0

WORKDIR /build/hyperbytedb

# Pin toolchain alongside the workspace. Build context is the repo root
# (see deploy/kind/setup.sh).
COPY rust-toolchain.toml ./
RUN rustup show active-toolchain

COPY Cargo.toml Cargo.lock ./
COPY hyperbytedb/Cargo.toml hyperbytedb/
COPY hyperbytedb-proxy/Cargo.toml hyperbytedb-proxy/
RUN --mount=type=cache,target=/root/.cargo/registry \
    --mount=type=cache,target=/root/.cargo/git \
    cargo fetch --locked

COPY hyperbytedb-proxy/src/ hyperbytedb-proxy/src/
RUN --mount=type=cache,target=/root/.cargo/registry \
    --mount=type=cache,target=/root/.cargo/git \
    --mount=type=cache,target=/build/hyperbytedb/target \
    cargo build --release --locked -p hyperbytedb-proxy \
    && mkdir -p /artifacts \
    && cp /build/hyperbytedb/target/release/hyperbytedb-proxy /artifacts/

# ---------------------------------------------------------------------------
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /artifacts/hyperbytedb-proxy /usr/local/bin/hyperbytedb-proxy

# Same default port as hyperbytedb itself: clients (Grafana, Telegraf) flip
# their endpoint URL to point at the proxy Service with no port change.
EXPOSE 8086

ENV HYPERBYTEDB_PROXY_LISTEN=0.0.0.0:8086 \
    HYPERBYTEDB_PROXY_BACKEND_PORT=8086

ENTRYPOINT ["/usr/local/bin/hyperbytedb-proxy"]
