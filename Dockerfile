# syntax=docker/dockerfile:1
FROM debian:bookworm-slim AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl ca-certificates build-essential pkg-config \
    clang llvm-dev libclang-dev \
    pkg-config libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Install rustup itself, but not a toolchain — the actual rustc/cargo version
# is selected by rust-toolchain.toml (copied below). This keeps the Docker
# build reproducible against the same compiler the lockfile was generated
# with; otherwise rustup's "latest stable" can drift past `--locked`.
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \
    | sh -s -- -y --default-toolchain none --profile minimal
ENV PATH="/root/.cargo/bin:${PATH}"
ENV CARGO_REGISTRIES_CRATES_IO_PROTOCOL=sparse
ENV CARGO_INCREMENTAL=0

RUN curl -sL https://lib.chdb.io | bash

# NOTE: this image builds from the PARENT directory as its context (the dir
# containing both `hyperbytedb/` and `chdb-rust/`), because hyperbytedb depends
# on `chdb-rust` via a path dependency for the Arrow C Data Interface insert
# path that the published crate lacks. deploy/kind/setup.sh runs:
#   docker build -f hyperbytedb/Dockerfile <parent>
WORKDIR /build

# chdb-rust path dependency. Copied to /build/chdb-rust so `../../chdb-rust`
# from /build/hyperbytedb/hyperbytedb resolves correctly.
COPY chdb-rust /build/chdb-rust

# Pinned toolchain spec must be in place before any cargo invocation so
# rustup auto-installs the right version on first use.
COPY hyperbytedb/rust-toolchain.toml /build/hyperbytedb/
WORKDIR /build/hyperbytedb
RUN rustup show active-toolchain

COPY hyperbytedb/Cargo.toml hyperbytedb/Cargo.lock ./
COPY hyperbytedb/hyperbytedb/Cargo.toml hyperbytedb/
COPY hyperbytedb/hyperbytedb-proxy/Cargo.toml hyperbytedb-proxy/
COPY hyperbytedb/hyperbytedb-cli/Cargo.toml hyperbytedb-cli/
RUN mkdir -p hyperbytedb-cli/src && echo "fn main(){}" > hyperbytedb-cli/src/main.rs && echo "" > hyperbytedb-cli/src/lib.rs
# Create stub files so cargo can parse [[bench]] entries
RUN mkdir -p hyperbytedb/benches hyperbytedb/benches/support \
    hyperbytedb-proxy/benches hyperbytedb-proxy/benches/support \
    && echo "fn main(){}" > hyperbytedb/benches/ingestion_columnar.rs \
    && echo "fn main(){}" > hyperbytedb/benches/ingestion_line_protocol.rs \
    && echo "fn main(){}" > hyperbytedb/benches/query_fixed_dataset.rs \
    && echo "fn main(){}" > hyperbytedb/benches/flush_service.rs \
    && echo "" > hyperbytedb/benches/support/mod.rs \
    && echo "fn main(){}" > hyperbytedb-proxy/benches/routing.rs \
    && echo "" > hyperbytedb-proxy/benches/support/mod.rs
RUN --mount=type=cache,target=/root/.cargo/registry \
    --mount=type=cache,target=/root/.cargo/git \
    cargo fetch --locked

# Copy source and compile (registry + target dirs persist across builds via BuildKit cache)
COPY hyperbytedb/hyperbytedb/src/ hyperbytedb/src/
COPY hyperbytedb/hyperbytedb/tests/ hyperbytedb/tests/
COPY hyperbytedb/hyperbytedb/benches/ hyperbytedb/benches/
COPY hyperbytedb/hyperbytedb-cli/src/ hyperbytedb-cli/src/
COPY hyperbytedb/hyperbytedb-cli/tests/ hyperbytedb-cli/tests/
# target/ is a cache mount — artifacts are not in the image layer unless copied out.
RUN --mount=type=cache,target=/root/.cargo/registry \
    --mount=type=cache,target=/root/.cargo/git \
    --mount=type=cache,target=/build/hyperbytedb/target \
    cargo build --release --locked -p hyperbytedb --bin hyperbytedb -p hyperbytedb-cli --bin hyperbytedb-cli \
    && mkdir -p /artifacts \
    && cp /build/hyperbytedb/target/release/hyperbytedb /artifacts/ \
    && cp /build/hyperbytedb/target/release/hyperbytedb-cli /artifacts/

# Flat export target for release tarballs (see .github/workflows/release.yml).
FROM scratch AS artifacts
COPY --from=builder /artifacts/hyperbytedb /hyperbytedb
COPY --from=builder /artifacts/hyperbytedb-cli /hyperbytedb-cli
COPY --from=builder /usr/local/lib/libchdb.so /libchdb.so

# ---------------------------------------------------------------------------
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates libstdc++6 curl \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /usr/local/lib/libchdb.so /usr/local/lib/libchdb.so
COPY --from=builder /usr/local/include/chdb.h /usr/local/include/chdb.h
RUN ldconfig

COPY --from=builder /artifacts/hyperbytedb /usr/local/bin/hyperbytedb
COPY --from=builder /artifacts/hyperbytedb-cli /usr/local/bin/hyperbytedb-cli

RUN mkdir -p /var/lib/hyperbytedb/wal /var/lib/hyperbytedb/meta \
             /var/lib/hyperbytedb/chdb /var/lib/hyperbytedb/raft

ENV HYPERBYTEDB__STORAGE__WAL_DIR=/var/lib/hyperbytedb/wal \
    HYPERBYTEDB__STORAGE__META_DIR=/var/lib/hyperbytedb/meta \
    HYPERBYTEDB__CHDB__SESSION_DATA_PATH=/var/lib/hyperbytedb/chdb \
    HYPERBYTEDB__CLUSTER__RAFT_DIR=/var/lib/hyperbytedb/raft \
    HYPERBYTEDB__LOGGING__LEVEL=info

# Cap glibc's per-thread heap arenas. Without this, glibc creates up to
# ~8×CPU arenas of 64 MiB each — we observed 22 of them resident in
# production, contributing ~200–300 MiB of fragmented anonymous RSS
# that the application never asked for. `2` is the conventional
# server-side Rust+Tokio setting: enough to avoid the single-arena
# contention pathology, small enough that fragmentation stays bounded.
ENV MALLOC_ARENA_MAX=2

EXPOSE 8086

ENTRYPOINT ["hyperbytedb"]
CMD ["serve"]
