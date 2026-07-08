# AGENTS.md — HyperbyteDB

## Build prerequisites

- **chdb-rust path dependency**: `hyperbytedb/Cargo.toml` uses `path = "../../chdb-rust"`.
  The repo must live as a sibling of the workspace root. Clone it:
  ```
  bash scripts/checkout-chdb-rust.sh
  ```
  CI does this automatically. An agent that runs `cargo build` without this step will fail.

- **libchdb.so** must be on the system: `curl -sL https://lib.chdb.io | bash && sudo ldconfig`.
  `scripts/install-dev-deps.sh` automates everything.

- **`.cargo/config.toml`** sets `LD_LIBRARY_PATH` to a chdb-rust checkout path (local machine).
  CI uses a Docker container; the host-side `.cargo/config.toml` is not used there.

## Commands

| Task | Command |
|------|---------|
| Build all | `cargo build` |
| Build release | `cargo build --release` |
| Run server | `cargo run -- serve` |
| Unit tests | `cargo test --lib` |
| Integration tests | `cargo test --test '*'` |
| CLI tests | `cargo test -p hyperbytedb-cli` |
| All tests (CI order) | `cargo test --lib && cargo test --test '*' && cargo test -p hyperbytedb-cli` |
| Lint | `cargo clippy --all-targets -- -D warnings` |
| Format check | `cargo fmt --check` |
| Benchmarks | `cargo bench` or `./scripts/bench-all.sh` |
| Single bench | `cargo bench --bench ingestion_line_protocol` |
| Install git hooks | `sh scripts/install-git-hooks.sh` |
| Dev deps (all) | `sh scripts/install-dev-deps.sh --all` |

## CI pipeline order (`.github/workflows/ci.yml`)

`fmt` → `clippy` (needs fmt) → `test` (needs clippy). Test runs three suites sequentially in one container:
`cargo test --lib` then `cargo test --test '*'` then `cargo test -p hyperbytedb-cli`.

## Git hooks (`.githooks/pre-commit`)

Runs `cargo fmt --check` + `cargo clippy --all-targets -- -D warnings`.
Installs automatically via `install-git-hooks.sh`. Bypass with `git commit --no-verify`.

## Workspace layout

Three crates: `hyperbytedb` (server), `hyperbytedb-cli` (CLI client), `hyperbytedb-proxy` (reverse proxy).
Edition 2024, toolchain pinned to **1.94** in `rust-toolchain.toml` (reproducible builds).

## Key architecture facts

- **Hexagonal** layout: `domain/`, `ports/` (traits), `adapters/` (RocksDB, chDB, HTTP, cluster), `application/` (services)
- **WAL**: RocksDB. **Query engine**: embedded ClickHouse via chDB (libchdb.so).
- **Clustering**: OpenRaft 0.9 for consensus; async or sync_quorum replication per node.
- **jemalloc** is the global allocator (`tikv-jemallocator`, background purging).
- `columnar-ingest` feature is **default-on**. Disable with `--no-default-features`.

## Configuration

`config.toml` or env vars with `HYPERBYTEDB__` prefix + `__` separators (e.g. `HYPERBYTEDB__SERVER__PORT=9090`).
The proxy uses **env vars only** (no TOML).

## Docker build

Context must be the **parent directory** (not the repo root) because of the `chdb-rust` path dependency:
```
docker build -f hyperbytedb/Dockerfile <parent-dir>
```
`deploy/kind/setup.sh` handles this. See the Dockerfile for `ARG CHDB_RUST_REF` (branch `feat_arrow_insert`).

## Multi-arch releases

`v*` tags trigger release workflow: native builds on amd64 (ubuntu-latest) + arm64 (ubuntu-24.04-arm).
Docker images at `ghcr.io/hyperbyte-cloud/hyperbytedb`. Release tarballs include `libchdb.so`.

## Code conventions

- `thiserror` for all error types (`HyperbytedbError`)
- `#![cfg_attr(not(test), warn(clippy::unwrap_used, clippy::expect_used))]` in main.rs
- Never hold a `Mutex`/`RwLock` across `.await`
- Integration tests use in-process Axum (no TCP)
- All Rust jobs in CI use `RUSTFLAGS="-D warnings"`, `CARGO_INCREMENTAL=0`
