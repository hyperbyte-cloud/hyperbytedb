# Testing

HyperbyteDB uses unit tests, integration tests, a compatibility suite, cluster tests, and Criterion benchmarks.

## Test suites

| Suite | Location | Scope |
|-------|----------|-------|
| Unit tests | `src/**/*.rs` (`#[cfg(test)]`) | Parsers, domain logic, WAL batching, Raft log store |
| Integration | `tests/integration.rs` | Auth, cardinality, users, metrics, backup manifests, chDB layout |
| Compatibility | `tests/compat/` | InfluxDB v1 HTTP, DDL, query, and error behavior |
| Raft integration | `tests/raft_integration.rs` | Multi-node cluster, membership, replication log |
| Sync quorum | `tests/sync_quorum_integration.rs` | `sync_quorum` replication mode and ack semantics |
| E2E | `tests/e2e/` | Production bootstrap + background flush + HTTP; backup/restore round-trip |
| Load scripts | `scripts/load.sh`, `load.js`, `query.js` | Ad-hoc load testing (used by kind setup) |
| Benchmarks | `benches/` | Ingestion + fixed-dataset query (Criterion) |

## Running tests

```bash
# Unit tests (fast, no network)
cargo test --lib

# Individual integration crates
cargo test --test integration
cargo test --test raft_integration
cargo test --test sync_quorum_integration
cargo test --test compat
cargo test --test e2e

# Everything
cargo test

# Feature-gated code paths
cargo test --all-features
```

Integration and compat tests need libchdb on `LD_LIBRARY_PATH`. CI installs it via `https://lib.chdb.io`; locally, run `sh scripts/install-dev-deps.sh` first.

### Benchmarks

```bash
cargo bench                              # all three default suites
cargo bench --bench ingestion_line_protocol
cargo bench --bench ingestion_columnar
cargo bench --bench query_fixed_dataset
BENCH_DATASET=medium cargo bench --bench query_fixed_dataset
```

Criterion writes HTML reports to `target/criterion/`. See [Benchmarks](../benchmarks.md) for details.

## How tests work

Integration and compat tests build an in-process Axum application:

1. Create temporary WAL, metadata, and chDB directories.
2. Wire services via `build_services()` or a focused test harness.
3. Send HTTP requests directly to the router (no TCP).
4. Clean up on drop.

Cluster tests spin up multiple in-process nodes to exercise replication, membership, and sync quorum behavior.

## Writing tests

**Unit tests** — inline in the source file:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_duration() {
        assert_eq!(parse_duration("1h"), Ok(3600));
    }
}
```

**Integration tests** — follow patterns in `tests/integration.rs` and `tests/compat/`.

**Cluster tests** — follow `tests/raft_integration.rs` for multi-node setup.

Before deleting a test, confirm another suite covers the same behavior. See [test ownership](../engineering/code-review-rubric.md#test-ownership) in the review rubric.

## CI

The pipeline in `.github/workflows/ci.yml` runs:

| Job | Command |
|-----|---------|
| Format | `cargo fmt --all --check` |
| Clippy | `cargo clippy --all-targets -- -D warnings` |
| Tests | `cargo test --lib` + `cargo test --test '*'` |
| Build | `cargo build --release` |

All Rust jobs install system build dependencies and libchdb.

## See also

- [Building & CI](building-and-ci.md)
- [Contributing](contributing.md)
- [Benchmarks](../benchmarks.md)
