# Coding Standards

Conventions, patterns, and rules for writing code in the HyperbyteDB codebase.

---

## Error Handling

- Use `thiserror` for all error types. The central error type is `HyperbytedbError` in `src/error.rs`.
- Return `Result<T, HyperbytedbError>` from all fallible functions.
- Use `?` for propagation. Add context with descriptive error messages.
- Never use `.unwrap()` in production code. Use `.expect()` only for programmer errors (invariants that should never fail).
- The crate root enforces this with `#![cfg_attr(not(test), warn(clippy::unwrap_used, clippy::expect_used))]`.

```rust
// Good
let db = metadata.get_database(name)
    .ok_or_else(|| HyperbytedbError::DatabaseNotFound(name.to_string()))?;

// Bad
let db = metadata.get_database(name).unwrap();
```

---

## Async Patterns

- Use `tokio::task::spawn_blocking` for CPU-intensive work (chDB queries) and other blocking I/O.
- Use `tokio::spawn` for concurrent I/O tasks.
- Never hold a `Mutex` or `RwLock` across an `.await` point.
- Use `tokio::sync::watch` for shutdown signaling.
- Use `tokio::select!` for timer + shutdown patterns.

```rust
pub async fn run(&self, interval: Duration, mut shutdown_rx: watch::Receiver<bool>) {
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            _ = ticker.tick() => {
                if let Err(e) = self.do_work().await {
                    tracing::error!("service error: {}", e);
                }
            }
            _ = shutdown_rx.changed() => {
                if *shutdown_rx.borrow() {
                    tracing::info!("service shutting down");
                    break;
                }
            }
        }
    }
}
```

---

## Naming Conventions

| Category | Convention | Examples |
|----------|-----------|---------|
| Types | `UpperCamelCase` | `CompactionService`, `FlushResult` |
| Functions | `snake_case` | `partition_by_hour`, `build_schema` |
| Constants | `SCREAMING_SNAKE_CASE` | `WAL_READ_CHUNK`, `MIN_BATCH_POINTS` |
| Getters | No `get_` prefix | `fn name(&self) -> &str` |
| Builders | `new()` or `build()` | `ChdbPool::new(config)` |

---

## Dependencies and Injection

- Use `Arc<dyn Trait>` for dependency injection. All services accept ports as `Arc<dyn SomePort>`.
- Use `async_trait` for async trait methods.
- Keep domain types free of I/O dependencies.
- Wire dependencies in `src/bootstrap.rs` (the composition root).

```rust
pub struct IngestionServiceImpl {
    metadata: Arc<dyn MetadataPort>,
    wal: Arc<dyn WalPort>,
}
```

---

## Metrics

- Use the `metrics` crate (`counter!`, `gauge!`, `histogram!`).
- Prefix all metrics with `hyperbytedb_`.
- Use labels sparingly to avoid high-cardinality explosion.
- Emit metrics at the point of observation, not at call sites.

```rust
counter!("hyperbytedb_write_requests_total").increment(1);
histogram!("hyperbytedb_query_duration_seconds").record(elapsed.as_secs_f64());
gauge!("hyperbytedb_flush_points_total")
    .set(count as f64);
```

---

## Logging

Use `tracing` with structured fields:

```rust
tracing::info!(points = count, db = %db, "flush complete");
```

| Level | Usage |
|-------|-------|
| `ERROR` | Failures that need operator attention |
| `WARN` | Recoverable issues (replication failures, timeout retries) |
| `INFO` | Lifecycle events (startup, shutdown, flush/compaction summaries) |
| `DEBUG` | Per-request/per-operation details |
| `TRACE` | Internal algorithm details |

---

## Port Trait Design

Port traits (`src/ports/`) define the boundaries between application logic and infrastructure:

- Traits must be `Send + Sync` (for use in async contexts).
- Use `#[async_trait]` for async methods.
- Prefer `&str` and `&[T]` at trait boundaries over owned types.
- Return `Result<T, HyperbytedbError>` for all fallible operations.

---

## Module Organization

- **`domain/`** — Pure types with no I/O. Includes `domain/cluster/` DTOs and `chdb_naming`. Never depends on adapters or application.
- **`ports/`** — Traits only. No implementations, no business logic.
- **`application/`** — Business logic. Depends on ports and domain. Never on adapters. Cluster orchestration lives in `application/cluster/`.
- **`adapters/`** — Concrete implementations of ports. Depends on domain and ports. Cluster I/O lives in `adapters/cluster/`.
- **`timeseriesql/`** — Self-contained Influx-compatible query language (parser, AST, ClickHouse translator).

---

## Code Review Anchors

Key architectural invariants enforced during review:

1. **Port boundary integrity** — Application code never imports from `adapters::*`.
2. **Domain purity** — `domain/` types have no I/O dependencies.
3. **Config documentation** — New config keys must appear in `docs/user-guide/configuration.md` and `config.toml.example`.
4. **WAL ordering** — Flush must not drop acknowledged writes.
5. **Identifier quoting** — InfluxQL-to-ClickHouse translation must quote/escape user input.

See [Contributing](contributing.md) for the full review rubric.

---

## See Also

- [Architecture](architecture.md) — System design and patterns
- [Contributing](contributing.md) — Review process and rubric
