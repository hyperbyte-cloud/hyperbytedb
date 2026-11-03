# Lint and Test-Support Cleanup Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:**
- Production builds no longer contain helpers that only tests call.
- Clippy's performance findings are fixed.
- The useful lints are switched on, so new regressions fail CI.

**Architecture:** There are three small tasks:
1. Gate test-only helpers with `#[cfg(test)]`, and mark the ones integration tests need as `#[doc(hidden)]` test support.
2. Fix redundant clones, very large futures and hot-path lock scopes.
3. Add a `[lints.clippy]` table to the crate so CI's `-D warnings` enforces them.

**Tech Stack:** Rust 1.94, clippy.

**Spec:** the "lower-value findings" in `docs/superpowers/plans/2026-11-03-code-health-fixes.md`. Census taken at `7ea9643`; there is no `[lints]` table or `clippy.toml` today, and `lib.rs` only sets `warn(clippy::unwrap_used, clippy::expect_used)`.

**Depends on:** code-health Task 2, which deletes `enqueue_reconciliation`, one of the test-only items.

## Census (validated)

Production-unused items referenced only by **unit** tests, and not gated today. These get `#[cfg(test)]`:
- `FlushServiceImpl::with_sharded_wal_retention_bytes` (`application/flush_service.rs:149`)
- `quote_backticks_validated` (`domain/chdb_naming.rs:126`)
- `as_concrete` (`timeseriesql/ast.rs:454`)
- `DEFAULT_TAG_LOW_CARDINALITY_MAX` (`adapters/chdb/native_adapter.rs:75`)
- `append_batch_sync` (`adapters/wal/rocksdb_wal.rs:358`)
- `total_pending` (`adapters/cluster/hinted_handoff.rs:219`)
- `min_wal_ack` (`adapters/cluster/replication_log.rs:187`)
- `read_mutations_from` (`adapters/cluster/replication_log.rs:259`)

Items **integration** tests call. They can't be `#[cfg(test)]`, because integration tests link the non-test library. These get `#[doc(hidden)]` and a `/// Test support.` line:
- `lane_count` (`forward_coalescer.rs:83`)
- `parse_columnar_msgpack_to_points` (`columnar_msgpack.rs:144`)
- `with_defaults` (`replication_apply.rs:204`)
- `recorded_heartbeat_series` (`shard_scheduler.rs:859`)
- `tick_once_at` (`continuous_query_service.rs:75`)
- `empty_arrow_stream` (`ports/query.rs:20`)
- `total_rows` (`domain/prepared_wal.rs:31`)
- `engine_probe_count` (`native_adapter.rs:487`)
- `set_backends_for_test` (`hyperbytedb-proxy/src/pool.rs:85`)

Already gated, no action: `with_test_force_leader`, `with_test_propose_sink`, `seed_rollup_dest`, `tick_once_for_test`, `try_failover_for_test`, `movers_in_flight` (`shard_scheduler.rs`), and `decide_transition` (`cluster/heartbeat.rs:65`, a test wrapper over the production `decide_transition_with`).

Clippy hits (run with `-W clippy::<lint>`):
- **`redundant_clone`:**
  - `adapters/chdb/native_adapter.rs:1367`, `adapters/http/router.rs:205`, `application/replication_apply.rs:143`;
  - `domain/sharding/ops.rs:248`, `:280`;
  - `domain/sharding/query_merge.rs:317`, `:326`;
  - `server.rs:286`, `timeseriesql/to_clickhouse/materialized_view.rs:172`.
- **`large_futures`:** `application/cluster/bootstrap.rs:318`, `application/query_service.rs:693`, `server.rs:36`, `server.rs:163`.
- **`needless_collect`:** `adapters/chdb/native_adapter.rs:791`.
- **`iter_with_drain`:** `adapters/wal/batching_wal.rs:130`.
- **`or_fun_call`:** `application/runtime/region_write_stats.rs:22`, `application/shard_scheduler.rs:208`, `config.rs:566`, `:572`.
- **`significant_drop_tightening`** (a nursery lint, 39 hits). This plan fixes only the write/query hot-path ones:
  - `adapters/chdb/native_adapter.rs:364`, `:365`, `:415`, `:507`, `:910`, `:922`, `:1213`, `:1224`;
  - `domain/sharding/location_cache.rs:243`, `:290`, `:314`;
  - `application/ingest_metadata.rs:275`, `application/physical_names.rs:172`;
  - `application/runtime/region_write_stats.rs:21`, `:28`;
  - `adapters/cluster/forward_queue.rs:272`, `application/statement_summary.rs:136`.

Line numbers will have moved after earlier plans; re-run the lints to locate them.

## Global Constraints

- No behaviour change.
- `Cargo.lock` keeps chdb-rust 2.0.0 (clippy rewrites it, so reset it before each commit); commits carry no Claude trailers.
- The gate: `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings`, `cargo test --lib`, `cargo test --no-fail-fast --test '*'`, `cargo test -p hyperbytedb-cli`.

## Review Focus

1. **A lock scope narrowed past a read that needs it.** Narrowing must not split a read-then-write that relied on holding the lock (check-then-insert). Task 2 leaves any such site as is with a `#[allow(clippy::significant_drop_tightening)]` and a one-line reason.
2. **The proxy and CLI crates under the new lints.** The table goes in each crate's `Cargo.toml`. Task 3 runs `cargo clippy --workspace --all-targets`.
3. **`#[cfg(test)]` on an item a benchmark uses.** Benches link the non-test library too. Task 1's grep covers `benches/`.

---

### Task 1: Gate or label test-only helpers

- [ ] **Step 1: Re-check usage.** For every item in the two census lists, run `grep -rnw <name> hyperbytedb*/src hyperbytedb/tests hyperbytedb/benches hyperbytedb-proxy/tests`. Move any item that now has a production or bench use out of the lists.
- [ ] **Step 2: Gate and label.** Add `#[cfg(test)]` to the eight unit-test-only items (and `#[cfg(test)]` to imports only they use), and `#[doc(hidden)]` plus `/// Test support.` to the nine integration-test helpers.
- [ ] **Step 3: Run the gate,** in particular `cargo build --release` with no test cfg, to catch a gated item still used. Then commit with the message "Test-only helpers leave production builds".

### Task 2: Fix the clippy performance findings

- [ ] **Step 1: Collect the current hits.** Run `cargo clippy --lib -p hyperbytedb -- -A clippy::all -W clippy::redundant_clone -W clippy::large_futures -W clippy::needless_collect -W clippy::iter_with_drain -W clippy::or_fun_call -W clippy::significant_drop_tightening`, then `git checkout -- Cargo.lock`.
- [ ] **Step 2: Fix them.**
  - `redundant_clone`, `needless_collect`, `iter_with_drain`, `or_fun_call`: apply clippy's suggestion.
  - `large_futures`: wrap the future in `Box::pin(...)`.
  - `significant_drop_tightening`: fix only the hot-path sites listed in the census, ending the guard's scope (an inner block or `drop(guard)`) right after its last use. A check-then-act site gets the `allow` described in Review Focus 1.
- [ ] **Step 3: Re-run Step 1's command.** Expected: only `significant_drop_tightening` hits outside the hot-path list. Then run the gate and commit with the message "Fix clippy performance findings".

### Task 3: Turn the lints on

**Files:** `hyperbytedb/Cargo.toml`, `hyperbytedb-cli/Cargo.toml`, `hyperbytedb-proxy/Cargo.toml`.

- [ ] **Step 1: Add the lint table.** In each crate, add:

  ```toml
  [lints.clippy]
  redundant_clone = "warn"
  large_futures = "warn"
  needless_collect = "warn"
  iter_with_drain = "warn"
  or_fun_call = "warn"
  ```

  CI's `-D warnings` makes these errors. `significant_drop_tightening` stays off because of its false positives.
- [ ] **Step 2: Run** `cargo clippy --workspace --all-targets -- -D warnings`. Expected: clean. Fix any hit in the CLI or proxy the same way as Task 2, then reset `Cargo.lock`.
- [ ] **Step 3: Run the gate, then commit** with the message "Enable clippy performance lints".
