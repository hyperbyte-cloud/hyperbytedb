# Code-Health Fixes Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix every validated finding of the 2026-11-03 code-health scan:
- one bug;
- dead code and unused crates;
- duplicated helpers;
- hexagonal layering leaks;
- two avoidable costs on the write path.

None of these changes behaviour, apart from the bug fix.

**Architecture:** The work comes in five independent phases (A–E). Each task leaves the tree green and is its own commit. Layering fixes move code toward the rule *domain ← ports ← application ← adapters/composition*:
- pure logic moves into `domain`;
- capabilities the application needs become `ports` traits;
- wiring moves out of `application`.

**Tech Stack:** Rust 2024 (toolchain 1.94), tokio, openraft 0.9, RocksDB, chDB (chdb-rust 2.0.0), criterion.

**Spec:** the 2026-11-03 scan report in this session, validated against branch `fix/review-2026-11-bugs` at `7ea9643`. Every finding below was re-checked against the code, and line numbers refer to that commit. The validation evidence is listed with each task.

**Base:** branch off `fix/review-2026-11-bugs` (PR #132), or off `main` once #132 has merged. Tasks 15 and 16 change code that #132 adds.

## Global Constraints

- `Cargo.lock` must keep chdb-rust **2.0.0**. `cargo clippy` may rewrite the lock; run `git checkout -- Cargo.lock` before every commit.
- Commits carry no Claude / Co-Authored-By trailers.
- The gate for every task, in CI order: `cargo fmt --check`, then `cargo clippy --all-targets -- -D warnings`, then `cargo test --lib`, then `cargo test --no-fail-fast --test '*'`, then `cargo test -p hyperbytedb-cli`.
- Raft log and HTTP wire formats must not change:
  - moving a type must keep its serde representation;
  - `ShardMapOp::ClearVerified`, the `transfer_verified` fields and the `[sharding] transfer_clear_proposals_enabled` config key (`config.rs:108`) **stay**. Query routing and placement still read them.
- Do not touch the three bugs still open from the 2026-11-02 review: `delete_series_matching` ignoring its predicate, `DROP SERIES` without `FROM`, and `ingest_seq` ordering. They have their own work.

## Review Focus

1. **Mixed-version Raft log.** An entry written by a node on the old version must still decode after `ClusterRequest` moves module. Task 11 pins its JSON encoding with a fixture.
2. **`DELETE` with `?rp=` in Raft mode.** It must hit the named retention policy, not the default one. Task 1 tests both the Raft and the non-Raft builders.
3. **Strings with control characters** (tab, newline) in tag values, in `WHERE` clauses, bind parameters and line-protocol writes. They must round-trip after the escapers merge. Task 6 adds a query test with a tab in a tag value.
4. **Old config files.** A `config.toml` that still sets `transfer_clear_proposals_enabled` must still load after the reconciliation code is deleted. Task 2 adds a config-load test.
5. **A field widening, then conflicting, within one batch** (int, then float, then string). The middle point must widen and the last must be dropped after the in-place change. Task 15 adds this test.

---

## Phase A — Bug

### Task 1: Raft-mode DELETE / DROP SERIES honour `?rp=`; one mutation builder

**Validated:**
- `peer_query_service.rs:402` `mutation_request_from_statement(stmt, db, metadata, shard_routing)` has no retention-policy input. Its `Delete` arm (~416) and `DropSeries` arm (~550) call `metadata.get_default_rp`.
- `execute_raft_mutation` (`:186`) never receives `retention_policy`.
- The non-Raft inline arms of `execute_query` build the same mutations a second time with the request's policy: `Delete` at ~700–735, `DropSeries` at ~1035–1080.

**Files:**
- Modify: `hyperbytedb/src/application/peer_query_service.rs`
- Test: the same file's `#[cfg(test)] mod tests`

**Interfaces:**
- Produces: `async fn mutation_request_from_statement(stmt: &Statement, db: &str, retention_policy: Option<&str>, metadata: &Arc<dyn MetadataPort>, shard_routing: Option<&Arc<ShardRoutingContext>>) -> Result<MutationRequest, HyperbytedbError>`

- [ ] **Step 1: Write the failing test** `raft_delete_uses_the_requested_retention_policy`:
  - Set up `RocksDbMetadata` with database `db`, plus a non-default policy `week` created with `create_retention_policy`, and measurement `cpu` registered in `week` with tag key `host`.
  - Call `mutation_request_from_statement(&parse("DELETE FROM cpu WHERE host='a'")[0], "db", Some("week"), &meta, None)`.
  - Assert it is `MutationRequest::Delete { rp, .. }` with `rp == "week"`.
  - Repeat with `DROP SERIES FROM cpu WHERE host='a'` and assert `MutationRequest::DropSeries { rp, .. }` with `rp == "week"`.
  - With `None` instead of `Some("week")`, assert `rp == "autogen"`.
- [ ] **Step 2: Run it and check that it fails.** Run `cargo test --lib peer_query_service::tests::raft_delete_uses_the_requested_retention_policy`. Expected: a compile error (wrong number of arguments).
- [ ] **Step 3: Add the parameter.** `Delete` and `DropSeries` use `retention_policy` when it is set, else `get_default_rp`. Thread it through:
  - give `execute_raft_mutation` a `retention_policy: Option<&str>` parameter;
  - pass it from `execute_query` (call at ~:662);
  - replace the inline `Delete` and `DropSeries` request-building blocks with `mutation_request_from_statement(&stmt, db, retention_policy, &self.metadata, self.shard_routing.get())`.
- [ ] **Step 4: Run the test (expect PASS), then the full gate.**
- [ ] **Step 5: Commit** with the message "Raft-mode DELETE / DROP SERIES honour ?rp=; build cluster mutations in one place".

---

## Phase B — Dead code

### Task 2: Delete the unused split-transfer reconciliation in the shard scheduler

**Validated:**
- The callers were removed in 0.9.1 (#116), when splits became metadata-only. The code's own note (`shard_scheduler.rs:261-264`) says the deletion deserves its own review.
- With the 17 `#[allow(dead_code)]` attributes removed, rustc reports these as unused (539 lines in all):
  - `PendingTransfer` (:266);
  - `RECONCILE_PARK_AFTER_TICKS` (:285) and `RECONCILE_MIN_AGE_SECS` (:288);
  - the fields `points_sink` (:300) and `reconciliation_queue` (:325);
  - `try_activate_staged_regions` (:740), `enqueue_reconciliation` (:1866) and `propose_clear_verified` (:1910);
  - `push_and_drop_range` (:2877) and `request_region_rehome` (:2927);
  - `TransferDisposition` (:3017) and `classify_transfer_error` (:3028);
  - `now_unix` (:3170);
  - `rebuild_queue_from_map` (:3179), `drain_reconciliation` (:3225), `attempt_reconciliation` (:3349), `escalate_stuck_transfer` (:3468), `remove_reconciliation` (:3497) and `pending_transfers` (:3506);
  - `ReconcileOutcome` (:3515).
- `ShardScheduler::new` (:389) takes `points_sink` as a parameter; there are 8 call sites in `src/` and `tests/`.

**Files:**
- Modify: `hyperbytedb/src/application/shard_scheduler.rs`
- Modify: every `ShardScheduler::new(` call site (find them with `grep -rn "ShardScheduler::new(" src tests`)
- Modify: `docs/deep-dive/deep-dive-clustering.md:706-738`
- Test: `hyperbytedb/src/config.rs` tests

- [ ] **Step 1: Write the config test** `old_config_with_transfer_clear_proposals_still_loads`. Parse a TOML string containing `[sharding]\ntransfer_clear_proposals_enabled = false` into `Config`, and assert it parses with that field `false`. It passes today and pins Review Focus 4.
- [ ] **Step 2: Remove the items listed above.** Also remove every `#[allow(dead_code)]` they carried, the `points_sink` parameter of `ShardScheduler::new` and its argument at each call site, and any import left unused. Then run `grep -n "allow(dead_code)" src/application/shard_scheduler.rs`. Expected: no output. If anything is left, it is still in use: keep it and drop only its allow.
- [ ] **Step 3: Fix the stale comment.** Change the comment near `:2241` (it says ``Re-read: `drain_reconciliation` runs before the tick loop``) so it no longer names a removed function.
- [ ] **Step 4: Rewrite the documentation section.** Replace the "Split-transfer reconciliation" section of `deep-dive-clustering.md` with a short paragraph. It should say that splits are metadata-only, so no transfer debt is created, and that `transfer_verified` / `ClearVerified` remain only for routing, placement, and decoding older Raft logs.
- [ ] **Step 5: Run the full gate.** Expect no new warnings and every test passing.
- [ ] **Step 6: Commit** with the message "Remove the unused split-transfer reconciliation from the shard scheduler".

### Task 3: Delete the other unreferenced items

**Validated:** each of these is referenced nowhere in `hyperbytedb/src`, `tests`, `benches`, `hyperbytedb-cli`, `hyperbytedb-proxy` or `docs`, apart from its own definition or re-export:
- `application/arrow_ingest/` (the whole module: `mod.rs`, `columnar.rs`, `line_protocol.rs`, `msgpack.rs`, `points.rs`); remove `pub mod arrow_ingest;` from `application/mod.rs:1`
- `MaterializedViewService::drop_ch_objects_for_def` (`materialized_view_service.rs:473`)
- `parse_line_body_to_points_with_capacity` (`line_protocol.rs:157`)
- `ReplicationApplyQueue::with_workers` (`replication_apply.rs:183`)
- `SharedShardMapPort` (`ports/sharding.rs:97`)
- `merge_field_disc` (`domain/field_type.rs:56`)
- `SanitizedIdent::sanitize` (`domain/chdb_naming.rs:28`)
- `FieldValue::type_name` (`domain/point.rs:14`), keeping `type_name_from_discriminant`
- `LocationCache::invalidate_all` (`domain/sharding/location_cache.rs:172`)
- `ChdbQueryAdapter::with_concurrency_limit` (`adapters/chdb/query_adapter.rs:41`)
- `ChdbNativeAdapter::with_arrow_inserts` (`adapters/chdb/native_adapter.rs:306`)
- `default_pool_size` (`adapters/chdb/session.rs:74`)
- `ArrowCache::insert_batch` (`adapters/wal/arrow_cache.rs:33`)
- `with_message` (`adapters/cluster/raft/types.rs:32`)
- `RaftAdaptor` (`adapters/cluster/raft/mod.rs:25`)
- `build_coalesced_fact_view_with_row_meta` (`timeseriesql/to_clickhouse/coalesce.rs:26`, and its re-export in `to_clickhouse/mod.rs:21`)
- the CLI's `first_error` (`hyperbytedb-cli/src/client/query.rs:246`)

- [ ] **Step 1: Delete the items,** then re-run the check: `grep -rnw "<name>" hyperbytedb* docs` for each name. Expected: no hits.
- [ ] **Step 2: Run the full gate.** Expected: PASS. A compile error means something uses an item after all: restore that item and note it in the commit message.
- [ ] **Step 3: Commit** with the message "Remove unreferenced code".

### Task 4: Remove unused dependencies

**Validated:** `cargo machete` and a grep agree on:
- `hyperbytedb/Cargo.toml`: `tower` (:11), `crc32fast` (:95), `comfy-table` (:106), `colored` (:107), `simd-json` (:108);
- `hyperbytedb-proxy/Cargo.toml`: `thiserror`, `tower`;
- `hyperbytedb-cli/Cargo.toml`: `anyhow`, `url`.

- [ ] **Step 1: Remove the entries,** then run `cargo build --workspace --all-targets`. Expected: success.
- [ ] **Step 2: Check the lock diff.** Run `git diff Cargo.lock`: only removals are expected, and chdb-rust must still be `2.0.0`. Commit this lock change; it is the one exception to resetting the lock.
- [ ] **Step 3: Run the full gate, then commit** with the message "Drop unused dependencies".

---

## Phase C — Duplicates

### Task 5: One "raw write into an MV destination" rule

**Validated:** the same condition (`materialized && materialized_rp.is_none_or(|mr| mr == rp)`) is written out in six places:
- `ingestion_service.rs:152` and `:251`;
- `peer_ingestion_service.rs:228`, `:896` and `:1046`;
- `native_adapter.rs:691`.

**Files:** `hyperbytedb/src/domain/measurement.rs` (impl at :29), plus the six sites above.

**Interfaces:**
- Produces: `impl MeasurementMeta { pub fn refuses_raw_writes(&self, rp: &str) -> bool }`

- [ ] **Step 1: Write the failing test** `refuses_raw_writes_only_for_its_own_rp` in `domain/measurement.rs`, with three cases:
  - `materialized: false` gives `false` for `"autogen"`;
  - `materialized: true, materialized_rp: Some("rollup")` gives `true` for `"rollup"` and `false` for `"autogen"`;
  - `materialized: true, materialized_rp: None` gives `true` for any policy.
- [ ] **Step 2: Implement it,** replace the six conditions with calls to it, and run the full gate.
- [ ] **Step 3: Commit** with the message "One rule for raw writes into materialized-view destinations".

### Task 6: One ClickHouse string-literal escaper

**Validated:** there are four escapers.
- These three escape only `\` and `'`:
  - `query_service.rs:2005` `escape_clickhouse_string_literal`;
  - `adapters/http/query.rs:75` `escape_bind_string`;
  - `to_clickhouse/conditions.rs:403` `quote_string`.
- `native_adapter.rs:2649` `append_quoted_string` also escapes `\n`, `\r`, `\t` and other control characters as `\xNN`.

**Files:**
- Modify: `hyperbytedb/src/domain/chdb_naming.rs`
- Modify: the four sites above

**Interfaces:**
- Produces: `pub fn push_string_literal_body(out: &mut String, s: &str)` (escapes, no quotes) and `pub fn string_literal(s: &str) -> String` (`'…'` quoted), both in `domain/chdb_naming.rs`, with the native adapter's escaping.

- [ ] **Step 1: Write the failing unit test** `string_literal_escapes_quotes_backslashes_and_controls`:
  - `string_literal("a'b\\c")` gives `'a\'b\\c'`;
  - `string_literal("x\ty\n")` gives `'x\ty\n'`, with the backslash sequences written literally;
  - `string_literal("\u{1}")` gives `'\x01'`.
- [ ] **Step 2: Write the integration test** `tag_value_with_a_tab_round_trips` in `tests/compat/query_tests.rs`, marked `#[serial(chdb)]` (Review Focus 3).
  - Write `cpu,host=a\tb v=1 1`; in the line-protocol body the tag value holds a literal tab.
  - Then `SELECT v FROM cpu WHERE host = 'a<TAB>b'` must return 1 row.
- [ ] **Step 3: Implement the two functions,** and make the four sites call them:
  - `quote_string` and `escape_clickhouse_string_literal` become `string_literal` and the body-only form;
  - `escape_bind_string` becomes the body-only form;
  - `append_quoted_string` delegates.
- [ ] **Step 4: Run the full gate.** Exact-SQL test expectations may change only where an input string contains a control character. If any other expectation changes, stop: the escaping semantics differ.
- [ ] **Step 5: Commit** with the message "One ClickHouse string-literal escaper".

### Task 7: One JSON POST helper in the Raft network

**Validated:** `adapters/cluster/raft/network.rs:117` `post_json` and `:145` `post_json_snapshot` are line-for-line identical. They differ only in the error type parameter of `RPCError<u64, BasicNode, E>`.

- [ ] **Step 1: Merge them** into one `async fn post_json<Req: Serialize, Resp: DeserializeOwned, E: std::error::Error>(&self, path: &str, req: &Req, option: &RPCOption) -> Result<Resp, RPCError<u64, BasicNode, E>>`, and update its callers.
- [ ] **Step 2: Run the gate.** The Raft tests cover this path: `cargo test --test raft_integration`, then the full gate.
- [ ] **Step 3: Commit** with the message "One JSON POST helper for Raft RPCs".

### Task 8: Leadership port (removes the duplicated `is_raft_leader`)

**Validated:** four places read `metrics().current_leader` directly:
- `retention_service.rs:48` and `continuous_query_service.rs:33`: identical `is_raft_leader`, over `Option<HyperbytedbRaft>`; `None` means leader;
- `runtime/shard_heartbeat.rs:99-103`: reads `current_leader`;
- `raft_leader_callbacks.rs:44`.

The scheduler, `drain.rs` and `peer_query_service.rs` also *propose* through Raft (`client_write`), so they keep `HyperbytedbRaft`. That part is out of scope (see the end of the plan).

**Files:**
- Create: `hyperbytedb/src/ports/leadership.rs`, registered in `ports/mod.rs`
- Modify: `adapters/cluster/raft/mod.rs` (the impl), `retention_service.rs`, `continuous_query_service.rs`, `runtime/shard_heartbeat.rs`, and their constructor call sites (5 in all; find them with `grep -rn "RetentionService::new\|ContinuousQueryService::new\|run_region_heartbeat_reporter(" src tests`)

**Interfaces:**
- Produces: `pub trait LeadershipPort: Send + Sync { fn current_leader(&self) -> Option<u64>; fn is_leader(&self, node_id: u64) -> bool { self.current_leader() == Some(node_id) } }`, implemented in `adapters/cluster/raft/mod.rs` as `impl LeadershipPort for HyperbytedbRaft`.
- The retention and continuous-query services take `Option<Arc<dyn LeadershipPort>>`; `None` keeps meaning "single node: always leader". `run_region_heartbeat_reporter` takes `Arc<dyn LeadershipPort>`.

- [ ] **Step 1: Write the failing test** `retention_and_cq_gate_on_leadership`.
  - Each service replaces its private `is_raft_leader` with `fn may_run(&self) -> bool`, which `None` (single node) makes `true`. The retention service checks leadership inside `run` (`retention_service.rs:58`) and has no single-tick entry, so the test calls `may_run` directly.
  - Use a stub `LeadershipPort` whose leader is node 2. Construct each service as node 1 with `Some(stub)` and assert `!may_run()`; as node 2, assert `may_run()`; with `None`, assert `may_run()`.
- [ ] **Step 2: Implement the port and swap the three files.** Delete both `is_raft_leader` copies in favour of `leadership.is_leader(node_id)`. Then run `grep -n "adapters::cluster::raft" src/application/retention_service.rs src/application/continuous_query_service.rs src/application/runtime/shard_heartbeat.rs`. Expected: no output.
- [ ] **Step 3: Run the full gate, then commit** with the message "Leadership port; retention, continuous queries and heartbeats stop holding the Raft handle".

### Task 9: One write-body parser for both ingestion services

**Validated:** `ingestion_service.rs:192-211` and `peer_ingestion_service.rs:968-987` have the same `match format { LineProtocol | Msgpack | ColumnarMsgpack => Err(..) }` parse block, followed by the same `validate_point_count` rule. Merging the two whole `ingest()` functions is not planned: they differ in replication and sharding.

**Interfaces:**
- Produces: `pub fn parse_write_body(body: &[u8], precision: Option<&str>, format: WritePayloadFormat, max_points: usize) -> Result<Vec<Point>, HyperbytedbError>` in `application/ingest_metadata.rs`, next to `validate_point_count` (:86).

- [ ] **Step 1: Write the failing test** `parse_write_body_counts_only_non_line_protocol`. With `max_points = 1`:
  - a 2-line line-protocol body parses, since line protocol is limited inside the parser;
  - a 2-point msgpack body returns `RequestPointLimitExceeded`;
  - `ColumnarMsgpack` returns `Internal`.
- [ ] **Step 2: Implement it,** replace both blocks, and run the full gate.
- [ ] **Step 3: Commit** with the message "One write-body parser for both ingestion services".

---

## Phase D — Hexagonal layering

### Task 10: Password hashing moves to the domain

**Validated:** `hash_password` lives in `adapters/http/auth_middleware.rs:309` (argon2) and is called from application code: `query_service.rs:1702` and `:1728`, and `peer_query_service.rs:1222`. It is also called from `bootstrap.rs:709`.

**Interfaces:**
- Produces: `pub fn hash_password(password: &str) -> Result<String, HyperbytedbError>` in `domain/user.rs`. `auth_middleware` re-exports it, so HTTP code is unchanged.

- [ ] **Step 1: Move the function and its test** (if `auth_middleware` has one), keep a `pub use` in `auth_middleware`, and point the three application callers at `crate::domain::user::hash_password`. Then run `grep -rn "adapters::http" src/application`. Expected: no output.
- [ ] **Step 2: Run the full gate, then commit** with the message "Password hashing lives in the domain".

### Task 11: Raft request types and create-database logic leave the adapter

**Validated:**
- `ClusterRequest` (`adapters/cluster/raft/types.rs:11`) and `ClusterResponse` (:19) depend only on serde and domain types. Application uses them in `drain.rs`, `shard_scheduler.rs:2667`, `peer_query_service.rs` and `shard_routing.rs`.
- `apply_create_database` (`adapters/cluster/raft/state_machine.rs:19`) is called from `schema_mutation_apply.rs:170`.

**Files:**
- Create: `hyperbytedb/src/domain/cluster/raft_request.rs`
- Modify: `adapters/cluster/raft/types.rs` (re-export), `state_machine.rs`, `schema_mutation_apply.rs`, and the application users above

- [ ] **Step 1: Write the fixture test** `cluster_request_json_is_unchanged` (Review Focus 1). Serialize one value of every `ClusterRequest` variant with `serde_json`, and compare against JSON literals captured from the current code *before* the move: run the test once, then paste its output into the assertions.
- [ ] **Step 2: Move the types** into `domain/cluster/raft_request.rs` with `pub use` from `raft/types.rs`. Move `apply_create_database` into `schema_mutation_apply.rs`, with `state_machine.rs` calling it there.
- [ ] **Step 3: Check the imports.** Run `grep -rn "adapters::cluster::raft::types\|raft::state_machine" src/application`. Expected: no output.
- [ ] **Step 4: Run the full gate, then commit** with the message "Raft request types and create-database logic leave the Raft adapter".

### Task 12: Application uses `ShardMapPort`, not `RocksDbShardMap`

**Validated:**
- Application calls only `snapshot()` (46 sites) and `apply_op()` (4) on the shard map; both are in `ShardMapPort` (`ports/sharding.rs:11,28`). `RocksDbShardMap::open` appears only at construction (bootstrap and tests).
- The concrete type appears in `ShardRoutingContext::shard_map` (`shard_routing.rs:30`), `ShardScheduler::new` (`shard_scheduler.rs:390`), `runtime/shard_heartbeat.rs` and `cluster/bootstrap.rs`. The last one is composition, see Task 14.

- [ ] **Step 1: Change the types** in `shard_routing.rs`, `shard_scheduler.rs` and `runtime/shard_heartbeat.rs` to `Arc<dyn ShardMapPort>`. Construction sites coerce `Arc<RocksDbShardMap>` to it.
- [ ] **Step 2: Check the imports.** Run `grep -rn "RocksDbShardMap" src/application --include=*.rs | grep -v "cluster/bootstrap.rs" | grep -v "mod tests" `. Expected: only test-module hits.
- [ ] **Step 3: Run the full gate** (the sharding integration tests cover this), **then commit** with the message "Application depends on ShardMapPort".

### Task 13: Replication acknowledgement port for flush and drain

**Validated:**
- `flush_service.rs` uses `ReplicationLog::get_wal_ack`, `min_max_wal_ack_for_peers(&self, peer_ids: &[u64]) -> Result<(u64, u64), _>` (`replication_log.rs:213`), `min_mutation_ack` (:349) and `truncate_mutations_before` (:374).
- `cluster/drain.rs` uses `last_mutation_seq` (:344) and `get_mutation_ack` (:326).

**Files:**
- Create: `hyperbytedb/src/ports/replication_acks.rs`
- Modify: `flush_service.rs`, `cluster/drain.rs`, `adapters/cluster/replication_log.rs` (the impl)

**Interfaces:**
- Produces: a `pub trait ReplicationAcks: Send + Sync` with exactly those six methods. Copy their signatures verbatim from `replication_log.rs:164,213,326,344,349,374`. `impl ReplicationAcks for ReplicationLog` delegates to the inherent methods. `FlushServiceImpl::with_replication_log` takes `Arc<dyn ReplicationAcks>`.

- [ ] **Step 1: Add the trait and impl, then swap the two users.** Run `grep -rn "replication_log::ReplicationLog" src/application | grep -v "cluster/bootstrap.rs\|mod tests"`. Expected: no output.
- [ ] **Step 2: Run the full gate** (the flush-service truncation tests at `flush_service.rs:1043+` cover this), **then commit** with the message "Flush and drain depend on a replication-acks port".

### Task 14: Wire formats move to the domain; composition leaves `application`

**Validated:**
- The line-protocol point codec (`POINTS_FRAME_MAGIC` :48, `is_binary_points` :52, `POINT_ENCODED_SIZE_HINT` :82, `encode_bincode_into` :86 in `application/line_protocol.rs`) is used by the adapters `wal/batching_wal.rs` and `cluster/peer_client.rs`.
- `ColumnarMsgpackBatch` (`application/columnar_msgpack.rs:21`) is used by `ports/points_sink.rs`, the only port→application reference, and by `native_adapter.rs`.
- `application/cluster/{bootstrap,leader_monitor,raft_formation}.rs` build concrete adapters, and take `AppState` from `adapters::http::router`. They are composition, called from `bootstrap.rs:12` and `server.rs:447,467`, plus one test import (`tests/sharding_failover_integration.rs:13`).

**Files:**
- Create: `hyperbytedb/src/domain/point_codec.rs` and `hyperbytedb/src/domain/columnar_batch.rs`
- Create: `hyperbytedb/src/composition/mod.rs`, then move the three files there
- Modify: `lib.rs`, `application/cluster/mod.rs`, `bootstrap.rs`, `server.rs`, the test import, and `application/line_protocol.rs` / `application/columnar_msgpack.rs` (both keep `pub use` re-exports)

- [ ] **Step 1: Move the codec items and `ColumnarMsgpackBatch` into the domain** with re-exports, and point `batching_wal.rs`, `peer_client.rs`, `native_adapter.rs` and `ports/points_sink.rs` at the domain paths. Then run `grep -rn "crate::application" src/ports src/adapters/wal src/adapters/cluster/peer_client.rs`. Expected: no output.
- [ ] **Step 2: Move the three composition files** to `src/composition/` (`pub mod composition;` in `lib.rs`), and update their imports and callers. No re-export is left at the old path.
- [ ] **Step 3: Recount the layering.** Re-run the layer count: `python3 <scratchpad>/scan/layers.py src 'application>adapters'`. Expected: only the out-of-scope items listed at the end of this plan remain.
- [ ] **Step 4: Run the full gate, then commit** with the message "Wire formats in the domain; composition out of application".

---

## Phase E — Write-path cost

Measured on a release build, per batch of 5,000 Telegraf-shaped points with 10 fields each:
- parsing takes 24–27 ms;
- `split_field_type_conflicts` takes **5.7 ms**, and **1.3 ms** with Task 15's change;
- `PhysicalNameGuard::check_points` takes **1.35 ms**.

### Task 15: Field-type check without per-field allocation

**Validated:** `ingest_metadata.rs:705` runs `known.insert(k.clone(), widened)` for every field of every point.

- [ ] **Step 1: Write the test** `field_widens_then_conflicts_within_one_batch` (Review Focus 5). It passes today and pins the behaviour.
  - Use an empty store and one batch for `m`: field `f` as `Integer(1)`, then `Float(2.0)`, then `String("x")`.
  - Expect the first two kept, the third dropped, and `partial == Some(PartialWrite { dropped: 1, .. })`.
- [ ] **Step 2: Add the benchmark** `split_field_type_conflicts_5000x10` to `benches/ingestion_line_protocol.rs`, with the same body shape as above. Run `cargo bench --bench ingestion_line_protocol -- split_field_type` and record the baseline.
- [ ] **Step 3: Change the loop** to update known fields in place (`get_mut` and widen) and clone a key only when it is new. Re-run the bench: expect about a 4× reduction. Then run the full gate.
- [ ] **Step 4: Commit** with both numbers in the message.

### Task 16: Build the batch's name sets once

**Validated:** `PhysicalNameGuard::check_points` (`physical_names.rs`) walks every point to collect field and tag keys per measurement. `prepare_batch_metadata` (`ingest_metadata.rs` ~:735) walks every point again to build its per-measurement `(field types, tag keys)`. The guard also takes `checked.read()` once per hash (`physical_names.rs:159`).

**Interfaces:**
- Produces: `pub struct BatchSchema { pub measurements: HashMap<String, (HashMap<String, u8>, BTreeSet<String>)> }` with `pub fn from_points(points: &[Point]) -> Result<BatchSchema, HyperbytedbError>`, in `ingest_metadata.rs`. It returns the same in-batch `FieldTypeConflict` that `prepare_batch_metadata` raises today.
- The ingest paths change as follows:
  - `PhysicalNameGuard::check_schema(&self, metadata, db, rp, schema: &BatchSchema)` replaces `check_points`;
  - `prepare_batch_metadata_with_schema(metadata, db, rp, points, &schema, limits, schema_cache)` is the new entry the two client ingest paths call;
  - `prepare_batch_metadata` keeps its signature for the other callers, and builds the schema itself.

- [ ] **Step 1: Write the test** `batch_schema_matches_prepare_batch_metadata`. Using the Task 15 batch shape plus a second measurement, assert the keys and widened types per measurement.
- [ ] **Step 2: Add the benchmark** `name_guard_5000x10` and record the baseline.
- [ ] **Step 3: Implement.** Take the guard's read lock once per measurement, call `BatchSchema::from_points` once per request in `ingestion_service.rs` and `peer_ingestion_service.rs`, and pass the schema to both. Then run `cargo test --lib physical_names ingest_metadata` and `cargo test --test compat names_that_share_storage`. Expected: PASS.
- [ ] **Step 4: Re-run the bench** (expect the guard well under 0.1 ms), **run the full gate, then commit** with the numbers.

---

## Not in this plan (validated, deliberately deferred)

Each deferred item now has its own plan in this directory:
- `2026-11-03-peer-rpc-port.md`: the peer transport and Raft proposals;
- `2026-11-03-query-analysis-module.md`: the domain using the SQL translator;
- `2026-11-03-adapter-application-inversions.md`: driven adapters calling application code, plus the architecture test;
- `2026-11-03-split-large-functions.md`: very large functions;
- `2026-11-03-lint-and-test-support-cleanup.md`: the lower-value findings.


- **Peer transport port.** `PeerClient` (43 uses in application), direct `reqwest` calls in 11 application files, and Raft *proposals* (`client_write` with openraft's `ForwardToLeader` handling, in `peer_query_service.rs:129`, `drain.rs:252` and the scheduler at ~:2670) need a cluster-transport port designed as a whole. That redesign touches every cluster path and needs its own plan.
- **The domain using the SQL translator:** `domain/rollup.rs`, `domain/sharding/query_merge.rs` and `domain/sharding/multi_selector.rs` import from `timeseriesql::to_clickhouse`. `timeseriesql` sits outside the four layers, and moving this means splitting the translator.
- **Driven adapters calling application code:**
  - `native_adapter.rs:552` calls `backfill_tag_metadata`;
  - `sync_client` calls `wal_append`;
  - the Raft state machine calls `apply_schema_mutation`. That one is an adapter driving the application, the same as HTTP handlers do, and it is acceptable.
- **Splitting very large functions:** `execute_statement` (1,220 lines), the cluster `execute_query` (570) and `apply_shard_map_op` (422). That is refactoring with no defect behind it.
- **Lower-value findings:**
  - clippy's 39 lock scopes and 9 redundant clones (none on a hot path);
  - the 24 test-only helpers;
  - the per-batch `MeasurementMeta` copies, measured at microseconds and dropped.
