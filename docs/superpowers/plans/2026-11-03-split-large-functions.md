# Split the Largest Functions Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Break up the three largest functions with no behaviour change:
- `execute_statement` (1,228 lines);
- the cluster `execute_query` (578 lines);
- `apply_shard_map_op` (426 lines).

The cluster `execute_query`'s 18 hand-written replicate arms collapse onto the single mutation builder.

**Architecture:**
- **Cluster `execute_query`:** a golden test first records the exact mutation every arm replicates today. The arms are then replaced by one path: run the statement locally, and on success replicate `mutation_request_from_statement(...)`. Only arms whose golden output differs from the builder keep custom code.
- **The other two:** pure moves. Each match arm becomes a named function in a focused module, and the match becomes a dispatch table. Review moved code with `git diff --color-moved=zebra`: moved lines must show as moved, not changed.

**Tech Stack:** Rust 2024, serde_json (for golden snapshots).

**Spec:** the deferred item "splitting very large functions" in `docs/superpowers/plans/2026-11-03-code-health-fixes.md`, validated at `7ea9643`.

**Depends on:** code-health Task 1, which makes `mutation_request_from_statement` take `retention_policy` (Task 2 here needs it).

## Census (validated)

- **`application/query_service.rs:705-1932`** `async fn execute_statement(svc: &QueryServiceImpl, db: &str, stmt: &Statement, epoch: Option<&str>, query_rp: Option<&str>, statement_id: u32) -> Result<StatementResult, HyperbytedbError>` has 32 arms. The largest:
  - `ShowTagValues` (@856, 200 lines), `DropSeries` (@1774, 91), `Select` (@1135, 83);
  - `ShowTagKeys` (@790, 66), `ShowSeries` (@1221, 60), `Delete` (@1315, 60);
  - `ShowFieldKeys` (@1056, 55), `ShowRetentionPolicies` (@1561, 44), `ShowMaterializedViews` (@1460, 40);
  - `Revoke` (@1896, 36), `ShowSettings` (@1513, 35), `DropMeasurement` (@1281, 34);
  - `ShowContinuousQueries` (@1402, 33), `CreateRetentionPolicyStmt` (@1655, 32).

  The file is 4,023 lines.
- **`application/peer_query_service.rs:633-1210`** `execute_query` has 18 arms of the same shape: `execute_inner_statement(...)`, then on success `self.replicate_mutation(MutationRequest::X { .. })`.
  - The arms are `Delete`, `CreateContinuousQuery`, `DropContinuousQuery`, `CreateMaterializedView`, `DropMaterializedView`, `CreateRetentionPolicyStmt`, `DropRetentionPolicyStmt`, `CreateUser`, `SetPassword`, `AlterRetentionPolicyStmt`, `DropSeries`, `DropMeasurement`, `Grant`, `Revoke` and `SetSetting`, plus `CreateDatabase` and `DropDatabase`.
  - Known extras: `CreateRetentionPolicyStmt` (@861) binds `ref db` from the statement, shadowing the request's `db`, and derives the shard duration; `SetSetting` (@1168) runs `runtime_settings::validate`.
  - `mutation_request_from_statement` (`:402`) builds the same requests for the Raft path.
- **`domain/sharding/ops.rs:237-662`** `pub fn apply_shard_map_op(map: &mut ShardMap, op: ShardMapOp) -> Result<(), ShardMapApplyError>` has 12 arms: `Split` (66 lines), `RemovePeer` (55), `Merge` (42), `MovePeer` (40), `TransferPrimary` (38), `AddPeer` (36), `BootstrapMeasurementPresplit` (32), `PromotePeer` (29), `ActivateRegion` (26), `BootstrapMeasurement` (25), `ClearVerified` (22), `DropSpace` (8). It bumps `map.map_version` before the match.
- `PeerQueryService::new(inner, metadata, replication_port: Arc<dyn ReplicationPort>, peer_http)` (`peer_query_service.rs:48`). `ReplicationPort` (`ports/replication.rs`) has `replicate_write`, `replicate_write_sync`, `replicate_mutation`, `replicate_mutation_sync` and `replication_peer_count`, so a recording stub is possible. `MutationRequest` is `Serialize` (`domain/cluster/types.rs:9`).
- Not in scope: `FlushServiceImpl::flush` (263 lines). Its chunk paths are already extracted (`flush_prepared_chunk`), and what remains is ordering-sensitive loop control.

## Global Constraints

- No behaviour change, and the SQL and replicated mutations are byte-identical (except argon2 hashes, which are salted).
- `Cargo.lock` keeps chdb-rust 2.0.0; commits carry no Claude trailers.
- The gate: `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings`, `cargo test --lib`, `cargo test --no-fail-fast --test '*'`.

## Review Focus

1. **`CREATE RETENTION POLICY … ON otherdb` sent with `?db=mydb`.** The replicated `db` must stay whatever it is today. Task 1's golden test includes this exact statement.
2. **`CREATE USER` / `SET PASSWORD` replication.** It must carry a hash, never the plain password. The golden test asserts the `password_hash` field starts with `$argon2` and is not equal to the input.
3. **`SET <setting>` with an invalid value.** It must still be refused before anything replicates. The golden test includes one invalid `SET` and asserts nothing was recorded.
4. **Shard-map op version bumps.** A refused op must still bump `map_version` exactly as today; the bump stays in the dispatcher. Task 4 adds `refused_op_still_bumps_version`.
5. **`SHOW TAG VALUES` with `WITH KEY =~ /re/` and a `WHERE`** (its 200-line arm). The output must be unchanged; the compat `metadata_tests` cover it and must pass untouched.

---

### Task 1: Golden test of every replicated mutation

**Files:** create `hyperbytedb/tests/compat/cluster_replication_tests.rs` (add `mod cluster_replication_tests;` in `tests/compat/main.rs`; it needs `TestContext`, which lives in the integration-test crate) and the fixture `hyperbytedb/tests/compat/testdata/replicated_mutations.json`.

**Interfaces:**
- `struct RecordingReplication { seen: parking_lot::Mutex<Vec<MutationRequest>> }`, which implements `ReplicationPort` by recording in both `replicate_mutation` and `replicate_mutation_sync`. The write methods return `Ok` and `replication_peer_count` returns 0.

- [ ] **Step 1: Write the test** `every_cluster_mutation_replicates_as_before`.
  - Build `PeerQueryService::new(ctx.query_service.clone(), ctx.metadata.clone(), Arc::new(RecordingReplication::default()), &ClusterAuth::new(None)?)`, with `ctx` from `TestContext::new_no_chdb()`; that is non-Raft, as `raft` is never set.
  - Run one statement per arm (18), plus `CREATE RETENTION POLICY r ON otherdb DURATION 1d REPLICATION 1` sent with `db = "mydb"`, plus an invalid `SET`.
  - Serialize the recorded list with `serde_json`, masking every `password_hash` to `"<hash>"` after asserting it starts with `$argon2`.
  - Compare against the fixture. On the first run, write the fixture when the env var `UPDATE_GOLDEN=1` is set.
- [ ] **Step 2: Generate and check the fixture.** Run it on the unchanged code with `UPDATE_GOLDEN=1`, inspect the fixture by eye, and run again without the env var. Expected: PASS.
- [ ] **Step 3: Commit** with the message "Golden test of replicated cluster mutations".

### Task 2: Collapse the replicate arms onto the builder

- [ ] **Step 1: Replace the arms.** In `execute_query`, replace the 18 arms with:

  ```rust
  stmt if is_cluster_mutation(&stmt) => {
      let result = self.execute_inner_statement(db, stmt_query, epoch, retention_policy, caller, statement_id).await;
      if result.error.is_none() {
          let req = mutation_request_from_statement(&stmt, db, retention_policy, &self.metadata, self.shard_routing.get()).await?;
          self.replicate_mutation(req).await?;
      }
      result
  }
  ```

  Keep the `_ =>` fallback.
- [ ] **Step 2: Run the golden test.** For each statement whose output differs, the builder disagrees with the inline arm. Make the builder match the inline arm, since that is what has been replicating in non-Raft clusters, unless the inline behaviour is the bug code-health Task 1 fixed. Note each change in the commit message. Re-run until PASS.
- [ ] **Step 3: Run the gate,** especially `--test raft_integration --test sync_quorum_integration --test security_auth`, **then commit** with the message "Cluster statements replicate through one builder".

### Task 3: `execute_statement` becomes a dispatcher over statement modules

**Files:** `application/query_service.rs` becomes `application/query_service/mod.rs`, plus these new files under `application/query_service/statements/`:
- `show_schema.rs`: `ShowTagValues`, `ShowTagKeys`, `ShowSeries`, `ShowFieldKeys`, and any other `Show*` over measurement schema;
- `show_catalog.rs`: `ShowRetentionPolicies`, `ShowMaterializedViews`, `ShowContinuousQueries`, `ShowSettings`, and `ShowDatabases` / `ShowUsers` if present;
- `ddl.rs`: create/drop/alter of databases, retention policies, measurements, continuous queries and materialized views;
- `delete.rs`: `Delete`, `DropSeries`;
- `users.rs`: `CreateUser`, `DropUser`, `SetPassword`, `Grant`, `Revoke`;
- `select.rs`: `Select`.

**Interfaces:** every arm of 20 lines or more becomes `pub(super) async fn <snake_case_variant>(svc: &QueryServiceImpl, db: &str, <the statement's fields by reference>, epoch: Option<&str>, query_rp: Option<&str>, statement_id: u32) -> Result<StatementResult, HyperbytedbError>`. Drop any parameter the body does not use. Arms under 20 lines stay inline.

- [ ] **Step 1: Convert the file to a directory module** (`git mv`), then move arms one module at a time. After each module, run `cargo test --lib` and `cargo test --test compat`.
- [ ] **Step 2: Check the move is clean.** Run `git diff --color-moved=zebra -M origin/<base> -- src/application/query_service*`. Every moved body must show as moved; changed lines may only be the new function signatures, call sites and `use` lines.
- [ ] **Step 3: Check the size target.** `execute_statement` must be under 200 lines (count with `awk`). Run the full gate, then commit with the message "Split execute_statement into statement modules".

### Task 4: `apply_shard_map_op` becomes a dispatcher

**Interfaces:** each arm becomes `fn apply_<snake_op>(map: &mut ShardMap, <op fields by value>) -> Result<(), ShardMapApplyError>` in `domain/sharding/ops.rs` (or `ops/apply.rs` if the file passes 1,000 lines). The `map_version` bump stays in `apply_shard_map_op`, before the dispatch.

- [ ] **Step 1: Write** `refused_op_still_bumps_version` (Review Focus 4). Apply a `Merge` whose `epoch` does not match the left region's (refused with `ShardMapApplyError::StaleEpoch("Merge")`, `ops.rs:~393`) to a map at version 5, assert that `Err`, and assert `map.map_version == 6`. It passes today.
- [ ] **Step 2: Move the 12 arms,** and check with `--color-moved` as in Task 3.
- [ ] **Step 3: Run the gate,** especially `cargo test --lib domain::sharding` and the sharding integration tests, **then commit** with the message "Split apply_shard_map_op per op".
