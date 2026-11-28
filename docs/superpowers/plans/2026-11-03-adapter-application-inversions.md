# Adapter→Application Inversions and Layering Guard Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Driven adapters (storage, WAL, the sync client) stop calling into `application`. A test then enforces the layering rule, so new leaks fail CI.

**Architecture:** Each leak is fixed by moving the code it needs to where it belongs:
- `StageTimer` is instrumentation, so it moves to the crate-level `telemetry` module, which sits outside the layers.
- Tag backfill only uses `MetadataPort`, so it becomes a provided method on that port.
- The columnar timestamp helper is a wire-format rule, so it moves to the domain.
- `SyncClient` splits into an adapter part that fetches over HTTP (through `PeerRpc`) and an application `CatchUpService` that applies what was fetched.

Finally, `tests/architecture.rs` scans `src/` and fails on any forbidden import that is not on an explicit allowlist.

**Tech Stack:** Rust 2024, async-trait.

**Spec:** the deferred item "driven adapters calling application code" in `docs/superpowers/plans/2026-11-03-code-health-fixes.md`, validated at `7ea9643`.

**Depends on:**
- code-health Task 14, which moves `ColumnarMsgpackBatch` to `domain/columnar_batch.rs` and the point codec to `domain/point_codec.rs`;
- `2026-11-03-peer-rpc-port.md` Task 1 (`PeerRpc`), for Task 3 only.

## Census (validated)

The adapters' production imports of `crate::application`:
- `adapters/chdb/native_adapter.rs`:
  - `ingest_metadata::backfill_tag_metadata`, at `:56` and its call at `:552`, in `persist_new_series_metadata` (`:533`);
  - `columnar_msgpack::columnar_timestamps_ns` (`:1299`, `:2354`; defined at `application/columnar_msgpack.rs:60`);
  - `stage_timer::StageTimer` (`:1388`; defined at `application/stage_timer.rs:8`);
  - `ColumnarMsgpackBatch`, which code-health Task 14 handles.
- `adapters/cluster/sync_client.rs`:
  - `ingest_metadata::{IngestCardinalityLimits, prepare_batch_metadata}` (`:6`);
  - `wal_append::append_points_with_prepared` (`:7`, used at `:557`);
  - `runtime_settings::{RuntimeSettings, apply_or_store}` (`:32`, `:94`, `:467`).
- `adapters/cluster/raft/log_store.rs:21-23` (`MaterializedViewService`, `schema_mutation_apply`, `ShardRoutingContext`) and `raft/state_machine.rs` (`apply_schema_mutation`). **Allowed:** the Raft state machine drives the application, as HTTP handlers do.
- `adapters/http/*`: **allowed** (driving adapters).
- `adapters/wal/batching_wal.rs` and `adapters/cluster/peer_client.rs` use the point codec, which code-health Task 14 handles.

`SyncClient` (`adapters/cluster/sync_client.rs`, 792 lines) has:
- HTTP fetches:
  - `send_join_request` (`:268`, POST `/internal/membership/join`, `JoinRequest`);
  - `get_manifest` (`:308`, GET `/internal/sync/manifest`, `SyncManifest`);
  - the fetch half of `sync_metadata` (`:326`, GET `/internal/sync/metadata`, `MetadataSnapshot`);
  - the fetch half of `wal_catchup` (`:490`, GET `/internal/sync/wal?from_seq=&max_entries=5000`, `WalSyncResponse`);
  - `fetch_shard_map` (`:582`, GET `/internal/shard/map`, `ShardMap`).
- Application work: `reconnect_sync` (`:114`), `join_and_sync` (`:187`), `pick_sync_peer` (`:246`), `sync_shard_map` (`:369`), `import_metadata_entry` (`:403`), `append_catchup_entry` (`:539`) and `verify_catchup_progress` (`:606`).

All the wire types are in `domain/cluster/sync.rs`. `SyncClient` is constructed in `application/cluster/bootstrap.rs:122` (composition after code-health Task 14) and in `adapters/http/peer_handlers.rs:913`.

## Global Constraints

- No behaviour change, and endpoints, payloads and error variants are unchanged. `PeerUnreachable` vs `SyncFailed` stays mapped per call as today.
- `Cargo.lock` keeps chdb-rust 2.0.0; commits carry no Claude trailers.
- The gate: `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings`, `cargo test --lib`, `cargo test --no-fail-fast --test '*'`.

## Review Focus

1. **A node joining with a sharded map.** It must skip WAL pull as today. The existing `a_sharded_node_syncs_without_pulling_a_peer_wal` (`sync_client.rs:725`) must still pass after the split; it moves with `CatchUpService`.
2. **Synced users keep their grants.** The existing `a_synced_user_keeps_its_grants` (`:680`) moves too and must pass.
3. **Tag backfill for a measurement that does not exist yet.** It must create it with the tag keys, as `backfill_tag_metadata`'s `else` branch does. Task 1 tests the port method on both branches.
4. **The architecture test on a new leak.** Adding `use crate::adapters::...` in `domain/` must fail the test with the file and line. Task 4 includes a self-test of the scanner.

---

### Task 1: Tag backfill becomes a `MetadataPort` method; `StageTimer` moves to telemetry

**Files:** `ports/metadata.rs`, `application/ingest_metadata.rs` (`:344` and its caller at `:454`), `adapters/chdb/native_adapter.rs`, `application/stage_timer.rs`, `telemetry.rs`, and every `StageTimer` user: `native_adapter.rs`, `adapters/http/shard_handlers.rs`, `application/peer_ingestion_service.rs`, `application/query_service.rs`.

**Interfaces:**
- Produces: a provided trait method on `MetadataPort`: `async fn backfill_tag_keys(&self, db: &str, rp: &str, measurement: &str, tags: Vec<(String, String)>) -> Result<(), HyperbytedbError>`. Its body is today's `backfill_tag_metadata` body, using `self`.
- Produces: `crate::telemetry::StageTimer`, the same type with the same API. `application/stage_timer.rs` is deleted, not re-exported.

- [ ] **Step 1: Write the failing tests** `backfill_adds_missing_tag_keys` and `backfill_creates_unknown_measurement` in `adapters/metadata/rocksdb_meta.rs` tests:
  - existing measurement `cpu` with `tag_keys = ["host"]`, then backfilling `[("dc","x")]`, gives tag keys `["dc","host"]`;
  - unknown measurement `mem`, then backfilling `[("host","a")]`, gives `tag_keys ["host"]`, `field_types` empty.
- [ ] **Step 2: Implement.**
  - `backfill_tag_metadata` is deleted; its two callers call `metadata.backfill_tag_keys(..)`.
  - Move `StageTimer`.
  - Then run `grep -rn "crate::application" src/adapters/chdb`. Expected: only the `columnar_timestamps_ns` lines, which Task 2 handles.
- [ ] **Step 3: Run the gate, then commit** with the message "Tag backfill on the metadata port; StageTimer in telemetry".

### Task 2: Columnar timestamps in the domain

- [ ] **Step 1: Move the helper.** Move `columnar_timestamps_ns` (`application/columnar_msgpack.rs:60`) and any tests of it into `domain/columnar_batch.rs` (created by code-health Task 14; `Precision` is already domain, at `domain/database.rs:175`). Keep a `pub use` in `columnar_msgpack.rs`, and point `native_adapter.rs:1299`, `:2354` at the domain path. Then run `grep -rn "crate::application" src/adapters/chdb`. Expected: no output.
- [ ] **Step 2: Run the gate, then commit** with the message "Columnar timestamp rule lives with the columnar batch".

### Task 3: Split `SyncClient` into fetch (adapter) and catch-up (application)

**Files:**
- Create: `hyperbytedb/src/application/cluster/catch_up.rs`
- Modify: `adapters/cluster/sync_client.rs`, `ports/peer_rpc.rs`, `adapters/cluster/http_peer_rpc.rs`, and the two construction sites

**Interfaces:**
- Add to `PeerRpc`:
  - `async fn join(&self, addr: &str, req: &JoinRequest, timeout: Duration) -> Result<(), PeerRpcError>`
  - `async fn sync_metadata_snapshot(&self, addr: &str, timeout: Duration) -> Result<MetadataSnapshot, PeerRpcError>`
  - `async fn sync_wal(&self, addr: &str, from_seq: u64, max_entries: u32, timeout: Duration) -> Result<WalSyncResponse, PeerRpcError>`
  - `async fn shard_map(&self, addr: &str, timeout: Duration) -> Result<ShardMap, PeerRpcError>`

  (`sync_manifest` exists from peer-rpc Task 5.)
- Produces: `pub struct CatchUpService` in application, holding the fields `SyncClient` has today (`sync_client.rs:19`) with `client` replaced by `rpc: SharedPeerRpc`, and the same `pub` methods: `reconnect_sync`, `join_and_sync`, `sync_shard_map`. `SyncClient` is deleted once its last caller moves. `remote_map_is_newer` (`:578`) moves with it.

- [ ] **Step 1: Move the two existing tests** (`:680`, `:725`) and their `spawn_sync_peer` helper (`:630`) to `catch_up.rs`. They fail to compile until Step 2.
- [ ] **Step 2: Implement the `PeerRpc` methods and `CatchUpService`,** and migrate `application/cluster/bootstrap.rs:122` and `adapters/http/peer_handlers.rs:913`. Then run `grep -rn "crate::application" src/adapters/cluster/sync_client.rs src/adapters/cluster/http_peer_rpc.rs`. Expected: no output (the file is gone or HTTP-only).
- [ ] **Step 3: Run the gate,** especially `--test raft_integration --test integration`, **then commit** with the message "Catch-up sync is an application service over PeerRpc".

### Task 4: Architecture test

**Files:** create `hyperbytedb/tests/architecture.rs`.

**Interfaces:**
- `fn forbidden_imports(src_root: &Path) -> Vec<String>` returns `"<file>:<line>: <layer> imports <target>"` for every production (non-`#[cfg(test)] mod`) line matching `crate::(adapters|application|ports)` that breaks these rules:
  - `domain/` may import none of `adapters`, `application` or `ports`;
  - `ports/` may not import `adapters` or `application`;
  - `application/` may not import `adapters`;
  - `adapters/` may import `application` only from `adapters/http/` and `adapters/cluster/raft/{log_store,state_machine}.rs`.
- `const ALLOWLIST: &[&str]` holds `"file::target-prefix"` entries for the items `2026-11-03-peer-rpc-port.md` has not removed yet. Fill it from the test's first run, and leave a comment naming that plan beside each entry.

- [ ] **Step 1: Write** `scanner_flags_a_domain_adapter_import` (Review Focus 4). Run `forbidden_imports` on a temp dir holding `domain/x.rs` with `use crate::adapters::wal;`, and assert one finding naming `domain/x.rs:1`.
- [ ] **Step 2: Write** `layering_rules_hold`, which asserts `forbidden_imports("src")`, minus `ALLOWLIST`, is empty. Run it, and paste the remaining findings into `ALLOWLIST`. Each one must be one of the peer-rpc plan's items; anything else is a leak to fix now.
- [ ] **Step 3: Run the gate, then commit** with the message "Architecture test enforces the layering rules".
