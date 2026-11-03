# Peer RPC and Consensus Ports Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Application code stops building HTTP requests and stops holding the concrete `PeerClient` and Raft handle. Every node-to-node call goes through two ports: `PeerRpc` for typed calls to peers, and `ConsensusPort` for Raft proposals.

**Architecture:**
- **`PeerRpc`** has one typed method per internal endpoint. Methods return a `PeerRpcError` that keeps the HTTP status, because call sites act on 404, 405, 409, 429 and 503. A reqwest adapter implements it, reusing the pooled clients `PeerClient` already has.
- **`ConsensusPort`** extends the code-health plan's `LeadershipPort`. Its `propose` either writes to the local Raft leader or forwards to the leader, as `PeerQueryService::client_write_with_forward` does today.
- **Rollout:** call sites move one endpoint group at a time, so each task is a behaviour-neutral refactor that the sharding and Raft integration suites check.

**Tech Stack:** Rust 2024, async-trait 0.1, futures 0.3, bytes 1, reqwest 0.12 (no new features), openraft 0.9.

**Spec:** the deferred item "Peer transport port" in `docs/superpowers/plans/2026-11-03-code-health-fixes.md`, and the census below, taken at `7ea9643`.

**Depends on** these code-health tasks:
- Task 2 deletes `push_and_drop_range` / `request_region_rehome`, which are the only callers of `/internal/shard/rehome`.
- Task 8 adds `LeadershipPort`.
- Task 11 moves `ClusterRequest` / `ClusterResponse` to `domain/cluster/raft_request.rs`.
- Task 12 makes application use `ShardMapPort`.
- Task 14 moves `leader_monitor.rs` and `raft_formation.rs` out of application.

Re-run the census command in Task 1, Step 1 before starting. Line numbers will have moved.

## Census (validated at `7ea9643`)

These are the internal endpoints that application code calls today. "Sites" are file:line in `hyperbytedb/src/application/`; "status" lists the codes the site handles specially.

| Endpoint | Sites | Request | Response use | Status |
|---|---|---|---|---|
| `/internal/shard/bootstrap` | `shard_routing.rs:197` | `ShardBootstrapRequest` (`domain/sharding/types.rs:460`) | none | — |
| `/internal/shard/write` | `shard_routing.rs:899` | raw body bytes, headers as sent today | `text()` | 409, 503, 429 |
| `/internal/shard/write-batch` | `forward_coalescer.rs:258` | raw body bytes | `ShardWriteBatchResponse` (`types.rs:454`) | 404, 405, 503, 429 |
| `/internal/shard/query` | `query_service.rs:2517`, `:2605` | `ShardQueryRequest` (`types.rs:468`) | Arrow IPC **stream**, read with `resp.chunk()` (`query_arrow_series.rs:325`); the error body via `text()` | 409 |
| `/internal/shard/metadata` | `shard_metadata_scatter.rs:49` | `ShardMetadataRequest` (`types.rs:488`) | body (decode kept at the site) | 409 |
| `/internal/shard/delete` | `shard_routing.rs:1087` | `ShardDeleteRequest` (`types.rs:516`) | none | 409 |
| `/internal/shard/mv-backfill` | `sharded_mv_backfill.rs:182` | `ShardMvBackfillRequest` (`types.rs:548`) | none | 409, 503 |
| `/internal/shard/transfer/raw` | `shard_transfer.rs:687` | text body (`CONTENT_TYPE` text/plain) | `Response` | 404 means fall back to the JSON form |
| `/internal/shard/transfer` | `shard_transfer.rs:705` (push), `:739` (ack) | `ShardTransferPayload` (`domain/sharding/transfer.rs:26`; `ShardTransferPayload::ack(..)` for the ack) | body via `rejection_reason` (`shard_transfer.rs:1075`) | 404 |
| `/internal/shard/heartbeat` | `runtime/shard_heartbeat.rs:203` (`post_region_heartbeat`) | `RegionHeartbeat` (`types.rs:396`) | `"stale_epoch"` reason string | — |
| `/internal/mv/handoff` | `shard_scheduler.rs:2142` | `MvHandoffRequest` (`application/mv_handoff.rs:38`; **moves to domain**) | none | 404 |
| `/internal/sync/manifest` | `shard_scheduler.rs:3120` (`peer_region_wal_watermark`) | GET | `SyncManifest` (`domain/cluster/sync.rs`) from `text()` | — |
| `/internal/sync/trigger` | `cluster/hinted_handoff.rs:59` | POST, no body (unverified: read the site) | none | — |
| `/cluster/raft/client-write` | `peer_query_service.rs:292` (`forward_client_write`), `shard_routing.rs:965` (`propose_shard_map_op_via_raft`) | `ClusterRequest` | `text()` | — |
| `/health`, `/ping` | `cluster/heartbeat.rs:254-255` (`probe_peers`) | GET | `text()` / status | — |

Raft proposals made directly with `client_write`:
- `peer_query_service.rs:129` (`client_write_with_forward`, which retries 3 times and follows `ForwardToLeader`);
- `cluster/drain.rs:252`;
- `shard_scheduler.rs:~2667` (checks `resp.data.ok`).

Application currently uses `PeerClient` for:
- `membership()` (`peer_client.rs:164`, which returns `&SharedMembership` from `domain/cluster/membership.rs:235`);
- `http_client()` (`:193`);
- `cluster_auth()` (`:170`);
- `pending_hints()`.

Two port-like pieces exist already:
- `adapters/cluster/transport.rs` holds an `HttpTransport` / `ReqwestTransport` seam. Its header comment says it is not wired up, and it has no references outside the file.
- `ports/peer_http.rs` `PeerHttp` exposes `reqwest::ClientBuilder` in a port signature.

## Global Constraints

- The wire format of every endpoint is unchanged: path, method, headers, body and status codes. The server-side handlers are not touched.
- No new crate features. Stream the query body with `reqwest::Response::chunk()` inside `futures::stream::unfold`.
- `Cargo.lock` keeps chdb-rust 2.0.0; run `git checkout -- Cargo.lock` before each commit unless a task changes dependencies.
- Commits carry no Claude trailers.
- The gate for every task: `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings`, `cargo test --lib`, `cargo test --no-fail-fast --test '*'`, `cargo test -p hyperbytedb-cli`.

## Review Focus

1. **A transport error versus an HTTP error status.** "Unreachable" must stay distinct from "answered 503". Today the forward queue retries 503 and 429 but not other codes; Task 2's adapter test pins `PeerRpcError::Status { code: 503 }` against `Unreachable`.
2. **A shard query streaming a large Arrow result.** It must still be decoded chunk by chunk, not buffered whole. Task 3 tests a multi-chunk stub response.
3. **A leader change mid-proposal.** `propose` must follow `ForwardToLeader` and retry 3 times, as today. Task 6 tests it with a stub leadership/forwarder.
4. **The transfer push against an older peer without `/transfer/raw`.** A 404 must fall back to the JSON push. Task 4 keeps that branch and tests it with a stub returning 404.
5. **Shard deletes during a peer outage.** A failed delete must count as `failed` in `ShardDeleteOutcome` and not abort the fan-out. The existing `sharding_cluster_integration` delete tests cover this; Task 3 must keep them green.

---

### Task 1: Error type, port skeleton and reqwest adapter; remove the unused seams

**Files:**
- Create: `hyperbytedb/src/domain/cluster/peer_rpc.rs`, `hyperbytedb/src/ports/peer_rpc.rs`, `hyperbytedb/src/adapters/cluster/http_peer_rpc.rs`
- Delete: `hyperbytedb/src/adapters/cluster/transport.rs` (and its `pub mod transport;` at `adapters/cluster/mod.rs:9`)
- Move: `MvHandoffRequest` from `application/mv_handoff.rs:38` to `domain/sharding/types.rs`, with a `pub use` left in `mv_handoff.rs`

**Interfaces:**
- Produces: `pub enum PeerRpcError { Status { code: u16, body: String }, Unreachable(String) }`, with `impl From<PeerRpcError> for HyperbytedbError`:
  - `Status` becomes `PeerUnreachable(format!("HTTP {code}: {body}"))`;
  - `Unreachable(s)` becomes `PeerUnreachable(s)`.
- Produces: `#[async_trait] pub trait PeerRpc: Send + Sync`. Its methods are added by Tasks 3–5; this task adds the trait, `pub type SharedPeerRpc = Arc<dyn PeerRpc>`, and the adapter `pub struct HttpPeerRpc { peers: Arc<PeerClient> }` with two private helpers:
  - `async fn post_json<Req: Serialize + ?Sized>(&self, addr: &str, path: &str, req: &Req, timeout: Duration) -> Result<reqwest::Response, PeerRpcError>`;
  - `async fn get(&self, addr: &str, path: &str, timeout: Duration) -> Result<reqwest::Response, PeerRpcError>`.

  Both map a non-2xx status to `Status { code, body: resp.text() }` and a send error to `Unreachable`.

- [ ] **Step 1: Re-run the census** and update the table if it changed: `grep -rnE '"(http://\{[^}]*\})?/(internal|cluster|health|ping)' hyperbytedb/src/application`
- [ ] **Step 2: Write the failing adapter test** `status_and_transport_errors_stay_distinct` in `http_peer_rpc.rs`. Use an axum stub on an ephemeral port (as in `forward_coalescer.rs` tests, `:319`) that answers 503 with body `busy`. Assert:
  - `post_json` returns `Status { code: 503, body: "busy" }`;
  - against a closed port it returns `Unreachable(_)`.
- [ ] **Step 3: Implement the type, trait and adapter.** Delete `transport.rs`; `grep -rnw "HttpTransport\|ReqwestTransport"` must find nothing. Move `MvHandoffRequest`.
- [ ] **Step 4: Run the gate, then commit** with the message "PeerRpc port skeleton and reqwest adapter; drop the unused transport seam".

### Task 2: Wire the port in

**Files:** `ShardRoutingContext` (`application/shard_routing.rs:29`) and its construction sites; `ShardScheduler::new` (`shard_scheduler.rs:389`) and its 8 call sites; `application/runtime/shard_heartbeat.rs`; `application/query_service.rs` (`with_sharding`, `:125`).

**Interfaces:**
- `ShardRoutingContext` gains `pub peer_rpc: SharedPeerRpc` and `pub membership: SharedMembership`, next to `peer_client` (which is removed in Task 7). The scheduler and heartbeat reporter receive `SharedPeerRpc` the same way.

- [ ] **Step 1: Add the fields and parameters,** and construct `HttpPeerRpc::new(peer_client.clone())` where `PeerClient` is built today (composition code after code-health Task 14).
- [ ] **Step 2: Run the gate,** expecting no behaviour change, **then commit** with the message "Thread PeerRpc through sharding contexts".

### Task 3: Shard data path (bootstrap, write, write-batch, query, metadata, delete)

**Interfaces:** add to `PeerRpc`:
- `async fn shard_bootstrap(&self, addr: &str, req: &ShardBootstrapRequest, timeout: Duration) -> Result<(), PeerRpcError>`
- `async fn shard_write(&self, addr: &str, body: bytes::Bytes, timeout: Duration) -> Result<(), PeerRpcError>`. It sends exactly the headers `try_forward_shard_write_to_region` sends today.
- `async fn shard_write_batch(&self, addr: &str, body: bytes::Bytes, timeout: Duration) -> Result<ShardWriteBatchResponse, PeerRpcError>`
- `async fn shard_query(&self, addr: &str, req: &ShardQueryRequest, timeout: Duration) -> Result<futures::stream::BoxStream<'static, Result<bytes::Bytes, PeerRpcError>>, PeerRpcError>`
- `async fn shard_metadata(&self, addr: &str, req: &ShardMetadataRequest, timeout: Duration) -> Result<bytes::Bytes, PeerRpcError>`
- `async fn shard_delete(&self, addr: &str, req: &ShardDeleteRequest, timeout: Duration) -> Result<(), PeerRpcError>`

`series_from_ipc_response` (`query_arrow_series.rs:325`) changes its first parameter to `stream: BoxStream<'static, Result<Bytes, PeerRpcError>>`.

- [ ] **Step 1: Write the failing test** `shard_query_decodes_a_multi_chunk_stream` in `query_arrow_series.rs`. Feed `series_from_ipc_response` a `futures::stream::iter` of an Arrow IPC stream split into 3-byte chunks, and assert the same series as one whole chunk.
- [ ] **Step 2: Implement the six adapter methods.** The query stream is `futures::stream::unfold(resp, |mut r| async { r.chunk().await.transpose().map(|c| (c.map_err(..), r)) })`.
- [ ] **Step 3: Migrate the call sites.** Each site keeps its status handling, now written as a `match` on `PeerRpcError::Status { code, .. }` with the same codes:
  - `shard_routing.rs:197`, `:899`, `:1087`;
  - `forward_coalescer.rs:258`, which no longer takes a `reqwest::Client`; its `send_jobs` and `post_batch` take `&dyn PeerRpc`;
  - `query_service.rs:2517`, `:2605`;
  - `shard_metadata_scatter.rs:49`.
- [ ] **Step 4: Run the gate,** especially `cargo test --test sharding_cluster_integration --test sharding_integration`, **then commit** with the message "Shard data path goes through PeerRpc".

### Task 4: Transfers, backfill, handoff and heartbeats

**Interfaces:** add to `PeerRpc`:
- `async fn shard_transfer_push(&self, addr: &str, payload: &ShardTransferPayload, raw_body: Option<bytes::Bytes>, timeout: Duration) -> Result<bytes::Bytes, PeerRpcError>`. With a raw body it POSTs `/transfer/raw` (text/plain) first, and on a 404 falls back to the JSON payload, as `send_transfer_push` (`shard_transfer.rs:~674`) does today.
- `async fn shard_transfer_ack(&self, addr: &str, payload: &ShardTransferPayload, timeout: Duration) -> Result<(), PeerRpcError>`
- `async fn shard_mv_backfill(&self, addr: &str, req: &ShardMvBackfillRequest, timeout: Duration) -> Result<(), PeerRpcError>`
- `async fn mv_handoff(&self, addr: &str, req: &MvHandoffRequest, timeout: Duration) -> Result<(), PeerRpcError>`
- `async fn shard_heartbeat(&self, addr: &str, hb: &RegionHeartbeat, timeout: Duration) -> Result<(), PeerRpcError>`

`rejection_reason` (`shard_transfer.rs:1075`) takes `&PeerRpcError` instead of `reqwest::Response`; its 4 tests (`:1199-1234`) are rewritten to match.

- [ ] **Step 1: Write the failing test** `transfer_push_falls_back_to_json_on_404`. An axum stub answers 404 on `/internal/shard/transfer/raw` and 200 on `/internal/shard/transfer`. Assert one call to each path.
- [ ] **Step 2: Implement the five methods and migrate the sites:**
  - `shard_transfer.rs:687`, `:705`, `:739`;
  - `sharded_mv_backfill.rs:182`;
  - `shard_scheduler.rs:2142`;
  - `runtime/shard_heartbeat.rs:203`, keeping the `"stale_epoch"` reason mapping.
- [ ] **Step 3: Run the gate,** especially `--test sharding_mv_integration --test sharding_placement_integration`, **then commit** with the message "Transfers, backfill, handoff and heartbeats go through PeerRpc".

### Task 5: Cluster control (sync manifest and trigger, health probes)

**Interfaces:** add to `PeerRpc`:
- `async fn sync_manifest(&self, addr: &str, timeout: Duration) -> Result<SyncManifest, PeerRpcError>`
- `async fn sync_trigger(&self, addr: &str, timeout: Duration) -> Result<(), PeerRpcError>`. Copy the method and body from `hinted_handoff.rs:59` (not yet read).
- `async fn probe(&self, addr: &str, timeout: Duration) -> Result<String, PeerRpcError>`. It GETs `/health` and falls back to `/ping` exactly as `probe_peers` (`cluster/heartbeat.rs:254`) does; read that function first.

- [ ] **Step 1: Write the failing test** `probe_reports_health_body`, using an axum stub that serves `/health`.
- [ ] **Step 2: Implement and migrate:** `shard_scheduler.rs:3120` (`peer_region_wal_watermark`), `cluster/hinted_handoff.rs:59`, and `cluster/heartbeat.rs:254-255`. If `leader_monitor.rs` / `raft_formation.rs` are still in application (code-health Task 14 not landed), migrate their `:80`, `:157` and `:134` calls too.
- [ ] **Step 3: Run the gate, then commit** with the message "Cluster control calls go through PeerRpc".

### Task 6: ConsensusPort

**Files:** create `hyperbytedb/src/ports/consensus.rs` and `hyperbytedb/src/adapters/cluster/raft/consensus.rs`.

**Interfaces:**
- Produces: `#[async_trait] pub trait ConsensusPort: LeadershipPort { async fn propose(&self, req: ClusterRequest) -> Result<ClusterResponse, HyperbytedbError>; }`
- Adapter: `pub struct RaftConsensus { raft: HyperbytedbRaft, rpc: SharedPeerRpc, membership: SharedMembership }`. It does what `client_write_with_forward` (`peer_query_service.rs:121`) does today:
  - write locally when this node is leader;
  - on `ForwardToLeader`, POST `/cluster/raft/client-write` to the leader through a new `PeerRpc::raft_client_write(&self, addr: &str, req: &ClusterRequest, timeout: Duration) -> Result<ClusterResponse, PeerRpcError>`;
  - make at most 3 attempts.

  It returns the `ClusterResponse`; callers keep checking `.ok`, as `shard_scheduler.rs:~2675` does.

- [ ] **Step 1: Write the failing test** `propose_follows_forward_to_leader`. A stub `PeerRpc` records `raft_client_write` calls; a stub Raft is not possible, so drive `RaftConsensus` through its forwarding branch by factoring that branch into `async fn forward(&self, leader_addr: &str, req: &ClusterRequest) -> Result<ClusterResponse, HyperbytedbError>` and testing it directly. The 3-attempt loop is covered by `tests/raft_integration.rs`.
- [ ] **Step 2: Implement and migrate the proposers:**
  - `peer_query_service.rs` (`client_write_with_forward` and `forward_client_write` are deleted; the `raft_client: reqwest::Client` field at `:44` goes);
  - `cluster/drain.rs:252`;
  - `shard_scheduler.rs:~2667`;
  - `shard_routing.rs:960` `propose_shard_map_op_via_raft`.

  The scheduler's `raft: HyperbytedbRaft` field becomes `consensus: Arc<dyn ConsensusPort>`, and its `is_leader` (`:942`) uses it.
- [ ] **Step 3: Run the gate,** especially `--test raft_integration --test sharding_failover_integration`, **then commit** with the message "Raft proposals go through ConsensusPort".

### Task 7: Remove `PeerClient` and reqwest from application; ports free of reqwest

- [ ] **Step 1: Remove the remaining fields and imports.** Remove `ShardRoutingContext::peer_client` and the scheduler's `peer_client`; move `pending_hints` use in `cluster/hinted_handoff.rs` behind a `fn pending_hints(&self) -> usize` on `PeerRpc` (or keep `hinted_handoff.rs` in composition if code-health Task 14 moved it). Delete `ports/peer_http.rs` if `grep -rnw PeerHttp src` shows only the adapter impl; otherwise change its signature to not expose `reqwest`.
- [ ] **Step 2: Check what's left.** Each command must print nothing:
  - `grep -rn "reqwest::\|PeerClient\|HyperbytedbRaft\|openraft::" src/application --include=*.rs | grep -v "mod tests"`
  - `grep -rn "reqwest" src/ports`
- [ ] **Step 3: Remove the matching allowlist entries** from the architecture test added by `2026-11-03-adapter-application-inversions.md` Task 4, if it has landed.
- [ ] **Step 4: Run the gate, then commit** with the message "Application holds no HTTP client or Raft handle".
