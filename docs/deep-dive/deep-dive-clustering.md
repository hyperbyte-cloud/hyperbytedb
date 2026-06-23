# Deep Dive: Clustering

This document describes HyperbyteDB's clustering subsystem: node state machine, cluster bootstrap, startup and reconnect sync, Raft consensus for schema mutations, write replication, the replication log, and graceful drain.

## Table of Contents

1. [Architecture Overview](#1-architecture-overview)
2. [Node State Machine](#2-node-state-machine)
3. [Cluster Bootstrap](#3-cluster-bootstrap)
4. [Startup Sync](#4-startup-sync)
5. [Reconnect Sync](#5-reconnect-sync)
6. [Raft Consensus](#6-raft-consensus)
7. [Write Replication](#7-write-replication)
8. [Mutation Replication](#8-mutation-replication)
9. [Replication Log](#9-replication-log)
10. [Cluster Convergence](#10-cluster-convergence)
11. [Graceful Drain](#11-graceful-drain)
12. [Peer Internal Endpoints](#13-peer-internal-endpoints)
13. [Cluster Configuration](#14-cluster-configuration)
14. [Metrics](#15-metrics)

---

## 1. Architecture Overview

HyperbyteDB uses a **hybrid replication model**:

- **Data writes** are replicated to all peers (master-master). Each node independently accepts writes and fans them out via HTTP. The fan-out is **per-node configurable** as either `async` (fire-and-forget; default) or `sync_quorum` (await W-of-N peer acks before responding to the client). See [Section 7 -- Write Replication](#7-write-replication).
- **Schema mutations** (CREATE/DROP DATABASE, DELETE, user management, CQ management) are routed through **Raft consensus** (via `openraft`) to ensure consistent ordering across the cluster. Schema replication uses a separate code path and is not affected by the data replication mode.

```
            +---------+     +---------+     +---------+
            | Node 1  |<--->| Node 2  |<--->| Node 3  |
            +---------+     +---------+     +---------+
               |                |                |
               v                v                v
          Write repl.      Write repl.      Write repl.
          (async HTTP)     (async HTTP)     (async HTTP)
               |                |                |
               +-------+-------+-------+--------+
                       |               |
                  Raft (schema mutations)
                       |
              openraft consensus
```

### Key properties

- **Every node is readable and writable** -- clients can connect to any node.
- **Write replication is fire-and-forget** -- local write succeeds immediately; replication is best-effort with retries.
- **Schema mutations are consistent** -- Raft ensures all nodes apply mutations in the same order.
- **Self-healing** via WAL catch-up and metadata sync on startup/reconnect (see sections 4–5).
- **Graceful drain** ensures no data loss when removing a node.

---

## 2. Node State Machine

**File:** `src/domain/cluster/membership.rs`

Each node in the cluster transitions through the following states:

```
  Joining --> Syncing --> Active --> Draining --> Leaving
                 ^           |
                 |           v
                 +---- Disconnected
```

### States

| State | Description | Accepts Writes | Accepts Queries |
|-------|-------------|---------------|-----------------|
| `Joining` | Node is requesting to join the cluster | No | No |
| `Syncing` | Node is synchronizing data from a peer | No | No |
| `Active` | Node is fully operational | Yes | Yes |
| `Disconnected` | Node has missed heartbeats | No | Yes (stale) |
| `Draining` | Node is preparing to leave (flushing WAL, waiting for acks) | No | Yes |
| `Leaving` | Node has completed drain and is shutting down | No | No |

### State transitions

- **Joining -> Syncing:** After sending a join request to an active peer.
- **Syncing -> Active:** After successful data synchronization.
- **Active -> Disconnected:** When heartbeat misses exceed `heartbeat_miss_threshold`.
- **Disconnected -> Syncing:** When the node reconnects and needs to catch up.
- **Active -> Draining:** When a graceful shutdown is initiated.
- **Draining -> Leaving:** After the drain procedure completes.

### ClusterMembership

```rust
pub struct ClusterMembership {
    pub version: u64,                      // Monotonic version counter
    pub nodes: HashMap<u64, NodeInfo>,     // node_id -> NodeInfo
}

pub struct NodeInfo {
    pub node_id: u64,
    pub addr: String,
    pub state: NodeState,
    pub joined_at: i64,
    pub last_heartbeat: i64,
    pub needs_sync: bool,
}
```

- `version` is bumped on every mutation (`add_node`, `remove_node`, `set_state`).
- `SharedMembership = Arc<RwLock<ClusterMembership>>` provides thread-safe shared access.

### Key methods

| Method | Description |
|--------|-------------|
| `active_peers(exclude_id)` | Returns nodes with `state == Active`, excluding the given node |
| `all_peers(exclude_id)` | Returns all nodes except the given one |
| `next_node_id()` | Returns `max(node_ids) + 1` for assigning new node IDs |
| `set_needs_sync(node_id, bool)` | Marks a node for re-synchronization |

---

## 3. Cluster Bootstrap

**File:** `src/application/cluster/bootstrap.rs`

### `ClusterBootstrap::init(config)`

Called during application startup when `cluster.enabled = true`.

1. **Create replication log directory** if it doesn't exist.
2. **Open `ReplicationLog`** -- RocksDB-backed store for WAL and mutation ack tracking.
3. **Initialize membership** -- create `ClusterMembership` with the local node as `Active`.
4. **Build `PeerClient`** -- HTTP client for replication, configured with the peer list.
5. **Derive `peer_addrs`** from `config.cluster.peers` (comma-separated).

### `start_raft(config, metadata)`

1. Open `RaftStore` (RocksDB with `meta`, `logs`, `state` column families) in `config.raft_dir`.
2. Build Raft `Network` (HTTP transport).
3. Build `openraft::Raft<TypeConfig>` with configured heartbeat interval, election timeout, and snapshot threshold.
4. **If `node_id == 1`:** Initialize the Raft cluster with the local node as the sole voter.
5. Return `HyperbytedbRaft`.

### `run_startup_sync(config, metadata, wal, data_dir)`

1. Set node state to `Syncing`.
2. Build a `SyncClient`.
3. Determine if this is a new node (no existing data) or reconnect:
   - **New node:** Call `sync_client.join_and_sync()`.
   - **Existing node:** Call `sync_client.reconnect_sync()`.
4. Retry up to 5 times on failure.
5. On success: set state to `Active`.
6. On failure after retries: set state to `Active` with `needs_sync = true` (allows the node to operate but flags it for later re-sync).

---

## 4. Startup Sync

**File:** `src/adapters/cluster/sync_client.rs`

### `join_and_sync()` -- Full join flow for new nodes

1. **Pick a sync peer** via `pick_sync_peer()`:
   - First active peer from membership.
   - If none, probe fallback addresses from config.
2. **Send join request** -- `POST /internal/membership/join` with `JoinRequest { node_id, addr }`.
3. **Get manifest** — `GET /internal/sync/manifest` from the peer.
   - Returns `SyncManifest { node_id, wal_last_seq, databases }` with database/RP/measurement definitions.
4. **Sync metadata** — `GET /internal/sync/metadata`:
   - Imports databases, measurements, users, tombstones, and CQ definitions.
5. **WAL catchup** — `GET /internal/sync/wal`:
   - Stream WAL entries from the peer starting from the local sequence.
   - Append each entry to the local WAL; flush builds local MergeTree tables.
6. Set state to `Active`.

---

## 5. Reconnect Sync

**File:** `src/adapters/cluster/sync_client.rs`

### `reconnect_sync()` -- For existing nodes reconnecting after downtime

The reconnect strategy is chosen based on the WAL sequence gap between the local node and the peer:

```
Gap Analysis:
  local_wal_seq vs peer_wal_seq
       |
       v
  +-------------------+
  | gap == 0          | --> nothing to do
  | gap <= 10,000     | --> WAL catchup via /internal/sync/wal
  | gap > 10,000      | --> Full metadata sync + WAL catchup
  +-------------------+
```

### Gap == 0 (no missed writes)

No action required.

### Small gap (<= 10,000 entries)

WAL catchup from the peer (`GET /internal/sync/wal`), then mark `Active`.

### Large gap (> 10,000 entries)

Full metadata sync plus WAL catchup (same metadata/WAL endpoints as startup join).

---

## 6. Raft Consensus

**Files:** `src/adapters/cluster/raft/`

HyperbyteDB integrates `openraft` for schema mutation consensus.

### Type configuration

```rust
// src/adapters/cluster/raft/mod.rs
struct TypeConfig;
impl openraft::RaftTypeConfig for TypeConfig {
    type D = ClusterRequest;     // Log entry data
    type R = ClusterResponse;    // Apply result
    type NodeId = u64;
    type Node = BasicNode;
    type SnapshotData = Cursor<Vec<u8>>;
    type AsyncRuntime = TokioRuntime;
}

pub type HyperbytedbRaft = openraft::Raft<TypeConfig>;
```

### Cluster request types

```rust
// src/adapters/cluster/raft/types.rs
pub enum ClusterRequest {
    SetNodeState { node_id: u64, state: NodeState },
    SchemaMutation(MutationRequest),
}

pub struct ClusterResponse {
    pub ok: bool,
    pub message: String,
}
```

### RaftStore (log storage)

**File:** `src/adapters/cluster/raft/log_store.rs`

RocksDB-backed storage with three column families:

| Column Family | Purpose |
|---------------|---------|
| `meta` | Raft vote state, last purged log ID |
| `logs` | Raft log entries (key: log ID as big-endian u64) |
| `state` | State machine: last applied, membership, cluster data |

Implements `RaftStorage`, `RaftLogReader`, and `RaftSnapshotBuilder`.

### State machine application

**File:** `src/adapters/cluster/raft/state_machine.rs`

When a `ClusterRequest` is committed by Raft:

- **`SetNodeState`:** Updates the local `SharedMembership`.
- **`SchemaMutation`:** Calls the appropriate `MetadataPort` method:
  - `CreateDatabase` -> `metadata.create_database()`
  - `DropDatabase` -> `metadata.drop_database()`
  - `Delete` -> `metadata.store_tombstone()`
  - `CreateUser` -> `metadata.create_user()`
  - etc.

### Raft-to-membership synchronization

`sync_raft_membership_to_shared()` propagates Raft membership changes to the local `SharedMembership`. Raft voters are mapped to `Active` or `Syncing` states.

### Network transport

**File:** `src/adapters/cluster/raft/network.rs`

Raft messages are exchanged via HTTP:

| Endpoint | Purpose |
|----------|---------|
| `POST /internal/raft/append` | AppendEntries RPC |
| `POST /internal/raft/vote` | RequestVote RPC |
| `POST /internal/raft/snapshot` | InstallSnapshot RPC |
| `POST /cluster/raft/change-membership` | Membership change |

### Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| `raft_heartbeat_interval_ms` | 300 | Raft heartbeat interval |
| `raft_election_timeout_ms` | 1000 | Raft election timeout |
| `raft_snapshot_threshold` | 1000 | Log entries before snapshot |

---

## 7. Write Replication

**Files:** `src/application/peer_ingestion_service.rs`, `src/adapters/cluster/peer_client.rs`

### Per-node modes

`[cluster.replication]` selects this node's **coordinator** behavior. Receivers always serve both styles from the same `/internal/replicate` endpoint, so any combination of modes across the cluster is safe.

| Mode | Coordinator behavior |
|------|----------------------|
| `async` (default) | Fire-and-forget HTTP fan-out. Returns to the client immediately after the local WAL append. Failures retry and trip hinted handoff. |
| `sync_quorum` | Fan out with `X-Hyperbytedb-Sync: true` and await `W` peer acks before returning to the client. `W = sync_quorum.min_acks.resolve(active_peers)` -- self is never counted. |

### Async flow (`mode = "async"`)

1. Client writes to any node.
2. `PeerIngestionService::ingest()`:
   - Parse line protocol, register metadata, append to local WAL.
   - Dispatch to `PeerClient::replicate_write()` (spawns background task).
   - Return `204 No Content` to the client.
3. `PeerClient::replicate_write()`:
   - Push the batch onto the bounded outbound coalescer.
   - The outbound loop coalesces consecutive batches with the same `(db, rp, precision)` and contiguous `wal_seq` (up to `replication_max_coalesce_body_bytes`).
   - For each peer, spawn a task that POSTs to `/internal/replicate` with the line-protocol body, `X-Hyperbytedb-*` routing headers, and `X-Hyperbytedb-Replicated: true`.
   - On success (2xx): `replication_log.set_wal_ack(peer_id, wal_seq)`.
   - On failure: retry with exponential backoff; on exhaustion, push to hinted handoff.

### Sync quorum flow (`mode = "sync_quorum"`)

1. Client writes to any node. `PeerIngestionService::ingest()` parses, registers metadata, and appends to the local WAL.
2. Resolve `required = min_acks.resolve(active_peers().len())`. If `required == 0` (single-node cluster), return `Ok(())` immediately.
3. Acquire one shared inflight permit from `batch_semaphore` (sync and async share the same backpressure budget).
4. Spawn one per-peer task; each sets `X-Hyperbytedb-Sync: true` and parses the peer's `200 OK` body for `ack_seq`. Per-peer retry/backoff is identical to the async path.
5. Coordinator awaits acks via a `select_all` loop with deadline `ack_timeout_ms`:
   - Once `required` peers ack, return `204 No Content` to the client.
   - On timeout, return `504 Gateway Timeout` (`ReplicationQuorumTimeout`) -- but the in-flight peer tasks **continue** so durability still ratchets forward in the background.
6. Receiver: same `ReplicationApplyQueue` as the async path; the only difference is the handler awaits the apply oneshot before responding (see `handle_replicate_write` in `src/adapters/http/peer_handlers.rs`).

### Retry behavior (both modes)

- Base delay: 1 second.
- Max delay: 30 seconds.
- Max retries: `replication_max_retries` (default 5).
- Backoff: `min(delay * 2, 30s)`.
- On exhaustion: push to hinted handoff (when configured) and return failure.

### Mixed-mode and rolling restart

The mode is **per-node coordinator config**. Receivers ignore the mode entirely and react only to `X-Hyperbytedb-Sync`. Concretely:

- `[A=sync_quorum, B=async, C=async]`: A's writes await acks from B and C; B and C's writes are async as today. Safe.
- Rolling-restart node A out of `sync_quorum`: B and C in `sync_quorum` recompute `required` against the smaller `active_peers()` count and continue serving (e.g. 1 required peer ack instead of 2 in a 3-node cluster).
- A new `sync_quorum` coordinator talking to an old binary that ignores `X-Hyperbytedb-Sync` still works -- the old peer 200s on success, which counts as one ack.

### Loop prevention

The receiving node checks for `X-Hyperbytedb-Replicated: true`. If present, it:
- Persists the write locally (WAL + metadata).
- Does NOT re-replicate to other peers.

---

## 8. Mutation Replication

**File:** `src/adapters/cluster/peer_client.rs`

### With Raft

When Raft is configured, schema mutations are routed through Raft consensus:

```rust
raft.client_write(ClusterRequest::SchemaMutation(mutation_request)).await
```

Raft ensures all nodes apply the mutation in the same order.

### Without Raft (fallback)

Mutations are broadcast directly to all peers:

1. Append to the local replication log: `replication_log.append_mutation(request)` -> returns `seq`.
2. For each active peer:
   - `POST /internal/replicate-mutation` with `MutationReplicateRequest { seq, origin_node_id, mutation }`.
   - On success: `replication_log.set_mutation_ack(peer_id, seq)`.
   - On failure: retry with exponential backoff.

### MutationRequest variants

```rust
pub enum MutationRequest {
    CreateDatabase(String),
    DropDatabase(String),
    CreateRetentionPolicy { db: String, rp: RetentionPolicy },
    CreateUser { username: String, password_hash: String, admin: bool },
    DropUser(String),
    Delete { database: String, measurement: String, predicate_sql: String },
    CreateContinuousQuery { database: String, name: String, definition: ContinuousQueryDef },
    DropContinuousQuery { database: String, name: String },
}
```

### Deduplication

The receiving node calls `replication_log.check_and_record_mutation(origin_node_id, seq)`. This returns `true` only if `seq` is greater than the last applied sequence for that origin, preventing duplicate application.

---

## 9. Replication Log

**File:** `src/adapters/cluster/replication_log.rs`

The replication log is a RocksDB-backed store that tracks WAL and mutation acknowledgements from peers.

### Key layout

| Key Pattern | Value | Purpose |
|-------------|-------|---------|
| `repl_ack:{peer_id}` | `u64` (big-endian) | Last WAL sequence acked by peer |
| `mutation_log:{seq:016x}` | Serialized `MutationReplicateRequest` | Mutation log entry |
| `mutation_ack:{peer_id}` | `u64` (big-endian) | Last mutation seq acked by peer |

### Key operations

| Method | Description |
|--------|-------------|
| `set_wal_ack(peer_id, seq)` | Record WAL ack (only moves forward) |
| `get_wal_ack(peer_id)` | Get last WAL ack for a peer |
| `min_wal_ack()` | Minimum WAL ack across all peers (for safe truncation) |
| `append_mutation(request)` | Append mutation, return sequence number |
| `read_mutations_from(from_seq, max)` | Read mutation log entries |
| `set_mutation_ack(peer_id, seq)` | Record mutation ack |
| `check_and_record_mutation(origin_id, seq)` | Dedup: apply only if `seq > last` for origin |
| `truncate_mutations_before(seq)` | Delete old mutation log entries |
| `remove_peer(peer_id)` | Clear all ack keys for a peer |

### Safe WAL truncation

The flush service uses `min_wal_ack()` to determine how far the WAL can be safely truncated:

```
safe_truncate = min(chunk_max_seq, min_wal_ack_across_peers)
```

This ensures peers that are catching up can still read needed WAL entries.

---

## 10. Cluster Convergence

Peers stay aligned through three mechanisms:

1. **Write replication** — after local WAL append, the coordinator fans out line protocol to peers (`ReplicationPort` / `PeerClient`).
2. **Startup and reconnect sync** — `SyncClient` exchanges metadata snapshots and WAL tail entries via `/internal/sync/{manifest,metadata,wal}`.
3. **Local flush** — each peer replays its WAL (including replicated entries) into local MergeTree tables via `PointsSinkPort`.

There is no separate file-repair loop. chDB storage on each node is derived from the shared WAL + metadata contract.

## 11. Graceful Drain

**File:** `src/application/cluster/drain.rs`

The drain procedure ensures no data loss when removing a node from the cluster.

### `DrainService::drain()`

```
Step 1: Set node state to Draining
        (HTTP write handler starts rejecting writes with 503)
        |
Step 2: Flush all WAL entries to chDB MergeTree tables
        (flush_service.drain() via FlushPort -- loops until WAL is empty)
        |
Step 3: Wait for replication acks (up to 60 seconds)
        (loop: check if all peers have acked local WAL seq
         and mutation seq, sleep 2s between checks)
        |
Step 4: Notify peers of leave
        (POST /internal/membership/leave to all active peers)
        |
Step 5: Set node state to Leaving
```

(There is no Merkle verify step. Any tail divergence is reconciled by per-file CRC diff repair on the remaining peers.)

### Step details

**Step 1 -- Reject writes:** The HTTP write handler checks node state and returns `503 Service Unavailable` with an optional `X-Hyperbytedb-Redirect` header for `Draining`, `Leaving`, `Syncing`, or `Joining` states.

**Step 2 -- Flush WAL:** `flush_service.drain()` repeatedly calls `flush()` until no WAL entries remain. This ensures all ingested data is persisted in native MergeTree tables.

**Step 3 -- Wait for acks:** Polls `replication_log.get_wal_ack(peer_id)` and `get_mutation_ack(peer_id)` for each active peer. Waits until all peers have acked up to the local WAL and mutation sequence numbers. Times out after 90 seconds with a warning.

**Step 4 -- Notify peers:** Sends `POST /internal/membership/leave` to all active peers so they remove this node from their membership.

**Step 5 -- Set Leaving:** Final state transition. The node can be safely shut down.

---

## 12. Peer Internal Endpoints

**Files:** `src/adapters/http/peer_handlers.rs`, `src/adapters/http/raft_handlers.rs`

### Replication endpoints

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/internal/replicate` | POST | Receive replicated write data |
| `/internal/replicate-mutation` | POST | Receive replicated schema mutation |

### Membership endpoints

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/internal/membership/join` | POST | Handle join request from new node |
| `/internal/membership/leave` | POST | Handle leave notification |
| `/ping` | GET | Lightweight liveness probe used by the heartbeat updater (no body, just `200 OK`) |

### Sync endpoints

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/internal/sync/manifest` | GET | Sync manifest (metadata + WAL tail summary) |
| `/internal/sync/metadata` | GET | Metadata snapshot for joiners |
| `/internal/sync/wal` | GET | WAL entries for catch-up (`from_seq`, `max_entries`) |
| `/internal/sync/trigger` | POST | Trigger reconnect sync on a peer |

**Cluster consistency after replication:** WAL replication plus metadata/WAL sync on join/reconnect. Each peer builds local MergeTree tables by flushing replicated WAL entries.

### Raft endpoints

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/internal/raft/append` | POST | Raft AppendEntries RPC |
| `/internal/raft/vote` | POST | Raft RequestVote RPC |
| `/internal/raft/snapshot` | POST | Raft InstallSnapshot RPC |
| `/internal/raft/membership` | POST | Raft membership change |

### Drain endpoint

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/internal/drain` | POST | Trigger drain procedure |

---

## 13. Cluster Configuration

**File:** `src/config.rs` -- `ClusterConfig`

| Parameter | Default | Description |
|-----------|---------|-------------|
| `enabled` | `false` | Enable cluster mode |
| `node_id` | `1` | Unique node identifier |
| `cluster_addr` | `"127.0.0.1:8086"` | Address other nodes use to reach this node |
| `peers` | `""` | Comma-separated list of peer addresses |
| `heartbeat_interval_secs` | `2` | How often to send heartbeats |
| `heartbeat_miss_threshold` | `5` | Missed heartbeats before marking disconnected |
| `anti_entropy_interval_secs` | `60` | **Ignored.** Accepted for config compatibility; has no effect |
| `replication_log_dir` | `"./replication_log"` | RocksDB directory for replication tracking |
| `raft_dir` | `"./raft"` | RocksDB directory for Raft state |
| `sync_max_concurrent_files` | `4` | Max concurrent file downloads during sync |
| `replication_max_retries` | `5` | Max retries for failed replications |
| `raft_heartbeat_interval_ms` | `300` | Raft heartbeat interval (milliseconds) |
| `raft_election_timeout_ms` | `1000` | Raft election timeout (milliseconds) |
| `raft_snapshot_threshold` | `1000` | Log entries before Raft snapshot |
| `replication.mode` | `"async"` | Coordinator replication mode: `"async"` (default) or `"sync_quorum"` |
| `replication.ack_timeout_ms` | `5000` | `sync_quorum` worst-case latency budget; on timeout client gets `504` and unacked peers fall back to hinted handoff |
| `replication.sync_quorum.min_acks` | `"majority"` | Required PEER acks for `sync_quorum`. `"majority"` resolves to `floor(N/2)` peer acks at request time; integers are clamped to `active_peers().len()` |

---

## 14. Metrics

### Replication metrics

| Metric | Type | Description |
|--------|------|-------------|
| `hyperbytedb_replication_writes_total` | counter | Write replication attempts |
| `hyperbytedb_replication_errors_total` | counter | Failed write replications |
| `hyperbytedb_replication_duration_seconds` | histogram | Replication latency |
| `hyperbytedb_replication_mutations_total` | counter | Mutation replication attempts |
| `hyperbytedb_replication_mode{mode}` | gauge | `1` for the currently configured coordinator mode, `0` for the others (`mode` ∈ {`async`, `sync_quorum`}) |
| `hyperbytedb_replication_sync_acks_total{outcome}` | counter | Outcome of `sync_quorum` quorum waits (`outcome` ∈ {`ok`, `timeout`, `error`}) |
| `hyperbytedb_replication_sync_duration_seconds` | histogram | `sync_quorum` time from coordinator-accept to required-acks-received |
| `hyperbytedb_replication_sync_peer_ack_seconds{peer}` | histogram | Per-peer ack RTT in `sync_quorum` mode (helps spot the straggler) |
| `hyperbytedb_replication_sync_required_acks` | gauge | Currently resolved `required` peer-ack count (tracks membership changes) |
| `hyperbytedb_replication_sync_apply_received_total` | counter | Receiver count of inbound `sync_quorum` requests (where `X-Hyperbytedb-Sync: true`) |
| `hyperbytedb_replication_sync_apply_errors_total` | counter | Receiver errors while applying a `sync_quorum` request (returned as `500` to coordinator) |

### Cluster state metrics

| Metric | Type | Description |
|--------|------|-------------|
| `hyperbytedb_cluster_node_state` | gauge | Current node state (0=Joining, 1=Syncing, 2=Active, 3=Disconnected, 4=Draining, 5=Leaving) |
| `hyperbytedb_drain_total` | counter | Drain procedures initiated |
| `hyperbytedb_cluster_peers_active` | gauge | Number of active peers |
| `hyperbytedb_uptime_seconds` | gauge | Node uptime in seconds |
