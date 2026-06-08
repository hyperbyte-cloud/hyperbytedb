# Replication design and migration path

Aligned with `src/domain/cluster/replication_wire.rs`, `src/config.rs` (`[cluster.replication]`), `src/adapters/cluster/peer_client.rs`, and `src/adapters/http/peer_handlers.rs` (`/internal/replicate`, `/internal/replicate-mutation`).

## Current wire format

Write replication uses **`Content-Type: application/vnd.hyperbytedb.replicate+line.v1`** with an **Influx line protocol** body (same encoding as `POST /write`). Database, retention policy, and optional precision are carried in **`X-Hyperbytedb-DB`**, **`X-Hyperbytedb-RP`**, and **`X-Hyperbytedb-Precision`**. Constants and the hinted-handoff binary envelope live in [`replication_wire.rs`](../../../src/domain/cluster/replication_wire.rs).

There is **no JSON body** for data replication; metadata mutations still use JSON on `/internal/replicate-mutation` as a separate API.

Application services fan out writes through the [`ReplicationPort`](../../../src/ports/replication.rs) trait; [`PeerClient`](../../../src/adapters/cluster/peer_client.rs) is the production adapter.

## Per-node replication mode

Each node has a coordinator-side replication mode controlled by `[cluster.replication]` in `config.toml`:

| Mode | Behavior on accepted client write |
|------|-----------------------------------|
| `async` (default) | Local WAL append → fire-and-forget HTTP fan-out → return `204` to client. Failures trigger hinted handoff and retries; convergence is eventual. |
| `sync_quorum` | Local WAL append → fan-out with `X-Hyperbytedb-Sync: true` → await W-of-N peer acks → return to client. On timeout returns `504` and unacked peers fall back to hinted handoff in the background. |

`min_acks = "majority"` resolves at request time against current `active_peers().len()`, so the required count auto-adjusts during membership changes (rolling restart, peer crash, scale-out). The local node is **never** counted toward the quorum — the local WAL append is always done first, so self-durability is implicit.

### Mixed-mode safety

The mode controls **only the coordinator side**. Every node always serves both styles from the same `/internal/replicate` endpoint:

- Header absent or `false` → enqueue and return `204` immediately (today's behavior, byte-for-byte).
- Header `true` → enqueue, await the WAL apply, return `200 OK` with `{"ok":true,"ack_seq":<u64>}`.

This makes any combination of per-node modes safe at any moment, including during a rolling restart where some nodes have flipped to `sync_quorum` and others have not. A `sync_quorum` coordinator talking to a peer running `async` (or an older binary that ignores the header) still works — the peer 200s on success and the coordinator counts that as one ack.

### Sync wire details

- Header: `X-Hyperbytedb-Sync: true` (see `HTTP_HEADER_SYNC` in [`replication_wire.rs`](../../../src/domain/cluster/replication_wire.rs)).
- Receiver awaits the existing `ReplicationApplyQueue` oneshot before responding; the apply queue itself is unchanged and still bounded by `cluster.replicate_receiver_queue_depth`.
- Coordinator counts successes via a `select_all` loop and returns once `required` peers ack. The remaining per-peer tasks are NOT cancelled — they continue retrying so all peers eventually persist (and trip hinted handoff on failure).

## Target evolution

**Next step:** opaque **WAL / framed log shipping** (single encode on the writer, followers append bytes).

**Longer term:** **Raft-style or single-writer log** if master–master cost is too high.

## Hinted handoff

Hints are stored as **`CFh1`** binary payloads ([`ReplicationHintPayload`](../../../src/domain/cluster/replication_wire.rs)). Older JSON entries in RocksDB are **discarded on drain** (invalid magic).

## WAL truncation

See [`flush_service`](../../../src/application/flush_service.rs): per-peer ack watermarks and optional **stale peer** exclusion via `replication_truncate_stale_peer_multiplier` in cluster config.

## Operational knobs

`[cluster]` in `config.rs`: `replication_queue_depth`, `replication_max_inflight_batches`, `replication_max_coalesce_body_bytes`, `replicate_receiver_queue_depth`, `replication_truncate_stale_peer_multiplier`, etc.

## Flow control

- **Outbound:** bounded queue, WAL-sequence coalescing, semaphore on fan-out rounds.
- **Inbound:** [`ReplicationApplyQueue`](../../../src/application/replication_apply.rs); **503** when full.

## Cluster sync (metadata + WAL)

Startup and reconnect sync exchange **metadata snapshots** and **WAL tail entries** via `/internal/sync/{manifest,metadata,wal}`. Each peer builds local MergeTree tables by flushing replicated WAL entries.

See [Deep dive: clustering](../../deep-dive/deep-dive-clustering.md) for bootstrap, drain, and internal endpoints.
