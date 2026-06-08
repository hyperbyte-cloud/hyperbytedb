# Deep Dive: Write Path

This document traces the HyperbyteDB write path from HTTP request to durable storage in embedded chDB MergeTree tables. It covers line protocol ingestion, WAL durability, the background flush pipeline, native table writes, and cluster replication.

---

## Table of Contents

1. [Overview](#1-overview)
2. [HTTP Write Handler](#2-http-write-handler)
3. [Ingestion Service](#3-ingestion-service)
4. [WAL Append](#4-wal-append)
5. [Flush Pipeline](#5-flush-pipeline)
6. [chDB Native Sink](#6-chdb-native-sink)
7. [Cluster Write Replication](#7-cluster-write-replication)
8. [Metrics](#8-metrics)

---

## 1. Overview

The write path has two phases:

**Phase 1 — Synchronous (client-blocking):** HTTP handling, line protocol parsing, metadata registration, and WAL append. The client receives `204 No Content` once the WAL append completes. Data is durable at this point.

**Phase 2 — Asynchronous (background):** The flush service reads WAL entries on a timer, groups them by measurement, INSERTs batches into chDB MergeTree tables, and truncates the WAL when safe.

```
Client POST /write
       |
       v
+-------------------------------+
| HTTP Handler (write.rs)       |
|  - Validate params            |
|  - Decompress gzip            |
|  - Cluster state check        |
+-------------------------------+
       |
       v
+-------------------------------+
| IngestionService              |
|  - Parse line protocol        |
|  - Register metadata          |
|  - Append to WAL              |
+-------------------------------+
       |
       v  (204 returned to client)
       |
       v  (background, every flush.interval_secs)
+-------------------------------+
| FlushService                  |
|  - Read WAL entries           |
|  - Group by (db, rp, meas)   |
|  - INSERT via PointsSinkPort  |
|  - Truncate WAL               |
+-------------------------------+
       |
       v
+-------------------------------+
| ChdbNativeAdapter             |
|  ReplacingMergeTree tables    |
|  under chdb.session_data_path |
+-------------------------------+
```

**Key source files:** `adapters/http/write.rs`, `application/ingestion_service.rs`, `application/flush_service.rs`, `adapters/chdb/native_adapter.rs`.

---

## 2. HTTP Write Handler

**File:** `src/adapters/http/write.rs`

### Entry point

`POST /write?db=mydb&rp=autogen&precision=ns`

### Steps

1. **Validate parameters** — `db` is required. Optional `rp` (defaults to the database default retention policy) and `precision` (nanoseconds by default).
2. **Decompress body** — Supports gzip-compressed payloads (`Content-Encoding: gzip`).
3. **Cluster gate** — In cluster mode, rejects writes when the node is draining or not accepting traffic.
4. **Delegate to ingestion** — Calls `IngestionPort::ingest()` with the raw body. In cluster mode, `PeerIngestionService` wraps the base service.

### Response

Returns `204 No Content` on success. Errors follow InfluxDB v1 JSON format (`{"error": "..."}`).

---

## 3. Ingestion Service

**File:** `src/application/ingestion_service.rs`

### Parse formats

| Format | Parser |
|--------|--------|
| Line protocol (default) | `parse_line_body_to_points()` via `influxdb-line-protocol` |
| MessagePack | `parse_msgpack_body_to_points()` |
| Columnar MessagePack (`columnar-ingest` feature) | Fast path: metadata from wire batch, then WAL serialization |

### Metadata registration

Before WAL append, `prepare_batch_metadata()` (or `prepare_columnar_metadata()` for columnar ingest):

1. Verifies the database exists.
2. Registers field types and tag keys for each measurement.
3. Enforces cardinality limits (`max_tag_values_per_measurement`, `max_measurements_per_database`).
4. Uses an in-memory schema cache to avoid redundant metadata reads.

Field types are enforced on subsequent writes — a type conflict returns HTTP 400.

### WAL entry construction

```rust
WalEntry {
    database: db.to_string(),
    retention_policy: retention_policy.clone(),
    points,
    origin_node_id: 0,  // set by replication apply path on peers
}
```

---

## 4. WAL Append

**Port:** `WalPort`  
**Adapter:** `RocksDbWal` (`adapters/rocksdb/wal.rs`)

### Structure

| Column Family | Purpose |
|---------------|---------|
| `wal` | Ordered entries keyed by big-endian `u64` sequence |
| `wal_meta` | `last_seq` counter |

Entries are `bincode`-serialized `WalEntry` values. Sequence numbers use big-endian encoding so RocksDB lexicographic order matches numeric order.

### Operations used by the write path

| Operation | Caller | Purpose |
|-----------|--------|---------|
| `append(entry)` | IngestionService | Durably store incoming points |
| `read_range(start, count)` | FlushService | Read up to 5,000 entries per chunk |
| `truncate_before(seq)` | FlushService | Remove flushed entries |
| `last_sequence()` | FlushService | Snapshot upper bound for a flush tick |

The WAL provides crash-safe durability between client acknowledgment and chDB INSERT.

---

## 5. Flush Pipeline

**File:** `src/application/flush_service.rs`  
**Port:** `FlushPort` (used by cluster drain)

### Timer

Runs every `flush.interval_secs` (default 10s) as a Tokio background task. Also listens on a shutdown `watch` channel for graceful stop.

### Flush cycle

1. **Snapshot** — Read `last_sequence()`; skip if nothing new since `last_flushed`.
2. **Read chunk** — `read_range(cursor + 1, 5000)` up to the snapshot sequence.
3. **Group** — Bucket points by `(database, retention_policy, measurement, origin_node_id)`.
4. **Sub-batch** — Split large groups by `max_points_per_batch` (10k–500k; auto-detected from available memory when config is 0).
5. **Write** — Spawn parallel tasks calling `PointsSinkPort::write_points()` for each sub-batch.
6. **Truncate** — `truncate_before(safe_seq + 1)` where `safe_seq` respects cluster replication acks.

### Cluster-aware truncation

When replication is enabled, truncation waits for peer WAL acks so lagging peers can still catch up:

- Uses `ReplicationLog::min_max_wal_ack_for_peers()` across active peers.
- If some peers have acked and others are still at 0, holds the WAL (returns safe seq 0).
- If all peers are at ack 0, applies `MAX_WAL_RETENTION_ENTRIES` (500k) as a safety valve.
- Pure replica nodes (no locally originated writes) skip the ack barrier.
- Stale peers (configurable heartbeat policy) can be excluded from the barrier.

### Drain

`FlushPort::drain()` loops flush until the WAL is empty — used during graceful cluster drain and shutdown.

---

## 6. chDB Native Sink

**Adapter:** `ChdbNativeAdapter` (`adapters/chdb/native_adapter.rs`)  
**Port:** `PointsSinkPort`

### Table naming

Each `(database, retention_policy, measurement)` maps to one physical table. Names are sanitised via `domain/chdb_naming` (for example `mydb_autogen_cpu`).

### Schema management

On flush, the adapter:

1. Loads `MeasurementMeta` from the metadata store.
2. Creates or alters the MergeTree table to match registered tag and field columns.
3. INSERTs the batch with `ReplacingMergeTree` ordering on `(time, tag columns…)`.

Tables live under `chdb.session_data_path` (configured in `[chdb]`).

### Query visibility

Data becomes queryable after flush INSERT completes. Until then it exists only in the WAL. With the default 10s flush interval, wait briefly after writing before querying.

---

## 7. Cluster Write Replication

**Files:** `application/peer_ingestion_service.rs`, `adapters/cluster/peer_client.rs`  
**Port:** `ReplicationPort`

In cluster mode, after the local WAL append succeeds:

1. `PeerIngestionService` serialises the line protocol body.
2. `PeerClient` POSTs to each active peer's `/internal/replicate` endpoint via `ReplicationPort`.
3. Peers apply the write through their own ingestion path with `origin_node_id` set to the source node.

Replication is asynchronous — the client receives 204 without waiting for peer confirmation. Failed sends are logged; hinted handoff can queue writes for unreachable peers.

WAL truncation on the origin node coordinates with peer acks (see [§5](#5-flush-pipeline)).

For the full replication and sync protocol, see [Deep Dive: Clustering](deep-dive-clustering.md).

---

## 8. Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `hyperbytedb_ingestion_points_total` | counter | Points ingested (label: `db`) |
| `hyperbytedb_wal_appends_total` | counter | WAL append operations |
| `hyperbytedb_ingest_parse_seconds` | histogram | Line protocol parse time |
| `hyperbytedb_ingest_metadata_register_seconds` | histogram | Metadata registration time |
| `hyperbytedb_ingest_wal_append_seconds` | histogram | WAL append time |
| `hyperbytedb_flush_points_total` | counter | Points flushed to chDB |
| `hyperbytedb_flush_runs_total` | counter | Flush cycles completed |
| `hyperbytedb_flush_duration_seconds` | histogram | Flush cycle duration |
| `hyperbytedb_flush_errors_total` | counter | Flush failures |
| `hyperbytedb_native_rows_written_total` | counter | Rows INSERTed by native adapter |
| `hyperbytedb_wal_last_sequence` | gauge | Last flushed WAL sequence |

---

## Related documents

- [Architecture](../developer-guide/architecture.md) — hexagonal overview and sequence diagrams
- [Read path](deep-dive-read-path.md) — query execution
- [Clustering](deep-dive-clustering.md) — replication, sync, drain
