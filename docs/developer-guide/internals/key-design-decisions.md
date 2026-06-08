# Key Design Decisions

Deep technical dives into the major subsystems of HyperbyteDB. Each section explains the design rationale, algorithm details, and implementation specifics.

---

## Write Path

The write path is split into two phases:

**Phase 1 (synchronous, client-blocking):** HTTP request → line protocol parsing → metadata registration → WAL append → 204 response. Data is durable at this point.

**Phase 2 (asynchronous, background):** Flush service reads WAL entries → groups by (db, rp, measurement) → INSERT batches into native MergeTree tables via `PointsSinkPort` → truncates WAL.

### WAL Design

The WAL uses RocksDB with two column families: `wal` (entries keyed by big-endian u64 sequence numbers) and `wal_meta` (single key `last_seq`). This encoding preserves numerical ordering in RocksDB's lexicographic key space. Entries are serialized with bincode for compact binary encoding.

The `BatchingWal` decorator provides optional group commit by batching multiple appends through a channel, reducing the number of RocksDB write operations under high concurrency.

### Metadata Registration

Before WAL append, the ingestion service registers schema information: field types (enforced on subsequent writes — type conflicts return HTTP 400), tag keys, tag values (for SHOW TAG VALUES), and cardinality limits. An in-memory `IngestSchemaCache` reduces repeated metadata lookups on the hot path.

### Flush Pipeline

```
WAL entries → group by (db, rp, measurement)
            → PointsSinkPort::write_points (chDB INSERT batches)
            → truncate WAL (cluster-aware when peers lag)
```

Flush batch sizing: `max_points_per_batch` defaults to 50k (ConfigMap / `[flush]` in config.toml). Explicit values are clamped to [10K, 500K]; `0` means use the default.

---

## Read Path

TimeseriesQL (Influx-compatible) queries are processed through a multi-stage pipeline:

1. **Parse** — Hand-rolled recursive descent parser produces an AST (`src/timeseriesql/`).
2. **Dispatch** — SHOW/DDL statements execute directly against metadata. SELECT statements proceed to translation.
3. **Schema lookup** — Load measurement metadata and tombstones for the target table(s).
4. **Translation** — AST → ClickHouse SQL against native MergeTree tables (`domain/chdb_naming` for table/column identifiers).
5. **Tombstone injection** — `AND NOT (predicate)` appended for each active tombstone.
6. **Execution** — chDB executes the SQL in `spawn_blocking`, returning JSONEachRow.
7. **Result parsing** — JSONEachRow → InfluxDB v1 series format (grouped by tag combination).

Key mappings: `MEAN` → `avg`, `FIRST` → `argMin(f, time)`, `LAST` → `argMax(f, time)`, `PERCENTILE(f, N)` → `quantile(N/100.0)(f)`. Transform functions (DERIVATIVE, MOVING_AVERAGE, etc.) use ClickHouse window functions (`lagInFrame`, windowed `avg`, `sum`).

Time buckets use `toStartOfInterval(time, INTERVAL N UNIT) AS __time`. The internal `__time` alias avoids collision with the raw `time` column.

Fill modes: `fill(null)` → `WITH FILL`, `fill(previous)` → `INTERPOLATE (col AS col)`, `fill(linear)` → `INTERPOLATE (col AS col USING LINEAR)`.

---

## Storage (chDB MergeTree)

Time-series data lives in embedded chDB under `chdb.session_data_path`. Each `(database, retention_policy, measurement)` tuple maps to a `ReplacingMergeTree` table. The native adapter (`ChdbNativeAdapter`) auto-creates and alters tables from measurement metadata on flush.

Table and column names are derived from line-protocol identifiers via `domain/chdb_naming` so ingestion, flush, and query translation agree on physical schema.

Background merge and part consolidation are handled by ClickHouse/chDB internally — there is no application-level compaction layer.

---

## Clustering

### Hybrid Replication Model

- **Data writes** — master-master replication via HTTP (`ReplicationPort`). Default mode is async fire-and-forget; optional sync-quorum waits for peer WAL acks. Hinted handoff queues writes for unreachable peers.
- **Schema mutations** — Raft consensus (OpenRaft) for consistent ordering. All nodes apply mutations in the same order.

### Node State Machine

```
Joining → Syncing → Active → Draining → Leaving
                      ↑           
                      └── Disconnected
```

### Cluster Convergence

Peers stay aligned through:

1. **Write replication** — line protocol fan-out after local WAL append.
2. **Startup / reconnect sync** — metadata snapshot + WAL catch-up via `/internal/sync/{manifest,metadata,wal}`.
3. **Local flush** — each peer replays replicated WAL entries into its own MergeTree tables.

There is no separate file-level repair loop; chDB state is rebuilt from the shared WAL/metadata contract.

### WAL Truncation Safety

In cluster mode, the flush service uses peer ack watermarks from `ReplicationLog` as the safe truncation point, ensuring lagging peers can still read needed entries.

### Graceful Drain

Sets Draining state (rejects writes), flushes WAL completely via [`FlushPort`](../../../src/ports/flush.rs), waits for peer acks (up to 60s), notifies peers of departure, and sets Leaving state.

---

## Replication Wire Format

Data replication uses `Content-Type: application/vnd.hyperbytedb.replicate+line.v1` with line protocol body. Database, RP, and precision in `X-Hyperbytedb-*` headers. No JSON for data replication; mutations use JSON on `/internal/replicate-mutation`.

Hinted handoff hints are stored as `CFh1` binary payloads.

---

## Authentication

Passwords hashed with Argon2id (random salt via `SaltString::generate(OsRng)`). Credential extraction order: query parameters (`u`/`p`) → HTTP Basic → Token header. Minimal hand-rolled Base64 decoder for Basic auth (no external dependency). Short TTL verification cache to avoid repeated Argon2 computations.

---

## See Also

- [Core Modules](core-modules.md) — Source code walkthrough
- [Extension Points](extension-points.md) — Adding new functionality
