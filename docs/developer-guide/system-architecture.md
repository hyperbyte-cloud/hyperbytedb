# HyperbyteDB system architecture

This document is a long-form internal design walkthrough. It complements the shorter [Architecture](architecture.md) page and the [deep dive series](../deep-dive/README.md). If anything disagrees with `src/`, prefer the code and the focused deep dive.

It is intended for contributors who need to understand how HyperbyteDB works under the hood.

**Canonical short reference:** [Architecture](architecture.md) and [Core modules](internals/core-modules.md).

---

## Table of Contents

1. [Architecture Overview](#1-architecture-overview)
2. [Hexagonal Architecture](#2-hexagonal-architecture)
3. [Module Structure](#3-module-structure)
4. [Write Path](#4-write-path)
5. [Query Path](#5-query-path)
6. [WAL (Write-Ahead Log)](#6-wal-write-ahead-log)
7. [chDB Storage](#7-chdb-storage)
8. [Metadata Store](#8-metadata-store)
9. [Flush Pipeline](#9-flush-pipeline)
10. [Background Merges](#10-background-merges)
11. [Query Engine (chDB)](#11-query-engine-chdb)
12. [TimeseriesQL Parser](#12-timeseriesql-parser)
13. [ClickHouse SQL Translator](#13-clickhouse-sql-translator)
14. [Retention Enforcement](#14-retention-enforcement)
15. [DELETE and Tombstones](#15-delete-and-tombstones)
16. [Continuous Queries](#16-continuous-queries)
17. [Authentication Internals](#17-authentication-internals)
18. [Clustering and Replication](#18-clustering-and-replication)
19. [Background Services](#19-background-services)
20. [Error Handling](#20-error-handling)
21. [Observability](#21-observability)
22. [Concurrency Model](#22-concurrency-model)
23. [Dependencies](#23-dependencies)
24. [Statement Summary](#24-statement-summary)
25. [Debug Binary](#25-debug-binary)
26. [Kubernetes Operator](#26-kubernetes-operator)

---

## 1. Architecture Overview

HyperbyteDB is a time-series database with InfluxDB v1 API compatibility, embedded chDB storage, RocksDB WAL/metadata, and optional master-master clustering.

```
 Client (Telegraf, Grafana, curl)
       │
       ▼
 ┌─────────────────────────────┐
 │    HTTP Layer (axum)        │  Line protocol, TimeseriesQL, auth, gzip
 └──────────────┬──────────────┘
                │
                ▼
 ┌─────────────────────────────┐
 │   Application Services      │  Ingestion, Query, Flush, Retention, CQ
 └──────────────┬──────────────┘
                │
                ▼
 ┌─────────────────────────────┐
 │   Port Traits (interfaces)  │  WalPort, QueryPort, MetadataPort,
 │                              │  PointsSinkPort, ReplicationPort
 └──────────────┬──────────────┘
                │
       ┌────────┴────────┐
       ▼                 ▼
 ┌──────────┐    ┌──────────────┐
 │ RocksDB  │    │ chDB         │
 │ WAL+meta │    │ MergeTree    │
 └──────────┘    └──────────────┘
```

**RocksDB** provides the WAL (durable, ordered write log) and metadata store (databases, measurements, schemas, users, tombstones, CQ definitions).

**chDB** (embedded ClickHouse) is both the query engine and storage backend. TimeseriesQL is transpiled to ClickHouse SQL; the flush service INSERTs WAL batches into per-measurement MergeTree tables under `chdb.session_data_path`.

---

## 2. Hexagonal Architecture

HyperbyteDB uses the hexagonal (ports and adapters) pattern. Business logic lives in the **application** and **domain** layers and depends only on port traits, never on concrete implementations.

```
                  ┌────────────────────────────────────┐
                  │            Domain Layer             │
                  │  Point, FieldValue, Database,       │
                  │  cluster/ DTOs, chdb_naming         │
                  └────────────────────────────────────┘
                                    │
                  ┌────────────────────────────────────┐
                  │         Application Services        │
                  │  IngestionService, QueryService,    │
                  │  FlushService, RetentionService,    │
                  │  cluster/ bootstrap, drain          │
                  └──────────┬────────────┬────────────┘
                             │  depends   │
                             │  only on   │
                             ▼  ports     ▼
           ┌─────────────────────────────────────────────┐
           │              Port Traits                     │
           │  WalPort  QueryPort  MetadataPort           │
           │  PointsSinkPort  ReplicationPort  FlushPort │
           └────────────┬────────────────┬───────────────┘
                        │                │
          ┌─────────────▼──┐    ┌────────▼───────────┐
          │   Adapters     │    │   Adapters          │
          │ (inbound)      │    │ (outbound)          │
          │ HTTP handlers, │    │ RocksDB WAL,        │
          │ Peer handlers  │    │ RocksDB Metadata,   │
          │                │    │ chDB query + sink,  │
          │                │    │ cluster/ peer IO    │
          └────────────────┘    └─────────────────────┘
```

This means:
- Swapping RocksDB for another WAL requires only implementing `WalPort`.
- Swapping chDB for another engine requires implementing `QueryPort` and `PointsSinkPort`.
- Application services use `ReplicationPort` and `FlushPort` instead of concrete cluster clients.
- The HTTP layer can be replaced without touching business logic.

---

## 3. Module Structure

```
src/
├── main.rs                          CLI, server bootstrap, graceful shutdown
├── lib.rs                           Top-level module declarations
├── bootstrap.rs                     Composition root: builds all adapters and services
├── config.rs                        Figment-based configuration loading
├── error.rs                         HyperbytedbError enum + From impls
│
├── domain/
│   ├── point.rs, database.rs, wal.rs, …   Core TSDB types
│   ├── chdb_naming.rs               Shared ClickHouse table/column naming
│   └── cluster/                     Membership, sync DTOs, replication wire types
│
├── ports/
│   ├── wal.rs, metadata.rs, query.rs, ingestion.rs, auth.rs
│   ├── points_sink.rs               Native MergeTree flush sink
│   ├── replication.rs               Outbound peer replication
│   └── flush.rs                     Graceful drain flush hook
│
├── application/
│   ├── ingestion_service.rs, query_service.rs, flush_service.rs, …
│   ├── replication_apply.rs, replication_dispatch.rs
│   └── cluster/                     bootstrap, drain, heartbeat, sync_manifest
│
├── timeseriesql/                    Parser, AST, ClickHouse translator
│
└── adapters/
    ├── http/                        Axum handlers + internal cluster endpoints
    ├── chdb/                        QueryPort + PointsSinkPort (native adapter)
    ├── wal/, metadata/, auth.rs
    └── cluster/                     peer_client, sync_client, replication_log, raft/
```

---

## 4. Write Path

The write path is optimized for low-latency ingestion. Data is durable the moment the WAL append returns, and becomes queryable after the next flush cycle. For an exhaustive treatment of every step, see [Deep Dive: Write Path](../deep-dive/deep-dive-write-path.md).

```
 Client POST /write?db=mydb&precision=ns
       │
       ▼
 ┌─────────────────────────────────────────┐
 │ write.rs: handle_write()                │
 │  1. Extract db, rp, precision params    │
 │  2. Gzip decompress if needed           │
 │  3. Call IngestionPort.ingest()         │
 └──────────────┬──────────────────────────┘
                │
                ▼
 ┌─────────────────────────────────────────┐
 │ ingestion_service.rs                    │
 │  1. Verify database exists (metadata)   │
 │  2. Parse line protocol body            │
 │  3. Convert ParsedLine → Vec<Point>     │
 │     - Apply precision to timestamps     │
 │     - Default to current time if absent │
 │  4. Register field types + tag keys     │
 │     in metadata                         │
 │  5. Check cardinality limits            │
 │  6. Store tag values for SHOW queries   │
 │  7. Append WalEntry to WAL             │
 └──────────────┬──────────────────────────┘
                │
                ▼
 ┌─────────────────────────────────────────┐
 │ rocksdb_wal.rs: append()               │
 │  1. Atomic fetch_add sequence number    │
 │  2. bincode::serialize(WalEntry)        │
 │  3. WriteBatch: put to "wal" CF +       │
 │     update "last_seq" in "wal_meta" CF  │
 │  4. Return sequence number             │
 └─────────────────────────────────────────┘
```

In cluster mode, `PeerIngestionService` wraps the base service and, after the local WAL append succeeds, fires off async HTTP POST requests to all peers via `PeerClient.replicate_write()`. The local write returns 204 immediately without waiting for replication.

### Data types

The `Point` struct carries:
- `measurement: String` — measurement name
- `tags: BTreeMap<String, String>` — sorted tag key-value pairs
- `fields: BTreeMap<String, FieldValue>` — field key-value pairs
- `timestamp: i64` — nanoseconds since Unix epoch

`FieldValue` has five variants:

| Variant | Discriminant | Storage type |
|---------|-------------|--------------|
| `Float(f64)` | 0 | Float64 |
| `Integer(i64)` | 1 | Int64 |
| `UInteger(u64)` | 2 | UInt64 |
| `String(String)` | 3 | String |
| `Boolean(bool)` | 4 | Boolean |

Field types are registered on first write and enforced on subsequent writes. A write that sends an integer where a float was previously registered returns a `FieldTypeConflict` error (HTTP 400).

---

## 5. Query Path

For an exhaustive treatment of every step, see [Deep Dive: Read Path](../deep-dive/deep-dive-read-path.md).

```
 Client GET /query?db=mydb&q=SELECT mean("value") FROM "cpu" WHERE time > now() - 1h GROUP BY time(5m)
       │
       ▼
 ┌────────────────────────────────────────────────────────────────┐
 │ query.rs: handle_query_impl()                                  │
 │  1. Extract q, db, epoch, pretty, chunked, params              │
 │  2. Substitute $param bind parameters if present               │
 │  3. Call QueryService.execute_query() with timeout wrapper      │
 │  4. Format response as JSON, CSV, or chunked                   │
 └──────────────┬─────────────────────────────────────────────────┘
                │
                ▼
 ┌────────────────────────────────────────────────────────────────┐
 │ query_service.rs: execute_query()                              │
 │  1. tokio::time::timeout wraps entire execution                │
 │  2. Parse TimeseriesQL string → Vec<Statement>                     │
 │  3. For each statement, dispatch:                              │
 │     ┌─────────────────────────────────────────────┐            │
 │     │ SHOW DATABASES  → metadata.list_databases() │            │
 │     │ SHOW MEASUREMENTS → metadata.list_meas()    │            │
 │     │ SHOW TAG KEYS   → metadata.get_meas()       │            │
 │     │ SHOW TAG VALUES → metadata.list_tag_values() │           │
 │     │ SHOW FIELD KEYS → metadata.get_meas()       │            │
 │     │ CREATE DATABASE → metadata.create_database() │           │
 │     │ DROP DATABASE   → metadata.drop_database()   │           │
 │     │ DELETE          → metadata.store_tombstone()  │           │
 │     │ SELECT          → see SELECT flow below       │           │
 │     └─────────────────────────────────────────────┘            │
 └──────────────┬─────────────────────────────────────────────────┘
                │ (SELECT only)
                ▼
 ┌────────────────────────────────────────────────────────────────┐
 │ handle_select()                                                │
 │  1. Extract measurement name from FROM clause                  │
 │  2. Handle regex measurements: query metadata for matches,     │
 │     execute UNION ALL across all matching measurements         │
 │  3. Handle subqueries: translate inner SELECT first            │
 │  4. Resolve default retention policy from metadata             │
 │  5. Determine time range from WHERE clause                     │
 │  6. Resolve native MergeTree table via chdb_naming              │
 │  7. Load tombstones for the measurement                        │
 │  8. Translate TimeseriesQL AST → ClickHouse SQL                │
 │     (to_clickhouse::translate_native_table)                    │
 │  9. Execute SQL via chDB (QueryPort.execute_sql)               │
 │ 10. Parse JSONEachRow output → SeriesResult[]                  │
 │     - Group by tag combinations                                │
 │     - Apply epoch formatting to timestamps                     │
 │ 11. Handle SLIMIT/SOFFSET (series-level pagination)            │
 │ 12. Handle INTO clause (write results to target measurement)   │
 └────────────────────────────────────────────────────────────────┘
```

### Result formatting

chDB returns data in **JSONEachRow** format (one JSON object per row):

```json
{"__time":"2024-01-15 10:00:00","host":"server01","mean_usage_idle":42.5}
{"__time":"2024-01-15 10:05:00","host":"server01","mean_usage_idle":38.2}
```

The query service transforms this into InfluxDB v1 series format:
1. Parse each line as a JSON object.
2. Rename `__time` back to `time`.
3. Convert ClickHouse datetime strings to nanosecond timestamps.
4. Apply the `epoch` parameter (convert to `ns`/`us`/`ms`/`s` integers or leave as RFC3339 strings).
5. Group rows by tag combination into separate `SeriesResult` objects.
6. Each `SeriesResult` gets a `name` (measurement), `tags` map, `columns` list, and `values` array.

---

## 6. WAL (Write-Ahead Log)

The WAL provides crash-safe durability for incoming writes before they're flushed into chDB MergeTree tables.

### Implementation: `RocksDbWal`

- **Backing store**: RocksDB with two column families.
- **Location**: Configured via `storage.wal_dir` (default `./wal`).

| Column Family | Purpose |
|---------------|---------|
| `wal` | Ordered WAL entries. Keys are big-endian `u64` sequence numbers (8 bytes). Values are `bincode`-serialized `WalEntry`. |
| `wal_meta` | Single key `last_seq` storing the current sequence number as big-endian `u64`. |

### WalEntry structure

```rust
struct WalEntry {
    database: String,
    retention_policy: String,
    points: Vec<Point>,
}
```

Serialized with `bincode` for compact binary encoding.

### Operations

| Operation | Description |
|-----------|-------------|
| `append(entry)` | Atomically increments sequence, serializes entry, writes to `wal` CF and updates `last_seq` in a single `WriteBatch`. Returns sequence number. |
| `read_from(seq)` | Forward iterator from `seq` to end. Returns `Vec<(u64, WalEntry)>`. |
| `read_range(start, count)` | Reads up to `count` entries starting at `start`. Used by flush service for chunked reads. |
| `truncate_before(seq)` | Deletes all entries with sequence < `seq` using `delete_range_cf`. |
| `last_sequence()` | Returns the current sequence number from the atomic counter. |

### Key encoding

Sequence numbers are encoded as **big-endian `u64`** so that RocksDB's lexicographic ordering preserves numerical order. This allows efficient range scans and truncation.

---

## 7. chDB Storage

Time-series data is stored in embedded chDB MergeTree tables under `chdb.session_data_path` (configured in `[chdb]`).

### Table layout

Each `(database, retention_policy, measurement)` maps to one physical table. Names are sanitised via `domain/chdb_naming` (for example `mydb_autogen_cpu`).

The native adapter (`ChdbNativeAdapter`, implementing `PointsSinkPort`) auto-creates and alters tables from `MeasurementMeta` on flush. Tables use `ReplacingMergeTree` ordered by `(time, tag columns…)`.

### Schema

Columns mirror the measurement schema registered in metadata:

| Column | Type | Notes |
|--------|------|-------|
| `time` | `DateTime64(9)` | Nanosecond timestamps |
| tag keys | `String` | One column per tag; collision-safe naming |
| fields | Float / Int / String / … | From registered field types |

---

## 8. Metadata Store

### Implementation: `RocksDbMetadata`

- **Backing store**: RocksDB column family `metadata`
- **Location**: `storage.meta_dir` (default `./meta`)
- **Serialization**: JSON values

### Key schema

| Key Pattern | Value | Description |
|-------------|-------|-------------|
| `db:{name}` | `Database` | Database + retention policies |
| `meas:{db}:{name}` | `MeasurementMeta` | Field types, tag keys |
| `tag_val:{db}:{meas}:{key}:{value}` | empty | Tag value index (SHOW TAG VALUES) |
| `user:{username}` | `StoredUser` | Auth credentials |
| `tombstone:{db}:{meas}:{uuid}` | predicate + timestamp | DELETE tombstones |
| `cq:{db}:{name}` | `ContinuousQueryDef` | Continuous query definitions |

---

## 9. Flush Pipeline

The flush service (`FlushServiceImpl`) bridges the WAL and chDB. It runs as a background Tokio task.

### Lifecycle

1. Timer tick every `flush.interval_secs` (default 10s).
2. Read WAL from `last_flushed_seq + 1` in chunks of 5,000 entries.
3. Group points by `(database, retention_policy, measurement)`.
4. Sub-batch by `max_points_per_batch` (auto-detected from memory when 0).
5. Call `PointsSinkPort::write_points` for each batch (INSERT into MergeTree).
6. Truncate WAL up to the flushed sequence (cluster-aware using peer acks when enabled).

In cluster mode, truncation waits until replication acks allow safe removal of entries peers may still need.

---

## 10. Background Merges

HyperbyteDB does not run an application-level compaction service. MergeTree part consolidation and background merges are handled internally by chDB/ClickHouse.

Retention deletes expired rows via `RetentionService` (`ALTER TABLE … DELETE`), not by deleting external files.

---

## 11. Query Engine (chDB)

HyperbyteDB uses **chDB** (embedded ClickHouse) as its query engine and storage backend. chDB provides the full ClickHouse SQL dialect including window functions and aggregates.

### Session management

`chDB::Session` is:
- `Send` but not `Sync` — requires `Mutex` wrapping.
- **Synchronous** — all calls block the thread, so they must run in `spawn_blocking`.

### Single session (`pool_size = 1`)

```rust
struct ChdbQueryAdapter {
    session: Arc<Mutex<Session>>,
}
```

Each query:
1. Clones the `Arc<Mutex<Session>>`.
2. `spawn_blocking` → `blocking_lock()` → `session.execute(sql, JSONEachRow)`.
3. Returns the UTF-8 result string.

### Session pool (`pool_size > 1`)

```rust
struct ChdbPool {
    sessions: Vec<Arc<Mutex<Session>>>,
    next: AtomicUsize,
}
```

Round-robin assignment. Each session has its own `session_data_path` subdirectory (`{base}/pool_{i}`). Multiple sessions allow concurrent queries without contending on a single Mutex.

### Output format

All queries use `OutputFormat::JSONEachRow` — one JSON object per result row. This is parsed by the query service into InfluxDB v1 series format.

---

## 12. TimeseriesQL Parser

The query language module is `src/timeseriesql/` (Influx-compatible TimeseriesQL).

The parser is a **hand-rolled recursive descent parser** (no parser generator). It lives in `src/timeseriesql/parser.rs`.

### Parse flow

```
Input: "SELECT mean(\"value\") FROM \"cpu\" WHERE time > now() - 1h; SHOW DATABASES"
                                    │
                                    ▼
                        split_statements(";")
                                    │
                        ┌───────────┼───────────┐
                        ▼                       ▼
              parse_statement()        parse_statement()
              first token = "SELECT"   first token = "SHOW"
                        │                       │
                        ▼                       ▼
                parse_select()         parse_show()
                        │                       │
                        ▼                       ▼
              SelectStatement          Statement::ShowDatabases
```

### Statement dispatch

The parser examines the first keyword (case-insensitive) and dispatches:

| First token | Handler |
|-------------|---------|
| `SELECT` | `parse_select()` |
| `SHOW` | `parse_show()` → further dispatch by second/third token |
| `CREATE` | `parse_create()` → `CREATE DATABASE`, `CREATE RETENTION POLICY`, `CREATE USER`, `CREATE CONTINUOUS QUERY` |
| `DROP` | `parse_drop()` → `DROP DATABASE`, `DROP MEASUREMENT`, etc. |
| `DELETE` | `parse_delete()` |
| `ALTER` | `parse_alter()` |
| `SET` | `parse_set_password()` |
| `GRANT` | `parse_grant()` |
| `REVOKE` | `parse_revoke()` |

### Expression parsing

The SELECT field list and WHERE clause use a **precedence-climbing expression parser**:

| Precedence | Operators |
|------------|-----------|
| 1 (lowest) | `OR` |
| 2 | `AND` |
| 3 | `=`, `!=`, `<>`, `<`, `<=`, `>`, `>=`, `=~`, `!~` |
| 4 | `+`, `-` |
| 5 | `*`, `/`, `%` |
| 6 (highest) | Unary `-`, `NOT` |

Atoms include: identifiers (`"column"` or bare), string literals (`'value'`), integer/float literals, duration literals (`1h`, `30s`), `now()`, function calls (`mean(...)`, `derivative(...)`), `*`, regex (`/pattern/`), and subqueries.

### Duration parsing

| Suffix | Duration |
|--------|----------|
| `ns` | Nanoseconds |
| `u` | Microseconds |
| `ms` | Milliseconds |
| `s` | Seconds |
| `m` | Minutes |
| `h` | Hours |
| `d` | Days |
| `w` | Weeks |

### AST types

Key AST nodes (in `src/timeseriesql/ast.rs`):

- `Statement` — enum of all statement types
- `SelectStatement` — fields, from, into, condition, group_by, order_by, limit, offset, slimit, soffset, fill, timezone
- `Field` — expression + optional alias
- `Expr` — recursive expression tree (identifiers, literals, function calls, binary/unary ops, subqueries)
- `FunctionCall` — name + args
- `GroupBy` — list of `Dimension` (Time, Tag, Regex)
- `FillOption` — Null, None, Previous, Linear, Value(f64)
- `Measurement` — optional database, optional RP, name or regex

---

## 13. ClickHouse SQL Translator

The translator (`src/timeseriesql/to_clickhouse.rs`) converts a TimeseriesQL `SelectStatement` AST into ClickHouse SQL that queries native MergeTree tables.

### Translation pipeline

```
 SelectStatement
       │
       ▼
 ┌─────────────────────────────────────┐
 │ SELECT clause                       │
 │  - Time bucket: toStartOfInterval() │
 │  - GROUP BY tags added to SELECT    │
 │  - Aggregate function mapping       │
 │  - Transform → window functions     │
 │  - fill(N) → ifNull() wrapping      │
 │  - Arithmetic expressions           │
 │  - Default aliases (mean_field)     │
 └──────────────┬──────────────────────┘
                │
 ┌──────────────▼──────────────────────┐
 │ FROM clause                          │
 │  - Native table: `db_rp_measurement` │
 │  - Subqueries become inline SELECTs │
 └──────────────┬──────────────────────┘
                │
 ┌──────────────▼──────────────────────┐
 │ WHERE clause                         │
 │  - now() → now64()                   │
 │  - Duration → INTERVAL              │
 │  - Epoch literals → fromUnixTimestamp│
 │  - Regex =~ → match()               │
 │  - Tombstone predicates appended    │
 │  - String comparisons preserved     │
 └──────────────┬──────────────────────┘
                │
 ┌──────────────▼──────────────────────┐
 │ GROUP BY clause                      │
 │  - time(5m) → toStartOfInterval()   │
 │  - Tag dimensions                    │
 └──────────────┬──────────────────────┘
                │
 ┌──────────────▼──────────────────────┐
 │ ORDER BY clause                      │
 │  - WITH FILL for fill modes          │
 │  - INTERPOLATE for fill(previous)    │
 │    and fill(linear)                  │
 └──────────────┬──────────────────────┘
                │
 ┌──────────────▼──────────────────────┐
 │ LIMIT / OFFSET                       │
 └─────────────────────────────────────┘
```

### Function mapping

| TimeseriesQL | ClickHouse |
|----------|------------|
| `MEAN(f)` | `avg(f)` |
| `MEDIAN(f)` | `median(f)` |
| `COUNT(f)` | `count(f)` |
| `SUM(f)` | `sum(f)` |
| `MIN(f)` | `min(f)` |
| `MAX(f)` | `max(f)` |
| `FIRST(f)` | `argMin(f, time)` |
| `LAST(f)` | `argMax(f, time)` |
| `PERCENTILE(f, N)` | `quantile(N/100.0)(f)` |
| `SPREAD(f)` | `(max(f) - min(f))` |
| `STDDEV(f)` | `stddevPop(f)` |
| `MODE(f)` | `topKWeighted(1)(f, 1)` |
| `DISTINCT(f)` | `DISTINCT f` |

### Transform function translation

Transforms use ClickHouse window functions:

| TimeseriesQL | ClickHouse |
|----------|------------|
| `DERIVATIVE(f, 1s)` | `(f - lagInFrame(f, 1) OVER (ORDER BY __time)) / nullIf(dateDiff('second', lagInFrame(__time, 1) OVER ..., __time), 0) * scale` |
| `NON_NEGATIVE_DERIVATIVE(...)` | Same as above, wrapped in `greatest(..., 0)` |
| `DIFFERENCE(f)` | `f - lagInFrame(f, 1) OVER (ORDER BY __time)` |
| `MOVING_AVERAGE(f, N)` | `avg(f) OVER (ORDER BY __time ROWS BETWEEN N-1 PRECEDING AND CURRENT ROW)` |
| `CUMULATIVE_SUM(f)` | `sum(f) OVER (ORDER BY __time ROWS UNBOUNDED PRECEDING)` |
| `ELAPSED(f, unit)` | `dateDiff('unit', lagInFrame(__time, 1) OVER ..., __time)` |

### Time bucket translation

```sql
-- TimeseriesQL: GROUP BY time(5m)
-- ClickHouse:
toStartOfInterval(time, INTERVAL 5 MINUTE) AS __time

-- TimeseriesQL: GROUP BY time(1h, 15m)  -- offset
-- ClickHouse:
toStartOfInterval(time - INTERVAL 15 MINUTE, INTERVAL 1 HOUR) + INTERVAL 15 MINUTE AS __time
```

The internal alias `__time` avoids collision with the raw `time` column. It's renamed back to `time` in the result parser.

### Fill translation

| Fill mode | ClickHouse |
|-----------|------------|
| `fill(null)` | `ORDER BY __time WITH FILL FROM ... TO ... STEP INTERVAL ...` |
| `fill(none)` | No `WITH FILL` |
| `fill(0)` | `ifNull(agg, 0)` + `WITH FILL` |
| `fill(previous)` | `WITH FILL` + `INTERPOLATE (col AS col)` |
| `fill(linear)` | `WITH FILL` + `INTERPOLATE (col AS col USING LINEAR)` |

---

## 14. Retention Enforcement

The `RetentionService` runs every 60 seconds:

1. Lists all databases from metadata.
2. For each database, iterates retention policies.
3. For RPs with a finite `duration`, calculates the cutoff time: `now - duration`.
4. For each measurement, issues `ALTER TABLE {table} DELETE WHERE time < cutoff` via chDB.

Data in the WAL that has not yet been flushed is not affected by retention until it is inserted into MergeTree tables.

---

## 15. DELETE and Tombstones

DELETE uses a **tombstone-based** approach:

### On DELETE execution

1. Parse the DELETE statement to extract measurement name and WHERE predicate.
2. Convert the WHERE predicate to a ClickHouse SQL fragment.
3. Store a tombstone record in metadata:
   ```
   tombstone:{db}:{measurement}:{uuid} → {"predicate_sql": "time < ...", "created_at": "..."}
   ```
4. In cluster mode, replicate the DELETE mutation to all peers.

### On query execution

Before executing a SELECT query, the query service loads all tombstones for the measurement and appends `AND NOT (predicate)` for each tombstone to the WHERE clause.

Tombstones are applied at query time. Physical row removal is handled by retention enforcement and MergeTree background merges.

---

## 16. Continuous Queries

### Storage

CQ definitions are stored in metadata under `cq:{db}:{name}` keys:

```rust
struct ContinuousQueryDef {
    name: String,
    database: String,
    query_text: String,          // The full SELECT ... INTO ... statement
    resample_every_secs: u64,    // Execution interval
    resample_for_secs: u64,      // Look-back window
    created_at: String,          // RFC3339 timestamp
}
```

### Execution

The `ContinuousQueryService` runs a loop with a 10-second tick:

1. Load all CQ definitions from metadata across all databases.
2. For each CQ, check if `resample_every_secs` has elapsed since the last execution.
3. If due, execute the query via the `QueryService`.
4. The SELECT ... INTO ... clause in the query writes results to the target measurement.

### Cluster behavior

CQ create/drop operations are replicated to all peers via Raft. Execution is leader-only: only the current Raft leader runs the CQ scheduler, so downsampling writes happen once per interval. When leadership transfers, the new leader picks up scheduling on its next tick. Single-node deployments without Raft run CQs locally.

---

## 17. Authentication Internals

### Password storage

Passwords are hashed using **Argon2** with random salts via `SaltString::generate(OsRng)`. The resulting hash string (in PHC format) is stored in the metadata store under `user:{username}`.

### Credential extraction order

The auth middleware checks three sources in order:

1. **Query parameters**: `u` and `p`
2. **HTTP Basic auth**: `Authorization: Basic <base64(user:pass)>`
3. **Token auth**: `Authorization: Token user:pass`

The first match wins. If none match and auth is enabled, the request is rejected with 401.

### Base64 decoding

A minimal hand-rolled Base64 decoder is used (no external dependency) for parsing Basic auth headers.

### Verification

```rust
Argon2::default().verify_password(input_bytes, &stored_hash)
```

Uses the default Argon2id variant with parameters from the stored hash.

---

## 18. Clustering and Replication

### Model

HyperbyteDB uses **master-master (peer-to-peer) replication** for data writes, with **Raft consensus** (via `openraft`) for schema mutations. Every node accepts reads and writes. Data writes are replicated asynchronously to all peers. Schema-mutating operations (CREATE/DROP DATABASE, DELETE, user/CQ/RP management) are routed through Raft to ensure consistent ordering across the cluster. For a comprehensive treatment, see [Deep Dive: Clustering](../deep-dive/deep-dive-clustering.md).

### Replicated operations

| Operation | Endpoint | Replication target |
|-----------|----------|-------------------|
| Write (line protocol) | `/internal/replicate` | All peers |
| CREATE DATABASE | `/internal/replicate-mutation` | All peers |
| DROP DATABASE | `/internal/replicate-mutation` | All peers |
| DELETE | `/internal/replicate-mutation` | All peers |
| CREATE USER | `/internal/replicate-mutation` | All peers |
| DROP USER | `/internal/replicate-mutation` | All peers |
| CREATE CONTINUOUS QUERY | `/internal/replicate-mutation` | All peers |
| DROP CONTINUOUS QUERY | `/internal/replicate-mutation` | All peers |
| CREATE RETENTION POLICY | `/internal/replicate-mutation` | All peers |

### Replication protocol

1. Client writes to node A.
2. Node A persists locally (WAL + metadata).
3. Node A returns 204 to the client.
4. Node A spawns an async task that POSTs to each peer's `/internal/replicate` endpoint.
5. Each POST includes a `X-Hyperbytedb-Replicated: true` header.
6. The receiving node checks for this header; if present, it persists locally but does **not** re-replicate (preventing loops).

### MutationRequest types

```rust
enum MutationRequest {
    CreateDatabase(String),
    DropDatabase(String),
    CreateRetentionPolicy { db, rp },
    CreateUser { username, password_hash, admin },
    DropUser(String),
    Delete { database, measurement, predicate_sql },
    CreateContinuousQuery { database, name, definition },
    DropContinuousQuery { database, name },
}
```

### Failure handling

- Replication is **fire-and-forget with logging**. If a peer is unreachable, the error is logged at WARN level.
- There is no retry queue or WAL replay for failed replications.
- On peer recovery, data can be re-synchronized via backup/restore from a healthy node.

### Network requirements

- All nodes must be reachable by all other nodes on their `cluster_addr` and port.
- The `peers` list should not include the node's own address (filtered at startup).
- HTTP timeout for replication requests: 10 seconds.

---

## 19. Background Services

HyperbyteDB runs four background services as Tokio tasks:

| Service | Interval | Purpose |
|---------|----------|---------|
| Flush | `flush.interval_secs` (default 10s) | WAL → chDB MergeTree INSERT |
| Retention | 60s (fixed) | `ALTER TABLE … DELETE` for expired rows |
| Continuous Query | 10s (fixed) | Execute CQ schedules |
| Cluster Heartbeat | 60s (fixed, cluster mode only) | Log cluster status |

All services listen on a `watch::Receiver<bool>` for graceful shutdown. On `ctrl+c`:

1. The shutdown signal is sent via `watch::channel`.
2. Each service finishes its current iteration.
3. The flush service performs one final flush.
4. The main task awaits all service handles.
5. Logs "HyperbyteDB shut down cleanly".

---

## 20. Error Handling

### HyperbytedbError

All internal errors are represented by the `HyperbytedbError` enum:

| Variant | HTTP Status | When |
|---------|-------------|------|
| `DatabaseNotFound(name)` | 404 | Query or write to non-existent DB |
| `RetentionPolicyNotFound(name)` | 404 | Reference to non-existent RP |
| `FieldTypeConflict{field, measurement, got, expected}` | 400 | Write sends wrong type for existing field |
| `LineProtocolParse{line, reason}` | 400 | Malformed line protocol |
| `QueryParse(msg)` | 400 | Invalid TimeseriesQL syntax |
| `AuthFailed` | 401 | Bad credentials |
| `DatabaseRequired` | 400 | `/write` without `db` parameter |
| `MissingParameter(name)` | 400 | `/query` without `q` parameter |
| `CardinalityExceeded{...}` | 422 | Tag cardinality limit hit |
| `QueryTimeout` | 408 | Query exceeded `query_timeout_secs` |
| `Wal(msg)` | 500 | RocksDB WAL error |
| `Storage(msg)` | 500 | File I/O or S3 error |
| `Chdb(msg)` | 500 | chDB execution error |
| `Metadata(msg)` | 500 | RocksDB metadata error |
| `Internal(msg)` | 500 | Serialization or other internal error |

Error responses follow InfluxDB v1 format:

```json
{"error": "database not found: \"nonexistent\""}
```

---

## 21. Observability

### Metrics

Uses the `metrics` crate with `metrics-exporter-prometheus`:

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `hyperbytedb_write_requests_total` | counter | — | Write requests received |
| `hyperbytedb_query_requests_total` | counter | — | Query requests received |
| `hyperbytedb_query_errors_total` | counter | — | Failed queries |
| `hyperbytedb_query_duration_seconds` | histogram | — | Query latency distribution |
| `hyperbytedb_ingestion_points_total` | counter | — | Points ingested |
| `hyperbytedb_flush_duration_seconds` | histogram | — | Flush cycle duration |
| `hyperbytedb_flush_points_total` | counter | — | Points flushed to chDB |

### Tracing

Uses `tracing` + `tracing-subscriber` with configurable filter levels via the `RUST_LOG` environment variable or the `[logging]` config section.

Structured JSON logging is available with `format = "json"`.

### Health endpoint

`GET /health` returns:
```json
{"status": "pass", "message": "ready for queries and writes"}
```

Always returns 200 as long as the HTTP server is running.

---

## 22. Concurrency Model

HyperbyteDB is built on **Tokio** with a multi-threaded runtime (`#[tokio::main]` with `features = ["full"]`).

### Thread usage

| Work | Thread type | Notes |
|------|------------|-------|
| HTTP request handling | Tokio async workers | Non-blocking |
| TimeseriesQL parsing | Tokio async workers | CPU-bound but fast |
| WAL operations | Tokio async workers | RocksDB ops are synchronous but fast |
| chDB query execution | `spawn_blocking` pool | chDB Session is synchronous |
| Native INSERT (flush) | `tokio::spawn` async tasks | Parallel batch writes |
| Peer replication | `tokio::spawn` async tasks | Non-blocking HTTP POSTs |

### Synchronization

| Resource | Mechanism |
|----------|-----------|
| WAL sequence number | `AtomicU64` (lock-free) |
| chDB Session | `tokio::sync::Mutex` (one per session) |
| Last flushed sequence | `tokio::sync::Mutex<u64>` |
| Shutdown signal | `tokio::sync::watch` channel |

---

## 23. Dependencies

### Core runtime

| Crate | Version | Purpose |
|-------|---------|---------|
| `tokio` | 1.x | Async runtime |
| `axum` | 0.8 | HTTP framework |
| `axum-server` | 0.7 | TLS support |
| `tower` / `tower-http` | 0.5 / 0.6 | Middleware (tracing, CORS, timeout) |
| `hyper` | 1.x | HTTP transport |

### Storage

| Crate | Version | Purpose |
|-------|---------|---------|
| `rocksdb` | 0.22 | WAL and metadata store |
| `chdb-rust` | 1.3 | Embedded ClickHouse query engine and native storage |
| `arrow` | 54 | Optional columnar ingest (`columnar-ingest` feature) |

### Serialization

| Crate | Version | Purpose |
|-------|---------|---------|
| `serde` / `serde_json` | 1.x | JSON serialization |
| `bincode` | 1.x | Binary WAL entry serialization |
| `serde_urlencoded` | 0.7 | Form-encoded POST body parsing |

### Parsing and protocol

| Crate | Version | Purpose |
|-------|---------|---------|
| `influxdb-line-protocol` | 2.x | Line protocol parsing |
| `regex` | 1.x | Regular expression support |

### Configuration

| Crate | Version | Purpose |
|-------|---------|---------|
| `figment` | 0.10 | Config from TOML + env vars |
| `clap` | 4.x | CLI argument parsing |

### Observability

| Crate | Version | Purpose |
|-------|---------|---------|
| `tracing` / `tracing-subscriber` | 0.1 / 0.3 | Structured logging |
| `metrics` / `metrics-exporter-prometheus` | 0.24 / 0.16 | Prometheus metrics |

### Auth and crypto

| Crate | Version | Purpose |
|-------|---------|---------|
| `argon2` | 0.5 | Password hashing |
| `rand_core` | 0.6 | Cryptographic RNG for salt generation |

### Utilities

| Crate | Version | Purpose |
|-------|---------|---------|
| `chrono` | 0.4 | Date/time handling |
| `uuid` | 1.x | Request IDs, tombstone keys |
| `bytes` | 1.x | Zero-copy byte buffers |
| `futures` | 0.3 | Async stream utilities |
| `async-trait` | 0.1 | Async trait methods |
| `thiserror` | 2.x | Error derive macros |
| `anyhow` | 1.x | Top-level error handling |
| `flate2` | 1.x | Gzip decompression |
| `reqwest` | 0.12 | HTTP client for peer replication |
| `openraft` | 0.10 | Raft consensus for schema mutations |
| `indexmap` | 2.x | Insertion-ordered maps |
| `crc32fast` | 1.x | CRC32 checksums (cluster sync verification) |
| `sha2` | 0.10 | SHA-256 hashing (query digest / canonical statement summary) |

---

## 24. Statement Summary

The `StatementSummary` service tracks recently executed TimeseriesQL statements for debugging and observability. When enabled (`statement_summary.enabled = true`), it records the normalized query text, digest, execution time, and error status for up to `max_entries` (default 1,000) recent statements in a bounded ring buffer. Results are exposed via `GET /api/v1/statements`.

---

## 25. Kubernetes Operator

The `hyperbytedb-operator/` directory contains a Go-based Kubernetes operator built with Kubebuilder. It defines a `HyperbytedbCluster` CRD for declarative multi-node cluster management, handling StatefulSet creation, peer configuration, and rolling updates.

---

## Deep Dive Documents

For detailed technical documentation on specific subsystems, see:

- [Deep Dive: Write Path](../deep-dive/deep-dive-write-path.md) — line protocol ingestion through chDB MergeTree INSERT
- [Deep Dive: Read Path](../deep-dive/deep-dive-read-path.md) — TimeseriesQL parsing, ClickHouse SQL translation, and query execution
- [Deep Dive: Compaction](../deep-dive/deep-dive-compaction.md) — MergeTree background merges (no application-level compaction service)
- [Deep Dive: Self-Repair](../deep-dive/deep-dive-self-repair.md) — WAL/metadata sync convergence between peers
- [Deep Dive: Clustering](../deep-dive/deep-dive-clustering.md) — Raft consensus, replication, and graceful drain
- [Developer guide](index.md) — contributing, building, testing, and extending HyperbyteDB
