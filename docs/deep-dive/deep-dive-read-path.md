# Deep Dive: Read Path

This document traces the HyperbyteDB query path from HTTP request to InfluxDB-compatible JSON response. It covers TimeseriesQL parsing, ClickHouse SQL translation against native MergeTree tables, chDB execution, tombstone filtering, and result formatting.

---

## Table of Contents

1. [Overview](#1-overview)
2. [HTTP Query Handler](#2-http-query-handler)
3. [TimeseriesQL Parser](#3-timeseriesql-parser)
4. [Query Dispatch](#4-query-dispatch)
5. [SELECT Execution](#5-select-execution)
6. [Tombstone Injection](#6-tombstone-injection)
7. [chDB Execution](#7-chdb-execution)
8. [Result Formatting](#8-result-formatting)
9. [Cluster Query Behavior](#9-cluster-query-behavior)
10. [Metrics](#10-metrics)

---

## 1. Overview

```
Client GET/POST /query
       |
       v
+------------------------------------+
| HTTP Handler (query.rs)            |
|  - Extract q, db, epoch, params    |
|  - Substitute bind parameters      |
|  - Delegate to QueryService        |
+------------------------------------+
       |
       v
+------------------------------------+
| QueryServiceImpl::execute_query()  |
|  - Parse TimeseriesQL              |
|  - Timeout wrapper                 |
|  - Dispatch each statement         |
+------------------------------------+
       |
       v (SELECT statements)
+------------------------------------+
| execute_measurement_query()        |
|  - Resolve native table name       |
|  - Translate AST → ClickHouse SQL  |
|  - Inject tombstone predicates     |
|  - Execute via chDB                |
|  - Parse JSONEachRow → Series      |
+------------------------------------+
       |
       v
+------------------------------------+
| chDB (embedded ClickHouse)         |
|  SELECT FROM MergeTree tables      |
|  Returns JSONEachRow               |
+------------------------------------+
```

**Key source files:** `adapters/http/query.rs`, `application/query_service.rs`, `timeseriesql/`, `adapters/chdb/query_adapter.rs`.

---

## 2. HTTP Query Handler

**File:** `src/adapters/http/query.rs`

### Entry points

- `GET /query?q=...&db=...`
- `POST /query` with form-encoded or query-string parameters

POST merges form body parameters into query string parameters (body takes precedence).

### Parameters

| Parameter | Purpose |
|-----------|---------|
| `q` | Query string (required) |
| `db` | Default database |
| `epoch` | Timestamp format: `ns`, `us`, `ms`, `s`, or RFC3339 strings |
| `chunked` | Enable chunked response for large result sets |
| `pretty` | Pretty-print JSON |
| `$param` | Bind parameter substitution in query text |

### Timeout

The handler wraps execution in `tokio::time::timeout` using `server.query_timeout_secs`.

---

## 3. TimeseriesQL Parser

**Module:** `src/timeseriesql/` (Influx-compatible query language)

The parser is a hand-rolled recursive descent parser in `parser.rs`. It splits multi-statement input on `;` and dispatches on the first keyword:

| First token | Statement type |
|-------------|----------------|
| `SELECT` | Measurement query |
| `SHOW` | Metadata introspection |
| `CREATE` / `DROP` / `ALTER` | DDL |
| `DELETE` | Tombstone mutation |
| `SET` / `GRANT` / `REVOKE` | Auth admin |

SELECT parsing builds a `SelectStatement` AST with fields, FROM clause, WHERE, GROUP BY (including `time(5m)` buckets), ORDER BY, LIMIT/OFFSET, SLIMIT/SOFFSET, fill options, and subqueries.

Expression parsing uses precedence climbing for `OR`, `AND`, comparisons, arithmetic, and function calls.

---

## 4. Query Dispatch

**File:** `src/application/query_service.rs`

`execute_query()` parses the input into `Vec<Statement>` and dispatches each:

| Statement | Handler |
|-----------|---------|
| `SHOW DATABASES` | `metadata.list_databases()` |
| `SHOW MEASUREMENTS` | `metadata.list_measurements()` |
| `SHOW TAG KEYS/VALUES` | Metadata indexes |
| `SHOW FIELD KEYS` | `MeasurementMeta` |
| `CREATE DATABASE` / `DROP DATABASE` | Metadata + cluster Raft mutation |
| `DELETE` | Store tombstone + replicate |
| `SELECT` | `handle_select()` → `execute_measurement_query()` |

SHOW and DDL statements never touch chDB tables directly — they read or mutate the RocksDB metadata store (and Raft log in cluster mode).

---

## 5. SELECT Execution

**Function:** `execute_measurement_query()`

### Steps

1. **Resolve measurement** — Extract name from FROM clause; handle regex measurements by querying metadata for matches and UNION ALL across tables.
2. **Retention policy** — Default RP from metadata when omitted.
3. **Native table name** — Build the physical table identifier via `domain/chdb_naming` (same naming as the write path).
4. **Translate** — `to_clickhouse::translate_native_table(stmt, &table, column_mapping)` converts the AST to ClickHouse SQL targeting the MergeTree table directly (not `file()` over external files).
5. **Tombstones** — Append `AND NOT (predicate)` for each stored tombstone (see [§6](#6-tombstone-injection)).
6. **Execute** — `QueryPort::execute_sql()` runs the SQL through chDB.
7. **Parse results** — Transform JSONEachRow into InfluxDB v1 series format.

### Translation highlights

| TimeseriesQL | ClickHouse |
|--------------|------------|
| `GROUP BY time(5m)` | `toStartOfInterval(time, INTERVAL 5 MINUTE) AS __time` |
| `mean("field")` | `avg("field")` |
| `now() - 1h` | `now64(9) - INTERVAL 1 HOUR` |
| `fill(null)` | `ORDER BY __time WITH FILL …` |
| Subqueries | Inline SELECT |

The internal alias `__time` avoids collision with the raw `time` column and is renamed back to `time` in the result parser.

---

## 6. Tombstone Injection

DELETE statements do not immediately remove rows from MergeTree tables. Instead:

1. The WHERE predicate is converted to a ClickHouse SQL fragment.
2. A tombstone record is stored in metadata: `tombstone:{db}:{measurement}:{uuid}`.
3. In cluster mode, the DELETE mutation is replicated via Raft.

On SELECT, `inject_tombstone_predicates()` loads all tombstones for the measurement and appends exclusion predicates to the WHERE clause. Deleted data is hidden at query time without rewriting stored rows.

---

## 7. chDB Execution

**Adapter:** `ChdbQueryAdapter` (`adapters/chdb/query_adapter.rs`)  
**Port:** `QueryPort`

### Session model

chDB's `Session` is synchronous and not `Sync`, so it runs inside `spawn_blocking` behind a `tokio::sync::Mutex`. The engine is a process-global singleton — `chdb.pool_size` is ignored; tune `server.max_concurrent_queries` instead.

### Output format

Queries use `FORMAT JSONEachRow`. Each line is a JSON object:

```json
{"__time":"2024-01-15 10:00:00","host":"server01","mean_usage_idle":42.5}
```

---

## 8. Result Formatting

The query service transforms JSONEachRow into InfluxDB v1 series format:

1. Parse each line as a JSON object.
2. Rename `__time` back to `time`.
3. Convert ClickHouse datetime strings to nanosecond timestamps.
4. Apply the `epoch` parameter (`ns`/`us`/`ms`/`s` integers or RFC3339 strings).
5. Group rows by tag combination into separate `SeriesResult` objects.
6. Apply SLIMIT/SOFFSET for series-level pagination.

### SELECT INTO

When the query includes an INTO clause, results are written back through the ingestion path as new points in the target measurement.

---

## 9. Cluster Query Behavior

Each node executes queries against its **local** chDB tables. There is no distributed scatter-gather for data queries — clients should route to a healthy peer or use a load balancer.

Schema mutations (CREATE DATABASE, DELETE tombstones, CQ definitions) go through Raft for ordering and are replicated to all peers.

For replication and sync that keeps peer data aligned, see [Deep Dive: Clustering](deep-dive-clustering.md).

---

## 10. Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `hyperbytedb_query_requests_total` | counter | Query requests received |
| `hyperbytedb_query_errors_total` | counter | Failed queries |
| `hyperbytedb_query_duration_seconds` | histogram | End-to-end query latency |

When enabled, `StatementSummary` records normalized query text and timing for `GET /api/v1/statements`.

---

## Related documents

- [Architecture](../developer-guide/architecture.md) — hexagonal overview
- [Write path](deep-dive-write-path.md) — how data reaches MergeTree tables
- [API & TimeseriesQL Reference](../user-guide/reference.md) — HTTP endpoints and syntax
