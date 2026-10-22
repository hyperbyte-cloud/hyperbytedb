# API & TimeseriesQL Reference

Core HTTP routes for writes, queries, and health, plus TimeseriesQL. **Authentication** (which routes need credentials, `401` vs `403` on internal APIs, `Basic`/`Token`/`u`&`p`) is covered in [Authentication](authentication.md). For **cluster, replication, Raft, and internal** route lists, see [Cluster, replication, and internal routes](#cluster-replication-and-internal-routes) below; the authoritative list is [`src/adapters/http/router.rs`](../../src/adapters/http/router.rs).

---

## HTTP API

Every response carries these headers:

| Header | Value |
|--------|-------|
| `X-Hyperbytedb-Version` | The version, e.g. `0.9.3-beta` (from 0.9.3) |
| `X-Influxdb-Version` | `HyperbyteDB-<version>`, for InfluxDB clients and tools |
| `X-Influxdb-Build` | `OSS`, for InfluxDB clients and tools |
| `Request-Id` | UUID per request |
| `X-Request-Id` | Same UUID |

### POST /write

Write time-series data using InfluxDB line protocol.

**Query parameters:**

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| `db` | Yes | — | Target database name |
| `rp` | No | Default RP | Retention policy |
| `precision` | No | `ns` | Timestamp precision: `ns`, `us`/`u`, `ms`, `s` |
| `u` | No | — | Username (when auth enabled) |
| `p` | No | — | Password (when auth enabled) |

**Supports:** `Content-Encoding: gzip` for compressed payloads.

**Response codes:**

| Code | Meaning |
|------|---------|
| 204 | Success (empty body) |
| 400 | Parse error, or `partial write` (points with a field type conflict dropped, the rest stored) |
| 401 | Authentication failed (missing/invalid credentials when auth is enabled) |
| 404 | Database not found |
| 422 | Cardinality limit exceeded |
| 500 | Internal error |
| 503 | Node draining/syncing (cluster mode) |

### GET/POST /query

Execute TimeseriesQL queries.

**Query parameters:**

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| `q` | Yes | — | TimeseriesQL query string |
| `db` | Depends | — | Required for data queries |
| `epoch` | No | RFC3339 | Timestamp format: `ns`, `us`/`u`, `ms`, `s` |
| `pretty` | No | `false` | Pretty-print JSON |
| `chunked` | No | `false` | Stream results per statement |
| `params` | No | — | JSON object for `$param` bind parameters |

**Accept header:**

| Value | Response format |
|-------|-----------------|
| `application/json` (default) | JSON |
| `text/csv` or `application/csv` | CSV |

**POST body:** Parameters can be sent as `application/x-www-form-urlencoded`. Body parameters override query string parameters.

### GET/HEAD /ping

Returns `204 No Content` with version headers. Compatible with InfluxDB client library connection tests.

### GET /health

Returns JSON health status:
```json
{"status": "pass", "message": "ready for queries and writes"}
```

### GET /metrics

Prometheus-format metrics. See [Administration](administration.md#monitoring) for the full metric catalog.

### GET/DELETE /api/v1/statements

- **GET** — Returns recently executed statement summaries (when `statement_summary.enabled = true`).
- **DELETE** — Resets the statement summary ring buffer.

### Cluster-only endpoints (subset)

These are available when cluster mode is enabled and are often used with internal automation or `curl`.

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/cluster/metrics` | GET | Cluster mode info, node address, peer list |
| `/cluster/nodes` | GET | All nodes with a `self` flag |
| `/internal/drain` | POST | Initiate graceful node drain |

### Cluster, replication, and internal routes

The following apply when the corresponding features are compiled in and enabled. When **`[auth] enabled = true`**, these routes require **admin** credentials (same `u`/`p`, `Basic`, or `Token` as elsewhere); a logged-in **non-admin** user receives **403**. If auth is off, the HTTP layer does not check credentials—use network controls in production. Details: [Authentication](authentication.md#cluster--internal--raft-routes-admin-only).

**Cluster / peer (non-Raft)**

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/internal/replicate` | POST | Ingest-sized write replication from peer |
| `/internal/replicate-mutation` | POST | Schema/mutation replication |
| `/internal/membership` | GET | Membership view |
| `/internal/membership/join` | POST | Join cluster |
| `/internal/membership/leave` | POST | Leave cluster |
| `/internal/sync/manifest` | GET | Sync manifest (metadata + WAL tail summary) |
| `/internal/sync/metadata` | GET | Metadata snapshot for startup/reconnect sync |
| `/internal/sync/wal` | GET | WAL entries for catch-up |
| `/internal/sync/trigger` | POST | Trigger reconnect sync on a peer |

**Raft (when Raft is enabled)**

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/internal/raft/vote` | POST | Raft RPC |
| `/internal/raft/append` | POST | Raft RPC |
| `/internal/raft/snapshot` | POST | Raft RPC |
| `/cluster/raft/metrics` | GET | Raft metrics |
| `/cluster/raft/add-learner` | POST | Add learner node |
| `/cluster/raft/change-membership` | POST | Change membership |
| `/cluster/raft/client-write` | POST | Client write through Raft |
| `/cluster/leader` | GET | Current Raft leader |
| `/cluster/membership/add-node` | POST | Operator-style add node |
| `/cluster/membership/remove-node` | POST | Operator-style remove node |

**Series sharding (when `[sharding] enabled = true`)**

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/internal/shard/bootstrap` | POST | Register a measurement region (Raft leader) |
| `/internal/shard/write` | POST | Apply forwarded line protocol for a region |
| `/internal/shard/query` | POST | Execute region-scoped ClickHouse SQL |
| `/internal/shard/metadata` | POST | Region-scoped SHOW TAG KEYS/VALUES/SERIES |
| `/internal/shard/delete` | POST | Physical delete cleanup for a region |
| `/internal/shard/transfer` | POST | Receive WAL export during region transfer |
| `/internal/shard/heartbeat` | POST | Region stats to the Raft leader |
| `/internal/shard/map` | GET | Read-only shard map snapshot |

See [Deep Dive: Clustering — Series Sharding](../deep-dive/deep-dive-clustering.md#15-series-sharding-experimental) for scatter, failover, and epoch semantics.

### POST /api/v1/chdb

Execute raw ClickHouse SQL against the embedded chDB engine. **Admin-only** when auth is enabled.

**Query parameters:**

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| `db` | Yes | — | Target database name |

**Body:** Raw ClickHouse SQL as `application/x-www-form-urlencoded` with key `q`.

### GET /health/ready

Readiness probe: verifies the query engine (chDB) is usable. Used for Kubernetes readiness checks (see [Installation](installation.md)).

---

## TimeseriesQL Reference

### SELECT

```sql
SELECT <field_expression>[, ...] FROM <measurement>
  [INTO <target_measurement>]
  [WHERE <condition>]
  [GROUP BY time(<interval>[, <offset>])[, <tag_key>...]]
  [ORDER BY time [ASC|DESC]]
  [LIMIT <n>] [OFFSET <n>]
  [SLIMIT <n>] [SOFFSET <n>]
  [fill(<option>)]
  [TZ('<timezone>')]
```

**Field expressions:**
- Column references: `"usage_idle"`, `*`
- Aggregate functions: `mean("value")`, `count(*)`, etc.
- Transform functions: `derivative(mean("value"), 1s)`, etc.
- Arithmetic: `mean("a") * 100 + mean("b")`
- Aliases: `mean("value") AS avg_value`
- Type hints: `"host"::tag`, `"value"::field`

**Column names** follow InfluxQL: an alias wins; otherwise a function call is named after the function (`mean("value")` → `mean`), an expression after the fields and functions in it (`"a" + "b"` → `a_b`), and a repeated name gets `_1`, `_2` (`mean("a"), mean("b")` → `mean`, `mean_1`).

**The `time` column** is always first. Raw selects return each point's time and `GROUP BY time()` returns each bucket's start. An aggregate without `GROUP BY time()` returns one row stamped `1970-01-01T00:00:00Z`, whatever the `WHERE` time range; a lone `first()`, `last()`, `max()` or `min()` returns the selected point's own time.

**FROM clause:**
- Single: `FROM "cpu"`
- Fully-qualified: `FROM "mydb"."myrp"."cpu"`
- Regex: `FROM /^cpu.*/`
- Subquery: `FROM (SELECT mean("value") FROM "cpu" GROUP BY time(1h))`

### WHERE Operators

| Operator | Description | Example |
|----------|-------------|---------|
| `=` | Equals | `"host" = 'server01'` |
| `!=`, `<>` | Not equals | `"host" != 'server02'` |
| `<`, `<=`, `>`, `>=` | Comparison | `time > now() - 1h` |
| `AND`, `OR` | Logical | `"host" = 'a' AND time > now() - 1h` |
| `=~` | Regex match | `"host" =~ /^us-.*/` |
| `!~` | Regex not match | `"host" !~ /^eu-.*/` |

### Time Expressions

- `now()` — current server time
- Duration literals: `1ns`, `5u`, `100ms`, `30s`, `15m`, `2h`, `7d`, `4w`
- RFC3339: `'2024-01-01T00:00:00Z'`

### Fill Options

| Fill | Behavior |
|------|----------|
| `fill(null)` | Empty buckets filled with NULL |
| `fill(none)` | Empty buckets omitted |
| `fill(0)` | Empty buckets filled with 0 (or any numeric value) |
| `fill(previous)` | Forward-fill from last known value |
| `fill(linear)` | Linear interpolation between known values |

### Aggregate Functions

| Function | Description |
|----------|-------------|
| `MEAN(field)` | Arithmetic mean |
| `MEDIAN(field)` | Median value |
| `COUNT(field)` | Row count |
| `SUM(field)` | Sum of values |
| `MIN(field)` | Minimum value |
| `MAX(field)` | Maximum value |
| `FIRST(field)` | Value at earliest timestamp |
| `LAST(field)` | Value at latest timestamp |
| `PERCENTILE(field, N)` | Nth percentile |
| `SPREAD(field)` | max - min |
| `STDDEV(field)` | Standard deviation (population) |
| `MODE(field)` | Most frequent value |
| `DISTINCT(field)` | Unique values |

### Selector Functions

These return up to N stored points per group (each `GROUP BY time()` bucket
and tag set), each at its own time, in time order.

| Function | Description |
|----------|-------------|
| `TOP(field, N)` | The N largest values; the earliest point wins a tie |
| `TOP(field, tag[, tag...], N)` | The N largest values, at most one per tag-value combination; the tags are returned as columns |
| `BOTTOM(field, N)` / `BOTTOM(field, tag[, tag...], N)` | The N smallest values, likewise |
| `SAMPLE(field, N)` | N points picked at random |

Other fields and tags may be selected beside them (`SELECT top("value", 3), "host"`), taken from the same points, but not other functions. `LIMIT` and `OFFSET` apply to the points returned.

A raw field cannot be grouped by time: `SELECT "value" ... GROUP BY time(1m)` fails with `GROUP BY requires at least one aggregate function`, as in InfluxDB.

### Transform Functions

| Function | Description |
|----------|-------------|
| `DERIVATIVE(field_or_agg, unit)` | Rate of change per unit |
| `NON_NEGATIVE_DERIVATIVE(field_or_agg, unit)` | Rate of change, clamping negatives to 0 |
| `DIFFERENCE(field_or_agg)` | Difference from previous value |
| `NON_NEGATIVE_DIFFERENCE(field_or_agg)` | Non-negative difference |
| `MOVING_AVERAGE(field_or_agg, N)` | N-point moving average |
| `CUMULATIVE_SUM(field_or_agg)` | Running total |
| `ELAPSED(field, unit)` | Time elapsed since previous point |

Transforms can wrap aggregates:
```sql
SELECT non_negative_derivative(mean("bytes_recv"), 1s) FROM "net"
  WHERE time > now() - 1h GROUP BY time(10s)
```

### SHOW Commands

```sql
SHOW DATABASES
SHOW MEASUREMENTS [ON <database>]
SHOW TAG KEYS [FROM <measurement>]
SHOW TAG VALUES [FROM <measurement>] WITH KEY = "<key>"
SHOW TAG VALUES [FROM <measurement>] WITH KEY =~ /<regex>/
SHOW TAG VALUES [FROM <measurement>] WITH KEY IN ("<key1>", "<key2>")
  [WHERE <tag conditions>] [LIMIT <n>] [OFFSET <n>]
                                      -- tag conditions: =, !=, =~, !~, AND, OR,
                                      -- with or without ::tag; time is ignored
SHOW FIELD KEYS [FROM <measurement>]
SHOW SERIES [FROM <measurement>]
SHOW SERIES [EXACT] CARDINALITY [ON <db>] [FROM <measurement>] [WHERE <tag condition>]
  [LIMIT n] [OFFSET n]
SHOW MEASUREMENT [EXACT] CARDINALITY [ON <db>] [FROM <measurement>]
  [WITH MEASUREMENT <op> <name|/regex/>]
  [WHERE <tag condition>]
SHOW TAG KEY [EXACT] CARDINALITY [ON <db>] [FROM <measurement>] [WHERE <tag condition>]
SHOW TAG VALUES [EXACT] CARDINALITY [ON <db>] [FROM <measurement>] [WITH KEY <op> <key>]
  [WHERE <tag condition>]
SHOW FIELD KEY [EXACT] CARDINALITY [ON <db>] [FROM <measurement>]
SHOW RETENTION POLICIES [ON <database>]
SHOW USERS
SHOW GRANTS FOR <user>                -- admin; database, privilege
SHOW CONTINUOUS QUERIES
```

#### Cardinality

The `CARDINALITY` statements count what the matching `SHOW` would list, and
the counts are always exact. Without a `WHERE`, they come from the metadata
caches without scanning anything: the series index (on a sharded cluster, each
region's peer counts its own range, so a replicated series counts once), the
tag value counts, and the measurement metadata. A `WHERE` is matched against
each series' tags, like `SHOW TAG VALUES ... WHERE`.

The output follows InfluxDB 1.x. `SHOW SERIES CARDINALITY` with no `FROM` or
`WHERE`, and `SHOW MEASUREMENT CARDINALITY` with no `FROM`, `WITH` or `WHERE`, return
one row under `cardinality estimation`. InfluxDB reports an estimate there;
HyperbyteDB reports the exact count. `EXACT`, or any filter, returns `count`
instead, with one series per measurement, and measurements with a count of
zero are left out. `SHOW FIELD KEY CARDINALITY` refuses `WHERE`, because
fields aren't indexed by tag.

```sql
> SHOW SERIES CARDINALITY ON telegraf
cardinality estimation
----------------------
1520

> SHOW SERIES EXACT CARDINALITY ON telegraf FROM cpu
name: cpu
count
-----
304
```

### DDL Statements

```sql
CREATE DATABASE "<name>"
DROP DATABASE "<name>"
DROP MEASUREMENT "<name>"

CREATE RETENTION POLICY "<name>" ON "<db>"
  DURATION <duration> REPLICATION <n> [SHARD DURATION <duration>] [DEFAULT]
ALTER RETENTION POLICY "<name>" ON "<db>"
  DURATION <duration> [REPLICATION <n>] [DEFAULT]
DROP RETENTION POLICY "<name>" ON "<db>"

CREATE USER "<name>" WITH PASSWORD '<password>'
DROP USER "<name>"
SET PASSWORD FOR "<name>" = '<password>'
GRANT READ | WRITE | ALL ON "<db>" TO "<user>"
REVOKE READ | WRITE | ALL ON "<db>" FROM "<user>"
GRANT ALL PRIVILEGES TO "<user>"       -- make admin
REVOKE ALL PRIVILEGES FROM "<user>"

CREATE CONTINUOUS QUERY "<name>" ON "<db>"
  [RESAMPLE EVERY <interval> FOR <interval>]
  BEGIN <select_into_statement> END
DROP CONTINUOUS QUERY "<name>" ON "<db>"

CREATE MATERIALIZED VIEW "<name>" ON "<db>"
  AS <select_into_statement>          -- every column needs AS "<name>"
DROP MATERIALIZED VIEW "<name>" ON "<db>"
SHOW MATERIALIZED VIEWS

SET <setting> = <integer> | TRUE | FALSE | '<text>' | DEFAULT
                                      -- admin; applied on every node at once
SHOW SETTINGS                         -- name, value, source, config, description
```

### Runtime settings

A `SET` overrides the config file on every node. `source` in
`SHOW SETTINGS` is `set` (an override), `config` (the node's config file) or
`auto` (derived from the node's CPUs). Each node keeps its own config value
underneath, so `DEFAULT` returns every node to its own.

| Setting | Values | Config key | Takes effect |
|---|---|---|---|
| `log_level` | filter text, e.g. `'info'`, `'hyperbytedb=debug,info'` | `logging.level` (or `RUST_LOG`) | next log line |
| `query_pool_size` | 1-128 | `chdb.query_pool_size`, else CPUs + 1 | pool resizes at once |
| `write_pool_size` | 1-128 | `chdb.write_pool_size` | pool resizes at once |
| `max_concurrent_queries` | 0-100000 (0: unlimited) | `server.max_concurrent_queries` | next query; running ones finish under the old cap |
| `query_timeout_secs` | 1-86400 | `server.query_timeout_secs` | next query |
| `max_inflight_writes` | 1-1000000 | `server.max_inflight_writes`, else 32 per CPU | next request; a shrink takes permits back as writes finish |
| `rate_limit_per_second` | 0 or more (0: off) | `rate_limit.max_requests_per_second` when `rate_limit.enabled` | next request; turning it on starts with a full bucket |
| `max_tag_values_per_measurement` | 0 or more (0: unlimited) | `cardinality.max_tag_values_per_measurement` | next new series |
| `max_measurements_per_database` | 0 or more (0: unlimited) | `cardinality.max_measurements_per_database` | next new measurement |
| `flush_interval_secs` | 1-3600 | `flush.interval_secs` | after the current wait |
| `add_peer_proposals_enabled` | `TRUE`/`FALSE` | `sharding.add_peer_proposals_enabled` | next scheduler tick |
| `remove_peer_proposals_enabled` | `TRUE`/`FALSE` | `sharding.remove_peer_proposals_enabled` | next scheduler tick |
| `peer_heal_enabled` | `TRUE`/`FALSE` | `sharding.peer_heal_enabled` | next scheduler tick |
| `schedule_limit` | 1-256 | `sharding.schedule_limit` | at once; running copies finish |
| `movement_budget_bytes_per_sec` | 0 or more (0: unlimited) | `sharding.movement_budget_bytes_per_sec` | next scheduler tick |
| `region_split_series` | 1 or more | `sharding.region_split_series` | next split check |
| `region_max_series` | 1 or more | `sharding.region_max_series` | next split check |
| `region_merge_series` | 0 or more | `sharding.region_merge_series` | next merge check |
| `split_merge_interval_secs` | 1-86400 | `sharding.split_merge_interval_secs` | next split check |
| `bootstrap_regions_per_node` | 0-16 (0: one region) | `sharding.bootstrap_regions_per_node` | next new measurement |

The cardinality limits apply to client writes. Region copies, replicas and
node syncs copy data that already exists, so a lower limit never refuses
them.

The scheduler settings only do anything on a sharded cluster; elsewhere
`SHOW SETTINGS` lists them without a value. Settings that need a restart
anyway (listen addresses, data paths, Raft timing, replication factor, TLS,
auth) are not runtime settings.

`SET` is replicated through Raft and stored, so it survives a restart and
reaches nodes that join later. `SET <setting> = DEFAULT` removes the
override. Do not run `SET` while a rolling upgrade from a version without it
is in progress: nodes on the older build cannot decode it.

### DELETE

```sql
DELETE FROM "<measurement>" [WHERE <condition>]
```

DELETE marks data with tombstone records. Tombstoned data is excluded from queries at read time; underlying MergeTree rows are not rewritten in place.

---

## InfluxDB v1 Compatibility Matrix

### What works identically

- Line protocol write format and semantics
- `/write` and `/query` endpoint behavior
- `/ping` response for client library connection tests
- JSON response shapes (`{"results":[...]}`)
- `epoch` parameter for timestamp formatting
- Authentication methods (query params, Basic, Token)
- Multiple statement queries (`;` separator)
- All SHOW commands listed above
- CREATE/DROP DATABASE, retention policy management
- Gzip write support
- Chunked query responses

### Known Differences

| Area | Difference |
|------|------------|
| **Query engine** | ClickHouse (chDB) internally. Minor floating-point edge cases at bucket boundaries. |
| **Storage format** | chDB MergeTree tables instead of TSM shards. No direct TSM import. |
| **fill(previous/linear)** | Via ClickHouse `INTERPOLATE`; may differ at series boundaries. |
| **SELECT INTO with regex** | Not supported. Use explicit measurement names. |
| **Permissions** | Admin users, plus `GRANT`/`REVOKE` `READ`, `WRITE` or `ALL` per database, as in InfluxDB v1. |
| **Subscriptions** | Not supported. |

### Recommendations

- Use `fill(null)` or `fill(none)` when exact InfluxDB behavior is required at time boundaries.
- Test `fill(previous)` and `fill(linear)` results for critical dashboards.
- Avoid `SELECT INTO` with regex measurement sources.
- Plan authorization around admin/non-admin roles only.

---

## See Also

- [Authentication](authentication.md) — Enabling auth, public endpoints, `401`/`403` on internal routes
- [Basic operations](basic-operations.md) — Getting started with writes and queries
- [Advanced features](advanced-features.md) — Clustering, CQs, TLS
- [Configuration](configuration.md) — All settings reference
