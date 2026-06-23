# Configuration Reference

HyperbyteDB loads configuration in this order (later sources override earlier):

1. **Built-in defaults**
2. **TOML config file** (path from `--config` / `-c` flag; defaults to `./config.toml`)
3. **Environment variables** with prefix `HYPERBYTEDB__`

---

## Environment Variable Format

```
HYPERBYTEDB__<SECTION>__<KEY>=value
```

Double underscores (`__`) separate the section name and key. For nested sections, add another level:

```bash
HYPERBYTEDB__SERVER__PORT=9090
HYPERBYTEDB__STORAGE__WAL_DIR=/var/lib/hyperbytedb/wal
HYPERBYTEDB__CLUSTER__PEERS="node2:8086,node3:8086"
```

---

## [server]

HTTP server settings.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `bind_address` | string | `"0.0.0.0"` | Network interface to bind to |
| `port` | integer | `8086` | HTTP listen port |
| `max_body_size_bytes` | integer | `26214400` | Maximum request body size (25 MB) |
| `request_timeout_secs` | integer | `30` | HTTP request timeout |
| `query_timeout_secs` | integer | `30` | TimeseriesQL query execution timeout |
| `max_concurrent_queries` | integer | `0` | Max concurrent TimeseriesQL executions; `0` = unlimited (bounded by work-stealing / resources). Use with single chDB session. |
| `tls_enabled` | boolean | `false` | Enable HTTPS with TLS |
| `tls_cert_path` | string | `""` | Path to PEM certificate file |
| `tls_key_path` | string | `""` | Path to PEM private key file |

---

## [storage]

Local directories for the write-ahead log and metadata store. Time-series data lives in embedded chDB MergeTree tables under `[chdb].session_data_path`.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `wal_dir` | string | `"./wal"` | Write-ahead log directory (RocksDB) |
| `meta_dir` | string | `"./meta"` | Metadata directory (RocksDB) |
| `wal_format` | string | `"bincode"` | Durable WAL value encoding: `bincode` or `arrow_ipc` (Arrow IPC stream + embedded legacy entry for peer sync) |

---

## [flush]

Controls the background WAL-to-chDB flush pipeline.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `interval_secs` | integer | `10` | How often the flush service runs (seconds) |
| `wal_size_threshold_mb` | integer | `64` | WAL size that triggers an immediate flush (MB) |
| `time_bucket_duration` | string | `"1h"` | Time bucket granularity used when grouping WAL entries for flush |
| `max_points_per_batch` | integer | `50000` | Max points per chDB insert batch (server clamps to 10k–500k; `0` uses the same default) |
| `wal_batch_size` | integer | `64` | WAL group-commit: max entries to coalesce per write batch; `0` = disabled |
| `wal_batch_delay_us` | integer | `200` | WAL group-commit: max microseconds to wait for more entries before flushing |
| `arrow_wal_enabled` | boolean | `true` | Keep chDB-ready Arrow `RecordBatch`es in an in-memory WAL cache for zero-copy flush |

---

## [chdb]

Embedded ClickHouse (chDB) query engine settings.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `session_data_path` | string | `"./chdb_data"` | chDB session state directory |
| `pool_size` | integer | `4` | Number of chDB connections to the same `session_data_path`. Each connection has its own client mutex, so flush inserts and concurrent queries overlap when `pool_size > 1`. Clamped to 1–32. For best overlap, set `server.max_concurrent_queries` ≥ `pool_size`. |

---

## [auth]

Authentication configuration.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `enabled` | boolean | `false` | Enable authentication on `/write` and `/query` |

When enabled, `/write` and `/query` require valid credentials. Health/metrics and other public routes, plus **admin-only** internal/cluster APIs, are documented in **[Authentication](authentication.md)**.

---

## [cardinality]

Limits to prevent unbounded series growth from high-cardinality data.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `max_tag_values_per_measurement` | integer | `100000` | Max distinct tag values per tag key per measurement |
| `max_measurements_per_database` | integer | `10000` | Max measurements per database |

If a write exceeds these limits, it returns HTTP 422 with a `cardinality limit exceeded` error.

---

## [cluster]

Master-master peer-to-peer clustering with Raft consensus for schema mutations.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `enabled` | boolean | `false` | Enable cluster mode |
| `node_id` | integer | `1` | Unique node identifier |
| `cluster_addr` | string | `"127.0.0.1:8086"` | Address other nodes use to reach this node |
| `peers` | string | `""` | Optional seed list; when empty, use operator/HTTP membership APIs. See [Deep Dive: Clustering](../deep-dive/deep-dive-clustering.md). |
| `heartbeat_interval_secs` | integer | `2` | How often to send heartbeats |
| `heartbeat_miss_threshold` | integer | `5` | Missed heartbeats before marking a peer disconnected |
| `anti_entropy_enabled` | boolean | `false` | **Deprecated.** No effect; logs a warning if set |
| `anti_entropy_interval_secs` | integer | `60` | **Deprecated.** No effect |
| `replication_log_dir` | string | `"./replication_log"` | RocksDB directory for replication tracking |
| `raft_dir` | string | `"./raft"` | RocksDB directory for Raft consensus state |
| `replication_max_retries` | integer | `5` | Max retries for failed replications |
| `replication_queue_depth` | integer | `8192` | Bounded outbound replication queue (ingest-sized batches) |
| `replication_max_inflight_batches` | integer | `8` | Max concurrent outbound replication fan-out rounds |
| `replication_max_coalesce_body_bytes` | integer | `8388608` | Max bytes for coalescing consecutive WAL batches (same db/rp/precision) |
| `replicate_receiver_queue_depth` | integer | `1024` | Bounded apply queue on the replicate receiver |
| `replicate_receiver_workers` | integer | `1` | **Ignored.** Receiver uses a single ordered worker |
| `replication_truncate_stale_peer_multiplier` | integer | `2` | When >0, peers with ack 0 and stale heartbeats are omitted from truncate barrier (× heartbeat interval) |
| `raft_heartbeat_interval_ms` | — | *unset* | Optional Raft heartbeat (ms); uses internal default if omitted |
| `raft_election_timeout_ms` | — | *unset* | Optional Raft election timeout (ms) |
| `raft_snapshot_threshold` | — | *unset* | Optional log entries before Raft snapshot |

### [cluster.replication]

Per-node coordinator replication behavior. If the whole block is omitted, mode is `async` (fire-and-forget fan-out, same as legacy).

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `mode` | string | `"async"` | `"async"` or `"sync_quorum"` (await W peer acks before client response) |
| `ack_timeout_ms` | integer | `5000` | For `sync_quorum`: max wait for peer acks; on timeout, HTTP 504 and hinted handoff for unacked peers |
| `sync_quorum.min_acks` | string or int | `"majority"` | Peer acks required: `"majority"` (of cluster, excluding self) or explicit count |

---

## [logging]

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `level` | string | `"info"` | Log level: `trace`, `debug`, `info`, `warn`, `error` |
| `format` | string | `"text"` | Output format: `"text"` or `"json"` (structured, for Loki and similar) |

---

## [statement_summary]

Query statement tracking for debugging and observability.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `enabled` | boolean | `true` | Enable statement summary tracking |
| `max_entries` | integer | `1000` | Max recent statements kept in the ring buffer |

When enabled, recently executed statements are accessible via `GET /api/v1/statements`.

---

## [hinted_handoff]

Hinted handoff stores writes destined for unreachable peers and replays them when the peer recovers.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `enabled` | boolean | `true` | Enable hinted handoff (cluster mode only) |
| `max_hints_per_peer` | integer | `100000` | Max queued hints per unreachable peer before oldest are dropped |
| `max_hint_age_secs` | integer | `3600` | Hints older than this (seconds) are discarded on drain |

---

## [rate_limit]

HTTP rate limiting for `/write` and `/query`.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `enabled` | boolean | `false` | Enable per-endpoint request rate limiting |
| `max_requests_per_second` | integer | `0` | Max requests per second per endpoint; `0` = unlimited when enabled (set a positive value to enforce) |

---

## [retention]

Controls the background retention enforcement loop. Per-policy `duration` values are stored in metadata (`CREATE/ALTER RETENTION POLICY`).

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `enabled` | boolean | `true` | When `false`, expired rows are not deleted automatically |
| `interval` | string | `"12h"` | How often retention scans run (`humantime` duration, e.g. `1m`, `1h`) |

---

## Example: Minimal Single-Node

```toml
[server]
bind_address = "0.0.0.0"
port = 8086

[storage]
wal_dir = "./wal"
meta_dir = "./meta"

[flush]
interval_secs = 10

[chdb]
session_data_path = "./chdb_data"
pool_size = 4

[logging]
level = "info"
```

## Example: Production Cluster Node

```toml
[server]
bind_address = "0.0.0.0"
port = 8086
query_timeout_secs = 60
max_concurrent_queries = 32
tls_enabled = true
tls_cert_path = "/etc/hyperbytedb/cert.pem"
tls_key_path = "/etc/hyperbytedb/key.pem"

[storage]
wal_dir = "/var/lib/hyperbytedb/wal"
meta_dir = "/var/lib/hyperbytedb/meta"

[flush]
interval_secs = 10

[chdb]
session_data_path = "/var/lib/hyperbytedb/chdb"

[cluster]
enabled = true
node_id = 1
cluster_addr = "10.0.0.1:8086"
replication_log_dir = "/var/lib/hyperbytedb/replication_log"
raft_dir = "/var/lib/hyperbytedb/raft"

[auth]
enabled = true

[cardinality]
max_tag_values_per_measurement = 100000
max_measurements_per_database = 10000

[logging]
level = "info"
format = "json"
```

## Example: Environment Variable Overrides

```bash
export HYPERBYTEDB__SERVER__PORT=9090
export HYPERBYTEDB__SERVER__QUERY_TIMEOUT_SECS=60
export HYPERBYTEDB__STORAGE__WAL_DIR=/var/lib/hyperbytedb/wal
export HYPERBYTEDB__STORAGE__META_DIR=/var/lib/hyperbytedb/meta
export HYPERBYTEDB__CHDB__SESSION_DATA_PATH=/var/lib/hyperbytedb/chdb
export HYPERBYTEDB__SERVER__MAX_CONCURRENT_QUERIES=32
export HYPERBYTEDB__LOGGING__LEVEL=debug
export HYPERBYTEDB__LOGGING__FORMAT=json
export HYPERBYTEDB__RETENTION__INTERVAL=5m
```

---

## See Also

- [Installation](installation.md) — Deployment methods
- [Administration](administration.md) — Operational tuning
- [Authentication](authentication.md) — Enabling auth, credentials, admin for internal routes
- [Advanced features](advanced-features.md) — Clustering, TLS, S3
