# Administration

Monitoring, backup and restore, retention, cluster operations, and background services.


## Monitoring

### Prometheus Metrics

HyperbyteDB exposes a Prometheus-compatible metrics endpoint at `GET /metrics` on the same port as the API (default 8086). There is no separate metrics port.

**Key metrics:**

| Metric | Type | Description |
|--------|------|-------------|
| `hyperbytedb_write_requests_total` | counter | Total write requests received |
| `hyperbytedb_write_errors_total` | counter | Failed write requests |
| `hyperbytedb_write_payload_bytes` | histogram | Raw payload size in bytes |
| `hyperbytedb_write_duration_seconds` | histogram | Write handler latency |
| `hyperbytedb_query_requests_total` | counter | Total query requests received |
| `hyperbytedb_query_errors_total` | counter | Failed queries |
| `hyperbytedb_query_duration_seconds` | histogram | Query execution latency |
| `hyperbytedb_ingestion_points_total` | counter | Total points ingested |
| `hyperbytedb_flush_runs_total` | counter | Flush cycles completed |
| `hyperbytedb_flush_errors_total` | counter | Failed flush cycles |
| `hyperbytedb_flush_points_total` | counter | Points flushed to chDB |
| `hyperbytedb_flush_duration_seconds` | histogram | Flush cycle duration |
| `hyperbytedb_wal_last_sequence` | gauge | Last flushed WAL sequence |

**Cluster-specific metrics:**

| Metric | Type | Description |
|--------|------|-------------|
| `hyperbytedb_replication_writes_total` | counter | Write replication attempts |
| `hyperbytedb_replication_errors_total` | counter | Failed write replications |
| `hyperbytedb_replication_duration_seconds` | histogram | Replication latency |
| `hyperbytedb_cluster_node_state` | gauge | Node state (0=Joining through 5=Leaving) |
| `hyperbytedb_cluster_peers_active` | gauge | Number of active peers |
| `hyperbytedb_uptime_seconds` | gauge | Node uptime |

### Prometheus scrape configuration

```yaml
scrape_configs:
  - job_name: 'hyperbytedb'
    static_configs:
      - targets: ['hyperbytedb:8086']
    metrics_path: /metrics
    scrape_interval: 15s
```

For clusters, scrape each node individually.

### Logging

Logs are written to stderr. Control verbosity with the `[logging]` config section:

| Level | Use case |
|-------|----------|
| `error` | Production: errors only |
| `warn` | Production: errors + warnings |
| `info` | Default: startup, shutdown, periodic summaries |
| `debug` | Development: query details, flush activity |
| `trace` | Deep debugging: all internal operations |

Set `format = "json"` for structured output compatible with log aggregation (Loki, Elasticsearch, and similar).

Environment variable equivalents:

```bash
HYPERBYTEDB__LOGGING__LEVEL=info
HYPERBYTEDB__LOGGING__FORMAT=json
HYPERBYTEDB__LOGGING__DETAILED_TRACE=true
```

### Distributed tracing

HyperbyteDB exports OpenTelemetry traces over OTLP HTTP when `logging.otlp_endpoint` is set. This is independent of `detailed_trace`: you can export sampled traces in production without enabling per-phase span creation on every request.

| Setting | Purpose |
|---------|---------|
| `detailed_trace = true` | Creates spans on write, query, and flush paths |
| `otlp_endpoint` | Collector URL (Tempo, Grafana Alloy, or any OTLP HTTP endpoint) |
| `otlp_sample_ratio` | Export fraction (`1.0` = all traces; use `0.1` or lower under load) |

Traces are tagged with `service.name=hyperbytedb` (override with `OTEL_SERVICE_NAME`).

The root `docker-compose.yml` ships Alloy, Loki, Tempo, and Grafana with trace-to-log correlation preconfigured. After starting the stack:

1. Open Grafana → **Explore** → **Tempo**.
2. Search for `service.name=hyperbytedb`.
3. Run a few writes and queries, then inspect span timings.

See [Configuration](configuration.md#logging) for all logging keys.

### Statement summary

When `statement_summary.enabled = true`, recently executed InfluxQL statements are available at `GET /api/v1/statements`. Each entry includes the normalized query text, digest, execution time, and error status. Useful for correlating slow queries with Tempo traces and Loki logs.

### Health endpoint

`GET /health` returns:
```json
{"status": "pass", "message": "ready for queries and writes"}
```

Always returns 200 as long as the HTTP server is running. In cluster mode, a node in `Draining` or `Leaving` state still responds to `/health` but rejects writes.

---

## Backup and Restore

### Create a backup

```bash
hyperbytedb backup --output /backups/hyperbytedb-$(date +%Y%m%d)
```

The backup directory contains:

| Directory | Contents |
|-----------|----------|
| `wal/` | RocksDB checkpoint of the WAL |
| `meta/` | RocksDB checkpoint of metadata |
| `data/` | Copy of the chDB session data directory (`chdb.session_data_path`) |
| `manifest.json` | Timestamp, WAL sequence, engine data paths |

Backups can run while HyperbyteDB is serving traffic. RocksDB checkpoints are consistent point-in-time snapshots. For off-node copies, use your operator backup CRD or object storage tooling.

### Restore

```bash
# 1. Stop HyperbyteDB
# 2. Restore (overwrites configured directories)
hyperbytedb restore --input /backups/hyperbytedb-20240115
# 3. Start HyperbyteDB
hyperbytedb serve
```

Restore **overwrites** the configured `wal_dir`, `meta_dir`, and chDB session data directory.

---

## Retention

Retention policies are enforced by a background loop that runs `ALTER TABLE … DELETE` against expired rows in each measurement's MergeTree table. Tune frequency with `[retention].interval` in config. See [Configuration](configuration.md#retention).

---

## Cluster Operations

### Cluster inspection (HTTP)

Use the built-in HTTP endpoints for on-call inspection:

| Endpoint | Description |
|----------|-------------|
| `GET /cluster/metrics` | Node id, state, membership version, peer counts |
| `GET /cluster/nodes` | All nodes with health and addresses |
| `GET /internal/sync/manifest` | WAL watermark and measurement catalog used for sync |
| `GET /metrics` | Prometheus metrics |

```bash
curl -s http://node1:8086/cluster/metrics | jq .
curl -s http://node2:8086/internal/sync/manifest | jq .
```

Compare manifests across nodes to spot replication lag or catalog drift.

### Graceful drain

To remove a node from the cluster without data loss:

```bash
curl -sS -XPOST 'http://node-to-remove:8086/internal/drain'
```

The drain procedure:
1. Sets node state to `Draining` (rejects new writes with 503).
2. Flushes all WAL entries into chDB MergeTree tables.
3. Waits for replication acks from all peers (up to 60 seconds).
4. Notifies peers of departure.
5. Sets state to `Leaving`.

### Cluster sync

In cluster mode, the Raft leader periodically compares `/internal/sync/manifest` responses from peers and may mark peers as needing sync. New nodes pull metadata and WAL deltas via the sync APIs. See [Deep Dive: Clustering](../deep-dive/deep-dive-clustering.md).

---

## Background Services

HyperbyteDB runs several background services as Tokio tasks:

| Service | Interval | Purpose |
|---------|----------|---------|
| Flush | `flush.interval_secs` (10s) | WAL → chDB MergeTree |
| Retention | `retention.interval` (60s) | `ALTER TABLE … DELETE` for expired rows |
| Continuous Query | 10s (fixed) | Execute CQ schedules |
| Heartbeat | `heartbeat_interval_secs` (2s, cluster) | Peer liveness detection |
| Leader sync monitor | 30s (cluster) | Compare peer manifests and trigger sync when needed |

All services shut down gracefully on `ctrl+c`: the flush service performs a final flush, then all service handles are awaited.

---

## See Also

- [Configuration](configuration.md) — Full reference for all tuning parameters
- [Troubleshooting](troubleshooting.md) — Diagnosing common issues
- [Common workflows](common-workflows.md) — Backup procedures, monitoring setup
