# Common workflows

Migration from InfluxDB 1.x, Telegraf and Grafana integration, monitoring, backups, and downsampling.


## Migrating from InfluxDB 1.x

HyperbyteDB is designed as a drop-in replacement for InfluxDB 1.x. Most clients, libraries, Telegraf, and Grafana work without modification.

### Migrating historical data

Use any tool that can export InfluxDB 1.x data as line protocol and POST it to HyperbyteDB's `/write` endpoint. Common approaches:

1. **Dual-write window** — point Telegraf (or your collectors) at both InfluxDB and HyperbyteDB during cutover.
2. **InfluxDB export** — use `influx_inspect export` or chunked `SELECT … INTO` queries from InfluxDB, then replay with `curl`:

```bash
# Example: replay a line-protocol file
curl -sS -XPOST 'http://hyperbytedb:8086/write?db=mydb&precision=ns' \
  --data-binary @/path/to/export.lp
```

3. **Measurement-by-measurement** — migrate one measurement at a time to limit blast radius during validation.

HyperbyteDB accepts the same line protocol format as InfluxDB 1.x; no separate migration binary is required.

### What works identically

- Line protocol write format and semantics
- `/write` and `/query` endpoint behavior
- `/ping` response for client library connection tests
- JSON response shapes (`{"results":[...]}`)
- `epoch` parameter for timestamp formatting
- [Authentication](authentication.md) (query params, Basic, Token; internal APIs need admin)
- Gzip write support
- Chunked query responses

### Known differences

| Area | Difference |
|------|------------|
| Query engine | ClickHouse (chDB) instead of TSM; minor floating-point edge cases |
| Storage format | Embedded chDB MergeTree tables instead of TSM shards |
| `fill(previous/linear)` | Implemented via ClickHouse `INTERPOLATE`; may differ at series boundaries |
| `SELECT INTO` with regex | Not supported; use explicit measurement names |
| Permissions | Admin vs non-admin only; no per-database GRANT/REVOKE |
| Subscriptions | Not supported |

---

## Integrating Telegraf

Telegraf works with HyperbyteDB out of the box using its InfluxDB v1 output plugin.

### telegraf.conf

```toml
[[outputs.influxdb]]
  urls = ["http://hyperbytedb:8086"]
  database = "telegraf"
  skip_database_creation = false
  timeout = "5s"

[[inputs.cpu]]
  percpu = true
  totalcpu = true

[[inputs.mem]]

[[inputs.disk]]
  ignore_fs = ["tmpfs", "devtmpfs"]

[[inputs.net]]

[[inputs.system]]
```

Point Telegraf at HyperbyteDB just as you would at InfluxDB. The `skip_database_creation = false` setting tells Telegraf to create the database if it doesn't exist.

---

## Integrating Grafana

HyperbyteDB works as an InfluxDB v1 datasource in Grafana.

### Add datasource

1. Open Grafana → Configuration → Data Sources → Add data source.
2. Select **InfluxDB**.
3. Set the URL to `http://hyperbytedb:8086`.
4. Set the database name (e.g., `telegraf`).
5. If auth is enabled, enter credentials in the InfluxDB Details section.
6. Click **Save & Test**.

### Docker Compose (pre-configured)

The included `docker-compose.yml` ships Grafana with pre-provisioned datasources:
- **HyperbyteDB** (InfluxDB v1 API) for Telegraf host metrics
- **Prometheus** for `/metrics`
- **Loki** for container logs (via Alloy)
- **Tempo** for traces

Grafana is accessible at `http://localhost:3000` with login `admin`/`admin`. Use **Explore** to correlate logs (`{container=~".*hyperbytedb.*"}`) and traces (`service.name=hyperbytedb`).

---

## Setting Up Monitoring

### Prometheus scrape config

Add HyperbyteDB as a Prometheus target:

```yaml
scrape_configs:
  - job_name: 'hyperbytedb'
    static_configs:
      - targets: ['hyperbytedb:8086']
    metrics_path: /metrics
    scrape_interval: 15s
```

### Key metrics to watch

| Metric | Type | What it tells you |
|--------|------|-------------------|
| `hyperbytedb_write_requests_total` | counter | Write throughput |
| `hyperbytedb_query_requests_total` | counter | Query throughput |
| `hyperbytedb_query_duration_seconds` | histogram | Query latency (P50/P95/P99) |
| `hyperbytedb_ingestion_points_total` | counter | Points ingested |
| `hyperbytedb_flush_duration_seconds` | histogram | WAL-to-chDB flush health |
| `hyperbytedb_native_rows_written_total` | counter | Rows inserted into chDB during flush |

### Alert recommendations

| Condition | Alert |
|-----------|-------|
| `rate(hyperbytedb_query_errors_total[5m]) > 0` | Query failures |
| `hyperbytedb_query_duration_seconds{quantile="0.99"} > 10` | Slow queries |
| `rate(hyperbytedb_write_errors_total[5m]) > 0` | Write failures |
| `hyperbytedb_flush_duration_seconds{quantile="0.99"} > 30` | Slow flushes |

---

## Backup and Restore

### Create a backup

```bash
hyperbytedb backup --output /backups/hyperbytedb-$(date +%Y%m%d)
```

The backup contains:
- `wal/` — RocksDB checkpoint of the WAL
- `meta/` — RocksDB checkpoint of metadata
- `data/` — Copy of the chDB session directory (`[chdb].session_data_path`)
- `manifest.json` — Timestamp, WAL sequence, and `engine_data_paths` file list

Backups can be taken while HyperbyteDB is running. RocksDB checkpoints provide a consistent snapshot without stopping writes.

### Restore from backup

```bash
# 1. Stop HyperbyteDB
# 2. Restore (overwrites wal_dir, meta_dir, and chDB session data)
hyperbytedb restore --input /backups/hyperbytedb-20240115
# 3. Start HyperbyteDB
hyperbytedb serve
```

> **Warning:** Restore **overwrites** the configured `wal_dir`, `meta_dir`, and `[chdb].session_data_path`. Ensure the config file points to the correct directories.

---

## Downsampling with Continuous Queries

A typical pattern for long-term data retention:

1. **Raw data** — kept for 7 days in the default retention policy.
2. **5-minute rollups** — kept for 90 days.
3. **1-hour rollups** — kept indefinitely.

```sql
-- Create retention policies
CREATE RETENTION POLICY "7d" ON "mydb" DURATION 7d REPLICATION 1 DEFAULT
CREATE RETENTION POLICY "90d" ON "mydb" DURATION 90d REPLICATION 1
CREATE RETENTION POLICY "forever" ON "mydb" DURATION INF REPLICATION 1

-- Create downsampling CQs
CREATE CONTINUOUS QUERY "cq_5m" ON "mydb"
BEGIN
  SELECT mean("usage_idle") AS "usage_idle", mean("usage_user") AS "usage_user"
  INTO "mydb"."90d"."cpu_5m"
  FROM "cpu"
  GROUP BY time(5m), *
END

CREATE CONTINUOUS QUERY "cq_1h" ON "mydb"
BEGIN
  SELECT mean("usage_idle") AS "usage_idle", mean("usage_user") AS "usage_user"
  INTO "mydb"."forever"."cpu_1h"
  FROM "cpu"
  GROUP BY time(1h), *
END
```

---

## See Also

- [Administration](administration.md) — Backup procedures, cluster operations, compaction tuning
- [Troubleshooting](troubleshooting.md) — Common problems and fixes
- [API & TimeseriesQL Reference](reference.md) — Full syntax reference
