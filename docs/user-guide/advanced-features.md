# Advanced features

Clustering, continuous queries, TLS, tracing, and optional columnar ingest. See [Authentication](authentication.md) for credential setup.


## Clustering

HyperbyteDB supports **master-master replication** where every node in the cluster accepts both reads and writes. Data writes are replicated asynchronously to all peers. Schema mutations (CREATE/DROP DATABASE, DELETE, etc.) use Raft consensus for consistent ordering.

### How it works

1. A client writes to any node.
2. That node persists to its local WAL and returns `204` to the client.
3. The node fans out the write via HTTP to all peers.
4. A header (`X-Hyperbytedb-Replicated: true`) prevents infinite replication loops.

### Configuration

Each node needs a unique `node_id` and must list all other nodes as `peers`:

**Node 1:**
```toml
[cluster]
enabled = true
node_id = 1
cluster_addr = "node1.example.com:8086"
peers = "node2.example.com:8086,node3.example.com:8086"
```

**Node 2:**
```toml
[cluster]
enabled = true
node_id = 2
cluster_addr = "node2.example.com:8086"
peers = "node1.example.com:8086,node3.example.com:8086"
```

No initialization step is required. Nodes begin replicating as soon as they start.

### Cluster endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/cluster/metrics` | GET | Cluster mode, node address, peer list |
| `/cluster/nodes` | GET | All nodes with a `self` flag |

### Important considerations

- Replication is **asynchronous and best-effort**. If a peer is down, the write succeeds locally but the peer misses it until anti-entropy or hinted handoff delivers it.
- There is no distributed query fan-out. Each node queries its own embedded chDB tables.
- Schema mutations (CREATE DATABASE, DROP DATABASE, DELETE, etc.) are replicated via Raft for consistent ordering.
- Hinted handoff stores writes for unreachable peers and replays them on reconnection.

---

## Continuous Queries

Continuous queries (CQs) automatically downsample data on a schedule, writing aggregated results to a target measurement.

### Create a continuous query

```sql
CREATE CONTINUOUS QUERY "cq_cpu_1h" ON "mydb"
BEGIN
  SELECT mean("usage_idle") AS "mean_usage_idle"
  INTO "cpu_1h"
  FROM "cpu"
  GROUP BY time(1h), *
END
```

### With resample control

```sql
CREATE CONTINUOUS QUERY "cq_cpu_1h" ON "mydb"
RESAMPLE EVERY 30m FOR 2h
BEGIN
  SELECT mean("usage_idle") INTO "cpu_1h" FROM "cpu" GROUP BY time(1h), *
END
```

- `RESAMPLE EVERY 30m` — execute every 30 minutes (instead of every `resample_every_secs` default).
- `FOR 2h` — look back 2 hours on each execution.

### Manage continuous queries

```sql
SHOW CONTINUOUS QUERIES
DROP CONTINUOUS QUERY "cq_cpu_1h" ON "mydb"
```

CQ definitions are stored in metadata and survive restarts. The background scheduler evaluates CQs every 10 seconds.

---

## Materialized Views

Materialized views downsample data incrementally using ClickHouse `MATERIALIZED VIEW` objects. Each new flush to the source measurement triggers aggregation into the destination measurement — no background scheduler.

### Create a materialized view

```sql
CREATE MATERIALIZED VIEW "mv_cpu_1h" ON "mydb"
AS SELECT mean("usage_idle") INTO "cpu_1h" FROM "cpu" GROUP BY time(1h), *
```

Requirements (same as `SELECT INTO` / continuous queries):

- `INTO` destination measurement is required
- `GROUP BY time(<interval>)` is required
- Single concrete source measurement (no regex `FROM` in v1)

On `CREATE`, HyperbyteDB:

1. Registers the destination measurement in metadata
2. Creates destination MergeTree tables in chDB
3. Installs fact and series ClickHouse materialized views
4. Backfills historical data from the source

### Manage materialized views

```sql
SHOW MATERIALIZED VIEWS
DROP MATERIALIZED VIEW "mv_cpu_1h" ON "mydb"
```

`DROP MATERIALIZED VIEW` removes the ClickHouse MV objects and metadata. The destination measurement and its data are kept.

### Continuous queries vs materialized views

| | Continuous Query | Materialized View |
|--|------------------|-------------------|
| Trigger | 10s scheduler | Each flush to source |
| Latency | Up to resample interval | Near real-time |
| Backfill | Re-scans window each run | One-time on CREATE; then incremental |
| Engine | WAL writeback | ClickHouse MV |

---

### User authentication

Enable `[auth] enabled = true` to require credentials on `/write` and `/query`. How credentials are sent, which HTTP paths stay public, and when **admin** is required for cluster/internal APIs are all covered in **[Authentication](authentication.md)**.

---

## TLS / HTTPS

Enable encrypted connections:

```toml
[server]
tls_enabled = true
tls_cert_path = "/etc/hyperbytedb/cert.pem"
tls_key_path = "/etc/hyperbytedb/key.pem"
```

HyperbyteDB uses `rustls` (no OpenSSL dependency). Both files must be PEM-encoded. When TLS is enabled, the server only accepts HTTPS connections — there is no mixed HTTP+HTTPS mode.

### Generate a self-signed certificate (testing only)

```bash
openssl req -x509 -newkey rsa:4096 -keyout key.pem -out cert.pem \
  -days 365 -nodes -subj '/CN=hyperbytedb'
```

---

## Columnar MessagePack Ingest

Columnar MessagePack ingest is enabled in default builds (`columnar-ingest` feature). It accepts columnar data encoded as MessagePack, reducing parse overhead compared to line protocol for bulk imports.

Disable with `cargo build --no-default-features` if you need a slimmer binary.

This adds support for `Content-Type: application/x-msgpack` on the `/write` endpoint.

---

## Observability (logs and traces)

```toml
[logging]
level = "info"
format = "json"
detailed_trace = true
otlp_endpoint = "http://alloy:4318"
otlp_sample_ratio = 0.1
```

- `format = "json"` — structured logs for Loki and similar collectors.
- `detailed_trace = true` — per-phase spans on write, query, and flush paths.
- `otlp_endpoint` — export traces to Tempo (or any OTLP HTTP collector).

The Docker Compose stack enables these settings by default. See [Administration](administration.md#distributed-tracing) for Grafana workflows.

## Statement summary

```toml
[statement_summary]
enabled = true
max_entries = 1000
```

```bash
curl -sS 'http://localhost:8086/api/v1/statements'
```

Each entry includes the normalized query text, digest, execution time, and error status.

---

## See Also

- [Authentication](authentication.md) — User auth, public routes, admin for internal APIs
- [Configuration](configuration.md) — Full reference for all settings
- [Administration](administration.md) — Backup, monitoring, cluster operations
- [Common workflows](common-workflows.md) — Migration, Grafana integration
