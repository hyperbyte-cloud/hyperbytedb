# Resource Sizing Guide

Guidelines for provisioning CPU, memory, disk, and cluster topology for HyperbyteDB workloads.

---

## What sizing means

HyperbyteDB sizing has three mostly independent dimensions:

| Dimension | What it drives | What it does **not** drive |
|-----------|----------------|----------------------------|
| **Query load** | CPU and RAM | Disk capacity |
| **Data volume and retention** | Disk capacity | CPU (directly) |
| **Write throughput** | Disk IOPS and flush cadence | CPU and RAM (in most cases) |

**Write rate alone is a poor way to pick CPU and RAM.** The ingest path — HTTP accept, parse, WAL append, and periodic flush to chDB — is optimized for high throughput. On SSD or NVMe storage, a single node can sustain **around 1 million points/sec end-to-end** (write through to queryable storage) with modest CPU usage. That does not mean you need 16 cores because you ingest 500K points/sec.

**Query load is the main reason to add CPU and RAM.** Dashboards, alerts, and ad-hoc TimeseriesQL that scan wide time ranges, aggregate with `GROUP BY`, or run concurrently will consume far more CPU and memory than the write path at the same ingest rate.

Plan each dimension separately, then add headroom for your heaviest expected query load.

---

## Query load — primary CPU and RAM driver

Size CPU and RAM from how many queries run at once and how heavy they are — not from points/sec ingest.

| Profile | Typical pattern | CPU | RAM | Example deployment |
|---------|----------------|-----|-----|-------------------|
| Dev / lab | Occasional ad-hoc queries, 1–2 concurrent | 2 cores | 4 GB | Single Docker container |
| Light production | A few dashboards, narrow time ranges, ≤4 concurrent queries | 4 cores | 8 GB | Small VM |
| Medium production | Multiple dashboards, `GROUP BY time` or tag, 8–16 concurrent queries | 8 cores | 16 GB | Dedicated instance |
| Heavy analytics | Many concurrent wide scans and aggregates over long retention | 16+ cores | 32 GB+ | Large instance with tuned concurrency limits |

**What makes a query heavy:**

- **No time filter** — scans all retained data for a measurement.
- **Wide time range** — e.g. 30 days of raw data in one chart.
- **Aggregations** — `MEAN`, `COUNT`, `GROUP BY time(1h)`, `GROUP BY host` over large spans.
- **Concurrency** — many Grafana panels, alert rules, or API clients querying at once.

**Practical suggestions:**

- Add time bounds to dashboards and alerts (`WHERE time > now() - 24h` or similar).
- Cap concurrent queries so heavy scans do not pile up:
  ```toml
  [server]
  max_concurrent_queries = 16

  [chdb]
  query_pool_size = 4
  write_pool_size = 4
  ```
  Set `max_concurrent_queries` ≥ `query_pool_size` so concurrent queries can use the query pool. Ingest/flush uses a separate `write_pool_size` pool. See [Configuration](configuration.md).
- If queries are slow but CPU is idle, you may be I/O-bound on disk — check storage type and free space before adding cores.
- If CPU is saturated while ingest stays healthy, reduce concurrency or simplify queries before scaling write throughput assumptions.

There is no distributed query fan-out in a cluster: each node serves queries from its own chDB. Balance read traffic across nodes (client-side or via hyperbytedb-proxy). See [V1 Stable Scope](v1-stable-scope.md).

---

## Write throughput — disk and flush, not CPU

High ingest rates mainly affect **disk IOPS**, **data growth**, and whether the **flush pipeline keeps up** — not how many CPU cores you need.

| Sustained ingest | Disk guidance | When to tune flush |
|-----------------|---------------|-------------------|
| < 100K points/sec | SSD; 3000+ IOPS usually sufficient | Defaults are fine for most deployments |
| 100K–500K points/sec | SSD or NVMe | Watch flush duration and WAL sequence if ingest is bursty |
| 500K–1M points/sec | NVMe recommended | Shorten `flush.interval_secs` or raise `max_points_per_batch` if the WAL grows between flushes |

**What high write rates do affect:**

- **Daily data volume** — feeds the storage budget below.
- **WAL disk use** — short-lived; typically 1–5% of total data volume.
- **Flush lag** — if ingest outruns flush, the WAL grows until catch-up; this shows up in metrics before CPU spikes.

**What high write rates usually do not require:**

- Proportional CPU scaling — the WAL uses bounded in-memory structures and group commit.
- Proportional RAM scaling — RocksDB WAL footprint is capped by design; Arrow WAL cache size follows flush batch settings, not ingest rate linearly.

If writes are slow or the WAL grows without bound, check disk IOPS and flush settings first, not core count.

---

## Storage — retention and data volume

Disk sizing follows **how much data you keep**, not query concurrency.

### Estimating capacity

1. Estimate raw daily volume:
   ```
   daily_raw_bytes ≈ points/sec × 86,400 × average_point_size
   ```
   A typical line-protocol point is ~100 bytes (measurement, two tags, several fields).

2. Apply compression — numeric time-series often compresses **5–10×** on disk (LZ4 default).

3. Multiply by retention days and add **20–30% free space** for MergeTree compaction.

**Example:** 100K points/sec, ~100 bytes/point, 30-day retention:
- Raw ≈ 864 GB/day → on-disk ≈ 90–170 GB/day after compression → **~3–5 TB** for 30 days plus headroom.

| Retention (30d, ~100 byte points) | Approx. ingest | Suggested disk |
|----------------------------------|----------------|----------------|
| Dev / lab | < 10K pts/s | 50 GB |
| Light production | 10K–50K pts/s | 200 GB |
| Medium production | 50K–200K pts/s | 1 TB |
| Large retention / high volume | 200K+ pts/s | 5 TB+ |

Adjust up for longer retention, larger points, or low-cardinality tag explosion.

### WAL (RocksDB)

| Metric | Guideline |
|--------|-----------|
| Disk type | SSD or NVMe recommended |
| IOPS | 3000+ for sustained ingest |
| WAL size | Typically 1–5% of total data volume |
| Budget | Allocate ~10% of total data budget for WAL + metadata |

### chDB MergeTree (data)

| Metric | Guideline |
|--------|-----------|
| Disk type | SSD for primary data; HDD acceptable for cold storage |
| Compression | LZ4 default; expect 5–10× compression for numeric time-series |
| Raw → on-disk | 100 MB raw → ~10–20 MB on disk |
| Retention | Expired data is removed by the retention service; verify policies match your budget |

### Compaction headroom

MergeTree compaction runs in the background. Leave **20–30% free disk space** so compaction can proceed without blocking ingest.

---

## Memory

| Component | Typical usage | Primary driver |
|-----------|--------------|----------------|
| chDB | 500 MB–4 GB+ | Query complexity and concurrent queries |
| RocksDB WAL | ~100–300 MB | Bounded memtable footprint; not unbounded with ingest |
| Arrow WAL cache | 100 MB–2 GB | Flush batch size; disable with `arrow_wal_enabled = false` if memory is tight |
| HTTP / connections | 50–200 MB | Number of clients |
| OS / page cache | 500 MB–1 GB+ | Helps read performance; leave headroom |

**Rule of thumb:** Pick RAM from the **query profile** table above. A node ingesting 500K points/sec with two dashboards needs far less RAM than a node ingesting 50K points/sec with fifty concurrent heavy aggregates.

---

## Cluster sizing

In a multi-node cluster, each node ingests, replicates, and **queries its own chDB copy**. Scale cluster size for **read capacity, fault tolerance, and ingest fan-out** — not by multiplying CPU per point/sec.

| Concern | Guidance |
|---------|----------|
| Query load | Split dashboards and read clients across nodes; each node needs CPU/RAM for its share of concurrent queries |
| Replication | Expect **~25–40% extra CPU and network** on coordinators for fan-out, depending on cluster size and sync mode |
| Per-node spec | Use the query profile table for CPU/RAM; use the storage table for disk per node |
| Async replication | Default; lower write latency, eventual consistency across peers |
| Sync quorum | Stronger durability; higher write latency — size network and CPU headroom accordingly |

Example starting points (per node, before replication overhead):

| Cluster | Per-node CPU / RAM | Typical use |
|---------|-------------------|-------------|
| 3 nodes | 4 CPU / 8 GB | HA + moderate query load |
| 3 nodes | 8 CPU / 16 GB | HA + heavier dashboards per node |
| 5 nodes | 8 CPU / 16 GB | Higher read fan-out, more ingest spread |

---

## Tuning by symptom

| Symptom | Likely cause | What to try |
|---------|-------------|-------------|
| High CPU | Many concurrent or heavy queries | Lower `max_concurrent_queries`; narrow time ranges in queries; reduce `query_pool_size` if threads oversubscribe |
| Slow queries | Wide scans, missing time filter | Add `WHERE time > ...`; reduce concurrent query load |
| High memory | chDB working set or Arrow cache | Cap concurrent queries; set `arrow_wal_enabled = false` if flush cache is the issue |
| Disk filling up | Retention too long or underestimated volume | Shorten retention policies; verify `[retention]` is enabled |
| WAL growing / flush lag | Ingest faster than flush or slow disk | Faster disk; decrease `flush.interval_secs`; increase `max_points_per_batch` |
| Write errors under load | Disk full or IOPS saturated | Free space; move to NVMe; check compaction headroom |

See [Troubleshooting](troubleshooting.md) for step-by-step fixes.

---

## After deployment — what to watch

Once the node is running with representative ingest **and** query traffic, use Prometheus metrics (see [Administration](administration.md)) to confirm sizing:

| Metric | Healthy signal | May need adjustment |
|--------|---------------|---------------------|
| `hyperbytedb_query_duration_seconds` | P95 stable under your SLO | High latency → query load or disk; add time filters or CPU |
| `hyperbytedb_write_duration_seconds` | P99 under ~500 ms | Spikes → disk IOPS or flush backlog |
| `hyperbytedb_flush_duration_seconds` | P99 under ~10 s | Rising trend → flush or disk tuning |
| `hyperbytedb_wal_last_sequence` | Grows then drops after each flush | Steady climb → flush falling behind ingest |
| Node CPU / RAM (host metrics) | Headroom during peak dashboard load | Saturated CPU with low ingest → query concurrency |

Start with defaults, deploy with realistic query patterns (same dashboards and alerts you expect in production), then adjust concurrency limits and hardware based on what you observe.

---

## See Also

- [Configuration](configuration.md) — Tuning parameters (`max_concurrent_queries`, `query_pool_size`, `write_pool_size`, flush settings)
- [Administration](administration.md) — Metrics and monitoring
- [Troubleshooting](troubleshooting.md) — Query timeouts, memory, and flush issues
- [V1 Stable Scope](v1-stable-scope.md) — Supported topologies and availability model
