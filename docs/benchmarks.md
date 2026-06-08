# Benchmarks

HyperbyteDB ships four **default** Criterion benchmark suites, plus one for the proxy:

1. **Line protocol ingestion** — parse → metadata → WAL
2. **Columnar MessagePack ingestion** — decode → metadata → WAL (enabled by default via `columnar-ingest`)
3. **Fixed-dataset queries** — InfluxQL parse and execute against seeded chDB tables
4. **Flush service** — WAL → chDB flush, incremental tick, and drain
5. **Proxy routing** (`hyperbytedb-proxy`) — backend pick and drain-response detection

Run all of them:

```bash
cargo bench
# or
./scripts/bench-all.sh
```

Throughput is reported as **logical points per second** for ingestion batches of **1000** rows (`Throughput::Elements(1000)`), unless noted.

For HTTP load testing against a live server, use [`scripts/load.sh`](../scripts/load.sh) with `WRITE_FORMAT=line` or `msgpack` (k6 soak; set `RUN_BENCHES=1` to also run all Criterion suites).

---

## What exists

| Artifact | Command | Scope |
|----------|---------|-------|
| [`benches/ingestion_line_protocol.rs`](../hyperbytedb/benches/ingestion_line_protocol.rs) | `cargo bench --bench ingestion_line_protocol` | Line protocol ingest path |
| [`benches/ingestion_columnar.rs`](../hyperbytedb/benches/ingestion_columnar.rs) | `cargo bench --bench ingestion_columnar` | Columnar msgpack ingest path |
| [`benches/query_fixed_dataset.rs`](../hyperbytedb/benches/query_fixed_dataset.rs) | `cargo bench --bench query_fixed_dataset` | InfluxQL queries on fixed datasets |
| [`benches/flush_service.rs`](../hyperbytedb/benches/flush_service.rs) | `cargo bench --bench flush_service` | WAL → chDB flush path |
| [`hyperbytedb-proxy/benches/routing.rs`](../hyperbytedb-proxy/benches/routing.rs) | `cargo bench -p hyperbytedb-proxy --bench routing` | Backend routing hot path |
| [`scripts/load.sh`](../scripts/load.sh) | k6 HTTP soak | End-to-end write + optional all Criterion benches |
| [`scripts/bench-all.sh`](../scripts/bench-all.sh) | `cargo bench` wrapper | All Criterion suites (hyperbytedb + proxy) |

`columnar-ingest` is a **default feature** (`Cargo.toml`), so columnar benches build without extra flags.

---

## Prerequisites

- **Release profile:** Criterion uses `[profile.bench]` (inherits `release` with debug symbols).
- **libchdb:** required for query and flush benches. Ensure `/usr/local/lib` is on the loader path (`sudo ldconfig` after install, or `LD_LIBRARY_PATH=/usr/local/lib`).
- **Ingestion batch size:** `const BATCH: u64 = 1000` in both ingest bench files.
- **Dataset profiles:** `BENCH_DATASET=small|medium|large` (default `small`) for query and flush benches. Seeding runs once per process before timed iterations (flush full/drain re-seed WAL per sample via `iter_batched`).

| Profile | Points | Hosts | Measurements |
|---------|--------|-------|--------------|
| `small` (default) | 10k | 10 | `cpu` |
| `medium` | 1M | 100 | `cpu`, `mem`, `disk` |
| `large` | 10M | 1000 | `cpu` |

```bash
BENCH_DATASET=medium cargo bench --bench query_fixed_dataset
```

---

## Line protocol (`ingestion_line_protocol`)

```bash
cargo bench --bench ingestion_line_protocol
```

### Sequential groups

| Group | Function | What it measures |
|-------|----------|------------------|
| `line_protocol_parse` | `parse_1000` | `parse_line_body_to_points` only |
| `line_protocol_metadata` | `parse_plus_metadata_1000` | Parse + `prepare_batch_metadata` |
| `line_protocol_wal` | `metadata_plus_wal_append_1000` | Parse + metadata + WAL append |

### Concurrent groups

| Group | What it measures |
|-------|------------------|
| `line_protocol_parse_concurrent` | Parallel parse (`c1`, `c4`, `c16`, `c<cpus>`) |
| `line_protocol_metadata_concurrent` | Parallel parse + metadata |
| `line_protocol_wal_concurrent` | Parallel full WAL path |
| `line_protocol_wal_batched_concurrent` | Parallel path through `BatchingWal` |

Synthetic lines: `bench,host=bench v={i} {ts}` with millisecond timestamps.

**Excludes:** HTTP, flush, chDB, replication.

---

## Columnar MessagePack (`ingestion_columnar`)

```bash
cargo bench --bench ingestion_columnar
```

### Point-expansion path

| Group | Function | What it measures |
|-------|----------|------------------|
| `columnar_decode` | `parse_1000` | msgpack → `Vec<Point>` |
| `columnar_metadata` | `prepare_batch_metadata_1000` | Points + metadata |
| `columnar_wal` | `metadata_plus_wal_append_1000` | Full WAL path |

### Fast path

| Group | Function | What it measures |
|-------|----------|------------------|
| `columnar_decode_fast` | `decode_only_1000` | `decode_columnar_batch` |
| `columnar_decode_fast` | `decode_to_points_1000` | Decode + `columnar_batch_to_points` |
| `columnar_decode_fast` | `decode_to_record_batch_1000` | Decode + Arrow `RecordBatch` |
| `columnar_metadata_fast` | `prepare_columnar_metadata_1000` | Structured batch + metadata |
| `columnar_wal_fast` | `fast_metadata_plus_wal_append_1000` | Fast path + WAL |

### Concurrent groups

Same pattern as line protocol: `columnar_decode_concurrent`, `columnar_metadata_concurrent`, `columnar_wal_concurrent`, `columnar_wal_batched_concurrent`.

See [Columnar MessagePack write format](#columnar-messagepack-write-format-v1) below.

---

## Fixed-dataset queries (`query_fixed_dataset`)

Seeds data via line protocol ingest + flush, then benchmarks InfluxQL execution through `QueryServiceImpl` (parse → translate → chDB → JSON response).

```bash
cargo bench --bench query_fixed_dataset
BENCH_DATASET=medium cargo bench --bench query_fixed_dataset
```

### Groups (suffix is dataset profile, e.g. `_small`)

| Group | Query | What it measures |
|-------|-------|------------------|
| `query_parse_{profile}` | `SELECT mean(idle) …` | `timeseriesql::parse` only |
| `query_metadata_show_measurements_{profile}` | `SHOW MEASUREMENTS` | Metadata-only query |
| `query_metadata_show_tag_keys_{profile}` | `SHOW TAG KEYS FROM cpu` | Metadata + catalog |
| `query_point_limit10_{profile}` | `SELECT * FROM cpu LIMIT 10` | Small scan |
| `query_point_limit1000_{profile}` | `SELECT * FROM cpu LIMIT 1000` | Larger scan |
| `query_aggregate_mean_{profile}` | `SELECT mean(idle) FROM cpu` | Full-table aggregate |
| `query_aggregate_group_by_time_{profile}` | `… GROUP BY time(1h)` | Time bucket aggregate |
| `query_aggregate_group_by_tag_{profile}` | `… GROUP BY host` | Tag aggregate |
| `query_filtered_host_{profile}` | `WHERE host = 'host1' LIMIT 100` | Tag filter |
| `query_time_range_{profile}` | `WHERE time >= … AND time < …` | Time filter |

Query strings align with [`scripts/query.js`](../scripts/query.js) categories (metadata, point, aggregate, filtered). Concurrent query fan-out is covered by the k6 script against a live server.

**Excludes:** HTTP layer, auth, replication.

---

## Flush service (`flush_service`)

Seeds WAL entries via line protocol ingest (no flush), then benchmarks `FlushServiceImpl` (WAL read → prepare → chDB sink → truncate).

```bash
cargo bench --bench flush_service
BENCH_DATASET=medium cargo bench --bench flush_service
```

### Groups (suffix is dataset profile, e.g. `_small`)

| Group | Function | What it measures |
|-------|----------|------------------|
| `flush_full_{profile}` | `flush_all_{profile}` | Full WAL → chDB flush for entire dataset |
| `flush_incremental_{profile}` | `flush_1000_{profile}` | After baseline flush, ingest 1000 points then flush (models periodic tick) |
| `flush_drain_{profile}` | `drain_{profile}` | `drain()` until WAL empty |

**Throughput:** `flush_full_*` and `flush_drain_*` report points/sec for the full dataset; `flush_incremental_*` reports 1000 points/sec.

**`flush_drain_large`:** skipped by default for the `large` profile (10M points). Set `BENCH_FLUSH_DRAIN_LARGE=1` to enable.

**Excludes:** HTTP, replication, cluster truncate barrier logic.

---

## Proxy routing (`hyperbytedb-proxy`)

```bash
cargo bench -p hyperbytedb-proxy --bench routing
```

### Groups

| Group | Functions | What it measures |
|-------|-----------|------------------|
| `proxy_pick_active` | `pick_{n}_backends` (n = 1, 4, 8, 16, 32) | Round-robin pick over `Active` backends |
| `proxy_pick_active_excluding` | `pick_excluding_{n}_backends` | Pick excluding one backend (retry path) |
| `proxy_pick_active_concurrent` | `pick_8_backends_c{1,4,16,<cpus>}` | Parallel `pick_active` under contention |
| `proxy_looks_like_drain` | `drain_json_pass`, `drain_json_fail`, `success_body`, `binary_body` | Drain JSON envelope detection |

**Excludes:** HTTP forwarding, DNS discovery, health probes.

---

## Columnar MessagePack write format (v1)

Optional ingest encoding, enabled by default (`columnar-ingest` feature).

### HTTP

- **Method / path:** `POST /write`
- **Query:** `db` (required), `precision` optional
- **`Content-Type`:** `application/vnd.hyperbytedb.columnar-msgpack.v1`

### MessagePack body

| Key | Type | Required | Description |
|-----|------|----------|-------------|
| `measurement` | string | yes | Shared measurement |
| `tags` | map | no | Constant tags per row |
| `field` | string | yes | Single float field name |
| `values` | float64[] | yes | One sample per row |
| `timestamps` | int64[] | no | Parallel to `values` |

---

## Reporting environment

When publishing numbers, record:

- Git: `git rev-parse HEAD`
- Rust: `rustc -V`
- CPU / RAM / disk type
- `BENCH_DATASET` for query and flush benches

---

## Expectations and limits

- Ingestion benches isolate parse → metadata → WAL on temp RocksDB.
- Query and flush benches include chDB execution but not HTTP or cluster replication.
- Proxy routing benches use synthetic backends (no live hyperbytedb required).
- Criterion HTML reports: `target/criterion/`
- `BENCH_DATASET=large` seeding can take several minutes (query and flush suites).

---

## See also

- [Development setup](developer-guide/development-setup.md)
- [Deep dive: Write path](deep-dive/deep-dive-write-path.md)
- [Deep dive: Read path](deep-dive/deep-dive-read-path.md)
