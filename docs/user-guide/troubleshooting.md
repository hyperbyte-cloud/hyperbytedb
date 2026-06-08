# Troubleshooting

Common problems and their solutions when running HyperbyteDB.

---

## Startup Failures

### `libchdb.so: cannot open shared object file`

**Cause:** libchdb is not installed, or it is on disk (often under `/usr/local/lib/`) but the dynamic linker is not using that path—so the binary fails immediately at process start, sometimes right after a successful `cargo build` / `cargo run` link step:
```
error while loading shared libraries: libchdb.so: cannot open shared object file: No such file or directory
```

**Fix — install and refresh the cache:**
```bash
curl -sL https://lib.chdb.io | bash
sudo ldconfig
```

**Fix — library is installed but the loader still skips `/usr/local/lib`:** if `ls /usr/local/lib/libchdb.so` succeeds, register that directory with the dynamic linker, then refresh the cache:
```bash
echo "/usr/local/lib" | sudo tee /etc/ld.so.conf.d/chdb.conf
sudo ldconfig
```

Verify: `ls /usr/local/lib/libchdb.so` and, optionally, `ldconfig -p | grep chdb`

### `std::bad_function_call` crash on startup

**Symptom:** HyperbyteDB aborts immediately with:
```
terminate called after throwing an instance of 'std::__1::bad_function_call'
  what():  std::bad_function_call
Aborted
```

**Cause:** Incompatible system-installed `libchdb.so`. The crash happens during dynamic library loading, before any Rust code runs.

**Fix (Option A — Recommended):** Use the chdb-rust bundled library:
```bash
# Temporarily move the system libchdb
sudo mv /usr/local/lib/libchdb.so /usr/local/lib/libchdb.so.bak
sudo mv /usr/local/include/chdb.h /usr/local/include/chdb.h.bak

# Rebuild so chdb-rust downloads its own
cargo clean -p chdb-rust && cargo build --release

# Run with the bundled library
LIBCHDB_DIR=$(find target -name "libchdb.so" -path "*/build/chdb-rust-*/out/*" | head -1 | xargs dirname)
LD_LIBRARY_PATH="$LIBCHDB_DIR:$LD_LIBRARY_PATH" ./target/release/hyperbytedb serve

# Optionally restore for other tools
sudo mv /usr/local/lib/libchdb.so.bak /usr/local/lib/libchdb.so
```

**Fix (Option B):** Reinstall libchdb from the latest release:
```bash
curl -sL https://lib.chdb.io | bash
```

### `failed to open WAL`

**Cause:** Corrupted WAL directory (e.g., unclean shutdown, disk error).

**Fix:** Restore from a backup, or delete the `wal_dir` to start fresh (data in the WAL that hasn't been flushed will be lost).

### `address already in use`

**Cause:** Another process is listening on the same port.

**Fix:** Change the `port` in config, or stop the conflicting process:
```bash
lsof -i :8086
```

### `tls_cert_path ... not found`

**Cause:** TLS is enabled but certificate files are missing.

**Fix:** Check the paths in your config, or disable TLS:
```toml
[server]
tls_enabled = false
```

---

## Writes Succeed but Queries Return Empty

Data must be flushed from the WAL into chDB MergeTree tables before it becomes queryable.

**Checklist:**

1. **Wait for flush.** The default flush interval is 10 seconds. Wait at least that long after writing.

2. **Check logs for flush errors:**
   ```bash
   # Look for flush-related messages
   journalctl -u hyperbytedb | grep -i flush
   ```

3. **Verify chDB data path is writable** (configured via `[chdb].session_data_path` in `config.toml`):
   ```bash
   ls -la /var/lib/hyperbytedb/chdb/
   ```

4. **Verify the database exists:**
   ```bash
   curl -sS -G 'http://localhost:8086/query' \
     --data-urlencode 'q=SHOW DATABASES'
   ```

5. **Check the measurement exists:**
   ```bash
   curl -sS -G 'http://localhost:8086/query' \
     --data-urlencode 'db=mydb' \
     --data-urlencode 'q=SHOW MEASUREMENTS'
   ```

---

## Query Timeouts

**Symptom:** Queries return HTTP 408 or take very long.

**Fixes:**

1. **Increase the timeout:**
   ```toml
   [server]
   query_timeout_secs = 120
   ```

2. **Add a time range to your query.** Queries without `WHERE time > ...` scan all data.

3. **Cap concurrent queries.** chDB is a process-global singleton (one real session); `chdb.pool_size` is ignored. Tune `server.max_concurrent_queries` instead so heavy queries don't pile up on the single engine session.

4. **Narrow the time range** in your query to reduce scanned data volume.

---

## Cardinality Limit Errors

**Symptom:** Writes return HTTP 422 with `cardinality limit exceeded`.

**Possible causes:**
- High-cardinality values (UUIDs, timestamps, request IDs) used as tag values. These should be fields instead.
- Legitimate growth beyond the configured limits.

**Fixes:**

1. **Investigate the data model.** Tags are indexed; use fields for high-cardinality values.

2. **Increase limits if the growth is expected:**
   ```toml
   [cardinality]
   max_tag_values_per_measurement = 500000
   max_measurements_per_database = 50000
   ```

---

## Field Type Conflict

**Symptom:** Writes return HTTP 400 with `field type conflict`.

**Cause:** A field was previously registered with one type (e.g., float) and a new write sends a different type (e.g., integer) for the same field name.

**Fix:** Ensure all writes use the same type for each field. If the original type was wrong, you need to drop the measurement and recreate it:
```sql
DROP MEASUREMENT "problematic_measurement"
```

---

## Cluster Replication Issues

### Writes not appearing on peer nodes

1. **Check connectivity:** Ensure all nodes can reach each other on their `cluster_addr` and port.

2. **Check logs for replication errors:**
   ```bash
   journalctl -u hyperbytedb | grep -i "replication failed"
   ```

3. **Verify peers configuration:** The `peers` list should NOT include the node's own `cluster_addr`.

4. **Check node states:**
   ```bash
   curl -s http://node1:8086/cluster/metrics | jq .
   curl -s http://node2:8086/cluster/metrics | jq .
   ```
   Nodes must be in `Active` state to accept replicated writes.

### Persistent data gaps between nodes

**Symptoms:** One node shows fewer series or buckets than others for the same time range; `/internal/sync/manifest` responses differ between peers; metrics may show `hyperbytedb_replication_lag_wal_seq` increasing.

**Checklist:**

1. **Check peer reachability.** Sync and replication only contact **active** members; fix connectivity or heartbeat issues first.

2. **Compare manifests across nodes:**
   ```bash
   curl -s http://node1:8086/internal/sync/manifest | jq .
   curl -s http://node2:8086/internal/sync/manifest | jq .
   ```

3. **Verify all peers run the same HyperbyteDB version** and compatible `libchdb.so`.

**Reference:** [Deep Dive: Clustering](../deep-dive/deep-dive-clustering.md).

### Split-brain detection

Compare membership views across nodes:
```bash
curl -s http://node1:8086/cluster/metrics | jq '.membership'
curl -s http://node2:8086/cluster/metrics | jq '.membership'
curl -s http://node3:8086/cluster/metrics | jq '.membership'
```

If nodes have different membership views, check network partitions and ensure all peers can communicate.

---

## High Memory Usage

1. **Reduce flush batch size:**
   ```toml
   [flush]
   max_points_per_batch = 50000
   ```

2. **Tune WAL batching** if ingest pressure is high:
   ```toml
   [flush]
   wal_batch_size = 32
   wal_batch_delay_us = 500
   ```

---

## See Also

- [Configuration](configuration.md) — All tuning parameters
- [Administration](administration.md) — Monitoring and operational procedures
