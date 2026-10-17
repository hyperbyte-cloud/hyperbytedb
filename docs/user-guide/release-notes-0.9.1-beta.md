# HyperbyteDB 0.9.1-beta

The beta of sharded clusters whose write throughput grows with the number of
nodes, and that scale up and down without losing points or materialized
view rollups. 228 commits since 0.8.5-beta. Pair it with operator v0.3.0.
0.9.0-beta was tagged but never published; this release replaces it.

For what to expect when running it (timings, failure modes, metrics), see
[Operating a sharded cluster](operating-sharded-clusters.md).

## Highlights

- **Sharding that scales writes.** A measurement is split into regions
  spread across the nodes (2 per node at creation), placed by rendezvous
  hashing, with replication factor 3 by default (the tests below used 2).
  On a single shared 24-core host the acceptance test (fresh cluster per
  size, 90M points, equal host load) passes: 865k points/s at 3 nodes and
  1.58M at 6 (1.82x against a 1.80x bar), every point read back. Right after
  a 3 -> 6 scale-up, 6 nodes wrote 1.38M points/s.
- **Scale-ups and scale-downs keep every point.** Regions are copied to a
  new node as a learner and promoted only when complete; a departing node is
  emptied before it leaves. In the final test, 1.02 billion points across six
  databases read back in full after 3 -> 6 -> 8 -> 3, with a writer running
  throughout.
- **Materialized views on sharded clusters.** Rollups are computed where
  rows are first stored, queries add every node's partial rollups, and a
  departing node hands its rollups to a node that stays. A view stays exact
  through 3 -> 6 -> 3. `WITH BACKFILL` rolls up history once.
- **Faster reads.** Tag filters (equality, OR, and Grafana's
  `host =~ /^(a|b|c)$/`) go only to the regions that hold those series and are
  pushed down before the series join; results stream as Arrow.
- **Runtime settings.** `SET <setting> = <value> | DEFAULT` and
  `SHOW SETTINGS`, replicated to every node and kept across restarts. 20
  settings: log level, query and write pools, query cap and timeout, write
  admission, rate limit, cardinality limits, flush interval and the shard
  scheduler's gates, copy slots, copy budget and split/merge thresholds. The
  query pool now defaults to one connection per CPU plus one.
- **Auth on clusters.** With auth on, nodes and the operator reach each
  other's internal routes with a shared cluster token
  (`auth.cluster_token`); before, a cluster with auth on could not run Raft
  or replication at all. `auth.admin_username` / `admin_password` create the
  first admin at startup. Operator v0.3.0 keeps all three in a Secret when
  `spec.auth.enabled` is set.
- **Per-database READ and WRITE.** `GRANT READ | WRITE | ALL ON <db>`,
  `REVOKE` of one privilege, and `SHOW GRANTS FOR <user>`, as in InfluxDB v1.
  Grants survive restarts, node syncs, `SET PASSWORD` and admin changes;
  before, all of those silently dropped a user's grants.
- **Grafana fixes from UAT.** `/query` answers gzipped when asked;
  `SHOW TAG VALUES` applies `WHERE` (with `::tag`), `LIMIT` and `OFFSET`
  (all three were ignored); aggregates keep answering while a region's
  primary restarts; wide panels merge and fill on the coordinator instead of
  sorting in every region.
- **One region per new measurement.** A new measurement starts as one
  region and splits as it grows (`sharding.bootstrap_regions_per_node`
  now defaults to 0; it was 2). Small neighbouring regions are moved onto
  the same nodes and merged, so measurements pre-split before this collapse
  back. A merge of two regions at different epochs is only proposed once
  every node runs a build that applies it.
- **Region copies keep every row, and run 20-30x faster.** Copies page on
  `(series_id, time)` in the table's index order, with every version of a
  point in one chunk; a series longer than one chunk (50,000 rows) had the
  rest of its rows skipped on every move. A chunk too big for the receiver
  (413) is halved instead of retried forever. Chunks were sorted from the
  whole remaining region, went as JSON with one number per byte, and were
  read only after the previous one was applied: on n3 a copy ran at
  0.3-0.5 MB/s on 2.4 cores, and now runs at about 10 MB/s per copy
  (20 MB/s from one sending node) at 0.14 cores per MB/s.
- **Smaller WAL, faster ingest.** The sharded WAL keeps at most 1 GiB behind
  the flush (it used to keep 500k entries, 18-72 GB on n3); ingest caches and
  series ids are computed once per batch. WAL files stay compressed: storing
  them uncompressed (tried during this release) saturated a shared disk at
  6 nodes and cut writes by about a third.
- **Cardinality limits set at run time apply to client writes only.** A
  `SET max_tag_values_per_measurement` (or `max_measurements_per_database`)
  also refused region copies and replicas of measurements already over it.
- **Python client.** `clients/python`: standard library only, batched and
  gzipped writes that back off on 503 / `Retry-After`, InfluxQL queries.

## Upgrading from 0.8.x

- **Config is strict now.** 0.9 refuses to start on a config key it does not
  know (0.8 ignored them). Operator v0.2.0 and later render each cluster for the
  version it runs; with a hand-written config, remove keys 0.9 does not list
  in [Configuration](configuration.md).
- **Pools.** `chdb.query_pool_size` and `chdb.write_pool_size` replace the
  single `pool_size` (still accepted for both). Leave `query_pool_size` unset
  to let the database size it.
- **Rolling upgrade of a sharded cluster.** Turn off
  `sharding.add_peer_proposals_enabled` and
  `sharding.remove_peer_proposals_enabled` for the duration (nodes on the
  older build cannot decode those Raft entries), and do not run `SET`.
- **Raft timing.** Leaving `raft_heartbeat_interval_ms` unset now means a
  third of the election timeout. Operator v0.2.0's CRD used to default both
  to 1000, which Raft refuses (every node crash-loops); set
  `raftHeartbeatIntervalMs` below `raftElectionTimeoutMs` on clusters made
  from that CRD, or apply the new CRD (default 300, and a rule that rejects
  the bad pair).
- The `X-Influxdb-Version` header now reports the running version
  (`HyperbyteDB-0.9.1-beta`); it was fixed at 0.8.5.

## Known limits

- **Disk space is not reclaimed after a scale-up:** a node keeps its copy of
  a region it handed over (never read). After 3 -> 6 the cluster stored 1.65x
  the rows replication needs.
- **Regions split by series count only.** A measurement with few series and
  many points (50,000 series, 280M points on n3) stays one region, and that
  region moves as one copy: about half an hour at 10 MB/s.
- **Replicas of a region can disagree after a scale-down under writes.** In
  one 8 -> 3 run on n3, a database created as the scale-down began ended with
  two of three replicas (the primary among them) short of 3.9M of 282M
  acknowledged points; the third replica had them all, so nothing was lost,
  but queries read the short primary. Being investigated; avoid creating
  measurements while nodes are being removed.
- **Queries are cut off at `server.request_timeout_secs` (30 s)** whatever
  `query_timeout_secs` is set to: raise both for long queries.
- **Query cost grows with regions per node:** a 10-host `GROUP BY host` took
  about 120 ms on a 6-region measurement and 1.5 s on a 48-region one.
- A departing node can stay Raft leader until its pod is deleted.
- Destination regions of materialized views keep naming nodes that have
  left (no effect on data or queries).
- Do not scale up while a scale-down is still in progress.
- Boolean fields are returned as `1` / `0`, not `true` / `false`.
- `SHOW TAG VALUES FROM /regex/` is not parsed; omit `FROM` or name the measurement.
