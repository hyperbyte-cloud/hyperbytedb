# HyperbyteDB 0.9.0-beta

The beta of sharded clusters whose write throughput grows with the number of
nodes, and that scale up and down without losing points or materialized
view rollups. 204 commits since 0.8.5-beta. Pair it with operator v0.2.0.

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
- **Runtime settings.** `SET query_pool_size = <n> | DEFAULT` and
  `SHOW SETTINGS`, replicated to every node. The query pool now defaults to
  one connection per CPU plus one.
- **Smaller WAL, faster ingest.** The sharded WAL keeps at most 1 GiB behind
  the flush (it used to keep 500k entries, 18-72 GB on n3); ingest caches and
  series ids are computed once per batch. WAL files stay compressed: storing
  them uncompressed (tried during this release) saturated a shared disk at
  6 nodes and cut writes by about a third.

## Upgrading from 0.8.x

- **Config is strict now.** 0.9 refuses to start on a config key it does not
  know (0.8 ignored them). Operator v0.2.0 renders each cluster for the
  version it runs; with a hand-written config, remove keys 0.9 does not list
  in [Configuration](configuration.md).
- **Pools.** `chdb.query_pool_size` and `chdb.write_pool_size` replace the
  single `pool_size` (still accepted for both). Leave `query_pool_size` unset
  to let the database size it.
- **Rolling upgrade of a sharded cluster.** Turn off
  `sharding.add_peer_proposals_enabled` and
  `sharding.remove_peer_proposals_enabled` for the duration (nodes on the
  older build cannot decode those Raft entries), and do not run `SET`.
- The `X-Influxdb-Version` header now reports the running version
  (`HyperbyteDB-0.9.0-beta`); it was fixed at 0.8.5.

## Known limits

- **Disk space is not reclaimed after a scale-up:** a node keeps its copy of
  a region it handed over (never read). After 3 -> 6 the cluster stored 1.65x
  the rows replication needs.
- **Region copies are slow:** about 2.2 MB/s per copy, so a scale-up of a
  few hundred million points takes 45 minutes or more to settle, and write
  throughput is lower until it does.
- **Query cost grows with regions per node:** a 10-host `GROUP BY host` took
  about 120 ms on a 6-region measurement and 1.5 s on a 48-region one.
- A departing node can stay Raft leader until its pod is deleted.
- Destination regions of materialized views keep naming nodes that have
  left (no effect on data or queries).
- Do not scale up while a scale-down is still in progress.
