# Operating a sharded cluster

What to expect when you run, scale and restart a sharded HyperbyteDB cluster
on Kubernetes. Every number here was measured, and each section says where
and on which build. Treat the numbers as an order of magnitude for your own
hardware, not as a promise.

**Test bed.** Unless a section says otherwise: the `n3` k3s cluster, one
shared host with 24 cores (48 threads), pods limited to 3 CPU and 16 GiB,
replication factor 2, local-path volumes, the in-cluster `hyperbytedb-cli`
bench (2 clients per node, 64 requests in flight each, 5,000-point batches,
400,000 series per node). The host also runs other workloads, so throughput
varies from run to run by tens of percent; compare builds only side by side.

## Quick reference

| You do | What happens | How long (n3) |
|---|---|---|
| Restart every pod (Parallel policy) | All pods restart together; writes fail until a coordinator is back | ~105 s for 8 pods |
| Restart every pod (OrderedReady) | One pod at a time | over an hour for 8 pods |
| Scale up 3 → 6 | New pods ready in seconds; regions copy onto them in the background | pods 20 s, layout 48 min |
| Scale up 6 → 8 | Same | pods 18 s, layout over 60 min |
| Scale down 8 → 3 | Departing nodes go unready, their regions are copied off, then they leave | hours at current copy speed (see below) |
| `CREATE MATERIALIZED VIEW` | Replicated through Raft to every node | ~30 ms |

No points were lost in any of these steps on build `8d7086e` / `792c94b`
(2026-10-10): every database written during the run read back in full, about
1.02 billion points across six databases.

## Operator annotations

All are opt-in, per cluster, and set on the `HyperbytedbCluster` object.

| Annotation | Effect | Recommended |
|---|---|---|
| `hyperbytedb.hyperbyte.cloud/parallel-pods: "true"` | The StatefulSet uses `Parallel` pod management. The operator recreates the StatefulSet once to apply it (pods are kept). | Yes. Under `OrderedReady` a departing node reports unready, and Kubernetes will not create a pod above an unready one: a pod deleted during a scale-down was never recreated, and a full restart took over an hour. |
| `hyperbytedb.hyperbyte.cloud/evacuate-together: "true"` | A scale-down decommissions all departing nodes at once instead of one at a time. | Yes, on `8d7086e` or later (see "Scaling down"). |
| `hyperbytedb.hyperbyte.cloud/control-listener: "true"` | Liveness and Raft traffic use a separate listener (server port + 1) on its own runtime, so a write-saturated node still answers probes and Raft. | Yes. |

## Writes

- **Throughput.** N=3 reached 1.12-1.28 M points/s. At N=6 and N=8 the same
  bench gave anything from 0.33 M to 1.25 M points/s across runs. Runs
  taken while region copies were still in progress were at the low end. An
  earlier acceptance run under equal host load measured 1.98x at N=6 and 2.52x
  at N=8 against N=3 (2026-10-05).
- **Errors under overload.** When a coordinator cannot reach another region's
  primary in time, the write fails with
  `write partially applied: X of Y forwarded regions failed (local bucket applied)`.
  The part already applied is safe to write again (rows are idempotent per
  series and time), so clients should retry the whole batch. The bench saw
  this at N=6 on a busy host (6,474 errors, all retried; nothing lost).
- **During scaling** writes keep working. A steady writer saw 3 errors in
  37,411 requests during 3 → 6, and 0 errors in 52,715 requests during
  8 → 3, including a restart of every pod in the middle of it.
- **Acknowledged writes survive a process crash**: the WAL is synchronous.
  On a sharded node the WAL keeps at most 1 GiB behind the flush.

## Reads

- A query whose `WHERE` fixes every tag (`host = 'a'`, an `OR` of those, or a
  Grafana-style `host =~ /^(a|b|c)$/`, each with time bounds) goes only to the
  regions that hold those series. Anything else goes to every region.
- Each region a query touches is one chDB query on the node that holds it,
  so concurrent multi-region queries compete for that node's **query pool**.
  The database default is 12 connections (`chdb.query_pool_size`). Operator
  `25812f2` and later writes 12 when the spec sets no pool field; older
  operators write `pool_size = 1`, so set `spec.chdb.queryPoolSize`
  explicitly there. With 4 concurrent
  10-host `GROUP BY host` queries over 6 regions, p50 was 250-450 ms with a
  pool of 1 and 90-130 ms with a pool of 4; a single such query alone takes
  about 55 ms, the same as one host.
- A point lookup (`last(value)` for one host) is 30-60 ms p50 under the same
  load.
- During a restart, a query that needs a node that is not up yet fails with
  `peer unreachable` (HTTP 502) rather than returning partial results.

## Scaling up

1. The new pods start and join in seconds.
2. The scheduler copies regions onto them in the background, a few at a
   time (`sharding.schedule_limit`, default 4). A copy adds the new node as a
   learner, copies the data, then promotes it; reads never go to a learner.
3. "Settled" means no copy is in flight and every node holds regions. 3 → 6
   took 48 minutes with about 280 million points stored (105 region moves);
   6 → 8 did not settle within an hour.

**Copy speed is the main limit today**: about 2.2 MB/s per copy, ~4.5 MB/s
for the whole cluster with two copies running. The movement budget
(`sharding.movement_budget_bytes_per_sec`, default 50 MiB/s) is not what
limits it. `79a4eaf` makes the chunk export about 4x cheaper (0.24 s → 0.06 s
per 50,000 rows); the rest of the per-chunk cost is still being profiled.

Watch: `hyperbytedb_shard_transfer_bytes` (rate per pod),
`hyperbytedb_shard_learners_added_total` and
`hyperbytedb_shard_learners_promoted_total`, and the layout at
`GET /internal/shard/map` (`staging_peers` lists learners).

## Scaling down

1. The operator marks the departing nodes `decommissioning` (all at once
   with `evacuate-together`, otherwise one at a time).
2. **Departing pods report unready** (`/health` returns 503). This is
   expected; they keep serving the copies of their data.
3. Each region on a departing node gets a copy on a remaining node, then the
   departing peer is removed. When a node holds no region it becomes
   `leaving`, and the operator deletes its pod and PVC.

Expect it to take as long as copying the departing nodes' data at the copy
speed above.

**Fixed in `8d7086e`:** removing a departing peer used to wait for one of the
4 mover slots that copies hold for minutes. On n3 an 8 → 3 scale-down added
live copies to 80 regions but dropped almost no departing peers, so no node
could leave. After the fix the departing nodes dropped about 63 peers in the
first minute.

**Avoid scaling up while a scale-down is in progress.** On n3, raising
replicas from 3 to 6 mid-scale-down brought two departing nodes back to
`active`, but the node that was also the Raft leader stayed
`decommissioning` inside a 6-replica cluster. No data was lost; wait for the
scale-down to finish first.

Watch: `GET /cluster/nodes` (each node's state),
`hyperbytedb_shard_decommission_completed_total`, and the per-node peer
counts in `GET /internal/shard/map`.

## Restarts and upgrades

- With `parallel-pods`, deleting every pod restarted 8 pods in about 105 s
  and 3 pods in 107-114 s. Writes fail while no coordinator is up.
- A rolling restart under `OrderedReady` takes about 16 s per pod when every
  pod is ready; during a scale-down it stalls (see the annotation above).
- `GET /cluster/leader` shows the Raft leader and term. A term that keeps
  rising means leadership is being lost (before `7c81d77`, every
  `CREATE MATERIALIZED VIEW` cost an election).

## Materialized views

- **Each node rolls up the rows written through it** into its own copy of
  the destination. A query on the destination asks every node that may hold
  rollups (active, draining, disconnected or decommissioning) and adds the
  results. If one of those nodes cannot answer, the query fails instead of
  returning a short count.
- `CREATE MATERIALIZED VIEW` goes through Raft and returns in about 30 ms.
  A node that has not seen the source measurement yet gets its schema with
  the statement. `hyperbytedb_schema_mutations_applied_total{op="create_materialized_view"}`
  should rise by one on every node; a node where an apply fails logs
  `schema mutation apply failed`.
- `WITH BACKFILL` rolls up the existing history once, run by the Raft leader
  after its own apply; a node that has not created the view yet is retried
  for up to 30 s (`b570f03` and later; before that every node backfilled,
  counting the history once per node). Verification on n3 is pending.
- Destination regions are never moved by the scheduler.
- **Known limit:** a node that leaves the cluster takes its rollups with it.
  Rebuild the view `WITH BACKFILL` after a scale-down if you need them.
- Rows that reach a destination any other way are dropped at flush with an
  error log and counted in
  `hyperbytedb_flush_rows_dropped_total{reason="materialized_destination"}`.
  This should stay at 0.

Verified on n3 (`6d354fb`): 60,000 points written through three pods rolled
up to exactly 60,000 (counts and means match per host per minute), both right
away and five minutes later.

## Pending measurements

These are being measured on the beta candidate and will be added here:

- Memory and time for `DROP DATABASE` on a large database.
- Materialized view counts after scaling 3 → 6 → 3.
- Query pool 4 vs 12 under the read bench.
- Where the rest of a region copy's time goes.
