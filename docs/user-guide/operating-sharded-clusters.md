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

## Versions

This page describes HyperbyteDB 0.9.0-beta with operator v0.2.0. The
operator renders each cluster for the HyperbyteDB version it runs (see
[HyperbytedbCluster: annotations and versions](operator/cluster.md#annotations-and-versions));
on 0.8.x the sharding, control-listener and evacuate-together features below
are not available.

## Operator annotations

All are opt-in, per cluster, and set on the `HyperbytedbCluster` object.

| Annotation | Effect | Recommended |
|---|---|---|
| `hyperbytedb.hyperbyte.cloud/parallel-pods: "true"` | The StatefulSet uses `Parallel` pod management. The operator recreates the StatefulSet once to apply it (pods are kept). | Yes. Under `OrderedReady` a departing node reports unready, and Kubernetes will not create a pod above an unready one: a pod deleted during a scale-down was never recreated, and a full restart took over an hour. |
| `hyperbytedb.hyperbyte.cloud/evacuate-together: "true"` | A scale-down decommissions all departing nodes at once instead of one at a time. | Yes, on `8d7086e` or later (see "Scaling down"). |
| `hyperbytedb.hyperbyte.cloud/control-listener: "true"` | Liveness and Raft traffic use a separate listener (server port + 1) on its own runtime, so a write-saturated node still answers probes and Raft. | Yes. |

## Writes

- **Throughput.** The acceptance test (fresh cluster per size, 90M points,
  equal host load via a twin cluster, 3 runs each; `20c761f`, 2026-10-11):
  N=3 865k points/s (860-904k), N=6 1.58M (1.51-1.60M), **1.82x**, every
  point read back, 0 restarts, 10.6-11.8 us of CPU per point. Without the
  twin load, N=3 writes 1.05-1.24M points/s.
- **Right after a scale-up** writes are close to a fresh cluster: 3 -> 6 with
  90M points stored and a writer running, then N=6 wrote 1.38M points/s with
  41 errors as soon as the layout settled.
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

## Disks

All of a node's writes (WAL, chDB parts, merges) land on its volume, and on
a shared host the disk is usually what saturates first. On n3 every volume
is on one NVMe: at 6 nodes writing flat out it carries 320-340 MB/s at about
a third busy. Saturated, it looks like this:

- writes refused with `write partially applied: ... operation timed out`
  and HTTP 503 (`still rejected after 12 retries` from a client that
  retries);
- `hyperbytedb_shard_forward_capacity_retries_total` and
  `hyperbytedb_write_admission_rejected_total{kind="shard"}` climbing fast
  (about a million capacity retries in one 90M-point run);
- the host's disk near 100% busy with write waits of 50 ms or more.

That is what an uncompressed WAL did during this release (`ec65682`,
reverted in `20c761f`): 490-540 MB/s, 69-75% busy, and N=6 writes down to
0.72-1.02M points/s. Give each node its own disk where you can, and watch
disk busy % alongside CPU.

**Disk space after a scale-up is not reclaimed.** A node that gives a
region to a new node keeps its copy of the rows (queries never read them;
only the region's current peers are asked). After 3 -> 6 with about 91M
points at replication factor 2 (182M rows needed), the cluster stored 300M:
the three old nodes kept their full pre-scale-up copies. Size disks for the
pre-scale-up data until this is fixed.

## Reads

- A query whose `WHERE` fixes every tag (`host = 'a'`, an `OR` of those, or a
  Grafana-style `host =~ /^(a|b|c)$/`, each with time bounds) goes only to the
  regions that hold those series. Anything else goes to every region.
- Each region a query touches is one chDB query on the node that holds it,
  so concurrent multi-region queries compete for that node's **query pool**.
  The default is one connection per CPU the pod may use, plus one (4 at
  3 CPU). Change it on a running cluster with `SET query_pool_size = <n>`
  (every node resizes at once; `SHOW SETTINGS` shows the value and where it
  comes from; `SET query_pool_size = DEFAULT` goes back). With 4 concurrent
  10-host `GROUP BY host` queries over 6 regions, p50 was 250-450 ms with a
  pool of 1 and 90-130 ms with a pool of 4; a single such query alone takes
  about 55 ms, the same as one host.
- The coordinator merges every region's partial result. Before `bbadb08`
  that merge was quadratic in rows per series: a Grafana panel (mean per
  30 s by host over 12 h, 5 hosts, 10 regions) spent 746 of 804 ms merging.
  On `bbadb08` the same query takes 70-150 ms on the UAT cluster: about
  60-90 ms in the region queries and 18 ms merging.
- Against InfluxDB 1.11 on the same Telegraf data (UAT, 6 nodes vs 1, ~500
  series, `110ff32`, p50): single-series reads 11-33 ms (InfluxDB 1-2 ms),
  per-host panels 18-37 ms (2-7 ms), 245-series panels 53-56 ms (177-178 ms),
  `SHOW` calls 2-23 ms (1-2 ms). Every query touches several regions, so
  HyperbyteDB pays a fan-out cost InfluxDB does not; it wins once a query
  returns many series. Before `0182f6f` set TCP_NODELAY, every query that
  touched a remote region waited up to 40 ms more (delayed ACK).
- A point lookup (`last(value)` for one host) is 30-60 ms p50 under the same
  load.
- Read bench with 90M points (4 concurrent clients, p50), at 3 nodes and
  then at 6 after scaling up: one-host `last` 72 / 46 ms, raw series 32 /
  23 ms, 10-host `GROUP BY host` (`last`) 399 / 215 ms, the Grafana regex
  form 207 / 110 ms, a full `count` 14.5 / 9.3 s. No query errors.
- **A larger pool is not always faster.** Each chDB query uses several
  threads, so on 3-CPU pods a pool of 12 was 10-40% slower than 4 for
  10-host queries (p50 `GROUP BY host` 2.0-2.2 s vs 1.5-1.6 s, 2 interleaved
  rounds, `8d7086e`). Size the pool to the CPU limit.
- **Cost grows with the regions a node holds.** The same 10-host query took
  ~120 ms on a 6-region measurement and ~1.5 s on one with 48 regions over
  3 nodes: each node runs one chDB query per region it holds.
- During a restart, a query that needs a node that is not up yet fails with
  `peer unreachable` (HTTP 502) rather than returning partial results.

## Regions per measurement

A new measurement starts as one region with `replication_factor` copies,
and splits once a region passes `region_split_series` (100k). Small
neighbouring regions (under `region_merge_series`, 20k, together) are moved
onto the same nodes and merged back. A query touches every region it can
not rule out, and waits for the slowest, so fewer regions is faster: on UAT
a 1-region query took 11-13 ms where a 10-region one took 20-60 ms. After
UAT's 130 regions collapsed to 13 (one per measurement; 7 minutes with
`split_merge_interval_secs = 60`), per-host panels took 15-22 ms p50. A
245-series panel over 2 h took 117 ms against 53 ms on 10 regions: one
region's data is read by one node, so very wide queries lose the fan-out
they had (InfluxDB took 200 ms).

Before `ebd92e8`, `bootstrap_regions_per_node` defaulted to 2: every
new measurement started with two regions per node, even one of 30 rows, and
since regions spread across nodes by design never shared a node set, they
never merged back. Set it (or `SET bootstrap_regions_per_node = 2`) only
when a new measurement will take heavy writes at once; it spreads them from
the first point instead of after the first split.

## Scaling up

1. The new pods start and join in seconds.
2. The scheduler copies regions onto them in the background, a few at a
   time (`sharding.schedule_limit`, default 4; `SET schedule_limit = <n>`
   changes it live). A copy adds the new node as a
   learner, copies the data, then promotes it; reads never go to a learner.
3. "Settled" means no copy is in flight and every node holds regions. 3 → 6
   took 24 minutes with 91 million points stored and a writer running, and
   48 minutes with about 280 million; 6 → 8 did not settle within an hour.

**Copy speed is the main limit today**: about 2.2 MB/s per copy, ~4.5 MB/s
for the whole cluster with two copies running. The movement budget
(`sharding.movement_budget_bytes_per_sec`, default 50 MiB/s) is not what
limits it. `79a4eaf` makes the chunk export about 4x cheaper (0.24 s → 0.06 s
per 50,000 rows); the rest of the per-chunk cost is still being profiled.

Before `3934cf8` a copy could lose or never finish: it paged through a
region by series id alone, so a series with more than 50,000 rows (about 6
days at 10 s) had the rest of its rows skipped, and a chunk of a wide
measurement (Telegraf's `cpu`, 10 fields) was over the receiver's size limit
and refused (413) forever, leaving its new copies unpromoted. Copies now page
on `(series_id, time, ingest_seq)` and halve their chunks on a 413
(`hyperbytedb_shard_transfer_chunk_shrunk_total`).

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
   departing peer is removed. Its materialized view rollups are handed to a
   remaining node (see Materialized views). When it holds no region other
   than view destinations and its rollups are handed off, the node marks
   itself `leaving` (its `/health` reports it), and the operator deletes its
   pod and PVC.

Expect it to take as long as copying the departing nodes' data at the copy
speed above.

**Fixed in `8d7086e`:** removing a departing peer used to wait for one of the
4 mover slots that copies hold for minutes. On n3 an 8 → 3 scale-down added
live copies to 80 regions but dropped almost no departing peers, so no node
could leave. After the fix the departing nodes dropped about 63 peers in the
first minute.

6 → 3 (no restarts, `8d7086e`) finished in 35 minutes.

**Avoid scaling up while a scale-down is in progress.** On n3, raising
replicas from 3 to 6 mid-scale-down re-admitted two departing nodes (they
went `leaving` → `syncing` → `active`), but the node that was also the Raft
leader stayed departing inside a 6-replica cluster until the next scale-down
removed it. No data was lost; wait for the scale-down to finish first.

**A departing node can stay Raft leader** until its pod is deleted, which
then triggers an election. **Membership state can differ between nodes for a
while**: on n3 the leader showed a node as `leaving` while a follower's
`GET /cluster/nodes` still showed `decommissioning`. Ask the leader
(`GET /cluster/leader` names it) when the state matters.

Before `25803d7` a new measurement created during a scale-down could have
its regions placed on a departing node (see Materialized views).

Watch: `GET /cluster/nodes` (each node's state),
`hyperbytedb_shard_decommission_completed_total`, and the per-node peer
counts in `GET /internal/shard/map`.

## Auth

With `spec.auth.enabled` the operator keeps the cluster token and the admin
login in Secret `<cluster>-auth` and every node gets them as env (see
[Authentication](authentication.md)). On the n3 UAT cluster (6 nodes,
`b7aa4f4`, 5 Telegraf writers, 2026-10-11):

- Membership, Raft and replication run as without auth: 6 active voters, no
  restarts.
- A `telegraf` user with `GRANT WRITE` writes but cannot read or create
  users; a `grafana` user with `GRANT READ` reads but cannot write. Internal
  routes refuse a missing or wrong token (401); the token does not work on
  `/write`.
- Grants survive restarting all six pods at once. Before `b7aa4f4` they did
  not: a restarted node rebuilt every user without its grants, and Telegraf
  got 403 on every write.
- A non-admin running an admin statement (`CREATE USER`) gets HTTP 200 with
  `forbidden: admin privileges required` in the body; a refused read or
  write is a 403.
- While all pods restart together, writers see 503s, connection refusals and
  `write partially applied` 500s for about two minutes. Telegraf retries
  from its buffer; all 13 of its measurements were present afterwards (point
  counts across the restart were not checked).

- Changing `spec.version` (or the image) rolls the pods one at a time
  through the StatefulSet: 6 UAT pods took 13 minutes, about 2 minutes each
  (mostly the old pod draining). Telegraf logged 50 retried write errors over
  the whole rollout: writes need the region primary, and a draining primary
  takes none until its regions' primacy has moved.
- Reads keep working through a rollout from `23fe04b`. Before it, aggregates
  read only from the region primary, and a draining one counted as down:
  Grafana showed `no active peers for region 121` while the pods holding
  that region restarted. Now a draining primary still answers, and when the
  primary is gone a replica does (it can lag by the replication delay). The
  next rollout logged no such error.

## Runtime settings

`SET <setting> = <value>` changes a running cluster without a restart;
`SET <setting> = DEFAULT` puts each node back on its own config.
`SHOW SETTINGS` lists all 20 with their source (full table in the
[reference](reference.md#runtime-settings)). Uses that come up:

- Debug one problem: `SET log_level = 'hyperbytedb=debug,info'`, then
  `DEFAULT` when done.
- Hold placement during an upgrade or an incident:
  `SET add_peer_proposals_enabled = FALSE` (and `remove_...`). Copies in
  flight finish; no new ones start.
- More or fewer copies at once while scaling: `SET schedule_limit = <n>`.
  Raising it only helps if the disk has headroom (see Disks).
- Shed or admit write load: `SET max_inflight_writes = <n>` (per node; past
  it clients get 503 and retry), `SET rate_limit_per_second = <n>`
  (`0` turns it off).

A `SET` goes through Raft, so it needs a leader; it reaches every node at
once and survives restarts. On n3 (6 nodes, `d50523a`) each `SET` returned
in under a second and `SHOW SETTINGS` on every pod showed it 5 s later; a
restarted pod applied all 8 stored overrides at startup; `log_level` debug
lines appeared within 20 s and stopped after `DEFAULT`. No Raft election.

## Restarts and upgrades

- With `parallel-pods`, deleting every pod restarted 8 pods in about 105 s
  and 3 pods in 107-114 s. Writes fail while no coordinator is up.
- A rolling restart under `OrderedReady` takes about 16 s per pod when every
  pod is ready; during a scale-down it stalls (see the annotation above).
- `GET /cluster/leader` shows the Raft leader and term. A term that keeps
  rising means leadership is being lost (before `7c81d77`, every
  `CREATE MATERIALIZED VIEW` cost an election).

## Materialized views

- **Each row is rolled up on the node that first stored it**: the primary of
  its region at the time it was written. That node keeps the rollup in its
  own copy of the destination, even after the region moves elsewhere. A query
  on the destination asks every node that may hold rollups (active, draining,
  disconnected or decommissioning) and adds the results. If one of those nodes
  cannot answer, the query fails instead of returning a short count.
- `CREATE MATERIALIZED VIEW` goes through Raft and returns in about 30 ms.
  A node that has not seen the source measurement yet gets its schema with
  the statement. `hyperbytedb_schema_mutations_applied_total{op="create_materialized_view"}`
  should rise by one on every node; a node where an apply fails logs
  `schema mutation apply failed`.
- `WITH BACKFILL` rolls up the existing history once, run by the Raft leader
  after its own apply; a node that has not created the view yet is retried
  for up to 30 s (`b570f03` and later; before that every node backfilled,
  counting the history once per node). n3, 60,000 points: 60,000 rolled up
  on `8d7086e`, 90,900 on `792c94b`.
- Destination regions are never moved by the scheduler.
- **A departing node hands its rollups to a node that stays** before it
  leaves (`6dac99e`): the leader asks it to send its destination rows to the
  lowest-id active node, each range inserted once even if retried, then it
  empties its own tables. Logs: `view rollups handed off` on the leader,
  `materialized view rollups handed off` on the departing node. A node that
  cannot hand off within 30 minutes leaves without it (`view rollup handoff
  kept failing` at error level); its share of the rollups is then lost.
  Before `6dac99e` every scale-down lost the departing nodes' rollups: on n3
  a view held 44,700 of 60,000 points after 3 -> 6 -> 3. On `0082e44`, a view
  created and written at 6 nodes (through all 6 pods) still held exactly
  60,000 of 60,000 after scaling to 3; all three departing nodes handed off
  within a minute and the scale-down finished in 59 s.
- **Destination regions keep naming departed nodes.** They never move, so
  after a scale-down `GET /internal/shard/map` can show a destination region
  whose only peer is a node that has left. This has no effect on data or
  queries (destination queries ask every member node, not region peers).
- Rows that reach a destination any other way are dropped at flush with an
  error log and counted in
  `hyperbytedb_flush_rows_dropped_total{reason="materialized_destination"}`.
  This should stay at 0.

Verified on n3 (`6d354fb`): 60,000 points written through three pods rolled
up to exactly 60,000 (counts and means match per host per minute), both right
away and five minutes later.

## Dropping a database

`DROP DATABASE` on a 90-million-point database returned in 7 s on 3 nodes,
with peak memory up about 0.2 GiB per pod and no restarts (`8d7086e`).

## Pending measurements

These are being measured on the beta candidate and will be added here:

- Where the rest of a region copy's time goes.
