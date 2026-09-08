# Linear scaling for sharded HyperbyteDB

**Date:** 2026-09-07
**Status:** design approved, phase 1 not started
**Builds on:** PR #115 `feat/sharding-joiner-replica-place` (Phase 1 of first-class
sharding), and the validation plan in
`2026-09-07-sharding-test-plan-design.md`.

## Goal

A sharded cluster whose **single-measurement write throughput** and **query
throughput** grow linearly with node count, up to **~50 nodes**, with graceful
scale-down as a first-class operation.

Today neither holds. A measurement's write throughput is capped at RF nodes
regardless of cluster size, and there is no way to shrink a cluster at all.

## Why single-measurement throughput is capped today

`try_split` clones the parent's peer set:

```rust
let mut left = region.clone();
let mut right = region.clone();
```

`ShardRegion` carries `peers: Vec<u64>` and `primary: u64`, so a region's
replica set is already per-region — the data model is right. But because splits
clone, every descendant region of a measurement inherits the peer set its first
region bootstrapped onto. Splitting a hot measurement 128 ways spreads it across
128 regions and still exactly RF nodes.

`try_rebalance` cannot fix this: it only moves a primary among peers *already in
the region*. PR #115's placement rules can, but only one node at a time and only
for the most recently joined member — they were built to give a joiner some work,
not to distribute a measurement across a cluster.

Three further ceilings, confirmed in the code:

- **No `RemovePeer` op.** `ShardMapOp` has `BootstrapMeasurement`, `Split`,
  `Merge`, `MovePeer`, `AddPeer`, `TransferPrimary`, `ClearVerified`. A region's
  peer set can grow but never shrink. `NodeState::Draining` and `Leaving` exist
  in the enum but appear in `shard_scheduler.rs` only in test fixtures — no
  production path evacuates a draining node.
- **Every node is a Raft voter.** `raft_formation.rs` promotes all discovered
  learners with `change_membership(discovered_ids)`. At 50 nodes every shard-map
  commit needs 26 acks.
- **Nothing is capacity-driven.** All three placement rules key on recency
  (`max(joined_at)`, or the region's newest peer). An idle node stays idle unless
  it happens to be the newest.

## Decisions

| Question | Decision |
|---|---|
| Scaling axes | Single hot measurement write throughput; query throughput/concurrency |
| Target cluster size | ~50 nodes |
| Scale-down | In scope — graceful drain **and** permanent node loss |
| Compatibility | Free rein. Sharding is beta, off by default, pre-v1; map format and conventions may change |
| Placement | Deterministic rendezvous (HRW) hashing with node weights |
| Primary selection | Deterministic — top-scoring peer from the same hash |
| Split behaviour | Eager: children get computed peer sets and data moves immediately |
| Region granularity | Unchanged fixed series thresholds |
| Control plane | Fixed 5-voter Raft set, remaining nodes as learners |
| Movement governance | Global byte-rate budget with priority tiers |
| Cross-region percentiles | Exact two-pass, with a cardinality guard |
| Milestone 1 | Placement + drain/`RemovePeer` together |
| Acceptance | Single-measurement ingest vs node count at N = 3, 6, 12, 24, 48 |

## The placement function

For region *r* and node *n*:

```
score(r, n) = weight(n) / -ln(hash(r, n) / HASH_MAX)
```

Peers are the top *RF* nodes by score. **Primary is the top-scoring peer.** The
function is pure in (region key, live membership, node weights) — every
coordinator computing it agrees without coordination.

Rendezvous hashing is chosen for its movement property: adding or removing a
node moves only the regions that node wins or loses. There is no reshuffle, and
the moved fraction is provably minimal (~1/N on join, exactly the departing
node's regions on removal).

RF remains **a count, not a set** — `effective_replication_factor(configured,
active_members)` already caps it at live membership and stays as is.

### Region identity: key on `(measurement_key, region.start)`

Not `region_id`. `ShardMapOp::Split` allocates the right child's `region_id`
**at apply time**, deliberately:

```rust
// Allocate region_id at apply time so concurrent split proposals cannot
// collide on a stale proposer-chosen id.
let allocated = map.next_region_id;
right.region_id = allocated;
```

A proposer therefore cannot hash on an id that does not exist yet, and apply
has no view of live membership, so placement cannot be computed there either.

`region.start` is known at propose time for both children (`left.start` is the
parent's, `right.start` is `split_key`), unique within a measurement, and stable
across everything but a merge. Keyed this way, placement yields a property worth
stating on its own:

> **The left child's placement is identical to the parent's, by construction.**
> Same measurement, same start ⇒ same hash ⇒ same peers, same primary.

So an eager split moves **only the right child**. This is not a compromise on
eagerness — it is the same placement function returning a no-op for the left
child — and it halves the data movement of every split.

On merge the same property holds. `try_merge` builds the survivor as
`let mut merged = left.clone(); merged.end = right.end;`, so the merged region
keeps the left region's `start` and therefore its placement is unchanged. Only
the right region's rows move — which is already exactly what `try_merge` stages
today.

### Membership epoch

Placement depends on membership, so the map records the membership version its
placements were computed against. A placement diff is computed at a specific
epoch and rejected on apply if membership has moved on, reusing the existing
epoch-CAS discipline rather than inventing a second one.

## What this replaces in PR #115

`try_place_live_member`, `try_place_idle_member` and `try_place_primary` are
**subsumed and deleted**.

Their central difficulty was proving termination without a cooldown. That is why
primary placement keys on the region's *newest peer*, why `try_place_idle_member`
fires on `max(joined_at)` rather than on an actual join (review finding #8,
accepted as a beta wart), and why the PR argues convergence "by construction
rather than by cooldown."

With a pure target function the argument disappears. Convergence is *move toward
what the function says, stop when you match it*. There is no oscillation class to
defend against, and no recency bias to document.

**PR #115's infrastructure is the foundation and survives intact:**

- `ShardMapOp::AddPeer` and its duplicate/stale-epoch/transfer-debt guards
- Stage-then-commit ordering, and the reasoning for it — heal's
  commit-then-stage would publish a peer holding no rows, and scatter falls back
  to replicas, so it surfaces as silent empty results
- `ShardMapPort::replace_map` and shard-map catch-up on join
- The rollup-destination guard (`apply_transfer_push` is not idempotent, so a
  re-staged `SummingMergeTree` destination sums twice)
- Stall detection (`hyperbytedb_shard_placement_stalled_total`) and the
  per-peer-per-tick map-version probe cache

Only the *policy* is replaced. The mechanism is reused as built.

## Membership semantics

Two red-team rounds found the same class of defect twice: a state was
relabelled without following what the label controls. The root cause is that
`NodeState` is one enum answering **three independent questions**, and the code
tests `== Active` for all of them.

| | Can it serve traffic now? | Should it hold regions? | Must its data be evacuated? |
|---|---|---|---|
| Restart | no | **yes** | no |
| Decommission | no | no | **yes** |
| Brief network blip | no | **yes** | no |

No ordering of a single state satisfies that table, which is why one predicate
could never be right. The three questions get three named predicates, and every
consumer says which it means:

- **`is_serving`** — may take traffic. `Active` only. This is what
  `is_active_peer` already tests, and it is correct: writes aimed at a draining
  primary *should* fail fast until failover moves the primary.
- **`holds_placement`** — should be assigned regions and keep the ones it has.
  `Active | Draining | Disconnected`. A briefly unreachable node keeps its
  regions; that is what the dead-node timeout is for.
- **`is_departing`** — regions must be evacuated. `Decommissioning | Leaving`.

### Drain gives up the crown; decommission gives up the seat

| | Primary role | Peer membership | Data | Returns? |
|---|---|---|---|---|
| **Drain** (restart) | handed off — writes stop landing here | **kept** | **kept** | yes — resumes its regions |
| **Decommission** | handed off | **removed** | evacuated | no — removed from the cluster |

This is the fix for the defect both rounds found. `shard_handoff`
(`drain.rs:110-190`) currently does **both** — `TransferPrimary` at `:161` *and*
`MovePeer { from_peer: self.node_id }` at `:179`, with `drop_source: true` — so
every routine restart evicts the node from every region it primaries, before
placement has any say. Drain keeps only the `TransferPrimary` half. A restarting
node stops taking writes and keeps its seat, so there is nothing to re-stage
when it comes back.

Decommission does not reuse drain's single-pass, error-swallowing walk. It sets
the state and lets the convergence loop evacuate via `RemovePeer`, which is
RF-safe (stage before commit, `Add` before `Remove`) and retried.

### `NodeState::Decommissioning` is a state, not a flag

A new variant makes every `match` on `NodeState` a compile error, forcing a
visit to all 14 files that branch on it — the census that failing to do by hand
produced both rounds' defects. A boolean would let consumers be missed silently,
and would create a product space (`Draining + decommissioning`) needing rules of
its own.

There is **no backward-compatibility gate**. `NodeState` crosses the wire in
`JoinResponse` (`domain/cluster/sync.rs:58`) and an unknown variant fails the
whole join, but sharding is pre-v1, off by default, and single-operator. The
rollout constraint is simply: upgrade all nodes before decommissioning one.

**`write.rs:54` has a `_ => {}` catch-all, so the compiler will not flag it.**
Adding the variant is the moment to replace the remaining non-exhaustive
`NodeState` matches — `write.rs`, `ping.rs`, `log_store.rs`, `peer_handlers.rs`
— with explicit arms, so the exhaustiveness benefit is real rather than assumed.

### A returning drained node resyncs; nothing changes on the replicate path

Red-team round 2 argued that `handle_replicate_write`'s 503 while `Draining`
(`peer_handlers.rs:51`) makes a node return stale, because `Draining → Active`
is "a pure label flip with no catch-up". **That is wrong, and the plan was
changed on it before checking.** Two mechanics cover the gap:

- **`leader_monitor`** iterates `all_peers` — not `active_peers` — pulls each
  peer's `/internal/sync/manifest`, and fires `/internal/sync/trigger` whenever
  `needs_sync || wal_gap > 0 || region_lag`. It is continuous and
  state-independent, so a returning node is reconciled whether or not any
  transition was observed.
- **Hinted handoff** fires on any transition into `Active` from a non-`Active`
  state (`hinted_handoff.rs`: the variable is named `was_disconnected` but
  tests `!= Active`), replaying queued hints and triggering a sync.

Changing the 503 would also have been a **no-op**: `replication_peers` filters
to `Active | Disconnected`, so peers never replicate to a draining node in the
first place. Keeping one current would require adding `Draining` to
`replication_peers` too — deliberately not done. The node is shutting down, the
process-death-to-restart gap dwarfs the drain window, and admitting new inbound
writes during `wait_for_replication_acks` would fight the drain it is trying to
finish.

### One writer of `Leaving`

`drain()` sets `Leaving` (`drain.rs:102`) after a 90s-capped replication wait
(`:194`) — a timer, not a fact about evacuation. The scheduler's
`decommission_complete()` check is the only thing that may write `Leaving`.
Restart never reaches it; the shutdown path's skip-double-drain check keys on
"a drain already ran", not on `Leaving`.

### Legal transitions belong in the domain

`set_state` (`membership.rs:81`) has no guard — it logs and overwrites, so the
preStop hook's `/internal/drain` can put a decommissioning node back into
`Draining` mid-evacuation. A `transition()` method rejects illegal moves;
`Decommissioning` and `Leaving` are terminal and cannot be downgraded.

### Hysteresis

A demotion now costs data movement, so `Active → Disconnected` requires
`cluster.heartbeat_miss_threshold` misses rather than one
(`heartbeat.rs:64-70`). The knob already exists (default 5, `config.rs:829`,
surfaced in the CRD) and is consumed only by `flush_service.rs` today.

Two decisions the first hysteresis sketch got wrong:

- **The counter lives on `NodeInfo`**, beside `last_heartbeat` — not inside
  `probe_peers`, which is stateless and called fresh each tick
  (`heartbeat.rs:158`). A counter local to that function resets every tick,
  pinning the count at 1: hysteresis that compiles, passes its unit tests, and
  demotes on the first miss exactly as before.
- **A success decrements rather than resets.** Hard-reset-on-success never
  demotes a node alternating four misses and one success — unreachable 80% of
  the time yet permanently `Active`, which is worse than today for the
  degraded-but-not-dead case. Decrementing makes sustained flapping converge on
  demotion while a single blip still recovers.

`primary_failover_after_secs` starts from the first unreachable probe rather
than from the state transition, so detection and the failover timer overlap
instead of stacking. Otherwise the window grows from ~65s to ~85s, and every
write to the region stalls a full `scatter_peer_timeout_ms` before failing —
`resolve_region_peers` yields **zero** targets for a non-serving primary, with
no replica fallback by design (`shard_peer_resolution.rs:63-66`).

## Movement rules

| Event | Rule |
|---|---|
| **Split** | Left child stays put. Right child computes peers, stages, commits |
| **Merge** | Merged region keeps the left region's placement; right region's rows move |
| **Node joins** | The node enters top-RF for ~RF/N of regions. Each stages onto it, commits `AddPeer`, then sheds its lowest-ranked peer via `RemovePeer` |
| **Graceful drain** | Node weight → 0. Per region: stage onto the next-best node, **then** `RemovePeer` the draining node. RF never dips. The node leaves once no region references it |
| **Dead node** | `RemovePeer` immediately — there is nothing to preserve — then the existing heal path stages onto the replacement |

Drain and death differ deliberately. A planned decommission has a live source to
copy from, so restore-first costs only time. A node that is already gone has no
source, so preserving its membership buys nothing and delays RF restoration.

### `RemovePeer`

New `ShardMapOp` variant. Apply refuses when:

- the removal would drop the region below effective RF
- the epoch is stale
- the region carries unverified transfer debt
- the target is not currently a peer

Mirrors `AddPeer`'s guards. Like `AddPeer`, it is a new `ClusterRequest` variant
in the Raft log and carries the same rolling-upgrade hazard: a node on a build
that cannot deserialise it fails the whole append RPC. It gets the same
opt-out flag treatment.

### Movement budget

A cluster-wide byte-rate cap enforced on the leader, with priority tiers:

1. **RF violation** — regions below effective RF preempt everything
2. **Drain evacuation** — bounded, so a decommission finishes predictably
3. **Convergence** — pure balance moves use leftover budget only

Tier 1 exists because safety must not queue behind optimization. Tier 3 is
allowed to take as long as it takes.

## Phases

**Phase 1 — placement and membership (this milestone).**
Placement function; split rewiring; deterministic primary; `RemovePeer`; drain
evacuation; dead-node handling; movement budget and tiers; deletion of the three
recency rules.

**Phase 2 — control plane.**
Fixed 5-voter Raft set with the remaining nodes as learners, and voter
replacement when a voter dies. Shard-map commits cost 3 acks at any N. Learners
continue to receive the map by Raft replication.

**Phase 3 — query scaling.**
Exact two-pass `PERCENTILE`/`MEDIAN`/`MODE`/`SPREAD` across regions, replacing
today's hard `QueryParse` rejection. Bounded scatter fan-out —
`shard_metadata_scatter.rs` and `query_service.rs` currently use `try_join_all`
across all regions with no concurrency cap and fail-fast semantics, so one slow
region fails an entire metadata query.

The two-pass path is the only part of this design that degrades as regions
multiply: coordinator cost grows with cardinality × regions. It therefore ships
**with a cardinality guard that fails loudly**, naming the cap, rather than
risking coordinator OOM. If the guard trips often in practice, mergeable
sketches (t-digest/DDSketch) are the escape hatch — deliberately not chosen now,
because exact answers were the requirement.

## Acceptance

**Primary benchmark.** One measurement carrying **≥5M series**, fixed client
load, sustained points/sec measured at **N = 3, 6, 12, 24, 48**.

The target is **≥ 80% of ideal linear scaling at N = 48**, measured against the
N = 3 baseline (i.e. ≥ 12.8× throughput at 16× the nodes). Intermediate points
must be monotonic — no configuration may be slower than a smaller one. 80% is
the proposed bar; it is the one acceptance number chosen rather than derived, so
it is the first thing to revisit once real measurements exist.

**The cardinality floor is a documented operating requirement, not a defect.** A
measurement occupies at most as many nodes as it has regions. With
`region_split_series = 100_000` and `region_max_series = 150_000`, region count
is roughly series ÷ 125k, so saturating N nodes needs ~125k series per node. A
1M-series measurement tops out near 8 nodes however good placement is. The
benchmark is sized accordingly; the limit is written into user documentation.

**Supporting gates.**

- Distribution fairness: max/mean regions, primaries and bytes per node stay
  within a threshold as N grows
- Rebalance convergence: time from scale-up to steady state is bounded and
  measured
- Drain: RF never dips below effective RF at any point during a decommission
- Zero data loss or duplication across every scale-up, scale-down and split

## Risks and open questions

- **Region granularity limits the benchmark.** Accepted with fixed thresholds.
  `load_split_qps_threshold` already exists and defaults to 0; enabling it later
  is the cheapest route to spreading a hot, low-cardinality measurement, and is
  the first thing to revisit if the acceptance curve flattens early.
- **Eager splits move data under load.** Splits fire exactly when a measurement
  is growing. The movement budget bounds the cost, but split latency and
  foreground impact need measuring, not assuming.
- **Weights are unspecified.** The function takes node weights; how they are
  derived (capacity, manual, measured) is a Phase 1 decision left to the
  implementation plan. Uniform weights are a valid starting point.
- **Merge interacts with placement.** A merged region inherits the left
  region's placement. Repeated split/merge cycles could in principle drift a
  measurement's distribution; the function is stable, but the cycle deserves a
  test rather than an argument.
- **`RemovePeer` carries the `AddPeer` upgrade hazard** and needs the same
  rollout discipline.
- **Phase 1 deletes code PR #115 just added.** Sequencing matters: #115 should
  merge on its own terms first, so the placement work is a clean replacement
  against a known-good baseline rather than a rebase against an open PR.
