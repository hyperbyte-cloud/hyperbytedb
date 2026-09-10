# Sharding real-world test plan

Five gaps in real-cluster coverage, ordered by where the next bug is most
likely. Every bug this branch had was a **silent data-correctness bug at a
seam**, visible only under churn — so these add dimensions to churn rather
than new subsystems.

Existing coverage: `run-sharding-e2e.sh` (G0–G9 routing/scatter/split/raft),
`run-sharding-scaledown-e2e.sh` (D0–D7 decommission contract),
`run-sharding-lifecycle-e2e.sh` (L0–L10 query correctness across
down→kill→up).

## 1. Writes during topology change — `run-sharding-churn-e2e.sh`

Every test so far seeds, *then* moves. Real clusters write continuously.

A background writer appends known values throughout a scale-down. It records
every write that returned 204. The assertion is **exactly-once**: final count
equals seeded + acknowledged, no more and no less. `apply_transfer_push` is
not idempotent, so duplication is as plausible as loss.

Targets the windows nothing covers: a write arriving mid-transfer, a write
routed to a node that just became `Decommissioning`, and a write during a
primary handoff — when `resolve_region_peers` yields *zero* targets for a
non-serving primary, with no replica fallback by design.

**Gate:** `count == seeded + acked` exactly; zero 5xx that were counted.

## 2. Mixed-version rolling upgrade — manual runbook first

The largest documented hazard, with zero coverage. `AddPeer` and `RemovePeer`
are new `ClusterRequest` variants in the Raft log; a node on an older build
fails to deserialise the whole append RPC and stops replicating, so a leader
mid-rollout can lose control-plane quorum. This is why both ops ship behind
opt-out flags — and the flags have never been exercised.

Deploy the PR #115 base image at 6 nodes, roll to the new image pod-by-pod
watching `/internal/raft/metrics`, then repeat with the flags off and confirm
they actually prevent it. Manual first: the failure is a wedged control plane
and a human should watch it.

**Gate:** quorum never lost with flags off; hazard reproduces with them on
(if it does not, the flags' rationale is wrong and should be revisited).

## 3. Kill during evacuation — extend the churn script

We have only killed a pod while quiescent. Kill instead: the node *receiving*
a staged transfer; the Raft leader mid-evacuation; and the departing node
before it reaches `leaving` — which exercises the operator's "an unreachable
departing pod never gates" escape hatch, and whether it loses data.

**Gate:** count exact after each; RF restored; no region left orphaned.

## 4. Restart durability

`ShardMapPort::replace_map` bypasses the state machine and leaves
`last_applied` untouched, and Raft snapshot-carry is deferred to Phase 2. So:
does a node that restarts after receiving a region still hold the map *and*
the rows? Plan task U1, never built.

**Gate:** after restarting a node that recently received a region, its local
row count for that range is unchanged and the map survives.

## 5. Linearity benchmark — the phase's acceptance criterion

`scripts/sharding-linearity-bench.sh` exists and has never been run. The whole
justification for this work is throughput scaling with node count, currently
proven only at the pure-function level.

Needs ≥5M series to mean anything: a measurement occupies at most as many
nodes as it has regions, and at `region_split_series=100000` that is roughly
`series/125k`. Below ~125k series per node the curve flattens by arithmetic,
not defect — the script warns when `regions < nodes` for exactly this reason.

**Gate:** ≥80% of ideal linear scaling at the largest node count tested,
monotonic throughout.
