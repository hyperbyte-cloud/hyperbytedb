# Sharding validation test plan

**Date:** 2026-09-07
**Code under test:** hyperbytedb PR #115, branch `feat/sharding-joiner-replica-place`
(9 commits `f7b550a`..`425f80a`, +2570/−59)
**Companion:** hyperbytedb-operator PR #15 `feat/sharding-n1-cluster` — **merged**,
so the `replicas: 1` + `sharding.enabled` deployment path exists on operator `main`.

## Purpose

Establish that sharding works as designed on the PR #115 build: that the
subsystem's existing behaviour still holds, and that the joiner-placement
behaviour the PR introduces holds in a real deployment rather than only
in-process.

The plan drives three layers — in-process cargo tests, kind e2e gates, and a
short manual runbook — and defines a gate per claim. A gate is PASS, FAIL, or
an explicitly justified SKIP. Nothing is warn-downgraded.

## Relationship to the linear-scaling work

`2026-09-07-linear-scaling-design.md` supersedes PR #115's *placement policy* —
Phase 1 of that design deletes `try_place_live_member`, `try_place_idle_member`
and `try_place_primary` and replaces them with convergence toward a
deterministic target. It does **not** supersede #115's mechanism, and it does
not supersede this plan.

Most gates here survive unchanged and several matter more afterwards, because
Phase 1 adds a second Raft-log op (`RemovePeer`) carrying the same
rolling-upgrade hazard as `AddPeer`. Gates whose wording is tied to the
deleted rules are marked **[policy-dependent]** below: their intent survives,
their assertion needs restating once Phase 1 lands. They are not deleted here,
because #115 should merge on its own terms first and this plan is what
validates that merge.

## What PR #115 claims

Sharding partitions each measurement's `series_id` space into regions. Before
this PR a process joining an existing cluster never received any of them:
`try_split` clones the parent peer set and `try_rebalance` only moves a primary
among peers already in the region, so a measurement's peer set could not grow
to include a new node. A joiner added membership and HA, and no write capacity.

The PR is Phase 1 of first-class sharding:

- **P1.1** RF is `min(configured, live members)` — a 1-member cluster gets
  `peers = [self]` rather than an impossible peer set at configured RF 3.
- **P1.2** A joiner installs the committed shard map (`ShardMapPort::replace_map`)
  before any data moves, so staging cannot run against a lagging epoch.
- **P1.3** `ShardMapOp::AddPeer` grows a region's replica set;
  `try_place_live_member` stages a region onto a live member below effective RF,
  then commits.
- **P1.4** `try_place_primary` hands the primary to the region's newest peer
  while that peer holds strictly fewer primaries than the incumbent.
- **P1.5** Replica-only is not a pass — the joiner must apply writes locally.
- **P1.6** `try_place_idle_member` covers 3→4, where no region is under RF and
  `AddPeer` correctly declines; an existing replica steps aside via `MovePeer`.

All three placement steps stage before they commit. The heal path's
commit-then-stage ordering would publish a peer holding no rows, and scatter
reads fall back to replicas, so that would surface as silent empty results.

## What already proves what

PR #115's in-process coverage is strong and this plan does not re-litigate it.

**Pure placement policy** — 14 unit tests in `shard_scheduler.rs`, including
`primary_placement_does_not_oscillate`, `primary_placement_stops_at_an_even_split`,
`primary_placement_never_moves_off_the_newest_peer`, `idle_member_swap_converges`,
`idle_member_swap_skips_debt_and_balanced_regions`,
`live_replica_candidates_empty_when_rf_full_or_indebted`,
`effective_rf_never_exceeds_membership`, `joiner_map_catchup_blocks_a_lagging_joiner`.

**Multi-node integration** — five tests in `sharding_cluster_integration.rs`:
`one_member_cluster_owns_region_after_first_write` (P1.1),
`joiner_map_version_matches_before_region_movement` (P1.2),
`joiner_receives_existing_region_as_replica` (P1.3),
`joiner_takes_region_primary_and_applies_writes_locally` (P1.4/P1.5),
`fourth_node_joining_rf_complete_cluster_takes_a_region` (P1.6).

**Wiring and regressions** — `replace_map_installs_peer_snapshot`,
`bootstrap_n1_peers_are_self`, `placement_refuses_rollup_destinations`,
`tick_places_a_live_joiner_as_a_region_peer`,
`placement_skips_a_lagging_candidate_for_a_caught_up_one`,
`map_version_is_probed_once_per_peer_per_tick`,
`tick_withholds_add_peer_while_the_upgrade_gate_is_closed`.

**The pre-existing sharding surface** — `sharding_integration.rs` (config
validation, location cache, split/merge ops, region selection, peer
resolution), `sharding_cluster_integration.rs` (forwarded writes, scatter and
fallback, transfer, multi-region aggregates, limit/offset, percentile refusal,
sync quorum), `sharding_failover_integration.rs`, `sharding_mv_integration.rs`,
and the kind G0–G9 harness in `deploy/kind/run-sharding-e2e.sh`.

## The gaps this plan closes

| Gap | Why it matters |
|---|---|
| No real-deployment proof | The PR states it: "No kind/e2e run. G0–G9 does not cover this phase and the dedicated 1→2 gate has not been built; placement is proven by in-process tests only." |
| Mixed-version rolling upgrade | The PR's largest operational hazard. `AddPeer` is a new `ClusterRequest` variant in the Raft log; a node on an older build fails to deserialise the whole append RPC — axum rejects the body before the handler runs — so it stops replicating, and a leader mid-rollout can lose control-plane commit quorum. Documented, never tested. |
| Restart durability after placement | `replace_map` bypasses the state machine and leaves `last_applied` untouched, and Raft snapshot-install map carry is deferred to Phase 2. The boundary is asserted nowhere. |
| Placement under live write load | Every existing integration test moves a primary against a quiescent cluster. |
| Sharding-off regression | Sharding ships off by default; nothing proves this build leaves the non-sharded paths alone. |
| Corruption shape of the rollup guard | `placement_refuses_rollup_destinations` proves the guard branch fires. Nothing proves the double-count it exists to prevent — `apply_transfer_push` is not idempotent, so a re-staged `SummingMergeTree` destination sums twice, permanently and silently. |
| Convergence of the wired tick | The policy functions are proven to terminate as pure functions. Nothing runs the real scheduler past convergence to confirm it stops proposing. |
| The rolling-upgrade opt-out itself | `add_peer_proposals_enabled = false` is the documented procedure for a rollout. Only the unit-level withholding is tested; no test joins a node under the closed gate and confirms the cluster stays healthy. |

## Layer 0 — Baseline

Establish the build and the starting numbers. Record actual output; do not
carry the PR body's figures forward.

**Executed 2026-09-07 against PR HEAD `425f80a`** in a clean worktree. All
gates PASS. The PR body's figures reproduce exactly — 521 lib, 288 across 11
suites, 38 CLI — so its testing claims are accurate, not stale.

| Gate | Assertion | Result |
|---|---|---|
| B0 | `feat/sharding-joiner-replica-place` checked out; `chdb-rust` sibling present per `scripts/checkout-chdb-rust.sh`; `libchdb.so` on the system | **PASS** — worktree clean at `425f80a` |
| B1 | `cargo fmt --check` clean (per package — `--all` trips on the sibling `chdb-rust` worktree, which is not a workspace member) | **PASS** |
| B2 | `cargo clippy --all-targets -- -D warnings` clean | **PASS** |
| B3 | `cargo test --lib` — record pass count, 0 failed | **PASS** — 561 / 0 failed (hyperbytedb 521, cli 38, proxy 2) |
| B4 | `cargo test --test '*'` — record pass count and suite count, 0 failed | **PASS** — 288 / 0 failed / 0 ignored, 11 suites |
| B5 | `cargo test -p hyperbytedb-cli` — record pass count, 0 failed | **PASS** — 47 / 0 failed |
| B6 | `hyperbytedb:local` image built from the branch (`docker build -f hyperbytedb/Dockerfile <parent-dir>`; context is the parent directory because of the `chdb-rust` path dependency) | not run |

Two observations from the run, neither a failure:

- `joiner_receives_existing_region_as_replica` tripped cargo's "running for over
  60 seconds" warning. It passed, but it is the slowest test in the suite and it
  is the one gating the staging-before-commit contract — the most timing-
  sensitive assertion in the PR. A slower CI runner could make it flaky.
- All five placement tests ran by name and none carries `#[ignore]`; there are
  no `#[ignore]` attributes anywhere in `hyperbytedb/tests/` or `src/`. The doc
  comment at `tests/compat/http_tests.rs:8` claiming otherwise is stale.

## Layer 1 — In-process (cargo)

Five new tests in `hyperbytedb/tests/`, reusing the helpers in
`tests/common/sharding_cluster.rs` (`start_sharded_joiner`,
`install_shard_map_from_peer`, `apply_add_peer_on_nodes`,
`apply_move_peer_on_nodes`, `start_sharded_single_node`,
`promote_region_primary_on_nodes`, `local_wal_points`).

**U1 — restart durability.** Place a region onto a joiner and move the primary
to it, then restart the joiner process. Assert: the shard map survives, the
region's rows are still local, the joiner is still primary, and a subsequent
write applies locally. This pins the Phase 1/Phase 2 boundary — if the map is
reconstructed rather than persisted, this is where it shows.

**U2 — placement under write load.** Drive concurrent writers at both nodes
across a primary move. Assert: zero write errors, exact final point count, no
duplicated values. The existing P1.4 test moves a primary against a silent
cluster; this asserts the move is not a window.

**U3 — convergence.** After 1→2 has converged, run further scheduler ticks.
Assert: no additional `AddPeer` or `MovePeer` committed, region epoch stable,
`hyperbytedb_shard_placement_stalled_total` == 0. The unit tests prove the
policy function terminates; this proves the wired tick does.

**U4 — rollup corruption shape.** Create a sharded `SummingMergeTree`
materialized view, then run a placement tick. Assert destination sums are
exact. Asserts the guard's consequence rather than its branch.

**U5 — gate-off opt-out.** With `add_peer_proposals_enabled = false`: a joiner
joins, owns no region, and the cluster stays healthy and writable. This is the
documented rolling-upgrade procedure; it has to actually work.

## Layer 2 — kind e2e

### 2a. Helper refactor (prerequisite)

`deploy/kind/run-sharding-e2e.sh` is 1361 lines and hard-assumes a fixed 6-node
RF=2 cluster from entry — `wait_for_full_cluster` blocks on 6/6. The scale-up
scenarios need a different cluster lifecycle, so they get a sibling script
rather than extra phases.

Lift the reusable helpers into `deploy/kind/lib/e2e-common.sh`, sourced by both
scripts: `log`/`warn`/`err`, `record`/`pass`/`fail`/`skip`, `write_report`,
`kubectl_ctx`, `api_url`, `api_port`, `setup_port_forwards`,
`teardown_port_forwards`, `curl_api`, `curl_write`, `curl_write_retry`,
`write_ts`, `query_post`, `count_from_query`, `count_from_query_checked`,
`query_scalar`, `query_series_group_count`, `query_series_value_count`,
`wait_for_count`, `wait_flush_boundary`, `metric_max_across_pods`,
`metric_value`, `metric_counter`, `wait_for_cluster_ready`, `wait_for_pod_ping`,
`region_count`, `region_count_for_db`, `scale_map_summary`,
`bootstrap_region_id`, `shard_map_next_region_id`, `max_region_id`,
`stale_epoch_failures`, `stale_epoch_logs`, `wait_for_raft_leader`,
`region_primary`, `wait_for_primary_failover`.

The phase-window machinery (`phase_enabled`, `run_phase_if_needed`,
`run_prerequisites_for`) and `wait_for_full_cluster` stay with the G0–G9 script;
the scale-up script needs its own membership wait.

**Refactor gate R0:** G0–G9 produces the same gate IDs and results after the
lift as before it. If you would rather not touch the working script, the
fallback is a private helper copy in the new script — noted as a deliberate
duplication, not an oversight.

### 2b. Regression run

Run the existing G0–G9 against the PR-built `hyperbytedb:local` image on a
6-node sharded CR (`deploy/kind/setup.sh hdb-reset --sharded`). This is a
baseline, not new coverage.

**Known pre-existing issue, out of scope for this plan:** the last recorded run
had G7 (region primary failover) time out — `Primary failover not confirmed
within 75s (map primary=2)` — and the script downgraded it to a warning, so the
run still reported PASS. This plan neither fixes nor re-scopes that; it records
whether the PR build reproduces it. The warn-downgrade is a property of the
existing script and is left alone; the new script does not inherit it.

### 2c. Scale-up gates

New `deploy/kind/run-sharding-scaleup-e2e.sh`, with two new CRs:
`hyperbytedb-cr-1node-sharded.yaml` (RF=2) and
`hyperbytedb-cr-3node-sharded.yaml` (RF=3). RF=3 is required for S7: at 3 nodes
and RF=3 no region is under RF, which is exactly the case `try_place_idle_member`
exists to cover.

| Gate | Assertion |
|---|---|
| S0 | 1-replica sharded CR admitted by the operator; pod Running/Ready, CR Healthy, `[sharding] enabled = true` in the ConfigMap |
| S1 | First write bootstraps a region with `peers = [1]`, `primary = 1` — effective RF = min(2, 1) = 1 (P1.1) |
| S2 | Scale 1→2; joiner reaches Active and its `map_version` is equal to or ahead of the leader's **before** any region movement (P1.2) |
| S3 | Joiner is a committed peer via `AddPeer` **and holds the rows** — a query served from the joiner alone returns the full region, not empty (P1.3). The differential matters: staging before committing is the whole contract |
| S4 | Primary moves onto the joiner; a write aimed at it lands in its own WAL rather than being forwarded (P1.4/P1.5) |
| S5 | Exact point count and per-series value checksum unchanged across the entire 1→2 sequence — no loss, no duplication |
| S6 | Convergence: further ticks commit no placement ops; `hyperbytedb_shard_placement_stalled_total` == 0; no `stale_epoch` heartbeat failures |
| S7 | **[policy-dependent]** 3→4 on the RF=3 CR: the 4th node ends with at least one region membership, and no region drops below effective RF (P1.6). Phase 1 deletes `try_place_idle_member`; the assertion then becomes "the 4th node holds a share of the measurement's regions", since hashing guarantees a share rather than any specific region |
| S8 | `add_peer_proposals_enabled = false` → scale up → no `AddPeer` committed, joiner owns nothing, cluster healthy and writable. The rolling-upgrade opt-out works. Extends to `remove_peer_proposals_enabled` after Phase 1 |
| S9 | A sharded materialized view present across a scale-up: rollup destination sums exact, no double-count |
| S10 | Sustained write load across the scale-up: zero write errors, exact final count |
| S11 | Sharding-off control CR on the same image passes the existing non-sharded smoke |

Timing follows the existing script's convention — waits derived from
`heartbeatIntervalSecs` and `splitMergeIntervalSecs` rather than hardcoded, so
the gates stay honest if the CR changes.

## Layer 3 — Manual runbook

**M1 — mixed-version rolling upgrade.** The one scenario that should not be
automated first, because the failure mode is a wedged control plane and a human
should watch it. Deploy the `main` image at 3 nodes with sharding on; set
`add_peer_proposals_enabled = false`; roll pods to the PR image one at a time,
watching Raft commit health and `/internal/raft/metrics` at each step; once every
node is upgraded, re-enable the flag and confirm placement resumes. Then repeat
*without* the flag set to off, and confirm the documented hazard reproduces —
if it does not, the flag's rationale needs revisiting.

This runbook is **reused verbatim for `RemovePeer`** when linear-scaling Phase 1
ships. It is the reason both ops carry an opt-out flag defaulting on, and the
reason this plan outlives PR #115.

**M2 — observability.** Watch the Grafana `hyperbytedb-cluster` dashboard across
a scale-up. Confirm region counts, primary distribution, transfer bytes and
stage counters move as the gates claim, and that a reader could diagnose a
stalled placement from the dashboard alone.

**M3 — operator UX.** A CR with `replicas: 1` and `sharding.enabled: true` is
accepted (operator PR #15 dropped the webhook rule forbidding it); the rejection
path for genuinely invalid sharding config still rejects.

## Execution order

1. Layer 0 baseline (B0–B6)
2. Layer 1 in-process (U1–U5)
3. Helper refactor + R0
4. G0–G9 regression on the PR image
5. Scale-up gates S0–S11
6. Manual runbook M1–M3

Layers 0 and 1 gate the rest: if the branch does not build clean and pass its
own suites, the e2e work is wasted.

## Environment notes

- A kind cluster named `hyperbytedb` is up with 6 workers and currently holds
  the 6-node sharded CR. The scale-up gates need a reset to a 1-node CR, so the
  regression run and the scale-up run **cannot share one cluster state** —
  sequence them, resetting between.
- `deploy/kind/setup.sh hdb-reset` must wipe hostPath contents, not just PVC
  objects; the scale-up CRs need the same treatment adding.
- The e2e scripts run in-cluster as a Job by default (no port-forwards) and
  `--local` for port-forwarded runs from a workstation. New gates should work in
  both modes.

## Exit criteria

- Every gate in Layers 0–2 is PASS, or a SKIP with a recorded justification.
- No gate in the new script is warn-downgraded; failures use `fail`, not `warn`.
- M1 completed with an explicit written finding on whether the mixed-version
  hazard reproduces as documented.
- The Layer 0 numbers, the G0–G9 result, and the scale-up report are recorded
  together so the build under test is identifiable later.

## Explicitly out of scope

- **G7's primary-failover timeout** — pre-existing, recorded only.
- **Performance and chaos.** No `scripts/sharding-perf-bench.sh` run, no
  deliberate fault injection during staging beyond what S10 exercises
  incidentally.
- **Phase 2 work** — Raft snapshot-install map carry, and whether the catch-up
  gate should compare the Raft applied index instead of `map_version`. U1 pins
  the current boundary; it does not extend it.
- **Hexagonal cleanup** — raw peer HTTP in the application layer. Tracked
  separately.
- **Defaults** — server and operator defaults stay off; this plan tests the
  opt-in path, not a change of default.
- **Linear-scaling acceptance.** Whether throughput grows with node count is
  measured by `scripts/sharding-linearity-bench.sh` under
  `2026-09-07-linear-scaling-phase-1.md`, not here. This plan asks whether
  sharding is *correct* on the #115 build; that one asks whether it is *fast
  enough as it grows*. Keeping them separate matters — every gate here can pass
  on a cluster whose throughput is flat, which is exactly the situation that
  prompted the linear-scaling work.
- **Hexagonal cleanup** is listed above as tracked separately. Note that
  linear-scaling Phase 1 moves placement policy into
  `domain/sharding/placement.rs` as pure logic, which resolves part of it as a
  side effect rather than as a goal.

## Risks

- **The helper refactor touches a working 1361-line script.** R0 exists for
  exactly this. The fallback is a private helper copy in the new script.
- **`try_place_idle_member` triggers on greatest `joined_at`, not on a recent
  join**, so a stable-but-unbalanced cluster rebalances on the first tick after
  upgrade. Accepted while sharding is beta; S7 should record the behaviour it
  observes rather than assume a join triggered it.
- **Scale-up gates are timing-sensitive.** Derive every wait from the CR's
  configured intervals, and on timeout record the observed map state as
  evidence rather than only the failure.
