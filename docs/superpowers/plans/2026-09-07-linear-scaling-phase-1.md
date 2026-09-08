# Linear Scaling Phase 1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make a region's peer set a pure function of its identity and live cluster membership, so a single measurement's write throughput grows with node count instead of being capped at RF, and so a node can be drained or lost with regions moving correctly.

**Architecture:** Rendezvous (HRW) hashing over `(MeasurementKey, region.start)` produces the target peer set and primary for every region. The scheduler stops *inventing* placements and instead executes the difference between where a region is and where the function says it belongs, reusing PR #115's stage-then-commit machinery. `try_split` computes the right child's placement instead of cloning the parent's. A new `RemovePeer` op lets peer sets shrink, which is what makes drain and rebalancing possible at all.

**Tech Stack:** Rust (edition 2024, toolchain 1.94), OpenRaft 0.9, RocksDB, chDB, axum, `thiserror`, `metrics`. Tests are in-process Axum, no TCP.

**Spec:** `docs/superpowers/specs/2026-09-07-linear-scaling-design.md`

## Global Constraints

- Toolchain pinned to **1.94** in `rust-toolchain.toml`. Do not change it.
- `cargo clippy --all-targets -- -D warnings` and `cargo fmt --check` must pass; the pre-commit hook runs both and is slow on a cold worktree (several minutes). Budget for it.
- `#![cfg_attr(not(test), warn(clippy::unwrap_used, clippy::expect_used))]` is set in `main.rs`. Non-test code must not `unwrap()` or `expect()`.
- **Never hold a `Mutex`/`RwLock` across `.await`.** The scheduler drops guards explicitly; follow that.
- Errors use `thiserror` via `HyperbytedbError`.
- `chdb-rust` must exist as a sibling: `bash scripts/checkout-chdb-rust.sh`.
- **The placement hash must be stable forever.** `DefaultHasher` is explicitly not stable across Rust releases and every node recomputes placement continuously. Task 1 freezes a mixer; nothing may substitute `DefaultHasher`.
- New `ShardMapOp` variants are serialised into the Raft log. A node that cannot deserialise a variant rejects the whole append RPC and stops replicating. Every new op ships behind an opt-out flag defaulting **on**, matching `add_peer_proposals_enabled`.

## Existing names this plan builds on

Verified against `feat/sharding-joiner-replica-place` @ `425f80a`. Use these exactly.

| Name | Location | Shape |
|---|---|---|
| `self.propose(op)` | `shard_scheduler.rs:1580` | `async fn propose(&self, op: ShardMapOp) -> Result<(), HyperbytedbError>` |
| `self.is_rollup_dest(key)` | `shard_scheduler.rs:239` | `async fn is_rollup_dest(&self, key: &MeasurementKey) -> bool` |
| `self.cached_peer_map_version(id, addr, client)` | `shard_scheduler.rs:195` | `-> Result<u64, HyperbytedbError>` |
| `self.note_placement_stall(region_id, candidates)` / `self.clear_placement_stall(region_id)` | `shard_scheduler.rs` | `async fn` |
| `joiner_map_caught_up(cluster_ver, joiner_ver)` | `shard_scheduler.rs:1619` | `pub fn -> bool` |
| `current_region(&map, key, region_id)` | `shard_scheduler.rs:1688` | `pub fn -> Option<&ShardRegion>` |
| `request_region_stage(pc, &membership, from_primary, key, region, to_peer)` | `shard_scheduler.rs:1889` | `async fn` |
| `stage_region_transfer_data(pc, &metadata, &wal, query_port, node_id, key, region, to_peer, max_points)` | `shard_transfer.rs:761` | `async fn -> Result<RegionTransferOutcome, HyperbytedbError>` |
| `sample_region_peers(peers, primary)` | `shard_scheduler.rs:2568` (test mod) | `fn -> ShardRegion` |
| `RegionTransferOutcome` | `shard_transfer.rs:27` | fields `transfer_id`, `exported`, `applied`; method `verified()` |
| `PlacementTestHarness::new(add_peer_proposals_enabled, peers, regions)` | `shard_scheduler.rs:3374` (test mod) | `peers` is `&[(node_id, map_version_delta)]`; `regions` is how many measurements to bootstrap, each one region owned by this node. Fields: `scheduler`, `proposals`, `probes`. Only extra method is `probe_count(peer)` |
| `ReconcileTestHarness::new()` | `shard_scheduler.rs:3351` (test mod) | Fields: `shard_map`, `scheduler`, `proposals` |
| `self.seed_rollup_dest(key)` | `shard_scheduler.rs:247` | `#[cfg(test)] async fn` — already exists, do not add a new one |
| `self.scheduler.tick_once_for_test()` | `shard_scheduler.rs:252` | `async fn -> Result<(), HyperbytedbError>` |
| `idle_member_replica_swap(...)` | `shard_scheduler.rs` | free fn used by `try_place_idle_member` |

## Ground-truth pass — 2026-09-08, after Wave 1

Re-checked against `feat/linear-scaling-p1` (Wave 1 landed). Findings, each
spot-checked against source:

**Line numbers in this plan are indicative, not authoritative — grep the
symbol.** Wave 1 shifted `shard_scheduler.rs` by **+39** after ~line 1050 (the
two extracted methods), `ops.rs` by **+14**, `write.rs` by **+1**. They will
shift again with every task. Symbols are stable; line numbers rot.

**`PlacementTestHarness::new` bootstraps `MeasurementKey::new("db", "autogen",
format!("cpu{i}"))`** — so a single-region harness uses measurement **`cpu0`**,
not `m0`. Every harness test in T10/T13/T15/T19 must use that. The harness
exposes only `scheduler`, `proposals`, `probes` and `probe_count`; a `region()`
/ `state_of()` accessor has to be added.

**`try_split` already stages pre-commit** (`stage_region_transfer_data`, guarded
on `region.primary == self.node_id && right.primary != self.node_id`), and sets
`right.primary` from `pick_best_primary` just before it. T9's assignment must
replace that primary selection and sit *before* the staging call, or staging
targets the old primary.

**`drain.rs` passes `drop_source: true`** into `run_region_transfer`, commented
"This node is draining away; its local copy must go." So drain deletes the local
data as well as evicting via `MovePeer`. T19 must set this `false` on the
restart path — keeping the seat while dropping the data is worse than either.

**`handle_leave` (`peer_handlers.rs`) uses `set_state`**, and its self-leave arm
sets `Draining` — the same clobber hazard as `/internal/drain`, on a path this
plan never mentioned. It must use `transition()`.

**Fourteen production `set_state` callers exist.** The rest are safe:
`heartbeat.rs` and `ping.rs` gate on allow-lists (`Active | Syncing`,
`Syncing | Joining`) that cannot match `Decommissioning`; `bootstrap.rs` and
`sync_client.rs` set only the node's *own* state, which peers override via
`decide_transition` returning `None` for `Decommissioning`. Recorded rather than
changed.

**T11's delete list was incomplete.** It also needs the free functions
`primary_placement_candidate` (called by `try_place_primary`),
`region_memberships` (called by `try_place_idle_member`), and
`live_replica_candidate` (singular — **already has no production caller**, only
tests), plus the test `live_replica_candidate_none_with_transfer_debt`.

## Dependency graph and parallel dispatch

```
T17 (new state) ─┬─► T18 (hysteresis)
                 ├─► T19 (/internal/decommission)
                 └─► T8 ─► T9
T1 ─► T2 ────────┴─► T10 ─► T11 ─┬─► T14
T3 ──────────────────┤           ├─► T15
T4 ──────────────────┤           └─► T13
T5 ──────────────────┘   T12 ────┘
T6 ─► T10      T7 ─► T10      T16 (independent)
```

**Dispatch in waves.** Wave 1: **T1, T3, T4, T5, T6, T7, T16, T17** — eight
tasks, no shared state, safe in parallel. Wave 2: **T2, T12, T18, T19**.
Wave 3: **T8, T10**. Wave 4: **T9, T11**. Wave 5: **T13, T14, T15**.

**T17 gates the membership work** (T8, T18, T19, T14) and is no longer small:
adding the variant makes every exhaustive `NodeState` match a compile error
across the 14 files that branch on it — which is the point, since that census is
what two red-team rounds found me failing to do by hand.

T11 is the only high-blast-radius task (it deletes 17 tests). Give it a dedicated review.

## File Structure

**Create:**
- `hyperbytedb/src/domain/sharding/placement.rs` — frozen hash, HRW placement, and the pure step diff. No I/O, no async, no config. The whole policy, isolated so it is exhaustively testable as pure logic.
- `hyperbytedb/tests/sharding_placement_integration.rs` — multi-node behaviour.
- `scripts/sharding-linearity-bench.sh` — acceptance benchmark.

**Modify:** `domain/sharding/mod.rs`, `domain/sharding/ops.rs`, `application/shard_routing.rs`, `application/shard_scheduler.rs`, `config.rs`.

---

## Wave 1

### Task 1: The frozen placement hash

**Files:** Create `hyperbytedb/src/domain/sharding/placement.rs`; modify `hyperbytedb/src/domain/sharding/mod.rs`

**Interfaces:**
- Consumes: `MeasurementKey` from `super::types`.
- Produces: `pub fn placement_hash(key: &MeasurementKey, start: u64, node_id: u64) -> u64`

- [ ] **Step 1: Write the failing test**

Create `hyperbytedb/src/domain/sharding/placement.rs` containing only the test module:

```rust
//! Deterministic region placement.

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::sharding::types::MeasurementKey;

    fn key() -> MeasurementKey {
        MeasurementKey::new("db", "autogen", "cpu")
    }

    #[test]
    fn hash_is_stable_and_sensitive_to_every_input() {
        let a = placement_hash(&key(), 0, 1);
        assert_eq!(a, placement_hash(&key(), 0, 1), "not deterministic");
        assert_ne!(a, placement_hash(&key(), 0, 2), "node_id ignored");
        assert_ne!(a, placement_hash(&key(), 1, 1), "start ignored");
        assert_ne!(
            a,
            placement_hash(&MeasurementKey::new("db", "autogen", "mem"), 0, 1),
            "measurement ignored"
        );
        assert_ne!(
            a,
            placement_hash(&MeasurementKey::new("db2", "autogen", "cpu"), 0, 1),
            "db ignored"
        );
    }

    #[test]
    fn hash_avalanches_across_node_ids() {
        // Poor mixing shows up here first and would silently skew placement.
        let mut seen = std::collections::HashSet::new();
        for n in 1..=64u64 {
            seen.insert(placement_hash(&key(), 0, n) >> 56);
        }
        assert!(seen.len() > 40, "top byte collides too often: {}", seen.len());
    }
}
```

Add `pub mod placement;` to `hyperbytedb/src/domain/sharding/mod.rs`.

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test --lib placement::`
Expected: FAIL — `cannot find function placement_hash in this scope`.

- [ ] **Step 3: Implement**

Insert above the `#[cfg(test)]` block:

```rust
use super::types::MeasurementKey;

/// SplitMix64 mixing constants. **Frozen — never change these.**
///
/// Placement must agree across every node, including nodes on different builds
/// mid-rollout. `std::collections::hash_map::DefaultHasher` is explicitly not
/// stable across Rust releases and must never be used here.
const MIX_1: u64 = 0xbf58_476d_1ce4_e5b9;
const MIX_2: u64 = 0x94d0_49bb_1331_11eb;
const GOLDEN: u64 = 0x9e37_79b9_7f4a_7c15;

fn mix(mut z: u64) -> u64 {
    z = (z ^ (z >> 30)).wrapping_mul(MIX_1);
    z = (z ^ (z >> 27)).wrapping_mul(MIX_2);
    z ^ (z >> 31)
}

fn mix_bytes(seed: u64, bytes: &[u8]) -> u64 {
    let mut acc = seed;
    for &b in bytes {
        acc = mix(acc.wrapping_add(u64::from(b)).wrapping_add(GOLDEN));
    }
    acc
}

/// Stable 64-bit score for placing region `(key, start)` on `node_id`.
pub fn placement_hash(key: &MeasurementKey, start: u64, node_id: u64) -> u64 {
    let mut acc = mix_bytes(GOLDEN, key.db.as_bytes());
    acc = mix_bytes(acc, key.rp.as_bytes());
    acc = mix_bytes(acc, key.measurement.as_bytes());
    acc = mix(acc.wrapping_add(start).wrapping_add(GOLDEN));
    mix(acc.wrapping_add(node_id).wrapping_add(GOLDEN))
}
```

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test --lib placement::`
Expected: PASS, 2 tests. If `hash_avalanches_across_node_ids` fails, fix the mixer — do not weaken the assertion.

- [ ] **Step 5: Commit**

```bash
cargo fmt -p hyperbytedb && cargo clippy --all-targets -- -D warnings
git add hyperbytedb/src/domain/sharding/placement.rs hyperbytedb/src/domain/sharding/mod.rs
git commit -m "Add a frozen hash for region placement"
```

---

### Task 3: The placement step diff

**Files:** Modify `hyperbytedb/src/domain/sharding/placement.rs`

Independent of Tasks 1/2 — it takes a target slice, not a hash.

**Interfaces:**
- Consumes: `ShardRegion` from `super::types`.
- Produces: `pub enum PlacementStep { Add(u64), Remove(u64), Promote(u64) }`, `pub fn next_placement_step(region: &ShardRegion, target: &[u64]) -> Option<PlacementStep>`

- [ ] **Step 1: Write the failing tests**

Add to the `mod tests` block in `placement.rs`:

```rust
    use crate::domain::sharding::types::ShardRegion;

    fn region(peers: Vec<u64>, primary: u64) -> ShardRegion {
        ShardRegion {
            region_id: 1,
            start: 0,
            end: u64::MAX,
            epoch: Default::default(),
            peers,
            primary,
            last_split_at: 0,
            transfer_verified: None,
            transfer_first_seen: None,
        }
    }

    #[test]
    fn converged_region_needs_no_step() {
        assert_eq!(next_placement_step(&region(vec![3, 1, 2], 3), &[3, 1, 2]), None);
    }

    #[test]
    fn missing_peer_is_added_before_anything_else() {
        assert_eq!(
            next_placement_step(&region(vec![1, 2], 1), &[3, 1, 2]),
            Some(PlacementStep::Add(3))
        );
    }

    #[test]
    fn primary_is_promoted_once_peers_are_present() {
        assert_eq!(
            next_placement_step(&region(vec![1, 2, 3], 1), &[3, 1, 2]),
            Some(PlacementStep::Promote(3))
        );
    }

    #[test]
    fn extra_peer_is_removed_last() {
        assert_eq!(
            next_placement_step(&region(vec![1, 2, 3, 4], 3), &[3, 1, 2]),
            Some(PlacementStep::Remove(4))
        );
    }

    #[test]
    fn add_precedes_remove_so_rf_never_dips() {
        assert_eq!(
            next_placement_step(&region(vec![1, 2, 3], 3), &[3, 2, 4]),
            Some(PlacementStep::Add(4))
        );
    }

    #[test]
    fn primary_is_never_the_node_being_removed() {
        assert_eq!(
            next_placement_step(&region(vec![1, 2, 3], 1), &[2, 3]),
            Some(PlacementStep::Promote(2))
        );
    }

    #[test]
    fn empty_target_is_a_no_op() {
        assert_eq!(next_placement_step(&region(vec![1, 2, 3], 1), &[]), None);
    }

    #[test]
    fn repeated_steps_converge_to_a_fixed_point() {
        // This is the whole convergence argument that replaces PR #115's
        // recency keying. If it loops, the design is wrong, not the test.
        let mut r = region(vec![1, 2, 3], 1);
        let target = vec![5, 4, 2];
        for _ in 0..16 {
            match next_placement_step(&r, &target) {
                Some(PlacementStep::Add(n)) => r.peers.push(n),
                Some(PlacementStep::Remove(n)) => r.peers.retain(|p| *p != n),
                Some(PlacementStep::Promote(n)) => r.primary = n,
                None => break,
            }
        }
        assert_eq!(next_placement_step(&r, &target), None, "did not converge");
        assert_eq!(r.primary, 5);
        let mut peers = r.peers.clone();
        peers.sort_unstable();
        assert_eq!(peers, vec![2, 4, 5]);
    }
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test --lib placement::`
Expected: FAIL — `cannot find type PlacementStep`.

- [ ] **Step 3: Implement**

Add above the test module:

```rust
use super::types::ShardRegion;

/// One mutation moving a region toward its target placement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlacementStep {
    /// Stage data onto this node, then commit `AddPeer`.
    Add(u64),
    /// Commit `RemovePeer` for this node.
    Remove(u64),
    /// Commit `TransferPrimary` to this node.
    Promote(u64),
}

/// The next step toward `target`, or `None` at the fixed point.
///
/// Callers must act on the returned step; ignoring it stalls convergence.
///
/// Priority is deliberate and load-bearing:
///
/// 1. **Add** before remove, so replication factor never dips while
///    converging. This is what gives drain correct behaviour for free.
/// 2. **Promote** before remove, so the primary is never the node leaving.
/// 3. **Remove** last, only once the target set is fully present.
///
/// One step per call: a committed step bumps the region epoch, so the caller
/// must re-read the region before the next one.
#[must_use]
pub fn next_placement_step(region: &ShardRegion, target: &[u64]) -> Option<PlacementStep> {
    // Both deleted candidate functions opened with this guard
    // (shard_scheduler.rs:1635, :1659) and AddPeer apply rejects on outstanding
    // debt. Without it, convergence stages a full region copy over the network
    // and then has the proposal rejected, every tick, uncounted.
    if region.transfer_outstanding() {
        return None;
    }
    if target.is_empty() {
        return None;
    }
    if let Some(&missing) = target.iter().find(|t| !region.peers.contains(t)) {
        return Some(PlacementStep::Add(missing));
    }
    let want_primary = target[0];
    if region.primary != want_primary {
        return Some(PlacementStep::Promote(want_primary));
    }
    if let Some(&extra) = region.peers.iter().find(|p| !target.contains(p)) {
        return Some(PlacementStep::Remove(extra));
    }
    None
}
```

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test --lib placement::`
Expected: PASS, 8 new tests.

- [ ] **Step 5: Commit**

```bash
cargo fmt -p hyperbytedb && cargo clippy --all-targets -- -D warnings
git add hyperbytedb/src/domain/sharding/placement.rs
git commit -m "Add single-step placement diff toward a target peer set"
```

---

### Task 4: The `RemovePeer` op

**Files:** Modify `hyperbytedb/src/domain/sharding/ops.rs`

**Interfaces:**
- Produces: `ShardMapOp::RemovePeer { key: MeasurementKey, region_id: u64, from_peer: u64, epoch: ShardEpoch }`

Mirrors `AddPeer`'s guards (`ops.rs:282`) and adds two: never remove the primary, never remove the last peer. RF policy is the caller's job; this op only keeps the map structurally valid.

- [ ] **Step 1: Write the failing tests**

Add to `mod tests` in `ops.rs`:

```rust
    fn bootstrapped(peers: Vec<u64>, primary: u64) -> (MeasurementKey, ShardMap, ShardEpoch) {
        let key = MeasurementKey::new("db", "autogen", "cpu");
        let mut region = sample_region(1, 0, u64::MAX, primary);
        region.peers = peers;
        region.primary = primary;
        let epoch = region.epoch;
        let mut map = ShardMap::default();
        apply_shard_map_op(
            &mut map,
            ShardMapOp::BootstrapMeasurement { key: key.clone(), region },
        )
        .unwrap();
        (key, map, epoch)
    }

    fn remove(key: &MeasurementKey, from_peer: u64, epoch: ShardEpoch) -> ShardMapOp {
        ShardMapOp::RemovePeer {
            key: key.clone(),
            region_id: 1,
            from_peer,
            epoch,
        }
    }

    #[test]
    fn remove_peer_drops_the_peer_and_bumps_conf_ver() {
        let (key, mut map, epoch) = bootstrapped(vec![1, 2, 3], 1);
        apply_shard_map_op(&mut map, remove(&key, 3, epoch)).unwrap();
        let r = &map.spaces.values().next().unwrap().regions[0];
        assert_eq!(r.peers, vec![1, 2]);
        assert_eq!(r.primary, 1);
        assert_eq!(r.epoch.conf_ver, epoch.conf_ver + 1);
    }

    #[test]
    fn remove_peer_refuses_the_primary() {
        let (key, mut map, epoch) = bootstrapped(vec![1, 2, 3], 1);
        assert!(matches!(
            apply_shard_map_op(&mut map, remove(&key, 1, epoch)).unwrap_err(),
            ShardMapApplyError::Invalid(_)
        ));
    }

    #[test]
    fn remove_peer_refuses_a_non_peer() {
        let (key, mut map, epoch) = bootstrapped(vec![1, 2, 3], 1);
        assert!(matches!(
            apply_shard_map_op(&mut map, remove(&key, 9, epoch)).unwrap_err(),
            ShardMapApplyError::Invalid(_)
        ));
    }

    #[test]
    fn remove_peer_refuses_the_last_peer() {
        let (key, mut map, epoch) = bootstrapped(vec![1], 1);
        assert!(matches!(
            apply_shard_map_op(&mut map, remove(&key, 1, epoch)).unwrap_err(),
            ShardMapApplyError::Invalid(_)
        ));
    }

    #[test]
    fn remove_peer_refuses_stale_epoch() {
        let (key, mut map, epoch) = bootstrapped(vec![1, 2, 3], 1);
        let stale = ShardEpoch { conf_ver: epoch.conf_ver + 5, version: epoch.version };
        assert!(matches!(
            apply_shard_map_op(&mut map, remove(&key, 3, stale)).unwrap_err(),
            ShardMapApplyError::StaleEpoch("RemovePeer")
        ));
    }

    #[test]
    fn remove_peer_refuses_while_transfer_debt_outstanding() {
        let key = MeasurementKey::new("db", "autogen", "cpu");
        let mut region = sample_region(1, 0, u64::MAX, 1);
        region.peers = vec![1, 2, 3];
        region.transfer_verified = Some(false);
        let epoch = region.epoch;
        let mut map = ShardMap::default();
        apply_shard_map_op(
            &mut map,
            ShardMapOp::BootstrapMeasurement { key: key.clone(), region },
        )
        .unwrap();
        assert!(matches!(
            apply_shard_map_op(&mut map, remove(&key, 3, epoch)).unwrap_err(),
            ShardMapApplyError::Invalid(_)
        ));
    }
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test --lib remove_peer`
Expected: FAIL — `no variant named RemovePeer found for enum ShardMapOp`.

- [ ] **Step 3: Implement**

Add to `enum ShardMapOp` immediately after the `AddPeer` variant:

```rust
    /// Shrink a region's replica set. Does not change primary.
    ///
    /// The mirror of [`ShardMapOp::AddPeer`], and what makes drain and
    /// rebalancing possible: without it a peer set can only grow. Refuses to
    /// remove the primary or the last peer, so a region is never left
    /// ownerless. RF policy belongs to the caller.
    RemovePeer {
        key: MeasurementKey,
        region_id: u64,
        from_peer: u64,
        #[serde(default)]
        epoch: ShardEpoch,
    },
```

Add to the `key()` match arm, alongside `AddPeer`:

```rust
            | ShardMapOp::RemovePeer { key, .. }
```

Add the apply arm immediately after the `AddPeer` arm (`ops.rs:312`):

```rust
        ShardMapOp::RemovePeer { key, region_id, from_peer, epoch } => {
            let space = map
                .spaces
                .get_mut(&key)
                .ok_or(ShardMapApplyError::UnknownSpace)?;
            let region = space
                .regions
                .iter_mut()
                .find(|r| r.region_id == region_id)
                .ok_or(ShardMapApplyError::UnknownRegion(region_id))?;
            if region.epoch != epoch {
                return Err(ShardMapApplyError::StaleEpoch("RemovePeer"));
            }
            if region.transfer_outstanding() {
                return Err(ShardMapApplyError::Invalid(
                    "cannot remove peer while transfer debt is outstanding".into(),
                ));
            }
            if !region.peers.contains(&from_peer) {
                return Err(ShardMapApplyError::Invalid(format!(
                    "peer {from_peer} not in region"
                )));
            }
            if region.primary == from_peer {
                return Err(ShardMapApplyError::Invalid(format!(
                    "cannot remove peer {from_peer}: it is the region primary"
                )));
            }
            if region.peers.len() <= 1 {
                return Err(ShardMapApplyError::Invalid(
                    "cannot remove the last peer of a region".into(),
                ));
            }
            region.peers.retain(|p| *p != from_peer);
            region.epoch = epoch.bump_conf_ver();
            sort_space_regions(space);
        }
```

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test --lib remove_peer`
Expected: PASS, 6 tests.

- [ ] **Step 5: Commit**

```bash
cargo fmt -p hyperbytedb && cargo clippy --all-targets -- -D warnings
git add hyperbytedb/src/domain/sharding/ops.rs
git commit -m "Add RemovePeer shard-map op"
```

---

### Task 5: Config flags

**Files:** Modify `hyperbytedb/src/config.rs`

**Interfaces:**
- Produces: `ShardingConfig::remove_peer_proposals_enabled: bool` (default `true`), `ShardingConfig::movement_budget_bytes_per_sec: u64` (default `0` = unlimited)

- [ ] **Step 1: Write the failing tests**

Add to `mod tests` in `config.rs`:

```rust
    #[test]
    fn remove_peer_proposals_default_on() {
        assert!(ShardingConfig::default().remove_peer_proposals_enabled);
    }

    #[test]
    fn movement_budget_defaults_to_unlimited() {
        assert_eq!(ShardingConfig::default().movement_budget_bytes_per_sec, 0);
    }
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test --lib movement_budget`
Expected: FAIL — `no field named movement_budget_bytes_per_sec`.

- [ ] **Step 3: Implement**

Add to `struct ShardingConfig` after `add_peer_proposals_enabled`:

```rust
    /// Propose `RemovePeer` shard-map ops to shrink a region's replica set.
    /// Disable for the duration of a rolling upgrade: a node on a build older
    /// than the `RemovePeer` op cannot decode it, rejects the append RPC
    /// carrying it, and stops replicating until every node is upgraded.
    #[serde(default = "default_remove_peer_proposals_enabled")]
    pub remove_peer_proposals_enabled: bool,
    /// Cluster-wide cap on region-transfer bytes per second for
    /// convergence-tier moves. 0 = unlimited. RF violations and drain
    /// evacuation are never throttled.
    #[serde(default = "default_movement_budget_bytes_per_sec")]
    pub movement_budget_bytes_per_sec: u64,
```

Add to the `Default` impl body:

```rust
            remove_peer_proposals_enabled: default_remove_peer_proposals_enabled(),
            movement_budget_bytes_per_sec: default_movement_budget_bytes_per_sec(),
```

Add next to `default_add_peer_proposals_enabled`:

```rust
fn default_remove_peer_proposals_enabled() -> bool {
    true
}

fn default_movement_budget_bytes_per_sec() -> u64 {
    0
}
```

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test --lib -- config`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cargo fmt -p hyperbytedb && cargo clippy --all-targets -- -D warnings
git add hyperbytedb/src/config.rs
git commit -m "Add RemovePeer gate and movement budget config"
```

---

### Task 6: Extract the staging call from `try_place_live_member`

**Files:** Modify `hyperbytedb/src/application/shard_scheduler.rs:1118-1148`

Pure refactor, **no behaviour change**. Extracts the two-branch staging block so Task 10 can reuse it without duplicating it.

**Interfaces:**
- Produces: `async fn stage_region_onto(&self, key: &MeasurementKey, region: &ShardRegion, to_peer: u64) -> Result<(), HyperbytedbError>`

- [ ] **Step 1: Extract the method**

Add to `impl ShardScheduler`:

```rust
    /// Copy a region's rows onto `to_peer`, from whichever node holds them.
    ///
    /// Staging always precedes the map commit. Heal's commit-then-stage
    /// ordering would publish a peer holding no rows, and scatter reads fall
    /// back to replicas, so that surfaces as silent empty results rather than
    /// an error.
    async fn stage_region_onto(
        &self,
        key: &MeasurementKey,
        region: &ShardRegion,
        to_peer: u64,
    ) -> Result<(), HyperbytedbError> {
        let Some(pc) = self.peer_client.as_ref() else {
            return Err(HyperbytedbError::ShardMap("no peer client".into()));
        };
        if region.primary == self.node_id {
            let outcome = stage_region_transfer_data(
                pc,
                &self.metadata,
                &self.wal,
                self.query_port.as_ref(),
                self.node_id,
                key,
                region,
                to_peer,
                self.max_points_per_request,
            )
            .await?;
            if !outcome.verified() {
                return Err(HyperbytedbError::ShardMap(
                    format!(
                        "placement stage unverified: exported={} applied={}",
                        outcome.exported, outcome.applied
                    )
                    .into(),
                ));
            }
            Ok(())
        } else {
            request_region_stage(
                pc.as_ref(),
                &self.membership,
                region.primary,
                key,
                region,
                to_peer,
            )
            .await
        }
    }
```

- [ ] **Step 2: Replace the inlined block**

In `try_place_live_member`, replace everything from `if region.primary == self.node_id {` through the closing brace of the `else` branch (the `request_region_stage(...).await?;` block) with:

```rust
        self.stage_region_onto(key, region, joiner).await?;
```

- [ ] **Step 3: Run the full suite to prove nothing changed**

Run: `cargo test --lib && cargo test --test '*'`
Expected: PASS — 561 lib and 288 integration, the Layer 0 baseline numbers. A refactor that changes a count changed behaviour; investigate rather than accept.

- [ ] **Step 4: Commit**

```bash
cargo fmt -p hyperbytedb && cargo clippy --all-targets -- -D warnings
git add hyperbytedb/src/application/shard_scheduler.rs
git commit -m "Extract region staging from live-member placement"
```

---

### Task 7: Extract the catch-up candidate probe

**Files:** Modify `hyperbytedb/src/application/shard_scheduler.rs:1086-1114`

Pure refactor, **no behaviour change**. Isolates the probe loop that fixed review findings #9/#10 so Task 10 reuses it rather than reimplementing it.

**Interfaces:**
- Produces: `async fn first_caught_up(&self, region_id: u64, cluster_ver: u64, candidates: &[(u64, String)]) -> Option<u64>`

- [ ] **Step 1: Extract the method**

Add to `impl ShardScheduler`:

```rust
    /// First candidate whose shard map has caught up to `cluster_ver`.
    ///
    /// Walks candidates rather than committing to one up front: choosing
    /// before probing let a single lagging node block every region forever
    /// (review findings #9/#10). A persistent stall warns and increments
    /// `hyperbytedb_shard_placement_stalled_total`.
    async fn first_caught_up(
        &self,
        region_id: u64,
        cluster_ver: u64,
        candidates: &[(u64, String)],
    ) -> Option<u64> {
        let pc = self.peer_client.as_ref()?;
        // Preserve the original's accounting: no candidates is not a stall.
        if candidates.is_empty() {
            self.clear_placement_stall(region_id).await;
            return None;
        }
        for (id, addr) in candidates {
            let ver = match self.cached_peer_map_version(*id, addr, pc.http_client()).await {
                Ok(v) => v,
                Err(e) => {
                    tracing::debug!(error = %e, candidate = id, "map version probe failed");
                    continue;
                }
            };
            if joiner_map_caught_up(cluster_ver, ver) {
                self.clear_placement_stall(region_id).await;
                return Some(*id);
            }
            tracing::debug!(
                region_id,
                candidate = id,
                cluster_ver,
                candidate_ver = ver,
                "candidate map not caught up"
            );
        }
        self.note_placement_stall(region_id, candidates.len()).await;
        None
    }
```

- [ ] **Step 2: Replace the inlined block**

In `try_place_live_member`, replace from `let cluster_ver = map.map_version;` through `self.clear_placement_stall(region.region_id).await;` with:

```rust
        let Some(joiner) = self
            .first_caught_up(region.region_id, map.map_version, &candidates)
            .await
        else {
            return Ok(false);
        };
```

- [ ] **Step 3: Run the full suite to prove nothing changed**

Run: `cargo test --lib && cargo test --test '*'`
Expected: PASS — 561 and 288. `placement_skips_a_lagging_candidate_for_a_caught_up_one` and `map_version_is_probed_once_per_peer_per_tick` are the ones that matter here.

- [ ] **Step 4: Commit**

```bash
cargo fmt -p hyperbytedb && cargo clippy --all-targets -- -D warnings
git add hyperbytedb/src/application/shard_scheduler.rs
git commit -m "Extract caught-up candidate probe from live-member placement"
```

---

### Task 16: The acceptance benchmark script

**Files:** Create `scripts/sharding-linearity-bench.sh`

Independent of all Rust work. Runs against an already-deployed cluster.

- [ ] **Step 1: Write the script**

**Do not delegate to `scripts/sharding-perf-ingest.sh`.** It hardcodes 100k
series into measurement `metrics` and honours only `API` and `DB` — `SERIES`,
`DURATION_SECS` and `MEASUREMENT` are ignored. Driving it with `SERIES=5000000`
would silently ingest 100k series, produce roughly one region, and report a
perfectly flat curve at every node count — "proving" the design failed when
nothing was actually measured. This script generates its own load.

```bash
#!/usr/bin/env bash
# Acceptance benchmark for linear scaling.
#
# One measurement, fixed load, sustained points/sec. Run at N = 3, 6, 12, 24,
# 48 and compare against the N=3 baseline. Target is >=80% of ideal linear
# scaling at N=48 (>=12.8x throughput at 16x nodes), with no configuration
# slower than a smaller one.
#
# SERIES defaults to 5M for a reason. A measurement occupies at most as many
# nodes as it has regions; at region_split_series=100000 /
# region_max_series=150000 region count is roughly series/125k, so fewer than
# ~5M series cannot saturate 48 nodes however good placement is.
#
# BEFORE TRUSTING A FLAT CURVE: check that the region count is at least the
# node count. A flat curve with an even distribution over too few regions is
# the cardinality floor, not a placement failure.
set -euo pipefail

API="${API:-http://localhost:18080}"
NODES="${NODES:?set NODES to the cluster size under test}"
SERIES="${SERIES:-5000000}"
BATCH="${BATCH:-1000}"
DB="${DB:-linearity}"
RP="${RP:-autogen}"
MEASUREMENT="${MEASUREMENT:-cpu}"

echo "[bench] nodes=$NODES series=$SERIES batch=$BATCH db=$DB"
curl -sf -X POST "${API}/query" --data-urlencode "q=CREATE DATABASE ${DB}" >/dev/null

batches=$(( (SERIES + BATCH - 1) / BATCH ))
errors=0
start_ts=$(date +%s)

for b in $(seq 0 $((batches - 1))); do
  base=$((b * BATCH))
  body=""
  for i in $(seq "$base" $((base + BATCH - 1))); do
    body+="${MEASUREMENT},host=s${i} value=1 $((1700000000 + i))000000000\n"
  done
  code=$(curl -s -o /dev/null -w "%{http_code}" \
    -X POST "${API}/write?db=${DB}&rp=${RP}&precision=ns" \
    --data-binary "$(printf "%b" "$body")")
  [[ "$code" == "204" ]] || { echo "batch $b failed: HTTP $code" >&2; errors=$((errors + 1)); }
  if (( b % 100 == 0 )); then echo "  batch $b/$((batches - 1)) (HTTP $code)"; fi
done

duration=$(( $(date +%s) - start_ts ))
(( duration > 0 )) || duration=1
echo "[bench] nodes=$NODES series=$SERIES duration=${duration}s errors=${errors}"
echo "[bench] points_per_sec=$(( SERIES / duration ))"

regions=$(curl -sf "${API}/internal/shard/map" \
  | grep -o "\"region_id\"" | wc -l | tr -d ' ')
echo "[bench] regions=${regions} nodes=${NODES}"
if (( regions < NODES )); then
  echo "[bench] WARNING: regions ($regions) < nodes ($NODES) -- the measurement"
  echo "[bench] cannot occupy every node. A flat curve here is the cardinality"
  echo "[bench] floor, NOT a placement defect. Raise SERIES before concluding."
fi
```

- [ ] **Step 2: Verify it is executable and lints**

```bash
chmod +x scripts/sharding-linearity-bench.sh
bash -n scripts/sharding-linearity-bench.sh
NODES=3 SERIES=10 BATCH=10 bash scripts/sharding-linearity-bench.sh 2>&1 | tail -20
```
Expected: `bash -n` silent. The run needs a live cluster on `$API`; without one
it fails at the `CREATE DATABASE` curl. Confirm the failure is the curl, not a
syntax or unbound-variable error. With a cluster up, confirm
`points_per_sec` and `regions=` both print.

Verify the region-count probe against the real payload: `/internal/shard/map`
must actually contain `"region_id"` keys. If the field is named differently,
fix the `grep -o` pattern rather than leaving a probe that always reports 0.

- [ ] **Step 3: Commit**

```bash
git add scripts/sharding-linearity-bench.sh
git commit -m "Add linear-scaling acceptance benchmark"
```

---

## Wave 2

### Task 2: `target_placement`

**Files:** Modify `hyperbytedb/src/domain/sharding/placement.rs`
**Depends on:** Task 1

**Interfaces:**
- Produces: `pub fn target_placement(key: &MeasurementKey, start: u64, candidates: &[u64], rf: usize) -> Vec<u64>` — up to `rf` ids, best-first. **Index 0 is the target primary.** Every later task relies on that.

- [ ] **Step 1: Write the failing tests**

Add to `mod tests` in `placement.rs`:

```rust
    #[test]
    fn placement_is_deterministic_and_order_independent() {
        let asc: Vec<u64> = (1..=10).collect();
        let desc: Vec<u64> = (1..=10).rev().collect();
        let a = target_placement(&key(), 0, &asc, 3);
        assert_eq!(a.len(), 3);
        assert_eq!(a, target_placement(&key(), 0, &asc, 3));
        assert_eq!(a, target_placement(&key(), 0, &desc, 3));
    }

    #[test]
    fn different_starts_spread_across_nodes() {
        // The property the whole design exists for.
        let nodes: Vec<u64> = (1..=12).collect();
        let mut primaries = std::collections::HashSet::new();
        for i in 0..48u64 {
            primaries.insert(target_placement(&key(), i * 1_000_000, &nodes, 3)[0]);
        }
        assert!(primaries.len() >= 10, "only {} distinct primaries", primaries.len());
    }

    #[test]
    fn rf_is_capped_by_candidate_count() {
        assert_eq!(target_placement(&key(), 0, &[1, 2], 3).len(), 2);
        assert_eq!(target_placement(&key(), 0, &[], 3).len(), 0);
    }

    #[test]
    fn adding_a_node_moves_a_minimal_share() {
        let before: Vec<u64> = (1..=10).collect();
        let after: Vec<u64> = (1..=11).collect();
        let total = 500u64;
        let moved = (0..total)
            .filter(|&i| target_placement(&key(), i, &before, 3) != target_placement(&key(), i, &after, 3))
            .count() as u64;
        assert!(
            moved > total / 10 && moved < total / 2,
            "expected a minimal-but-nonzero share, got {moved}/{total}"
        );
    }

    #[test]
    fn removing_a_node_only_moves_its_own_regions() {
        let before: Vec<u64> = (1..=10).collect();
        let after: Vec<u64> = (1..=9).collect();
        for i in 0..500u64 {
            let a = target_placement(&key(), i, &before, 3);
            if !a.contains(&10) {
                assert_eq!(a, target_placement(&key(), i, &after, 3), "region {i} moved needlessly");
            }
        }
    }
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test --lib placement::`
Expected: FAIL — `cannot find function target_placement`.

- [ ] **Step 3: Implement**

```rust
/// Target peer set for a region, best-first. Index 0 is the target primary.
///
/// Ranking by raw hash descending is equivalent to the weighted rendezvous
/// score `w / -ln(h/MAX)` in the design doc whenever all weights are equal,
/// because that is a monotonic transform of `h`. Phase 1 ships uniform
/// weights; a weighted variant must switch to the full score.
#[must_use]
///
/// `candidates` is the set of nodes eligible to hold data — every `Active`
/// member. A node excluded from it (draining, dead) can never be selected,
/// which is what makes evacuation converge.
pub fn target_placement(
    key: &MeasurementKey,
    start: u64,
    candidates: &[u64],
    rf: usize,
) -> Vec<u64> {
    let mut scored: Vec<(u64, u64)> = candidates
        .iter()
        .map(|&n| (placement_hash(key, start, n), n))
        .collect();
    // Descending by score; node_id breaks ties so the order is total and
    // independent of the caller's candidate ordering.
    scored.sort_unstable_by(|a, b| b.0.cmp(&a.0).then(b.1.cmp(&a.1)));
    scored.truncate(rf);
    scored.into_iter().map(|(_, n)| n).collect()
}
```

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test --lib placement::`
Expected: PASS, 5 new tests.

- [ ] **Step 5: Commit**

```bash
cargo fmt -p hyperbytedb && cargo clippy --all-targets -- -D warnings
git add hyperbytedb/src/domain/sharding/placement.rs
git commit -m "Add rendezvous-hash target placement"
```

---

### Task 12: Movement tiers

**Files:** Modify `hyperbytedb/src/application/shard_scheduler.rs`
**Depends on:** nothing (pure function over existing types)

**Interfaces:**
- Produces: `pub(crate) enum MoveTier { RfViolation, Drain, Convergence }` (derives `Ord`, declared in that order), `fn move_tier(region: &ShardRegion, rf: usize, draining: &[u64]) -> MoveTier`

- [ ] **Step 1: Write the failing tests**

Add to `mod tests` in `shard_scheduler.rs`:

```rust
    #[test]
    fn under_replicated_region_is_the_top_tier() {
        assert_eq!(move_tier(&sample_region_peers(vec![1, 2], 1), 3, &[]), MoveTier::RfViolation);
    }

    #[test]
    fn region_holding_a_draining_node_is_the_drain_tier() {
        assert_eq!(move_tier(&sample_region_peers(vec![1, 2, 3], 1), 3, &[3]), MoveTier::Drain);
    }

    #[test]
    fn a_merely_misplaced_region_is_the_convergence_tier() {
        assert_eq!(move_tier(&sample_region_peers(vec![1, 2, 3], 1), 3, &[]), MoveTier::Convergence);
    }

    #[test]
    fn rf_violation_outranks_drain() {
        // Both under RF and holding a draining node: durability comes first.
        assert_eq!(move_tier(&sample_region_peers(vec![1, 3], 1), 3, &[3]), MoveTier::RfViolation);
    }

    #[test]
    fn tier_ordering_puts_safety_first() {
        assert!(MoveTier::RfViolation < MoveTier::Drain);
        assert!(MoveTier::Drain < MoveTier::Convergence);
    }
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test --lib move_tier`
Expected: FAIL — `cannot find function move_tier`.

- [ ] **Step 3: Implement**

```rust
/// Priority class for a pending region movement.
///
/// Variant order is the sort order, and it is the point: durability must never
/// queue behind balance.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum MoveTier {
    RfViolation,
    Drain,
    Convergence,
}

/// Classify why a region needs to move.
///
/// `unhealthy` is peers whose node is not `Active`. A region at full peer count
/// with a dead peer is under-replicated in substance even though `peers.len()`
/// still reads RF, and must not be throttled as ordinary balance work.
fn move_tier(region: &ShardRegion, rf: usize, draining: &[u64], unhealthy: &[u64]) -> MoveTier {
    let live = region.peers.iter().filter(|p| !unhealthy.contains(p)).count();
    if live < rf {
        MoveTier::RfViolation
    } else if region.peers.iter().any(|p| draining.contains(p)) {
        MoveTier::Drain
    } else {
        MoveTier::Convergence
    }
}
```

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test --lib move_tier`
Expected: PASS, 5 tests.

- [ ] **Step 5: Commit**

```bash
cargo fmt -p hyperbytedb && cargo clippy --all-targets -- -D warnings
git add hyperbytedb/src/application/shard_scheduler.rs
git commit -m "Classify region movements into priority tiers"
```

---

## Wave 3

### Task 8: `active_candidate_ids` and bootstrap rewiring

**Files:** Modify `hyperbytedb/src/application/shard_scheduler.rs`, `hyperbytedb/src/application/shard_routing.rs:301-333`
**Depends on:** Task 2

**Interfaces:**
- Produces: `async fn active_candidate_ids(&self) -> Vec<u64>` on `ShardScheduler`. `select_bootstrap_peers` keeps its signature.

- [ ] **Step 1: Write the failing test**

Add to `mod tests` in `shard_routing.rs`:

```rust
    #[test]
    fn bootstrap_peers_come_from_the_placement_function() {
        // A measurement's first region starts at 0, so bootstrap placement must
        // equal target_placement(key, 0, members, rf). If they diverge, a fresh
        // region is immediately "misplaced" and the scheduler moves it on tick 1.
        use crate::domain::sharding::placement::target_placement;
        use crate::domain::sharding::types::MeasurementKey;

        let key = MeasurementKey::new("db", "autogen", "cpu");
        let members: Vec<u64> = vec![1, 2, 3, 4, 5];
        let placed = target_placement(&key, 0, &members, 3);
        assert_eq!(placed.len(), 3);
        assert!(members.contains(&placed[0]));
        // Order must be stable regardless of how membership enumerates.
        let mut rev = members.clone();
        rev.reverse();
        assert_eq!(placed, target_placement(&key, 0, &rev, 3));
    }
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test --lib bootstrap_peers_come_from`
Expected: FAIL — unresolved import if `placement` is not reachable, otherwise it compiles and passes (it exercises Task 2 directly). If it passes here, treat it as a guard, and rely on Step 4's full-suite run for the behavioural check.

- [ ] **Step 3: Implement**
    /// Node ids eligible to hold region data.
    ///
    /// `holds_placement` (T17): `Active | Draining | Disconnected`. A
    /// restarting node keeps its regions — excluding it would make every
    /// rolling upgrade evacuate and refill the cluster, since the preStop hook
    /// drains every pod (`statefulset.go:243`), not just scale-down targets. A
    /// briefly unreachable node keeps them too; the dead-node timeout handles
    /// real death.
    ///
    /// `Decommissioning` and `Leaving` are excluded, which is exactly what
    /// makes convergence evacuate them without a dedicated rule.
    async fn active_candidate_ids(&self) -> Vec<u64> {
        let m = self.membership.read().await;
        let mut ids: Vec<u64> = m.nodes.values().filter(|n| holds_placement(n)).map(|n| n.node_id).collect();
        drop(m);
        ids.sort_unstable();
        ids.dedup();
        ids
    }
```

In `shard_routing.rs`, replace the `// Spread placement:` comment, the `use std::hash::{Hash, Hasher};` line, the `peers.sort_by_key(...)` block and the `peers.truncate(...)` call with:

```rust
    let rf = effective_replication_factor(ctx.config.replication_factor, member_count);
    let key = crate::domain::sharding::types::MeasurementKey::new(db, rp, measurement);
    Ok(crate::domain::sharding::placement::target_placement(
        &key, 0, &peers, rf,
    ))
```

Replace the doc comment's second paragraph with:

```rust
/// Candidates are ordered by [`crate::domain::sharding::placement::target_placement`],
/// the same function that places every other region, so a freshly bootstrapped
/// region already sits where the scheduler would put it and needs no movement.
```

- [ ] **Step 4: Run the full suite**

Run: `cargo test --lib && cargo test --test '*'`
Expected: PASS. `location_cache_locates_after_bootstrap` and `bootstrap_n1_peers_are_self` are the regression checks; n=1 must still yield `[self]`.

- [ ] **Step 5: Commit**

```bash
cargo fmt -p hyperbytedb && cargo clippy --all-targets -- -D warnings
git add hyperbytedb/src/application/shard_routing.rs hyperbytedb/src/application/shard_scheduler.rs
git commit -m "Bootstrap regions through the shared placement function"
```

---

### Task 10: `try_converge_placement`, added but not wired

**Files:** Modify `hyperbytedb/src/application/shard_scheduler.rs`
**Depends on:** Tasks 2, 3, 4, 5, 6, 7

Adds the method and its unit tests **without** touching `tick`. The old rules stay live and all 288 integration tests keep passing. Task 11 flips the switch. Keeping these separate means the risky deletion is its own reviewable change.

**Interfaces:**
- Produces: `async fn try_converge_placement(&self, key: &MeasurementKey, region: &ShardRegion) -> Result<bool, HyperbytedbError>` — `Ok(true)` when a step committed.

- [ ] **Step 1: Write the failing tests**

`PlacementTestHarness` (`shard_scheduler.rs:3374`) exposes only `scheduler`,
`proposals`, `probes` and `probe_count`. Add one field and one accessor to it,
mirroring `ReconcileTestHarness` which already carries `shard_map`:

```rust
    // in struct PlacementTestHarness
        shard_map: Arc<RocksDbShardMap>,

    // in impl PlacementTestHarness
        /// The single region of measurement `m0`, which `new(.., regions: 1)`
        /// bootstraps owned by this node.
        async fn region(&self) -> ShardRegion {
            let map = self.shard_map.snapshot().await.unwrap();
            map.spaces
                .get(&Self::key())
                .and_then(|s| s.regions.first().cloned())
                .expect("harness bootstrapped one region")
        }

        fn key() -> MeasurementKey {
            MeasurementKey::new("db", "autogen", "m0")
        }
```

Use the measurement name `new` actually bootstraps — read it out of
`PlacementTestHarness::new` and make `key()` match it exactly rather than
assuming `m0`.

Add to `mod tests` in `shard_scheduler.rs`. These call `try_converge_placement`
directly, so `tick` stays untouched until Task 11:

```rust
    #[tokio::test]
    async fn converge_adds_a_missing_target_peer() {
        // Peer 2 is live and caught up (delta 0), so the region is below its
        // target of RF peers and convergence must act.
        let h = PlacementTestHarness::new(true, &[(2, 0)], 1).await;
        let region = h.region().await;
        assert!(
            h.scheduler
                .try_converge_placement(&PlacementTestHarness::key(), &region)
                .await
                .unwrap(),
            "a region below its target should have acted"
        );
        let ops = h.proposals.lock().unwrap();
        assert!(
            ops.iter().any(|op| matches!(op, ShardMapOp::AddPeer { .. })),
            "expected an AddPeer proposal, got {ops:?}"
        );
    }

    #[tokio::test]
    async fn converge_withholds_add_while_the_upgrade_gate_is_closed() {
        let h = PlacementTestHarness::new(false, &[(2, 0)], 1).await;
        let region = h.region().await;
        assert!(
            !h.scheduler
                .try_converge_placement(&PlacementTestHarness::key(), &region)
                .await
                .unwrap(),
            "AddPeer proposed with the gate closed"
        );
        assert!(
            h.proposals.lock().unwrap().is_empty(),
            "gate closed but something was proposed"
        );
    }

    #[tokio::test]
    async fn converge_skips_a_lagging_candidate() {
        // Peer 2 reports a map behind the cluster's; staging onto it would run
        // against a lagging epoch.
        let h = PlacementTestHarness::new(true, &[(2, -5)], 1).await;
        let region = h.region().await;
        assert!(
            !h.scheduler
                .try_converge_placement(&PlacementTestHarness::key(), &region)
                .await
                .unwrap(),
            "staged onto a peer whose map had not caught up"
        );
    }

    #[tokio::test]
    async fn converge_refuses_rollup_destinations() {
        // apply_transfer_push is not idempotent: a re-staged SummingMergeTree
        // destination sums twice, permanently and silently.
        let h = PlacementTestHarness::new(true, &[(2, 0)], 1).await;
        let key = PlacementTestHarness::key();
        h.scheduler.seed_rollup_dest(&key).await;
        let region = h.region().await;
        assert!(
            !h.scheduler
                .try_converge_placement(&key, &region)
                .await
                .unwrap(),
            "a rollup destination was placed"
        );
    }
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test --lib converge_`
Expected: FAIL — `no method named try_converge_placement`, plus missing harness methods.

- [ ] **Step 3: Implement**

```rust
    /// Move one region one step toward its target placement.
    ///
    /// Replaces the recency-keyed rules. Those keyed on the newest peer/member
    /// to prove termination without a cooldown; with a pure target function
    /// termination is trivial — each step strictly reduces the difference and
    /// `next_placement_step` returns `None` at the fixed point.
    ///
    /// Returns `Ok(true)` when a step committed, so the caller yields the
    /// region: a commit bumps the epoch and invalidates this tick's snapshot.
    async fn try_converge_placement(
        &self,
        key: &MeasurementKey,
        region: &ShardRegion,
    ) -> Result<bool, HyperbytedbError> {
        if self.is_rollup_dest(key).await {
            return Ok(false);
        }
        // No peer client means nothing can be staged. Skip quietly rather than
        // erroring every tick, matching what try_place_live_member did.
        if self.peer_client.is_none() {
            return Ok(false);
        }
        // Re-read: drain_reconciliation runs before the tick loop and can bump
        // epochs, so the caller's snapshot may already be stale.
        let map = self.shard_map.snapshot().await?;
        let Some(region) = current_region(&map, key, region.region_id) else {
            return Ok(false);
        };
        let candidates = self.active_candidate_ids().await;
        // Absent is not empty. No candidates means membership is unreadable or
        // this node sees nothing Active -- not that the region is converged.
        // Falling through would return the same `None` as a healthy region and
        // clear the stall counter, masking the failure.
        if candidates.is_empty() {
            self.note_placement_stall(region.region_id, 0).await;
            return Ok(false);
        }
        let rf = effective_replication_factor(self.config.replication_factor, candidates.len());
        let target = target_placement(key, region.start, &candidates, rf);
        let Some(step) = next_placement_step(region, &target) else {
            self.clear_placement_stall(region.region_id).await;
            return Ok(false);
        };
        match step {
            PlacementStep::Add(to_peer) => {
                if !self.config.add_peer_proposals_enabled {
                    return Ok(false);
                }
                let addr = {
                    let m = self.membership.read().await;
                    m.get_node(to_peer).map(|n| n.addr.clone())
                };
                let Some(addr) = addr else { return Ok(false) };
                if self
                    .first_caught_up(region.region_id, map.map_version, &[(to_peer, addr)])
                    .await
                    .is_none()
                {
                    return Ok(false);
                }
                self.stage_region_onto(key, region, to_peer).await?;
                self.propose(ShardMapOp::AddPeer {
                    key: key.clone(),
                    region_id: region.region_id,
                    to_peer,
                    epoch: region.epoch,
                })
                .await?;
            }
            PlacementStep::Promote(new_primary) => {
                self.propose(ShardMapOp::TransferPrimary {
                    key: key.clone(),
                    region_id: region.region_id,
                    new_primary,
                    epoch: region.epoch,
                })
                .await?;
            }
            PlacementStep::Remove(from_peer) => {
                if !self.config.remove_peer_proposals_enabled {
                    return Ok(false);
                }
                self.propose(ShardMapOp::RemovePeer {
                    key: key.clone(),
                    region_id: region.region_id,
                    from_peer,
                    epoch: region.epoch,
                })
                .await?;
                // Drop the range on the departing node. `apply_transfer_push`
                // is not idempotent, so a node that keeps its rows after
                // removal silently double-counts if membership churn ever
                // re-selects it for this region — which HRW makes routine, not
                // exotic. `push_and_drop_range` is the same cleanup split
                // already performs on its source.
                self.drop_range_on_peer(key, region, from_peer).await?;
            }
        }
        counter!("hyperbytedb_shard_placements_total").increment(1);
        Ok(true)
    }
```

Add a companion that reuses the existing drop path:

```rust
    /// Drop a region's range on a node that no longer holds it.
    ///
    /// Required for correctness, not tidiness: staging is not idempotent, so a
    /// removed peer that keeps its rows double-counts when placement later
    /// re-selects it.
    async fn drop_range_on_peer(
        &self,
        key: &MeasurementKey,
        region: &ShardRegion,
        peer: u64,
    ) -> Result<(), HyperbytedbError> {
        // NOT push_and_drop_range: drop_region_data (shard_transfer.rs:137)
        // deletes through in-process metadata/sink ports, so it can only drop
        // the CALLER's copy -- and the scheduler runs on the leader, not on the
        // departing peer. request_region_rehome (shard_scheduler.rs:1843) takes
        // an arbitrary target_node and drop_source, which is what this needs.
        request_region_rehome(
            pc.as_ref(),
            &self.membership,
            peer,            // target_node: the departing peer drops its own copy
            key,
            region,
            region.primary,  // dest_primary: already holds the range
            true,            // drop_source
        )
        .await
    }
```

Resolve `pc` from `self.peer_client` before the call, returning `Ok(())` when
absent.

**The `Remove` arm must not propagate a drop failure with `?`.** Once
`RemovePeer` commits, the region matches its target, so `next_placement_step`
will never ask for that cleanup again and the stale copy is orphaned forever --
reopening the double-count hazard. `try_split` handles the identical case by
enqueueing a `PendingTransfer` on the `reconciliation_queue`
(`shard_scheduler.rs:762-793`). Mirror that: on failure, enqueue for retry and
log, do not return `Err`. Read that call site for the entry shape.

Add `use crate::domain::sharding::placement::{next_placement_step, target_placement, PlacementStep};` to the file's imports.

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test --lib converge_ && cargo test --lib && cargo test --test '*'`
Expected: PASS. Integration counts unchanged at 288 — `tick` is untouched, so nothing else may move.

- [ ] **Step 5: Commit**

```bash
cargo fmt -p hyperbytedb && cargo clippy --all-targets -- -D warnings
git add hyperbytedb/src/application/shard_scheduler.rs
git commit -m "Add placement convergence alongside the existing rules"
```

---

## Wave 4

### Task 9: Splits place the right child

**Files:** Modify `hyperbytedb/src/application/shard_scheduler.rs` (`try_split`); create `hyperbytedb/tests/sharding_placement_integration.rs`
**Depends on:** Tasks 2, 8

The left child keeps the parent's `start` and therefore its placement — do not recompute it and do not move its data. The right child's `start` is `split_key`, known at propose time even though its `region_id` is not.

- [ ] **Step 1: Write the characterization tests**

These pin properties of the pure function and **will pass as soon as Task 2 is in**. They are guards, not red-green TDD; the behavioural check is Step 4.

Create `hyperbytedb/tests/sharding_placement_integration.rs`:

```rust
//! Placement behaviour across splits, drain and node loss.

use hyperbytedb::domain::sharding::placement::target_placement;
use hyperbytedb::domain::sharding::types::MeasurementKey;

#[test]
fn split_children_do_not_share_the_parent_peer_set() {
    // The defect this phase exists to fix.
    let key = MeasurementKey::new("db", "autogen", "cpu");
    let members: Vec<u64> = (1..=6).collect();
    assert_ne!(
        target_placement(&key, 0, &members, 3),
        target_placement(&key, 1u64 << 63, &members, 3),
        "right child inherited the parent's placement; splits are not spreading"
    );
}

#[test]
fn left_child_placement_equals_parent_placement() {
    // What makes eager splits cheap: the left child keeps the parent's start,
    // so its placement is unchanged and its rows never move.
    let key = MeasurementKey::new("db", "autogen", "cpu");
    let members: Vec<u64> = (1..=6).collect();
    let parent = target_placement(&key, 0, &members, 3);
    assert_eq!(parent, target_placement(&key, 0, &members, 3));
}
```

- [ ] **Step 2: Run to confirm they pass**

Run: `cargo test --test sharding_placement_integration`
Expected: PASS, 2 tests. A failure here means Task 2's hash does not spread — stop and fix Task 2.

- [ ] **Step 3: Place the right child**

In `try_split`, after `right` is cloned and its range set, and **before** the op is proposed:

```rust
        // The left child keeps the parent's `start`, so its placement is
        // unchanged by construction and its rows never move. Only the right
        // child is re-placed. `region_id` is allocated at apply time, so the
        // hash is keyed on `start` — which is `split_key`, known here.
        let candidates = self.active_candidate_ids().await;
        let rf = effective_replication_factor(self.config.replication_factor, candidates.len());
        let target = target_placement(key, split_key, &candidates, rf);
        if !target.is_empty() {
            // ONLY the primary. Setting the full target set here would commit
            // RF-1 peers holding zero rows: try_split stages to right.primary
            // alone (shard_scheduler.rs:657), scatter falls back to replicas,
            // and the result is silently empty reads. Worse, it is self-sealing
            // -- once peers == target, next_placement_step reports the region
            // converged and never backfills. Convergence grows it to RF via
            // staged AddPeer steps in the unthrottled RfViolation tier.
            right.peers = vec![target[0]];
            right.primary = target[0];
        }
```

Apply-time normalization already flags a child whose primary differs from the parent's with `transfer_verified = Some(false)`, so the right child's transfer debt is recorded without further change.

- [ ] **Step 4: Run the full suite**

Run: `cargo test --lib && cargo test --test '*'`
Expected: PASS. `transfer_moves_flushed_data_and_drops_source_range` exercises the split-transfer path and is the regression check.

- [ ] **Step 5: Commit**

```bash
cargo fmt -p hyperbytedb && cargo clippy --all-targets -- -D warnings
git add hyperbytedb/src/application/shard_scheduler.rs hyperbytedb/tests/sharding_placement_integration.rs
git commit -m "Place the right split child instead of cloning the parent"
```

---

### Task 11: Wire the tick and delete the recency rules

**Files:** Modify `hyperbytedb/src/application/shard_scheduler.rs`
**Depends on:** Task 10

**High blast radius — review this one on its own.** Deletes 3 methods, ~5 free functions and 17 unit tests.

- [ ] **Step 1: Rewire `tick`**

Replace the `for step in ["live", "idle", "primary"]` block (`shard_scheduler.rs:444-462`, preceded by `let mut placed = false;` at 443) with:

```rust
                let placed = match self.try_converge_placement(&space.key, region).await {
                    Ok(placed) => placed,
                    Err(e) => {
                        tracing::debug!(
                            error = %e,
                            region_id = region.region_id,
                            "placement convergence skipped"
                        );
                        false
                    }
                };
```

- [ ] **Step 2: Run the integration suite before deleting anything**

Run: `cargo test --test '*'`
Expected: PASS, 288. If `fourth_node_joining_rf_complete_cluster_takes_a_region` fails, that is **expected and correct**: under hashing the fourth node is guaranteed a *share* across the measurement, not one specific region. Change its assertion to "node 4 holds at least one region membership across the measurement". Do not restore the old behaviour to make it pass.

- [ ] **Step 3: Delete the superseded code**

Delete the methods `try_place_live_member`, `try_place_idle_member`, `try_place_primary`, and the free functions left without a caller: `live_replica_candidate` (singular — already unused in production), `live_replica_candidates`, `primary_counts`, `idle_member_replica_swap`, `primary_placement_candidate`, `region_memberships`. (`latest_member` is a local variable inside `try_place_idle_member`, not a function — it disappears with its method.) Let `cargo clippy -- -D warnings` find the dead ones rather than guessing. Let `cargo clippy -- -D warnings` find dead code; it will fail on unused functions.

Delete these unit tests: `live_replica_candidate_picks_lowest_id_joiner`, `live_replica_candidate_none_when_rf_full`, `live_replica_candidate_none_when_already_peer`, `live_replica_candidates_empty_when_rf_full_or_indebted`, `live_replica_candidates_returns_every_option_in_order`, `primary_counts_tallies_every_space`, `primary_placement_hands_only_region_to_the_joiner`, `primary_placement_does_not_oscillate`, `primary_placement_stops_at_an_even_split`, `primary_placement_drains_a_lopsided_owner`, `primary_placement_skips_inactive_and_indebted`, `primary_placement_never_moves_off_the_newest_peer`, `idle_member_displaces_a_loaded_replica_not_the_primary`, `idle_member_swap_converges`, `idle_member_swap_skips_debt_and_balanced_regions`, `live_replica_candidate_none_with_transfer_debt`, `live_replica_candidates_returns_every_option_in_order`, `tick_places_a_live_joiner_as_a_region_peer`, `placement_refuses_rollup_destinations`, `tick_withholds_add_peer_while_the_upgrade_gate_is_closed`.

**Keep** `rollup_destinations_are_excluded_from_reconciliation` and
`rebuild_skips_flagged_rollup_destinations` — they cover the reconciliation
path, not placement.

**Also delete `try_rebalance` and its tests.** It proposes `TransferPrimary` to
the lightest peer by heartbeat bytes; `try_converge_placement` proposes
`TransferPrimary` to `target[0]`. Left in place they fight every tick, forever:
converge promotes the hash's choice, converge then returns `false`, rebalance
promotes the lightest peer, and the next tick promotes back. That is an
unbounded proposal loop, not a slow convergence. Deterministic primary
selection subsumes it — the approved design accepts "no reaction to real load
imbalance, relies on the law of large numbers" as the cost of a pure function.

**Keep** `try_replace_dead_peer`, `try_failover_unhealthy_primary`,
`stage_region_onto`, `first_caught_up`, `joiner_map_caught_up`, `current_region`
and `push_and_drop_range`.

- [ ] **Step 4: Run everything**

Run: `cargo test --lib && cargo test --test '*' && cargo clippy --all-targets -- -D warnings`
Expected: PASS. Lib count drops by ~18 from 521; integration holds at 288.

- [ ] **Step 5: Commit**

```bash
cargo fmt -p hyperbytedb
git add hyperbytedb/src/application/shard_scheduler.rs hyperbytedb/tests/
git commit -m "Replace recency-keyed placement with convergence toward a target"
```

---

## Wave 5

### Task 13: Budget convergence-tier moves

**Files:** Modify `hyperbytedb/src/application/shard_scheduler.rs`
**Depends on:** Tasks 11, 12

- [ ] **Step 1: Write the failing test**

```rust
    #[test]
    fn budget_gates_only_the_convergence_tier() {
        // Pure check of the policy the tick enforces: safety tiers are exempt.
        let converging = sample_region_peers(vec![1, 2, 3], 1);
        let under_rf = sample_region_peers(vec![1, 2], 1);
        assert_eq!(move_tier(&converging, 3, &[]), MoveTier::Convergence);
        assert_eq!(move_tier(&under_rf, 3, &[]), MoveTier::RfViolation);
        assert!(budget_exhausted(MoveTier::Convergence, 100, 100));
        assert!(!budget_exhausted(MoveTier::RfViolation, 100, 100));
        assert!(!budget_exhausted(MoveTier::Drain, 100, 100));
        assert!(!budget_exhausted(MoveTier::Convergence, 0, u64::MAX), "0 means unlimited");
        assert!(!budget_exhausted(MoveTier::Convergence, 100, 99));
    }

    #[tokio::test]
    async fn a_budgeted_tick_still_places_an_under_replicated_region() {
        // Budget of 1 byte bars optional movement; the harness region is below
        // its target, so an RF-tier placement must still be proposed.
        let h = PlacementTestHarness::new(true, &[(2, 0)], 1).await;
        h.scheduler.tick_once_for_test().await.unwrap();
        let ops = h.proposals.lock().unwrap();
        assert!(
            ops.iter().any(|op| matches!(op, ShardMapOp::AddPeer { .. })),
            "an RF violation was throttled, got {ops:?}"
        );
    }
```

`budget_exhausted` is a pure helper added in Step 3, which keeps the policy
testable without a harness that can inject byte counts.

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test --lib convergence_moves_stop`
Expected: FAIL — no budget is enforced, so the convergence move proceeds.

- [ ] **Step 3: Implement**

Add the pure policy helper next to `move_tier`:

```rust
/// True when a move must yield to the budget.
///
/// Only `Convergence` is throttled; RF violations and drain evacuation are
/// exempt, because safety must never queue behind balance. A budget of 0 means
/// unlimited.
fn budget_exhausted(tier: MoveTier, allowance: u64, moved: u64) -> bool {
    tier == MoveTier::Convergence && allowance > 0 && moved >= allowance
}
```

Store the scheduler's tick interval on `ShardScheduler` as
`tick_interval_secs: u64`, set from the `interval` already passed to
`ShardScheduler::run` (`shard_scheduler.rs:299`, sourced from
`sharding.heartbeat_interval_secs` at `runtime/mod.rs:228`). The budget is
meaningless without it.

Add a per-tick counter field to `ShardScheduler`: `tick_moved_bytes: AtomicU64`.
Not `Arc<AtomicU64>` — the scheduler is already shared behind an `Arc`, so
wrapping again buys nothing (`own-arc-shared`). `Relaxed` ordering is correct
here: the counter is written and read only by the single scheduler task and
orders no other memory (`conc-atomic-ordering`). Reset it at the top of `tick` alongside the `peer_map_versions` clear:

```rust
        self.tick_moved_bytes.store(0, Ordering::Relaxed);
```

In `try_converge_placement`, after the `let Some(step) = next_placement_step(...)
else { clear_placement_stall; return Ok(false) };` binding and before matching on
`step`, with `let draining = self.decommissioning_node_ids().await;` on the line above:

```rust
        // Safety never queues behind balance: RF violations and drain
        // evacuation are exempt from the budget entirely.
        // Tick cadence is sharding.heartbeat_interval_secs (runtime/mod.rs:228),
        // NOT split_merge_interval_secs -- that is an unrelated split/merge
        // cooldown defaulting to 3600, which would hand every 10s tick an
        // hour's allowance.
        let allowance = self
            .config
            .movement_budget_bytes_per_sec
            .saturating_mul(self.tick_interval_secs.max(1));
        // AFTER next_placement_step, never before: gating first would skip the
        // `None` branch for already-converged regions once a tick's budget was
        // spent, leaving their stall counters uncleared and reporting healthy
        // regions as stalled under exactly the load stall detection exists for.
        if budget_exhausted(
            move_tier(region, rf, &draining),
            allowance,
            self.tick_moved_bytes.load(Ordering::Relaxed),
        ) {
            return Ok(false);
        }
```

After a successful `PlacementStep::Add`, add the staged byte count:

```rust
                self.tick_moved_bytes
                    .fetch_add(staged_bytes, Ordering::Relaxed);
```

Have `stage_region_onto` return `Result<u64, HyperbytedbError>` (bytes staged, from `outcome.exported` on the local branch and 0 on the remote branch, where the source node accounts for it) and bind `let staged_bytes = self.stage_region_onto(key, region, to_peer).await?;`. Update Task 6's call site in any remaining caller accordingly.

Add:

```rust
    /// Node ids being permanently removed.
    ///
    /// NOT `Draining` — that is a restart, and its regions stay put.
    async fn decommissioning_node_ids(&self) -> Vec<u64> {
        let m = self.membership.read().await;
        let ids = m
            .nodes
            .values()
            .filter(|n| is_departing(n))
            .map(|n| n.node_id)
            .collect();
        drop(m);
        ids
    }
```

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test --lib && cargo test --test '*'`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cargo fmt -p hyperbytedb && cargo clippy --all-targets -- -D warnings
git add hyperbytedb/src/application/shard_scheduler.rs
git commit -m "Budget convergence-tier region movement"
```

---

### Task 14: Decommission evacuation

**Files:** Modify `hyperbytedb/src/application/shard_scheduler.rs`; add to `hyperbytedb/tests/sharding_placement_integration.rs`
**Depends on:** Tasks 11, 13

`active_candidate_ids` (T8) includes `Active` and `Draining` but not
`Decommissioning`, so a node moving to `Decommissioning` drops out of every
target and convergence evacuates it — while a `Draining` node stays put,
because it is restarting. `next_placement_step` returns `Add` before `Remove`, so **RF never dips**. Do not add a shortcut that removes first.

- [ ] **Step 1: Write the failing tests**

Add to `sharding_placement_integration.rs`:

```rust
#[test]
fn draining_a_node_removes_it_from_every_target_without_disturbing_others() {
    let key = MeasurementKey::new("db", "autogen", "cpu");
    let all: Vec<u64> = (1..=6).collect();
    let drained: Vec<u64> = (1..=5).collect();
    for i in 0..200u64 {
        let start = i * 1_000_000;
        let after = target_placement(&key, start, &drained, 3);
        assert!(!after.contains(&6), "draining node still targeted");
        assert_eq!(after.len(), 3, "RF must hold at 3 with 5 candidates");
        let before = target_placement(&key, start, &all, 3);
        if !before.contains(&6) {
            assert_eq!(before, after, "region {i} moved though it never held node 6");
        }
    }
}
```

Add to `mod tests` in `shard_scheduler.rs`:

```rust
    #[tokio::test]
    async fn decommission_completes_only_once_no_region_references_the_node() {
        let h = PlacementTestHarness::new(true, &[(2, 0)], 1).await;
        let owner = h.region().await.primary;
        assert!(
            !h.scheduler.decommission_complete(owner).await,
            "the owning node still holds a region"
        );
        assert!(
            h.scheduler.decommission_complete(9999).await,
            "a node holding nothing must read as drained"
        );
    }
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test --lib decommission_completes && cargo test --test sharding_placement_integration draining`
Expected: the `shard_scheduler` test FAILS with `no method named drain_complete`. The `sharding_placement_integration` test passes — it guards Task 2's function.

- [ ] **Step 3: Implement**

```rust
    /// True when no region anywhere in the map still lists `node_id`.
    ///
    /// A `Decommissioning` node stays in membership until this holds, so operators can
    /// watch evacuation progress and nothing removes a node that still owns
    /// rows.
    async fn decommission_complete(&self, node_id: u64) -> bool {
        let Ok(map) = self.shard_map.snapshot().await else {
            return false;
        };
        !map.spaces
            .values()
            .flat_map(|s| s.regions.iter())
            .any(|r| r.peers.contains(&node_id))
    }
```

At the end of `tick`, after the region loop:

```rust
        for node_id in self.decommissioning_node_ids().await {
            if self.decommission_complete(node_id).await {
                let mut m = self.membership.write().await;
                if let Some(n) = m.nodes.get_mut(&node_id) {
                    n.state = NodeState::Leaving;
                }
                drop(m);
                counter!("hyperbytedb_shard_decommission_completed_total").increment(1);
                tracing::info!(node_id, "decommission complete; node marked Leaving");
            }
        }
```

- [ ] **Step 4: Run everything**

Run: `cargo test --lib && cargo test --test '*'`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cargo fmt -p hyperbytedb && cargo clippy --all-targets -- -D warnings
git add hyperbytedb/src/application/shard_scheduler.rs hyperbytedb/tests/sharding_placement_integration.rs
git commit -m "Evacuate draining nodes without dipping replication factor"
```

---

### Task 15: Dead-node removal

**Files:** Modify `hyperbytedb/src/application/shard_scheduler.rs` (`try_replace_dead_peer`)
**Depends on:** Tasks 4, 11

A dead node is **drop-first**: there is no live source to stage from, so preserving its membership only delays RF restoration. This is the deliberate asymmetry with drain.

- [ ] **Step 1: Write the failing test**

```rust
    #[tokio::test]
    async fn a_dead_peer_is_removed_so_convergence_can_replace_it() {
        let h = PlacementTestHarness::new(true, &[(2, 0)], 1).await;
        let key = PlacementTestHarness::key();
        // Place peer 2, then kill it.
        h.scheduler.try_converge_placement(&key, &h.region().await).await.unwrap();
        h.mark_node_dead(2).await;
        h.scheduler
            .try_replace_dead_peer(&key, &h.region().await)
            .await
            .unwrap();
        let ops = h.proposals.lock().unwrap();
        assert!(
            ops.iter()
                .any(|op| matches!(op, ShardMapOp::RemovePeer { from_peer: 2, .. })),
            "dead peer not removed; RF restoration is blocked behind it, got {ops:?}"
        );
    }

    #[tokio::test]
    async fn a_dead_primary_is_demoted_rather_than_removed() {
        // RemovePeer refuses the primary, so the dead-primary path must route
        // through failover instead and never leave the region ownerless.
        let h = PlacementTestHarness::new(true, &[(2, 0)], 1).await;
        let key = PlacementTestHarness::key();
        let primary = h.region().await.primary;
        h.mark_node_dead(primary).await;
        h.scheduler
            .try_replace_dead_peer(&key, &h.region().await)
            .await
            .unwrap();
        let ops = h.proposals.lock().unwrap();
        assert!(
            !ops.iter()
                .any(|op| matches!(op, ShardMapOp::RemovePeer { from_peer, .. } if *from_peer == primary)),
            "proposed RemovePeer for the primary; apply would reject it"
        );
    }
```

Add `mark_node_dead(&self, id: u64)` to `PlacementTestHarness` — sets that
node's `NodeState` to `Disconnected` with `last_heartbeat` far enough in the
past to exceed `primary_failover_after_secs`.

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test --lib a_dead_peer_is_removed`
Expected: FAIL — `try_replace_dead_peer` swaps via `MovePeer` and never removes.

- [ ] **Step 3: Implement**

In `try_replace_dead_peer`, when a peer's node has been non-`Active` past `primary_failover_after_secs`, propose `RemovePeer` for it instead of swapping, and let `try_converge_placement` stage the replacement on a later tick:

```rust
        if !self.config.remove_peer_proposals_enabled {
            return Ok(());
        }
        if region.primary == dead_peer {
            // RemovePeer refuses the primary. Demote first; the next tick
            // removes the peer once it is no longer the owner.
            return self.try_failover_unhealthy_primary(key, region, now).await;
        }
        self.propose(ShardMapOp::RemovePeer {
            key: key.clone(),
            region_id: region.region_id,
            from_peer: dead_peer,
            epoch: region.epoch,
        })
        .await?;
        counter!("hyperbytedb_shard_peer_heals_total").increment(1);
```

- [ ] **Step 4: Run everything**

Run: `cargo test --lib && cargo test --test '*' && cargo clippy --all-targets -- -D warnings`
Expected: PASS. `primary_failover_updates_shard_map_after_unhealthy_timeout` in `sharding_failover_integration.rs` is the regression check.

- [ ] **Step 5: Commit**

```bash
cargo fmt -p hyperbytedb
git add hyperbytedb/src/application/shard_scheduler.rs
git commit -m "Remove dead peers so convergence can replace them"
```

---

## Self-Review

**Spec coverage.** Placement function → T1, T2. Region identity on `start` → T1, T9. Step ordering / RF never dips → T3. `RemovePeer` and guards → T4. Upgrade gate → T5. Bootstrap → T8. Split → T9. Deletion of recency rules → T11. Movement tiers and budget → T12, T13. Drain → T14. Dead node → T15. Acceptance benchmark → T16. Merge needs no code — `try_merge` already builds the survivor as `left.clone()` with `end` extended, so the merged region keeps the left `start` and its placement is unchanged; verified before writing this plan.

**Deliberately out of scope.** Phase 2 (Raft voter tier) and Phase 3 (exact two-pass percentiles, bounded scatter fan-out) are separate milestones. Node weights are left uniform — `target_placement` takes `candidates` without weights; a weighted variant is a follow-up.

**Known behaviour changes an executor must not "fix".**
- `fourth_node_joining_rf_complete_cluster_takes_a_region` (T11) asserts a specific region under the old rules; under hashing it can only assert a share.
- Lib test count drops by ~18 at T11. That is the deletion, not a regression.
- T9's and T14's `sharding_placement_integration` tests pass as soon as T2 lands. They are characterization guards, labelled as such; the behavioural checks are the full-suite runs.

---

## Wave 1 (continued) — membership

### Task 17: `NodeState::Decommissioning`, predicates, and guarded transitions

**Files:** `hyperbytedb/src/domain/cluster/membership.rs`, `hyperbytedb/src/application/shard_peer_resolution.rs`

**Interfaces produced:**
- `NodeState::Decommissioning` variant (`Display`/`FromStr` as `"decommissioning"`).
- `pub fn is_serving(&NodeInfo) -> bool` — `Active` only.
- `pub fn holds_placement(&NodeInfo) -> bool` — `Active | Draining | Disconnected`.
- `pub fn is_departing(&NodeInfo) -> bool` — `Decommissioning | Leaving`.
- `ClusterMembership::transition(node_id, to) -> bool` — rejects illegal moves.

`is_active_peer` keeps its meaning and becomes a thin wrapper over `is_serving`; it is correct as-is and callers that mean "may take traffic" keep using it.

- [ ] **Step 1: Write the failing tests**

Add to `mod tests` in `membership.rs`:

```rust
    #[test]
    fn the_three_predicates_disagree_and_that_is_the_point() {
        // One enum, three independent questions. A draining node holds its
        // regions but serves nothing; a decommissioning node is departing.
        let draining = node(1, NodeState::Draining);
        assert!(!is_serving(&draining));
        assert!(holds_placement(&draining));
        assert!(!is_departing(&draining));

        let decomm = node(2, NodeState::Decommissioning);
        assert!(!is_serving(&decomm));
        assert!(!holds_placement(&decomm));
        assert!(is_departing(&decomm));

        // A blip keeps its regions; the dead-node timeout handles real death.
        let blip = node(3, NodeState::Disconnected);
        assert!(!is_serving(&blip));
        assert!(holds_placement(&blip));
        assert!(!is_departing(&blip));
    }

    #[test]
    fn departing_states_are_terminal_and_cannot_be_downgraded() {
        // The preStop hook fires /internal/drain on a pod the operator has
        // already decommissioned. Without this guard that call re-admits the
        // node as a placement candidate mid-evacuation.
        let mut m = ClusterMembership::new();
        m.add_node(node_info(1, NodeState::Active));
        assert!(m.transition(1, NodeState::Decommissioning));
        assert!(!m.transition(1, NodeState::Draining), "decommission was downgraded");
        assert!(!m.transition(1, NodeState::Active), "decommission was undone");
        assert_eq!(m.get_node(1).unwrap().state, NodeState::Decommissioning);

        assert!(m.transition(1, NodeState::Leaving));
        assert!(!m.transition(1, NodeState::Draining));
    }

    #[test]
    fn draining_returns_to_active() {
        let mut m = ClusterMembership::new();
        m.add_node(node_info(1, NodeState::Active));
        assert!(m.transition(1, NodeState::Draining));
        assert!(m.transition(1, NodeState::Active), "a restarted node must rejoin");
    }
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test --lib membership`
Expected: FAIL — `no variant named Decommissioning`, `cannot find function is_serving`, `no method named transition`.

- [ ] **Step 3: Implement**

Add the variant after `Draining`, plus its `Display` (`:21`) and `FromStr` (`:39`) arms:

```rust
    /// Permanently leaving. Regions are evacuated and the node is removed.
    /// Distinct from `Draining`, which is a restart that keeps its seat.
    Decommissioning,
```

Add the three predicates, and `transition`:

```rust
    /// Set `to`, refusing moves out of a terminal departing state.
    ///
    /// `set_state` overwrites unconditionally, which lets the preStop hook's
    /// `/internal/drain` clobber `Decommissioning` back to `Draining`
    /// mid-evacuation. Callers that mean "advance the lifecycle" use this.
    pub fn transition(&mut self, node_id: u64, to: NodeState) -> bool {
        let Some(node) = self.nodes.get(&node_id) else { return false };
        let from = node.state;
        let legal = match (from, to) {
            (NodeState::Decommissioning, NodeState::Leaving) => true,
            (NodeState::Decommissioning | NodeState::Leaving, _) => false,
            _ => true,
        };
        if legal { self.set_state(node_id, to) } else { false }
    }
```

**The compiler will now reject every exhaustive `match` on `NodeState`.** Visit each — that is the point of using a variant. `write.rs:54`, `ping.rs`, `log_store.rs` and `peer_handlers.rs` may have `_` catch-alls that swallow it silently: replace those with explicit arms. `write.rs` must reject client writes for `Decommissioning` exactly as it does for `Draining`.

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test --lib && cargo test --test '*'`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cargo fmt -p hyperbytedb && cargo clippy --all-targets -- -D warnings
git add hyperbytedb/src/domain/cluster/ hyperbytedb/src/application/shard_peer_resolution.rs hyperbytedb/src/adapters/http/
git commit -m "Add Decommissioning state, membership predicates, guarded transitions"
```

---

### Task 18: Probe hysteresis on `NodeInfo`

**Files:** `hyperbytedb/src/domain/cluster/membership.rs`, `hyperbytedb/src/application/cluster/heartbeat.rs`
**Depends on:** Task 17

**Interfaces:** `NodeInfo.consecutive_misses: u32`; `decide_transition(current, signal, consecutive_misses, miss_threshold)`.

**The counter lives on `NodeInfo`, not in `probe_peers`.** `probe_peers` (`heartbeat.rs:173`) is stateless and called fresh each tick (`:158`); a map local to it resets every tick and pins the count at 1 — hysteresis that compiles, passes its unit tests, and demotes on the first miss exactly as before.

- [ ] **Step 1: Write the failing tests**

```rust
    #[test]
    fn one_missed_probe_does_not_demote_but_the_threshold_does() {
        assert_eq!(decide_transition(NodeState::Active, &ProbeSignal::Unreachable, 1, 5), None);
        assert_eq!(
            decide_transition(NodeState::Active, &ProbeSignal::Unreachable, 5, 5),
            Some(NodeState::Disconnected)
        );
    }

    #[test]
    fn sustained_flapping_eventually_demotes() {
        // Hard-reset-on-success never demotes a node alternating 4 misses and
        // 1 success -- unreachable 80% of the time, permanently Active, worse
        // than the old single-miss rule. A success decrements instead.
        let mut misses: u32 = 0;
        for _ in 0..20 {
            for _ in 0..4 { misses = misses.saturating_add(1); }
            misses = misses.saturating_sub(1); // one success
        }
        assert!(misses >= 5, "flapping never accumulated: {misses}");
    }

    #[test]
    fn a_single_blip_recovers_without_demotion() {
        let mut misses: u32 = 1;
        misses = misses.saturating_sub(1);
        assert_eq!(decide_transition(NodeState::Active, &ProbeSignal::Unreachable, misses, 5), None);
    }
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test --lib demote`
Expected: FAIL — `decide_transition` takes 2 arguments, not 4.

- [ ] **Step 3: Implement**

Add `#[serde(default)] pub consecutive_misses: u32` to `NodeInfo` (mirroring `needs_sync` at `:52`). Widen `decide_transition` and change the `Unreachable` arm:

```rust
        (_, ProbeSignal::Unreachable) => {
            // A demotion now costs real data movement, so require sustained
            // failure. Slow to evict, fast to readmit.
            if matches!(current, Active | Syncing) && consecutive_misses >= miss_threshold {
                Some(Disconnected)
            } else {
                None
            }
        }
```

In `probe_peers`, after each probe: `Unreachable` increments that node's `consecutive_misses` (saturating), any `Response` **decrements** it (saturating, not reset). Pass the stored count and `config.cluster.heartbeat_miss_threshold` into `decide_transition`. `probe_peers` needs the threshold threaded in from `run_heartbeat_updater` (`:158`).

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test --lib && cargo test --test '*'`
Expected: PASS.

**The test burden here is behavioural, not mechanical.** There are 9 `probe_peers(` sites in `heartbeat.rs`. `unreachable_peer_is_disconnected` (`~:584`) probes once and asserts `Disconnected` — it must loop to the threshold, which is a rewrite. And a `decide_transition` call widened with a *below*-threshold count silently inverts what the test asserts: every widened call needs a deliberate `(n, threshold)` pair, with a comment saying which case it means.

- [ ] **Step 5: Commit**

```bash
cargo fmt -p hyperbytedb && cargo clippy --all-targets -- -D warnings
git add hyperbytedb/src/domain/cluster/membership.rs hyperbytedb/src/application/cluster/heartbeat.rs
git commit -m "Require sustained probe failure before demoting a node"
```

---

### Task 19: Split drain from decommission

**Files:** `hyperbytedb/src/application/cluster/drain.rs`, `hyperbytedb/src/adapters/http/peer_handlers.rs`, `hyperbytedb/src/adapters/http/router.rs`, `hyperbytedb/src/application/runtime/mod.rs`
**Depends on:** Task 17

**Interfaces:** `DrainService::decommission()`; `POST /internal/decommission` → 202, mirroring `handle_drain` (`peer_handlers.rs:479`).

**Drain gives up the crown; decommission gives up the seat.** `shard_handoff` (`drain.rs:110-190`) currently does both — `TransferPrimary` at `:161` **and** `MovePeer { from_peer: self.node_id }` at `:179` with `drop_source: true`. That eviction on every routine restart is the defect two red-team rounds found.

- [ ] **Step 1: Write the failing tests**

```rust
    #[tokio::test]
    async fn drain_hands_off_the_primary_but_keeps_the_seat() {
        // A restart must not evict the node: it is coming back, and its data
        // is still valid. Only the primary role moves.
        let h = PlacementTestHarness::new(true, &[(2, 0)], 1).await;
        h.drain_service().drain().await.unwrap();
        let region = h.region().await;
        assert_ne!(region.primary, h.node_id(), "primary was not handed off");
        assert!(region.peers.contains(&h.node_id()), "restart evicted the node");
        let ops = h.proposals.lock().unwrap();
        assert!(
            !ops.iter().any(|op| matches!(op, ShardMapOp::MovePeer { .. } | ShardMapOp::RemovePeer { .. })),
            "drain proposed an eviction, got {ops:?}"
        );
    }

    #[tokio::test]
    async fn decommission_sets_the_state_and_leaves_evacuation_to_convergence() {
        // Decommission must not reuse drain's single-pass, error-swallowing
        // walk, and must not self-declare Leaving on a 90s timer.
        let h = PlacementTestHarness::new(true, &[(2, 0)], 1).await;
        h.drain_service().decommission().await.unwrap();
        assert_eq!(h.state_of(h.node_id()).await, NodeState::Decommissioning);
        assert_ne!(h.state_of(h.node_id()).await, NodeState::Leaving);
    }
```

`PlacementTestHarness` needs `drain_service()`, `node_id()` and `state_of()`. Verify what the struct exposes before writing these — inventing helper names has twice produced tasks that could not compile.

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test --lib drain_hands_off && cargo test --lib decommission_sets`
Expected: FAIL — `drain` evicts via `MovePeer`, and `decommission` does not exist.

- [ ] **Step 3: Implement**

Split `shard_handoff` so the eviction half is decommission-only:

```rust
    /// Hand the primary role to another peer. Keeps this node in `region.peers`.
    ///
    /// A restart is coming back and its data stays valid, so it keeps its seat:
    /// no `MovePeer`, no `drop_source`, nothing to re-stage on return. Only
    /// writes stop landing here.
    async fn handoff_primaries(&self) -> Result<(), HyperbytedbError> { /* TransferPrimary only */ }
```

`handoff_primaries` must also pass **`drop_source: false`** into
`run_region_transfer`. Drain currently passes `true` ("This node is draining
away; its local copy must go") — correct under the old evict-on-drain model,
fatal under the new one, since keeping the seat while deleting the data leaves a
peer that owns a range it cannot serve.

Convert `drain()`'s two `set_state` calls to `transition()`, and do the same for
`handle_leave`'s self-leave arm in `peer_handlers.rs`, which sets `Draining`
and would otherwise clobber `Decommissioning` mid-evacuation.

`drain()` calls `handoff_primaries()` and transitions to `Draining`. It **must not** reach `Leaving` — the shutdown path's skip-double-drain check (`runtime/mod.rs:473`) changes from `state == Leaving` to "a drain already ran on this process", since a draining node now stays `Draining` until it restarts.

```rust
    /// Permanent removal. Sets the state and returns; the convergence loop
    /// evacuates via RemovePeer, which is RF-safe and retried, unlike drain's
    /// single-pass walk. Only the scheduler's decommission_complete() check
    /// may write Leaving.
    pub async fn decommission(&self) -> Result<(), HyperbytedbError> {
        {
            let mut m = self.membership.write().await;
            m.transition(self.node_id, NodeState::Decommissioning);
        }
        self.handoff_primaries().await?;
        self.flush_service.drain().await?;
        self.wait_for_replication_acks().await
    }
```

Add `handle_decommission` mirroring `handle_drain`, and register it in `router.rs` beside `/internal/drain` (`:189`).

**Do not change `handle_replicate_write` (`peer_handlers.rs:51`).** An earlier
revision said to remove `Draining` from its 503 check. That was wrong twice
over: `replication_peers` filters to `Active | Disconnected`, so peers never
replicate to a draining node anyway and the change is a no-op; and a returning
node is already resynced by `leader_monitor` (which walks `all_peers` and fires
on `wal_gap > 0`) plus hinted handoff (which fires on any transition into
`Active`). Add `NodeState::Decommissioning` to the reject arm alongside
`Draining` and `Leaving`.

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test --lib && cargo test --test '*'`
Expected: PASS. `sharding_failover_integration.rs` and `raft_integration.rs` are the regression checks.

- [ ] **Step 5: Commit**

```bash
cargo fmt -p hyperbytedb && cargo clippy --all-targets -- -D warnings
git add hyperbytedb/src/application/cluster/drain.rs hyperbytedb/src/adapters/http/ hyperbytedb/src/application/runtime/mod.rs
git commit -m "Drain hands off primaries; decommission evacuates and leaves"
```

---

## Companion operator work (separate repo)

Not part of this plan's tasks, but Phase 1 is not usable without it. Tracked in `hyperbytedb-operator`:

1. **`runScaleDownClusterHooks` calls `/internal/decommission`**, not `/internal/drain` (`hyperbytedbcluster_controller.go:799`). The preStop script (`statefulset.go:368`) keeps calling `/internal/drain` unchanged.
2. **Wait for `Leaving` before reducing `.spec.replicas` and before `CleanupOrphanPVCs`.** The current fixed ~120s clock (`podTerminationGraceSecs`) is unrelated to how long evacuating a node takes, and losing the race deletes a PVC whose regions have not finished moving.
3. **Deleting a pod is not decommissioning it.** Scale-down must decommission before the StatefulSet removes the ordinal, or the node is simply lost and RF is restored by heal rather than by evacuation.

## Red-team round 1

**Lenses:** adversarial contract, determinism/convergence, downstream consumer,
precedent/fresh-eyes. Four reviewers in parallel. **Verdict: SUBSTANTIVE from
all four**, on largely non-overlapping issues. Every finding below was validated
against the code before being accepted; file:line citations are ones I checked.

**Deviation:** the red-team skill mandates Cursor workers
(`composer-2.5-fast`, fallback `cursor-grok-4.6-high-fast`) and forbids Claude
models. This round ran in Claude Code on `sonnet`, and gave reviewers file paths
rather than the full plan inline (~1900 lines). Lens structure intact; workers
were not the specified ones.

### Blocking — Phase 1 is NOT ready to implement

**B1. Split publishes replicas holding no rows, and they are never backfilled.**
Task 9 set `right.peers = target.clone()`, but `try_split` stages to
`right.primary` only (`shard_scheduler.rs:657`). Every other node in the target
is committed as a peer with zero rows — the commit-then-stage anti-pattern
`ops.rs:61` warns about, which surfaces as silently empty reads because scatter
falls back to replicas. It is self-sealing: once `Split` commits, `peers ==
target`, so `next_placement_step` returns `None` and the gap is permanent. This
is safe today only because `pick_best_primary` (`:1963`) picks among existing
peers. **Fixed in Task 9:** the right child starts as `vec![target[0]]` and
convergence grows it to RF via ordinary staged `AddPeer` steps, landing in the
unthrottled `RfViolation` tier. Eagerness is preserved where it matters — write
load follows the primary, which is still hash-placed immediately.

**B2. `NodeState::Draining` already means "shutting down for any reason".**
`drain.rs:88` sets it unconditionally, and the operator attaches the preStop
hook to *every* pod in a multi-node cluster
(`hyperbytedb-operator/internal/hyperbytedb/statefulset.go:243`), not only
scale-down targets. Phase 1 gives that state a new meaning — "excluded from
every target placement" — so a routine image rollout would evacuate and refill
every node in sequence, in the one tier exempted from the movement budget.
Worse: `decide_transition` (`heartbeat.rs:85`) lists `Draining` as
operator-owned and never auto-promotes out of it, so a restarted node may stay
excluded indefinitely. **Needs a design decision, not a code fix** — see Open
questions.

**B3. The movement budget is ~360x too loose.** The scheduler ticks on
`sharding.heartbeat_interval_secs` (default 10s,
`runtime/mod.rs:228`), but the allowance was sized with
`split_merge_interval_secs` (default 3600). An hour's budget refreshed every ten
seconds. **Fixed in Task 13:** the tick interval is plumbed into the scheduler
and used for the allowance.

**B4. The RemovePeer data-drop cannot reach the departing node.**
`drop_region_data` (`shard_transfer.rs:137`) deletes through in-process
`metadata`/`sink` ports, so `push_and_drop_range` drops the *caller's* copy.
The scheduler runs on the leader, so it would delete the wrong node's data.
`request_region_rehome` (`:1843`) takes an arbitrary `target_node` and
`drop_source: bool` — that is the right primitive. Second half of the finding:
after `RemovePeer` commits, `next_placement_step` never asks again, so a failed
drop is unrecoverable; `try_split` enqueues a `PendingTransfer` for exactly this
(`:762-793`). **Fixed in Task 10:** correct primitive plus a reconciliation-queue
entry on failure.

**B5. Convergence drops the `transfer_outstanding()` guard.** Both deleted
candidate functions open with it (`:1635`, `:1659`), and `AddPeer` apply rejects
on outstanding debt. Without the guard, convergence stages a full region copy
over the network and then has the proposal rejected — every tick, with the
wasted bytes never counted against the budget. **Fixed in Task 3:**
`next_placement_step` returns `None` while debt is outstanding.

**B6. A single dropped heartbeat causes an unthrottled full re-placement.**
`decide_transition` (`heartbeat.rs:64-70`) demotes `Active → Disconnected` on
one unreachable probe with no hysteresis, and recovery is equally immediate. A
GC pause therefore shrinks `active_candidate_ids`, lowers effective RF, and
proposes `RemovePeer` across every region that node holds — classified
`Convergence` (peer count has not yet dropped below RF) and unthrottled by
default. The next successful probe re-adds it and re-stages everything. The
design spec promised a membership-epoch CAS (spec lines 123-128) precisely for
this; no task implements it. **Needs a design decision** — see Open questions.

### Non-blocking, fixed in this plan

- **Merge orphans right-only peers.** `try_merge` builds `merged = left.clone()`
  (`:978`); once children have independent peer sets, nodes in
  `right.peers \ left.peers` vanish from the map with no drop. The Self-Review's
  "merge needs no code" was true only while both children shared a peer set.
- **Budget gate ordering** suppressed `clear_placement_stall` for already-
  converged regions once a tick's budget was spent. Gate now sits after the
  converged check.
- **`MoveTier` ignored `Disconnected` peers**, misclassifying an
  under-replicated region as throttleable `Convergence`.
- **Absent-vs-empty:** an empty candidate set and a converged region both return
  `None`, so a membership glitch reads as "nothing to do" and clears the stall
  counter — masking the moment something is wrong.
- **Layering:** `MoveTier`/`move_tier`/`budget_exhausted` are pure policy and
  belong in `domain/sharding/placement.rs` beside `next_placement_step`, not in
  the application-layer scheduler.

### Scope findings — recorded, deferred

- **Raft voter membership is never cleaned up.** `/cluster/membership/remove-node`
  (`adapters/http/cluster.rs:242`) is the only caller of `change_membership` to
  drop a voter, and neither drain, dead-node handling, nor the operator invokes
  it. Every removed node becomes a permanent phantom voter, which Phase 2's
  fixed-voter-set migration inherits. Deferred to Phase 2, now stated rather
  than silently assumed.
- **The operator does not wait for evacuation.** `runScaleDownClusterHooks`
  fires `DrainNode` best-effort and proceeds; the preStop script `exit 0`s at
  `drainAckWaitSecs=90` inside `podTerminationGraceSecs=120`, and
  `CleanupOrphanPVCs` follows. A 90-120s clock is unrelated to how long
  evacuating a node takes, so the design's "zero data loss across scale-down"
  is not satisfiable by current operator mechanics.
- **No scale-down e2e gate exists.** The companion test plan's S0-S11 are
  scale-up only. B2 and the PVC race are exactly the class only a real
  Kubernetes run catches.
- **The spec claims the budget bounds split movement.** It does not —
  `try_split` moves data with no reference to the budget. Spec wording to
  correct.
- **Weighted placement** is specified but not implemented; equivalent only
  while weights are uniform.

### Open questions — resolved 2026-09-08

1. **Decommission vs restart: a new state.** `NodeState::Decommissioning` (T17).
   `Draining` keeps its flush-and-handoff meaning and **remains a placement
   candidate**, so a rolling restart moves no data; only `Decommissioning` is
   excluded and evacuated. `Draining` also becomes auto-recoverable, fixing a
   live defect where a restarted node stayed `Draining` in its peers' views
   forever (`heartbeat.rs:85`).

   **API: two endpoints, decided.** `/internal/drain` is unchanged;
   `/internal/decommission` is new (T19). A mode flag on the single endpoint was
   rejected: the preStop script is baked into the StatefulSet pod template, so
   pods predating an operator upgrade keep calling the old command with no
   parameter, and under a flag their behaviour would depend on which default the
   server chose — one of which evacuates the cluster. With two endpoints an old
   client *cannot* express decommission. The safety property is structural
   rather than conventional. They are also different lifecycle transitions:
   restart is reversible and expects return, decommission is terminal.

2. **Membership stability: hysteresis.** `decide_transition` requires
   `cluster.heartbeat_miss_threshold` consecutive missed probes before demoting
   (T18). Recovery stays immediate — slow to evict, fast to readmit. The knob
   already exists (default 5, `config.rs:829`, surfaced in the CRD as
   `heartbeatMissThreshold`) and was consumed only by `flush_service.rs`; it now
   means what its name says. The membership-epoch CAS the spec described is
   **not** implemented — recorded as a deliberate scope decision, since
   hysteresis addresses the flapping this phase actually introduces.

## Red-team round 2 — membership model

**Lenses:** state-machine/wire contract, hysteresis failure modes, operator
lifecycle. Three reviewers, scoped to T17-T19 plus T8's candidacy change and
T14. **Verdict: SUBSTANTIVE from all three.** Same deviation as round 1
(`sonnet`, paths not inline).

**The membership model does not work as designed.** T17-T19 relabel a state
machine, but the code that actually moves and serves data does not respect the
relabelling. Implementing them exactly as written **reproduces the round-1
defect they exist to fix.**

### Blocking

**R2-1. `drain()` already evicts, so `Draining` staying a candidate changes
nothing.** `shard_handoff` (`drain.rs:110-190`) runs unconditionally in
`drain()` and, for every region the node primaries, proposes `TransferPrimary`
**and `MovePeer { from_peer: self.node_id }`** (`drain.rs:181`) — removing the
node from `region.peers` outright, with `drop_source: true` on the transfer.
The preStop hook calls this on every pod. Placement never gets a say: the node
is gone from the map before convergence looks. Nothing in T17-T19 touches
`shard_handoff`. This is the B2 defect, intact.

**R2-2. Two writers of `Leaving`, and the wrong one wins.** `drain()` sets
`Leaving` (`drain.rs:102`) right after `wait_for_replication_acks()`, which is
hard-capped at 90s (`drain.rs:194`). T14 sets it on `decommission_complete()` —
actual evacuation. The operator is told to wait for `Leaving` before deleting a
PVC, so its safety gate resolves on a timer, not on evacuation, and fires first
for exactly the nodes that hold the most data. Found independently by two
lenses.

**R2-3. preStop clobbers `Decommissioning` mid-evacuation.** `set_state`
(`membership.rs:81-96`) has **no transition guard** — it logs and overwrites.
The operator decommissions, then Kubernetes terminates the pod, whose preStop
hook calls `/internal/drain`, which sets `Draining` — re-admitting the node as a
placement candidate while its regions are still moving off. `runtime/mod.rs:473`
only skips the shutdown drain when the state is already `Leaving`, so SIGTERM
does the same. And since T17 makes `Draining` auto-recover, a decommissioned
node can flip back to `Active`. Found independently by two lenses.

**R2-4. Wire hazard: an old node cannot join a cluster containing a
`Decommissioning` peer.** `JoinResponse` carries a whole `ClusterMembership`
(`domain/cluster/sync.rs:58`), returned by `handle_join`
(`peer_handlers.rs:308`). `NodeState` derives plain `Deserialize`
(`membership.rs:6`) with no `#[serde(other)]`, so an unknown variant string
fails the entire join. Un-gated, on a live path — the `AddPeer` hazard one layer
down, and this time without the opt-out flag.

**R2-5. The hysteresis counter has no specified owner, and the natural reading
silently no-ops it.** `probe_peers` (`heartbeat.rs:173`) is stateless and is
called fresh each tick (`:158`). T18 says "in `probe_peers`, keep a per-peer
consecutive-miss count" — put the map there and it re-initialises every tick,
pinning the count at 1. It compiles, the unit tests pass, and the live system
demotes on the first miss exactly as before.

**R2-6. Nothing in the serving path treats `Draining` as reachable.**
`is_active_peer` (`shard_peer_resolution.rs:38`) tests `== Active`, and
`resolve_region_peers` for a write yields **zero targets** for a non-Active
primary with no replica fallback. `handle_replicate_write`
(`peer_handlers.rs:51`) returns 503 while the local node is `Draining`, so its
copy falls behind — and T17's new `Draining → Active` recovery is a bare label
flip with no catch-up, so it can serve stale reads on return. "Rolling restarts
are free" is a stated premise with no enforcing rule.

### Non-blocking, recorded

- **Sub-threshold flapping is invisible.** Consecutive-with-hard-reset means 4
  misses / 1 success forever is never demoted, while 80% unreachable — worse
  than today for the degraded-but-not-dead case. Needs a window or decay, not
  "consecutive".
- **Failure detection gets slower, additively.** `primary_failover_after_secs`
  (60s) does not start until hysteresis flips the state, and `probe_timeout` is
  hardcoded to 5s (`runtime/mod.rs:93`) with the loop awaiting completion — so a
  hung peer costs ~5s per round, not the 2s interval. Worst case moves from
  ~65s to ~70-85s, during which every write to the region stalls a full
  `scatter_peer_timeout_ms` before failing.
- **`write.rs:54` has a `_ => {}` catch-all**, so `Decommissioning` falls
  through to "accept the write". Adding the variant does *not* produce a
  compile error there. Other `NodeState` matches need the same audit —
  `ping.rs`, `log_store.rs`, `peer_handlers.rs`.
- **Recovery re-selects the same node.** Placement is deterministic, so a node
  that flaps past the threshold and returns is very likely re-chosen for the
  regions it just vacated: a full evacuation followed by a full restage.
- **Test burden is behavioural, not mechanical.** There are 9 `probe_peers(`
  sites in `heartbeat.rs`; single-probe tests like
  `unreachable_peer_is_disconnected` must be rewritten to loop, and mechanically
  widening a `decide_transition` call with the wrong count silently inverts what
  the test asserts.
- **A 6→3 scale-down decommissions three nodes at once** while
  `OrderedReadyPodManagement` terminates them one at a time — half the candidate
  pool leaves the placement set simultaneously.
- **Rollout order is unstated.** If the operator ships before the server,
  `/internal/decommission` 404s and `DrainNode` swallows it — scale-down then
  sends no signal at all, worse than today.
- **Decommission is unreachable by a human.** No CRD field targets it; a direct
  curl leaves the operator unaware, and `Decommissioning` never auto-recovers.
- **Spec contradiction:** the "Movement rules" table still reads "Graceful drain
  → `RemovePeer` the draining node", which the new Membership semantics section
  forbids.

### Conclusion

Not a set of patches. The root cause is one mistake repeated: **a state was
relabelled without following what the label controls.** Round 1 caught it for
placement candidacy; round 2 finds the same gap in `shard_handoff`, peer
resolution, replication admission, and the join wire format.

Doing this properly means expanding scope into `drain.rs`, `shard_peer_resolution.rs`,
`peer_handlers.rs` and a wire-compatibility decision on `NodeState` — or
splitting membership into its own phase with its own design pass. Either way,
**T14 and T17-T19 must not be implemented as written.**

**Sequencing note.** PR #115 should merge## Red-team round 2 — membership model

**Lenses:** state-machine/wire contract, hysteresis failure modes, operator
lifecycle. Three reviewers, scoped to T17-T19 plus T8's candidacy change and
T14. **Verdict: SUBSTANTIVE from all three.** Same deviation as round 1
(`sonnet`, paths not inline).

**The membership model does not work as designed.** T17-T19 relabel a state
machine, but the code that actually moves and serves data does not respect the
relabelling. Implementing them exactly as written **reproduces the round-1
defect they exist to fix.**

### Blocking

**R2-1. `drain()` already evicts, so `Draining` staying a candidate changes
nothing.** `shard_handoff` (`drain.rs:110-190`) runs unconditionally in
`drain()` and, for every region the node primaries, proposes `TransferPrimary`
**and `MovePeer { from_peer: self.node_id }`** (`drain.rs:181`) — removing the
node from `region.peers` outright, with `drop_source: true` on the transfer.
The preStop hook calls this on every pod. Placement never gets a say: the node
is gone from the map before convergence looks. Nothing in T17-T19 touches
`shard_handoff`. This is the B2 defect, intact.

**R2-2. Two writers of `Leaving`, and the wrong one wins.** `drain()` sets
`Leaving` (`drain.rs:102`) right after `wait_for_replication_acks()`, which is
hard-capped at 90s (`drain.rs:194`). T14 sets it on `decommission_complete()` —
actual evacuation. The operator is told to wait for `Leaving` before deleting a
PVC, so its safety gate resolves on a timer, not on evacuation, and fires first
for exactly the nodes that hold the most data. Found independently by two
lenses.

**R2-3. preStop clobbers `Decommissioning` mid-evacuation.** `set_state`
(`membership.rs:81-96`) has **no transition guard** — it logs and overwrites.
The operator decommissions, then Kubernetes terminates the pod, whose preStop
hook calls `/internal/drain`, which sets `Draining` — re-admitting the node as a
placement candidate while its regions are still moving off. `runtime/mod.rs:473`
only skips the shutdown drain when the state is already `Leaving`, so SIGTERM
does the same. And since T17 makes `Draining` auto-recover, a decommissioned
node can flip back to `Active`. Found independently by two lenses.

**R2-4. Wire hazard: an old node cannot join a cluster containing a
`Decommissioning` peer.** `JoinResponse` carries a whole `ClusterMembership`
(`domain/cluster/sync.rs:58`), returned by `handle_join`
(`peer_handlers.rs:308`). `NodeState` derives plain `Deserialize`
(`membership.rs:6`) with no `#[serde(other)]`, so an unknown variant string
fails the entire join. Un-gated, on a live path — the `AddPeer` hazard one layer
down, and this time without the opt-out flag.

**R2-5. The hysteresis counter has no specified owner, and the natural reading
silently no-ops it.** `probe_peers` (`heartbeat.rs:173`) is stateless and is
called fresh each tick (`:158`). T18 says "in `probe_peers`, keep a per-peer
consecutive-miss count" — put the map there and it re-initialises every tick,
pinning the count at 1. It compiles, the unit tests pass, and the live system
demotes on the first miss exactly as before.

**R2-6. Nothing in the serving path treats `Draining` as reachable.**
`is_active_peer` (`shard_peer_resolution.rs:38`) tests `== Active`, and
`resolve_region_peers` for a write yields **zero targets** for a non-Active
primary with no replica fallback. `handle_replicate_write`
(`peer_handlers.rs:51`) returns 503 while the local node is `Draining`, so its
copy falls behind — and T17's new `Draining → Active` recovery is a bare label
flip with no catch-up, so it can serve stale reads on return. "Rolling restarts
are free" is a stated premise with no enforcing rule.

### Non-blocking, recorded

- **Sub-threshold flapping is invisible.** Consecutive-with-hard-reset means 4
  misses / 1 success forever is never demoted, while 80% unreachable — worse
  than today for the degraded-but-not-dead case. Needs a window or decay, not
  "consecutive".
- **Failure detection gets slower, additively.** `primary_failover_after_secs`
  (60s) does not start until hysteresis flips the state, and `probe_timeout` is
  hardcoded to 5s (`runtime/mod.rs:93`) with the loop awaiting completion — so a
  hung peer costs ~5s per round, not the 2s interval. Worst case moves from
  ~65s to ~70-85s, during which every write to the region stalls a full
  `scatter_peer_timeout_ms` before failing.
- **`write.rs:54` has a `_ => {}` catch-all**, so `Decommissioning` falls
  through to "accept the write". Adding the variant does *not* produce a
  compile error there. Other `NodeState` matches need the same audit —
  `ping.rs`, `log_store.rs`, `peer_handlers.rs`.
- **Recovery re-selects the same node.** Placement is deterministic, so a node
  that flaps past the threshold and returns is very likely re-chosen for the
  regions it just vacated: a full evacuation followed by a full restage.
- **Test burden is behavioural, not mechanical.** There are 9 `probe_peers(`
  sites in `heartbeat.rs`; single-probe tests like
  `unreachable_peer_is_disconnected` must be rewritten to loop, and mechanically
  widening a `decide_transition` call with the wrong count silently inverts what
  the test asserts.
- **A 6→3 scale-down decommissions three nodes at once** while
  `OrderedReadyPodManagement` terminates them one at a time — half the candidate
  pool leaves the placement set simultaneously.
- **Rollout order is unstated.** If the operator ships before the server,
  `/internal/decommission` 404s and `DrainNode` swallows it — scale-down then
  sends no signal at all, worse than today.
- **Decommission is unreachable by a human.** No CRD field targets it; a direct
  curl leaves the operator unaware, and `Decommissioning` never auto-recovers.
- **Spec contradiction:** the "Movement rules" table still reads "Graceful drain
  → `RemovePeer` the draining node", which the new Membership semantics section
  forbids.

### Conclusion

Not a set of patches. The root cause is one mistake repeated: **a state was
relabelled without following what the label controls.** Round 1 caught it for
placement candidacy; round 2 finds the same gap in `shard_handoff`, peer
resolution, replication admission, and the join wire format.

Doing this properly means expanding scope into `drain.rs`, `shard_peer_resolution.rs`,
`peer_handlers.rs` and a wire-compatibility decision on `NodeState` — or
splitting membership into its own phase with its own design pass. Either way,
**T14 and T17-T19 must not be implemented as written.**

**Sequencing note.** PR #115 should merge on its own terms before T11 lands, so the deletion is a clean replacement against a known-good baseline rather than a rebase against an open PR.
