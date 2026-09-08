//! Deterministic region placement.
//!
//! A region's peer set is a pure function of its identity, the live candidate
//! set, and the replication factor. Every coordinator computing it agrees
//! without coordination, so the scheduler never has to invent a placement — it
//! only executes the difference between where a region is and where this
//! function says it belongs.
//!
//! Regions are identified by `(MeasurementKey, region.start)`, not `region_id`.
//! [`crate::domain::sharding::ops::ShardMapOp::Split`] allocates the right
//! child's `region_id` at apply time so concurrent proposals cannot collide,
//! which means a proposer cannot hash on an id that does not exist yet.
//! `start` is known at propose time for both children and is stable for the
//! life of a region.

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
#[must_use]
pub fn placement_hash(key: &MeasurementKey, start: u64, node_id: u64) -> u64 {
    let mut acc = mix_bytes(GOLDEN, key.db.as_bytes());
    acc = mix_bytes(acc, key.rp.as_bytes());
    acc = mix_bytes(acc, key.measurement.as_bytes());
    acc = mix(acc.wrapping_add(start).wrapping_add(GOLDEN));
    mix(acc.wrapping_add(node_id).wrapping_add(GOLDEN))
}

/// Target peer set for a region, best-first. Index 0 is the target primary.
///
/// `candidates` is the set of nodes eligible to hold data — see
/// `holds_placement`. A node excluded from it (decommissioning, leaving) can
/// never be selected, which is what makes evacuation converge.
///
/// Ranking by raw hash descending is equivalent to the weighted rendezvous
/// score `w / -ln(h/MAX)` whenever all weights are equal, because that is a
/// monotonic transform of `h`. Phase 1 ships uniform weights; a weighted
/// variant must switch to the full score.
#[must_use]
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
/// Priority is deliberate and load-bearing:
///
/// 1. **Add** before remove, so replication factor never dips while
///    converging. This is what gives drain correct behaviour for free.
/// 2. **Promote** before remove, so the primary is never the node leaving.
/// 3. **Remove** last, only once the target set is fully present.
///
/// One step per call: a committed step bumps the region epoch, so the caller
/// must re-read the region before the next one.
///
/// Callers must act on the returned step; ignoring it stalls convergence.
#[must_use]
pub fn next_placement_step(region: &ShardRegion, target: &[u64]) -> Option<PlacementStep> {
    // Both deleted candidate functions opened with this guard, and `AddPeer`
    // apply rejects on outstanding debt. Without it, convergence stages a full
    // region copy over the network and then has the proposal rejected, every
    // tick, uncounted against the movement budget.
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

/// Priority class for a pending region movement.
///
/// Variant order is the sort order, and it is the point: durability must never
/// queue behind balance.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum MoveTier {
    /// Below effective RF in substance. Preempts everything, never throttled.
    RfViolation,
    /// Holds a peer being permanently removed. Bounded, never throttled, so a
    /// decommission finishes predictably.
    Evacuation,
    /// Merely misplaced. Uses leftover budget only.
    Convergence,
}

/// Classify why a region needs to move.
///
/// `unhealthy` is peers that are not serving, `departing` is peers being
/// permanently removed. A region at full peer count with a dead peer is
/// under-replicated in substance even though `peers.len()` still reads RF, and
/// must not be throttled as ordinary balance work.
#[must_use]
pub fn move_tier(
    region: &ShardRegion,
    rf: usize,
    departing: &[u64],
    unhealthy: &[u64],
) -> MoveTier {
    let live = region
        .peers
        .iter()
        .filter(|p| !unhealthy.contains(p))
        .count();
    if live < rf {
        MoveTier::RfViolation
    } else if region.peers.iter().any(|p| departing.contains(p)) {
        MoveTier::Evacuation
    } else {
        MoveTier::Convergence
    }
}

/// True when a move must yield to the movement budget.
///
/// Only `Convergence` is throttled; RF violations and evacuation are exempt,
/// because safety must never queue behind balance. An allowance of 0 means
/// unlimited.
#[must_use]
pub fn budget_exhausted(tier: MoveTier, allowance: u64, moved: u64) -> bool {
    tier == MoveTier::Convergence && allowance > 0 && moved >= allowance
}

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
        assert!(
            seen.len() > 40,
            "top byte collides too often: {}",
            seen.len()
        );
    }

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
        assert_eq!(
            next_placement_step(&region(vec![3, 1, 2], 3), &[3, 1, 2]),
            None
        );
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
    fn transfer_debt_blocks_convergence() {
        // AddPeer apply rejects while debt is outstanding, so proposing anyway
        // costs a full region copy for a guaranteed rejection, every tick.
        let mut r = region(vec![1, 2], 1);
        r.transfer_verified = Some(false);
        assert_eq!(next_placement_step(&r, &[3, 1, 2]), None);
    }

    #[test]
    fn repeated_steps_converge_to_a_fixed_point() {
        // This is the whole convergence argument that replaces the recency
        // keying. If it loops, the design is wrong, not the test.
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
        // The property the whole design exists for: one measurement's regions
        // must not all land on the same RF nodes.
        let nodes: Vec<u64> = (1..=12).collect();
        let mut primaries = std::collections::HashSet::new();
        for i in 0..48u64 {
            primaries.insert(target_placement(&key(), i * 1_000_000, &nodes, 3)[0]);
        }
        assert!(
            primaries.len() >= 10,
            "only {} distinct primaries",
            primaries.len()
        );
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
            .filter(|&i| {
                target_placement(&key(), i, &before, 3) != target_placement(&key(), i, &after, 3)
            })
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
                assert_eq!(
                    a,
                    target_placement(&key(), i, &after, 3),
                    "region {i} moved needlessly"
                );
            }
        }
    }

    #[test]
    fn under_replicated_region_is_the_top_tier() {
        assert_eq!(
            move_tier(&region(vec![1, 2], 1), 3, &[], &[]),
            MoveTier::RfViolation
        );
    }

    #[test]
    fn a_dead_peer_makes_a_full_region_under_replicated() {
        // peers.len() still reads 3, but only two can serve. Throttling this
        // as ordinary balance work would queue durability behind optimisation.
        assert_eq!(
            move_tier(&region(vec![1, 2, 3], 1), 3, &[], &[3]),
            MoveTier::RfViolation
        );
    }

    #[test]
    fn region_holding_a_departing_peer_is_the_evacuation_tier() {
        assert_eq!(
            move_tier(&region(vec![1, 2, 3], 1), 3, &[3], &[]),
            MoveTier::Evacuation
        );
    }

    #[test]
    fn a_merely_misplaced_region_is_the_convergence_tier() {
        assert_eq!(
            move_tier(&region(vec![1, 2, 3], 1), 3, &[], &[]),
            MoveTier::Convergence
        );
    }

    #[test]
    fn rf_violation_outranks_evacuation() {
        // Both under RF and holding a departing peer: durability comes first.
        assert_eq!(
            move_tier(&region(vec![1, 3], 1), 3, &[3], &[]),
            MoveTier::RfViolation
        );
    }

    #[test]
    fn tier_ordering_puts_safety_first() {
        assert!(MoveTier::RfViolation < MoveTier::Evacuation);
        assert!(MoveTier::Evacuation < MoveTier::Convergence);
    }

    #[test]
    fn budget_gates_only_the_convergence_tier() {
        assert!(budget_exhausted(MoveTier::Convergence, 100, 100));
        assert!(!budget_exhausted(MoveTier::RfViolation, 100, 100));
        assert!(!budget_exhausted(MoveTier::Evacuation, 100, 100));
        assert!(
            !budget_exhausted(MoveTier::Convergence, 0, u64::MAX),
            "0 allowance means unlimited"
        );
        assert!(!budget_exhausted(MoveTier::Convergence, 100, 99));
    }
}
