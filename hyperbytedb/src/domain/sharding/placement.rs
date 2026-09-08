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
}
