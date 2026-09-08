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
}
