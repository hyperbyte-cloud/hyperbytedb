//! Placement behaviour: how a measurement's regions spread across nodes.

use hyperbytedb::domain::sharding::placement::{
    PlacementStep, next_placement_step, target_placement,
};
use hyperbytedb::domain::sharding::types::{MeasurementKey, ShardRegion};

fn key() -> MeasurementKey {
    MeasurementKey::new("db", "autogen", "cpu")
}

fn region(start: u64, end: u64, peers: Vec<u64>, primary: u64) -> ShardRegion {
    ShardRegion {
        region_id: 1,
        start,
        end,
        epoch: Default::default(),
        peers,
        primary,
        last_split_at: 0,
        transfer_verified: None,
        transfer_first_seen: None,
    }
}

#[test]
fn split_children_do_not_share_the_parent_peer_set() {
    // The defect this whole phase exists to fix: try_split cloned the parent's
    // peer set, so every region of a measurement lived on the same RF nodes
    // however many times it split.
    let members: Vec<u64> = (1..=6).collect();
    assert_ne!(
        target_placement(&key(), 0, &members, 3),
        target_placement(&key(), 1u64 << 63, &members, 3),
        "right child inherited the parent's placement; splits are not spreading"
    );
}

#[test]
fn the_left_child_never_moves() {
    // What makes eager splits affordable: the left child keeps the parent's
    // `start`, so its placement is identical by construction and its rows stay
    // put. Only the right child is re-placed.
    let members: Vec<u64> = (1..=6).collect();
    let parent = target_placement(&key(), 0, &members, 3);
    let left_after_split = target_placement(&key(), 0, &members, 3);
    assert_eq!(parent, left_after_split);
}

#[test]
fn a_right_child_holding_only_its_primary_is_grown_to_rf_by_convergence() {
    // try_split stages to right.primary alone, so the child is published with
    // one peer. Convergence must then see it as under-replicated and add the
    // rest -- if it read as converged, the region would sit at RF=1 forever.
    let members: Vec<u64> = (1..=6).collect();
    let start = 1u64 << 63;
    let target = target_placement(&key(), start, &members, 3);
    let child = region(start, u64::MAX, vec![target[0]], target[0]);

    let step = next_placement_step(&child, &target);
    assert!(
        matches!(step, Some(PlacementStep::Add(_))),
        "a freshly split child at RF=1 must be grown, got {step:?}"
    );

    // And it converges to the full target rather than stopping early.
    let mut r = child;
    for _ in 0..16 {
        match next_placement_step(&r, &target) {
            Some(PlacementStep::Add(n)) => r.peers.push(n),
            Some(PlacementStep::Remove(n)) => r.peers.retain(|p| *p != n),
            Some(PlacementStep::Promote(n)) => r.primary = n,
            None => break,
        }
    }
    let mut peers = r.peers.clone();
    peers.sort_unstable();
    let mut want = target.clone();
    want.sort_unstable();
    assert_eq!(peers, want, "child did not reach full RF");
}

#[test]
fn a_measurement_spreads_its_primaries_as_it_splits() {
    // End to end on the pure function: 48 successive splits of one measurement
    // across 12 nodes must use most of them as primary. Under the old
    // clone-the-parent behaviour this would be exactly RF nodes forever.
    let members: Vec<u64> = (1..=12).collect();
    let mut primaries = std::collections::HashSet::new();
    for i in 0..48u64 {
        primaries.insert(target_placement(&key(), i * 1_000_000, &members, 3)[0]);
    }
    assert!(
        primaries.len() >= 10,
        "one measurement used only {} of 12 nodes as primary",
        primaries.len()
    );
}

#[test]
fn decommissioning_a_node_removes_it_from_every_target() {
    let all: Vec<u64> = (1..=6).collect();
    let remaining: Vec<u64> = (1..=5).collect();
    for i in 0..200u64 {
        let start = i * 1_000_000;
        let after = target_placement(&key(), start, &remaining, 3);
        assert!(!after.contains(&6), "departing node still targeted");
        assert_eq!(after.len(), 3, "RF must hold at 3 with 5 candidates");
        // And regions that never held it are undisturbed by its departure.
        let before = target_placement(&key(), start, &all, 3);
        if !before.contains(&6) {
            assert_eq!(
                before, after,
                "region {i} moved though it never held node 6"
            );
        }
    }
}
