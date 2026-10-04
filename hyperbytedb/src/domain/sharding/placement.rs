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

/// One region's input to [`balance_primaries`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrimaryCandidate {
    pub region_id: u64,
    /// Its current primary.
    pub current: u64,
    /// Its share of the load (series count); regions of equal weight count
    /// equally.
    pub weight: u64,
    /// Its target peer set in HRW order (`target_placement`).
    pub peers: Vec<u64>,
}

/// Choose each region's primary among its target peers so that every node
/// carries about the same primary load.
///
/// HRW picks which nodes hold a region, and used its first choice as the
/// primary too. That choice is effectively random, and with ~5 regions per
/// node one node ended up primary for 1.7-3x its share: 9 of 16 regions (56%
/// of the load) at N=3 on n3. Writes all go through the primary, so the
/// busiest node capped the cluster and N=3 -> N=6 scaled only 1.44x.
///
/// Heaviest regions first. A region keeps its current primary while that
/// node stays within `tolerance_pct` of its fair share, so a balanced layout
/// is left alone and nothing flaps; otherwise it goes to the least-loaded of
/// its peers, earlier HRW rank breaking ties. Deterministic for a given input.
#[must_use]
pub fn balance_primaries(
    regions: &[PrimaryCandidate],
    tolerance_pct: u64,
) -> std::collections::HashMap<u64, u64> {
    use std::collections::{BTreeSet, HashMap};

    let nodes: BTreeSet<u64> = regions
        .iter()
        .flat_map(|r| r.peers.iter().copied())
        .collect();
    let mut out = HashMap::with_capacity(regions.len());
    if nodes.is_empty() {
        return out;
    }
    let total: u128 = regions.iter().map(|r| u128::from(r.weight.max(1))).sum();
    // Fair share with tolerance, as (total * (100 + tol)) / (100 * nodes).
    let cap_num = total * u128::from(100 + tolerance_pct);
    let cap_den = 100 * nodes.len() as u128;

    let mut order: Vec<&PrimaryCandidate> =
        regions.iter().filter(|r| !r.peers.is_empty()).collect();
    order.sort_by(|a, b| b.weight.cmp(&a.weight).then(a.region_id.cmp(&b.region_id)));

    let mut load: HashMap<u64, u128> = HashMap::new();
    for r in order {
        let w = u128::from(r.weight.max(1));
        let within = |node: u64, load: &HashMap<u64, u128>| {
            (load.get(&node).copied().unwrap_or(0) + w) * cap_den <= cap_num
        };
        // A node that leads nothing yet may always keep its region: with
        // fewer regions than nodes a fair share is under one region, and
        // moving it would only relocate the imbalance (a lone region hopped
        // between its two peers).
        let unloaded =
            |node: u64, load: &HashMap<u64, u128>| load.get(&node).copied().unwrap_or(0) == 0;
        let chosen = if r.peers.contains(&r.current)
            && (within(r.current, &load) || unloaded(r.current, &load))
        {
            r.current
        } else {
            *r.peers
                .iter()
                .enumerate()
                .min_by_key(|(rank, n)| (load.get(n).copied().unwrap_or(0), *rank))
                .map(|(_, n)| n)
                .unwrap_or(&r.peers[0])
        };
        *load.entry(chosen).or_default() += w;
        out.insert(r.region_id, chosen);
    }

    // Greedy order can strand a node that is a peer of few regions (on a
    // 12-region layout it gave 5/5/2). Improve locally: move a region's
    // primary to another of its peers whenever that strictly lowers the
    // busier of the two. Terminates -- every move lowers the sum of squared
    // loads -- and a balanced layout has no such move, so nothing flaps.
    let by_id: HashMap<u64, &PrimaryCandidate> = regions.iter().map(|r| (r.region_id, r)).collect();
    let mut ids: Vec<u64> = out.keys().copied().collect();
    ids.sort_unstable();
    loop {
        let mut moved = false;
        for id in &ids {
            let r = by_id[id];
            let w = u128::from(r.weight.max(1));
            let from = out[id];
            let from_load = load.get(&from).copied().unwrap_or(0);
            let best = r
                .peers
                .iter()
                .filter(|p| **p != from)
                .min_by_key(|p| load.get(p).copied().unwrap_or(0))
                .copied();
            if let Some(to) = best
                && load.get(&to).copied().unwrap_or(0) + w < from_load
            {
                *load.entry(from).or_default() -= w;
                *load.entry(to).or_default() += w;
                out.insert(*id, to);
                moved = true;
            }
        }
        if !moved {
            break;
        }
    }
    out
}

/// Planning weight for each region of one space, from its hash-range width
/// and the series count its peers reported (0 when none has yet).
///
/// Series hash uniformly, so a region's expected size is the space's series
/// density times its width. A region with no report yet -- every region for a
/// while after a pre-split bootstrap -- is weighted at that expectation, not
/// at 0. Weighting it 0 made the nodes holding only unreported regions look
/// idle: at N=12 on n3 the plan started 17 copies onto one node of an already
/// even layout, the copies could not finish under write load, and that node
/// set the pace for the whole cluster. A region reporting more than expected
/// keeps its report. With no reports at all every region weighs 1.
#[must_use]
pub fn estimate_region_weights(regions: &[(u64, u64)]) -> Vec<u64> {
    let (series, width) = regions
        .iter()
        .filter(|(_, reported)| *reported > 0)
        .fold((0u128, 0u128), |(s, w), (width, reported)| {
            (s + u128::from(*reported), w + u128::from(*width))
        });
    regions
        .iter()
        .map(|(w, reported)| {
            let expected = if width == 0 {
                0
            } else {
                u128::from(*w) * series / width
            };
            u64::try_from(expected)
                .unwrap_or(u64::MAX)
                .max(*reported)
                .max(1)
        })
        .collect()
}

/// One region's input to [`plan_placement`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegionPlanInput {
    pub region_id: u64,
    pub current_peers: Vec<u64>,
    pub current_primary: u64,
    /// Its share of the load (series count).
    pub weight: u64,
    /// Every candidate node in HRW order for this region
    /// (`target_placement` with rf = all candidates); breaks ties.
    pub hrw: Vec<u64>,
}

/// Choose each region's peer set and primary so that both the copies each
/// node holds and the regions it leads are even.
///
/// Every peer applies every write to its region, so a node's work is its
/// primaries plus its replicas. Balancing primaries alone (H5) left replica
/// counts to HRW: on n3 at N=6 one node held no replicas and two held 8-9,
/// the loaded ones sat at their CPU limit and N=3 -> N=6 scaled 1.15x.
///
/// Heaviest regions first: keep current peers that are still candidates,
/// fill a short set from the least-loaded candidates. Then move a copy from
/// a region's most-loaded peer to the least-loaded non-peer only when that
/// closes a gap of at least two copies of the region (`load[to] + 2w <=
/// load[from]`), so a nearly-even layout never moves data. Finally
/// [`balance_primaries`] picks the leader within each set. Returns the
/// target (primary first) per region.
#[must_use]
pub fn plan_placement(
    regions: &[RegionPlanInput],
    rf: usize,
    tolerance_pct: u64,
) -> std::collections::HashMap<u64, Vec<u64>> {
    use std::collections::HashMap;

    let mut order: Vec<&RegionPlanInput> = regions.iter().filter(|r| !r.hrw.is_empty()).collect();
    order.sort_by(|a, b| b.weight.cmp(&a.weight).then(a.region_id.cmp(&b.region_id)));

    let rank =
        |r: &RegionPlanInput, n: u64| r.hrw.iter().position(|x| *x == n).unwrap_or(usize::MAX);
    let mut load: HashMap<u64, u128> = HashMap::new();
    for r in &order {
        for n in &r.hrw {
            load.entry(*n).or_default();
        }
    }
    let mut sets: HashMap<u64, Vec<u64>> = HashMap::new();
    for r in &order {
        let w = u128::from(r.weight.max(1));
        let want = rf.min(r.hrw.len());
        // Keep current peers that are still candidates, least loaded first
        // if there are too many.
        let mut set: Vec<u64> = r
            .current_peers
            .iter()
            .copied()
            .filter(|n| r.hrw.contains(n))
            .collect();
        set.sort_by_key(|n| (load[n], rank(r, *n)));
        set.truncate(want);
        while set.len() < want {
            let next = r
                .hrw
                .iter()
                .copied()
                .filter(|n| !set.contains(n))
                .min_by_key(|n| (load[n], rank(r, *n)));
            match next {
                Some(n) => set.push(n),
                None => break,
            }
        }
        for n in &set {
            *load.entry(*n).or_default() += w;
        }
        sets.insert(r.region_id, set);
    }

    // Move copies off the busiest nodes, only for a clear gain.
    loop {
        let mut moved = false;
        for r in &order {
            let w = u128::from(r.weight.max(1));
            let Some(set) = sets.get_mut(&r.region_id) else {
                continue;
            };
            let Some((i, from)) = set
                .iter()
                .copied()
                .enumerate()
                .max_by_key(|(_, n)| (load[n], std::cmp::Reverse(rank(r, *n))))
            else {
                continue;
            };
            let to = r
                .hrw
                .iter()
                .copied()
                .filter(|n| !set.contains(n))
                .min_by_key(|n| (load[n], rank(r, *n)));
            if let Some(to) = to
                && load[&to] + 2 * w <= load[&from]
            {
                *load.entry(from).or_default() -= w;
                *load.entry(to).or_default() += w;
                set[i] = to;
                moved = true;
            }
        }
        if !moved {
            break;
        }
    }

    let primaries = balance_primaries(
        &order
            .iter()
            .map(|r| {
                let mut peers = sets[&r.region_id].clone();
                peers.sort_by_key(|n| rank(r, *n));
                PrimaryCandidate {
                    region_id: r.region_id,
                    current: r.current_primary,
                    weight: r.weight,
                    peers,
                }
            })
            .collect::<Vec<_>>(),
        tolerance_pct,
    );
    order
        .iter()
        .map(|r| {
            let mut set = sets[&r.region_id].clone();
            set.sort_by_key(|n| rank(r, *n));
            (
                r.region_id,
                with_primary_first(set, primaries.get(&r.region_id).copied()),
            )
        })
        .collect()
}

/// `target` with `primary` moved to the front, when it is one of the peers.
#[must_use]
pub fn with_primary_first(mut target: Vec<u64>, primary: Option<u64>) -> Vec<u64> {
    if let Some(p) = primary
        && let Some(i) = target.iter().position(|n| *n == p)
    {
        target[..=i].rotate_right(1);
    }
    target
}

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
/// 2. **Remove** a non-primary extra next. It does not wait on a primacy
///    transfer: that transfer is a rebalance the progress guard may defer
///    for a long time, and queueing the removal behind it stalled every
///    decommission on n3 (the departing node sat in two regions whose
///    primaries were staying put).
/// 3. **Promote** before removing the primary, so the primary is never the
///    node leaving.
/// 4. **Remove** the old primary last, once it no longer is one.
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
    // Every step below removes a copy or hands off primacy. While a target
    // member is still a learner it holds nothing yet, so removing a full copy
    // now would leave the region one copy short. Wait for the promotion.
    if target.iter().any(|t| region.staging_peers.contains(t)) {
        return None;
    }
    if let Some(&extra) = region
        .peers
        .iter()
        .find(|p| !target.contains(p) && **p != region.primary)
    {
        return Some(PlacementStep::Remove(extra));
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

/// Bytes per second one move of `tier` may send: the cluster-wide `cap`
/// shared evenly by `movers` concurrent movers, so together they stay under
/// it. 0 means unpaced.
///
/// Only `Convergence` is paced; RF violations and evacuation are exempt,
/// because safety must never queue behind balance. A cap of 0 means
/// unlimited.
#[must_use]
pub fn move_rate_bytes_per_sec(tier: MoveTier, cap: u64, movers: usize) -> u64 {
    if tier != MoveTier::Convergence || cap == 0 {
        return 0;
    }
    (cap / movers.max(1) as u64).max(1)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::sharding::types::MeasurementKey;

    #[test]
    fn no_copy_is_removed_while_a_target_member_is_a_learner() {
        let mut region = ShardRegion {
            region_id: 1,
            start: 0,
            end: u64::MAX,
            epoch: Default::default(),
            peers: vec![1, 2, 3],
            primary: 1,
            last_split_at: 0,
            transfer_verified: None,
            transfer_first_seen: None,
            state: Default::default(),
            staged_from: None,
            staging_peers: vec![3],
        };
        assert_eq!(next_placement_step(&region, &[1, 3]), None);
        region.staging_peers.clear();
        assert_eq!(
            next_placement_step(&region, &[1, 3]),
            Some(PlacementStep::Remove(2))
        );
    }

    #[test]
    fn unreported_regions_are_weighted_at_the_space_density() {
        let w = u64::MAX / 4;
        // Two reported at ~100k, two not yet.
        let got = estimate_region_weights(&[(w, 100_000), (w, 98_000), (w, 0), (w, 0)]);
        assert_eq!(got[0], 100_000);
        assert_eq!(got[1], 99_000);
        assert_eq!(got[2], 99_000);
        assert_eq!(got[3], 99_000);
        // A half-width region expects half.
        let half = estimate_region_weights(&[(w, 100_000), (w / 2, 0)])[1];
        assert!((49_999..=50_000).contains(&half), "{half}");
        // Nothing reported: all equal.
        assert_eq!(estimate_region_weights(&[(w, 0), (w / 2, 0)]), vec![1, 1]);
    }

    /// The n3 N=12 run: a pre-split layout, already even, with only some
    /// regions reporting. The plan must leave it alone.
    #[test]
    fn a_balanced_presplit_layout_is_kept_while_reports_trickle_in() {
        let nodes: Vec<u64> = (1..=12).collect();
        let n = 24u64;
        let width = u64::MAX / n;
        let peers = |i: u64| {
            vec![
                nodes[(i % 12) as usize],
                nodes[((i + 1 + i / 12) % 12) as usize],
            ]
        };
        // Regions whose peers include node 4 have not reported yet.
        let reported: Vec<(u64, u64)> = (0..n)
            .map(|i| (width, if peers(i).contains(&4) { 0 } else { 100_000 }))
            .collect();
        let weights = estimate_region_weights(&reported);
        let inputs: Vec<RegionPlanInput> = (0..n)
            .map(|i| RegionPlanInput {
                region_id: i,
                current_peers: peers(i),
                current_primary: peers(i)[0],
                weight: weights[i as usize],
                hrw: target_placement(&key(), i * width, &nodes, nodes.len()),
            })
            .collect();
        let plan = plan_placement(&inputs, 2, 10);
        for r in &inputs {
            let mut want = r.current_peers.clone();
            want.sort_unstable();
            let mut got = plan[&r.region_id].clone();
            got.sort_unstable();
            assert_eq!(got, want, "region {} moved a copy", r.region_id);
        }
    }

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
            state: Default::default(),
            staged_from: None,
            staging_peers: Vec::new(),
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
    fn a_departing_non_primary_is_removed_before_a_primacy_rebalance() {
        // n3 region 28: peers [1,3,6,2,4], primary 3, node 6 departing, and
        // the target also wanted primacy on 2. Promote-first queued the
        // removal behind a transfer the progress guard kept deferring, so the
        // decommission never finished.
        assert_eq!(
            next_placement_step(&region(vec![1, 3, 6, 2, 4], 3), &[2, 1, 3, 4]),
            Some(PlacementStep::Remove(6))
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
    fn pacing_applies_only_to_the_convergence_tier() {
        assert_eq!(move_rate_bytes_per_sec(MoveTier::Convergence, 100, 4), 25);
        assert_eq!(move_rate_bytes_per_sec(MoveTier::RfViolation, 100, 4), 0);
        assert_eq!(move_rate_bytes_per_sec(MoveTier::Evacuation, 100, 4), 0);
        assert_eq!(
            move_rate_bytes_per_sec(MoveTier::Convergence, 0, 4),
            0,
            "a cap of 0 means unlimited"
        );
        // A cap smaller than the mover count must still pace, not unpace.
        assert_eq!(move_rate_bytes_per_sec(MoveTier::Convergence, 3, 4), 1);
        assert_eq!(move_rate_bytes_per_sec(MoveTier::Convergence, 100, 0), 100);
    }

    fn cand(id: u64, current: u64, weight: u64, peers: &[u64]) -> super::PrimaryCandidate {
        super::PrimaryCandidate {
            region_id: id,
            current,
            weight,
            peers: peers.to_vec(),
        }
    }

    fn primaries_per_node(plan: &std::collections::HashMap<u64, u64>) -> Vec<usize> {
        let mut c: std::collections::BTreeMap<u64, usize> = Default::default();
        for n in plan.values() {
            *c.entry(*n).or_default() += 1;
        }
        c.into_values().collect()
    }

    #[test]
    fn a_skewed_hrw_layout_is_evened_out() {
        // n3 at N=3: node 3 led 9 of 16 equal regions. Every region has node
        // 3 as its HRW first choice here, the worst case.
        let regions: Vec<_> = (1..=16u64)
            .map(|i| cand(i, 3, 75_000, if i % 2 == 0 { &[3, 1] } else { &[3, 2] }))
            .collect();
        let plan = super::balance_primaries(&regions, 10);
        let per = primaries_per_node(&plan);
        assert_eq!(per.len(), 3);
        assert!(per.iter().all(|&n| (5..=6).contains(&n)), "{per:?}");
        for r in &regions {
            assert!(
                r.peers.contains(&plan[&r.region_id]),
                "primary must be a peer"
            );
        }
    }

    #[test]
    fn a_balanced_layout_is_left_alone() {
        let regions: Vec<_> = (1..=6u64)
            .map(|i| cand(i, (i % 3) + 1, 100, &[(i % 3) + 1, ((i + 1) % 3) + 1]))
            .collect();
        let plan = super::balance_primaries(&regions, 10);
        for r in &regions {
            assert_eq!(plan[&r.region_id], r.current, "no flapping");
        }
    }

    #[test]
    fn a_lone_region_keeps_its_primary() {
        // One region, two peers: the fair share is half a region, so a
        // strict fair-share check would move it for no gain.
        let plan = super::balance_primaries(&[cand(1, 1, 50, &[2, 1])], 10);
        assert_eq!(plan[&1], 1);
    }

    #[test]
    fn a_primary_outside_its_target_set_is_replaced() {
        let plan = super::balance_primaries(&[cand(1, 9, 10, &[2, 3])], 10);
        assert_eq!(plan[&1], 2);
    }

    #[test]
    fn heavy_regions_are_spread_before_light_ones() {
        // Two heavy regions must not share a primary when they need not.
        let regions = vec![
            cand(1, 1, 1_000, &[1, 2]),
            cand(2, 1, 1_000, &[1, 2]),
            cand(3, 1, 10, &[1, 2]),
        ];
        let plan = super::balance_primaries(&regions, 10);
        assert_ne!(plan[&1], plan[&2]);
    }

    #[test]
    fn with_primary_first_rotates_only_a_member() {
        assert_eq!(
            super::with_primary_first(vec![1, 2, 3], Some(3)),
            vec![3, 1, 2]
        );
        assert_eq!(
            super::with_primary_first(vec![1, 2, 3], Some(9)),
            vec![1, 2, 3]
        );
        assert_eq!(
            super::with_primary_first(vec![1, 2, 3], None),
            vec![1, 2, 3]
        );
    }

    fn plan_in(id: u64, peers: &[u64], primary: u64, hrw: &[u64]) -> super::RegionPlanInput {
        super::RegionPlanInput {
            region_id: id,
            current_peers: peers.to_vec(),
            current_primary: primary,
            weight: 100,
            hrw: hrw.to_vec(),
        }
    }

    fn copies(
        plan: &std::collections::HashMap<u64, Vec<u64>>,
    ) -> std::collections::BTreeMap<u64, usize> {
        let mut c = std::collections::BTreeMap::new();
        for set in plan.values() {
            for n in set {
                *c.entry(*n).or_insert(0) += 1;
            }
        }
        c
    }

    #[test]
    fn replica_copies_are_evened_out() {
        // n3 at N=6: node 6 held no replicas, nodes 3-4 held 8-9. Here every
        // region sits on nodes 1-3 and nodes 4-6 are empty.
        let regions: Vec<_> = (1..=30u64)
            .map(|i| {
                plan_in(
                    i,
                    &[1 + i % 3, 1 + (i + 1) % 3],
                    1 + i % 3,
                    &[1, 2, 3, 4, 5, 6],
                )
            })
            .collect();
        let plan = super::plan_placement(&regions, 2, 10);
        let c = copies(&plan);
        assert_eq!(c.values().sum::<usize>(), 60);
        assert!(c.values().all(|&n| (9..=11).contains(&n)), "{c:?}");
        for set in plan.values() {
            assert_eq!(set.len(), 2);
            assert_ne!(set[0], set[1]);
        }
    }

    #[test]
    fn a_balanced_layout_moves_nothing() {
        let regions: Vec<_> = (1..=6u64)
            .map(|i| {
                let a = 1 + (i % 3);
                let b = 1 + ((i + 1) % 3);
                plan_in(i, &[a, b], a, &[1, 2, 3])
            })
            .collect();
        let plan = super::plan_placement(&regions, 2, 10);
        for r in &regions {
            let mut got = plan[&r.region_id].clone();
            got.sort_unstable();
            let mut had = r.current_peers.clone();
            had.sort_unstable();
            assert_eq!(got, had, "region {} moved", r.region_id);
            assert_eq!(plan[&r.region_id][0], r.current_primary);
        }
    }

    #[test]
    fn a_departed_node_is_replaced_and_a_short_set_filled() {
        // Node 9 is no longer a candidate; region 2 is under RF.
        let regions = vec![
            plan_in(1, &[1, 9], 9, &[1, 2, 3]),
            plan_in(2, &[3], 3, &[1, 2, 3]),
        ];
        let plan = super::plan_placement(&regions, 2, 10);
        assert!(!plan[&1].contains(&9));
        assert_eq!(plan[&1].len(), 2);
        assert!(plan[&1].contains(&1), "kept the surviving peer");
        assert_eq!(plan[&2].len(), 2);
        assert!(plan[&2].contains(&3));
    }
}
