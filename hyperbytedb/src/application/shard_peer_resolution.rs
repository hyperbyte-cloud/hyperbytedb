//! Liveness-aware peer selection for sharded scatter (queries, writes, replication).

use std::collections::HashSet;

use crate::domain::cluster::membership::{ClusterMembership, NodeState};
use crate::domain::sharding::ShardRegion;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RegionTargetRole {
    Read,
    /// Aggregate queries must read from the region primary for authoritative totals.
    PrimaryRead,
    Write,
    Replicate,
}

/// Scatter operation kind for metrics labels.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScatterKind {
    Query,
    Metadata,
    Write,
}

impl ScatterKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            ScatterKind::Query => "query",
            ScatterKind::Metadata => "metadata",
            ScatterKind::Write => "write",
        }
    }
}

/// Returns true when `node_id` is present in membership and `Active`.
#[must_use]
pub fn is_active_peer(membership: &ClusterMembership, node_id: u64) -> bool {
    membership
        .get_node(node_id)
        .is_some_and(|n| n.state == NodeState::Active)
}

/// Ordered Active peer candidates for scatter/replication.
///
/// - [`RegionTargetRole::Write`] / [`RegionTargetRole::PrimaryRead`]: the Active
///   primary only. Writes must land on exactly one node per region (the map's
///   primary); falling back to a replica would create two concurrent WAL
///   writers and divergent watermarks. An inactive primary yields no targets —
///   the caller fails fast until failover promotes a new primary.
/// - [`RegionTargetRole::Replicate`]: every Active member of `peers`, including
///   learners in `staging_peers`, so live writes reach a copy-in-progress replica.
/// - [`RegionTargetRole::Read`]: self → primary → other serving peers (learners
///   excluded — they may hold only a prefix of the range).
#[must_use]
pub fn resolve_region_peers(
    region: &ShardRegion,
    self_id: u64,
    membership: &ClusterMembership,
    role: RegionTargetRole,
) -> Vec<u64> {
    // A region that is not serving has no targets, for any role.
    //
    // The location cache already materialises staging regions away, so routing
    // should never see one -- this is the backstop for the paths that resolve
    // from a raw map snapshot instead. Returning nothing makes the caller fail
    // with "no active peers", which is the right outcome: a staging region's
    // peers would answer zero rows *successfully*, and a wrong answer is worse
    // than a failed one.
    if !region.is_serving() {
        return Vec::new();
    }
    if matches!(
        role,
        RegionTargetRole::Write | RegionTargetRole::PrimaryRead
    ) {
        if region.is_serving_peer(region.primary) && is_active_peer(membership, region.primary) {
            return vec![region.primary];
        }
        return Vec::new();
    }

    if role == RegionTargetRole::Replicate {
        let mut out = Vec::with_capacity(region.peers.len());
        for id in &region.peers {
            if *id != self_id && is_active_peer(membership, *id) {
                out.push(*id);
            }
        }
        return out;
    }

    let mut out = Vec::with_capacity(region.peers.len());
    let mut seen = HashSet::with_capacity(region.peers.len());

    let mut push = |id: u64| {
        // `is_serving_peer`, not `peers.contains`: a learner is a member and
        // receives replicated writes, but holds only a prefix of the range
        // until it is promoted, and scatter cannot tell a partial answer from
        // a correct one.
        if seen.insert(id) && region.is_serving_peer(id) && is_active_peer(membership, id) {
            out.push(id);
        }
    };

    push(self_id);
    push(region.primary);
    for id in &region.peers {
        push(*id);
    }

    out
}

/// Active replication targets for a region (all Active peers except self).
#[must_use]
pub fn active_region_peer_targets(
    region: &ShardRegion,
    self_id: u64,
    membership: &ClusterMembership,
) -> Vec<u64> {
    resolve_region_peers(region, self_id, membership, RegionTargetRole::Replicate)
        .into_iter()
        .filter(|id| *id != self_id)
        .collect()
}

/// Address for an Active peer, if known.
#[must_use]
pub fn peer_addr(membership: &ClusterMembership, node_id: u64) -> Option<String> {
    membership.get_node(node_id).map(|n| n.addr.clone())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::cluster::membership::NodeInfo;
    use crate::domain::sharding::ShardEpoch;

    fn node(id: u64, state: NodeState) -> NodeInfo {
        NodeInfo {
            node_id: id,
            addr: format!("127.0.0.1:{id}"),
            state,
            joined_at: 0,
            last_heartbeat: 0,
            needs_sync: false,
            consecutive_misses: 0,
        }
    }

    fn region(primary: u64, peers: &[u64]) -> ShardRegion {
        ShardRegion {
            region_id: 1,
            start: 0,
            end: u64::MAX,
            epoch: ShardEpoch::default(),
            peers: peers.to_vec(),
            primary,
            last_split_at: 0,
            transfer_verified: None,
            transfer_first_seen: None,
            state: Default::default(),
            staged_from: None,
            staging_peers: Vec::new(),
        }
    }

    fn membership(nodes: &[(u64, NodeState)]) -> ClusterMembership {
        let mut m = ClusterMembership::new();
        for &(id, state) in nodes {
            m.add_node(node(id, state));
        }
        m
    }

    #[test]
    fn self_preferred_when_active() {
        let r = region(2, &[1, 2, 3]);
        let m = membership(&[
            (1, NodeState::Active),
            (2, NodeState::Active),
            (3, NodeState::Active),
        ]);
        let peers = resolve_region_peers(&r, 1, &m, RegionTargetRole::Read);
        assert_eq!(peers, vec![1, 2, 3]);
    }

    #[test]
    fn write_primary_down_returns_no_targets() {
        let r = region(1, &[1, 2, 3]);
        let m = membership(&[
            (1, NodeState::Disconnected),
            (2, NodeState::Active),
            (3, NodeState::Active),
        ]);
        // Writes must not fall back to replicas: a second concurrent writer
        // would append to its own WAL and diverge from the region primary.
        let peers = resolve_region_peers(&r, 99, &m, RegionTargetRole::Write);
        assert!(peers.is_empty());
    }

    #[test]
    fn write_targets_only_active_primary() {
        let r = region(1, &[1, 2, 3]);
        let m = membership(&[
            (1, NodeState::Active),
            (2, NodeState::Active),
            (3, NodeState::Active),
        ]);
        let peers = resolve_region_peers(&r, 99, &m, RegionTargetRole::Write);
        assert_eq!(peers, vec![1]);
    }

    #[test]
    fn all_down_returns_empty() {
        let r = region(1, &[1, 2]);
        let m = membership(&[(1, NodeState::Disconnected), (2, NodeState::Leaving)]);
        let peers = resolve_region_peers(&r, 99, &m, RegionTargetRole::Read);
        assert!(peers.is_empty());
    }

    #[test]
    fn active_region_peer_targets_excludes_self() {
        let r = region(1, &[1, 2, 3]);
        let m = membership(&[
            (1, NodeState::Active),
            (2, NodeState::Active),
            (3, NodeState::Disconnected),
        ]);
        let targets = active_region_peer_targets(&r, 1, &m);
        assert_eq!(targets, vec![2]);
    }

    #[test]
    fn primary_read_returns_only_primary_when_active() {
        let r = region(1, &[1, 2, 3]);
        let m = membership(&[
            (1, NodeState::Active),
            (2, NodeState::Active),
            (3, NodeState::Active),
        ]);
        let peers = resolve_region_peers(&r, 2, &m, RegionTargetRole::PrimaryRead);
        assert_eq!(peers, vec![1]);
    }

    /// A region that is not serving has no targets, for any role.
    ///
    /// The location cache materialises staging regions away, so routing should
    /// never see one -- this is the backstop for paths that resolve from a raw
    /// map snapshot. Returning nothing makes the caller fail with "no active
    /// peers"; letting its peers answer would return zero rows *successfully*,
    /// and a wrong answer is worse than a failed one.
    #[test]
    fn a_staging_region_has_no_targets_for_any_role() {
        use crate::domain::sharding::types::RegionState;

        let mut membership = ClusterMembership::default();
        for id in [1, 2, 3] {
            membership.nodes.insert(id, node(id, NodeState::Active));
        }

        let mut r = region(1, &[1, 2, 3]);
        assert!(
            !resolve_region_peers(&r, 2, &membership, RegionTargetRole::Read).is_empty(),
            "a serving region must still resolve targets"
        );

        r.state = RegionState::Staging;
        r.staged_from = Some(9);
        for role in [
            RegionTargetRole::Write,
            RegionTargetRole::PrimaryRead,
            RegionTargetRole::Read,
            RegionTargetRole::Replicate,
        ] {
            assert!(
                resolve_region_peers(&r, 2, &membership, role).is_empty(),
                "a staging region must not be routable for {role:?}"
            );
        }
    }

    /// A learner receives replicated writes but must not answer reads.
    #[test]
    fn a_learner_is_never_a_read_target() {
        let mut membership = ClusterMembership::default();
        for id in [1, 2, 3] {
            membership.nodes.insert(id, node(id, NodeState::Active));
        }

        let mut r = region(1, &[1, 2, 3]);
        r.staging_peers = vec![3];

        for role in [
            RegionTargetRole::Read,
            RegionTargetRole::PrimaryRead,
            RegionTargetRole::Write,
        ] {
            let got = resolve_region_peers(&r, 2, &membership, role);
            assert!(
                !got.contains(&3),
                "learner 3 must not be a {role:?} target, got {got:?}"
            );
        }

        assert!(r.peers.contains(&3));

        r.staging_peers.clear();
        assert!(resolve_region_peers(&r, 2, &membership, RegionTargetRole::Read).contains(&3));
    }

    #[test]
    fn a_learner_is_a_replicate_target() {
        let mut membership = ClusterMembership::default();
        for id in [1, 2, 3] {
            membership.nodes.insert(id, node(id, NodeState::Active));
        }

        let mut r = region(1, &[1, 2, 3]);
        r.staging_peers = vec![3];

        let targets = resolve_region_peers(&r, 1, &membership, RegionTargetRole::Replicate);
        assert!(
            targets.contains(&3),
            "learner 3 must receive replication, got {targets:?}"
        );
        assert!(!targets.contains(&1));
    }

    /// A learner that happens to be the primary must not serve either -- that
    /// would defeat the point entirely.
    #[test]
    fn a_learner_primary_is_not_a_primary_read_target() {
        let mut membership = ClusterMembership::default();
        for id in [1, 2] {
            membership.nodes.insert(id, node(id, NodeState::Active));
        }
        let mut r = region(1, &[1, 2]);
        r.staging_peers = vec![1];
        assert!(
            resolve_region_peers(&r, 2, &membership, RegionTargetRole::PrimaryRead).is_empty(),
            "a learner primary must not answer PrimaryRead"
        );
    }
}
