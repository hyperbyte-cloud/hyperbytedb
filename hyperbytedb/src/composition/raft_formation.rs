use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use crate::adapters::http::router::AppState;
use crate::domain::cluster::membership::{NodeInfo, NodeState};
use crate::ports::peer_http::PeerHttp;
use openraft::BasicNode;
use openraft::RaftMetrics;

/// Voters the formation keeps: the bootstrap node and up to four others;
/// the rest stay learners.
const MAX_VOTERS: usize = 5;

/// The bootstrap node's next step for the peers it has discovered.
#[derive(Debug, Default, PartialEq, Eq)]
struct FormationStep {
    /// Discovered peers not yet in the Raft membership.
    add_learners: Vec<u64>,
    /// The voter set to change to: current voters plus every learner that
    /// has caught up, up to [`MAX_VOTERS`].
    promote_to: Option<BTreeSet<u64>>,
}

/// Plan one pass of cluster formation from the leader's Raft metrics.
///
/// Each learner is promoted as soon as it has caught up, not once every
/// configured peer has: with ordered startup (a StatefulSet's OrderedReady)
/// the next pod starts only when this one is Ready, and a learner is not
/// Ready, so waiting for all of them never finished. A peer is proposed as
/// a learner only once: re-proposing every pass appended a membership entry
/// just before the catch-up check, so no learner was ever caught up.
fn plan_formation(
    metrics: &RaftMetrics<u64, BasicNode>,
    discovered: &BTreeSet<u64>,
    leader_id: u64,
) -> FormationStep {
    let membership = metrics.membership_config.membership();
    let members: BTreeSet<u64> = membership.nodes().map(|(id, _)| *id).collect();
    let voters: BTreeSet<u64> = membership
        .get_joint_config()
        .iter()
        .flat_map(|set| set.iter())
        .copied()
        .collect();
    let add_learners: Vec<u64> = discovered
        .iter()
        .copied()
        .filter(|id| *id != leader_id && !members.contains(id))
        .collect();
    if !add_learners.is_empty() || voters.len() >= MAX_VOTERS {
        return FormationStep {
            add_learners,
            promote_to: None,
        };
    }
    let target_index = metrics.last_log_index.unwrap_or(0);
    let caught_up = |id: &u64| match metrics.replication.as_ref().and_then(|r| r.get(id)) {
        Some(Some(matched)) => matched.index >= target_index,
        Some(None) => target_index == 0,
        None => false,
    };
    let ready: Vec<u64> = members
        .iter()
        .copied()
        .filter(|id| !voters.contains(id) && discovered.contains(id) && caught_up(id))
        .take(MAX_VOTERS - voters.len())
        .collect();
    if ready.is_empty() {
        return FormationStep::default();
    }
    let mut promote_to = voters;
    promote_to.extend(ready);
    FormationStep {
        add_learners: Vec::new(),
        promote_to: Some(promote_to),
    }
}

/// Background task that forms the Raft cluster from the static peer list.
///
/// **Bootstrap node (node_id == 1)**:
///   Probes each configured peer address to discover its node_id, adds it as
///   a Raft learner, then promotes all learners to voters once the expected
///   cluster size is reached.
///
/// **Non-bootstrap nodes**:
///   No-op — they wait for the leader to discover and add them via Raft.
///   Membership changes propagate automatically via `sync_raft_membership_to_shared`.
pub async fn run_raft_cluster_formation(
    state: Arc<AppState>,
    node_id: u64,
    peer_addrs: Vec<String>,
    shutdown_rx: tokio::sync::watch::Receiver<bool>,
) {
    let raft = match &state.raft {
        Some(r) => r.clone(),
        None => return,
    };

    let auth: &dyn PeerHttp = &state.cluster_auth;
    let client = auth.build(auth.client_builder().timeout(Duration::from_secs(5)));

    // Give the local HTTP server a moment to start
    tokio::time::sleep(Duration::from_secs(1)).await;

    // When no static peers are configured the cluster is operator-driven:
    // Raft is initialized as a single-node group and the operator (or any
    // external controller) is responsible for calling /cluster/raft/add-learner
    // and /cluster/raft/change-membership as new pods become reachable.
    if peer_addrs.is_empty() {
        tracing::info!(
            node_id = node_id,
            "no static peers configured; raft initialized as single-node, awaiting API-driven membership changes"
        );
        return;
    }

    let expected_size = 1 + peer_addrs.len();

    // Discover peers, add each as a Raft learner, and promote each learner
    // once it has caught up ([`plan_formation`]). Learners are proposed
    // with blocking=false so an unreachable one (its HTTP server may still
    // be starting) does not hang the loop.
    for attempt in 1u32.. {
        if *shutdown_rx.borrow() {
            return;
        }

        let mut discovered_ids: BTreeSet<u64> = BTreeSet::new();
        discovered_ids.insert(node_id);
        let mut discovered_addrs = std::collections::HashMap::new();
        for peer_addr in &peer_addrs {
            let url = format!("http://{}/cluster/metrics", peer_addr);
            let probe = async {
                let resp = client.get(&url).send().await.ok()?;
                let v: serde_json::Value = resp.json().await.ok()?;
                v["node_id"].as_u64()
            };
            if let Some(peer_id) = probe.await {
                discovered_ids.insert(peer_id);
                discovered_addrs.insert(peer_id, peer_addr.clone());

                // Update SharedMembership so the data plane can see this peer
                if let Some(ref m) = state.membership {
                    let mut guard = m.write().await;
                    if guard.get_node(peer_id).is_none() {
                        let now = chrono::Utc::now().timestamp();
                        guard.add_node(NodeInfo {
                            node_id: peer_id,
                            addr: peer_addr.clone(),
                            state: NodeState::Active,
                            joined_at: now,
                            last_heartbeat: now,
                            needs_sync: false,
                            consecutive_misses: 0,
                        });
                    }
                }
            }
        }

        let metrics = raft.metrics().borrow().clone();
        let step = plan_formation(&metrics, &discovered_ids, node_id);
        for peer_id in &step.add_learners {
            let Some(peer_addr) = discovered_addrs.get(peer_id) else {
                continue;
            };
            let node = openraft::BasicNode::new(peer_addr.clone());
            match raft.add_learner(*peer_id, node, false).await {
                Ok(_) => tracing::info!(peer_id, peer_addr = %peer_addr, "raft learner added"),
                Err(e) => tracing::warn!(
                    peer_id,
                    error = %e,
                    attempt,
                    "add_learner failed (change in progress?)"
                ),
            }
        }
        if let Some(voters) = step.promote_to {
            match raft.change_membership(voters.clone(), false).await {
                Ok(_) => tracing::info!(voters = ?voters, "raft learners promoted to voters"),
                Err(e) => {
                    tracing::debug!(error = %e, attempt, "membership promotion not ready yet")
                }
            }
        }

        let metrics = raft.metrics().borrow().clone();
        let membership = metrics.membership_config.membership();
        let members = membership.nodes().count();
        let voters = membership
            .get_joint_config()
            .iter()
            .flat_map(|set| set.iter())
            .collect::<BTreeSet<_>>()
            .len();
        if members >= expected_size && voters >= expected_size.min(MAX_VOTERS) {
            tracing::info!(voters, members, "raft cluster fully formed");
            break;
        }

        let backoff = Duration::from_secs(if attempt < 10 { 2 } else { 5 });
        tokio::time::sleep(backoff).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use openraft::{CommittedLeaderId, LogId, Membership, StoredMembership};
    use std::collections::{BTreeMap, BTreeSet};

    fn ids(v: &[u64]) -> BTreeSet<u64> {
        v.iter().copied().collect()
    }

    /// Leader 1's metrics: `voters`, `learners`, a log ending at `last`, and
    /// each follower's matched index.
    fn metrics(
        voters: &[u64],
        learners: &[u64],
        last: u64,
        matched: &[(u64, u64)],
    ) -> RaftMetrics<u64, BasicNode> {
        let mut nodes = BTreeMap::new();
        for id in voters.iter().chain(learners) {
            nodes.insert(*id, BasicNode::new(format!("n{id}")));
        }
        let mut m = RaftMetrics::new_initial(1);
        m.membership_config = std::sync::Arc::new(StoredMembership::new(
            None,
            Membership::new(vec![ids(voters)], nodes),
        ));
        m.last_log_index = Some(last);
        let log_id = |index| LogId::new(CommittedLeaderId::new(1, 1), index);
        m.replication = Some(
            matched
                .iter()
                .map(|(id, index)| (*id, Some(log_id(*index))))
                .collect(),
        );
        m
    }

    /// Node 2 joined alone (node 3 not started, as OrderedReady does): it is
    /// promoted once caught up, without waiting for node 3.
    #[test]
    fn a_caught_up_learner_is_promoted_without_waiting_for_every_peer() {
        let m = metrics(&[1], &[2], 7, &[(2, 7)]);
        let step = plan_formation(&m, &ids(&[1, 2]), 1);
        assert_eq!(step.add_learners, Vec::<u64>::new());
        assert_eq!(step.promote_to, Some(ids(&[1, 2])));
    }

    #[test]
    fn a_lagging_learner_waits() {
        let m = metrics(&[1], &[2], 7, &[(2, 6)]);
        assert_eq!(
            plan_formation(&m, &ids(&[1, 2]), 1),
            FormationStep::default()
        );
    }

    /// A learner already in the membership is not proposed again: that
    /// appended an entry each pass, one past what any learner had matched.
    #[test]
    fn a_known_learner_is_not_added_again() {
        let m = metrics(&[1], &[2], 7, &[(2, 7)]);
        let step = plan_formation(&m, &ids(&[1, 2, 3]), 1);
        assert_eq!(step.add_learners, vec![3], "only the new peer");
        assert_eq!(step.promote_to, None, "promotion waits for the add");
    }

    #[test]
    fn voters_stop_at_five() {
        let m = metrics(&[1, 2, 3, 4], &[5, 6], 9, &[(5, 9), (6, 9)]);
        let step = plan_formation(&m, &ids(&[1, 2, 3, 4, 5, 6]), 1);
        assert_eq!(step.promote_to, Some(ids(&[1, 2, 3, 4, 5])));
        let m = metrics(&[1, 2, 3, 4, 5], &[6], 9, &[(6, 9)]);
        assert_eq!(
            plan_formation(&m, &ids(&[1, 2, 3, 4, 5, 6]), 1),
            FormationStep::default()
        );
    }
}
