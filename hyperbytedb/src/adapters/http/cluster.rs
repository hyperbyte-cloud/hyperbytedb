use std::sync::Arc;

use axum::Json;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use serde::{Deserialize, Serialize};

use super::router::AppState;

pub async fn handle_cluster_metrics(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    if state.peer_client.is_none() {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": "cluster mode not enabled"})),
        );
    }

    if let Some(ref membership) = state.membership {
        let m = membership.read().await;
        let peers: Vec<serde_json::Value> = m
            .nodes
            .values()
            .filter(|n| n.node_id != state.node_id)
            .map(|n| {
                serde_json::json!({
                    "node_id": n.node_id,
                    "addr": n.addr,
                    "state": n.state.to_string(),
                })
            })
            .collect();

        let my_state = m
            .get_node(state.node_id)
            .map(|n| n.state.to_string())
            .unwrap_or_else(|| "unknown".to_string());

        return (
            StatusCode::OK,
            Json(serde_json::json!({
                "mode": "master-master",
                "node_id": state.node_id,
                "node_addr": state.peer_client.as_ref().map(|pc| pc.node_addr()).unwrap_or(""),
                "state": my_state,
                "membership_version": m.version,
                "peers": peers,
                "peer_count": peers.len(),
            })),
        );
    }

    (
        StatusCode::OK,
        Json(serde_json::json!({
            "mode": "master-master",
            "node_addr": state.peer_client.as_ref().map(|pc| pc.node_addr()).unwrap_or(""),
            "peers": [],
            "peer_count": 0,
        })),
    )
}

pub async fn handle_cluster_nodes(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    if state.peer_client.is_none() {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": "cluster mode not enabled"})),
        );
    }

    if let Some(ref membership) = state.membership {
        let m = membership.read().await;
        let nodes: Vec<serde_json::Value> = m
            .nodes
            .values()
            .map(|n| {
                serde_json::json!({
                    "node_id": n.node_id,
                    "addr": n.addr,
                    "state": n.state.to_string(),
                    "self": n.node_id == state.node_id,
                    "last_heartbeat": n.last_heartbeat,
                })
            })
            .collect();

        return (StatusCode::OK, Json(serde_json::json!({"nodes": nodes})));
    }

    let nodes = vec![serde_json::json!({
        "addr": state.peer_client.as_ref().map(|pc| pc.node_addr()).unwrap_or(""),
        "self": true,
    })];
    (StatusCode::OK, Json(serde_json::json!({"nodes": nodes})))
}

/// Returns the current Raft leader's node_id and address (if known).
///
/// Intended for external orchestrators (e.g. the Kubernetes operator) so they
/// can target membership-change RPCs at the leader without having to parse
/// the full Raft metrics blob.
pub async fn handle_cluster_leader(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let raft = match state.raft.as_ref() {
        Some(r) => r,
        None => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": "raft not enabled"})),
            );
        }
    };

    let metrics = raft.metrics().borrow().clone();
    let leader_id = metrics.current_leader;

    let leader_addr = if let (Some(lid), Some(membership)) = (leader_id, &state.membership) {
        let m = membership.read().await;
        m.get_node(lid).map(|n| n.addr.clone())
    } else {
        None
    };

    (
        StatusCode::OK,
        Json(serde_json::json!({
            "leader_id": leader_id,
            "leader_addr": leader_addr,
            "this_node_id": state.node_id,
            "is_leader": leader_id == Some(state.node_id),
            "term": metrics.current_term,
        })),
    )
}

#[derive(Debug, Deserialize)]
pub struct AddNodeRequest {
    pub node_id: u64,
    pub addr: String,
    /// When true, also promote the new learner to a voter via a
    /// `change_membership` call. Defaults to true so a single API call from
    /// the operator fully integrates the node.
    #[serde(default = "default_promote")]
    pub promote: bool,
}

fn default_promote() -> bool {
    true
}

#[derive(Debug, Serialize)]
pub struct AddNodeResponse {
    pub node_id: u64,
    pub addr: String,
    pub added_as_learner: bool,
    pub promoted_to_voter: bool,
    pub leader_id: Option<u64>,
}

/// Operator-facing convenience endpoint to add a node to the cluster.
///
/// Wraps `add_learner` (and optionally `change_membership` to promote the
/// new node to a voter) in a single call. Must be invoked on the Raft
/// leader; on any other node returns 503 with the leader id so the caller
/// can retry against the leader.
pub async fn handle_cluster_add_node(
    State(state): State<Arc<AppState>>,
    Json(req): Json<AddNodeRequest>,
) -> impl IntoResponse {
    let raft = match state.raft.as_ref() {
        Some(r) => r,
        None => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": "raft not enabled"})),
            )
                .into_response();
        }
    };

    let metrics = raft.metrics().borrow().clone();
    let leader_id = metrics.current_leader;
    if leader_id != Some(state.node_id) {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({
                "error": "not raft leader",
                "leader_id": leader_id,
            })),
        )
            .into_response();
    }

    // Drop a node from Raft membership before re-adding it if it lost its log.
    //
    // The leader keeps each member's replication progress for as long as it
    // is a member. A node that comes back under the same id with an empty
    // disk (its PVC was deleted on scale-down) contradicts that progress, and
    // openraft 0.9 does not support it ("log reversion"): the leader sent it
    // logs without a snapshot, and the node crashed on its first apply.
    // Removing it resets its progress; the learner add below then starts it
    // from a snapshot.
    //
    // Only on proof of loss: the node's own log ends before what the leader
    // already replicated to it. The operator also calls add-node for a node
    // the leader merely lost sight of during a restart, and removing a
    // healthy voter then would cost quorum for nothing.
    let voters_now: std::collections::BTreeSet<u64> = metrics
        .membership_config
        .membership()
        .get_joint_config()
        .iter()
        .flat_map(|set| set.iter())
        .copied()
        .collect();
    let in_raft = metrics
        .membership_config
        .membership()
        .nodes()
        .any(|(id, _)| *id == req.node_id);
    let leader_matching = metrics
        .replication
        .as_ref()
        .and_then(|r| r.get(&req.node_id).copied().flatten())
        .map(|l| l.index);
    let reverted = in_raft && log_reverted(leader_matching, fetch_target_raft(&req.addr).await);
    let change = if reverted {
        reset_change(&voters_now, in_raft, req.node_id, state.node_id)
    } else {
        None
    };
    if let Some(change) = change {
        if let Err(e) = raft.change_membership(change, false).await {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({
                    "error": format!("removing node that lost its log before re-add failed: {e}"),
                    "leader_id": leader_id,
                })),
            )
                .into_response();
        }
        tracing::info!(
            node_id = req.node_id,
            leader_matching,
            "add-node: node lost its raft log; removed it so it rejoins from a snapshot"
        );
    }

    let node = openraft::BasicNode::new(req.addr.clone());
    if let Err(e) = raft.add_learner(req.node_id, node, false).await {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({
                "error": format!("add_learner failed: {e}"),
                "leader_id": leader_id,
            })),
        )
            .into_response();
    }

    // Re-admit a node that was previously scaled away.
    //
    // A StatefulSet keeps its PVCs on scale-down, so scaling back up restarts
    // the same node id with its old membership state: `Leaving`. That state is
    // terminal on purpose — heartbeats, sync and the preStop drain must never
    // undo a decommission mid-evacuation — so the node could never leave it,
    // failed readiness forever, and a cluster that had scaled down could not
    // scale back up. The operator calling add-node is an explicit decision to
    // bring the node back, which is the one thing that should be allowed to
    // supersede an earlier removal. Proposed through Raft so every node,
    // including the returning one, applies it.
    let current = match state.membership.as_ref() {
        Some(m) => m.read().await.get_node(req.node_id).map(|n| n.state),
        None => None,
    };
    if let Some(to) = readmit_state(current) {
        let proposal = crate::adapters::cluster::raft::types::ClusterRequest::SetNodeState {
            node_id: req.node_id,
            state: to,
        };
        match raft.client_write(proposal).await {
            Ok(_) => tracing::info!(
                node_id = req.node_id,
                from = ?current,
                to = ?to,
                "add-node: re-admitting previously departed node"
            ),
            Err(e) => tracing::warn!(
                error = %e,
                node_id = req.node_id,
                "add-node: could not re-admit departed node (operator may retry)"
            ),
        }
    }

    let mut promoted = false;
    if req.promote {
        // Build the desired voter set: current voters ∪ {new node}. Read it
        // again: the reset above may have changed it.
        let mut voters: std::collections::BTreeSet<u64> = raft
            .metrics()
            .borrow()
            .membership_config
            .membership()
            .get_joint_config()
            .iter()
            .flat_map(|set| set.iter())
            .copied()
            .collect();
        voters.insert(req.node_id);

        match raft.change_membership(voters, false).await {
            Ok(_) => promoted = true,
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    node_id = req.node_id,
                    "add-node: learner added but promotion to voter failed (operator may retry)"
                );
            }
        }
    }

    let resp = AddNodeResponse {
        node_id: req.node_id,
        addr: req.addr,
        added_as_learner: true,
        promoted_to_voter: promoted,
        leader_id,
    };
    (StatusCode::OK, Json(serde_json::json!(resp))).into_response()
}

/// The state a re-added node should move to, or `None` to leave it alone.
///
/// Only a departed node is touched. A node that is new, or already Active,
/// syncing or draining, is left exactly as it is: draining is a restart that
/// rejoins on its own, and rewriting a live node's state would be a regression.
#[must_use]
pub(crate) fn readmit_state(
    current: Option<crate::domain::cluster::membership::NodeState>,
) -> Option<crate::domain::cluster::membership::NodeState> {
    use crate::domain::cluster::membership::NodeState;
    match current {
        // `Syncing`, not `Joining`: nothing ever promotes `Joining`, so a
        // node re-admitted into it stayed there, holding no placement and
        // serving nothing (seen on n3). `Syncing` is where a new learner
        // starts, and the heartbeat moves it to `Active` once it reports
        // ready.
        Some(NodeState::Decommissioning | NodeState::Leaving) => Some(NodeState::Syncing),
        _ => None,
    }
}

/// What a member reports about its own Raft log.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TargetRaft {
    /// `None` for an empty log.
    pub last_log_index: Option<u64>,
    /// Its Raft core has stopped (openraft state `Shutdown`).
    pub shutdown: bool,
}

/// True when a member has provably lost its log.
///
/// Either its log ends before what the leader already replicated to it,
/// which only a lost disk explains, or its Raft core has died -- what a node
/// that came back empty and was then fed logs without a snapshot looks like
/// (seen on n3: `LogIndexNotFound`, state `Shutdown`). A dead core is not
/// participating, so resetting it costs no quorum.
///
/// `target` is `None` when the node could not be asked; that is never proof.
#[must_use]
pub(crate) fn log_reverted(leader_matching: Option<u64>, target: Option<TargetRaft>) -> bool {
    let Some(t) = target else {
        return false;
    };
    if t.shutdown {
        return true;
    }
    match leader_matching {
        Some(matched) => t.last_log_index.is_none_or(|l| l < matched),
        None => false,
    }
}

/// The node's own Raft log position and liveness, or `None` if unreadable.
async fn fetch_target_raft(addr: &str) -> Option<TargetRaft> {
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(3))
        .build()
        .ok()?;
    let v: serde_json::Value = client
        .get(format!("http://{addr}/cluster/raft/metrics"))
        .send()
        .await
        .ok()?
        .json()
        .await
        .ok()?;
    let last_log_index = match v.get("last_log_index")? {
        serde_json::Value::Null => None,
        n => Some(n.as_u64()?),
    };
    let shutdown = v.get("state").and_then(serde_json::Value::as_str) == Some("Shutdown");
    Some(TargetRaft {
        last_log_index,
        shutdown,
    })
}

/// The membership change that drops a returning node from Raft, or `None`
/// when no reset is needed.
///
/// A voter is removed with `retain = false`, which also drops it as a node; a
/// learner is removed as a node directly. Nothing to do for a node that is
/// not in membership (genuinely new), for the leader itself, or for the only
/// voter.
#[must_use]
pub(crate) fn reset_change(
    voters: &std::collections::BTreeSet<u64>,
    in_raft: bool,
    node_id: u64,
    self_id: u64,
) -> Option<openraft::ChangeMembers<u64, openraft::BasicNode>> {
    use openraft::ChangeMembers;
    if !in_raft || node_id == self_id {
        return None;
    }
    let only = std::collections::BTreeSet::from([node_id]);
    if voters.contains(&node_id) {
        if voters.len() == 1 {
            return None;
        }
        return Some(ChangeMembers::RemoveVoters(only));
    }
    Some(ChangeMembers::RemoveNodes(only))
}

#[derive(Debug, Deserialize)]
pub struct RemoveNodeRequest {
    pub node_id: u64,
}

#[derive(Debug, Serialize)]
pub struct RemoveNodeResponse {
    pub node_id: u64,
    pub removed_from_voters: bool,
    pub leader_id: Option<u64>,
    pub remaining_voters: Vec<u64>,
}

/// Operator-facing convenience endpoint to remove a node from the Raft voter set.
///
/// Must be invoked on the Raft leader after the departing pod has been drained.
/// Returns 503 with the leader id when called on a non-leader node.
pub async fn handle_cluster_remove_node(
    State(state): State<Arc<AppState>>,
    Json(req): Json<RemoveNodeRequest>,
) -> impl IntoResponse {
    let raft = match state.raft.as_ref() {
        Some(r) => r,
        None => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": "raft not enabled"})),
            )
                .into_response();
        }
    };

    let metrics = raft.metrics().borrow().clone();
    let leader_id = metrics.current_leader;
    if leader_id != Some(state.node_id) {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({
                "error": "not raft leader",
                "leader_id": leader_id,
            })),
        )
            .into_response();
    }

    let mut voters: std::collections::BTreeSet<u64> = metrics
        .membership_config
        .membership()
        .get_joint_config()
        .iter()
        .flat_map(|set| set.iter())
        .copied()
        .collect();

    let removed = voters.remove(&req.node_id);
    if !removed {
        let resp = RemoveNodeResponse {
            node_id: req.node_id,
            removed_from_voters: false,
            leader_id,
            remaining_voters: voters.into_iter().collect(),
        };
        return (StatusCode::OK, Json(serde_json::json!(resp))).into_response();
    }

    if voters.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "error": "cannot remove last raft voter",
                "node_id": req.node_id,
            })),
        )
            .into_response();
    }

    match raft.change_membership(voters.clone(), false).await {
        Ok(_) => {
            let resp = RemoveNodeResponse {
                node_id: req.node_id,
                removed_from_voters: true,
                leader_id,
                remaining_voters: voters.into_iter().collect(),
            };
            (StatusCode::OK, Json(serde_json::json!(resp))).into_response()
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({
                "error": format!("change_membership failed: {e}"),
                "leader_id": leader_id,
            })),
        )
            .into_response(),
    }
}

#[cfg(test)]
mod log_reverted_tests {
    use super::{TargetRaft, log_reverted};

    fn at(last: Option<u64>) -> Option<TargetRaft> {
        Some(TargetRaft {
            last_log_index: last,
            shutdown: false,
        })
    }

    #[test]
    fn an_empty_or_shorter_log_than_the_leader_replicated_is_reverted() {
        // n3: nodes 4-6 came back with empty disks; the leader had them at ~37k.
        assert!(log_reverted(Some(37_000), at(None)));
        assert!(log_reverted(Some(37_000), at(Some(36_000))));
    }

    #[test]
    fn a_dead_raft_core_is_reverted_whatever_its_log_says() {
        // What n3 actually showed by the time add-node ran: the leader had
        // already fed the empty node logs, so its log was not behind, but
        // its core had died on apply.
        let dead = Some(TargetRaft {
            last_log_index: Some(37_197),
            shutdown: true,
        });
        assert!(log_reverted(Some(37_197), dead));
        assert!(log_reverted(None, dead));
    }

    #[test]
    fn a_node_that_kept_its_log_is_not_reverted() {
        // The common case: a pod restarted and the leader briefly lost sight
        // of it. Removing it would cost a voter for nothing.
        assert!(!log_reverted(Some(37_000), at(Some(37_000))));
        assert!(!log_reverted(Some(37_000), at(Some(38_000))));
    }

    #[test]
    fn no_evidence_is_not_proof() {
        // Unreachable node, or a leader with no progress for it.
        assert!(!log_reverted(Some(37_000), None));
        assert!(!log_reverted(None, at(None)));
        assert!(!log_reverted(None, None));
    }
}

#[cfg(test)]
mod reset_change_tests {
    use super::reset_change;
    use openraft::ChangeMembers;
    use std::collections::BTreeSet;

    fn set(ids: &[u64]) -> BTreeSet<u64> {
        ids.iter().copied().collect()
    }

    #[test]
    fn a_returning_voter_is_removed_as_a_voter() {
        // Scale 6 -> 3 -> 6 left node 6 a voter; it came back with an empty
        // disk under the same id.
        let got = reset_change(&set(&[1, 2, 3, 4, 5, 6]), true, 6, 2);
        assert_eq!(got, Some(ChangeMembers::RemoveVoters(set(&[6]))));
    }

    #[test]
    fn a_returning_learner_is_removed_as_a_node() {
        // Not a voter, but the leader holds its progress all the same.
        // RemoveVoters would leave it in place.
        let got = reset_change(&set(&[1, 2, 3]), true, 4, 1);
        assert_eq!(got, Some(ChangeMembers::RemoveNodes(set(&[4]))));
    }

    #[test]
    fn a_new_node_the_leader_or_the_last_voter_is_left_alone() {
        assert_eq!(reset_change(&set(&[1, 2, 3]), false, 4, 1), None);
        assert_eq!(reset_change(&set(&[1, 2, 3]), true, 1, 1), None);
        assert_eq!(reset_change(&set(&[2]), true, 2, 1), None);
    }
}

#[cfg(test)]
mod readmit_tests {
    use super::readmit_state;
    use crate::domain::cluster::membership::NodeState;

    /// D0: a node scaled away and brought back must be re-admitted.
    ///
    /// The StatefulSet keeps the PVC on scale-down, so the pod returns with its
    /// persisted `Leaving` state — terminal by design — and failed readiness
    /// forever. An explicit re-add is the one signal allowed to supersede it.
    #[test]
    fn a_departed_node_is_readmitted_as_syncing() {
        assert_eq!(
            readmit_state(Some(NodeState::Leaving)),
            Some(NodeState::Syncing)
        );
        assert_eq!(
            readmit_state(Some(NodeState::Decommissioning)),
            Some(NodeState::Syncing)
        );
    }

    /// The seam that broke on n3: the re-admit state must be one the heartbeat
    /// actually promotes once the node reports itself active. `Joining` was
    /// not, and two nodes sat in it indefinitely.
    #[test]
    fn a_readmitted_node_reaches_active_through_the_heartbeat() {
        use crate::application::cluster::heartbeat::{
            HealthBody, ProbeSignal, decide_transition_with,
        };
        for departed in [NodeState::Leaving, NodeState::Decommissioning] {
            let readmitted = readmit_state(Some(departed)).expect("re-admitted");
            let signal = ProbeSignal::Response {
                health_ok: true,
                body: Some(HealthBody {
                    state: Some(NodeState::Active),
                    needs_sync: Some(true),
                }),
            };
            assert_eq!(
                decide_transition_with(readmitted, &signal, 0, 5, false),
                Some(NodeState::Active),
                "{departed:?} -> {readmitted:?} must reach Active"
            );
        }
    }

    /// Everything else is left exactly as it is. Rewriting a live node's state
    /// on add-node would be a regression, and Draining is a restart that
    /// rejoins by itself.
    #[test]
    fn a_live_or_new_node_is_left_alone() {
        for s in [
            NodeState::Active,
            NodeState::Syncing,
            NodeState::Joining,
            NodeState::Draining,
            NodeState::Disconnected,
        ] {
            assert_eq!(readmit_state(Some(s)), None, "{s:?} must not be rewritten");
        }
        assert_eq!(readmit_state(None), None, "an unknown node is simply added");
    }
}
