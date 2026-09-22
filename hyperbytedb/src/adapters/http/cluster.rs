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
        // Build the desired voter set: current voters ∪ {new node}.
        let mut voters: std::collections::BTreeSet<u64> = metrics
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
        Some(NodeState::Decommissioning | NodeState::Leaving) => Some(NodeState::Joining),
        _ => None,
    }
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
mod readmit_tests {
    use super::readmit_state;
    use crate::domain::cluster::membership::NodeState;

    /// D0: a node scaled away and brought back must be re-admitted.
    ///
    /// The StatefulSet keeps the PVC on scale-down, so the pod returns with its
    /// persisted `Leaving` state — terminal by design — and failed readiness
    /// forever. An explicit re-add is the one signal allowed to supersede it.
    #[test]
    fn a_departed_node_is_readmitted_as_joining() {
        assert_eq!(
            readmit_state(Some(NodeState::Leaving)),
            Some(NodeState::Joining)
        );
        assert_eq!(
            readmit_state(Some(NodeState::Decommissioning)),
            Some(NodeState::Joining)
        );
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
