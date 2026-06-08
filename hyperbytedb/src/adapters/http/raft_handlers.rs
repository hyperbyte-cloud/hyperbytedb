use std::collections::BTreeSet;
use std::sync::Arc;

use axum::Json;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;

use openraft::BasicNode;
use openraft::raft::{AppendEntriesRequest, InstallSnapshotRequest, VoteRequest};

use crate::adapters::cluster::raft::TypeConfig;
use crate::adapters::cluster::raft::types::ClusterRequest;

use super::router::AppState;

fn get_raft(
    state: &AppState,
) -> Result<&crate::adapters::cluster::raft::HyperbytedbRaft, (StatusCode, Json<serde_json::Value>)>
{
    state.raft.as_ref().ok_or_else(|| {
        (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": "raft not enabled"})),
        )
    })
}

fn json_ok(value: impl serde::Serialize) -> axum::response::Response {
    match serde_json::to_value(value) {
        Ok(v) => (StatusCode::OK, Json(v)).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": format!("serialization failed: {e}")})),
        )
            .into_response(),
    }
}

/// Handle Raft vote RPC from peers.
pub async fn handle_raft_vote(
    State(state): State<Arc<AppState>>,
    Json(req): Json<VoteRequest<u64>>,
) -> impl IntoResponse {
    let raft = match get_raft(&state) {
        Ok(r) => r,
        Err(e) => return e.into_response(),
    };
    match raft.vote(req).await {
        Ok(resp) => json_ok(resp),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

/// Handle Raft append_entries RPC from leader.
pub async fn handle_raft_append(
    State(state): State<Arc<AppState>>,
    Json(req): Json<AppendEntriesRequest<TypeConfig>>,
) -> impl IntoResponse {
    let raft = match get_raft(&state) {
        Ok(r) => r,
        Err(e) => return e.into_response(),
    };
    match raft.append_entries(req).await {
        Ok(resp) => json_ok(resp),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

/// Handle Raft install_snapshot RPC from leader.
pub async fn handle_raft_snapshot(
    State(state): State<Arc<AppState>>,
    Json(req): Json<InstallSnapshotRequest<TypeConfig>>,
) -> impl IntoResponse {
    let raft = match get_raft(&state) {
        Ok(r) => r,
        Err(e) => return e.into_response(),
    };
    match raft.install_snapshot(req).await {
        Ok(resp) => json_ok(resp),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

/// Return Raft metrics (leader, term, last_log, etc.).
pub async fn handle_raft_metrics(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let raft = match get_raft(&state) {
        Ok(r) => r,
        Err(e) => return e.into_response(),
    };
    let metrics = raft.metrics().borrow().clone();
    json_ok(metrics)
}

/// Add a learner node to the Raft cluster.
#[derive(serde::Deserialize)]
pub struct AddLearnerRequest {
    pub node_id: u64,
    pub addr: String,
}

pub async fn handle_add_learner(
    State(state): State<Arc<AppState>>,
    Json(req): Json<AddLearnerRequest>,
) -> impl IntoResponse {
    let raft = match get_raft(&state) {
        Ok(r) => r,
        Err(e) => return e.into_response(),
    };
    let node = BasicNode::new(req.addr);
    match raft.add_learner(req.node_id, node, false).await {
        Ok(resp) => json_ok(resp),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

/// Change the Raft cluster membership (promote learners to voters).
#[derive(serde::Deserialize)]
pub struct ChangeMembershipRequest {
    pub members: Vec<u64>,
}

pub async fn handle_change_membership(
    State(state): State<Arc<AppState>>,
    Json(req): Json<ChangeMembershipRequest>,
) -> impl IntoResponse {
    let raft = match get_raft(&state) {
        Ok(r) => r,
        Err(e) => return e.into_response(),
    };
    let members: BTreeSet<u64> = req.members.into_iter().collect();
    match raft.change_membership(members, false).await {
        Ok(resp) => json_ok(resp),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

/// Submit a ClusterRequest through Raft consensus.
pub async fn handle_client_write(
    State(state): State<Arc<AppState>>,
    Json(req): Json<ClusterRequest>,
) -> impl IntoResponse {
    let raft = match get_raft(&state) {
        Ok(r) => r,
        Err(e) => return e.into_response(),
    };
    match raft.client_write(req).await {
        Ok(resp) => json_ok(resp.data),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}
