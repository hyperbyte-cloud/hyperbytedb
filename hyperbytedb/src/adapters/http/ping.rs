use std::sync::Arc;

use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
};

use super::router::AppState;
use crate::domain::cluster::membership::NodeState;

/// GET/HEAD /ping - returns 204 No Content (liveness only).
pub async fn ping() -> Response {
    StatusCode::NO_CONTENT.into_response()
}

fn wal_batcher_healthy(state: &AppState) -> bool {
    state
        .wal_batcher_alive
        .as_ref()
        .is_none_or(|alive| alive.load(std::sync::atomic::Ordering::SeqCst))
}

fn disk_healthy(state: &AppState) -> bool {
    state
        .disk_read_only
        .as_ref()
        .is_none_or(|ro| !ro.load(std::sync::atomic::Ordering::SeqCst))
}

/// Self-reported readiness carried on every `/health` body.
///
/// Cluster peers parse these fields to drive membership transitions
/// ("self-truth" model): `state` mirrors this node's own record and
/// `needs_sync` flags a startup sync that failed. Fields are omitted when no
/// self-record exists (standalone node / pre-bootstrap), which peers treat as
/// "no inference".
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct SelfReadiness {
    pub state: Option<NodeState>,
    pub needs_sync: Option<bool>,
}

fn health_body(status: &str, message: &str, readiness: SelfReadiness) -> String {
    let mut body = serde_json::json!({
        "status": status,
        "message": message,
    });
    if let Some(state) = readiness.state {
        body["state"] = serde_json::Value::String(state.to_string());
    }
    if let Some(needs_sync) = readiness.needs_sync {
        body["needs_sync"] = serde_json::Value::Bool(needs_sync);
    }
    body.to_string()
}

fn json_response(code: StatusCode, body: String) -> Response {
    (code, [("Content-Type", "application/json")], body).into_response()
}

/// GET /health - readiness check.
/// Returns 200 when the node is Active (or standalone).
/// Returns 503 when the node is Syncing, Joining, Draining, or Leaving
/// so Kubernetes removes it from Service endpoints.
///
/// Every response body carries the machine-readable `{state, needs_sync}`
/// self-readiness fields used by cluster peers for membership convergence.
pub async fn health(State(state): State<Arc<AppState>>) -> Response {
    let readiness = self_readiness(&state).await;

    if let Some(node_state) = readiness.state
        && node_state != NodeState::Active
    {
        return json_response(
            StatusCode::SERVICE_UNAVAILABLE,
            health_body(
                "warn",
                &format!("node is {node_state:?}, not accepting traffic"),
                readiness,
            ),
        );
    }

    if !wal_batcher_healthy(&state) {
        return json_response(
            StatusCode::SERVICE_UNAVAILABLE,
            health_body("fail", "WAL batcher writer unavailable", readiness),
        );
    }

    if !disk_healthy(&state) {
        return json_response(
            StatusCode::SERVICE_UNAVAILABLE,
            health_body("fail", "disk read-only due to low free space", readiness),
        );
    }

    json_response(
        StatusCode::OK,
        health_body("pass", "ready for queries and writes", readiness),
    )
}

async fn self_readiness(state: &AppState) -> SelfReadiness {
    let Some(ref membership) = state.membership else {
        return SelfReadiness::default();
    };
    let m = membership.read().await;
    match m.get_node(state.node_id) {
        Some(node) => SelfReadiness {
            state: Some(node.state),
            needs_sync: Some(node.needs_sync),
        },
        None => SelfReadiness::default(),
    }
}

/// GET /health/ready - deep readiness check.
///
/// Runs `SELECT 1` end-to-end through the query port so a load balancer can
/// pull the pod out of rotation if the chDB engine has wedged or failed to
/// initialise. This is more expensive than `/health` (one round-trip into
/// libchdb) so it should be polled at the order of seconds, not millis.
pub async fn health_ready(State(state): State<Arc<AppState>>) -> Response {
    if !wal_batcher_healthy(&state) {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            [("Content-Type", "application/json")],
            r#"{"status":"fail","message":"WAL batcher writer unavailable"}"#.to_string(),
        )
            .into_response();
    }

    if !disk_healthy(&state) {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            [("Content-Type", "application/json")],
            r#"{"status":"fail","message":"disk read-only due to low free space"}"#.to_string(),
        )
            .into_response();
    }

    match state.query_port.ping().await {
        Ok(()) => (
            StatusCode::OK,
            [("Content-Type", "application/json")],
            r#"{"status":"pass","message":"chDB engine responsive"}"#.to_string(),
        )
            .into_response(),
        Err(e) => (
            StatusCode::SERVICE_UNAVAILABLE,
            [("Content-Type", "application/json")],
            format!(
                r#"{{"status":"fail","message":"chDB engine not ready: {}"}}"#,
                e.to_string().replace('"', "\\\"")
            ),
        )
            .into_response(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn health_body_carries_self_readiness_fields() {
        let body = health_body(
            "warn",
            "node is Syncing, not accepting traffic",
            SelfReadiness {
                state: Some(NodeState::Syncing),
                needs_sync: Some(true),
            },
        );
        let v: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(v["status"], "warn");
        assert_eq!(v["state"], "syncing");
        assert_eq!(v["needs_sync"], true);
    }

    #[test]
    fn health_body_omits_fields_without_self_record() {
        let body = health_body("pass", "ready", SelfReadiness::default());
        let v: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(v["status"], "pass");
        assert!(v.get("state").is_none(), "no self-record ⇒ no state field");
        assert!(v.get("needs_sync").is_none());
    }

    #[test]
    fn health_body_infra_failure_still_carries_state() {
        // Wedged WAL on an otherwise Active node: peers must still be able to
        // see membership truth through the 503.
        let body = health_body(
            "fail",
            "WAL batcher writer unavailable",
            SelfReadiness {
                state: Some(NodeState::Active),
                needs_sync: Some(false),
            },
        );
        let v: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(v["status"], "fail");
        assert_eq!(v["state"], "active");
        assert_eq!(v["needs_sync"], false);
    }
}
