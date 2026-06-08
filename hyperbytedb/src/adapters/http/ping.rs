use std::sync::Arc;

use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
};

use super::router::AppState;

/// GET/HEAD /ping - returns 204 No Content (liveness only).
pub async fn ping() -> Response {
    StatusCode::NO_CONTENT.into_response()
}

/// GET /health - readiness check.
/// Returns 200 when the node is Active (or standalone).
/// Returns 503 when the node is Syncing, Joining, Draining, or Leaving
/// so Kubernetes removes it from Service endpoints.
pub async fn health(State(state): State<Arc<AppState>>) -> Response {
    use crate::domain::cluster::membership::NodeState;

    if let Some(ref membership) = state.membership {
        let m = membership.read().await;
        if let Some(node) = m.get_node(state.node_id) {
            match node.state {
                NodeState::Active => {}
                other => {
                    let body = format!(
                        r#"{{"status":"warn","message":"node is {:?}, not accepting traffic"}}"#,
                        other
                    );
                    return (
                        StatusCode::SERVICE_UNAVAILABLE,
                        [("Content-Type", "application/json")],
                        body,
                    )
                        .into_response();
                }
            }
        }
    }

    (
        StatusCode::OK,
        [("Content-Type", "application/json")],
        r#"{"status":"pass","message":"ready for queries and writes"}"#.to_string(),
    )
        .into_response()
}

/// GET /health/ready - deep readiness check.
///
/// Runs `SELECT 1` end-to-end through the query port so a load balancer can
/// pull the pod out of rotation if the chDB engine has wedged or failed to
/// initialise. This is more expensive than `/health` (one round-trip into
/// libchdb) so it should be polled at the order of seconds, not millis.
pub async fn health_ready(State(state): State<Arc<AppState>>) -> Response {
    match state.query_port.ping().await {
        Ok(()) => (
            StatusCode::OK,
            [("Content-Type", "application/json")],
            r#"{"status":"pass","message":"chdb engine reachable"}"#.to_string(),
        )
            .into_response(),
        Err(e) => {
            let body = format!(
                r#"{{"status":"fail","message":"chdb ping failed: {}"}}"#,
                e.to_string().replace('"', "\\\"")
            );
            (
                StatusCode::SERVICE_UNAVAILABLE,
                [("Content-Type", "application/json")],
                body,
            )
                .into_response()
        }
    }
}
