use std::sync::Arc;

use axum::{
    Json,
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde::Deserialize;

use super::auth_middleware::AuthenticatedUser;
use super::router::AppState;

#[derive(Debug, Deserialize)]
pub struct ChdbQueryBody {
    pub q: String,
}

/// Execute raw ClickHouse SQL against the local chDB engine.
///
/// Requires admin credentials when auth is enabled. Intended for debugging
/// schema, materialized views, and physical table layout — not for application
/// queries (use `/query` and TimeseriesQL instead).
pub async fn handle_chdb(
    State(state): State<Arc<AppState>>,
    auth_user: Option<axum::Extension<AuthenticatedUser>>,
    Json(body): Json<ChdbQueryBody>,
) -> Response {
    if state.auth_enabled {
        match auth_user {
            Some(axum::Extension(user)) if user.user.admin => {}
            _ => {
                return (
                    StatusCode::FORBIDDEN,
                    "admin privileges required for chDB queries",
                )
                    .into_response();
            }
        }
    }

    let q = body.q.trim();
    if q.is_empty() {
        return (StatusCode::BAD_REQUEST, "query body `q` is required").into_response();
    }

    match state.query_port.execute_sql(q).await {
        Ok(raw) => (StatusCode::OK, raw).into_response(),
        Err(e) => (StatusCode::BAD_REQUEST, e.to_string()).into_response(),
    }
}
