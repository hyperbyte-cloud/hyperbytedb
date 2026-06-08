use axum::{extract::State, http::StatusCode, response::IntoResponse};
use std::sync::Arc;

use super::router::AppState;

pub async fn handle_metrics(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let body = match &state.prometheus_handle {
        Some(handle) => handle.render(),
        None => "# hyperbytedb_up 1\n".to_string(),
    };

    (
        StatusCode::OK,
        [("Content-Type", "text/plain; version=0.0.4")],
        body,
    )
}
