//! Proxy-local endpoints. These aren't proxied; they answer about the proxy
//! itself.
//!
//! - `GET /healthz`         — liveness, always 200 once the process is up.
//! - `GET /readyz`          — readiness, 200 only when ≥1 backend is `Active`.
//! - `GET /metrics`         — Prometheus exposition.
//! - `GET /admin/backends`  — JSON dump of the current pool, for debugging.
//!
//! Routes are chosen so they can be allowlisted before the catch-all proxy
//! handler, with no risk of colliding with a hyperbytedb path.

use std::sync::Arc;

use axum::Json;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde::Serialize;

use crate::backend::Health;
use crate::pool::BackendPool;

#[derive(Clone)]
pub struct AdminState {
    pub pool: Arc<BackendPool>,
    pub prometheus: Option<metrics_exporter_prometheus::PrometheusHandle>,
}

pub async fn healthz() -> Response {
    (StatusCode::OK, "ok").into_response()
}

pub async fn readyz(State(state): State<AdminState>) -> Response {
    let snap = state.pool.snapshot().await;
    let active = snap.iter().filter(|b| b.health() == Health::Active).count();
    if active > 0 {
        (
            StatusCode::OK,
            [("content-type", "application/json")],
            format!(r#"{{"status":"ready","active_backends":{}}}"#, active),
        )
            .into_response()
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            [("content-type", "application/json")],
            r#"{"status":"unready","active_backends":0}"#.to_string(),
        )
            .into_response()
    }
}

pub async fn metrics_endpoint(State(state): State<AdminState>) -> Response {
    match state.prometheus.as_ref() {
        Some(h) => (
            StatusCode::OK,
            [("content-type", "text/plain; version=0.0.4")],
            h.render(),
        )
            .into_response(),
        None => (StatusCode::NOT_FOUND, "metrics disabled").into_response(),
    }
}

#[derive(Serialize)]
struct BackendInfo {
    addr: String,
    port: u16,
    health: &'static str,
    inflight: usize,
    consecutive_failures: usize,
    last_probe_unix: i64,
}

pub async fn list_backends(State(state): State<AdminState>) -> Response {
    let snap = state.pool.snapshot().await;
    let body: Vec<BackendInfo> = snap
        .iter()
        .map(|b| BackendInfo {
            addr: b.addr.to_string(),
            port: b.port,
            health: b.health().as_str(),
            inflight: b.inflight(),
            consecutive_failures: b.consecutive_failures(),
            last_probe_unix: b.last_probe_unix(),
        })
        .collect();
    Json(body).into_response()
}
