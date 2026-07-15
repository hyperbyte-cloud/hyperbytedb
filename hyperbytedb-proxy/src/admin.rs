//! Proxy-local endpoints. These aren't proxied; they answer about the proxy
//! itself.
//!
//! - `GET /healthz`         — liveness, always 200 once the process is up.
//! - `GET /readyz`          — readiness, 200 only when ≥1 backend is routable (Active and not excluded).
//! - `GET /metrics`         — Prometheus exposition.
//! - `GET /admin/backends`  — JSON dump of the current pool, for debugging.
//!
//! Routes are chosen so they can be allowlisted before the catch-all proxy
//! handler, with no risk of colliding with a hyperbytedb path.

use std::net::IpAddr;
use std::sync::Arc;

use axum::Json;
use axum::extract::{Path, State};
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
    let active = snap
        .iter()
        .filter(|b| b.health() == Health::Active && !b.is_excluded())
        .count();
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

// ---------------------------------------------------------------------------
// Operator-driven backend exclusion endpoints
// ---------------------------------------------------------------------------

#[derive(Serialize)]
struct ExcludeResponse {
    status: &'static str,
    ip: String,
}

/// `POST /admin/backends/{ip}/exclude` — Tell the proxy to stop routing to
/// this backend. Called by the operator before killing a pod during rolling
/// upgrades.
pub async fn exclude_backend(State(state): State<AdminState>, Path(ip): Path<IpAddr>) -> Response {
    match state.pool.exclude_backend(ip).await {
        Ok(true) => (
            StatusCode::OK,
            Json(ExcludeResponse {
                status: "excluded",
                ip: ip.to_string(),
            }),
        )
            .into_response(),
        Ok(false) => (
            StatusCode::CONFLICT,
            Json(ExcludeResponse {
                status: "already_excluded",
                ip: ip.to_string(),
            }),
        )
            .into_response(),
        Err(e) => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"status": "error", "message": e.to_string()})),
        )
            .into_response(),
    }
}

/// `POST /admin/backends/{ip}/include` — Clear the exclusion flag so the
/// proxy may route to this backend again. Called by the operator after the
/// replacement pod is healthy.
pub async fn include_backend(State(state): State<AdminState>, Path(ip): Path<IpAddr>) -> Response {
    let was_excluded = state.pool.include_backend(ip).await;
    let status = if was_excluded {
        "included"
    } else {
        "not_excluded"
    };
    (
        StatusCode::OK,
        Json(ExcludeResponse {
            status,
            ip: ip.to_string(),
        }),
    )
        .into_response()
}

/// `GET /admin/pool` — Full pool status including exclusion flags.
pub async fn pool_status(State(state): State<AdminState>) -> Response {
    let statuses = state.pool.pool_status().await;
    Json(statuses).into_response()
}
