//! Reverse-proxy axum handler.
//!
//! Routing rules:
//! 1. Pick an `Active` backend (round-robin).
//! 2. If none → wait up to `hold_timeout` for one (this is the "rolling
//!    restart invisible to clients" knob).
//! 3. Forward the request. If it fails with a transient error or the backend
//!    returns 503-with-Draining, retry against another backend up to
//!    `max_retries` times.
//! 4. On final failure surface 503 to the client.

use std::sync::Arc;
use std::time::Instant;

use axum::body::Body;
use axum::extract::{Request, State};
use axum::http::{HeaderName, StatusCode};
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use http_body_util::BodyExt;

use crate::pool::BackendPool;

/// Headers that hop-by-hop semantics (RFC 7230 §6.1) say we must not forward.
const HOP_BY_HOP: &[&str] = &[
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
    // `host` is rewritten when we re-target the request.
    "host",
];

#[derive(Clone)]
pub struct ProxyState {
    pub pool: Arc<BackendPool>,
    pub client: reqwest::Client,
}

impl ProxyState {
    pub fn new(pool: Arc<BackendPool>) -> anyhow::Result<Self> {
        let cfg = pool.config();
        let client = reqwest::Client::builder()
            .timeout(cfg.request_timeout)
            // Per-host pool sized to absorb a moderate burst without
            // re-handshaking; keep_alive_while_idle keeps connections warm
            // across the typical inter-request gap of a Grafana refresh.
            .pool_max_idle_per_host(64)
            .pool_idle_timeout(std::time::Duration::from_secs(90))
            .tcp_nodelay(true)
            .http2_prior_knowledge() // hyperbytedb supports h2; saves the upgrade
            .build()?;
        Ok(Self { pool, client })
    }
}

/// The single fallback handler that captures every URI/method.
pub async fn handle(State(state): State<ProxyState>, req: Request) -> Response {
    let started = Instant::now();
    let cfg = state.pool.config();

    // Buffer the request body once. We may need to send it more than once if
    // a backend returns a transient failure mid-restart.
    //
    // For very large writes this would be a regression — fortunately
    // hyperbytedb's `/write` body cap is `server.max_body_size_bytes` (25 MiB
    // by default), so buffering is bounded and predictable.
    let (parts, body) = req.into_parts();
    let body_bytes = match body.collect().await {
        Ok(c) => c.to_bytes(),
        Err(e) => {
            tracing::warn!(error = %e, "failed to read incoming request body");
            return error_response(StatusCode::BAD_REQUEST, "could not read request body");
        }
    };

    let path_query = parts
        .uri
        .path_and_query()
        .map(|pq| pq.as_str().to_string())
        .unwrap_or_else(|| parts.uri.path().to_string());

    // 1. Wait for at least one active backend (cheap if one already exists).
    let first = match state.pool.wait_for_active(cfg.hold_timeout).await {
        Some(b) => b,
        None => {
            tracing::warn!(
                hold_timeout_secs = cfg.hold_timeout.as_secs(),
                "no active backend available; held until timeout"
            );
            metrics::counter!("hyperbytedb_proxy_no_backend_total").increment(1);
            return error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "no healthy hyperbytedb backend available",
            );
        }
    };

    // 2. Try the first backend, then up to `max_retries` more.
    // Initial `None` here is intentionally overwritten by the Retryable arm
    // before the format! at the bottom of the loop ever reads it.
    #[allow(unused_assignments)]
    let mut last_status: Option<StatusCode> = None;
    #[allow(unused_assignments)]
    let mut last_err: Option<String> = None;
    let mut attempt: u32 = 0;
    let mut current = first;

    loop {
        let _guard = current.enter();
        let url = format!("{}{}", current.origin, path_query);
        tracing::debug!(
            attempt,
            backend = %current.addr,
            method = %parts.method,
            path = %path_query,
            "forwarding"
        );

        let outcome = forward_once(
            &state.client,
            &url,
            &parts.method,
            &parts.headers,
            body_bytes.clone(),
        )
        .await;

        match outcome {
            ForwardOutcome::Ok(resp) => {
                let status = resp.status();
                metrics::counter!(
                    "hyperbytedb_proxy_requests_total",
                    "outcome" => "ok",
                    "status" => status.as_u16().to_string(),
                )
                .increment(1);
                metrics::histogram!("hyperbytedb_proxy_request_duration_seconds")
                    .record(started.elapsed().as_secs_f64());
                return resp;
            }
            ForwardOutcome::Retryable { status, msg } => {
                tracing::info!(
                    attempt,
                    backend = %current.addr,
                    ?status,
                    err = msg.as_deref().unwrap_or("(none)"),
                    "retryable failure, will pick another backend"
                );
                last_status = status;
                last_err = msg;
            }
            ForwardOutcome::Fatal(resp) => {
                metrics::counter!(
                    "hyperbytedb_proxy_requests_total",
                    "outcome" => "fatal",
                )
                .increment(1);
                return resp;
            }
        }

        if attempt >= cfg.max_retries {
            break;
        }
        attempt += 1;

        // Pick a different backend; if there isn't one, hold briefly.
        current = match state.pool.pick_active_excluding(&current).await {
            Some(b) => b,
            None => match state.pool.wait_for_active(cfg.hold_timeout).await {
                Some(b) => b,
                None => break,
            },
        };
    }

    metrics::counter!(
        "hyperbytedb_proxy_requests_total",
        "outcome" => "exhausted",
    )
    .increment(1);
    metrics::histogram!("hyperbytedb_proxy_request_duration_seconds")
        .record(started.elapsed().as_secs_f64());

    let body = format!(
        r#"{{"status":"fail","message":"all backends exhausted (last status {:?}, err {:?})"}}"#,
        last_status.map(|s| s.as_u16()),
        last_err.as_deref().unwrap_or("none")
    );
    (
        StatusCode::SERVICE_UNAVAILABLE,
        [("content-type", "application/json")],
        body,
    )
        .into_response()
}

enum ForwardOutcome {
    Ok(Response),
    /// Pickable for another backend.
    Retryable {
        status: Option<StatusCode>,
        msg: Option<String>,
    },
    /// Don't retry: the failure is the client's fault (4xx) and shouldn't be
    /// blamed on backend health.
    Fatal(Response),
}

async fn forward_once(
    client: &reqwest::Client,
    url: &str,
    method: &http::Method,
    headers: &http::HeaderMap,
    body: Bytes,
) -> ForwardOutcome {
    let mut rb = client.request(method.clone(), url);
    for (name, value) in headers {
        if HOP_BY_HOP
            .iter()
            .any(|h| name.as_str().eq_ignore_ascii_case(h))
        {
            continue;
        }
        rb = rb.header(name, value);
    }
    if !body.is_empty() {
        rb = rb.body(body);
    }

    let resp = match rb.send().await {
        Ok(r) => r,
        Err(e) => {
            // Transport error → backend candidate failed; retry elsewhere.
            return ForwardOutcome::Retryable {
                status: None,
                msg: Some(e.to_string()),
            };
        }
    };

    let status = resp.status();
    let upstream_status =
        StatusCode::from_u16(status.as_u16()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);

    // Buffer body so we can return it as a fixed-size axum body. Streaming
    // would be nicer for large query results, but the simpler form keeps
    // the retry-with-buffered-request path consistent.
    let resp_headers = resp.headers().clone();
    let body_bytes = match resp.bytes().await {
        Ok(b) => b,
        Err(e) => {
            return ForwardOutcome::Retryable {
                status: Some(upstream_status),
                msg: Some(format!("response body read: {e}")),
            };
        }
    };

    // 503 with the well-known draining marker → another backend may serve us.
    if upstream_status == StatusCode::SERVICE_UNAVAILABLE && looks_like_drain(&body_bytes) {
        return ForwardOutcome::Retryable {
            status: Some(upstream_status),
            msg: Some("backend reports draining/syncing".into()),
        };
    }

    // 502/504 from the backend itself = transient infra problem upstream;
    // try another node.
    if matches!(upstream_status.as_u16(), 502 | 504,) {
        return ForwardOutcome::Retryable {
            status: Some(upstream_status),
            msg: Some("backend returned bad-gateway/timeout".into()),
        };
    }

    let mut out = Response::builder().status(upstream_status);
    let out_headers = out.headers_mut().expect("response builder has headers map");
    for (name, value) in resp_headers.iter() {
        if HOP_BY_HOP
            .iter()
            .any(|h| name.as_str().eq_ignore_ascii_case(h))
        {
            continue;
        }
        out_headers.insert(
            HeaderName::from_bytes(name.as_ref()).expect("valid hyper header name"),
            value.clone(),
        );
    }
    let resp = out
        .body(Body::from(body_bytes))
        .expect("axum response body construction is infallible");

    if upstream_status.is_client_error() {
        // 4xx is the client's problem; don't burn retries.
        ForwardOutcome::Fatal(resp)
    } else {
        ForwardOutcome::Ok(resp)
    }
}

/// Loose match against hyperbytedb not-ready JSON envelopes (503 /health).
pub fn looks_like_drain(body: &Bytes) -> bool {
    // Very loose match against the JSON envelopes hyperbytedb uses for
    // not-ready states. Avoids parsing JSON for every response.
    let s = match std::str::from_utf8(body) {
        Ok(s) => s,
        Err(_) => return false,
    };
    s.contains("\"status\":\"warn\"")
        || s.contains("Draining")
        || s.contains("Syncing")
        || s.contains("Joining")
        || s.contains("Leaving")
        || s.contains("draining")
}

fn error_response(status: StatusCode, msg: &str) -> Response {
    let body = format!(
        r#"{{"status":"fail","message":"{}"}}"#,
        msg.replace('"', "\\\"")
    );
    (status, [("content-type", "application/json")], body).into_response()
}
