//! Proxy configuration. Pure env-var driven so it slots cleanly into a
//! Kubernetes Deployment with no extra ConfigMap.

use std::env;
use std::net::IpAddr;
use std::time::Duration;

use anyhow::{Context, Result};

/// Default admin bind when `HYPERBYTEDB_PROXY_ADMIN_LISTEN` is unset.
pub const DEFAULT_ADMIN_LISTEN: &str = "0.0.0.0:8087";

/// All knobs the proxy understands.
#[derive(Debug, Clone)]
pub struct ProxyConfig {
    /// `host:port` for client-facing InfluxDB v1 traffic (`/write`, `/query` only).
    pub listen_addr: String,

    /// `host:port` for proxy-local admin: probes, metrics, `/admin/*`.
    /// Not exposed on the client Service; reachable via pod IP or loopback.
    pub admin_listen_addr: String,

    /// DNS name that resolves to one A record per backend pod (typically a
    /// Kubernetes headless Service: `<cluster>-headless.<ns>.svc.cluster.local`).
    pub backend_service: String,

    /// Port to dial on each backend.
    pub backend_port: u16,

    /// How often to re-resolve `backend_service` and refresh the pool.
    pub discovery_interval: Duration,

    /// How often each backend gets a `/health` probe.
    pub health_interval: Duration,

    /// HTTP path used for backend health probes. Defaults to `/health`; set
    /// to `/health/ready` for the deeper chDB-aware check once that lands.
    pub health_path: String,

    /// Per-probe timeout. Anything slower is treated as Down.
    pub health_timeout: Duration,

    /// Total per-request budget (proxy → backend round-trip). Independent of
    /// `health_timeout` because some queries take many seconds even when
    /// healthy.
    pub request_timeout: Duration,

    /// When no backend is currently routable, the proxy waits up to this long
    /// for one to come back before failing the request with 503. This is the
    /// behaviour that makes rolling restarts invisible to clients.
    pub hold_timeout: Duration,

    /// Maximum times we'll retry a failed request against another backend
    /// (e.g. peer returned 503 mid-restart, or connection got reset).
    pub max_retries: u32,

    /// On graceful shutdown (SIGTERM), how long to keep serving in-flight
    /// requests before forcing the executor to exit.
    pub shutdown_grace: Duration,

    /// Optional pod IP of the proxy itself (set via the Kubernetes Downward
    /// API in production). When present, discovery refuses to add this IP to
    /// the backend pool — defense-in-depth against a label/selector mistake
    /// that would otherwise let the proxy proxy to itself and infinitely
    /// recurse until the pod OOMs.
    pub self_ip: Option<IpAddr>,

    /// When true, the upstream `reqwest` client uses HTTP/2 prior knowledge
    /// (no ALPN upgrade). HyperbyteDB pods speak HTTP/1.1 via `axum::serve`
    /// today, so this defaults to `false`. Enable only when every backend is
    /// known to accept cleartext HTTP/2.
    pub http2_prior_knowledge: bool,
}

impl ProxyConfig {
    /// Build the config from `HYPERBYTEDB_PROXY_*` env vars, falling back to
    /// safe defaults.
    pub fn from_env() -> Result<Self> {
        Ok(Self {
            listen_addr: env_or("HYPERBYTEDB_PROXY_LISTEN", "0.0.0.0:8086"),
            admin_listen_addr: env_or("HYPERBYTEDB_PROXY_ADMIN_LISTEN", DEFAULT_ADMIN_LISTEN),
            backend_service: env_required("HYPERBYTEDB_PROXY_BACKEND_SERVICE")?,
            backend_port: env_u32("HYPERBYTEDB_PROXY_BACKEND_PORT", 8086)? as u16,
            discovery_interval: Duration::from_secs(env_u32(
                "HYPERBYTEDB_PROXY_DISCOVERY_INTERVAL_SECS",
                5,
            )? as u64),
            health_interval: Duration::from_secs(env_u32(
                "HYPERBYTEDB_PROXY_HEALTH_INTERVAL_SECS",
                2,
            )? as u64),
            health_path: env_or("HYPERBYTEDB_PROXY_HEALTH_PATH", "/health"),
            health_timeout: Duration::from_millis(env_u32(
                "HYPERBYTEDB_PROXY_HEALTH_TIMEOUT_MS",
                1500,
            )? as u64),
            request_timeout: Duration::from_secs(env_u32(
                "HYPERBYTEDB_PROXY_REQUEST_TIMEOUT_SECS",
                60,
            )? as u64),
            hold_timeout: Duration::from_secs(
                env_u32("HYPERBYTEDB_PROXY_HOLD_TIMEOUT_SECS", 30)? as u64
            ),
            max_retries: env_u32("HYPERBYTEDB_PROXY_MAX_RETRIES", 2)?,
            shutdown_grace: Duration::from_secs(env_u32(
                "HYPERBYTEDB_PROXY_SHUTDOWN_GRACE_SECS",
                30,
            )? as u64),
            self_ip: env_optional_ip("HYPERBYTEDB_PROXY_SELF_IP")?,
            http2_prior_knowledge: env_bool("HYPERBYTEDB_PROXY_HTTP2_PRIOR_KNOWLEDGE", false)?,
        })
    }
}

fn env_optional_ip(key: &str) -> Result<Option<IpAddr>> {
    match env::var(key) {
        Ok(v) if v.is_empty() => Ok(None),
        Ok(v) => v
            .parse::<IpAddr>()
            .map(Some)
            .with_context(|| format!("env var {key}={v} is not a valid IP")),
        Err(_) => Ok(None),
    }
}

fn env_or(key: &str, default: &str) -> String {
    env::var(key).unwrap_or_else(|_| default.to_string())
}

fn env_required(key: &str) -> Result<String> {
    env::var(key).with_context(|| format!("required env var {key} is not set"))
}

fn env_u32(key: &str, default: u32) -> Result<u32> {
    match env::var(key) {
        Ok(v) => v
            .parse::<u32>()
            .with_context(|| format!("env var {key}={v} is not a valid u32")),
        Err(_) => Ok(default),
    }
}

fn env_bool(key: &str, default: bool) -> Result<bool> {
    match env::var(key) {
        Ok(v) if v.is_empty() => Ok(default),
        Ok(v) => match v.to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" | "on" => Ok(true),
            "0" | "false" | "no" | "off" => Ok(false),
            _ => Err(anyhow::anyhow!("env var {key}={v} is not a valid bool")),
        },
        Err(_) => Ok(default),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_admin_listen_is_separate_from_public() {
        assert_eq!(DEFAULT_ADMIN_LISTEN, "0.0.0.0:8087");
    }

    #[test]
    fn http2_prior_knowledge_defaults_false() {
        let v = env_bool("HYPERBYTEDB_PROXY_HTTP2_PRIOR_KNOWLEDGE_TEST_UNSET", false)
            .expect("parse bool");
        assert!(!v);
    }
}
