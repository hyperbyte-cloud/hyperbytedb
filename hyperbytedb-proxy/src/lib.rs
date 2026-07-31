//! `hyperbytedb-proxy` — health-aware HTTP reverse proxy for hyperbytedb.
//!
//! Sits between clients (Grafana, Telegraf, anything that speaks the InfluxDB
//! v1 wire) and the hyperbytedb StatefulSet. Inspired by TiProxy in front of
//! TiDB; adapted to hyperbytedb's HTTP-only API.

pub mod admin;
pub mod backend;
pub mod config;
pub mod pool;
pub mod proxy;

use std::sync::Arc;

use anyhow::{Context, Result};
use axum::Router;
use axum::routing::{any, get, post};
use metrics_exporter_prometheus::PrometheusBuilder;
use tokio::net::TcpListener;
use tokio::signal::unix::{SignalKind, signal};
use tokio::sync::watch;
use tower_http::trace::TraceLayer;

use crate::admin::AdminState;
use crate::config::ProxyConfig;
use crate::pool::BackendPool;
use crate::proxy::ProxyState;

pub async fn run() -> Result<()> {
    init_tracing();
    let cfg = ProxyConfig::from_env()?;
    tracing::info!(?cfg, "hyperbytedb-proxy starting");
    if cfg.http2_prior_knowledge {
        tracing::warn!(
            "HYPERBYTEDB_PROXY_HTTP2_PRIOR_KNOWLEDGE=true: upstream client requires \
             cleartext HTTP/2; hyperbytedb pods use HTTP/1.1 unless you have enabled h2 \
             elsewhere"
        );
    } else {
        tracing::info!("upstream client uses HTTP/1.1 with ALPN negotiation (hyperbytedb default)");
    }

    let prometheus_handle = PrometheusBuilder::new()
        .install_recorder()
        .context("install prometheus recorder")?;

    let pool = BackendPool::new(cfg.clone())?;
    // Eagerly seed the pool once so /readyz reflects reality before the first
    // discovery tick.
    {
        let pool = Arc::clone(&pool);
        tokio::spawn(async move { pool.run_discovery().await });
    }
    {
        let pool = Arc::clone(&pool);
        tokio::spawn(async move { pool.run_health().await });
    }

    let proxy_state = ProxyState::new(Arc::clone(&pool))?;
    let admin_state = AdminState {
        pool: Arc::clone(&pool),
        prometheus: Some(prometheus_handle),
    };

    // Public listener: InfluxDB v1 write/query only. Cluster/internal routes on
    // hyperbytedb pods are never reachable through ingress aimed at this port.
    let public_router = Router::new()
        .route("/write", any(proxy::handle))
        .route("/query", any(proxy::handle))
        .fallback(proxy::not_found)
        .with_state(proxy_state)
        .layer(TraceLayer::new_for_http());

    // Admin listener: kubelet probes, Prometheus, operator backend exclusion.
    let admin_router = Router::new()
        .route("/healthz", get(admin::healthz))
        .route("/readyz", get(admin::readyz))
        .route("/metrics", get(admin::metrics_endpoint))
        .route("/admin/backends", get(admin::list_backends))
        .route("/admin/backends/{ip}/exclude", post(admin::exclude_backend))
        .route("/admin/backends/{ip}/include", post(admin::include_backend))
        .route("/admin/pool", get(admin::pool_status))
        .with_state(admin_state);

    let public_listener = TcpListener::bind(&cfg.listen_addr)
        .await
        .with_context(|| format!("bind public listener {}", cfg.listen_addr))?;
    tracing::info!(addr = %cfg.listen_addr, "public listener (write/query only)");

    let admin_listener = TcpListener::bind(&cfg.admin_listen_addr)
        .await
        .with_context(|| format!("bind admin listener {}", cfg.admin_listen_addr))?;
    tracing::info!(addr = %cfg.admin_listen_addr, "admin listener (probes/metrics/admin)");

    let shutdown_grace = cfg.shutdown_grace;
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    tokio::spawn(async move {
        wait_for_shutdown_signal().await;
        tracing::info!(
            grace_secs = shutdown_grace.as_secs(),
            "shutdown signal received; draining in-flight requests"
        );
        let _ = shutdown_tx.send(true);
    });

    let mut public_shutdown = shutdown_rx.clone();
    let pool_for_drain = Arc::clone(&pool);
    let public_serve =
        axum::serve(public_listener, public_router).with_graceful_shutdown(async move {
            let _ = public_shutdown.changed().await;
            drain_inflight(&pool_for_drain, shutdown_grace).await;
        });

    let mut admin_shutdown = shutdown_rx;
    let admin_serve =
        axum::serve(admin_listener, admin_router).with_graceful_shutdown(async move {
            let _ = admin_shutdown.changed().await;
        });

    tokio::try_join!(public_serve, admin_serve)?;

    tracing::info!("proxy shut down cleanly");
    Ok(())
}

async fn wait_for_shutdown_signal() {
    let mut sigterm = signal(SignalKind::terminate()).expect("install SIGTERM handler");
    let mut sigint = signal(SignalKind::interrupt()).expect("install SIGINT handler");
    tokio::select! {
        _ = sigterm.recv() => {}
        _ = sigint.recv() => {}
    }
}

/// Poll aggregate backend inflight until zero or `grace` elapses.
async fn drain_inflight(pool: &BackendPool, grace: std::time::Duration) {
    let deadline = tokio::time::Instant::now() + grace;
    loop {
        let inflight = pool.total_inflight().await;
        if inflight == 0 {
            tracing::info!("all in-flight proxy requests drained");
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            tracing::warn!(
                grace_secs = grace.as_secs(),
                inflight,
                "drain grace expired with requests still in flight"
            );
            break;
        }
        tracing::debug!(inflight, "waiting for in-flight proxy requests to drain");
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
}

fn init_tracing() {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
        tracing_subscriber::EnvFilter::new("info,hyperbytedb_proxy=debug,tower_http=info")
    });
    let json = std::env::var("LOG_FORMAT")
        .map(|v| v == "json")
        .unwrap_or(false);
    if json {
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .json()
            .init();
    } else {
        tracing_subscriber::fmt().with_env_filter(filter).init();
    }
}
