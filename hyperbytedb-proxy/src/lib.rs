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
use tower_http::trace::TraceLayer;

use crate::admin::AdminState;
use crate::config::ProxyConfig;
use crate::pool::BackendPool;
use crate::proxy::ProxyState;

pub async fn run() -> Result<()> {
    init_tracing();
    let cfg = ProxyConfig::from_env()?;
    tracing::info!(?cfg, "hyperbytedb-proxy starting");

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

    // Admin routes (kubelet probes, metrics, debug). Kept in a separate
    // sub-router with no TraceLayer so a not-yet-warm /readyz returning 503
    // doesn't show up as ERROR in the logs every 2s during startup.
    let admin_router = Router::new()
        .route("/healthz", get(admin::healthz))
        .route("/readyz", get(admin::readyz))
        .route("/metrics", get(admin::metrics_endpoint))
        .route("/admin/backends", get(admin::list_backends))
        .route("/admin/backends/{ip}/exclude", post(admin::exclude_backend))
        .route("/admin/backends/{ip}/include", post(admin::include_backend))
        .route("/admin/pool", get(admin::pool_status))
        .with_state(admin_state);

    // Order matters: admin routes first, then the catch-all proxy fallback.
    // The TraceLayer only wraps the proxy fallback so admin probes stay quiet.
    let app = admin_router.fallback_service(
        Router::new()
            .fallback(any(proxy::handle))
            .with_state(proxy_state)
            .layer(TraceLayer::new_for_http()),
    );

    let listener = TcpListener::bind(&cfg.listen_addr)
        .await
        .with_context(|| format!("bind {}", cfg.listen_addr))?;
    tracing::info!(addr = %cfg.listen_addr, "proxy listening");

    let shutdown_grace = cfg.shutdown_grace;
    let serve = axum::serve(listener, app).with_graceful_shutdown(async move {
        wait_for_shutdown_signal().await;
        tracing::info!(
            grace_secs = shutdown_grace.as_secs(),
            "shutdown signal received; draining"
        );
        tokio::spawn(async move {
            tokio::time::sleep(shutdown_grace).await;
            tracing::warn!(
                grace_secs = shutdown_grace.as_secs(),
                "drain grace expired; forcing exit"
            );
            std::process::exit(0);
        });
    });

    if let Err(e) = serve.await {
        tracing::error!(error = %e, "proxy server error");
        return Err(e.into());
    }
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
