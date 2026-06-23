use std::sync::Arc;
use std::time::Duration;

use crate::adapters::http::router::build_router;
use crate::application::cluster::heartbeat;
use crate::application::cluster::hinted_handoff;
use crate::application::cluster::leader_monitor;
use crate::application::cluster::raft_formation;
use crate::application::continuous_query_service::ContinuousQueryService;
use crate::application::retention_service::RetentionService;
use crate::bootstrap::build_services;
use crate::config::{HyperbytedbConfig, RetentionConfig};
use crate::domain::cluster::membership::NodeState;
use crate::ports::metadata::MetadataPort;
use crate::ports::points_sink::PointsSinkPort;
use crate::ports::wal::WalPort;

pub async fn serve(config: HyperbytedbConfig) -> anyhow::Result<()> {
    let bootstrapped = build_services(&config).await?;
    let mut app_state = bootstrapped.app_state;
    let flush_service = bootstrapped.flush_service;
    let cluster = bootstrapped.cluster;
    let peer_query_service_ref = bootstrapped.peer_query_service;

    let peer_client = app_state.peer_client.clone();
    let membership = app_state.membership.clone();
    let _replication_log_arc = app_state.replication_log.clone();

    // Two-phase shutdown
    let (api_shutdown_tx, _api_shutdown_rx) = tokio::sync::watch::channel(false);
    let (service_shutdown_tx, service_shutdown_rx) = tokio::sync::watch::channel(false);

    // Spawn background flush service
    let flush_interval = Duration::from_secs(config.flush.interval_secs);
    let flush_handle = {
        let flush = flush_service.clone();
        let rx = service_shutdown_rx.clone();
        tokio::spawn(async move {
            flush.run(flush_interval, rx).await;
        })
    };

    // Spawn retention enforcement service. Interval and toggle live in
    // [retention] in config.toml — operator-driven via the
    // HyperbytedbCluster CRD's `spec.retention` field. When disabled,
    // we skip spawning entirely so the loop has zero footprint.
    let retention_handle = if config.retention.enabled {
        let retention_interval = config.retention.interval_duration();
        if retention_interval == RetentionConfig::FALLBACK_INTERVAL
            && config.retention.interval.trim() != "60s"
        {
            tracing::warn!(
                configured = %config.retention.interval,
                fallback_secs = retention_interval.as_secs(),
                "retention.interval is invalid or zero, falling back to default"
            );
        }
        let retention_service = Arc::new(RetentionService::new(
            app_state.metadata.clone(),
            app_state.query_port.clone(),
        ));
        let rx = service_shutdown_rx.clone();
        Some(tokio::spawn(async move {
            retention_service.run(retention_interval, rx).await;
        }))
    } else {
        tracing::info!("retention service disabled by config");
        None
    };

    // Spawn uptime gauge updater
    let uptime_handle = {
        let rx = service_shutdown_rx.clone();
        let start_time = std::time::Instant::now();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(15));
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        metrics::gauge!("hyperbytedb_uptime_seconds").set(start_time.elapsed().as_secs_f64());
                    }
                    _ = async {
                        while !*rx.borrow() {
                            rx.clone().changed().await.ok();
                        }
                    } => { break; }
                }
            }
        })
    };

    // Spawn cluster heartbeat logger (every 60s)
    let cluster_heartbeat_handle = if let (Some(pc), Some(m_ref)) = (&peer_client, &membership) {
        let node_addr = pc.node_addr().to_string();
        let membership_ref = m_ref.clone();
        let self_id = config.cluster.node_id;
        let rx = service_shutdown_rx.clone();
        Some(tokio::spawn(async move {
            heartbeat::run_heartbeat_logger(node_addr, self_id, membership_ref, rx).await;
        }))
    } else {
        None
    };

    // Spawn periodic peer heartbeat updater (probes peers and updates last_heartbeat)
    let peer_heartbeat_handle = if let Some(m_ref) = &membership {
        let m = m_ref.clone();
        let self_id = config.cluster.node_id;
        let hb_interval = Duration::from_secs(config.cluster.heartbeat_interval_secs.max(1));
        let probe_timeout = Duration::from_secs(5);
        let rx = service_shutdown_rx.clone();
        Some(tokio::spawn(async move {
            heartbeat::run_heartbeat_updater(self_id, m, hb_interval, probe_timeout, rx).await;
        }))
    } else {
        None
    };

    // Run startup sync and initialize Raft if cluster mode is enabled.
    let raft_instance = if let Some(ref c) = cluster {
        let meta_port: Arc<dyn MetadataPort> = app_state.metadata.clone();
        let wal_port: Arc<dyn WalPort> = app_state.wal.clone();
        let sink_port: Arc<dyn PointsSinkPort> = app_state.points_sink.clone();
        c.run_startup_sync(&config.cluster, &meta_port, &wal_port, Some(sink_port))
            .await?;
        c.start_raft(
            &config.cluster,
            app_state.metadata.clone(),
            app_state.mv_service.clone(),
        )
        .await
    } else {
        None
    };

    // Wire Raft to PeerQueryService for consensus-based schema replication
    if let (Some(raft), Some(pqs)) = (&raft_instance, &peer_query_service_ref) {
        pqs.set_raft(raft.clone());
        if let Some(m) = &membership {
            pqs.set_membership(m.clone());
        }
        tracing::info!("schema mutations will be routed through raft consensus");
    }

    // Spawn hinted handoff drain watcher (drains queued writes when peers reconnect)
    let hinted_handoff_handle = if let (Some(pc), Some(m)) = (&peer_client, &membership) {
        let pc = pc.clone();
        let m = m.clone();
        let self_id = config.cluster.node_id;
        let rx = service_shutdown_rx.clone();
        Some(tokio::spawn(async move {
            hinted_handoff::run_hinted_handoff_watcher(self_id, pc, m, rx).await;
        }))
    } else {
        None
    };

    if config.cluster.anti_entropy_enabled {
        tracing::warn!(
            "cluster.anti_entropy_enabled is set but Merkle anti-entropy was \
             removed in 0.7; this flag is ignored."
        );
    }

    // Keep a handle to drain_service for the shutdown sequence
    let shutdown_drain_service = app_state.drain_service.clone();
    let shutdown_membership = membership.clone();
    let shutdown_node_id = config.cluster.node_id;

    // Set Raft instance on AppState before building the router
    app_state.raft = raft_instance;

    // Continuous queries run on a single node: the Raft leader when cluster
    // mode is enabled, otherwise the sole local instance.
    let cq_handle = {
        let cq_service = Arc::new(ContinuousQueryService::new(
            app_state.metadata.clone(),
            app_state.query.clone(),
            app_state.raft.clone(),
            config.cluster.node_id,
        ));
        let rx = service_shutdown_rx.clone();
        tokio::spawn(async move {
            cq_service.run(Duration::from_secs(10), rx).await;
        })
    };

    let app_state = Arc::new(app_state);
    let app = build_router(app_state.clone());

    // ── Raft cluster formation ───────────────────────────────────────────
    // Spawned before the server starts; the task retries with back-off
    // until the local HTTP server (and peers) are reachable.
    let raft_formation_handle =
        if config.cluster.enabled && config.cluster.node_id == 1 && app_state.raft.is_some() {
            let state_ref = app_state.clone();
            let formation_node_id = config.cluster.node_id;
            let formation_peers = cluster
                .as_ref()
                .map(|c| c.peer_addrs.clone())
                .unwrap_or_default();
            let rx = service_shutdown_rx.clone();
            Some(tokio::spawn(async move {
                raft_formation::run_raft_cluster_formation(
                    state_ref,
                    formation_node_id,
                    formation_peers,
                    rx,
                )
                .await;
            }))
        } else {
            None
        };

    // ── Leader-driven replication lag monitor ─────────────────────────────
    // Only the Raft leader actively monitors followers for data divergence
    // and triggers re-sync on lagging nodes.
    let _leader_monitor_handle = if config.cluster.enabled && app_state.raft.is_some() {
        let state_ref = app_state.clone();
        let monitor_node_id = config.cluster.node_id;
        let rx = service_shutdown_rx.clone();
        Some(tokio::spawn(async move {
            leader_monitor::run_leader_replication_monitor(state_ref, monitor_node_id, rx).await;
        }))
    } else {
        None
    };

    // ── Phase 1: Start HTTP server ──────────────────────────────────────
    // The server will accept requests until the shutdown signal fires.
    let addr = format!("{}:{}", config.server.bind_address, config.server.port);
    if config.server.tls_enabled {
        use axum_server::tls_rustls::RustlsConfig;

        if config.server.tls_cert_path.is_empty() || config.server.tls_key_path.is_empty() {
            anyhow::bail!("tls_enabled is true but tls_cert_path or tls_key_path is not set");
        }
        if !std::path::Path::new(&config.server.tls_cert_path).exists() {
            anyhow::bail!("TLS cert file not found: {}", config.server.tls_cert_path);
        }
        if !std::path::Path::new(&config.server.tls_key_path).exists() {
            anyhow::bail!("TLS key file not found: {}", config.server.tls_key_path);
        }

        let tls_config =
            RustlsConfig::from_pem_file(&config.server.tls_cert_path, &config.server.tls_key_path)
                .await
                .map_err(|e| anyhow::anyhow!("TLS config error: {}", e))?;

        let handle = axum_server::Handle::new();
        let shutdown_handle = handle.clone();
        tokio::spawn(async move {
            tokio::signal::ctrl_c().await.ok();
            tracing::info!("shutdown signal received, stopping API server");
            let _ = api_shutdown_tx.send(true);
            shutdown_handle.graceful_shutdown(Some(Duration::from_secs(10)));
        });

        tracing::info!(addr = %addr, "Hyperbytedb listening (TLS)");
        axum_server::bind_rustls(addr.parse()?, tls_config)
            .handle(handle)
            .serve(app.into_make_service())
            .await?;
    } else {
        let api_rx = _api_shutdown_rx;
        tracing::info!(addr = %addr, "Hyperbytedb listening");
        let listener = tokio::net::TcpListener::bind(&addr).await?;
        axum::serve(listener, app)
            .with_graceful_shutdown(async move {
                tokio::signal::ctrl_c().await.ok();
                tracing::info!("shutdown signal received, stopping API server");
                let _ = api_shutdown_tx.send(true);
                // Also consume api_rx so it is moved into this future
                drop(api_rx);
            })
            .await?;
    }

    // ── Phase 2: API is down — drain if cluster mode ────────────────────
    // The HTTP server has fully stopped; no new external requests will arrive.
    // Run the drain procedure so WAL is flushed and peers acknowledge replication
    // before we tear down background services.
    tracing::info!("API server stopped, beginning shutdown drain");

    if let Some(ref ds) = shutdown_drain_service {
        // Skip drain if it was already triggered by the Kubernetes preStop hook
        let already_drained = if let Some(ref m) = shutdown_membership {
            let guard = m.read().await;
            guard
                .get_node(shutdown_node_id)
                .map(|n| n.state == NodeState::Leaving)
                .unwrap_or(false)
        } else {
            false
        };

        if already_drained {
            tracing::info!("drain already completed (preStop hook), skipping");
        } else {
            tracing::info!("running drain procedure before stopping services");
            if let Err(e) = ds.drain().await {
                tracing::error!(error = %e, "drain procedure failed during shutdown");
            }
        }
    }

    // ── Phase 3: Stop background services ───────────────────────────────
    tracing::info!("stopping background services");
    let _ = service_shutdown_tx.send(true);

    flush_handle.await?;
    cq_handle.await?;
    if let Some(h) = retention_handle {
        h.await?;
    }
    uptime_handle.await?;
    if let Some(h) = cluster_heartbeat_handle {
        h.await?;
    }
    if let Some(h) = peer_heartbeat_handle {
        h.await?;
    }
    if let Some(h) = raft_formation_handle {
        h.abort();
        let _ = h.await;
    }
    if let Some(h) = hinted_handoff_handle {
        h.await?;
    }
    tracing::info!("Hyperbytedb shut down cleanly");

    Ok(())
}
