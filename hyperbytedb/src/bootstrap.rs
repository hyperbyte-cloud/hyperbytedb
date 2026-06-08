use std::sync::Arc;

use crate::adapters::auth::MetadataAuthAdapter;
use crate::adapters::chdb::catalog;
use crate::adapters::chdb::native_adapter::ChdbNativeAdapter;
use crate::adapters::chdb::query_adapter::ChdbQueryAdapter;
use crate::adapters::chdb::session::SharedSession;
use crate::adapters::http::router::AppState;
use crate::adapters::metadata::rocksdb_meta::RocksDbMetadata;
use crate::adapters::wal::batching_wal::BatchingWal;
use crate::adapters::wal::rocksdb_wal::RocksDbWal;
use crate::application::cluster::bootstrap::ClusterBootstrap;
use crate::application::cluster::drain::DrainService;
use crate::application::flush_service::FlushServiceImpl;
use crate::application::ingest_metadata::IngestCardinalityLimits;
use crate::application::ingestion_service::IngestionServiceImpl;
use crate::application::query_service::QueryServiceImpl;
use crate::application::replication_apply::ReplicationApplyQueue;
use crate::application::statement_summary::StatementSummary;
use crate::config::HyperbytedbConfig;
use crate::ports::metadata::MetadataPort;
use crate::ports::points_sink::PointsSinkPort;
use crate::ports::query::QueryService;

/// All constructed services returned by [`build_services`].
pub struct BootstrappedApp {
    pub app_state: AppState,
    pub flush_service: Arc<FlushServiceImpl>,
    pub cluster: Option<ClusterBootstrap>,
    pub peer_query_service: Option<Arc<crate::application::peer_query_service::PeerQueryService>>,
    pub prometheus_handle: metrics_exporter_prometheus::PrometheusHandle,
}

/// Construct all adapters and application services from config.
///
/// This is the composition root of the application.  `main.rs` calls
/// this, then runs the HTTP server and background tasks using the
/// returned handles.
pub async fn build_services(config: &HyperbytedbConfig) -> anyhow::Result<BootstrappedApp> {
    let prometheus_handle = {
        let builder = metrics_exporter_prometheus::PrometheusBuilder::new();
        let recorder = builder.build_recorder();
        let handle = recorder.handle();
        // Ignore AlreadySet: in-process restarts (e.g. e2e backup/restore) reuse the process.
        let _ = metrics::set_global_recorder(recorder);
        handle
    };

    tracing::info!("Hyperbytedb v{} starting up", env!("CARGO_PKG_VERSION"));

    let version_label = env!("CARGO_PKG_VERSION").to_string();
    let node_id_label = if config.cluster.enabled {
        config.cluster.node_id.to_string()
    } else {
        "standalone".to_string()
    };
    metrics::gauge!("hyperbytedb_info", "version" => version_label, "node_id" => node_id_label)
        .set(1.0);

    for dir in [
        &config.storage.wal_dir,
        &config.storage.meta_dir,
        &config.chdb.session_data_path,
    ] {
        std::fs::create_dir_all(dir)?;
    }

    if config.cluster.enabled {
        tracing::info!(
            "cluster enabled: replicated durability is via shared WAL/metadata sync; embedded \
             chDB state is rebuilt from the WAL on each peer"
        );
    }

    // -- Infrastructure adapters --

    let raw_wal = Arc::new(RocksDbWal::open(&config.storage.wal_dir)?);
    let wal: Arc<dyn crate::ports::wal::WalPort> = if config.flush.wal_batch_size > 0 {
        tracing::info!(
            batch_size = config.flush.wal_batch_size,
            batch_delay_us = config.flush.wal_batch_delay_us,
            "WAL group-commit enabled"
        );
        BatchingWal::new(
            raw_wal,
            config.flush.wal_batch_size * 4,
            config.flush.wal_batch_size,
            std::time::Duration::from_micros(config.flush.wal_batch_delay_us),
        )
    } else {
        raw_wal
    };
    let metadata = Arc::new(RocksDbMetadata::open(&config.storage.meta_dir)?);
    match metadata.warm_tag_value_counts().await {
        Ok(tag_counters) => {
            tracing::info!(tag_counters, "warmed tag value count cache from metadata")
        }
        Err(e) => tracing::warn!(
            error = %e,
            "failed to warm tag value count cache; counts rebuild on demand"
        ),
    }
    match metadata.warm_series().await {
        Ok(series) => tracing::info!(series, "warmed series dedup cache from metadata"),
        Err(e) => tracing::warn!(
            error = %e,
            "failed to warm series cache; series re-register on demand"
        ),
    }

    if let Err(e) =
        catalog::prepare_cold_start_metadata(std::path::Path::new(&config.chdb.session_data_path))
    {
        tracing::warn!(
            error = %e,
            "failed to prepare chDB metadata before session open"
        );
    }
    let shared_chdb = SharedSession::new_eager(&config.chdb.session_data_path)?;
    match catalog::reload_persisted_tables(&shared_chdb).await {
        Ok(attached) => tracing::info!(
            attached,
            "attached restored chDB tables from on-disk metadata"
        ),
        Err(e) => tracing::warn!(
            error = %e,
            "failed to attach restored chDB tables from on-disk metadata"
        ),
    }
    let chdb_adapter =
        ChdbQueryAdapter::from_shared(shared_chdb.clone(), config.server.max_concurrent_queries);
    if config.chdb.pool_size > 1 {
        tracing::warn!(
            configured = config.chdb.pool_size,
            "chdb.pool_size is deprecated and ignored; libchdb only supports one session per \
             process. Use server.max_concurrent_queries to bound parallelism."
        );
    }
    let chdb: Arc<dyn crate::ports::query::QueryPort> = Arc::new(chdb_adapter);

    let native_sink = ChdbNativeAdapter::with_metadata(shared_chdb.clone(), Some(metadata.clone()));
    match native_sink.warm_schemas_from_metadata().await {
        Ok(tables) => tracing::info!(
            tables,
            "warmed chDB native schema cache from measurement metadata"
        ),
        Err(e) => tracing::warn!(
            error = %e,
            "failed to warm chDB native schema cache from metadata; schemas rebuild on flush"
        ),
    }
    match native_sink.warm_series_from_metadata().await {
        Ok(series) => tracing::info!(series, "warmed chDB native series cache from metadata"),
        Err(e) => tracing::warn!(
            error = %e,
            "failed to warm chDB native series cache from metadata; series re-register on flush"
        ),
    }
    match native_sink.sync_materialized_from_engine().await {
        Ok(tables) => tracing::info!(
            tables,
            "synced chDB native schema materialization flags from engine catalog"
        ),
        Err(e) => tracing::warn!(
            error = %e,
            "failed to sync chDB materialization flags from engine catalog"
        ),
    }
    let points_sink: Arc<dyn PointsSinkPort> = Arc::new(native_sink);

    let auth: Arc<dyn crate::ports::auth::AuthPort> =
        Arc::new(MetadataAuthAdapter::new(metadata.clone()));

    let cluster = if config.cluster.enabled {
        Some(ClusterBootstrap::init(
            &config.cluster,
            config.hinted_handoff.max_hints_per_peer,
        )?)
    } else {
        None
    };

    let peer_client = cluster.as_ref().map(|c| c.peer_client.clone());
    let membership = cluster.as_ref().map(|c| c.membership.clone());
    let replication_log_arc = cluster.as_ref().map(|c| c.replication_log.clone());

    if let Some(ref m) = membership {
        let member = m.read().await;
        metrics::gauge!("hyperbytedb_cluster_peers")
            .set((member.nodes.len().saturating_sub(1)) as f64);
    } else {
        metrics::gauge!("hyperbytedb_cluster_peers").set(0.0);
    }

    let base_query_service: Arc<dyn QueryService> = {
        let mut qs = QueryServiceImpl::new(
            chdb.clone(),
            metadata.clone(),
            wal.clone(),
            config.server.query_timeout_secs,
            points_sink.clone(),
        );
        if let Some(ref pc) = peer_client {
            qs = qs.with_cluster_replication(
                pc.clone(),
                config.cluster.node_id,
                config.cluster.replication.clone(),
            );
        }
        Arc::new(qs)
    };

    let ingest_cardinality = IngestCardinalityLimits {
        max_tag_values_per_measurement: config.cardinality.max_tag_values_per_measurement,
        max_measurements_per_database: config.cardinality.max_measurements_per_database,
    };

    let ingestion_service: Arc<dyn crate::ports::ingestion::IngestionPort> =
        if let Some(ref pc) = peer_client {
            Arc::new(
                crate::application::peer_ingestion_service::PeerIngestionService::with_replication(
                    wal.clone(),
                    metadata.clone(),
                    pc.clone(),
                    config.cluster.node_id,
                    ingest_cardinality,
                    config.cluster.replication.clone(),
                ),
            )
        } else {
            Arc::new(IngestionServiceImpl::new(
                wal.clone(),
                metadata.clone(),
                config.cardinality.max_tag_values_per_measurement,
                config.cardinality.max_measurements_per_database,
            ))
        };

    let replication_apply = if cluster.is_some() {
        Some(ReplicationApplyQueue::new(
            config.cluster.replicate_receiver_queue_depth,
            metadata.clone(),
            wal.clone(),
            ingest_cardinality,
        ))
    } else {
        None
    };

    let peer_query_service_ref = peer_client.as_ref().map(|pc| {
        Arc::new(
            crate::application::peer_query_service::PeerQueryService::new(
                base_query_service.clone(),
                pc.clone(),
            ),
        )
    });

    let query_service: Arc<dyn QueryService> = if let Some(ref pqs) = peer_query_service_ref {
        pqs.clone()
    } else {
        base_query_service
    };

    let flush_service = {
        let mut fs = FlushServiceImpl::new(
            wal.clone(),
            config.flush.max_points_per_batch,
            points_sink.clone(),
        );
        if let Some(ref rl) = replication_log_arc {
            fs = fs.with_replication_log(rl.clone());
        }
        if let Some(ref m) = membership {
            fs = fs
                .with_membership(config.cluster.node_id, m.clone())
                .with_truncate_heartbeat_policy(
                    config.cluster.heartbeat_interval_secs,
                    config.cluster.heartbeat_miss_threshold,
                    config.cluster.replication_truncate_stale_peer_multiplier,
                );
        }
        Arc::new(fs)
    };

    let drain_service = if let (Some(m), Some(rl)) = (&membership, &replication_log_arc) {
        let flush_for_drain: Arc<dyn crate::ports::flush::FlushPort> = flush_service.clone();
        Some(Arc::new(DrainService::new(
            config.cluster.node_id,
            m.clone(),
            flush_for_drain,
            rl.clone(),
            wal.clone(),
        )))
    } else {
        None
    };

    let statement_summary = if config.statement_summary.enabled {
        Some(Arc::new(StatementSummary::new(
            config.statement_summary.max_entries,
        )))
    } else {
        None
    };

    let app_state = AppState {
        ingestion: ingestion_service,
        query: query_service,
        query_port: chdb.clone(),
        metadata: metadata.clone(),
        wal: wal.clone(),
        auth,
        peer_client,
        membership: membership.clone(),
        replication_log: replication_log_arc,
        drain_service,
        raft: None,
        auth_enabled: config.auth.enabled,
        prometheus_handle: Some(prometheus_handle.clone()),
        statement_summary,
        replication_apply,
        chdb_session_data_path: config.chdb.session_data_path.clone(),
        node_id: config.cluster.node_id,
        max_body_size_bytes: config.server.max_body_size_bytes,
        request_timeout_secs: config.server.request_timeout_secs,
        rate_limiter: if config.rate_limit.enabled && config.rate_limit.max_requests_per_second > 0
        {
            Some(Arc::new(tokio::sync::Semaphore::new(
                config.rate_limit.max_requests_per_second as usize,
            )))
        } else {
            None
        },
    };

    Ok(BootstrappedApp {
        app_state,
        flush_service,
        cluster,
        peer_query_service: peer_query_service_ref,
        prometheus_handle,
    })
}
