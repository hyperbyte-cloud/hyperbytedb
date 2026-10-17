use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use axum::http::StatusCode;
use axum::{
    Router,
    extract::DefaultBodyLimit,
    middleware,
    routing::{get, post},
};
use tower_http::{timeout::TimeoutLayer, trace::TraceLayer};

use crate::adapters::cluster::peer_client::PeerClient;
use crate::adapters::cluster::raft::HyperbytedbRaft;
use crate::adapters::cluster::replication_log::ReplicationLog;
use crate::application::cluster::drain::DrainService;
use crate::application::ingest_metadata::IngestCardinalityLimits;
use crate::application::materialized_view_service::MaterializedViewService;
use crate::application::replication_apply::ReplicationApplyQueue;
use crate::application::shard_routing::ShardRoutingContext;
use crate::application::shard_scheduler::ShardScheduler;
use crate::application::statement_summary::StatementSummary;
use crate::config::ReplicationConfig;
use crate::domain::cluster::membership::SharedMembership;
use crate::domain::sharding::ShardLocationCache;
use crate::ports::{
    auth::AuthPort, ingestion::IngestionPort, metadata::MetadataPort, points_sink::PointsSinkPort,
    query::QueryPort, sharding::ShardMapPort, wal::WalPort,
};

pub use crate::ports::query::QueryService;

use super::{
    auth_middleware, chdb, cluster, metrics, middleware as http_middleware, peer_handlers, ping,
    query, raft_handlers, rate_limit, shard_handlers, statements, write,
};

/// A write admission budget: a semaphore and the size it was created with,
/// which tokio does not expose and the oversize clamp needs.
#[derive(Debug)]
pub struct Admission {
    pub sem: Arc<tokio::sync::Semaphore>,
    capacity: std::sync::atomic::AtomicUsize,
}

impl Admission {
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        Self {
            sem: Arc::new(tokio::sync::Semaphore::new(capacity)),
            capacity: std::sync::atomic::AtomicUsize::new(capacity),
        }
    }

    #[must_use]
    pub fn capacity(&self) -> usize {
        self.capacity.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// `SET max_inflight_writes`. Requests already admitted keep their
    /// permits; a shrink takes them back as they finish.
    pub fn resize(&self, capacity: usize) {
        let capacity = capacity.max(1);
        let was = self
            .capacity
            .swap(capacity, std::sync::atomic::Ordering::Relaxed);
        crate::application::runtime_settings::resize_semaphore(&self.sem, was, capacity);
    }
}

pub struct AppState {
    pub ingestion: Arc<dyn IngestionPort>,
    pub query: Arc<dyn QueryService>,
    /// Raw chDB port; used by `/health/ready` to verify the engine is alive.
    pub query_port: Arc<dyn QueryPort>,
    pub metadata: Arc<dyn MetadataPort>,
    pub wal: Arc<dyn WalPort>,
    pub points_sink: Arc<dyn PointsSinkPort>,
    pub auth: Arc<dyn AuthPort>,
    pub peer_client: Option<Arc<PeerClient>>,
    pub membership: Option<SharedMembership>,
    pub replication_log: Option<Arc<ReplicationLog>>,
    pub drain_service: Option<Arc<DrainService>>,
    pub raft: Option<HyperbytedbRaft>,
    pub auth_enabled: bool,
    /// This node's runtime settings (`SET`, `SHOW SETTINGS`).
    pub runtime_settings: Arc<crate::application::runtime_settings::RuntimeSettings>,
    /// The cluster token peers send and this node accepts on internal routes.
    pub cluster_auth: crate::adapters::cluster::cluster_auth::ClusterAuth,
    /// When false (default), `?u=` / `?p=` query credentials are ignored.
    pub auth_allow_query_param_credentials: bool,
    pub prometheus_handle: Option<metrics_exporter_prometheus::PrometheusHandle>,
    pub statement_summary: Option<Arc<StatementSummary>>,
    /// When true and auth is enabled, `/api/v1/statements` requires credentials.
    pub statement_summary_require_auth: bool,
    pub mv_service: Arc<MaterializedViewService>,
    /// Applies `/internal/replicate` payloads off the HTTP thread (bounded).
    pub replication_apply: Option<Arc<ReplicationApplyQueue>>,
    pub chdb_session_data_path: String,
    pub node_id: u64,
    pub max_body_size_bytes: usize,
    /// HTTP body cap for `/internal/replicate` (resolved from `[cluster]` at startup).
    pub replicate_body_limit_bytes: usize,
    pub max_points_per_request: usize,
    pub request_timeout_secs: u64,
    pub rate_limiter: Option<Arc<rate_limit::EndpointRateLimiters>>,
    /// Bounds client `/write` requests in flight; see
    /// `ServerConfig::max_inflight_writes`. `None` admits everything.
    pub write_admission: Option<Arc<Admission>>,
    /// Same bound for writes other nodes forward here as region primary
    /// (`/internal/shard/write`). Client admission alone left these
    /// unbounded: at N=6 a hot primary took forwards from five entry nodes,
    /// its task queue grew until `/ping` missed the liveness deadline, and
    /// kubelet killed it mid-run.
    pub shard_write_admission: Option<Arc<Admission>>,
    /// Set when WAL group-commit is enabled; `false` after the writer task exits.
    pub wal_batcher_alive: Option<Arc<AtomicBool>>,
    /// Set when `[disk] enabled`; `true` when free space is below readonly threshold.
    pub disk_read_only: Option<Arc<AtomicBool>>,
    pub sharding_enabled: bool,
    pub shard_map: Option<Arc<dyn ShardMapPort>>,
    pub shard_location_cache: Arc<ShardLocationCache>,
    pub shard_routing: Option<Arc<ShardRoutingContext>>,
    pub shard_scheduler: Option<Arc<ShardScheduler>>,
    pub ingest_cardinality: IngestCardinalityLimits,
    /// Shared ingest fast-path cache. Used by both the local ingestion service
    /// and forwarded shard writes so steady-state batches skip metadata I/O.
    pub ingest_schema_cache: Arc<crate::application::ingest_metadata::IngestSchemaCache>,
    pub cluster_replication: ReplicationConfig,
}

pub fn build_router(state: Arc<AppState>) -> Router {
    let auth_state = state.clone();
    let body_limit = state.max_body_size_bytes;
    let replicate_body_limit = state.replicate_body_limit_bytes;
    let timeout_duration = std::time::Duration::from_secs(state.request_timeout_secs);

    let mut statements_router = Router::new().route(
        "/api/v1/statements",
        get(statements::handle_list).delete(statements::handle_reset),
    );
    if state.auth_enabled && state.statement_summary_require_auth {
        statements_router = statements_router.route_layer(axum::middleware::from_fn_with_state(
            auth_state.clone(),
            auth_middleware::auth_layer,
        ));
    }

    let public_router = Router::new()
        .route("/ping", get(ping::ping).head(ping::ping))
        .route("/health", get(ping::health).head(ping::health))
        .route(
            "/health/ready",
            get(ping::health_ready).head(ping::health_ready),
        )
        .route(
            "/write",
            post(write::handle_write)
                .layer(DefaultBodyLimit::max(body_limit))
                // Innermost of the three: a request that fails auth or the
                // rate limit never holds an admission permit.
                .layer(axum::middleware::from_fn_with_state(
                    auth_state.clone(),
                    auth_middleware::write_admission_layer,
                ))
                .layer(axum::middleware::from_fn_with_state(
                    auth_state.clone(),
                    auth_middleware::auth_layer,
                ))
                .layer(axum::middleware::from_fn_with_state(
                    auth_state.clone(),
                    auth_middleware::rate_limit_write_layer,
                )),
        )
        .merge(
            Router::new()
                .route(
                    "/query",
                    get(query::handle_query_get)
                        .post(query::handle_query_post)
                        .layer(DefaultBodyLimit::max(body_limit))
                        .layer(axum::middleware::from_fn_with_state(
                            auth_state.clone(),
                            auth_middleware::auth_layer,
                        ))
                        .layer(axum::middleware::from_fn_with_state(
                            auth_state.clone(),
                            auth_middleware::rate_limit_query_layer,
                        )),
                )
                // gzip when the client asks (Grafana does), as InfluxDB v1
                // does: a 245-series Grafana panel was 11.4 MB of JSON.
                // Fastest level: most of the gain for a fraction of the CPU.
                .layer(
                    tower_http::compression::CompressionLayer::new()
                        .gzip(true)
                        .quality(tower_http::CompressionLevel::Fastest),
                ),
        )
        .route("/metrics", get(metrics::handle_metrics))
        .merge(statements_router)
        .route(
            "/api/v1/chdb",
            post(chdb::handle_chdb).layer(axum::middleware::from_fn_with_state(
                auth_state.clone(),
                auth_middleware::auth_layer,
            )),
        );

    let mut cluster_router = Router::new();

    if state.peer_client.is_some() {
        cluster_router = cluster_router
            .route(
                "/internal/replicate",
                post(peer_handlers::handle_replicate_write)
                    .layer(DefaultBodyLimit::max(replicate_body_limit)),
            )
            .route(
                "/internal/replicate-batch",
                post(peer_handlers::handle_replicate_batch)
                    .layer(DefaultBodyLimit::max(replicate_body_limit)),
            )
            .route(
                "/internal/replicate-mutation",
                post(peer_handlers::handle_replicate_mutation)
                    .layer(DefaultBodyLimit::max(replicate_body_limit)),
            )
            .route("/cluster/metrics", get(cluster::handle_cluster_metrics))
            .route("/cluster/nodes", get(cluster::handle_cluster_nodes))
            .route(
                "/internal/membership",
                get(peer_handlers::handle_get_membership),
            )
            .route(
                "/internal/membership/join",
                post(peer_handlers::handle_join),
            )
            .route(
                "/internal/membership/leave",
                post(peer_handlers::handle_leave),
            )
            .route(
                "/internal/sync/manifest",
                get(peer_handlers::handle_sync_manifest),
            )
            .route(
                "/internal/sync/metadata",
                get(peer_handlers::handle_sync_metadata),
            )
            .route("/internal/sync/wal", get(peer_handlers::handle_sync_wal))
            .route(
                "/internal/sync/trigger",
                post(peer_handlers::handle_sync_trigger),
            )
            .route("/internal/drain", post(peer_handlers::handle_drain))
            .route(
                "/internal/decommission",
                post(peer_handlers::handle_decommission),
            );

        if state.sharding_enabled {
            cluster_router = cluster_router
                .route(
                    "/internal/shard/bootstrap",
                    post(shard_handlers::handle_shard_bootstrap),
                )
                .route(
                    "/internal/shard/write",
                    post(shard_handlers::handle_shard_write)
                        .layer(DefaultBodyLimit::max(replicate_body_limit))
                        .layer(axum::middleware::from_fn_with_state(
                            state.clone(),
                            auth_middleware::shard_write_admission_layer,
                        )),
                )
                .route(
                    "/internal/shard/write-batch",
                    post(shard_handlers::handle_shard_write_batch)
                        .layer(DefaultBodyLimit::max(replicate_body_limit))
                        .layer(axum::middleware::from_fn_with_state(
                            state.clone(),
                            auth_middleware::shard_write_admission_layer,
                        )),
                )
                .route(
                    "/internal/shard/query",
                    post(shard_handlers::handle_shard_query),
                )
                .route(
                    "/internal/shard/mv-backfill",
                    post(shard_handlers::handle_shard_mv_backfill),
                )
                .route(
                    "/internal/shard/heartbeat",
                    post(shard_handlers::handle_shard_heartbeat),
                )
                // Region transfers carry bulk line protocol, sized like
                // replication rather than like a control message. Without an
                // explicit limit these fell back to axum's 2 MiB default and a
                // large region could NEVER move: the receiver answered
                // "413 Failed to buffer the request body: length limit
                // exceeded", the split committed anyway, and the child sat
                // staging while its transfer retried forever.
                .route(
                    "/internal/shard/metadata",
                    post(shard_handlers::handle_shard_metadata),
                )
                .route(
                    "/internal/shard/delete",
                    post(shard_handlers::handle_shard_delete),
                )
                .route("/internal/shard/map", get(shard_handlers::handle_shard_map));
        }
    }

    if state.raft.is_some() {
        cluster_router = cluster_router
            .route("/internal/raft/vote", post(raft_handlers::handle_raft_vote))
            .route(
                "/internal/raft/append",
                post(raft_handlers::handle_raft_append),
            )
            .route(
                "/internal/raft/snapshot",
                post(raft_handlers::handle_raft_snapshot),
            )
            .route(
                "/cluster/raft/metrics",
                get(raft_handlers::handle_raft_metrics),
            )
            .route(
                "/cluster/raft/add-learner",
                post(raft_handlers::handle_add_learner),
            )
            .route(
                "/cluster/raft/change-membership",
                post(raft_handlers::handle_change_membership),
            )
            .route(
                "/cluster/raft/client-write",
                post(raft_handlers::handle_client_write),
            )
            .route("/cluster/leader", get(cluster::handle_cluster_leader))
            .route(
                "/cluster/membership/add-node",
                post(cluster::handle_cluster_add_node),
            )
            .route(
                "/cluster/membership/remove-node",
                post(cluster::handle_cluster_remove_node),
            );
    }

    // Region moves: bulk copies that legitimately run for minutes. They sit
    // outside the router-wide request timeout -- their callers bound them
    // (120s push, 300s rehome). Under it, any region whose copy took over
    // 30s came back 408, the learner never finished staging, and primaries
    // never moved: on n3 one node stayed primary of 26 of 35 regions for 25
    // minutes.
    let mut long_router = Router::new();
    if state.sharding_enabled && state.peer_client.is_some() {
        long_router = long_router
            // Region transfers carry bulk line protocol, sized like
            // replication rather than like a control message. Without an
            // explicit limit these fell back to axum's 2 MiB default and a
            // large region could NEVER move: the receiver answered
            // "413 Failed to buffer the request body: length limit
            // exceeded", the split committed anyway, and the child sat
            // staging while its transfer retried forever.
            .route(
                "/internal/shard/transfer",
                post(shard_handlers::handle_shard_transfer)
                    .layer(DefaultBodyLimit::max(replicate_body_limit)),
            )
            .route(
                "/internal/shard/transfer/raw",
                post(shard_handlers::handle_shard_transfer_raw)
                    .layer(DefaultBodyLimit::max(replicate_body_limit)),
            )
            .route(
                "/internal/shard/rehome",
                post(shard_handlers::handle_shard_rehome),
            )
            // A departing node's view rollups, sent to a node that stays
            // before it leaves; as long as copying its destination tables.
            .route(
                "/internal/mv/handoff",
                post(shard_handlers::handle_mv_handoff),
            )
            .route(
                "/internal/mv/handoff-chunk",
                post(shard_handlers::handle_mv_handoff_chunk)
                    .layer(DefaultBodyLimit::max(replicate_body_limit)),
            )
            .route_layer(axum::middleware::from_fn_with_state(
                state.clone(),
                auth_middleware::internal_auth_layer,
            ));
    }

    let router = if state.peer_client.is_some() || state.raft.is_some() {
        let internal_auth = state.clone();
        let cluster_router = cluster_router.route_layer(axum::middleware::from_fn_with_state(
            internal_auth,
            auth_middleware::internal_auth_layer,
        ));
        public_router.merge(cluster_router)
    } else {
        public_router
    };

    layer_routes(router, long_router, timeout_duration)
        .layer(
            TraceLayer::new_for_http()
                .make_span_with(|request: &axum::http::Request<_>| {
                    tracing::span!(
                        tracing::Level::DEBUG,
                        "request",
                        method = %request.method(),
                        path = %request.uri().path(),
                        version = ?request.version(),
                    )
                })
                .on_failure(log_server_failure),
        )
        .with_state(state)
}

/// Whether a failed response is expected load shedding (503/429), which is
/// logged at debug, rather than a server fault, which stays at error.
fn is_load_shedding(class: &tower_http::classify::ServerErrorsFailureClass) -> bool {
    matches!(
        class,
        tower_http::classify::ServerErrorsFailureClass::StatusCode(c)
            if *c == StatusCode::SERVICE_UNAVAILABLE || *c == StatusCode::TOO_MANY_REQUESTS
    )
}

/// `on_failure` for the HTTP trace layer. At saturation every admission
/// rejection was an ERROR line: ~10k in five minutes on one node, which cost
/// CPU and rotated every other log line (the scheduler's included) away.
fn log_server_failure(
    class: tower_http::classify::ServerErrorsFailureClass,
    latency: std::time::Duration,
    _span: &tracing::Span,
) {
    let latency_ms = latency.as_millis() as u64;
    if is_load_shedding(&class) {
        tracing::debug!(classification = %class, latency_ms, "response shed load");
    } else {
        tracing::error!(classification = %class, latency_ms, "response failed");
    }
}

/// Apply the request timeout to `router` only, then merge `long` (routes
/// bounded by their callers) and add version headers to both.
fn layer_routes<S>(router: Router<S>, long: Router<S>, timeout: std::time::Duration) -> Router<S>
where
    S: Clone + Send + Sync + 'static,
{
    router
        .layer(TimeoutLayer::with_status_code(
            StatusCode::REQUEST_TIMEOUT,
            timeout,
        ))
        .merge(long)
        .layer(middleware::map_response(
            http_middleware::add_version_headers,
        ))
}

/// Routes for the control listener (`cluster.control_port`): liveness and
/// health probes and the Raft RPCs, served on the control runtime so they
/// never wait behind write-path work. The same routes stay on the main
/// listener for peers and probes that still use it.
pub fn build_control_router(state: Arc<AppState>) -> Router {
    let raft_routes = Router::new()
        .route("/internal/raft/vote", post(raft_handlers::handle_raft_vote))
        .route(
            "/internal/raft/append",
            post(raft_handlers::handle_raft_append),
        )
        .route(
            "/internal/raft/snapshot",
            post(raft_handlers::handle_raft_snapshot),
        )
        .route_layer(axum::middleware::from_fn_with_state(
            state.clone(),
            auth_middleware::internal_auth_layer,
        ));
    let router = Router::new()
        .route("/ping", get(ping::ping).head(ping::ping))
        .route("/health", get(ping::health).head(ping::health))
        .merge(raft_routes);
    // On the control listener so it answers even when the node it is
    // diagnosing is saturated.
    #[cfg(feature = "profiling")]
    let router = router.route("/debug/pprof/profile", get(super::pprof::profile));
    router.with_state(state)
}

#[cfg(test)]
mod layer_routes_tests {
    use super::layer_routes;
    use axum::{Router, http::StatusCode, routing::get};
    use std::time::Duration;

    async fn slow() -> &'static str {
        tokio::time::sleep(Duration::from_millis(600)).await;
        "done"
    }

    async fn status_of(app: Router, path: &str) -> StatusCode {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        reqwest::get(format!("http://{addr}{path}"))
            .await
            .unwrap()
            .status()
    }

    /// Region copies run for minutes; under the router-wide timeout every
    /// copy over 30s came back 408 and primaries never moved.
    #[tokio::test]
    async fn long_routes_escape_the_request_timeout_and_others_do_not() {
        let app = layer_routes(
            Router::new().route("/short", get(slow)),
            Router::new().route("/long", get(slow)),
            Duration::from_millis(200),
        );
        assert_eq!(
            status_of(app.clone(), "/short").await,
            StatusCode::REQUEST_TIMEOUT
        );
        assert_eq!(status_of(app, "/long").await, StatusCode::OK);
    }
}

#[cfg(test)]
mod admission_tests {
    use super::Admission;

    #[tokio::test]
    async fn admission_resizes_live() {
        let a = Admission::new(4);
        a.resize(8);
        assert_eq!((a.capacity(), a.sem.available_permits()), (8, 8));
        a.resize(2);
        assert_eq!((a.capacity(), a.sem.available_permits()), (2, 2));
        a.resize(0);
        assert_eq!(a.capacity(), 1, "never closes the door entirely");
    }
}

#[cfg(test)]
mod load_shedding_tests {
    use super::is_load_shedding;
    use axum::http::StatusCode;
    use tower_http::classify::ServerErrorsFailureClass as C;

    #[test]
    fn only_503_and_429_count_as_load_shedding() {
        assert!(is_load_shedding(&C::StatusCode(
            StatusCode::SERVICE_UNAVAILABLE
        )));
        assert!(is_load_shedding(&C::StatusCode(
            StatusCode::TOO_MANY_REQUESTS
        )));
        assert!(!is_load_shedding(&C::StatusCode(
            StatusCode::INTERNAL_SERVER_ERROR
        )));
        assert!(!is_load_shedding(&C::StatusCode(StatusCode::BAD_GATEWAY)));
        assert!(!is_load_shedding(&C::Error("boom".into())));
    }
}
