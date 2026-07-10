use std::sync::Arc;

use axum::{
    Router,
    extract::DefaultBodyLimit,
    middleware,
    routing::{get, post},
};
use tower::ServiceBuilder;
use tower_http::trace::TraceLayer;

use crate::adapters::cluster::peer_client::PeerClient;
use crate::adapters::cluster::raft::HyperbytedbRaft;
use crate::adapters::cluster::replication_log::ReplicationLog;
use crate::application::cluster::drain::DrainService;
use crate::application::materialized_view_service::MaterializedViewService;
use crate::application::replication_apply::ReplicationApplyQueue;
use crate::application::statement_summary::StatementSummary;
use crate::domain::cluster::membership::SharedMembership;
use crate::ports::{
    auth::AuthPort, ingestion::IngestionPort, metadata::MetadataPort, points_sink::PointsSinkPort,
    query::QueryPort, wal::WalPort,
};

pub use crate::ports::query::QueryService;

use super::{
    auth_middleware, chdb, cluster, metrics, middleware as http_middleware, peer_handlers, ping,
    query, raft_handlers, rate_limit, statements, write,
};

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
}

pub fn build_router(state: Arc<AppState>) -> Router {
    let auth_state = state.clone();
    let body_limit = state.max_body_size_bytes;
    let replicate_body_limit = state.replicate_body_limit_bytes;
    let _timeout_duration = std::time::Duration::from_secs(state.request_timeout_secs);

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
                .layer(axum::middleware::from_fn_with_state(
                    auth_state.clone(),
                    auth_middleware::auth_layer,
                ))
                .layer(axum::middleware::from_fn_with_state(
                    auth_state.clone(),
                    auth_middleware::rate_limit_write_layer,
                )),
        )
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
            .route("/internal/drain", post(peer_handlers::handle_drain));
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

    router
        .layer(ServiceBuilder::new().layer(middleware::map_response(
            http_middleware::add_version_headers,
        )))
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}
