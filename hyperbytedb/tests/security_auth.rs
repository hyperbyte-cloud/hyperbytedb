//! Security-focused HTTP integration tests: cluster+auth route gating,
//! statement summary auth/redaction, and GRANT/REVOKE enforcement.

use std::sync::Arc;

use axum::http::StatusCode;
use hyperbytedb::adapters::chdb::native_adapter::ChdbNativeAdapter;
use hyperbytedb::adapters::chdb::query_adapter::ChdbQueryAdapter;
use hyperbytedb::adapters::chdb::session::SharedSession;
use hyperbytedb::adapters::cluster::peer_client::PeerClient;
use hyperbytedb::adapters::cluster::replication_log::ReplicationLog;
use hyperbytedb::adapters::http::router::{AppState, build_router};
use hyperbytedb::adapters::metadata::rocksdb_meta::RocksDbMetadata;
use hyperbytedb::adapters::wal::rocksdb_wal::RocksDbWal;
use hyperbytedb::application::ingest_metadata::IngestCardinalityLimits;
use hyperbytedb::application::materialized_view_service::MaterializedViewService;
use hyperbytedb::application::peer_ingestion_service::PeerIngestionService;
use hyperbytedb::application::peer_query_service::PeerQueryService;
use hyperbytedb::application::query_service::QueryServiceImpl;
use hyperbytedb::application::replication_apply::ReplicationApplyQueue;
use hyperbytedb::application::statement_summary::StatementSummary;
use hyperbytedb::domain::cluster::membership::{
    ClusterMembership, NodeInfo, NodeState, new_shared,
};
use hyperbytedb::ports::metadata::MetadataPort;
use hyperbytedb::ports::points_sink::PointsSinkPort;
use serial_test::serial;

struct AuthClusterNode {
    url: String,
    _handle: tokio::task::JoinHandle<()>,
}

async fn start_auth_cluster_node(dir: &std::path::Path) -> AuthClusterNode {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let listener_addr = listener.local_addr().unwrap();
    let url = format!("http://{}", listener_addr);

    let wal_dir = dir.join("wal");
    let meta_dir = dir.join("meta");
    let chdb_dir = dir.join("chdb");
    std::fs::create_dir_all(&wal_dir).unwrap();
    std::fs::create_dir_all(&meta_dir).unwrap();
    std::fs::create_dir_all(&chdb_dir).unwrap();

    let wal = Arc::new(RocksDbWal::open(&wal_dir).unwrap());
    let metadata = Arc::new(RocksDbMetadata::open(&meta_dir).unwrap());
    let shared = SharedSession::new_eager(chdb_dir.to_str().unwrap(), 1).unwrap();
    let chdb_path = shared.data_path().to_string();
    let chdb = Arc::new(ChdbQueryAdapter::from_shared(shared.clone(), 0));
    let sink: Arc<dyn PointsSinkPort> = Arc::new(ChdbNativeAdapter::new(shared));

    let admin_hash =
        hyperbytedb::adapters::http::auth_middleware::hash_password("adminpw").unwrap();
    metadata
        .create_user("admin", &admin_hash, true)
        .await
        .unwrap();
    let writer_hash =
        hyperbytedb::adapters::http::auth_middleware::hash_password("writerpw").unwrap();
    metadata
        .create_user("writer", &writer_hash, false)
        .await
        .unwrap();

    let repl_dir = dir.join("repl");
    std::fs::create_dir_all(&repl_dir).unwrap();
    let replication_log = Arc::new(ReplicationLog::open(&repl_dir).unwrap());

    let mut membership = ClusterMembership::new();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    membership.add_node(NodeInfo {
        node_id: 1,
        addr: listener_addr.to_string(),
        state: NodeState::Active,
        joined_at: now,
        last_heartbeat: now,
        needs_sync: false,
    });
    let shared_membership = new_shared(membership);

    let peer_client = Arc::new(PeerClient::new(
        1,
        listener_addr.to_string(),
        shared_membership.clone(),
        replication_log.clone(),
        5,
        8192,
        8,
        8 * 1024 * 1024,
    ));

    let replication_apply = Some(ReplicationApplyQueue::with_workers_and_sink(
        1024,
        8,
        metadata.clone(),
        wal.clone(),
        Some(sink.clone()),
        IngestCardinalityLimits::default(),
        0,
    ));

    let base_query_service: Arc<dyn hyperbytedb::adapters::http::router::QueryService> =
        Arc::new(QueryServiceImpl::new(
            chdb.clone(),
            metadata.clone(),
            wal.clone(),
            30,
            sink.clone(),
        ));

    let ingestion_service: Arc<dyn hyperbytedb::ports::ingestion::IngestionPort> =
        Arc::new(PeerIngestionService::new(
            wal.clone(),
            metadata.clone(),
            peer_client.clone(),
            1,
            IngestCardinalityLimits::default(),
        ));

    let query_service: Arc<dyn hyperbytedb::adapters::http::router::QueryService> = Arc::new(
        PeerQueryService::new(base_query_service, metadata.clone(), peer_client.clone()),
    );

    let app_state = Arc::new(AppState {
        ingestion: ingestion_service,
        query: query_service,
        query_port: chdb.clone(),
        metadata: metadata.clone(),
        wal: wal.clone(),
        points_sink: sink.clone(),
        mv_service: Arc::new(MaterializedViewService::new(
            metadata.clone(),
            chdb.clone(),
            sink.clone(),
        )),
        auth: Arc::new(hyperbytedb::adapters::auth::MetadataAuthAdapter::new(
            metadata.clone(),
        )),
        peer_client: Some(peer_client),
        membership: Some(shared_membership),
        replication_log: Some(replication_log),
        drain_service: None,
        raft: None,
        auth_enabled: true,
        prometheus_handle: None,
        statement_summary: Some(Arc::new(StatementSummary::new(100))),
        statement_summary_require_auth: true,
        replication_apply,
        chdb_session_data_path: chdb_path,
        node_id: 1,
        max_body_size_bytes: 25 * 1024 * 1024,
        replicate_body_limit_bytes: 32 * 1024 * 1024,
        max_points_per_request: 0,
        request_timeout_secs: 30,
        rate_limiter: None,
        wal_batcher_alive: None,
        disk_read_only: None,
    });

    let app = build_router(app_state);
    let handle = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    AuthClusterNode {
        url,
        _handle: handle,
    }
}

#[tokio::test]
#[serial(chdb)]
async fn cluster_auth_public_health_stays_open() {
    let dir = tempfile::tempdir().unwrap();
    let node = start_auth_cluster_node(dir.path()).await;
    let client = reqwest::Client::new();

    let ping = client
        .get(format!("{}/ping", node.url))
        .send()
        .await
        .unwrap();
    assert_eq!(ping.status(), StatusCode::NO_CONTENT);

    let health = client
        .get(format!("{}/health", node.url))
        .send()
        .await
        .unwrap();
    assert_eq!(health.status(), StatusCode::OK);

    let write = client
        .post(format!("{}/write", node.url))
        .query(&[("db", "mydb")])
        .body("cpu,host=a value=1")
        .send()
        .await
        .unwrap();
    assert_eq!(
        write.status(),
        StatusCode::UNAUTHORIZED,
        "non-admin write should require user auth, not admin auth"
    );
}

#[tokio::test]
#[serial(chdb)]
async fn cluster_auth_internal_routes_require_admin() {
    let dir = tempfile::tempdir().unwrap();
    let node = start_auth_cluster_node(dir.path()).await;
    let client = reqwest::Client::new();

    let unauth = client
        .get(format!("{}/cluster/metrics", node.url))
        .send()
        .await
        .unwrap();
    assert_eq!(unauth.status(), StatusCode::UNAUTHORIZED);

    let non_admin = client
        .get(format!("{}/internal/membership", node.url))
        .query(&[("u", "writer"), ("p", "writerpw")])
        .send()
        .await
        .unwrap();
    assert_eq!(non_admin.status(), StatusCode::FORBIDDEN);

    let admin = client
        .get(format!("{}/internal/membership", node.url))
        .query(&[("u", "admin"), ("p", "adminpw")])
        .send()
        .await
        .unwrap();
    assert_eq!(admin.status(), StatusCode::OK);
}

#[tokio::test]
#[serial(chdb)]
async fn statement_summary_requires_auth_and_redacts_passwords() {
    let dir = tempfile::tempdir().unwrap();
    let node = start_auth_cluster_node(dir.path()).await;
    let client = reqwest::Client::new();

    let unauth = client
        .get(format!("{}/api/v1/statements", node.url))
        .send()
        .await
        .unwrap();
    assert_eq!(unauth.status(), StatusCode::UNAUTHORIZED);

    let create_user = client
        .get(format!("{}/query", node.url))
        .query(&[
            ("q", r#"CREATE USER "leak" WITH PASSWORD 's3cret'"#),
            ("u", "admin"),
            ("p", "adminpw"),
        ])
        .send()
        .await
        .unwrap();
    assert_eq!(create_user.status(), StatusCode::OK);

    let list = client
        .get(format!("{}/api/v1/statements", node.url))
        .query(&[("u", "admin"), ("p", "adminpw")])
        .send()
        .await
        .unwrap();
    assert_eq!(list.status(), StatusCode::OK);
    let body: serde_json::Value = list.json().await.unwrap();
    let entries = body["statements"].as_array().expect("statements array");
    let sample = entries
        .iter()
        .find_map(|e| e["sample_query"].as_str())
        .expect("sample_query present");
    assert!(!sample.contains("s3cret"), "password leaked: {sample}");
    assert!(sample.contains("****"), "password not redacted: {sample}");
}

#[tokio::test]
#[serial(chdb)]
async fn grant_revoke_controls_write_access() {
    let dir = tempfile::tempdir().unwrap();
    let node = start_auth_cluster_node(dir.path()).await;
    let client = reqwest::Client::new();

    metadata_create_db(&node.url, &client).await;

    let grant = client
        .get(format!("{}/query", node.url))
        .query(&[
            ("db", "mydb"),
            ("q", r#"GRANT ALL ON "mydb" TO "writer""#),
            ("u", "admin"),
            ("p", "adminpw"),
        ])
        .send()
        .await
        .unwrap();
    assert_eq!(grant.status(), StatusCode::OK);

    let allowed = client
        .post(format!("{}/write", node.url))
        .query(&[("db", "mydb"), ("u", "writer"), ("p", "writerpw")])
        .body("cpu,host=a value=1")
        .send()
        .await
        .unwrap();
    assert_eq!(allowed.status(), StatusCode::NO_CONTENT);

    let revoke = client
        .get(format!("{}/query", node.url))
        .query(&[
            ("db", "mydb"),
            ("q", r#"REVOKE ALL ON "mydb" FROM "writer""#),
            ("u", "admin"),
            ("p", "adminpw"),
        ])
        .send()
        .await
        .unwrap();
    assert_eq!(revoke.status(), StatusCode::OK);

    let denied = client
        .post(format!("{}/write", node.url))
        .query(&[("db", "mydb"), ("u", "writer"), ("p", "writerpw")])
        .body("cpu,host=a value=2")
        .send()
        .await
        .unwrap();
    assert_eq!(denied.status(), StatusCode::FORBIDDEN);
}

async fn metadata_create_db(url: &str, client: &reqwest::Client) {
    let resp = client
        .get(format!("{url}/query"))
        .query(&[
            ("q", r#"CREATE DATABASE "mydb""#),
            ("u", "admin"),
            ("p", "adminpw"),
        ])
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
#[serial(chdb)]
async fn grant_revoke_controls_query_access() {
    let dir = tempfile::tempdir().unwrap();
    let node = start_auth_cluster_node(dir.path()).await;
    let client = reqwest::Client::new();

    metadata_create_db(&node.url, &client).await;

    let create_other = client
        .get(format!("{}/query", node.url))
        .query(&[
            ("q", r#"CREATE DATABASE "otherdb""#),
            ("u", "admin"),
            ("p", "adminpw"),
        ])
        .send()
        .await
        .unwrap();
    assert_eq!(create_other.status(), StatusCode::OK);

    let seed = client
        .post(format!("{}/write", node.url))
        .query(&[("db", "mydb"), ("u", "admin"), ("p", "adminpw")])
        .body("cpu,host=a value=1")
        .send()
        .await
        .unwrap();
    assert_eq!(seed.status(), StatusCode::NO_CONTENT);

    let denied_mydb = client
        .get(format!("{}/query", node.url))
        .query(&[
            ("db", "mydb"),
            ("q", r#"SELECT * FROM "cpu""#),
            ("u", "writer"),
            ("p", "writerpw"),
        ])
        .send()
        .await
        .unwrap();
    assert_eq!(denied_mydb.status(), StatusCode::FORBIDDEN);

    let grant = client
        .get(format!("{}/query", node.url))
        .query(&[
            ("db", "mydb"),
            ("q", r#"GRANT ALL ON "mydb" TO "writer""#),
            ("u", "admin"),
            ("p", "adminpw"),
        ])
        .send()
        .await
        .unwrap();
    assert_eq!(grant.status(), StatusCode::OK);

    let allowed_mydb = client
        .get(format!("{}/query", node.url))
        .query(&[
            ("db", "mydb"),
            ("q", r#"SELECT * FROM "cpu""#),
            ("u", "writer"),
            ("p", "writerpw"),
        ])
        .send()
        .await
        .unwrap();
    assert_eq!(allowed_mydb.status(), StatusCode::OK);

    let denied_otherdb = client
        .get(format!("{}/query", node.url))
        .query(&[
            ("db", "otherdb"),
            ("q", r#"SHOW MEASUREMENTS"#),
            ("u", "writer"),
            ("p", "writerpw"),
        ])
        .send()
        .await
        .unwrap();
    assert_eq!(denied_otherdb.status(), StatusCode::FORBIDDEN);
}
