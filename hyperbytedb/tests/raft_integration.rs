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
use hyperbytedb::application::peer_ingestion_service::PeerIngestionService;
use hyperbytedb::application::peer_query_service::PeerQueryService;
use hyperbytedb::application::query_service::QueryServiceImpl;
use hyperbytedb::application::replication_apply::ReplicationApplyQueue;
use hyperbytedb::domain::cluster::membership::{
    ClusterMembership, NodeInfo, NodeState, new_shared,
};
use hyperbytedb::ports::points_sink::PointsSinkPort;
use serial_test::serial;

struct ClusterTestNode {
    url: String,
    _handle: tokio::task::JoinHandle<()>,
}

async fn start_cluster_node(
    dir: &std::path::Path,
    node_id: u64,
    peer_addrs: Vec<String>,
) -> ClusterTestNode {
    let wal_dir = dir.join("wal");
    let meta_dir = dir.join("meta");
    let chdb_dir = dir.join("chdb");

    std::fs::create_dir_all(&wal_dir).unwrap();
    std::fs::create_dir_all(&meta_dir).unwrap();
    std::fs::create_dir_all(&chdb_dir).unwrap();

    let wal = Arc::new(RocksDbWal::open(&wal_dir).unwrap());
    let metadata = Arc::new(RocksDbMetadata::open(&meta_dir).unwrap());
    let shared = SharedSession::new_eager(chdb_dir.to_str().unwrap()).unwrap();
    let chdb = Arc::new(ChdbQueryAdapter::from_shared(shared.clone(), 0));
    let sink: Arc<dyn PointsSinkPort> = Arc::new(ChdbNativeAdapter::new(shared));

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let url = format!("http://{}", addr);

    let repl_dir = dir.join("repl");
    std::fs::create_dir_all(&repl_dir).unwrap();
    let replication_log = Arc::new(ReplicationLog::open(&repl_dir).unwrap());

    let mut membership = ClusterMembership::new();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    membership.add_node(NodeInfo {
        node_id,
        addr: addr.to_string(),
        state: NodeState::Active,
        joined_at: now,
        last_heartbeat: now,
        needs_sync: false,
    });
    for (i, peer_addr) in peer_addrs.iter().enumerate() {
        membership.add_node(NodeInfo {
            node_id: node_id + 1 + i as u64,
            addr: peer_addr.clone(),
            state: NodeState::Active,
            joined_at: now,
            last_heartbeat: now,
            needs_sync: false,
        });
    }
    let shared_membership = new_shared(membership);

    let peer_client = Arc::new(PeerClient::new(
        node_id,
        addr.to_string(),
        shared_membership.clone(),
        replication_log,
        5,
        8192,
        8,
        8 * 1024 * 1024,
    ));

    let replication_apply = Some(ReplicationApplyQueue::with_defaults(
        metadata.clone(),
        wal.clone(),
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
            node_id,
            IngestCardinalityLimits::default(),
        ));

    let query_service: Arc<dyn hyperbytedb::adapters::http::router::QueryService> = Arc::new(
        PeerQueryService::new(base_query_service, peer_client.clone()),
    );

    let app_state = Arc::new(AppState {
        ingestion: ingestion_service,
        query: query_service,
        query_port: chdb.clone(),
        metadata: metadata.clone(),
        wal: wal.clone(),
        auth: Arc::new(hyperbytedb::adapters::auth::MetadataAuthAdapter::new(
            metadata.clone(),
        )),
        peer_client: Some(peer_client),
        membership: Some(shared_membership),
        replication_log: None,
        drain_service: None,
        raft: None,
        auth_enabled: false,
        prometheus_handle: None,
        statement_summary: None,
        replication_apply,
        chdb_session_data_path: chdb_dir.to_string_lossy().into_owned(),
        node_id,
        max_body_size_bytes: 25 * 1024 * 1024,
        request_timeout_secs: 30,
        rate_limiter: None,
    });

    let app = build_router(app_state);
    let handle = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    // Give the server a moment to start
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    ClusterTestNode {
        url,
        _handle: handle,
    }
}

#[tokio::test]
#[serial(chdb)]
async fn test_single_node_cluster_write() {
    let dir = tempfile::tempdir().unwrap();
    let node = start_cluster_node(dir.path(), 1, vec![]).await;
    let client = reqwest::Client::new();

    // Create database
    let resp = client
        .get(format!("{}/query", node.url))
        .query(&[("q", "CREATE DATABASE testdb")])
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // Write data
    let resp = client
        .post(format!("{}/write", node.url))
        .query(&[("db", "testdb")])
        .body("cpu,host=server01 value=42.5 1000000000")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);

    // Verify data is visible in metadata
    let resp = client
        .get(format!("{}/query", node.url))
        .query(&[("q", "SHOW MEASUREMENTS"), ("db", "testdb")])
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = resp.text().await.unwrap();
    assert!(body.contains("cpu"), "Expected 'cpu' measurement: {body}");
}

#[tokio::test]
#[serial(chdb)]
async fn test_cluster_metrics_endpoint() {
    let dir = tempfile::tempdir().unwrap();
    let node = start_cluster_node(dir.path(), 1, vec!["fake-peer:8086".into()]).await;
    let client = reqwest::Client::new();

    let resp = client
        .get(format!("{}/cluster/metrics", node.url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["mode"], "master-master");
    assert_eq!(body["peer_count"], 1);
}

#[tokio::test]
#[serial(chdb)]
async fn test_cluster_nodes_endpoint() {
    let dir = tempfile::tempdir().unwrap();
    let node = start_cluster_node(
        dir.path(),
        1,
        vec!["peer1:8086".into(), "peer2:8086".into()],
    )
    .await;
    let client = reqwest::Client::new();

    let resp = client
        .get(format!("{}/cluster/nodes", node.url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body: serde_json::Value = resp.json().await.unwrap();
    let nodes = body["nodes"].as_array().unwrap();
    assert_eq!(nodes.len(), 3, "Should have self + 2 peers");
}

#[tokio::test]
#[serial(chdb)]
async fn test_drain_endpoint_without_drain_service() {
    let dir = tempfile::tempdir().unwrap();
    let node = start_cluster_node(dir.path(), 1, vec![]).await;
    let client = reqwest::Client::new();

    // Drain endpoint should return BAD_REQUEST when drain_service is None
    let resp = client
        .post(format!("{}/internal/drain", node.url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
#[serial(chdb)]
async fn test_cluster_endpoints_without_peers() {
    let dir = tempfile::tempdir().unwrap();
    let wal_dir = dir.path().join("wal");
    let meta_dir = dir.path().join("meta");
    let chdb_dir = dir.path().join("chdb");

    std::fs::create_dir_all(&wal_dir).unwrap();
    std::fs::create_dir_all(&meta_dir).unwrap();
    std::fs::create_dir_all(&chdb_dir).unwrap();

    let wal = Arc::new(RocksDbWal::open(&wal_dir).unwrap());
    let metadata = Arc::new(RocksDbMetadata::open(&meta_dir).unwrap());
    let shared = SharedSession::new_eager(chdb_dir.to_str().unwrap()).unwrap();
    let chdb = Arc::new(ChdbQueryAdapter::from_shared(shared.clone(), 0));
    let sink: Arc<dyn PointsSinkPort> = Arc::new(ChdbNativeAdapter::new(shared));

    let ingestion_service: Arc<dyn hyperbytedb::ports::ingestion::IngestionPort> = Arc::new(
        hyperbytedb::application::ingestion_service::IngestionServiceImpl::new(
            wal.clone(),
            metadata.clone(),
            100_000,
            10_000,
        ),
    );
    let query_service: Arc<dyn hyperbytedb::adapters::http::router::QueryService> = Arc::new(
        QueryServiceImpl::new(chdb.clone(), metadata.clone(), wal.clone(), 30, sink),
    );

    let app_state = Arc::new(AppState {
        ingestion: ingestion_service,
        query: query_service,
        query_port: chdb.clone(),
        metadata: metadata.clone(),
        wal,
        auth: Arc::new(hyperbytedb::adapters::auth::MetadataAuthAdapter::new(
            metadata,
        )),
        peer_client: None,
        membership: None,
        replication_log: None,
        drain_service: None,
        raft: None,
        auth_enabled: false,
        prometheus_handle: None,
        statement_summary: None,
        replication_apply: None,
        chdb_session_data_path: chdb_dir.to_string_lossy().into_owned(),
        node_id: 1,
        max_body_size_bytes: 25 * 1024 * 1024,
        request_timeout_secs: 30,
        rate_limiter: None,
    });

    let app = build_router(app_state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let url = format!("http://{}", addr);
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let client = reqwest::Client::new();

    // Cluster routes should not be registered when peer_client is None
    let resp = client
        .get(format!("{url}/cluster/metrics"))
        .send()
        .await
        .unwrap();
    assert_ne!(resp.status(), StatusCode::OK);
}
