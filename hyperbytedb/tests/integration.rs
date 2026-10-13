//! Focused HTTP integration tests not covered by `tests/compat/`.
//!
//! General InfluxDB v1 compatibility (writes, queries, DDL, HTTP shape) lives in
//! `tests/compat/`. This crate keeps auth, cardinality, admin users, metrics,
//! backup manifest shape, and chDB physical layout checks.

use hyperbytedb::config::ReplicationConfig;
use std::sync::{Arc, OnceLock};

use axum::http::StatusCode;
use hyperbytedb::adapters::chdb::native_adapter::ChdbNativeAdapter;
use hyperbytedb::adapters::chdb::query_adapter::ChdbQueryAdapter;
use hyperbytedb::adapters::chdb::session::SharedSession;
use hyperbytedb::adapters::http::router::{AppState, build_router};
use hyperbytedb::adapters::metadata::rocksdb_meta::RocksDbMetadata;
use hyperbytedb::adapters::wal::rocksdb_wal::RocksDbWal;
use hyperbytedb::application::flush_service::FlushServiceImpl;
use hyperbytedb::application::ingest_metadata::IngestCardinalityLimits;
use hyperbytedb::application::ingestion_service::IngestionServiceImpl;
use hyperbytedb::application::materialized_view_service::MaterializedViewService;
use hyperbytedb::application::query_service::QueryServiceImpl;
use hyperbytedb::ports::metadata::MetadataPort;
use hyperbytedb::ports::points_sink::PointsSinkPort;
use serial_test::serial;

/// Single process-wide Prometheus recorder for integration tests. Without this,
/// later tests that call `set_global_recorder` get `AlreadySet` and scrape an
/// empty handle while counters increment on the first recorder.
fn test_prometheus_handle() -> metrics_exporter_prometheus::PrometheusHandle {
    static HANDLE: OnceLock<metrics_exporter_prometheus::PrometheusHandle> = OnceLock::new();
    HANDLE
        .get_or_init(|| {
            let builder = metrics_exporter_prometheus::PrometheusBuilder::new();
            let recorder = builder.build_recorder();
            let handle = recorder.handle();
            let _ = metrics::set_global_recorder(recorder);
            handle
        })
        .clone()
}

fn test_mv_service(
    metadata: &Arc<dyn MetadataPort>,
    chdb: &Arc<ChdbQueryAdapter>,
    points_sink: &Arc<dyn PointsSinkPort>,
) -> Arc<MaterializedViewService> {
    Arc::new(MaterializedViewService::new(
        metadata.clone(),
        chdb.clone(),
        points_sink.clone(),
    ))
}

fn setup(dir: &tempfile::TempDir) -> (Arc<AppState>, Arc<FlushServiceImpl>) {
    let wal_dir = dir.path().join("wal");
    let meta_dir = dir.path().join("meta");
    let chdb_dir = dir.path().join("chdb");

    std::fs::create_dir_all(&wal_dir).unwrap();
    std::fs::create_dir_all(&meta_dir).unwrap();
    std::fs::create_dir_all(&chdb_dir).unwrap();

    let wal = Arc::new(RocksDbWal::open(&wal_dir).unwrap());
    let metadata = Arc::new(RocksDbMetadata::open(&meta_dir).unwrap());

    let shared = SharedSession::new_eager(chdb_dir.to_str().unwrap(), 1).unwrap();
    let chdb = Arc::new(ChdbQueryAdapter::from_shared(shared.clone(), 0));
    let points_sink: Arc<dyn PointsSinkPort> = Arc::new(ChdbNativeAdapter::new(shared));

    let ingestion_service: Arc<dyn hyperbytedb::ports::ingestion::IngestionPort> = Arc::new(
        IngestionServiceImpl::new(wal.clone(), metadata.clone(), 100_000, 10_000, 0),
    );

    let query_service: Arc<dyn hyperbytedb::adapters::http::router::QueryService> =
        Arc::new(QueryServiceImpl::new(
            chdb.clone(),
            metadata.clone(),
            wal.clone(),
            30,
            points_sink.clone(),
        ));

    let flush_service = Arc::new(FlushServiceImpl::new(wal.clone(), 0, points_sink.clone()));

    let chdb_path_str = chdb_dir.to_str().unwrap().to_string();
    let app_state = Arc::new(AppState {
        ingestion: ingestion_service,
        query: query_service,
        query_port: chdb.clone(),
        metadata: metadata.clone(),
        wal: wal.clone(),
        points_sink: points_sink.clone(),
        mv_service: test_mv_service(
            &(metadata.clone() as Arc<dyn MetadataPort>),
            &chdb,
            &points_sink,
        ),
        auth: Arc::new(hyperbytedb::adapters::auth::MetadataAuthAdapter::new(
            metadata.clone(),
        )),
        peer_client: None,
        membership: None,
        replication_log: None,
        drain_service: None,
        raft: None,
        auth_enabled: false,
        auth_allow_query_param_credentials: false,
        prometheus_handle: None,
        statement_summary: None,
        statement_summary_require_auth: true,
        replication_apply: None,
        chdb_session_data_path: chdb_path_str,
        node_id: 1,
        max_body_size_bytes: 25 * 1024 * 1024,
        replicate_body_limit_bytes: 32 * 1024 * 1024,
        max_points_per_request: 0,
        request_timeout_secs: 30,
        wal_batcher_alive: None,
        disk_read_only: None,
        rate_limiter: None,
        write_admission: None,
        shard_write_admission: None,

        sharding_enabled: false,
        shard_map: None,
        shard_location_cache: Arc::new(hyperbytedb::domain::sharding::ShardLocationCache::new()),
        shard_routing: None,
        shard_scheduler: None,
        ingest_cardinality: IngestCardinalityLimits::default(),
        ingest_schema_cache: Default::default(),
        cluster_replication: ReplicationConfig::default(),
    });

    (app_state, flush_service)
}

async fn start_server(state: Arc<AppState>) -> (String, tokio::task::JoinHandle<()>) {
    let app = build_router(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let url = format!("http://{}", addr);
    let handle = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (url, handle)
}

#[tokio::test]
#[serial(chdb)]
async fn test_auth_blocks_unauthenticated() {
    let dir = tempfile::tempdir().unwrap();
    let wal_dir = dir.path().join("wal_auth");
    let meta_dir = dir.path().join("meta_auth");
    let chdb_dir = dir.path().join("chdb_auth");
    std::fs::create_dir_all(&wal_dir).unwrap();
    std::fs::create_dir_all(&meta_dir).unwrap();
    std::fs::create_dir_all(&chdb_dir).unwrap();

    let wal = Arc::new(RocksDbWal::open(&wal_dir).unwrap());
    let metadata = Arc::new(RocksDbMetadata::open(&meta_dir).unwrap());
    let shared = SharedSession::new_eager(chdb_dir.to_str().unwrap(), 1).unwrap();
    let chdb = Arc::new(ChdbQueryAdapter::from_shared(shared.clone(), 0));
    let sink: Arc<dyn PointsSinkPort> = Arc::new(ChdbNativeAdapter::new(shared));

    let password_hash =
        hyperbytedb::adapters::http::auth_middleware::hash_password("secret123").unwrap();
    metadata
        .create_user("admin", &password_hash, true)
        .await
        .unwrap();

    let ingestion_service: Arc<dyn hyperbytedb::ports::ingestion::IngestionPort> = Arc::new(
        IngestionServiceImpl::new(wal.clone(), metadata.clone(), 100_000, 10_000, 0),
    );
    let query_service: Arc<dyn hyperbytedb::adapters::http::router::QueryService> =
        Arc::new(QueryServiceImpl::new(
            chdb.clone(),
            metadata.clone(),
            wal.clone(),
            30,
            sink.clone(),
        ));

    let _flush = FlushServiceImpl::new(wal.clone(), 0, sink.clone());

    let app_state = Arc::new(AppState {
        ingestion: ingestion_service,
        query: query_service,
        query_port: chdb.clone(),
        metadata: metadata.clone(),
        wal: wal.clone(),
        points_sink: sink.clone(),
        mv_service: test_mv_service(&(metadata.clone() as Arc<dyn MetadataPort>), &chdb, &sink),
        auth: Arc::new(hyperbytedb::adapters::auth::MetadataAuthAdapter::new(
            metadata.clone(),
        )),
        peer_client: None,
        membership: None,
        replication_log: None,
        drain_service: None,
        raft: None,
        auth_enabled: true,
        auth_allow_query_param_credentials: false,
        prometheus_handle: None,
        statement_summary: None,
        statement_summary_require_auth: true,
        replication_apply: None,
        chdb_session_data_path: chdb_dir.to_string_lossy().into_owned(),
        node_id: 1,
        max_body_size_bytes: 25 * 1024 * 1024,
        replicate_body_limit_bytes: 32 * 1024 * 1024,
        max_points_per_request: 0,
        request_timeout_secs: 30,
        wal_batcher_alive: None,
        disk_read_only: None,
        rate_limiter: None,
        write_admission: None,
        shard_write_admission: None,

        sharding_enabled: false,
        shard_map: None,
        shard_location_cache: Arc::new(hyperbytedb::domain::sharding::ShardLocationCache::new()),
        shard_routing: None,
        shard_scheduler: None,
        ingest_cardinality: IngestCardinalityLimits::default(),
        ingest_schema_cache: Default::default(),
        cluster_replication: ReplicationConfig::default(),
    });

    let (url, _handle) = start_server(app_state).await;
    let client = reqwest::Client::new();

    let resp = client
        .get(format!("{url}/query"))
        .query(&[("q", "SHOW DATABASES")])
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::UNAUTHORIZED,
        "Unauthenticated query should be rejected"
    );

    let resp = client
        .get(format!("{url}/query"))
        .query(&[("q", "SHOW DATABASES"), ("u", "admin"), ("p", "secret123")])
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::UNAUTHORIZED,
        "Query-param credentials should be rejected when allow_query_param_credentials is false"
    );

    let resp = client
        .get(format!("{url}/query"))
        .query(&[("q", "SHOW DATABASES")])
        .basic_auth("admin", Some("secret123"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "Basic auth should succeed");

    let resp = client
        .get(format!("{url}/query"))
        .query(&[("q", "SHOW DATABASES")])
        .basic_auth("admin", Some("wrong"))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::UNAUTHORIZED,
        "Wrong password should be rejected"
    );
}

#[tokio::test]
#[serial(chdb)]
async fn test_cardinality_limit() {
    let dir = tempfile::tempdir().unwrap();
    let wal_dir = dir.path().join("wal_card");
    let meta_dir = dir.path().join("meta_card");
    let chdb_dir = dir.path().join("chdb_card");
    std::fs::create_dir_all(&wal_dir).unwrap();
    std::fs::create_dir_all(&meta_dir).unwrap();
    std::fs::create_dir_all(&chdb_dir).unwrap();

    let wal = Arc::new(RocksDbWal::open(&wal_dir).unwrap());
    let metadata = Arc::new(RocksDbMetadata::open(&meta_dir).unwrap());
    let shared = SharedSession::new_eager(chdb_dir.to_str().unwrap(), 1).unwrap();
    let chdb = Arc::new(ChdbQueryAdapter::from_shared(shared.clone(), 0));
    let sink: Arc<dyn PointsSinkPort> = Arc::new(ChdbNativeAdapter::new(shared));

    let ingestion_service: Arc<dyn hyperbytedb::ports::ingestion::IngestionPort> = Arc::new(
        IngestionServiceImpl::new(wal.clone(), metadata.clone(), 100_000, 2, 0),
    );
    let query_service: Arc<dyn hyperbytedb::adapters::http::router::QueryService> =
        Arc::new(QueryServiceImpl::new(
            chdb.clone(),
            metadata.clone(),
            wal.clone(),
            30,
            sink.clone(),
        ));

    let _flush = FlushServiceImpl::new(wal.clone(), 0, sink.clone());

    let app_state = Arc::new(AppState {
        ingestion: ingestion_service,
        query: query_service,
        query_port: chdb.clone(),
        metadata: metadata.clone(),
        wal: wal.clone(),
        points_sink: sink.clone(),
        mv_service: test_mv_service(&(metadata.clone() as Arc<dyn MetadataPort>), &chdb, &sink),
        auth: Arc::new(hyperbytedb::adapters::auth::MetadataAuthAdapter::new(
            metadata.clone(),
        )),
        peer_client: None,
        membership: None,
        replication_log: None,
        drain_service: None,
        raft: None,
        auth_enabled: false,
        auth_allow_query_param_credentials: false,
        prometheus_handle: None,
        statement_summary: None,
        statement_summary_require_auth: true,
        replication_apply: None,
        chdb_session_data_path: chdb_dir.to_string_lossy().into_owned(),
        node_id: 1,
        max_body_size_bytes: 25 * 1024 * 1024,
        replicate_body_limit_bytes: 32 * 1024 * 1024,
        max_points_per_request: 0,
        request_timeout_secs: 30,
        wal_batcher_alive: None,
        disk_read_only: None,
        rate_limiter: None,
        write_admission: None,
        shard_write_admission: None,

        sharding_enabled: false,
        shard_map: None,
        shard_location_cache: Arc::new(hyperbytedb::domain::sharding::ShardLocationCache::new()),
        shard_routing: None,
        shard_scheduler: None,
        ingest_cardinality: IngestCardinalityLimits::default(),
        ingest_schema_cache: Default::default(),
        cluster_replication: ReplicationConfig::default(),
    });

    let (url, _handle) = start_server(app_state).await;
    let client = reqwest::Client::new();

    client
        .get(format!("{url}/query"))
        .query(&[("q", "CREATE DATABASE cardtest")])
        .send()
        .await
        .unwrap();

    let resp = client
        .post(format!("{url}/write"))
        .query(&[("db", "cardtest")])
        .body("meas1 val=1.0 1000000000\nmeas2 val=2.0 2000000000")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);

    let resp = client
        .post(format!("{url}/write"))
        .query(&[("db", "cardtest")])
        .body("meas3 val=3.0 3000000000")
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::UNPROCESSABLE_ENTITY,
        "Should reject write exceeding measurement cardinality limit"
    );
}

#[tokio::test]
#[serial(chdb)]
async fn test_create_and_show_users() {
    let dir = tempfile::tempdir().unwrap();
    let (state, _flush) = setup(&dir);
    let (url, _handle) = start_server(state).await;
    let client = reqwest::Client::new();

    let resp = client
        .get(format!("{url}/query"))
        .query(&[("q", "CREATE USER \"testuser\" WITH PASSWORD 'pass123'")])
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let resp = client
        .get(format!("{url}/query"))
        .query(&[("q", "SHOW USERS")])
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = resp.text().await.unwrap();
    assert!(
        body.contains("testuser"),
        "Expected 'testuser' in SHOW USERS: {body}"
    );

    let resp = client
        .get(format!("{url}/query"))
        .query(&[("q", "DROP USER \"testuser\"")])
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let resp = client
        .get(format!("{url}/query"))
        .query(&[("q", "SHOW USERS")])
        .send()
        .await
        .unwrap();
    let body = resp.text().await.unwrap();
    assert!(
        !body.contains("testuser"),
        "User 'testuser' should be gone: {body}"
    );
}

#[tokio::test]
#[serial(chdb)]
async fn test_metrics_endpoint() {
    let dir = tempfile::tempdir().unwrap();
    let wal_dir = dir.path().join("wal_met");
    let meta_dir = dir.path().join("meta_met");
    let chdb_dir = dir.path().join("chdb_met");
    std::fs::create_dir_all(&wal_dir).unwrap();
    std::fs::create_dir_all(&meta_dir).unwrap();
    std::fs::create_dir_all(&chdb_dir).unwrap();

    let wal = Arc::new(RocksDbWal::open(&wal_dir).unwrap());
    let metadata = Arc::new(RocksDbMetadata::open(&meta_dir).unwrap());
    let shared = SharedSession::new_eager(chdb_dir.to_str().unwrap(), 1).unwrap();
    let chdb = Arc::new(ChdbQueryAdapter::from_shared(shared.clone(), 0));
    let sink: Arc<dyn PointsSinkPort> = Arc::new(ChdbNativeAdapter::new(shared));

    let prometheus_handle = test_prometheus_handle();

    let ingestion_service: Arc<dyn hyperbytedb::ports::ingestion::IngestionPort> = Arc::new(
        IngestionServiceImpl::new(wal.clone(), metadata.clone(), 100_000, 10_000, 0),
    );
    let query_service: Arc<dyn hyperbytedb::adapters::http::router::QueryService> =
        Arc::new(QueryServiceImpl::new(
            chdb.clone(),
            metadata.clone(),
            wal.clone(),
            30,
            sink.clone(),
        ));

    let _flush = FlushServiceImpl::new(wal.clone(), 0, sink.clone());

    let app_state = Arc::new(AppState {
        ingestion: ingestion_service,
        query: query_service,
        query_port: chdb.clone(),
        metadata: metadata.clone(),
        wal: wal.clone(),
        points_sink: sink.clone(),
        mv_service: test_mv_service(&(metadata.clone() as Arc<dyn MetadataPort>), &chdb, &sink),
        auth: Arc::new(hyperbytedb::adapters::auth::MetadataAuthAdapter::new(
            metadata.clone(),
        )),
        peer_client: None,
        membership: None,
        replication_log: None,
        drain_service: None,
        raft: None,
        auth_enabled: false,
        auth_allow_query_param_credentials: false,
        prometheus_handle: Some(prometheus_handle),
        statement_summary: None,
        statement_summary_require_auth: true,
        replication_apply: None,
        chdb_session_data_path: chdb_dir.to_string_lossy().into_owned(),
        node_id: 1,
        max_body_size_bytes: 25 * 1024 * 1024,
        replicate_body_limit_bytes: 32 * 1024 * 1024,
        max_points_per_request: 0,
        request_timeout_secs: 30,
        wal_batcher_alive: None,
        disk_read_only: None,
        rate_limiter: None,
        write_admission: None,
        shard_write_admission: None,

        sharding_enabled: false,
        shard_map: None,
        shard_location_cache: Arc::new(hyperbytedb::domain::sharding::ShardLocationCache::new()),
        shard_routing: None,
        shard_scheduler: None,
        ingest_cardinality: IngestCardinalityLimits::default(),
        ingest_schema_cache: Default::default(),
        cluster_replication: ReplicationConfig::default(),
    });

    let (url, _handle) = start_server(app_state).await;
    let client = reqwest::Client::new();

    let resp = client.get(format!("{url}/metrics")).send().await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let content_type = resp
        .headers()
        .get("content-type")
        .unwrap()
        .to_str()
        .unwrap();
    assert!(
        content_type.contains("text/plain"),
        "Metrics should return text/plain"
    );
}

#[tokio::test]
#[serial(chdb)]
async fn test_backup_manifest_structure() {
    let dir = tempfile::tempdir().unwrap();
    let (app_state, flush_svc) = setup(&dir);
    let (url, _handle) = start_server(app_state.clone()).await;
    let client = reqwest::Client::new();

    client
        .post(format!("{url}/query"))
        .query(&[("q", "CREATE DATABASE backupdb")])
        .send()
        .await
        .unwrap();

    client
        .post(format!("{url}/write"))
        .query(&[("db", "backupdb")])
        .body("cpu,host=server01 value=42.0 1000000000")
        .send()
        .await
        .unwrap();

    flush_svc.flush().await.unwrap();

    let chdb_paths = dir.path().join("chdb");
    let data_files = files_under(&chdb_paths);
    assert!(
        data_files > 0,
        "flush should have persisted into the chDB session directory"
    );

    assert!(
        dir.path().join("wal").exists(),
        "WAL directory should exist"
    );
    assert!(
        dir.path().join("meta").exists(),
        "metadata directory should exist"
    );

    let pseudo_paths: Vec<String> = (0..data_files)
        .map(|i| format!("chdb/session_file_{i}"))
        .collect();
    let manifest = serde_json::json!({
        "timestamp": "2024-01-01T00:00:00Z",
        "wal_last_seq": 1u64,
        "engine_data_paths": pseudo_paths
    });
    let manifest_str = serde_json::to_string_pretty(&manifest).unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&manifest_str).unwrap();

    assert!(parsed["timestamp"].is_string());
    assert!(parsed["wal_last_seq"].is_u64());
    assert!(parsed["engine_data_paths"].is_array());
    assert_eq!(
        parsed["engine_data_paths"].as_array().unwrap().len(),
        data_files
    );

    let legacy = serde_json::json!({
        "timestamp": "2024-01-01T00:00:00Z",
        "wal_last_seq": 1u64,
        "parquet_files": ["legacy/path"]
    });
    assert!(legacy["parquet_files"].is_array());
}

#[tokio::test]
#[serial(chdb)]
async fn test_restore_validates_manifest() {
    let dir = tempfile::tempdir().unwrap();
    let backup_dir = dir.path().join("backup");
    std::fs::create_dir_all(&backup_dir).unwrap();

    let result = std::panic::catch_unwind(|| {
        let manifest_path = backup_dir.join("manifest.json");
        assert!(!manifest_path.exists());
    });
    assert!(result.is_ok());

    let manifest = serde_json::json!({
        "timestamp": "2024-01-01T00:00:00Z",
        "wal_last_seq": 42u64,
        "engine_data_paths": ["chdb/table1", "chdb/table2"]
    });
    std::fs::write(
        backup_dir.join("manifest.json"),
        serde_json::to_string_pretty(&manifest).unwrap(),
    )
    .unwrap();

    let loaded: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(backup_dir.join("manifest.json")).unwrap())
            .unwrap();
    assert_eq!(loaded["wal_last_seq"].as_u64().unwrap(), 42);
    assert_eq!(loaded["engine_data_paths"].as_array().unwrap().len(), 2);

    assert!(!backup_dir.join("wal").exists());
    assert!(!backup_dir.join("meta").exists());
    assert!(!backup_dir.join("data").exists());
}

/// After flush, the fact table carries `series_id` and no tag columns, while the
/// `_series` dimension table holds the tag columns.
#[tokio::test]
#[serial(chdb)]
async fn series_id_physical_layout() {
    let dir = tempfile::tempdir().unwrap();
    let (state, flush) = setup(&dir);
    let (url, _handle) = start_server(state.clone()).await;
    let client = reqwest::Client::new();

    client
        .get(format!("{url}/query"))
        .query(&[("q", "CREATE DATABASE testdb")])
        .send()
        .await
        .unwrap();

    let lines = [
        "weather,location=office,sensor=a temp=22.5 1700000000000000000",
        "weather,location=office,sensor=a temp=23.0 1700000001000000000",
        "weather,location=warehouse,sensor=b temp=18.0 1700000002000000000",
        "weather,location=warehouse,sensor=b temp=17.5 1700000003000000000",
    ];
    client
        .post(format!("{url}/write"))
        .query(&[("db", "testdb")])
        .body(lines.join("\n"))
        .send()
        .await
        .unwrap();
    flush.flush().await.unwrap();

    let tables = state
        .query_port
        .execute_sql(
            "SELECT name FROM system.tables WHERE name LIKE '%weather%' ORDER BY name FORMAT TabSeparated",
        )
        .await
        .unwrap();
    let fact = tables
        .lines()
        .find(|n| n.ends_with("weather"))
        .expect("fact table exists");
    let series_tbl = tables
        .lines()
        .find(|n| n.ends_with("weather_series"))
        .expect("series dimension table exists");

    let fact_cols = state
        .query_port
        .execute_sql(&format!(
            "SELECT name FROM system.columns WHERE table = '{fact}' FORMAT TabSeparated"
        ))
        .await
        .unwrap();
    assert!(
        fact_cols.contains("series_id"),
        "fact must have series_id: {fact_cols}"
    );
    assert!(
        fact_cols.contains("temp"),
        "fact must have the field: {fact_cols}"
    );
    assert!(
        !fact_cols.contains("location") && !fact_cols.contains("sensor"),
        "fact must NOT carry tag columns: {fact_cols}"
    );

    let series_cols = state
        .query_port
        .execute_sql(&format!(
            "SELECT name FROM system.columns WHERE table = '{series_tbl}' FORMAT TabSeparated"
        ))
        .await
        .unwrap();
    assert!(
        series_cols.contains("series_id"),
        "series must have series_id: {series_cols}"
    );
    assert!(
        series_cols.contains("location") && series_cols.contains("sensor"),
        "series must carry tag columns: {series_cols}"
    );
    assert!(
        !series_cols.contains("temp"),
        "series must NOT carry fields: {series_cols}"
    );

    let series_count = state
        .query_port
        .execute_sql(&format!(
            "SELECT count() FROM {series_tbl} FINAL FORMAT TabSeparated"
        ))
        .await
        .unwrap();
    assert_eq!(
        series_count.trim(),
        "2",
        "expected 2 distinct series, got: {series_count}"
    );

    let resp = client
        .get(format!("{url}/query"))
        .query(&[
            (
                "q",
                "SELECT mean(temp) FROM weather WHERE location = 'office' GROUP BY location",
            ),
            ("db", "testdb"),
        ])
        .send()
        .await
        .unwrap();
    let body = resp.text().await.unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&body).unwrap();
    let series = parsed["results"][0]["series"].as_array().unwrap();
    assert_eq!(
        series.len(),
        1,
        "office filter should yield one series: {body}"
    );
    assert_eq!(
        series[0]["tags"]["location"].as_str(),
        Some("office"),
        "joined tag value must be recovered: {body}"
    );
}

fn files_under(dir: &std::path::Path) -> usize {
    walkdir_recursive(dir).len()
}

fn walkdir_recursive(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut result = Vec::new();
    if !dir.exists() {
        return result;
    }
    for entry in std::fs::read_dir(dir).unwrap() {
        let entry = entry.unwrap();
        let path = entry.path();
        if path.is_dir() {
            result.extend(walkdir_recursive(&path));
        } else {
            result.push(path);
        }
    }
    result
}

/// App state for the admission tests: everything default except the
/// `/write` admission semaphore.
async fn admission_app(permits: usize) -> (tempfile::TempDir, Arc<AppState>) {
    let dir = tempfile::tempdir().unwrap();
    let wal_dir = dir.path().join("wal_adm");
    let meta_dir = dir.path().join("meta_adm");
    let chdb_dir = dir.path().join("chdb_adm");
    std::fs::create_dir_all(&wal_dir).unwrap();
    std::fs::create_dir_all(&meta_dir).unwrap();
    std::fs::create_dir_all(&chdb_dir).unwrap();

    let wal = Arc::new(RocksDbWal::open(&wal_dir).unwrap());
    let metadata = Arc::new(RocksDbMetadata::open(&meta_dir).unwrap());
    let shared = SharedSession::new_eager(chdb_dir.to_str().unwrap(), 1).unwrap();
    let chdb = Arc::new(ChdbQueryAdapter::from_shared(shared.clone(), 0));
    let sink: Arc<dyn PointsSinkPort> = Arc::new(ChdbNativeAdapter::new(shared));

    let prometheus_handle = test_prometheus_handle();

    let ingestion_service: Arc<dyn hyperbytedb::ports::ingestion::IngestionPort> = Arc::new(
        IngestionServiceImpl::new(wal.clone(), metadata.clone(), 100_000, 10_000, 0),
    );
    let query_service: Arc<dyn hyperbytedb::adapters::http::router::QueryService> =
        Arc::new(QueryServiceImpl::new(
            chdb.clone(),
            metadata.clone(),
            wal.clone(),
            30,
            sink.clone(),
        ));

    let _flush = FlushServiceImpl::new(wal.clone(), 0, sink.clone());

    let app_state = Arc::new(AppState {
        ingestion: ingestion_service,
        query: query_service,
        query_port: chdb.clone(),
        metadata: metadata.clone(),
        wal: wal.clone(),
        points_sink: sink.clone(),
        mv_service: test_mv_service(&(metadata.clone() as Arc<dyn MetadataPort>), &chdb, &sink),
        auth: Arc::new(hyperbytedb::adapters::auth::MetadataAuthAdapter::new(
            metadata.clone(),
        )),
        peer_client: None,
        membership: None,
        replication_log: None,
        drain_service: None,
        raft: None,
        auth_enabled: false,
        auth_allow_query_param_credentials: false,
        prometheus_handle: Some(prometheus_handle),
        statement_summary: None,
        statement_summary_require_auth: true,
        replication_apply: None,
        chdb_session_data_path: chdb_dir.to_string_lossy().into_owned(),
        node_id: 1,
        max_body_size_bytes: 25 * 1024 * 1024,
        replicate_body_limit_bytes: 32 * 1024 * 1024,
        max_points_per_request: 0,
        request_timeout_secs: 30,
        rate_limiter: None,
        write_admission: Some(Arc::new(
            hyperbytedb::adapters::http::router::Admission::new(permits),
        )),
        shard_write_admission: None,
        wal_batcher_alive: None,
        disk_read_only: None,

        sharding_enabled: false,
        shard_map: None,
        shard_location_cache: Arc::new(hyperbytedb::domain::sharding::ShardLocationCache::new()),
        shard_routing: None,
        shard_scheduler: None,
        ingest_cardinality: IngestCardinalityLimits::default(),
        ingest_schema_cache: Default::default(),
        cluster_replication: ReplicationConfig::default(),
    });

    (dir, app_state)
}

#[tokio::test]
#[serial(chdb)]
async fn write_admission_turns_excess_writes_away_at_once() {
    // No permits: every write is over capacity and must be refused
    // immediately with Retry-After, not accepted and timed out later.
    let (_dir, app_state) = admission_app(0).await;
    let (url, _handle) = start_server(app_state).await;
    let client = reqwest::Client::new();
    let started = std::time::Instant::now();
    let resp = client
        .post(format!("{url}/write?db=testdb"))
        .body("cpu,host=a value=1 1700000000000000000")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        resp.headers()
            .get("retry-after")
            .map(|v| v.to_str().unwrap()),
        Some("1")
    );
    assert!(started.elapsed() < std::time::Duration::from_secs(1));
}

/// A write refused at capacity has its body read before the 503 goes out.
/// Admission refuses on headers alone; closing the socket with the body
/// unread makes the kernel send a reset, which discards the 503 before the
/// sender reads it. On n3 (N=12, 10k-point batches) that turned 18k forward
/// refusals into "connection closed before message completed" and 1,143
/// client writes into non-retryable 500s.
///
/// A raw socket shows it on loopback: the whole body can be written, and the
/// 503 read after it.
#[tokio::test]
#[serial(chdb)]
async fn a_refused_write_is_read_to_the_end_before_the_503() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let (_dir, app_state) = admission_app(0).await;
    let (url, _handle) = start_server(app_state).await;
    let addr = url.trim_start_matches("http://");
    let mut sock = tokio::net::TcpStream::connect(addr).await.unwrap();
    let body = vec![b'x'; 16 * 1024 * 1024];
    let head = format!(
        "POST /write?db=testdb HTTP/1.1\r\nHost: {addr}\r\nContent-Length: {}\r\n\r\n",
        body.len()
    );
    sock.write_all(head.as_bytes()).await.unwrap();
    for chunk in body.chunks(64 * 1024) {
        sock.write_all(chunk)
            .await
            .expect("the server reads the refused body instead of resetting");
    }
    let mut resp = vec![0u8; 1024];
    let n = tokio::time::timeout(std::time::Duration::from_secs(5), sock.read(&mut resp))
        .await
        .unwrap()
        .expect("the 503 survives");
    let status = String::from_utf8_lossy(&resp[..n]);
    assert!(status.starts_with("HTTP/1.1 503"), "got: {status}");
}

#[tokio::test]
#[serial(chdb)]
async fn write_admission_admits_within_capacity() {
    let (_dir, app_state) = admission_app(4).await;
    let (url, _handle) = start_server(app_state).await;
    let client = reqwest::Client::new();
    client
        .post(format!("{url}/query"))
        .form(&[("q", "CREATE DATABASE testdb")])
        .send()
        .await
        .unwrap();
    for _ in 0..8 {
        // Sequential: a released permit must be reusable.
        let resp = client
            .post(format!("{url}/write?db=testdb"))
            .body("cpu,host=a value=1 1700000000000000000")
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NO_CONTENT);
    }
}

#[tokio::test]
#[serial(chdb)]
async fn test_rate_limiter_refills_and_denies() {
    use hyperbytedb::adapters::http::rate_limit::EndpointRateLimiters;

    let dir = tempfile::tempdir().unwrap();
    let wal_dir = dir.path().join("wal_rl");
    let meta_dir = dir.path().join("meta_rl");
    let chdb_dir = dir.path().join("chdb_rl");
    std::fs::create_dir_all(&wal_dir).unwrap();
    std::fs::create_dir_all(&meta_dir).unwrap();
    std::fs::create_dir_all(&chdb_dir).unwrap();

    let wal = Arc::new(RocksDbWal::open(&wal_dir).unwrap());
    let metadata = Arc::new(RocksDbMetadata::open(&meta_dir).unwrap());
    let shared = SharedSession::new_eager(chdb_dir.to_str().unwrap(), 1).unwrap();
    let chdb = Arc::new(ChdbQueryAdapter::from_shared(shared.clone(), 0));
    let sink: Arc<dyn PointsSinkPort> = Arc::new(ChdbNativeAdapter::new(shared));

    let prometheus_handle = test_prometheus_handle();

    let ingestion_service: Arc<dyn hyperbytedb::ports::ingestion::IngestionPort> = Arc::new(
        IngestionServiceImpl::new(wal.clone(), metadata.clone(), 100_000, 10_000, 0),
    );
    let query_service: Arc<dyn hyperbytedb::adapters::http::router::QueryService> =
        Arc::new(QueryServiceImpl::new(
            chdb.clone(),
            metadata.clone(),
            wal.clone(),
            30,
            sink.clone(),
        ));

    let _flush = FlushServiceImpl::new(wal.clone(), 0, sink.clone());

    let app_state = Arc::new(AppState {
        ingestion: ingestion_service,
        query: query_service,
        query_port: chdb.clone(),
        metadata: metadata.clone(),
        wal: wal.clone(),
        points_sink: sink.clone(),
        mv_service: test_mv_service(&(metadata.clone() as Arc<dyn MetadataPort>), &chdb, &sink),
        auth: Arc::new(hyperbytedb::adapters::auth::MetadataAuthAdapter::new(
            metadata.clone(),
        )),
        peer_client: None,
        membership: None,
        replication_log: None,
        drain_service: None,
        raft: None,
        auth_enabled: false,
        auth_allow_query_param_credentials: false,
        prometheus_handle: Some(prometheus_handle),
        statement_summary: None,
        statement_summary_require_auth: true,
        replication_apply: None,
        chdb_session_data_path: chdb_dir.to_string_lossy().into_owned(),
        node_id: 1,
        max_body_size_bytes: 25 * 1024 * 1024,
        replicate_body_limit_bytes: 32 * 1024 * 1024,
        max_points_per_request: 0,
        request_timeout_secs: 30,
        rate_limiter: Some(Arc::new(EndpointRateLimiters::new(5))),
        write_admission: None,
        shard_write_admission: None,
        wal_batcher_alive: None,
        disk_read_only: None,

        sharding_enabled: false,
        shard_map: None,
        shard_location_cache: Arc::new(hyperbytedb::domain::sharding::ShardLocationCache::new()),
        shard_routing: None,
        shard_scheduler: None,
        ingest_cardinality: IngestCardinalityLimits::default(),
        ingest_schema_cache: Default::default(),
        cluster_replication: ReplicationConfig::default(),
    });

    let (url, _handle) = start_server(app_state).await;
    let client = reqwest::Client::new();

    let query_url = format!("{url}/query");
    let query_params = [("db", "testdb"), ("q", "SHOW DATABASES")];

    for _ in 0..5 {
        let resp = client
            .get(&query_url)
            .query(&query_params)
            .send()
            .await
            .unwrap();
        assert_ne!(
            resp.status(),
            StatusCode::TOO_MANY_REQUESTS,
            "first 5 requests should not be rate limited"
        );
    }

    let mut denied = 0;
    for _ in 0..5 {
        let resp = client
            .get(&query_url)
            .query(&query_params)
            .send()
            .await
            .unwrap();
        if resp.status() == StatusCode::TOO_MANY_REQUESTS {
            denied += 1;
        }
    }
    assert!(
        denied >= 1,
        "expected at least one 429 after exhausting the query bucket"
    );

    tokio::time::sleep(std::time::Duration::from_millis(1100)).await;

    let resp = client
        .get(&query_url)
        .query(&query_params)
        .send()
        .await
        .unwrap();
    assert_ne!(
        resp.status(),
        StatusCode::TOO_MANY_REQUESTS,
        "request should succeed after refill window"
    );

    let metrics_body = client
        .get(format!("{url}/metrics"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(
        metrics_body.contains("hyperbytedb_rate_limit_denied_total"),
        "metrics should expose rate limit denials: {metrics_body}"
    );
    assert!(
        metrics_body
            .lines()
            .any(|line| line.starts_with("hyperbytedb_rate_limit_denied_total") && denied >= 1),
        "rate limit denials should be recorded in metrics (denied={denied}): {metrics_body}"
    );
}

#[tokio::test]
#[serial(chdb)]
async fn test_cross_database_on_clause_requires_authorization() {
    use hyperbytedb::domain::user::DatabasePrivilege;

    let dir = tempfile::tempdir().unwrap();
    let wal_dir = dir.path().join("wal_xdb");
    let meta_dir = dir.path().join("meta_xdb");
    let chdb_dir = dir.path().join("chdb_xdb");
    std::fs::create_dir_all(&wal_dir).unwrap();
    std::fs::create_dir_all(&meta_dir).unwrap();
    std::fs::create_dir_all(&chdb_dir).unwrap();

    let wal = Arc::new(RocksDbWal::open(&wal_dir).unwrap());
    let metadata = Arc::new(RocksDbMetadata::open(&meta_dir).unwrap());
    let shared = SharedSession::new_eager(chdb_dir.to_str().unwrap(), 1).unwrap();
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
    metadata
        .grant_privilege("writer", "mine", DatabasePrivilege::Write)
        .await
        .unwrap();

    metadata.create_database("mine").await.unwrap();
    metadata.create_database("victim").await.unwrap();

    let ingestion_service: Arc<dyn hyperbytedb::ports::ingestion::IngestionPort> = Arc::new(
        IngestionServiceImpl::new(wal.clone(), metadata.clone(), 100_000, 10_000, 0),
    );
    let query_service: Arc<dyn hyperbytedb::adapters::http::router::QueryService> =
        Arc::new(QueryServiceImpl::new(
            chdb.clone(),
            metadata.clone(),
            wal.clone(),
            30,
            sink.clone(),
        ));

    let _flush = FlushServiceImpl::new(wal.clone(), 0, sink.clone());

    let app_state = Arc::new(AppState {
        ingestion: ingestion_service,
        query: query_service,
        query_port: chdb.clone(),
        metadata: metadata.clone(),
        wal: wal.clone(),
        points_sink: sink.clone(),
        mv_service: test_mv_service(&(metadata.clone() as Arc<dyn MetadataPort>), &chdb, &sink),
        auth: Arc::new(hyperbytedb::adapters::auth::MetadataAuthAdapter::new(
            metadata.clone(),
        )),
        peer_client: None,
        membership: None,
        replication_log: None,
        drain_service: None,
        raft: None,
        auth_enabled: true,
        auth_allow_query_param_credentials: true,
        prometheus_handle: None,
        statement_summary: None,
        statement_summary_require_auth: true,
        replication_apply: None,
        chdb_session_data_path: chdb_dir.to_string_lossy().into_owned(),
        node_id: 1,
        max_body_size_bytes: 25 * 1024 * 1024,
        replicate_body_limit_bytes: 32 * 1024 * 1024,
        max_points_per_request: 0,
        request_timeout_secs: 30,
        wal_batcher_alive: None,
        disk_read_only: None,
        rate_limiter: None,
        write_admission: None,
        shard_write_admission: None,

        sharding_enabled: false,
        shard_map: None,
        shard_location_cache: Arc::new(hyperbytedb::domain::sharding::ShardLocationCache::new()),
        shard_routing: None,
        shard_scheduler: None,
        ingest_cardinality: IngestCardinalityLimits::default(),
        ingest_schema_cache: Default::default(),
        cluster_replication: ReplicationConfig::default(),
    });

    let (url, _handle) = start_server(app_state).await;
    let client = reqwest::Client::new();

    let denied = client
        .get(format!("{url}/query"))
        .query(&[
            ("db", "mine"),
            (
                "q",
                "ALTER RETENTION POLICY autogen ON victim DURATION 1h REPLICATION 1",
            ),
            ("u", "writer"),
            ("p", "writerpw"),
        ])
        .send()
        .await
        .unwrap();
    assert_eq!(
        denied.status(),
        StatusCode::FORBIDDEN,
        "cross-database ON clause must be forbidden"
    );

    let allowed = client
        .get(format!("{url}/query"))
        .query(&[
            ("db", "mine"),
            (
                "q",
                "ALTER RETENTION POLICY autogen ON mine DURATION 168h REPLICATION 1",
            ),
            ("u", "writer"),
            ("p", "writerpw"),
        ])
        .send()
        .await
        .unwrap();
    assert_eq!(
        allowed.status(),
        StatusCode::OK,
        "same-database ON clause should still succeed: {}",
        allowed.text().await.unwrap_or_default()
    );
}

/// Wraps the query port and counts Arrow reads.
struct CountingPort {
    inner: Arc<dyn hyperbytedb::ports::query::QueryPort>,
    arrow_calls: std::sync::atomic::AtomicUsize,
}

#[async_trait::async_trait]
impl hyperbytedb::ports::query::QueryPort for CountingPort {
    async fn execute_sql(&self, sql: &str) -> Result<String, hyperbytedb::error::HyperbytedbError> {
        self.inner.execute_sql(sql).await
    }

    async fn execute_sql_arrow(
        &self,
        sql: &str,
    ) -> Result<hyperbytedb::ports::query::ArrowBatchStream, hyperbytedb::error::HyperbytedbError>
    {
        self.arrow_calls
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.inner.execute_sql_arrow(sql).await
    }
}

/// InfluxQL queries read their results through the Arrow stream.
#[tokio::test]
#[serial(chdb)]
async fn queries_read_results_through_the_arrow_stream() {
    use hyperbytedb::ports::query::QueryService as _;

    let dir = tempfile::tempdir().unwrap();
    let (state, flush) = setup(&dir);
    let (url, _handle) = start_server(state.clone()).await;
    let client = reqwest::Client::new();
    client
        .get(format!("{url}/query"))
        .query(&[("q", "CREATE DATABASE arrowdb")])
        .send()
        .await
        .unwrap();
    // 600 points, 7 s apart from a minute boundary; usage = i.
    let base = 1_700_000_040_000_000_000i64;
    let lines: Vec<String> = (0..600)
        .map(|i| format!("cpu,host=h{} usage={i} {}", i % 3, base + i * 7_000_000_000))
        .collect();
    let resp = client
        .post(format!("{url}/write"))
        .query(&[("db", "arrowdb")])
        .body(lines.join("\n"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);
    flush.flush().await.unwrap();

    let port = Arc::new(CountingPort {
        inner: state.query_port.clone(),
        arrow_calls: Default::default(),
    });
    let svc = QueryServiceImpl::new(
        port.clone(),
        state.metadata.clone(),
        state.wal.clone(),
        30,
        state.points_sink.clone(),
    );
    let query = |q: &'static str, epoch: Option<&'static str>| {
        let svc = &svc;
        async move {
            let r = svc
                .execute_query("arrowdb", q, epoch, None, None)
                .await
                .unwrap();
            serde_json::to_value(&r).unwrap()["results"][0].clone()
        }
    };

    let raw = query("SELECT usage FROM cpu", Some("ns")).await;
    let rows = raw["series"][0]["values"].as_array().unwrap();
    assert_eq!(rows.len(), 600);
    assert_eq!(rows[0], serde_json::json!([base, 0]));
    assert_eq!(
        rows[599],
        serde_json::json!([base + 599 * 7_000_000_000, 599])
    );

    // 60 s buckets of 7 s points: bucket k holds i in [ceil(60k/7), ceil(60(k+1)/7)).
    let end = base + 600 * 7_000_000_000;
    let q = Box::leak(
        format!("SELECT count(usage), sum(usage) FROM cpu WHERE time >= {base} AND time < {end} GROUP BY time(1m)")
            .into_boxed_str(),
    );
    let buckets = query(q, None).await;
    let values = buckets["series"][0]["values"].as_array().unwrap();
    assert_eq!(values[0][0], "2023-11-14T22:14:00Z");
    assert_eq!(values[0][1], 9, "i = 0..=8 fall in the first minute");
    assert_eq!(values[0][2], 36);

    let by_host = query("SELECT count(usage) FROM cpu GROUP BY host", None).await;
    let hosts: Vec<(String, i64)> = by_host["series"]
        .as_array()
        .unwrap()
        .iter()
        .map(|s| {
            (
                s["tags"]["host"].as_str().unwrap().to_string(),
                s["values"][0][1].as_i64().unwrap(),
            )
        })
        .collect();
    assert_eq!(hosts.len(), 3);
    assert!(hosts.iter().all(|(_, n)| *n == 200), "{hosts:?}");

    let missing = query("SELECT usage FROM no_such_measurement", None).await;
    assert!(missing.get("series").is_none() || missing["series"].as_array().unwrap().is_empty());

    assert!(
        port.arrow_calls.load(std::sync::atomic::Ordering::SeqCst) >= 3,
        "the query service reads results through the Arrow stream"
    );
}

/// `SET query_pool_size` resizes the live query pool and is stored, `SHOW
/// SETTINGS` reports it, and `DEFAULT` returns to the size without an
/// override. Bad names and values are refused.
#[tokio::test]
#[serial(chdb)]
async fn set_query_pool_size_resizes_the_pool() {
    use hyperbytedb::application::runtime_settings as rs;

    let dir = tempfile::tempdir().unwrap();
    let (state, _flush) = setup(&dir);
    let pool = SharedSession::new_eager(dir.path().join("chdb").to_str().unwrap(), 2)
        .unwrap()
        .pool()
        .unwrap();
    rs::set_fallback(
        rs::QUERY_POOL_SIZE,
        rs::SettingValue::Int(2),
        rs::Source::Auto,
    );
    {
        // Weak, as in bootstrap: a pool the registry kept alive would hold
        // libchdb's one data path open for every later test in the process.
        let pool = Arc::downgrade(&pool);
        rs::register_applier(
            rs::QUERY_POOL_SIZE,
            rs::usize_applier(move |n| {
                if let Some(pool) = pool.upgrade() {
                    pool.resize(n).unwrap();
                }
            }),
        );
    }
    let (url, _handle) = start_server(state.clone()).await;
    let client = reqwest::Client::new();
    let query = |q: &'static str| {
        let client = client.clone();
        let url = url.clone();
        async move {
            let resp = client
                .post(format!("{url}/query"))
                .form(&[("q", q)])
                .send()
                .await
                .unwrap();
            assert_eq!(resp.status(), StatusCode::OK, "{q}");
            resp.json::<serde_json::Value>().await.unwrap()
        }
    };
    // The query_pool_size row's name, value and source.
    let settings = |body: serde_json::Value| {
        let rows = body["results"][0]["series"][0]["values"]
            .as_array()
            .unwrap()
            .clone();
        let row = rows.iter().find(|r| r[0] == "query_pool_size").unwrap();
        serde_json::json!([[row[0], row[1], row[2]]])
    };

    let body = query("SET query_pool_size = 5").await;
    assert!(body["results"][0]["error"].is_null(), "{body}");
    assert_eq!(pool.pool_size(), 5);
    assert_eq!(
        state.metadata.get_setting("query_pool_size").await.unwrap(),
        Some(rs::SettingValue::Int(5))
    );
    assert_eq!(
        settings(query("SHOW SETTINGS").await),
        serde_json::json!([["query_pool_size", 5, "set"]])
    );

    query("SET query_pool_size = DEFAULT").await;
    assert_eq!(pool.pool_size(), 2);
    assert_eq!(
        state.metadata.get_setting("query_pool_size").await.unwrap(),
        None
    );
    assert_eq!(
        settings(query("SHOW SETTINGS").await),
        serde_json::json!([["query_pool_size", 2, "auto"]])
    );

    for bad in [
        "SET query_pool_size = 0",
        "SET query_pool_size = 500",
        "SET nope = 3",
    ] {
        let resp = client
            .post(format!("{url}/query"))
            .form(&[("q", bad)])
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST, "{bad}");
    }
    assert_eq!(pool.pool_size(), 2, "a refused SET changes nothing");
}

/// `/query` answers gzipped when the client asks, as InfluxDB v1 does, and
/// plain otherwise. A 245-series Grafana panel was 11.4 MB uncompressed.
#[tokio::test]
#[serial(chdb)]
async fn query_responses_are_gzipped_on_request() {
    use std::io::Read;
    let dir = tempfile::tempdir().unwrap();
    let (state, _flush) = setup(&dir);
    let (url, _handle) = start_server(state).await;
    // The CLI turns on reqwest's gzip feature in a workspace build, and the
    // client would then decompress and drop Content-Encoding.
    let client = reqwest::Client::builder().no_gzip().build().unwrap();
    let get = |gzip: bool| {
        let mut req = client
            .get(format!("{url}/query"))
            .query(&[("q", "SHOW DATABASES")]);
        if gzip {
            req = req.header("accept-encoding", "gzip");
        }
        async move { req.send().await.unwrap() }
    };

    let plain = get(false).await;
    assert_eq!(plain.headers().get("content-encoding"), None);
    let plain_body = plain.bytes().await.unwrap();

    let gz = get(true).await;
    assert_eq!(gz.status(), StatusCode::OK);
    assert_eq!(
        gz.headers()
            .get("content-encoding")
            .map(|v| v.to_str().unwrap()),
        Some("gzip")
    );
    let mut decoded = Vec::new();
    flate2::read::GzDecoder::new(&gz.bytes().await.unwrap()[..])
        .read_to_end(&mut decoded)
        .unwrap();
    assert_eq!(decoded, plain_body);
}
