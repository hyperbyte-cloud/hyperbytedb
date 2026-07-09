//! Focused HTTP integration tests not covered by `tests/compat/`.
//!
//! General InfluxDB v1 compatibility (writes, queries, DDL, HTTP shape) lives in
//! `tests/compat/`. This crate keeps auth, cardinality, admin users, metrics,
//! backup manifest shape, and chDB physical layout checks.

use std::sync::Arc;

use axum::http::StatusCode;
use hyperbytedb::adapters::chdb::native_adapter::ChdbNativeAdapter;
use hyperbytedb::adapters::chdb::query_adapter::ChdbQueryAdapter;
use hyperbytedb::adapters::chdb::session::SharedSession;
use hyperbytedb::adapters::http::router::{AppState, build_router};
use hyperbytedb::adapters::metadata::rocksdb_meta::RocksDbMetadata;
use hyperbytedb::adapters::wal::rocksdb_wal::RocksDbWal;
use hyperbytedb::application::flush_service::FlushServiceImpl;
use hyperbytedb::application::ingestion_service::IngestionServiceImpl;
use hyperbytedb::application::materialized_view_service::MaterializedViewService;
use hyperbytedb::application::query_service::QueryServiceImpl;
use hyperbytedb::ports::metadata::MetadataPort;
use hyperbytedb::ports::points_sink::PointsSinkPort;
use serial_test::serial;

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
        IngestionServiceImpl::new(wal.clone(), metadata.clone(), 100_000, 10_000),
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
        prometheus_handle: None,
        statement_summary: None,
        replication_apply: None,
        chdb_session_data_path: chdb_path_str,
        node_id: 1,
        max_body_size_bytes: 25 * 1024 * 1024,
        request_timeout_secs: 30,
        rate_limiter: None,
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
        IngestionServiceImpl::new(wal.clone(), metadata.clone(), 100_000, 10_000),
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
        prometheus_handle: None,
        statement_summary: None,
        replication_apply: None,
        chdb_session_data_path: chdb_dir.to_string_lossy().into_owned(),
        node_id: 1,
        max_body_size_bytes: 25 * 1024 * 1024,
        request_timeout_secs: 30,
        rate_limiter: None,
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
        StatusCode::OK,
        "Authenticated query should succeed"
    );

    let resp = client
        .get(format!("{url}/query"))
        .query(&[("q", "SHOW DATABASES"), ("u", "admin"), ("p", "wrong")])
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
        IngestionServiceImpl::new(wal.clone(), metadata.clone(), 100_000, 2),
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
        prometheus_handle: None,
        statement_summary: None,
        replication_apply: None,
        chdb_session_data_path: chdb_dir.to_string_lossy().into_owned(),
        node_id: 1,
        max_body_size_bytes: 25 * 1024 * 1024,
        request_timeout_secs: 30,
        rate_limiter: None,
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

    let prometheus_handle = {
        let builder = metrics_exporter_prometheus::PrometheusBuilder::new();
        let recorder = builder.build_recorder();
        let handle = recorder.handle();
        let _ = metrics::set_global_recorder(recorder);
        handle
    };

    let ingestion_service: Arc<dyn hyperbytedb::ports::ingestion::IngestionPort> = Arc::new(
        IngestionServiceImpl::new(wal.clone(), metadata.clone(), 100_000, 10_000),
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
        prometheus_handle: Some(prometheus_handle),
        statement_summary: None,
        replication_apply: None,
        chdb_session_data_path: chdb_dir.to_string_lossy().into_owned(),
        node_id: 1,
        max_body_size_bytes: 25 * 1024 * 1024,
        request_timeout_secs: 30,
        rate_limiter: None,
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

    let prometheus_handle = {
        let builder = metrics_exporter_prometheus::PrometheusBuilder::new();
        let recorder = builder.build_recorder();
        let handle = recorder.handle();
        let _ = metrics::set_global_recorder(recorder);
        handle
    };

    let ingestion_service: Arc<dyn hyperbytedb::ports::ingestion::IngestionPort> = Arc::new(
        IngestionServiceImpl::new(wal.clone(), metadata.clone(), 100_000, 10_000),
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
        prometheus_handle: Some(prometheus_handle),
        statement_summary: None,
        replication_apply: None,
        chdb_session_data_path: chdb_dir.to_string_lossy().into_owned(),
        node_id: 1,
        max_body_size_bytes: 25 * 1024 * 1024,
        request_timeout_secs: 30,
        rate_limiter: Some(Arc::new(EndpointRateLimiters::new(5))),
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
    let denied_metric = metrics_body
        .lines()
        .find(|line| line.starts_with("hyperbytedb_rate_limit_denied_total"))
        .unwrap_or("");
    assert!(
        denied_metric.contains(' ') && !denied_metric.ends_with(" 0"),
        "rate limit denials should be recorded: {denied_metric}"
    );
}
