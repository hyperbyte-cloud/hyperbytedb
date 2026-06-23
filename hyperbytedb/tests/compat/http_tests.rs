//! HTTP-level compatibility tests.
//!
//! Spins up a full Hyperbytedb HTTP server and verifies exact JSON response
//! shapes, HTTP status codes, headers, and content types against InfluxDB v1
//! API behavior using `reqwest`.
//!
//! These tests use a mock QueryPort (no chDB required) for metadata and DDL
//! operations. Tests requiring SELECT query execution are marked `#[ignore]`.

use std::sync::Arc;

use async_trait::async_trait;
use axum::http::StatusCode;
use hyperbytedb::adapters::http::router::{AppState, QueryService, build_router};
use hyperbytedb::adapters::metadata::rocksdb_meta::RocksDbMetadata;
use hyperbytedb::adapters::wal::rocksdb_wal::RocksDbWal;
use hyperbytedb::application::flush_service::FlushServiceImpl;
use hyperbytedb::application::ingestion_service::IngestionServiceImpl;
use hyperbytedb::application::materialized_view_service::MaterializedViewService;
use hyperbytedb::application::query_service::QueryServiceImpl;
use hyperbytedb::domain::point::Point;
use hyperbytedb::error::HyperbytedbError;
use hyperbytedb::ports::metadata::MetadataPort;
use hyperbytedb::ports::points_sink::{PointsSinkPort, WriteAck};
use hyperbytedb::ports::query::QueryPort;

struct MockQueryPort;

#[async_trait]
impl QueryPort for MockQueryPort {
    async fn execute_sql(&self, sql: &str) -> Result<String, HyperbytedbError> {
        if sql.contains("FROM system.tables") {
            Ok("1".into())
        } else {
            Ok(String::new())
        }
    }
}

struct NoopPointsSink;

#[async_trait::async_trait]
impl PointsSinkPort for NoopPointsSink {
    async fn write_points(
        &self,
        _db: &str,
        _rp: &str,
        _measurement: &str,
        _origins: &[u64],
        _ingest_seq_base: u64,
        points: &[Point],
    ) -> Result<WriteAck, HyperbytedbError> {
        Ok(WriteAck {
            min_time: 0,
            max_time: 0,
            row_count: points.len(),
        })
    }
}

struct HttpTestContext {
    url: String,
    client: reqwest::Client,
    #[allow(dead_code)]
    flush: Arc<FlushServiceImpl>,
    _handle: tokio::task::JoinHandle<()>,
    _tmpdir: tempfile::TempDir,
}

impl HttpTestContext {
    async fn new() -> Self {
        let tmpdir = tempfile::tempdir().unwrap();
        let wal_dir = tmpdir.path().join("wal");
        let meta_dir = tmpdir.path().join("meta");
        let chdb_dir = tmpdir.path().join("chdb");

        std::fs::create_dir_all(&wal_dir).unwrap();
        std::fs::create_dir_all(&meta_dir).unwrap();
        std::fs::create_dir_all(&chdb_dir).unwrap();

        let wal = Arc::new(RocksDbWal::open(&wal_dir).unwrap());
        let metadata = Arc::new(RocksDbMetadata::open(&meta_dir).unwrap());
        let query_port: Arc<dyn QueryPort> = Arc::new(MockQueryPort);
        let points_sink: Arc<dyn PointsSinkPort> = Arc::new(NoopPointsSink);

        let ingestion: Arc<dyn hyperbytedb::ports::ingestion::IngestionPort> = Arc::new(
            IngestionServiceImpl::new(wal.clone(), metadata.clone(), 100_000, 10_000),
        );

        let query_service: Arc<dyn QueryService> = Arc::new(QueryServiceImpl::new(
            query_port.clone(),
            metadata.clone(),
            wal.clone(),
            30,
            points_sink.clone(),
        ));

        let flush = Arc::new(FlushServiceImpl::new(wal.clone(), 0, points_sink.clone()));

        let mv_service = Arc::new(MaterializedViewService::new(
            metadata.clone(),
            query_port.clone(),
            points_sink.clone(),
        ));

        let state = Arc::new(AppState {
            ingestion,
            query: query_service,
            query_port,
            metadata: metadata.clone() as Arc<dyn MetadataPort>,
            wal: wal.clone(),
            points_sink,
            mv_service,
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

        let app = build_router(state);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let url = format!("http://{}", addr);
        let handle = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        Self {
            url,
            client: reqwest::Client::new(),
            flush,
            _handle: handle,
            _tmpdir: tmpdir,
        }
    }

    async fn create_db(&self, name: &str) {
        self.client
            .get(format!("{}/query", self.url))
            .query(&[("q", format!("CREATE DATABASE {name}"))])
            .send()
            .await
            .unwrap();
    }

    async fn write(&self, db: &str, body: &str) -> reqwest::Response {
        self.client
            .post(format!("{}/write", self.url))
            .query(&[("db", db)])
            .body(body.to_string())
            .send()
            .await
            .unwrap()
    }

    async fn query_raw(&self, params: &[(&str, &str)]) -> reqwest::Response {
        self.client
            .get(format!("{}/query", self.url))
            .query(params)
            .send()
            .await
            .unwrap()
    }

    async fn query_json(&self, params: &[(&str, &str)]) -> serde_json::Value {
        let resp = self.query_raw(params).await;
        let body = resp.text().await.unwrap();
        serde_json::from_str(&body).unwrap_or_else(|e| panic!("Invalid JSON: {e}\n{body}"))
    }
}

// ---------------------------------------------------------------------------
// Ping / Health
// ---------------------------------------------------------------------------

#[tokio::test]
async fn ping_returns_204_with_version_header() {
    let ctx = HttpTestContext::new().await;
    let resp = ctx
        .client
        .get(format!("{}/ping", ctx.url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);
    assert!(
        resp.headers().contains_key("x-influxdb-version"),
        "Ping should return X-Influxdb-Version header"
    );
}

#[tokio::test]
async fn head_ping_returns_204() {
    let ctx = HttpTestContext::new().await;
    let resp = ctx
        .client
        .head(format!("{}/ping", ctx.url))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::NO_CONTENT,
        "HEAD /ping should return 204 (used by Telegraf for connectivity checks)"
    );
}

#[tokio::test]
async fn health_returns_200() {
    let ctx = HttpTestContext::new().await;
    let resp = ctx
        .client
        .get(format!("{}/health", ctx.url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

// ---------------------------------------------------------------------------
// Write API
// ---------------------------------------------------------------------------

#[tokio::test]
async fn write_returns_204_on_success() {
    let ctx = HttpTestContext::new().await;
    ctx.create_db("testdb").await;

    let resp = ctx.write("testdb", "cpu value=42.0 1000000000").await;
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);
}

#[tokio::test]
async fn write_missing_db_returns_400() {
    let ctx = HttpTestContext::new().await;
    let resp = ctx
        .client
        .post(format!("{}/write", ctx.url))
        .body("cpu value=1.0")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn write_nonexistent_db_returns_404() {
    let ctx = HttpTestContext::new().await;
    let resp = ctx.write("nonexistent", "cpu value=1.0 1000000000").await;
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn write_invalid_line_protocol_returns_400() {
    let ctx = HttpTestContext::new().await;
    ctx.create_db("testdb").await;

    let resp = ctx.write("testdb", "totally invalid!!!").await;
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn write_gzip_body_accepted() {
    use flate2::Compression;
    use flate2::write::GzEncoder;
    use std::io::Write;

    let ctx = HttpTestContext::new().await;
    ctx.create_db("testdb").await;

    let line = "cpu,host=srv1 value=42.5 1000000000\n";
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(line.as_bytes()).unwrap();
    let compressed = encoder.finish().unwrap();

    let resp = ctx
        .client
        .post(format!("{}/write", ctx.url))
        .query(&[("db", "testdb")])
        .header("Content-Encoding", "gzip")
        .body(compressed)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);
}

#[tokio::test]
async fn write_with_precision_param() {
    let ctx = HttpTestContext::new().await;
    ctx.create_db("testdb").await;

    let resp = ctx
        .client
        .post(format!("{}/write", ctx.url))
        .query(&[("db", "testdb"), ("precision", "s")])
        .body("cpu value=1.0 1234567890")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);
}

#[tokio::test]
async fn write_msgpack_returns_204() {
    use hyperbytedb::domain::point::FieldValue;
    use serde::Serialize;
    use std::collections::BTreeMap;

    #[derive(Serialize)]
    struct MsgpackPointWire {
        measurement: String,
        tags: BTreeMap<String, String>,
        fields: BTreeMap<String, FieldValue>,
        timestamp: Option<i64>,
    }

    let ctx = HttpTestContext::new().await;
    ctx.create_db("testdb").await;

    let mut tags = BTreeMap::new();
    tags.insert("host".into(), "srv1".into());
    let mut fields = BTreeMap::new();
    fields.insert("value".into(), FieldValue::Float(42.0));
    let batch = vec![MsgpackPointWire {
        measurement: "cpu".into(),
        tags,
        fields,
        timestamp: Some(1_000_000_i64),
    }];
    let body = rmp_serde::to_vec_named(&batch).expect("msgpack encode");

    let resp = ctx
        .client
        .post(format!("{}/write", ctx.url))
        .query(&[("db", "testdb"), ("precision", "ms")])
        .header("Content-Type", "application/msgpack")
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);
}

#[tokio::test]
async fn write_msgpack_invalid_returns_400() {
    let ctx = HttpTestContext::new().await;
    ctx.create_db("testdb").await;

    let resp = ctx
        .client
        .post(format!("{}/write", ctx.url))
        .query(&[("db", "testdb")])
        .header("Content-Type", "application/msgpack")
        .body(vec![0xc0_u8]) // msgpack nil, not an array
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
#[cfg(not(feature = "columnar-ingest"))]
async fn columnar_msgpack_v1_without_feature_returns_415() {
    let ctx = HttpTestContext::new().await;
    ctx.create_db("testdb").await;

    let resp = ctx
        .client
        .post(format!("{}/write", ctx.url))
        .query(&[("db", "testdb")])
        .header(
            "Content-Type",
            "application/vnd.hyperbytedb.columnar-msgpack.v1",
        )
        .body(vec![0x80]) // minimal invalid map; rejected before body parse
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNSUPPORTED_MEDIA_TYPE);
}

#[tokio::test]
#[cfg(feature = "columnar-ingest")]
async fn write_columnar_msgpack_v1_returns_204() {
    use hyperbytedb::application::columnar_msgpack::{CONTENT_TYPE, ColumnarMsgpackBatch};
    use std::collections::BTreeMap;

    let ctx = HttpTestContext::new().await;
    ctx.create_db("testdb").await;

    let mut tags = BTreeMap::new();
    tags.insert("host".into(), "srv1".into());
    let batch = ColumnarMsgpackBatch {
        measurement: "cpu".into(),
        tags,
        field: "value".into(),
        values: vec![1.0, 2.0],
        timestamps: Some(vec![1_000_000_i64, 1_000_001_i64]),
    };
    let body = rmp_serde::to_vec_named(&batch).expect("encode");

    let resp = ctx
        .client
        .post(format!("{}/write", ctx.url))
        .query(&[("db", "testdb"), ("precision", "ms")])
        .header("Content-Type", CONTENT_TYPE)
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);
}

// ---------------------------------------------------------------------------
// Query API - Response shape
// ---------------------------------------------------------------------------

#[tokio::test]
async fn query_returns_200_with_json_content_type() {
    let ctx = HttpTestContext::new().await;
    ctx.create_db("testdb").await;

    let resp = ctx.query_raw(&[("q", "SHOW DATABASES")]).await;
    assert_eq!(resp.status(), StatusCode::OK);
    let content_type = resp
        .headers()
        .get("content-type")
        .unwrap()
        .to_str()
        .unwrap();
    assert!(
        content_type.contains("application/json"),
        "Query response should be application/json, got: {content_type}"
    );
}

#[tokio::test]
async fn query_missing_q_returns_400() {
    let ctx = HttpTestContext::new().await;
    let resp = ctx.query_raw(&[("db", "testdb")]).await;
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn show_databases_json_shape() {
    let ctx = HttpTestContext::new().await;
    ctx.create_db("mydb").await;

    let json = ctx.query_json(&[("q", "SHOW DATABASES")]).await;

    assert!(json["results"].is_array());
    assert_eq!(json["results"][0]["statement_id"], 0);
    assert!(json["results"][0]["series"].is_array());
    assert_eq!(json["results"][0]["series"][0]["name"], "databases");
    assert_eq!(json["results"][0]["series"][0]["columns"][0], "name");

    let values = json["results"][0]["series"][0]["values"]
        .as_array()
        .unwrap();
    let db_names: Vec<&str> = values.iter().filter_map(|row| row[0].as_str()).collect();
    assert!(db_names.contains(&"mydb"));
}

#[tokio::test]
async fn show_measurements_json_shape() {
    let ctx = HttpTestContext::new().await;
    ctx.create_db("testdb").await;
    ctx.write(
        "testdb",
        "cpu value=1.0 1000000000\nmemory value=2.0 2000000000",
    )
    .await;

    let json = ctx
        .query_json(&[("q", "SHOW MEASUREMENTS"), ("db", "testdb")])
        .await;

    assert_eq!(json["results"][0]["statement_id"], 0);
    let series = &json["results"][0]["series"][0];
    assert_eq!(series["name"], "measurements");
    assert_eq!(series["columns"][0], "name");

    let names: Vec<&str> = series["values"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|row| row[0].as_str())
        .collect();
    assert!(names.contains(&"cpu"));
    assert!(names.contains(&"memory"));
}

#[tokio::test]
async fn show_tag_keys_json_shape() {
    let ctx = HttpTestContext::new().await;
    ctx.create_db("testdb").await;
    ctx.write("testdb", "cpu,host=srv1,region=us value=1.0 1000000000")
        .await;

    let json = ctx
        .query_json(&[("q", "SHOW TAG KEYS FROM cpu"), ("db", "testdb")])
        .await;

    let series = &json["results"][0]["series"][0];
    assert_eq!(series["name"], "cpu");
    assert_eq!(series["columns"][0], "tagKey");
}

#[tokio::test]
async fn show_field_keys_json_shape() {
    let ctx = HttpTestContext::new().await;
    ctx.create_db("testdb").await;
    ctx.write("testdb", "cpu value=42.0,count=10i 1000000000")
        .await;

    let json = ctx
        .query_json(&[("q", "SHOW FIELD KEYS FROM cpu"), ("db", "testdb")])
        .await;

    let series = &json["results"][0]["series"][0];
    assert_eq!(series["name"], "cpu");
    assert_eq!(series["columns"][0], "fieldKey");
    assert_eq!(series["columns"][1], "fieldType");
}

#[tokio::test]
async fn show_tag_values_json_shape() {
    let ctx = HttpTestContext::new().await;
    ctx.create_db("testdb").await;
    ctx.write(
        "testdb",
        "cpu,host=server01 value=1.0 1000000000\ncpu,host=server02 value=2.0 2000000000",
    )
    .await;

    let json = ctx
        .query_json(&[
            ("q", "SHOW TAG VALUES WITH KEY = \"host\""),
            ("db", "testdb"),
        ])
        .await;

    let series = &json["results"][0]["series"][0];
    assert_eq!(series["columns"][0], "key");
    assert_eq!(series["columns"][1], "value");
}

#[tokio::test]
async fn show_retention_policies_json_shape() {
    let ctx = HttpTestContext::new().await;
    ctx.create_db("testdb").await;

    let json = ctx
        .query_json(&[("q", "SHOW RETENTION POLICIES ON testdb"), ("db", "testdb")])
        .await;

    assert!(json["results"][0]["error"].is_null());
    let series = &json["results"][0]["series"][0];
    let columns: Vec<&str> = series["columns"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|v| v.as_str())
        .collect();
    assert!(columns.contains(&"name"));
    assert!(columns.contains(&"duration"));
    assert!(columns.contains(&"default"));
}

// ---------------------------------------------------------------------------
// Query API - POST form body (Telegraf style)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn query_via_post_form_body() {
    let ctx = HttpTestContext::new().await;
    ctx.create_db("testdb").await;

    let resp = ctx
        .client
        .post(format!("{}/query", ctx.url))
        .header("Content-Type", "application/x-www-form-urlencoded")
        .body("q=SHOW+DATABASES")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = resp.text().await.unwrap();
    assert!(body.contains("testdb"));
}

#[tokio::test]
async fn create_database_via_post() {
    let ctx = HttpTestContext::new().await;

    let resp = ctx
        .client
        .post(format!("{}/query", ctx.url))
        .header("Content-Type", "application/x-www-form-urlencoded")
        .body("q=CREATE+DATABASE+%22postdb%22")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let json = ctx.query_json(&[("q", "SHOW DATABASES")]).await;
    let names: Vec<&str> = json["results"][0]["series"][0]["values"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|row| row[0].as_str())
        .collect();
    assert!(names.contains(&"postdb"));
}

// ---------------------------------------------------------------------------
// Query API - CSV output
// ---------------------------------------------------------------------------

#[tokio::test]
async fn csv_output_via_accept_header() {
    let ctx = HttpTestContext::new().await;
    ctx.create_db("testdb").await;

    let resp = ctx
        .client
        .get(format!("{}/query", ctx.url))
        .query(&[("q", "SHOW DATABASES")])
        .header("Accept", "text/csv")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let content_type = resp
        .headers()
        .get("content-type")
        .unwrap()
        .to_str()
        .unwrap();
    assert!(content_type.contains("text/csv"));
    let body = resp.text().await.unwrap();
    assert!(body.contains("name"), "CSV should have header row");
    assert!(body.contains("testdb"));
}

// ---------------------------------------------------------------------------
// DDL via HTTP
// ---------------------------------------------------------------------------

#[tokio::test]
async fn create_and_drop_database_via_http() {
    let ctx = HttpTestContext::new().await;

    let resp = ctx.query_raw(&[("q", "CREATE DATABASE httpdb")]).await;
    assert_eq!(resp.status(), StatusCode::OK);

    let json = ctx.query_json(&[("q", "SHOW DATABASES")]).await;
    let names: Vec<&str> = json["results"][0]["series"][0]["values"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|row| row[0].as_str())
        .collect();
    assert!(names.contains(&"httpdb"));

    let resp = ctx.query_raw(&[("q", "DROP DATABASE httpdb")]).await;
    assert_eq!(resp.status(), StatusCode::OK);

    let json = ctx.query_json(&[("q", "SHOW DATABASES")]).await;
    let names: Vec<&str> = json["results"][0]["series"][0]["values"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|row| row[0].as_str())
        .collect();
    assert!(!names.contains(&"httpdb"));
}

#[tokio::test]
async fn retention_policy_lifecycle_via_http() {
    let ctx = HttpTestContext::new().await;
    ctx.create_db("rphttp").await;

    let resp = ctx
        .query_raw(&[(
            "q",
            "CREATE RETENTION POLICY \"short\" ON \"rphttp\" DURATION 1d REPLICATION 1",
        )])
        .await;
    assert_eq!(resp.status(), StatusCode::OK);

    let json = ctx
        .query_json(&[("q", "SHOW RETENTION POLICIES ON rphttp"), ("db", "rphttp")])
        .await;
    let body = serde_json::to_string(&json).unwrap();
    assert!(body.contains("short"), "RP 'short' should exist: {body}");

    let resp = ctx
        .query_raw(&[("q", "DROP RETENTION POLICY \"short\" ON \"rphttp\"")])
        .await;
    assert_eq!(resp.status(), StatusCode::OK);
}

// ---------------------------------------------------------------------------
// Error responses via HTTP
// ---------------------------------------------------------------------------

#[tokio::test]
async fn error_response_is_json_with_error_field() {
    let ctx = HttpTestContext::new().await;
    let resp = ctx
        .client
        .post(format!("{}/write", ctx.url))
        .body("cpu value=1.0")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);

    let content_type = resp
        .headers()
        .get("content-type")
        .unwrap()
        .to_str()
        .unwrap();
    assert!(
        content_type.contains("application/json"),
        "Error responses should be JSON"
    );

    let body = resp.text().await.unwrap();
    let json: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert!(
        json["error"].is_string(),
        "Error response should have 'error' field: {body}"
    );
}

// ---------------------------------------------------------------------------
// Version headers
// ---------------------------------------------------------------------------

#[tokio::test]
async fn query_response_has_version_header() {
    let ctx = HttpTestContext::new().await;
    let resp = ctx.query_raw(&[("q", "SHOW DATABASES")]).await;
    assert!(
        resp.headers().contains_key("x-influxdb-version"),
        "Query responses should include X-Influxdb-Version header"
    );
}

// ---------------------------------------------------------------------------
// Bind parameters
// ---------------------------------------------------------------------------

#[tokio::test]
async fn bind_parameters_substitution() {
    let ctx = HttpTestContext::new().await;
    ctx.create_db("testdb").await;
    ctx.write("testdb", "cpu,host=srv1 value=1.0 1000000000")
        .await;

    let resp = ctx
        .query_raw(&[
            ("q", "SHOW MEASUREMENTS"),
            ("db", "testdb"),
            ("params", r#"{"host":"srv1"}"#),
        ])
        .await;
    assert_eq!(resp.status(), StatusCode::OK);
}
