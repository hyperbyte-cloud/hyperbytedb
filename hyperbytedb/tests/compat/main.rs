//! InfluxDB v1 compatibility test suite.
//!
//! Uses the project's internal APIs directly (not HTTP) plus HTTP-level tests.

mod concurrent_tests;
mod ddl_tests;
mod error_tests;
mod http_tests;
mod metadata_tests;
mod query_tests;
mod write_tests;

use std::sync::Arc;

use async_trait::async_trait;
use hyperbytedb::adapters::chdb::native_adapter::ChdbNativeAdapter;
use hyperbytedb::adapters::chdb::query_adapter::ChdbQueryAdapter;
use hyperbytedb::adapters::chdb::session::SharedSession;
use hyperbytedb::adapters::http::router::QueryService;
use hyperbytedb::adapters::metadata::rocksdb_meta::RocksDbMetadata;
use hyperbytedb::adapters::wal::rocksdb_wal::RocksDbWal;
use hyperbytedb::application::flush_service::FlushServiceImpl;
use hyperbytedb::application::ingestion_service::IngestionServiceImpl;
use hyperbytedb::application::query_service::QueryServiceImpl;
use hyperbytedb::domain::point::Point;
use hyperbytedb::domain::query_result::QueryResponse;
use hyperbytedb::error::HyperbytedbError;
use hyperbytedb::ports::ingestion::{IngestionPort, WritePayloadFormat};
use hyperbytedb::ports::metadata::MetadataPort;
use hyperbytedb::ports::points_sink::{PointsSinkPort, WriteAck};
use hyperbytedb::ports::query::QueryPort;
use hyperbytedb::ports::wal::WalPort;

/// Mock QueryPort that returns empty results. Used when chDB is not required
/// (metadata-only tests, DDL, error handling).
struct MockQueryPort;

#[async_trait]
impl QueryPort for MockQueryPort {
    async fn execute_sql(&self, _sql: &str) -> Result<String, HyperbytedbError> {
        Ok(String::new())
    }
}

struct NoopPointsSink;

#[async_trait]
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

/// Full test context with all services. Uses chDB for query execution.
pub struct TestContext {
    pub wal: Arc<dyn WalPort>,
    pub metadata: Arc<dyn MetadataPort>,
    pub query_service: QueryServiceImpl,
    pub ingestion: IngestionServiceImpl,
    pub flush_service: Arc<FlushServiceImpl>,
    _tmpdir: tempfile::TempDir,
}

impl TestContext {
    /// Create a TestContext that uses chDB for queries.
    /// Fails if chDB is not available.
    pub fn new() -> Result<Self, HyperbytedbError> {
        let tmpdir = tempfile::tempdir().map_err(|e| HyperbytedbError::Internal(e.to_string()))?;
        let root = tmpdir.path();

        let wal_path = root.join("wal");
        let meta_path = root.join("meta");
        let chdb_path = root.join("chdb");

        std::fs::create_dir_all(&wal_path)?;
        std::fs::create_dir_all(&meta_path)?;
        std::fs::create_dir_all(&chdb_path)?;

        let wal = Arc::new(RocksDbWal::open(wal_path)?);
        let metadata = Arc::new(RocksDbMetadata::open(meta_path)?);

        let shared = SharedSession::new_eager(chdb_path.to_str().unwrap())?;
        let query_port: Arc<dyn QueryPort> =
            Arc::new(ChdbQueryAdapter::from_shared(shared.clone(), 0));
        let points_sink: Arc<dyn PointsSinkPort> = Arc::new(ChdbNativeAdapter::new(shared));

        let query_service = QueryServiceImpl::new(
            query_port,
            metadata.clone(),
            wal.clone(),
            30,
            points_sink.clone(),
        );

        let ingestion = IngestionServiceImpl::new(wal.clone(), metadata.clone(), 100_000, 10_000);

        let flush_service = Arc::new(FlushServiceImpl::new(wal.clone(), 0, points_sink));

        Ok(Self {
            wal,
            metadata,
            query_service,
            ingestion,
            flush_service,
            _tmpdir: tmpdir,
        })
    }

    /// Create a TestContext that uses a mock QueryPort (no chDB required).
    /// Use for metadata, DDL, ingestion, and error tests.
    pub fn new_no_chdb() -> Result<Self, HyperbytedbError> {
        let tmpdir = tempfile::tempdir().map_err(|e| HyperbytedbError::Internal(e.to_string()))?;
        let root = tmpdir.path();

        let wal_path = root.join("wal");
        let meta_path = root.join("meta");

        std::fs::create_dir_all(&wal_path)?;
        std::fs::create_dir_all(&meta_path)?;

        let wal = Arc::new(RocksDbWal::open(wal_path)?);
        let metadata = Arc::new(RocksDbMetadata::open(meta_path)?);

        let query_port: Arc<dyn QueryPort> = Arc::new(MockQueryPort);
        let points_sink: Arc<dyn PointsSinkPort> = Arc::new(NoopPointsSink);

        let query_service = QueryServiceImpl::new(
            query_port,
            metadata.clone(),
            wal.clone(),
            30,
            points_sink.clone(),
        );

        let ingestion = IngestionServiceImpl::new(wal.clone(), metadata.clone(), 100_000, 10_000);

        let flush_service = Arc::new(FlushServiceImpl::new(wal.clone(), 0, points_sink));

        Ok(Self {
            wal,
            metadata,
            query_service,
            ingestion,
            flush_service,
            _tmpdir: tmpdir,
        })
    }

    pub async fn write_and_flush(
        &self,
        db: &str,
        line_protocol: &str,
    ) -> Result<(), HyperbytedbError> {
        self.ingestion
            .ingest(
                db,
                None,
                None,
                line_protocol.as_bytes(),
                WritePayloadFormat::LineProtocol,
            )
            .await?;
        self.flush_service.flush().await?;
        Ok(())
    }

    /// Execute an InfluxQL query and return the response.
    pub async fn query(&self, db: &str, q: &str) -> Result<QueryResponse, HyperbytedbError> {
        self.query_service.execute_query(db, q, None, None).await
    }
}
