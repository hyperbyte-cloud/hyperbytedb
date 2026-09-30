use async_trait::async_trait;

use crate::domain::continuous_query::ContinuousQueryDef;
use crate::domain::query_result::QueryResponse;
use crate::error::HyperbytedbError;

/// Result of a single continuous query execution.
pub struct CqRunResult {
    pub window: crate::domain::cq_schedule::CqWindow,
    pub points_written: u64,
    pub duration_ms: u64,
}

/// A query result as Arrow record batches, in order. The channel closes after
/// the last batch; an `Err` item ends the stream.
pub type ArrowBatchStream =
    tokio::sync::mpsc::Receiver<Result<arrow::record_batch::RecordBatch, HyperbytedbError>>;

/// A stream with no batches: an empty result.
pub fn empty_arrow_stream() -> ArrowBatchStream {
    let (_tx, rx) = tokio::sync::mpsc::channel(1);
    rx
}

/// Low-level query port for executing raw SQL against the storage engine.
#[async_trait]
pub trait QueryPort: Send + Sync {
    /// Execute a ClickHouse SQL statement and return its text output
    /// (JSONEachRow unless the SQL names a `FORMAT`). For DDL, inserts and
    /// small internal lookups; query results are read with
    /// [`Self::execute_sql_arrow`].
    async fn execute_sql(&self, sql: &str) -> Result<String, HyperbytedbError>;

    /// Stream a query's result as Arrow record batches, one at a time. A
    /// missing table yields an empty stream.
    async fn execute_sql_arrow(&self, sql: &str) -> Result<ArrowBatchStream, HyperbytedbError>;

    /// Cheap end-to-end liveness probe for `/health/ready`. Implementations
    /// that don't need a real probe (e.g. mocks) can take the default no-op.
    async fn ping(&self) -> Result<(), HyperbytedbError> {
        Ok(())
    }
}

/// Application-level query service port for executing TimeseriesQL queries.
#[async_trait]
pub trait QueryService: Send + Sync {
    async fn execute_query(
        &self,
        db: &str,
        query: &str,
        epoch: Option<&str>,
        retention_policy: Option<&str>,
        caller: Option<&crate::domain::user::StoredUser>,
    ) -> Result<QueryResponse, HyperbytedbError>;

    /// Execute one InfluxDB v1-style continuous query run.
    async fn execute_continuous_query(
        &self,
        cq: &mut ContinuousQueryDef,
        now: chrono::DateTime<chrono::Utc>,
    ) -> Result<CqRunResult, HyperbytedbError>;
}
