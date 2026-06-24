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

/// Low-level query port for executing raw SQL against the storage engine.
#[async_trait]
pub trait QueryPort: Send + Sync {
    /// Execute a ClickHouse SQL query and return raw JSON output (JSONEachRow format).
    async fn execute_sql(&self, sql: &str) -> Result<String, HyperbytedbError>;

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
    ) -> Result<CqRunResult, HyperbytedbError> {
        let _ = (cq, now);
        Err(HyperbytedbError::Internal(
            "continuous query execution not supported".to_string(),
        ))
    }
}
