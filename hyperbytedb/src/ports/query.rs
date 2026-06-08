use async_trait::async_trait;

use crate::domain::query_result::QueryResponse;
use crate::error::HyperbytedbError;

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
        caller: Option<&crate::domain::user::StoredUser>,
    ) -> Result<QueryResponse, HyperbytedbError>;
}
