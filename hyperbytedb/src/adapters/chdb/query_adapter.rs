use std::sync::Arc;

use async_trait::async_trait;
use chdb_rust::format::OutputFormat;

use crate::adapters::chdb::session::{SharedSession, execute_connection};
use crate::error::HyperbytedbError;
use crate::ports::query::QueryPort;

/// Multi-connection chDB query adapter.
///
/// Reads and writes share a [`SharedSession`] connection pool: each pooled
/// `Connection` has its own `ChdbClient` mutex, so concurrent
/// `spawn_blocking` tasks can overlap when `chdb.pool_size > 1`. The optional
/// `Semaphore` caps in-flight queries so we don't oversubscribe the Tokio
/// blocking pool.
pub struct ChdbQueryAdapter {
    session: SharedSession,
    concurrency_limit: Option<Arc<tokio::sync::Semaphore>>,
}

impl ChdbQueryAdapter {
    /// Build the adapter with lazy pool initialisation and no concurrency cap.
    /// Used by tests, which create many adapters in one process; only the first
    /// one to actually run a query binds the singleton engine.
    pub fn new(data_path: &str) -> Result<Self, HyperbytedbError> {
        std::fs::create_dir_all(data_path)
            .map_err(|e| HyperbytedbError::Chdb(format!("failed to create chDB data dir: {e}")))?;
        tracing::info!(path = %data_path, "chDB connection pool will be built lazily on first query");
        Ok(Self {
            session: SharedSession::new(data_path),
            concurrency_limit: None,
        })
    }

    /// Build the adapter, eagerly opening the pool and capping concurrent
    /// queries at `max_concurrent` (`0` disables the cap).
    pub fn with_concurrency_limit(
        data_path: &str,
        max_concurrent: usize,
        pool_size: usize,
    ) -> Result<Self, HyperbytedbError> {
        let session = SharedSession::new_eager(data_path, pool_size)?;
        Ok(Self::from_shared(session, max_concurrent))
    }

    /// Build the adapter from an already-constructed [`SharedSession`].
    /// Used by [`crate::bootstrap`] so the read adapter and (when enabled) the
    /// native-write adapter point at the same pool.
    pub fn from_shared(session: SharedSession, max_concurrent: usize) -> Self {
        let concurrency_limit = if max_concurrent > 0 {
            tracing::info!(limit = max_concurrent, "chDB concurrency cap enabled");
            Some(Arc::new(tokio::sync::Semaphore::new(max_concurrent)))
        } else {
            tracing::info!("chDB concurrency cap disabled (unbounded)");
            None
        };
        Self {
            session,
            concurrency_limit,
        }
    }
}

#[async_trait]
impl QueryPort for ChdbQueryAdapter {
    /// Cheap end-to-end liveness probe for `/health/ready`.
    async fn ping(&self) -> Result<(), HyperbytedbError> {
        let pool = self.session.pool()?;
        tokio::task::spawn_blocking(move || {
            pool.with_connection(|conn| {
                execute_connection(conn, "SELECT 1", OutputFormat::JSONEachRow)
                    .map(|_| ())
                    .map_err(|e| HyperbytedbError::Chdb(e.to_string()))
            })
        })
        .await
        .map_err(|e| HyperbytedbError::Internal(format!("chDB ping join error: {e}")))?
    }

    async fn execute_sql(&self, sql: &str) -> Result<String, HyperbytedbError> {
        let _permit = match self.concurrency_limit {
            Some(ref sem) => Some(Arc::clone(sem).acquire_owned().await.map_err(|_| {
                HyperbytedbError::Internal("chDB concurrency semaphore closed".into())
            })?),
            None => None,
        };

        tracing::debug!(sql = sql, "executing chDB query");

        let pool = self.session.pool()?;
        let sql_owned = sql.to_string();

        let result = tokio::task::spawn_blocking(move || {
            pool.with_connection(|conn| {
                let qr = execute_connection(conn, &sql_owned, OutputFormat::JSONEachRow);

                match qr {
                    Ok(result) => result
                        .data_utf8()
                        .map_err(|e| HyperbytedbError::Chdb(e.to_string())),
                    Err(e) => {
                        let msg = e.to_string();
                        if msg.contains("CANNOT_EXTRACT_TABLE_STRUCTURE")
                            || msg.contains("no files with provided path")
                            || msg.contains("CANNOT_STAT")
                            || msg.contains("UNKNOWN_TABLE")
                            || msg.contains("Code: 60")
                        {
                            tracing::warn!(error = %msg, "chDB missing-table error, treating as empty result");
                            Ok(String::new())
                        } else {
                            Err(HyperbytedbError::Chdb(msg))
                        }
                    }
                }
            })
        })
        .await
        .map_err(|e| HyperbytedbError::Internal(format!("chDB task join error: {e}")))??;

        tracing::debug!(result_len = result.len(), "chDB query completed");
        Ok(result)
    }
}
