use std::sync::Arc;

use async_trait::async_trait;
use chdb_rust::format::OutputFormat;

use crate::adapters::chdb::session::{SharedSession, execute_connection};
use crate::error::HyperbytedbError;
use crate::ports::query::{ArrowBatchStream, QueryPort};

/// Multi-connection chDB query adapter.
///
/// Reads and writes share a [`SharedSession`] connection pool: each pooled
/// `Connection` has its own `ChdbClient` mutex, so concurrent
/// `spawn_blocking` tasks can overlap when `chdb.pool_size > 1`. The optional
/// `Semaphore` caps in-flight queries so we don't oversubscribe the Tokio
/// blocking pool.
pub struct ChdbQueryAdapter {
    session: SharedSession,
    /// Replaced whole by `SET max_concurrent_queries`: queries holding the
    /// old one's permits finish under it.
    concurrency_limit: parking_lot::RwLock<Option<Arc<tokio::sync::Semaphore>>>,
}

impl ChdbQueryAdapter {
    /// Build the adapter with lazy pool initialisation and no concurrency cap.
    /// Used by tests, which create many adapters in one process; only the first
    /// one to actually run a query binds the singleton engine.
    pub fn new(data_path: &str) -> Result<Self, HyperbytedbError> {
        std::fs::create_dir_all(data_path).map_err(|e| {
            HyperbytedbError::Chdb(format!("failed to create chDB data dir: {e}").into())
        })?;
        tracing::info!(path = %data_path, "chDB connection pool will be built lazily on first query");
        Ok(Self {
            session: SharedSession::new(data_path),
            concurrency_limit: parking_lot::RwLock::new(None),
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
        let adapter = Self {
            session,
            concurrency_limit: parking_lot::RwLock::new(None),
        };
        adapter.set_concurrency_limit(max_concurrent);
        adapter
    }

    /// Cap concurrent queries at `max_concurrent` (`0` removes the cap).
    pub fn set_concurrency_limit(&self, max_concurrent: usize) {
        let limit = if max_concurrent > 0 {
            tracing::info!(limit = max_concurrent, "chDB concurrency cap enabled");
            Some(Arc::new(tokio::sync::Semaphore::new(max_concurrent)))
        } else {
            tracing::info!("chDB concurrency cap disabled (unbounded)");
            None
        };
        *self.concurrency_limit.write() = limit;
    }

    async fn acquire(&self) -> Result<Option<tokio::sync::OwnedSemaphorePermit>, HyperbytedbError> {
        let sem = self.concurrency_limit.read().clone();
        match sem {
            Some(sem) => sem.acquire_owned().await.map(Some).map_err(|_| {
                HyperbytedbError::Internal("chDB concurrency semaphore closed".into())
            }),
            None => Ok(None),
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
                    .map_err(|e| HyperbytedbError::Chdb(crate::error::ChainedError::from_error(e)))
            })
        })
        .await
        .map_err(|e| HyperbytedbError::Internal(format!("chDB ping join error: {e}").into()))?
    }

    async fn execute_sql(&self, sql: &str) -> Result<String, HyperbytedbError> {
        let _permit = self.acquire().await?;

        tracing::debug!(sql = sql, "executing chDB query");

        let pool = self.session.pool()?;
        let sql_owned = sql.to_string();

        let result = tokio::task::spawn_blocking(move || {
            pool.with_connection(|conn| {
                let qr = execute_connection(conn, &sql_owned, OutputFormat::JSONEachRow);

                match qr {
                    Ok(result) => result
                        .data_utf8()
                        .map_err(|e| HyperbytedbError::Chdb(crate::error::ChainedError::from_error(e))),
                    Err(e) => {
                        let msg = e.to_string();
                        if is_missing_table_error(&msg) {
                            tracing::warn!(error = %msg, "chDB missing-table error, treating as empty result");
                            Ok(String::new())
                        } else {
                            Err(HyperbytedbError::Chdb(msg.into()))
                        }
                    }
                }
            })
        })
        .await
        .map_err(|e| HyperbytedbError::Internal(format!("chDB task join error: {e}").into()))??;

        tracing::debug!(result_len = result.len(), "chDB query completed");
        Ok(result)
    }

    /// Arrow streaming read: no text is formatted or parsed, and only the
    /// batches in flight are held.
    async fn execute_sql_arrow(&self, sql: &str) -> Result<ArrowBatchStream, HyperbytedbError> {
        let permit = self.acquire().await?;
        tracing::debug!(sql = sql, "executing chDB query (arrow stream)");
        let pool = self.session.pool()?;
        let sql_owned = sql.to_string();
        let (tx, rx) = tokio::sync::mpsc::channel(ARROW_STREAM_DEPTH);
        // The connection and the concurrency permit are held until the
        // stream ends or the receiver is dropped, which cancels the query.
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            pool.with_connection_mut(|conn| {
                let mut stream = match conn.query_stream_arrow(&sql_owned) {
                    Ok(s) => s,
                    Err(e) => {
                        send_stream_error(&tx, &e.to_string());
                        return;
                    }
                };
                loop {
                    match stream.next_batch() {
                        Ok(Some(batch)) => {
                            if tx.blocking_send(Ok(batch)).is_err() {
                                stream.cancel();
                                return;
                            }
                        }
                        Ok(None) => return,
                        Err(e) => {
                            send_stream_error(&tx, &e.to_string());
                            return;
                        }
                    }
                }
            });
        });
        Ok(rx)
    }
}

/// Batches buffered between chDB and the consumer.
const ARROW_STREAM_DEPTH: usize = 2;

/// Errors that mean "the table is not there (yet)": a query on a measurement
/// that has never been flushed. Treated as an empty result.
fn is_missing_table_error(msg: &str) -> bool {
    msg.contains("CANNOT_EXTRACT_TABLE_STRUCTURE")
        || msg.contains("no files with provided path")
        || msg.contains("CANNOT_STAT")
        || msg.contains("UNKNOWN_TABLE")
        || msg.contains("Code: 60")
}

fn send_stream_error(
    tx: &tokio::sync::mpsc::Sender<Result<arrow::record_batch::RecordBatch, HyperbytedbError>>,
    msg: &str,
) {
    if is_missing_table_error(msg) {
        tracing::warn!(error = %msg, "chDB missing-table error, treating as empty result");
        return;
    }
    let _ = tx.blocking_send(Err(HyperbytedbError::Chdb(msg.to_string().into())));
}
