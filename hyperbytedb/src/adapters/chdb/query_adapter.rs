use std::sync::Arc;

use async_trait::async_trait;
use chdb_rust::format::OutputFormat;

use crate::adapters::chdb::session::SharedSession;
use crate::application::system_trace::{self, PhaseTimer};
use crate::error::HyperbytedbError;
use crate::ports::query::QueryPort;

/// Single-session, multi-threaded chDB adapter.
///
/// libchdb only supports one active connection per process (calling
/// `chdb_connect` a second time with a different `--path` returns
/// "Connection failed"). To get parallelism we therefore keep _one_
/// session and call `execute` on it from many `spawn_blocking` tasks
/// at once. The optional `Semaphore` caps in-flight queries so we
/// don't oversubscribe the Tokio blocking pool — without it, a burst
/// of 1000 simultaneous queries would each pin a blocking thread.
///
/// The session itself lives on a [`SharedSession`] so the
/// [`crate::adapters::chdb::native_adapter::ChdbNativeAdapter`] can
/// hold a clone and write into the same chDB engine that this adapter
/// reads from.
pub struct ChdbQueryAdapter {
    session: SharedSession,
    concurrency_limit: Option<Arc<tokio::sync::Semaphore>>,
}

impl ChdbQueryAdapter {
    /// Build the adapter with lazy session initialisation and no
    /// concurrency cap. Used by tests, which create many adapters in
    /// one process; only the first one to actually run a query binds
    /// the singleton engine.
    pub fn new(data_path: &str) -> Result<Self, HyperbytedbError> {
        std::fs::create_dir_all(data_path)
            .map_err(|e| HyperbytedbError::Chdb(format!("failed to create chDB data dir: {e}")))?;
        tracing::info!(path = %data_path, "chDB session will be built lazily on first query");
        Ok(Self {
            session: SharedSession::new(data_path),
            concurrency_limit: None,
        })
    }

    /// Build the adapter, eagerly opening the chDB session and capping
    /// concurrent queries at `max_concurrent` (`0` disables the cap).
    pub fn with_concurrency_limit(
        data_path: &str,
        max_concurrent: usize,
    ) -> Result<Self, HyperbytedbError> {
        let session = SharedSession::new_eager(data_path)?;
        Ok(Self::from_shared(session, max_concurrent))
    }

    /// Build the adapter from an already-constructed [`SharedSession`].
    /// Used by [`crate::bootstrap`] so the read adapter and (when
    /// enabled) the native-write adapter point at the same singleton
    /// session.
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
        let session = self.session.get()?;
        tokio::task::spawn_blocking(move || {
            session
                .0
                .execute(
                    "SELECT 1",
                    Some(&[chdb_rust::arg::Arg::OutputFormat(OutputFormat::JSONEachRow)]),
                )
                .map(|_| ())
                .map_err(|e| HyperbytedbError::Chdb(e.to_string()))
        })
        .await
        .map_err(|e| HyperbytedbError::Internal(format!("chDB ping join error: {e}")))?
    }

    async fn execute_sql(&self, sql: &str) -> Result<String, HyperbytedbError> {
        let span = system_trace::chdb_sql_span(sql.len());
        let _guard = span.enter();
        let total_start = system_trace::start_timer();

        let mut sem_pt = PhaseTimer::start();
        // Acquire the optional concurrency permit BEFORE spawning a
        // blocking task. This keeps queued queries off the blocking
        // pool entirely, so we burn `Semaphore` waiters (cheap)
        // instead of blocking threads (expensive).
        let _permit = match self.concurrency_limit {
            Some(ref sem) => Some(Arc::clone(sem).acquire_owned().await.map_err(|_| {
                HyperbytedbError::Internal("chDB concurrency semaphore closed".into())
            })?),
            None => None,
        };
        sem_pt.record_phase_final("semaphore_wait_us");

        tracing::debug!(sql = sql, "executing chDB query");

        let session = self.session.get()?;
        let sql_owned = sql.to_string();

        let mut exec_pt = PhaseTimer::start();
        let result = tokio::task::spawn_blocking(move || {
            // No mutex: many of these tasks may run in parallel
            // against the same `Session`. libchdb serialises /
            // pipelines them internally.
            let qr = session.0.execute(
                &sql_owned,
                Some(&[chdb_rust::arg::Arg::OutputFormat(OutputFormat::JSONEachRow)]),
            );

            match qr {
                Ok(result) => result
                    .data_utf8()
                    .map_err(|e| HyperbytedbError::Chdb(e.to_string())),
                Err(e) => {
                    let msg = e.to_string();
                    // Swallow "table is not there" errors from both
                    // storage formats: in Parquet mode chDB reports
                    // missing files via CANNOT_STAT /
                    // CANNOT_EXTRACT_TABLE_STRUCTURE; in native mode
                    // it reports `UNKNOWN_TABLE` (Code: 60). Either
                    // way the right user-facing answer is "empty
                    // result", not 500.
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
        .await
        .map_err(|e| HyperbytedbError::Internal(format!("chDB task join error: {e}")))??;
        exec_pt.record_phase_final("chdb_execute_us");
        system_trace::record_usize("result_bytes", result.len());

        tracing::debug!(result_len = result.len(), "chDB query completed");
        system_trace::finish_span(&span, total_start, "chdb sql complete");
        Ok(result)
    }
}
