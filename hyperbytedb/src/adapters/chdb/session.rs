//! Shared chDB connection pool handle.
//!
//! libchdb is a process-global singleton per `--path`: a second
//! `chdb_connect` against a *different* path fails, but multiple connections
//! to the *same* path each get an independent `ChdbClient` (and
//! `client_mutex`). [`SharedSession`] opens `pool_size` such connections and
//! hands clones to the read and write adapters.

use std::sync::{Arc, OnceLock};

use chdb_rust::connection::Connection;
use chdb_rust::format::OutputFormat;
use chdb_rust::query_result::QueryResult;

use crate::adapters::chdb::connection_pool::{
    ChdbConnectionPool, DEFAULT_POOL_SIZE, MIN_POOL_SIZE, clamp_pool_size,
};
use crate::error::HyperbytedbError;

/// Execute SQL on a connection with an explicit output format.
pub(crate) fn execute_connection(
    conn: &Connection,
    query: &str,
    format: OutputFormat,
) -> Result<QueryResult, chdb_rust::error::Error> {
    conn.query(query, format)
}

/// Shared handle to a same-path chDB connection pool.
#[derive(Clone)]
pub struct SharedSession {
    data_path: String,
    pool_size: usize,
    pool: Arc<OnceLock<Result<Arc<ChdbConnectionPool>, String>>>,
}

impl SharedSession {
    /// Lazy pool holder (`pool_size` = 1). Used by tests.
    pub fn new(data_path: impl Into<String>) -> Self {
        Self::with_pool_size(data_path, MIN_POOL_SIZE)
    }

    /// Lazy pool holder with explicit size (built on first use).
    pub fn with_pool_size(data_path: impl Into<String>, pool_size: usize) -> Self {
        Self {
            data_path: data_path.into(),
            pool_size: clamp_pool_size(pool_size),
            pool: Arc::new(OnceLock::new()),
        }
    }

    /// Open the pool eagerly so bind failures surface at startup.
    pub fn new_eager(
        data_path: impl Into<String>,
        pool_size: usize,
    ) -> Result<Self, HyperbytedbError> {
        let data_path: String = data_path.into();
        let pool_size = clamp_pool_size(pool_size);
        let pool = Arc::new(ChdbConnectionPool::open(&data_path, pool_size)?);
        let cell = OnceLock::new();
        let _ = cell.set(Ok(pool));
        Ok(Self {
            data_path,
            pool_size,
            pool: Arc::new(cell),
        })
    }

    /// Default production pool size when not configured otherwise.
    pub fn default_pool_size() -> usize {
        DEFAULT_POOL_SIZE
    }

    pub fn data_path(&self) -> &str {
        &self.data_path
    }

    pub fn configured_pool_size(&self) -> usize {
        self.pool_size
    }

    fn ensure_pool(&self) -> Result<Arc<ChdbConnectionPool>, HyperbytedbError> {
        let res = self.pool.get_or_init(|| {
            tracing::info!(
                path = %self.data_path,
                pool_size = self.pool_size,
                "initializing chDB connection pool on first use"
            );
            ChdbConnectionPool::open(&self.data_path, self.pool_size)
                .map(Arc::new)
                .map_err(|e| e.to_string())
        });
        match res {
            Ok(p) => Ok(Arc::clone(p)),
            Err(msg) => Err(HyperbytedbError::Chdb(msg.clone())),
        }
    }

    /// Clone of the underlying pool (for `spawn_blocking` closures).
    pub fn pool(&self) -> Result<Arc<ChdbConnectionPool>, HyperbytedbError> {
        self.ensure_pool()
    }

    /// Run `f` on one pooled connection.
    pub fn with_connection<F, R>(&self, f: F) -> Result<R, HyperbytedbError>
    where
        F: FnOnce(&Connection) -> R,
    {
        let pool = self.ensure_pool()?;
        Ok(pool.with_connection(f))
    }
}
