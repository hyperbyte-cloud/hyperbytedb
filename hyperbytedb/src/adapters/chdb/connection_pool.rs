//! Same-path chDB connection pool for parallel inserts and queries.
//!
//! libchdb exposes one process-global [`EmbeddedServer`] per `--path`. Each
//! `Connection::open` to that path gets its own `ChdbClient` and
//! `client_mutex`, so concurrent work must use multiple connections — not one
//! shared session (see `chdb/insert/concurrency.rs`).

use std::sync::atomic::{AtomicUsize, Ordering};

use chdb_rust::connection::Connection;
use parking_lot::Mutex;

use crate::error::HyperbytedbError;

pub const MIN_POOL_SIZE: usize = 1;
pub const MAX_POOL_SIZE: usize = 128;
pub const DEFAULT_POOL_SIZE: usize = 4;
pub const DEFAULT_QUERY_POOL_SIZE: usize = 4;
pub const DEFAULT_WRITE_POOL_SIZE: usize = 4;

/// Clamp configured pool size to a safe range.
pub fn clamp_pool_size(size: usize) -> usize {
    if size == 0 {
        MIN_POOL_SIZE
    } else {
        size.clamp(MIN_POOL_SIZE, MAX_POOL_SIZE)
    }
}

/// Pool of independent chDB connections to one data directory.
pub struct ChdbConnectionPool {
    data_path: String,
    slots: Vec<Mutex<Connection>>,
    next: AtomicUsize,
}

impl ChdbConnectionPool {
    /// Open `pool_size` connections to `data_path` (same `--path` for all).
    pub fn open(data_path: &str, pool_size: usize) -> Result<Self, HyperbytedbError> {
        std::fs::create_dir_all(data_path)
            .map_err(|e| HyperbytedbError::Chdb(format!("failed to create chDB data dir: {e}")))?;

        let pool_size = clamp_pool_size(pool_size);
        let path_arg = format!("--path={data_path}");
        let mut slots = Vec::with_capacity(pool_size);
        for i in 0..pool_size {
            let conn = Connection::open(&[&path_arg]).map_err(|e| {
                HyperbytedbError::Chdb(format!(
                    "failed to open chDB connection {} / {pool_size} at {data_path}: {e}",
                    i + 1
                ))
            })?;
            slots.push(Mutex::new(conn));
        }

        tracing::info!(path = %data_path, pool_size, "initialized chDB connection pool");
        Ok(Self {
            data_path: data_path.to_string(),
            slots,
            next: AtomicUsize::new(0),
        })
    }

    pub fn data_path(&self) -> &str {
        &self.data_path
    }

    pub fn pool_size(&self) -> usize {
        self.slots.len()
    }

    /// Run `f` with one pool connection. Round-robin with `try_lock` on busy slots.
    pub fn with_connection<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&Connection) -> R,
    {
        let n = self.slots.len();
        if n == 1 {
            let guard = self.slots[0].lock();
            return f(&guard);
        }

        let start = self.next.fetch_add(1, Ordering::Relaxed);
        for offset in 0..n {
            let idx = (start + offset) % n;
            if let Some(guard) = self.slots[idx].try_lock() {
                return f(&guard);
            }
        }

        let idx = start % n;
        let guard = self.slots[idx].lock();
        f(&guard)
    }
}

impl std::fmt::Debug for ChdbConnectionPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChdbConnectionPool")
            .field("data_path", &self.data_path)
            .field("pool_size", &self.slots.len())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;
    use std::sync::{Arc, Barrier};

    #[test]
    fn clamp_pool_size_bounds() {
        assert_eq!(clamp_pool_size(0), 1);
        assert_eq!(clamp_pool_size(1), 1);
        assert_eq!(clamp_pool_size(4), 4);
        assert_eq!(clamp_pool_size(100), 100);
        assert_eq!(clamp_pool_size(200), 128);
    }

    #[test]
    #[serial]
    fn pool_opens_n_connections() {
        let dir = tempfile::tempdir().expect("tempdir");
        let pool = ChdbConnectionPool::open(dir.path().to_str().unwrap(), 4).expect("open");
        assert_eq!(pool.pool_size(), 4);
    }

    #[test]
    #[serial]
    fn concurrent_with_connection_from_threads() {
        let dir = tempfile::tempdir().expect("tempdir");
        let pool =
            Arc::new(ChdbConnectionPool::open(dir.path().to_str().unwrap(), 4).expect("open"));
        let barrier = Arc::new(Barrier::new(4));
        let mut handles = Vec::new();
        for _ in 0..4 {
            let pool = Arc::clone(&pool);
            let barrier = Arc::clone(&barrier);
            handles.push(std::thread::spawn(move || {
                barrier.wait();
                pool.with_connection(|conn: &Connection| {
                    conn.query("SELECT 1", chdb_rust::format::OutputFormat::TabSeparated)
                        .expect("query");
                })
            }));
        }
        for h in handles {
            h.join().expect("join");
        }
    }
}
