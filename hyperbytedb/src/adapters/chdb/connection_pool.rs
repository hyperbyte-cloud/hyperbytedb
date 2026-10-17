//! Same-path chDB connection pool for parallel inserts and queries.
//!
//! libchdb exposes one process-global [`EmbeddedServer`] per `--path`. Each
//! `Connection::open` to that path gets its own `ChdbClient` and
//! `client_mutex`, so concurrent work must use multiple connections — not one
//! shared session (see `chdb/insert/concurrency.rs`).

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use chdb_rust::connection::Connection;
use parking_lot::{Mutex, RwLock};

use crate::error::HyperbytedbError;

pub const MIN_POOL_SIZE: usize = 1;
pub const MAX_POOL_SIZE: usize = 128;
pub const DEFAULT_POOL_SIZE: usize = 4;
pub const DEFAULT_WRITE_POOL_SIZE: usize = 4;

/// Query pool size when nothing sets one: one connection per CPU this
/// process may use, plus one.
///
/// Every sharded query runs one chDB query per region it touches, so a small
/// pool queues concurrent multi-region queries: on n3 (3 CPU per pod, 4
/// concurrent 10-host GROUP BY queries) a pool of 1 ran them at 250-450 ms
/// p50 and a pool of 4 at 90-130 ms. Each query also runs threads of its
/// own, so a larger pool is not faster: 12 was 10-40% slower than 4 there.
/// `available_parallelism` reads the container's CPU quota.
#[must_use]
pub fn default_query_pool_size() -> usize {
    let cpus = std::thread::available_parallelism().map_or(1, std::num::NonZeroUsize::get);
    clamp_pool_size(cpus + 1)
}

/// Clamp configured pool size to a safe range.
pub fn clamp_pool_size(size: usize) -> usize {
    if size == 0 {
        MIN_POOL_SIZE
    } else {
        size.clamp(MIN_POOL_SIZE, MAX_POOL_SIZE)
    }
}

type Slot = Arc<Mutex<Connection>>;

/// Pool of independent chDB connections to one data directory.
///
/// Resizable while in use ([`Self::resize`]): growing opens connections,
/// shrinking stops handing out the ones above the new size, so no statement
/// in flight is disturbed either way.
pub struct ChdbConnectionPool {
    data_path: String,
    slots: RwLock<Vec<Slot>>,
    /// How many of `slots` are handed out.
    active: AtomicUsize,
    next: AtomicUsize,
}

fn open_connection(data_path: &str, i: usize, of: usize) -> Result<Slot, HyperbytedbError> {
    let path_arg = format!("--path={data_path}");
    let conn = Connection::open(&[&path_arg]).map_err(|e| {
        HyperbytedbError::Chdb(
            format!(
                "failed to open chDB connection {} / {of} at {data_path}: {e}",
                i + 1
            )
            .into(),
        )
    })?;
    Ok(Arc::new(Mutex::new(conn)))
}

impl ChdbConnectionPool {
    /// Open `pool_size` connections to `data_path` (same `--path` for all).
    pub fn open(data_path: &str, pool_size: usize) -> Result<Self, HyperbytedbError> {
        std::fs::create_dir_all(data_path).map_err(|e| {
            HyperbytedbError::Chdb(format!("failed to create chDB data dir: {e}").into())
        })?;

        let pool_size = clamp_pool_size(pool_size);
        let slots = (0..pool_size)
            .map(|i| open_connection(data_path, i, pool_size))
            .collect::<Result<Vec<_>, _>>()?;

        tracing::info!(path = %data_path, pool_size, "initialized chDB connection pool");
        Ok(Self {
            data_path: data_path.to_string(),
            slots: RwLock::new(slots),
            active: AtomicUsize::new(pool_size),
            next: AtomicUsize::new(0),
        })
    }

    pub fn data_path(&self) -> &str {
        &self.data_path
    }

    /// Connections handed out.
    pub fn pool_size(&self) -> usize {
        self.active.load(Ordering::Relaxed)
    }

    /// Hand out `size` connections (clamped), opening any not yet open.
    /// Returns the new size.
    pub fn resize(&self, size: usize) -> Result<usize, HyperbytedbError> {
        let size = clamp_pool_size(size);
        let have = self.slots.read().len();
        if size > have {
            // Opened outside the lock: queries keep running meanwhile.
            let more = (have..size)
                .map(|i| open_connection(&self.data_path, i, size))
                .collect::<Result<Vec<_>, _>>()?;
            let mut slots = self.slots.write();
            for slot in more {
                if slots.len() < size {
                    slots.push(slot);
                }
            }
        }
        let was = self.active.swap(size, Ordering::Relaxed);
        if was != size {
            tracing::info!(path = %self.data_path, from = was, to = size, "resized chDB connection pool");
        }
        Ok(size)
    }

    /// One of the handed-out connections: round-robin, preferring an idle one.
    fn pick(&self) -> Slot {
        let slots = self.slots.read();
        let n = self.active.load(Ordering::Relaxed).min(slots.len()).max(1);
        let start = self.next.fetch_add(1, Ordering::Relaxed);
        for offset in 0..n {
            let slot = &slots[(start + offset) % n];
            if !slot.is_locked() {
                return Arc::clone(slot);
            }
        }
        Arc::clone(&slots[start % n])
    }

    /// Run `f` with one pool connection.
    pub fn with_connection<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&Connection) -> R,
    {
        let slot = self.pick();
        let guard = slot.lock();
        f(&guard)
    }
}

impl ChdbConnectionPool {
    /// [`Self::with_connection`] with mutable access, for calls that borrow
    /// the connection for longer than one statement (a streamed result).
    pub fn with_connection_mut<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&mut Connection) -> R,
    {
        let slot = self.pick();
        let mut guard = slot.lock();
        f(&mut guard)
    }
}

impl std::fmt::Debug for ChdbConnectionPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChdbConnectionPool")
            .field("data_path", &self.data_path)
            .field("pool_size", &self.pool_size())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;
    use std::sync::Barrier;

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

    /// Growing opens connections, shrinking stops using the extra ones, and
    /// the pool keeps answering throughout.
    #[test]
    #[serial]
    fn a_pool_resizes_in_place() {
        let dir = tempfile::tempdir().expect("tempdir");
        let pool = ChdbConnectionPool::open(dir.path().to_str().unwrap(), 2).expect("open");
        let select = |pool: &ChdbConnectionPool| {
            pool.with_connection(|conn: &Connection| {
                conn.query("SELECT 1", chdb_rust::format::OutputFormat::TabSeparated)
                    .expect("query");
            });
        };
        assert_eq!(pool.resize(6).unwrap(), 6);
        assert_eq!(pool.pool_size(), 6);
        assert_eq!(pool.slots.read().len(), 6);
        select(&pool);
        assert_eq!(pool.resize(3).unwrap(), 3);
        assert_eq!(pool.pool_size(), 3);
        select(&pool);
        assert_eq!(pool.resize(0).unwrap(), 1);
        select(&pool);
    }

    #[test]
    fn the_default_query_pool_follows_the_cpus() {
        let cpus = std::thread::available_parallelism().map_or(1, |n| n.get());
        assert_eq!(default_query_pool_size(), clamp_pool_size(cpus + 1));
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
