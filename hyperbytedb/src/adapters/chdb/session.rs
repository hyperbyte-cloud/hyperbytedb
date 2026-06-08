//! Shared chDB session handle.
//!
//! libchdb is a process-global singleton: a second `chdb_connect` against
//! a different `--path` returns "Connection failed". We therefore build a
//! single [`SyncSession`] at startup and hand a clone of the same
//! `Arc<SyncSession>` to every adapter that needs to talk to chDB
//! ([`crate::adapters::chdb::query_adapter::ChdbQueryAdapter`] for reads,
//! [`crate::adapters::chdb::native_adapter::ChdbNativeAdapter`] for
//! writes in native-storage mode).
//!
//! The underlying `Session::execute` call is documented as thread-safe
//! by both `libchdb` (`chdb_query` / `chdb_query_n`) and chdb-rust
//! (`Connection`). We assert `Sync` here so a single session can be
//! shared across any number of `spawn_blocking` workers without a
//! per-query mutex; see the safety note inside this module for
//! details.

use std::sync::{Arc, OnceLock};

use chdb_rust::session::{Session, SessionBuilder};

use crate::error::HyperbytedbError;

/// Newtype around `chdb_rust::Session` that asserts thread-safety.
///
/// The upstream `Session` is declared `Send` only; the crate is
/// conservative. The underlying C library documents `chdb_query` as
/// _"thread-safe function that handles query execution in a separate
/// thread"_ (see `libchdb` `chdb_query`/`chdb_query_n` doc comments) and
/// the chdb-rust `Connection` doc comment likewise notes _"the
/// underlying chDB library is thread-safe for query execution"_. There
/// is exactly one global engine per process, but multiple `chdb_query`
/// calls against the same connection can run concurrently — libchdb
/// pipelines them internally.
///
/// We therefore assert `Sync` so we can hand a single
/// `Arc<SyncSession>` to many `spawn_blocking` workers without a
/// per-query mutex.
pub struct SyncSession(pub Session);

// SAFETY: see the doc comment above. `Session::execute` takes `&self`
// and internally calls `chdb_query`, which the upstream C library
// guarantees is thread-safe. We never hand out `&mut Session` and
// never call `Drop` from multiple threads (the `Arc` guarantees
// single-threaded drop).
unsafe impl Sync for SyncSession {}

/// Lazy holder for the process-global chDB session. Both adapters
/// share an [`Arc`] to one of these so the first `get()` builds the
/// session and every subsequent caller — read adapter, write adapter —
/// observes the same connection.
#[derive(Clone)]
pub struct SharedSession {
    data_path: String,
    cell: Arc<OnceLock<Result<Arc<SyncSession>, String>>>,
}

impl SharedSession {
    /// Build a new lazy holder. The session is not bound to libchdb
    /// until [`Self::get`] is called the first time.
    pub fn new(data_path: impl Into<String>) -> Self {
        Self {
            data_path: data_path.into(),
            cell: Arc::new(OnceLock::new()),
        }
    }

    /// Build the session eagerly so any libchdb bind failure surfaces
    /// at startup rather than at first query — important because
    /// libchdb is a process-global singleton and any later attempt to
    /// bind a different path would fail anyway.
    pub fn new_eager(data_path: impl Into<String>) -> Result<Self, HyperbytedbError> {
        let data_path: String = data_path.into();
        std::fs::create_dir_all(&data_path)
            .map_err(|e| HyperbytedbError::Chdb(format!("failed to create chDB data dir: {e}")))?;

        tracing::info!(path = %data_path, "initializing chDB session (eager)");
        let session = SessionBuilder::new()
            .with_data_path(&data_path)
            .build()
            .map_err(|e| {
                HyperbytedbError::Chdb(format!("failed to build chDB session at {data_path}: {e}"))
            })?;

        let cell = OnceLock::new();
        let _ = cell.set(Ok(Arc::new(SyncSession(session))));
        Ok(Self {
            data_path,
            cell: Arc::new(cell),
        })
    }

    /// Returns the configured data path (for diagnostics).
    pub fn data_path(&self) -> &str {
        &self.data_path
    }

    /// Returns a reference-counted handle to the chDB session, building
    /// it on first call. Cheap on the hot path (atomic load + clone of
    /// an `Arc`).
    pub fn get(&self) -> Result<Arc<SyncSession>, HyperbytedbError> {
        let res = self.cell.get_or_init(|| {
            tracing::info!(path = %self.data_path, "initializing chDB session on first use");
            SessionBuilder::new()
                .with_data_path(&self.data_path)
                .build()
                .map(|s| Arc::new(SyncSession(s)))
                .map_err(|e| e.to_string())
        });
        match res {
            Ok(s) => Ok(Arc::clone(s)),
            Err(msg) => Err(HyperbytedbError::Chdb(msg.clone())),
        }
    }
}
