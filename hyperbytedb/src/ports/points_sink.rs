//! Native points sink port.
//!
//! [`crate::application::flush_service`] hands whole `(db, rp, measurement)`
//! slices straight to a [`PointsSinkPort`] implementation, which is responsible
//! for whatever native engine lives behind it (in this codebase:
//! `ChdbNativeAdapter` writing `INSERT INTO ... VALUES (...)` against the
//! singleton chDB session).
//!
//! The port intentionally accepts a slice of [`crate::domain::point::Point`]
//! rather than Arrow batches: the only consumer (chdb-rust) does not
//! expose a binary insert API in the version we target, so encoding
//! happens inside the adapter where the values are pre-rendered into a
//! single SQL string. Adapters that gain a binary path later (Arrow
//! IPC, native ClickHouse `Block`s) can still satisfy this signature.

use async_trait::async_trait;

use crate::domain::point::Point;
use crate::error::HyperbytedbError;
use crate::ports::metadata::MeasurementMeta;

/// Result of a successful [`PointsSinkPort::write_points`] call.
/// The flush service uses these for logging / metrics; nothing in the
/// hot path depends on the values being precise.
#[derive(Debug, Clone, Copy)]
pub struct WriteAck {
    pub min_time: i64,
    pub max_time: i64,
    pub row_count: usize,
}

#[async_trait]
pub trait PointsSinkPort: Send + Sync {
    /// Persist `points` for the given `(db, rp, measurement)` tuple.
    /// `origins` is parallel to `points`: one `origin_node_id` per row — the
    /// cluster node that originally accepted that write (or this node's id for
    /// direct client writes). Carrying it per-row lets the flush group purely by
    /// `(db, rp, measurement)` and write rows from different origins in a single
    /// insert, instead of fanning out one insert per origin.
    ///
    /// `ingest_seq_base` is the WAL sequence the first point was
    /// drawn from; the adapter is free to use it (or a per-row
    /// monotonic counter derived from it) as the
    /// `ReplacingMergeTree` version column.
    async fn write_points(
        &self,
        db: &str,
        rp: &str,
        measurement: &str,
        origins: &[u64],
        ingest_seq_base: u64,
        points: &[Point],
    ) -> Result<WriteAck, HyperbytedbError>;

    /// Drop the backing table for `(db, rp, measurement)` if any. The
    /// default is a no-op so adapters that don't have a separate
    /// drop step (e.g. file-based storage) need not override.
    async fn drop_measurement(
        &self,
        db: &str,
        rp: &str,
        measurement: &str,
    ) -> Result<(), HyperbytedbError> {
        let _ = (db, rp, measurement);
        Ok(())
    }

    /// Ensure backing fact + `_series` tables exist for `meta` without inserting
    /// data. The default is a no-op for adapters that create tables lazily on
    /// first write.
    async fn ensure_measurement_schema(
        &self,
        db: &str,
        rp: &str,
        meta: &MeasurementMeta,
    ) -> Result<(), HyperbytedbError> {
        let _ = (db, rp, meta);
        Ok(())
    }
}
