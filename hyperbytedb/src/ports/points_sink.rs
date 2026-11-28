//! Native points sink port.

use async_trait::async_trait;

use crate::domain::point::Point;
use crate::domain::prepared_wal::{PreparedMeasurementBatch, PreparedWalSlot};
use crate::error::HyperbytedbError;
use crate::ports::metadata::MeasurementMeta;

/// Result of a successful [`PointsSinkPort::write_points`] call.
#[derive(Debug, Clone, Copy)]
pub struct WriteAck {
    pub min_time: i64,
    pub max_time: i64,
    pub row_count: usize,
}

#[async_trait]
pub trait PointsSinkPort: Send + Sync {
    async fn write_points(
        &self,
        db: &str,
        rp: &str,
        measurement: &str,
        origins: &[u64],
        ingest_seq_base: u64,
        points: &[Point],
    ) -> Result<WriteAck, HyperbytedbError>;

    /// [`Self::write_points`] with each row's `ingest_seq` given. The default
    /// writes each run of consecutive sequences separately.
    async fn write_points_versioned(
        &self,
        db: &str,
        rp: &str,
        measurement: &str,
        origins: &[u64],
        seqs: &[u64],
        points: &[Point],
    ) -> Result<WriteAck, HyperbytedbError> {
        let mut total = WriteAck {
            min_time: i64::MAX,
            max_time: i64::MIN,
            row_count: 0,
        };
        let mut start = 0;
        while start < points.len() {
            let mut end = start + 1;
            while end < points.len() && seqs[end] == seqs[end - 1].saturating_add(1) {
                end += 1;
            }
            let ack = self
                .write_points(
                    db,
                    rp,
                    measurement,
                    &origins[start..end],
                    seqs[start],
                    &points[start..end],
                )
                .await?;
            total.min_time = total.min_time.min(ack.min_time);
            total.max_time = total.max_time.max(ack.max_time);
            total.row_count += ack.row_count;
            start = end;
        }
        Ok(total)
    }

    /// Insert a pre-built fact (and optional series) batch without rebuilding Arrow.
    async fn write_prepared_batch(
        &self,
        db: &str,
        rp: &str,
        batch: &PreparedMeasurementBatch,
    ) -> Result<WriteAck, HyperbytedbError> {
        let _ = (db, rp, batch);
        Err(HyperbytedbError::Internal(
            "prepared batch insert not supported".into(),
        ))
    }

    /// Build a chDB-ready prepared WAL slot from points at ingest time.
    async fn build_prepared_wal_slot(
        &self,
        _db: &str,
        _rp: &str,
        _origin_node_id: u64,
        _points: &[Point],
    ) -> Result<PreparedWalSlot, HyperbytedbError> {
        Err(HyperbytedbError::Internal(
            "prepared WAL build not supported".into(),
        ))
    }

    /// Build a prepared WAL slot directly from a columnar wire batch.
    #[cfg(feature = "columnar-ingest")]
    async fn build_prepared_wal_slot_from_columnar(
        &self,
        _db: &str,
        _rp: &str,
        _origin_node_id: u64,
        _wire: &crate::application::columnar_msgpack::ColumnarMsgpackBatch,
        _precision: Option<&str>,
    ) -> Result<PreparedWalSlot, HyperbytedbError> {
        Err(HyperbytedbError::Internal(
            "columnar prepared WAL build not supported".into(),
        ))
    }

    async fn drop_measurement(
        &self,
        db: &str,
        rp: &str,
        measurement: &str,
    ) -> Result<(), HyperbytedbError> {
        let _ = (db, rp, measurement);
        Ok(())
    }

    async fn ensure_measurement_schema(
        &self,
        db: &str,
        rp: &str,
        meta: &MeasurementMeta,
    ) -> Result<(), HyperbytedbError> {
        let _ = (db, rp, meta);
        Ok(())
    }

    /// Re-warm chDB schema caches from metadata after peer sync or widening.
    async fn refresh_schema_cache(&self) -> Result<(), HyperbytedbError> {
        Ok(())
    }

    /// Delete flushed fact and series rows whose `series_id` lies in `[start, end)`.
    async fn delete_series_id_range(
        &self,
        db: &str,
        rp: &str,
        measurement: &str,
        start: u64,
        end: u64,
    ) -> Result<(), HyperbytedbError> {
        let _ = (db, rp, measurement, start, end);
        Ok(())
    }
}
