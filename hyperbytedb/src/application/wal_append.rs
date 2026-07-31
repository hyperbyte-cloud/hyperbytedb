use std::sync::Arc;

use metrics::histogram;

use crate::domain::point::Point;
use crate::error::HyperbytedbError;
use crate::ports::points_sink::PointsSinkPort;
use crate::ports::wal::{WalAppendBundle, WalEntry, WalPort};

pub async fn append_points_with_prepared(
    wal: &dyn WalPort,
    sink: Option<&Arc<dyn PointsSinkPort>>,
    db: &str,
    rp: &str,
    points: Vec<Point>,
    origin_node_id: u64,
    max_points_per_request: usize,
) -> Result<u64, HyperbytedbError> {
    crate::application::ingest_metadata::validate_point_count(
        points.len(),
        max_points_per_request,
    )?;
    let build_start = std::time::Instant::now();
    if wal.arrow_wal_enabled()
        && let Some(sink) = sink
    {
        match sink
            .build_prepared_wal_slot(db, rp, origin_node_id, &points)
            .await
        {
            Ok(prepared) => {
                histogram!("hyperbytedb_ingest_arrow_build_seconds")
                    .record(build_start.elapsed().as_secs_f64());
                let entry = WalEntry {
                    database: db.to_string(),
                    retention_policy: rp.to_string(),
                    points,
                    origin_node_id,
                };
                return wal
                    .append_bundle(WalAppendBundle {
                        entry,
                        prepared: Some(prepared),
                    })
                    .await;
            }
            Err(e) => {
                tracing::debug!(error = %e, "prepared WAL build failed; falling back");
            }
        }
    }

    let entry = WalEntry {
        database: db.to_string(),
        retention_policy: rp.to_string(),
        points,
        origin_node_id,
    };
    wal.append(entry).await
}

/// Parameters for appending a columnar wire batch to the WAL.
#[cfg(feature = "columnar-ingest")]
pub struct ColumnarWalAppend<'a> {
    pub db: &'a str,
    pub rp: &'a str,
    pub wire: &'a crate::application::columnar_msgpack::ColumnarMsgpackBatch,
    pub precision: Option<&'a str>,
    pub origin_node_id: u64,
    pub max_points_per_request: usize,
}

/// Append a columnar wire batch, building a prepared WAL slot without expanding
/// to `Vec<Point>` on the hot path. Falls back to point expansion when prepared
/// Arrow WAL is unavailable or the columnar prepared build fails.
#[cfg(feature = "columnar-ingest")]
pub async fn append_columnar_with_prepared(
    wal: &dyn WalPort,
    sink: Option<&Arc<dyn PointsSinkPort>>,
    req: &ColumnarWalAppend<'_>,
) -> Result<u64, HyperbytedbError> {
    use crate::application::columnar_msgpack::columnar_batch_to_points;

    crate::application::ingest_metadata::validate_point_count(
        req.wire.values.len(),
        req.max_points_per_request,
    )?;

    let build_start = std::time::Instant::now();
    if wal.arrow_wal_enabled()
        && let Some(sink) = sink
    {
        match sink
            .build_prepared_wal_slot_from_columnar(
                req.db,
                req.rp,
                req.origin_node_id,
                req.wire,
                req.precision,
            )
            .await
        {
            Ok(prepared) => {
                histogram!("hyperbytedb_ingest_arrow_build_seconds")
                    .record(build_start.elapsed().as_secs_f64());
                let entry = WalEntry {
                    database: req.db.to_string(),
                    retention_policy: req.rp.to_string(),
                    points: Vec::new(),
                    origin_node_id: req.origin_node_id,
                };
                return wal
                    .append_bundle(WalAppendBundle {
                        entry,
                        prepared: Some(prepared),
                    })
                    .await;
            }
            Err(e) => {
                tracing::debug!(error = %e, "columnar prepared WAL build failed; falling back");
            }
        }
    }

    let points = columnar_batch_to_points(req.wire, req.precision)?;
    append_points_with_prepared(
        wal,
        sink,
        req.db,
        req.rp,
        points,
        req.origin_node_id,
        req.max_points_per_request,
    )
    .await
}
