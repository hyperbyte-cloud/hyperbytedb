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
