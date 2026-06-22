use crate::adapters::chdb::native_adapter::ChdbNativeAdapter;
use crate::domain::point::Point;
use crate::domain::prepared_wal::PreparedWalSlot;
use crate::domain::wal::WalEntry;
use crate::error::HyperbytedbError;
use crate::ports::wal::WalAppendBundle;

pub async fn points_to_prepared_bundle(
    sink: &ChdbNativeAdapter,
    db: &str,
    rp: &str,
    origin_node_id: u64,
    points: Vec<Point>,
) -> Result<WalAppendBundle, HyperbytedbError> {
    let entry = WalEntry {
        database: db.to_string(),
        retention_policy: rp.to_string(),
        points: points.clone(),
        origin_node_id,
    };
    let prepared = sink
        .build_prepared_wal_slot(db, rp, origin_node_id, &points)
        .await?;
    Ok(WalAppendBundle {
        entry,
        prepared: Some(prepared),
    })
}

pub async fn points_to_prepared_slot(
    sink: &ChdbNativeAdapter,
    db: &str,
    rp: &str,
    origin_node_id: u64,
    points: &[Point],
) -> Result<PreparedWalSlot, HyperbytedbError> {
    sink.build_prepared_wal_slot(db, rp, origin_node_id, points)
        .await
}
