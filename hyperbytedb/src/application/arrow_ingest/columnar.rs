use crate::adapters::chdb::native_adapter::ChdbNativeAdapter;
use crate::application::arrow_ingest::points::points_to_prepared_bundle;
use crate::application::columnar_msgpack::ColumnarMsgpackBatch;
use crate::application::columnar_msgpack::columnar_batch_to_points;
use crate::error::HyperbytedbError;
use crate::ports::wal::WalAppendBundle;

pub async fn columnar_to_prepared_slot(
    sink: &ChdbNativeAdapter,
    db: &str,
    rp: &str,
    wire: &ColumnarMsgpackBatch,
    precision: Option<&str>,
    origin_node_id: u64,
) -> Result<WalAppendBundle, HyperbytedbError> {
    let points = columnar_batch_to_points(wire, precision)?;
    points_to_prepared_bundle(sink, db, rp, origin_node_id, points).await
}
