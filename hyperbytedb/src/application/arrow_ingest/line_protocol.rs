use crate::adapters::chdb::native_adapter::ChdbNativeAdapter;
use crate::application::arrow_ingest::points::points_to_prepared_bundle;
use crate::application::line_protocol::parse_line_body_to_points;
use crate::error::HyperbytedbError;
use crate::ports::wal::WalAppendBundle;

pub async fn line_body_to_prepared_slot(
    sink: &ChdbNativeAdapter,
    db: &str,
    rp: &str,
    body: &[u8],
    precision: Option<&str>,
    origin_node_id: u64,
) -> Result<WalAppendBundle, HyperbytedbError> {
    let points = parse_line_body_to_points(body, precision)?;
    points_to_prepared_bundle(sink, db, rp, origin_node_id, points).await
}
