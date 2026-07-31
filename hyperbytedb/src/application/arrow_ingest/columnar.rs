use crate::adapters::chdb::native_adapter::ChdbNativeAdapter;
use crate::application::columnar_msgpack::ColumnarMsgpackBatch;
use crate::application::columnar_msgpack::columnar_batch_to_record_batch;
use crate::domain::wal::WalEntry;
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
    // Validate the wire batch can be converted to Arrow before building the
    // chDB fact-table prepared slot (shared timestamp/field semantics).
    let _ = columnar_batch_to_record_batch(wire, precision)?;

    let prepared = sink
        .build_prepared_wal_slot_from_columnar(db, rp, origin_node_id, wire, precision)
        .await?;
    let entry = WalEntry {
        database: db.to_string(),
        retention_policy: rp.to_string(),
        points: Vec::new(),
        origin_node_id,
    };
    Ok(WalAppendBundle {
        entry,
        prepared: Some(prepared),
    })
}
