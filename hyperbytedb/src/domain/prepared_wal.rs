//! In-memory WAL slots holding chDB-ready Arrow batches for zero-copy flush.

use std::sync::Arc;

use arrow::array::Array;
use arrow::record_batch::RecordBatch;

/// chDB fact-table batch for one measurement, ready for `insert_record_batch_direct`.
#[derive(Debug, Clone)]
pub struct PreparedMeasurementBatch {
    pub measurement: String,
    pub table_name: String,
    pub series_table_name: String,
    pub batch: Arc<RecordBatch>,
    pub row_count: usize,
    pub min_time: i64,
    pub max_time: i64,
    pub new_series_batch: Option<Arc<RecordBatch>>,
}

/// One WAL sequence worth of prepared data (may span multiple measurements).
#[derive(Debug, Clone)]
pub struct PreparedWalSlot {
    pub database: String,
    pub retention_policy: String,
    pub origin_node_id: u64,
    pub measurements: Vec<PreparedMeasurementBatch>,
}

impl PreparedWalSlot {
    pub fn total_rows(&self) -> usize {
        self.measurements.iter().map(|m| m.row_count).sum()
    }
}

/// Patch the `ingest_seq` column (index 2 in the fact schema) after the WAL
/// sequence is assigned.
pub fn patch_ingest_seq(
    batch: &RecordBatch,
    ingest_seq_base: u64,
) -> Result<Arc<RecordBatch>, crate::error::HyperbytedbError> {
    use arrow::array::{ArrayRef, UInt64Array};
    use arrow::datatypes::DataType;

    let n = batch.num_rows();
    if n == 0 {
        return Ok(Arc::new(batch.clone()));
    }

    let schema = batch.schema();
    let ingest_idx = schema
        .fields()
        .iter()
        .position(|f| f.name() == "ingest_seq")
        .ok_or_else(|| {
            crate::error::HyperbytedbError::Internal(
                "prepared batch missing ingest_seq column".into(),
            )
        })?;

    let seq_col = batch.column(ingest_idx);
    if seq_col.data_type() != &DataType::UInt64 {
        return Err(crate::error::HyperbytedbError::Internal(
            "ingest_seq column has unexpected type".into(),
        ));
    }

    let mut seqs: Vec<u64> = (0..n)
        .map(|i| ingest_seq_base.saturating_add(i as u64))
        .collect();
    if let Some(existing) = seq_col.as_any().downcast_ref::<UInt64Array>() {
        for (i, seq) in seqs.iter_mut().enumerate().take(n) {
            if !existing.is_null(i) {
                *seq = ingest_seq_base.saturating_add(existing.value(i));
            }
        }
    }

    let mut columns: Vec<ArrayRef> = batch.columns().to_vec();
    columns[ingest_idx] = Arc::new(UInt64Array::from(seqs));

    let patched = RecordBatch::try_new(schema, columns)
        .map_err(|e| crate::error::HyperbytedbError::Internal(format!("patch ingest_seq: {e}")))?;
    Ok(Arc::new(patched))
}

impl PreparedWalSlot {
    pub fn patch_all_ingest_seqs(
        &mut self,
        seq: u64,
    ) -> Result<(), crate::error::HyperbytedbError> {
        let mut row_offset = 0u64;
        for m in &mut self.measurements {
            let base = seq.saturating_add(row_offset);
            m.batch = patch_ingest_seq(&m.batch, base)?;
            row_offset = row_offset.saturating_add(m.row_count as u64);
        }
        Ok(())
    }
}
