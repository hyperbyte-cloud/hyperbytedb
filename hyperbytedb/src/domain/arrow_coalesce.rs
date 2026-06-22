//! Merge prepared fact-table Arrow batches sharing a measurement.

use std::sync::Arc;

use arrow::array::ArrayRef;
use arrow::compute::{cast, concat_batches};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;

use crate::domain::prepared_wal::PreparedMeasurementBatch;
use crate::error::HyperbytedbError;

/// Concatenate multiple prepared batches for the same measurement. Row-level
/// partial-line merge is handled at ingest when possible; here we only stack
/// batches with identical schemas.
pub fn coalesce_prepared_batches(
    mut batches: Vec<PreparedMeasurementBatch>,
) -> Result<Option<PreparedMeasurementBatch>, HyperbytedbError> {
    if batches.is_empty() {
        return Ok(None);
    }
    if batches.len() == 1 {
        return Ok(Some(batches.remove(0)));
    }

    let template = &batches[0];
    let schema = template.batch.schema();
    let refs: Vec<&RecordBatch> = batches.iter().map(|b| b.batch.as_ref()).collect();
    let merged = concat_batches(&schema, refs)
        .map_err(|e| HyperbytedbError::Internal(format!("concat prepared batches: {e}")))?;

    let min_time = batches.iter().map(|b| b.min_time).min().unwrap_or(0);
    let max_time = batches.iter().map(|b| b.max_time).max().unwrap_or(0);
    let row_count = merged.num_rows();

    let series_batches: Vec<Arc<RecordBatch>> = batches
        .iter()
        .filter_map(|b| b.new_series_batch.clone())
        .collect();
    let series = merge_series_batches(series_batches)?;

    Ok(Some(PreparedMeasurementBatch {
        measurement: template.measurement.clone(),
        table_name: template.table_name.clone(),
        series_table_name: template.series_table_name.clone(),
        batch: Arc::new(merged),
        row_count,
        min_time,
        max_time,
        new_series_batch: series,
    }))
}

/// Concatenate per-ingest `_series` dimension batches into one, then re-encode
/// any dictionary columns so each dictionary holds only unique values.
fn merge_series_batches(
    batches: Vec<Arc<RecordBatch>>,
) -> Result<Option<Arc<RecordBatch>>, HyperbytedbError> {
    match batches.len() {
        // 0 series batches → nothing to register. A single batch was built by
        // `build_series_tag_column`, which already deduplicates its dictionary,
        // so it needs no further normalization.
        0 => Ok(None),
        1 => Ok(batches.into_iter().next()),
        _ => {
            let schema = batches[0].schema();
            let refs: Vec<&RecordBatch> = batches.iter().map(|b| b.as_ref()).collect();
            let merged = concat_batches(&schema, refs)
                .map_err(|e| HyperbytedbError::Internal(format!("concat series batches: {e}")))?;
            Ok(Some(Arc::new(normalize_dictionary_columns(merged)?)))
        }
    }
}

/// Re-encode every dictionary-typed column so its dictionary contains only
/// unique values.
///
/// `concat_batches` (via `arrow::compute::concat`) only merges/deduplicates
/// dictionary values when the combined dictionary would exceed the row count
/// (`should_merge_dictionary_values`). For low-cardinality tags the dictionary
/// is *smaller* than the row count, so concat instead appends each batch's
/// dictionary wholesale, leaving the shared tag values duplicated. chDB's Arrow
/// reader then rejects the column with `Code: 117 ... Expected Dictionary size
/// N, real Dictionary size is M ... caused by duplicated values`. Casting a
/// dictionary column to its value type and back rebuilds a unique-valued
/// dictionary, which chDB accepts regardless of arrow's merge heuristic.
fn normalize_dictionary_columns(batch: RecordBatch) -> Result<RecordBatch, HyperbytedbError> {
    if !batch
        .columns()
        .iter()
        .any(|c| matches!(c.data_type(), DataType::Dictionary(_, _)))
    {
        return Ok(batch);
    }

    let schema = batch.schema();
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(batch.num_columns());
    for col in batch.columns() {
        match col.data_type() {
            DataType::Dictionary(_, value_type) => {
                let target = col.data_type().clone();
                let decoded = cast(col, value_type).map_err(|e| {
                    HyperbytedbError::Internal(format!("decode dictionary column: {e}"))
                })?;
                let reencoded = cast(&decoded, &target).map_err(|e| {
                    HyperbytedbError::Internal(format!("re-encode dictionary column: {e}"))
                })?;
                columns.push(reencoded);
            }
            _ => columns.push(Arc::clone(col)),
        }
    }

    RecordBatch::try_new(schema, columns)
        .map_err(|e| HyperbytedbError::Internal(format!("rebuild series batch: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, DictionaryArray, Int32Array, StringArray, UInt64Array};
    use arrow::datatypes::{DataType, Field, Int32Type, Schema};
    use std::collections::HashSet;

    /// Build a single-measurement prepared batch whose `_series` dimension
    /// batch carries one dictionary-encoded tag column.
    fn prepared_with_series(
        series_ids: &[u64],
        dict_values: &[&str],
        keys: &[i32],
    ) -> PreparedMeasurementBatch {
        let fact_schema = Arc::new(Schema::new(vec![Field::new(
            "series_id",
            DataType::UInt64,
            false,
        )]));
        let fact = RecordBatch::try_new(
            fact_schema,
            vec![Arc::new(UInt64Array::from(series_ids.to_vec()))],
        )
        .unwrap();

        let series_schema = Arc::new(Schema::new(vec![
            Field::new("series_id", DataType::UInt64, false),
            Field::new(
                "host",
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                true,
            ),
        ]));
        let dict = DictionaryArray::<Int32Type>::try_new(
            Int32Array::from(keys.to_vec()),
            Arc::new(StringArray::from(dict_values.to_vec())),
        )
        .unwrap();
        let series = RecordBatch::try_new(
            series_schema,
            vec![
                Arc::new(UInt64Array::from(series_ids.to_vec())),
                Arc::new(dict),
            ],
        )
        .unwrap();

        PreparedMeasurementBatch {
            measurement: "cpu".into(),
            table_name: "`t`".into(),
            series_table_name: "`t_series`".into(),
            batch: Arc::new(fact),
            row_count: series_ids.len(),
            min_time: 0,
            max_time: 0,
            new_series_batch: Some(Arc::new(series)),
        }
    }

    /// Coalescing series batches whose dictionaries overlap must yield a
    /// dictionary with unique values, even though `concat` alone leaves
    /// duplicates for low-cardinality columns (the chDB Code 117 trigger).
    #[test]
    fn coalesce_deduplicates_overlapping_series_dictionaries() {
        // Each batch: 3 rows but only 2 distinct tag values, so
        // total dictionary values (4) < total rows (6) and arrow's
        // `concat` skips merging — appending duplicate "a" entries.
        let a = prepared_with_series(&[1, 2, 3], &["a", "b"], &[0, 1, 0]);
        let b = prepared_with_series(&[4, 5, 6], &["a", "c"], &[0, 1, 0]);

        let merged = coalesce_prepared_batches(vec![a, b]).unwrap().unwrap();
        let series = merged.new_series_batch.expect("merged series batch");

        let host = series
            .column(1)
            .as_any()
            .downcast_ref::<DictionaryArray<Int32Type>>()
            .expect("dictionary column");
        let values = host
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        // Dictionary must contain only unique values.
        let unique: HashSet<&str> = (0..values.len()).map(|i| values.value(i)).collect();
        assert_eq!(
            unique.len(),
            values.len(),
            "dictionary still has duplicate values: {:?}",
            (0..values.len())
                .map(|i| values.value(i))
                .collect::<Vec<_>>()
        );
        assert_eq!(unique, HashSet::from(["a", "b", "c"]));

        // Decoded values must still match the original per-row sequence.
        let decoded = cast(host, &DataType::Utf8).unwrap();
        let decoded = decoded.as_any().downcast_ref::<StringArray>().unwrap();
        let rows: Vec<&str> = (0..decoded.len()).map(|i| decoded.value(i)).collect();
        assert_eq!(rows, vec!["a", "b", "a", "a", "c", "a"]);
    }
}
