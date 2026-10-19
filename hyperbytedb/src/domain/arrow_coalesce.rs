//! Merge prepared fact-table Arrow batches sharing a measurement.

use std::sync::Arc;

use arrow::array::{ArrayRef, StringArray, new_null_array};
use arrow::compute::{CastOptions, cast, cast_with_options, concat_batches};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use crate::domain::prepared_wal::PreparedMeasurementBatch;
use crate::error::HyperbytedbError;

/// Concatenate multiple prepared batches for the same measurement. Row-level
/// partial-line merge is handled at ingest when possible; here the batches
/// are stacked by column name. They need not share a schema: two writes to a
/// measurement can carry different fields (`used`, then `free`), and stacking
/// them by position put `free`'s values under `used`.
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
    let refs: Vec<&RecordBatch> = batches.iter().map(|b| b.batch.as_ref()).collect();
    let merged = concat_by_name(&refs, Missing::Null)
        .map_err(|e| HyperbytedbError::Internal(format!("concat prepared batches: {e}").into()))?;

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
            // New series of one measurement can carry different tag keys.
            let refs: Vec<&RecordBatch> = batches.iter().map(|b| b.as_ref()).collect();
            let merged = concat_by_name(&refs, Missing::EmptyString).map_err(|e| {
                HyperbytedbError::Internal(format!("concat series batches: {e}").into())
            })?;
            Ok(Some(Arc::new(normalize_dictionary_columns(merged)?)))
        }
    }
}

/// What a batch without one of the merged columns gets in its place.
#[derive(Clone, Copy)]
pub(crate) enum Missing {
    /// A field the rows did not have.
    Null,
    /// A tag the series did not have: the series table stores "".
    EmptyString,
}

/// Stack `batches` by column name: the result has every column of any batch,
/// in the order they first appear, and each batch's columns go under their
/// own names. A column one batch lacks is filled per `missing`; a column whose
/// type differs between batches is cast to the wider type (see
/// [`wider_type`]).
pub(crate) fn concat_by_name(
    batches: &[&RecordBatch],
    missing: Missing,
) -> Result<RecordBatch, arrow::error::ArrowError> {
    let mut fields: Vec<Field> = Vec::new();
    for batch in batches {
        for field in batch.schema().fields() {
            match fields.iter_mut().find(|f| f.name() == field.name()) {
                Some(existing) => {
                    let wider = wider_type(existing.data_type(), field.data_type());
                    *existing = existing
                        .clone()
                        .with_data_type(wider)
                        .with_nullable(existing.is_nullable() || field.is_nullable());
                }
                None => fields.push(field.as_ref().clone()),
            }
        }
    }
    // A column some batch lacks is filled with nulls (or ""), so it must be
    // nullable when nulls are what fill it.
    for field in &mut fields {
        let everywhere = batches
            .iter()
            .all(|b| b.schema().index_of(field.name()).is_ok());
        if !everywhere && matches!(missing, Missing::Null) {
            *field = field.clone().with_nullable(true);
        }
    }
    let schema = Arc::new(Schema::new(fields));
    let safe = CastOptions {
        safe: true,
        ..Default::default()
    };
    let mut aligned = Vec::with_capacity(batches.len());
    for batch in batches {
        let n = batch.num_rows();
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());
        for field in schema.fields() {
            let column = match batch.schema().index_of(field.name()) {
                Ok(i) if batch.column(i).data_type() == field.data_type() => {
                    Arc::clone(batch.column(i))
                }
                Ok(i) => cast_with_options(batch.column(i), field.data_type(), &safe)?,
                Err(_) => match missing {
                    Missing::Null => new_null_array(field.data_type(), n),
                    Missing::EmptyString => {
                        let empty: ArrayRef = Arc::new(StringArray::from(vec![""; n]));
                        cast(&empty, field.data_type())?
                    }
                },
            };
            columns.push(column);
        }
        aligned.push(RecordBatch::try_new(Arc::clone(&schema), columns)?);
    }
    concat_batches(&schema, &aligned)
}

/// The type a column takes when batches disagree, as ingest widens a field
/// (`widen_field_disc`): float over signed integer, unsigned over signed, and
/// a plain string over a dictionary-encoded one. Anything else (float and
/// unsigned included, which ingest does not widen) keeps the first type; the
/// other batch's values are cast to it, and those that cannot be become null.
/// The insert then casts to the table's type, which decides.
fn wider_type(a: &DataType, b: &DataType) -> DataType {
    use DataType::*;
    match (a, b) {
        _ if a == b => a.clone(),
        (Float64, Int64) | (Int64, Float64) => Float64,
        (Int64, UInt64) | (UInt64, Int64) => UInt64,
        (Dictionary(_, _) | Utf8, Dictionary(_, _) | Utf8) => Utf8,
        _ => a.clone(),
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
                    HyperbytedbError::Internal(format!("decode dictionary column: {e}").into())
                })?;
                let reencoded = cast(&decoded, &target).map_err(|e| {
                    HyperbytedbError::Internal(format!("re-encode dictionary column: {e}").into())
                })?;
                columns.push(reencoded);
            }
            _ => columns.push(Arc::clone(col)),
        }
    }

    RecordBatch::try_new(schema, columns)
        .map_err(|e| HyperbytedbError::Internal(format!("rebuild series batch: {e}").into()))
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

    use arrow::array::{Float64Array, Int64Array};

    fn fact(columns: Vec<(&str, ArrayRef)>) -> PreparedMeasurementBatch {
        let n = columns[0].1.len();
        let schema = Arc::new(Schema::new(
            columns
                .iter()
                .map(|(name, a)| Field::new(*name, a.data_type().clone(), true))
                .collect::<Vec<_>>(),
        ));
        let batch =
            RecordBatch::try_new(schema, columns.into_iter().map(|(_, a)| a).collect()).unwrap();
        PreparedMeasurementBatch {
            measurement: "m".into(),
            table_name: "`t`".into(),
            series_table_name: "`t_series`".into(),
            batch: Arc::new(batch),
            row_count: n,
            min_time: 0,
            max_time: 0,
            new_series_batch: None,
        }
    }

    fn floats(v: &[Option<f64>]) -> ArrayRef {
        Arc::new(Float64Array::from(v.to_vec()))
    }

    /// Two writes with different fields, flushed together: each value stays
    /// under its own field. Stacked by position, `free`'s value landed under
    /// `used` (same column count, same type) and `free` was null.
    #[test]
    fn batches_with_different_fields_stack_by_name() {
        let sid = |v: u64| -> ArrayRef { Arc::new(UInt64Array::from(vec![v])) };
        let a = fact(vec![("series_id", sid(1)), ("used", floats(&[Some(1.0)]))]);
        let b = fact(vec![("series_id", sid(2)), ("free", floats(&[Some(2.0)]))]);
        let merged = coalesce_prepared_batches(vec![a, b])
            .unwrap()
            .unwrap()
            .batch;
        let col = |name: &str| {
            let i = merged.schema().index_of(name).unwrap();
            merged
                .column(i)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .clone()
        };
        assert_eq!(col("used"), Float64Array::from(vec![Some(1.0), None]));
        assert_eq!(col("free"), Float64Array::from(vec![None, Some(2.0)]));
    }

    /// A field written as an integer in one batch and a float in another is
    /// a float, as ingest widens it.
    #[test]
    fn a_field_whose_type_differs_takes_the_wider_type() {
        let a = fact(vec![(
            "v",
            Arc::new(Int64Array::from(vec![1i64])) as ArrayRef,
        )]);
        let b = fact(vec![("v", floats(&[Some(2.5)]))]);
        let merged = coalesce_prepared_batches(vec![a, b])
            .unwrap()
            .unwrap()
            .batch;
        let v = merged
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(v, &Float64Array::from(vec![1.0, 2.5]));
    }

    /// Float and unsigned are not widened into each other, as at ingest: the
    /// first type stays and the table's type decides at insert.
    #[test]
    fn float_and_unsigned_keep_the_first_type() {
        assert_eq!(
            wider_type(&DataType::Float64, &DataType::UInt64),
            DataType::Float64
        );
        assert_eq!(
            wider_type(&DataType::UInt64, &DataType::Float64),
            DataType::UInt64
        );
        assert_eq!(
            wider_type(&DataType::Int64, &DataType::Float64),
            DataType::Float64
        );
        assert_eq!(
            wider_type(&DataType::Int64, &DataType::UInt64),
            DataType::UInt64
        );
    }

    /// New series with different tags, flushed together: each tag value
    /// stays under its own tag, and a tag a series lacks is "".
    #[test]
    fn series_with_different_tags_stack_by_name() {
        let series = |sid: u64, tag: &str, value: &str| -> Arc<RecordBatch> {
            let schema = Arc::new(Schema::new(vec![
                Field::new("series_id", DataType::UInt64, false),
                Field::new(tag, DataType::Utf8, false),
            ]));
            Arc::new(
                RecordBatch::try_new(
                    schema,
                    vec![
                        Arc::new(UInt64Array::from(vec![sid])),
                        Arc::new(StringArray::from(vec![value])),
                    ],
                )
                .unwrap(),
            )
        };
        let merged = merge_series_batches(vec![series(1, "host", "a"), series(2, "region", "x")])
            .unwrap()
            .unwrap();
        let tag = |name: &str| {
            let i = merged.schema().index_of(name).unwrap();
            let a = merged
                .column(i)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            (0..a.len())
                .map(|r| a.value(r).to_string())
                .collect::<Vec<_>>()
        };
        assert_eq!(tag("host"), ["a", ""]);
        assert_eq!(tag("region"), ["", "x"]);
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
