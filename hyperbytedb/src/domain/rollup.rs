use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::domain::measurement::MeasurementMeta;
use crate::timeseriesql::ast::{Expr, Field, FunctionCall};

/// How to merge duplicate `(series_id, time)` rows when reading rollup / MV dest data.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RollupCombine {
    /// Additive partial aggregates (`sum`, `count`).
    Sum,
    Min,
    Max,
    /// Point selectors (`last`); merge by highest `ingest_seq`.
    Last,
    /// Point selectors (`first`); merge by earliest `time`.
    First,
}

/// Maps a logical field queried as `mean(x)` to stored sum/count columns.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MeanRollupField {
    pub sum_col: String,
    pub count_col: String,
}

/// Physical columns backing `mean(field)` on an MV destination measurement.
#[must_use]
pub fn mean_rollup_column_names(field: &str) -> (String, String) {
    (format!("sum_{field}"), format!("count_{field}"))
}

/// Derive rollup merge semantics from an MV SELECT field expression.
pub fn rollup_combine_from_field(
    field: &Field,
) -> Result<RollupCombine, crate::error::HyperbytedbError> {
    match &field.expr {
        Expr::Call(func) => rollup_combine_from_call(func),
        _ => Err(crate::error::HyperbytedbError::QueryParse(
            "materialized view field must be an aggregate call".to_string(),
        )),
    }
}

pub fn rollup_combine_from_call(
    func: &FunctionCall,
) -> Result<RollupCombine, crate::error::HyperbytedbError> {
    match func.name.to_uppercase().as_str() {
        "SUM" | "COUNT" => Ok(RollupCombine::Sum),
        "MIN" => Ok(RollupCombine::Min),
        "MAX" => Ok(RollupCombine::Max),
        "FIRST" => Ok(RollupCombine::First),
        "LAST" => Ok(RollupCombine::Last),
        "MEAN" => Err(crate::error::HyperbytedbError::QueryParse(
            "mean() uses sum/count storage; handled separately".to_string(),
        )),
        other => Err(crate::error::HyperbytedbError::QueryParse(format!(
            "materialized view aggregate {other} is not supported for incremental rollups"
        ))),
    }
}

/// Extract the single field argument name from `mean(\"field\")` / `sum(\"field\")` etc.
pub fn aggregate_source_field_name(
    func: &FunctionCall,
) -> Result<String, crate::error::HyperbytedbError> {
    let arg = func.args.first().ok_or_else(|| {
        crate::error::HyperbytedbError::QueryParse(format!(
            "{} requires one field argument",
            func.name
        ))
    })?;
    match arg {
        Expr::Identifier(name) | Expr::FieldRef { name, .. } => Ok(name.clone()),
        _ => Err(crate::error::HyperbytedbError::QueryParse(format!(
            "{} argument must be a field name",
            func.name
        ))),
    }
}

type FieldRollupsResult = Result<
    (
        HashMap<String, u8>,
        HashMap<String, RollupCombine>,
        HashMap<String, MeanRollupField>,
    ),
    crate::error::HyperbytedbError,
>;

/// Build rollup metadata for an MV destination from its SELECT fields.
pub fn field_rollups_from_mv_select(fields: &[Field]) -> FieldRollupsResult {
    use crate::timeseriesql::to_clickhouse::select_output_field_name;

    let mut field_types = HashMap::new();
    let mut field_rollups = HashMap::new();
    let mut mean_fields = HashMap::new();

    for field in fields {
        if let Expr::Call(func) = &field.expr
            && func.name.eq_ignore_ascii_case("mean")
        {
            let source = aggregate_source_field_name(func)?;
            let (sum_col, count_col) = mean_rollup_column_names(&source);
            field_types.insert(sum_col.clone(), 0);
            field_types.insert(count_col.clone(), 0);
            field_rollups.insert(sum_col.clone(), RollupCombine::Sum);
            field_rollups.insert(count_col.clone(), RollupCombine::Sum);
            mean_fields.insert(source, MeanRollupField { sum_col, count_col });
            continue;
        }

        let name = select_output_field_name(field).ok_or_else(|| {
            crate::error::HyperbytedbError::QueryParse(
                "materialized view field requires a name or alias".to_string(),
            )
        })?;
        let combine = rollup_combine_from_field(field)?;
        field_types.insert(name.clone(), 0);
        field_rollups.insert(name, combine);
    }

    Ok((field_types, field_rollups, mean_fields))
}

/// Field column names that should use additive merge in MV destination storage.
#[must_use]
pub fn summing_field_names(meta: &MeasurementMeta) -> Vec<String> {
    let mut cols: Vec<String> = meta
        .field_rollups
        .iter()
        .filter(|(_, combine)| matches!(combine, RollupCombine::Sum))
        .map(|(name, _)| name.clone())
        .collect();
    cols.sort();
    cols.dedup();
    cols
}
