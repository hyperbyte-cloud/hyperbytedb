use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};

use serde_json::{Value, json};

use crate::domain::measurement::MeasurementMeta;
use crate::domain::query_result::{QueryResponse, SeriesResult};
use crate::domain::rollup::{
    MeanRollupField, RollupCombine, aggregate_source_field_name, mean_rollup_column_names,
    rollup_combine_from_field,
};
use crate::error::HyperbytedbError;
use crate::timeseriesql::ast::{BinaryExpr, BinaryOp, Expr, Field, FunctionCall, SelectStatement};
use crate::timeseriesql::to_clickhouse::{select_has_true_aggregate, select_output_field_name};

const MULTI_REGION_UNSUPPORTED: &str = "aggregate is not supported across shard regions yet";

/// Metadata for rewriting a sharded query into per-region partial aggregation
/// and applying coordinator-side finalization.
#[derive(Debug, Clone)]
pub struct ShardedQueryPlan {
    pub region_stmt: SelectStatement,
    mean_partials: HashMap<String, MeanRollupField>,
    first_partials: HashMap<String, String>,
    last_partials: HashMap<String, String>,
    stddev_partials: HashMap<String, StddevPartialCols>,
    distinct_outputs: HashSet<String>,
    count_distinct_outputs: HashMap<String, String>,
    merge_rules: HashMap<String, ColumnMerge>,
    row_key_distinct_cols: HashSet<String>,
    saved_limit: Option<u64>,
    saved_offset: Option<u64>,
    saved_time_desc: Option<bool>,
    needs_global_sort: bool,
}

#[derive(Debug, Clone)]
struct StddevPartialCols {
    sum_col: String,
    sumsq_col: String,
    count_col: String,
}

/// Rewrite a SELECT for per-region partial aggregation when fan-out > 1.
pub fn prepare_sharded_region_query(
    stmt: &SelectStatement,
) -> Result<ShardedQueryPlan, HyperbytedbError> {
    let mut plan = ShardedQueryPlan {
        region_stmt: stmt.clone(),
        mean_partials: HashMap::new(),
        first_partials: HashMap::new(),
        last_partials: HashMap::new(),
        stddev_partials: HashMap::new(),
        distinct_outputs: HashSet::new(),
        count_distinct_outputs: HashMap::new(),
        merge_rules: HashMap::new(),
        row_key_distinct_cols: HashSet::new(),
        saved_limit: None,
        saved_offset: None,
        saved_time_desc: None,
        needs_global_sort: false,
    };

    validate_no_non_mergeable_aggs(stmt)?;

    let mut region_fields = Vec::new();
    for field in &stmt.fields {
        region_fields.extend(rewrite_field_for_region(field, &mut plan)?);
    }
    plan.region_stmt.fields = region_fields;
    plan.merge_rules = build_merge_rules(stmt, &plan);

    if stmt.limit.is_some() || stmt.offset.is_some() {
        let offset = stmt.offset.unwrap_or(0);
        let fetch = stmt
            .limit
            .map(|l| l.saturating_add(offset))
            .or_else(|| (offset > 0).then_some(offset));
        plan.region_stmt.limit = fetch;
        plan.region_stmt.offset = None;
        plan.saved_limit = stmt.limit;
        plan.saved_offset = stmt.offset;
    }

    if stmt.order_by.is_some()
        || stmt
            .group_by
            .as_ref()
            .is_some_and(|gb| gb.time_dimension().is_some())
    {
        plan.saved_time_desc = stmt.order_by.as_ref().map(|o| o.time_desc);
        plan.needs_global_sort = true;
        plan.region_stmt.order_by = None;
        ensure_time_field_for_global_sort(&mut plan.region_stmt.fields);
    }

    Ok(plan)
}

fn ensure_time_field_for_global_sort(fields: &mut Vec<Field>) {
    let has_time = fields.iter().any(|f| {
        matches!(&f.expr, Expr::Identifier(name) if name == "time" || name == "__time")
            || f.alias.as_deref() == Some("time")
    });
    if !has_time {
        fields.insert(
            0,
            Field {
                expr: Expr::Identifier("time".into()),
                alias: None,
            },
        );
    }
}

fn validate_no_non_mergeable_aggs(stmt: &SelectStatement) -> Result<(), HyperbytedbError> {
    for field in &stmt.fields {
        if let Expr::Call(func) = &field.expr
            && is_non_mergeable_agg(func)
        {
            return Err(HyperbytedbError::QueryParse(format!(
                "{MULTI_REGION_UNSUPPORTED}: {}",
                func.name.to_lowercase()
            )));
        }
    }
    Ok(())
}

fn is_non_mergeable_agg(func: &FunctionCall) -> bool {
    matches!(
        func.name.to_uppercase().as_str(),
        "PERCENTILE" | "MEDIAN" | "MODE" | "SPREAD"
    )
}

fn rewrite_field_for_region(
    field: &Field,
    plan: &mut ShardedQueryPlan,
) -> Result<Vec<Field>, HyperbytedbError> {
    let Some(output) = select_output_field_name(field) else {
        return Ok(vec![field.clone()]);
    };

    match &field.expr {
        Expr::Call(func) => match func.name.to_uppercase().as_str() {
            "MEAN" => {
                let source = aggregate_source_field_name(func)?;
                let (sum_col, count_col) = mean_rollup_column_names(&source);
                plan.mean_partials.insert(
                    output,
                    MeanRollupField {
                        sum_col: sum_col.clone(),
                        count_col: count_col.clone(),
                    },
                );
                Ok(vec![
                    field_with_call("sum", &source, Some(sum_col)),
                    field_with_call("count", &source, Some(count_col)),
                ])
            }
            "FIRST" => {
                let _source = aggregate_source_field_name(func)?;
                let time_col = format!("{output}__sel_time");
                plan.first_partials.insert(output.clone(), time_col.clone());
                Ok(vec![
                    field.clone(),
                    field_with_call("min", "time", Some(time_col)),
                ])
            }
            "LAST" => {
                let _source = aggregate_source_field_name(func)?;
                let time_col = format!("{output}__sel_time");
                plan.last_partials.insert(output.clone(), time_col.clone());
                Ok(vec![
                    field.clone(),
                    field_with_call("max", "time", Some(time_col)),
                ])
            }
            "STDDEV" => {
                let source = aggregate_source_field_name(func)?;
                let sum_col = format!("stddev_sum_{source}");
                let sumsq_col = format!("stddev_sumsq_{source}");
                let count_col = format!("stddev_count_{source}");
                plan.stddev_partials.insert(
                    output,
                    StddevPartialCols {
                        sum_col: sum_col.clone(),
                        sumsq_col: sumsq_col.clone(),
                        count_col: count_col.clone(),
                    },
                );
                Ok(vec![
                    field_with_call("sum", &source, Some(sum_col)),
                    field_with_call("sum", &format!("{source}*{source}"), Some(sumsq_col)),
                    field_with_call("count", &source, Some(count_col)),
                ])
            }
            "DISTINCT" => {
                plan.distinct_outputs.insert(output.clone());
                plan.row_key_distinct_cols.insert(output);
                Ok(vec![field.clone()])
            }
            "COUNT" => {
                if let Some(Expr::Call(inner)) = func.args.first()
                    && inner.name.eq_ignore_ascii_case("distinct")
                {
                    let source = aggregate_source_field_name(inner)?;
                    let distinct_output = format!("distinct_{source}");
                    plan.count_distinct_outputs
                        .insert(output, distinct_output.clone());
                    plan.row_key_distinct_cols.insert(distinct_output.clone());
                    return Ok(vec![Field {
                        expr: call_expr("distinct", field_arg(&source)),
                        alias: Some(distinct_output),
                    }]);
                }
                Ok(vec![field.clone()])
            }
            _ => Ok(vec![field.clone()]),
        },
        _ => Ok(vec![field.clone()]),
    }
}

fn field_with_call(name: &str, source: &str, alias: Option<String>) -> Field {
    let expr = if name.eq_ignore_ascii_case("sum") && source.contains('*') {
        let parts: Vec<&str> = source.split('*').collect();
        if parts.len() == 2 {
            call_expr(
                "sum",
                Expr::BinaryExpr(Box::new(BinaryExpr {
                    left: field_arg(parts[0]),
                    op: BinaryOp::Mul,
                    right: field_arg(parts[1]),
                })),
            )
        } else {
            call_expr(name, field_arg(source))
        }
    } else {
        call_expr(name, field_arg(source))
    };
    Field { expr, alias }
}

fn field_arg(name: &str) -> Expr {
    Expr::Identifier(name.to_string())
}

fn call_expr(name: &str, arg: Expr) -> Expr {
    Expr::Call(FunctionCall {
        name: name.to_string(),
        args: vec![arg],
    })
}

fn build_merge_rules(
    stmt: &SelectStatement,
    plan: &ShardedQueryPlan,
) -> HashMap<String, ColumnMerge> {
    let mut rules = HashMap::new();

    for (output, partial) in &plan.mean_partials {
        rules.insert(partial.sum_col.clone(), ColumnMerge::Sum);
        rules.insert(partial.count_col.clone(), ColumnMerge::Sum);
        let _ = output;
    }
    for (output, time_col) in &plan.first_partials {
        rules.insert(output.clone(), ColumnMerge::FirstByTime);
        rules.insert(time_col.clone(), ColumnMerge::Min);
    }
    for (output, time_col) in &plan.last_partials {
        rules.insert(output.clone(), ColumnMerge::LastByTime);
        rules.insert(time_col.clone(), ColumnMerge::Max);
    }
    for partial in plan.stddev_partials.values() {
        rules.insert(partial.sum_col.clone(), ColumnMerge::Sum);
        rules.insert(partial.sumsq_col.clone(), ColumnMerge::Sum);
        rules.insert(partial.count_col.clone(), ColumnMerge::Sum);
    }
    for col in &plan.distinct_outputs {
        rules.insert(col.clone(), ColumnMerge::DistinctKeep);
    }
    for distinct_col in plan.count_distinct_outputs.values() {
        rules.insert(distinct_col.clone(), ColumnMerge::DistinctKeep);
    }

    for field in &stmt.fields {
        let Some(name) = select_output_field_name(field) else {
            continue;
        };
        if rules.contains_key(&name) {
            continue;
        }
        let merge = if let Ok(combine) = rollup_combine_from_field(field) {
            match combine {
                RollupCombine::Sum => ColumnMerge::Sum,
                RollupCombine::Min => ColumnMerge::Min,
                RollupCombine::Max => ColumnMerge::Max,
                RollupCombine::First => ColumnMerge::First,
                RollupCombine::Last => ColumnMerge::Last,
            }
        } else if matches!(
            field.expr,
            Expr::Call(ref func)
                if func.name.eq_ignore_ascii_case("mean")
                    || func.name.eq_ignore_ascii_case("count")
        ) {
            ColumnMerge::Sum
        } else if plan.count_distinct_outputs.contains_key(&name) {
            ColumnMerge::Skip
        } else {
            ColumnMerge::Unsupported
        };
        rules.insert(name, merge);
    }

    rules
}

/// Merge multiple partial query responses into one, deduplicating series by name+tags.
pub fn merge_query_results(mut parts: Vec<QueryResponse>) -> QueryResponse {
    if parts.is_empty() {
        return QueryResponse::empty(0);
    }
    if parts.len() == 1 {
        if let Some(part) = parts.pop() {
            return part;
        }
        return QueryResponse::empty(0);
    }

    let statement_id = parts[0]
        .results
        .first()
        .map(|r| r.statement_id)
        .unwrap_or(0);
    let mut merged: HashMap<String, SeriesResult> = HashMap::new();

    for part in parts {
        for stmt in part.results {
            if let Some(series_list) = stmt.series {
                for series in series_list {
                    let key = format!(
                        "{}:{:?}",
                        series.name,
                        series.tags.as_ref().map(|t| {
                            let mut pairs: Vec<_> = t.iter().collect();
                            pairs.sort_by_key(|(k, _)| *k);
                            pairs
                        })
                    );
                    match merged.entry(key) {
                        Entry::Occupied(mut e) => {
                            e.get_mut().values.extend(series.values);
                        }
                        Entry::Vacant(e) => {
                            e.insert(series);
                        }
                    }
                }
            }
        }
    }

    let mut series: Vec<SeriesResult> = merged.into_values().collect();
    series.sort_by(|a, b| a.name.cmp(&b.name));

    QueryResponse::single(statement_id, series)
}

/// Merge sharded partial results, reducing global aggregates across regions.
pub fn merge_sharded_query_results(
    parts: Vec<QueryResponse>,
    stmt: &SelectStatement,
    plan: Option<&ShardedQueryPlan>,
) -> Result<QueryResponse, HyperbytedbError> {
    let merged = if !select_has_true_aggregate(stmt) {
        merge_query_results(parts)
    } else {
        merge_aggregate_parts(parts, stmt, plan)?
    };

    if let Some(plan) = plan {
        apply_sharded_post_merge(merged, stmt, plan)
    } else {
        Ok(merged)
    }
}

/// Apply coordinator-side finalization and global ORDER BY / LIMIT / OFFSET.
pub fn apply_sharded_post_merge(
    response: QueryResponse,
    original: &SelectStatement,
    plan: &ShardedQueryPlan,
) -> Result<QueryResponse, HyperbytedbError> {
    let mut resp = if select_has_true_aggregate(original) {
        finalize_sharded_aggregates(response, plan)?
    } else {
        response
    };

    if plan.needs_global_sort || plan.saved_limit.is_some() || plan.saved_offset.is_some() {
        resp = apply_global_sort_limit_offset(resp, original, plan)?;
    }
    Ok(resp)
}

/// Merge partial materialized-view destination rows from multiple shard regions.
pub fn merge_materialized_rollup_results(
    parts: Vec<QueryResponse>,
    meta: &MeasurementMeta,
) -> Result<QueryResponse, HyperbytedbError> {
    if parts.is_empty() {
        return Ok(QueryResponse::empty(0));
    }
    if parts.len() == 1 {
        if let Some(part) = parts.into_iter().next() {
            return Ok(part);
        }
        return Ok(QueryResponse::empty(0));
    }

    let statement_id = parts[0]
        .results
        .first()
        .map(|r| r.statement_id)
        .unwrap_or(0);
    let rules = rollup_merge_rules(meta);
    merge_rows_with_rules(parts, statement_id, &rules, &HashSet::new())
}

fn rollup_merge_rules(meta: &MeasurementMeta) -> HashMap<String, ColumnMerge> {
    let mut rules = HashMap::new();
    for (col, combine) in &meta.field_rollups {
        let merge = match combine {
            RollupCombine::Sum => ColumnMerge::Sum,
            RollupCombine::Min => ColumnMerge::Min,
            RollupCombine::Max => ColumnMerge::Max,
            RollupCombine::First => ColumnMerge::First,
            RollupCombine::Last => ColumnMerge::Last,
        };
        rules.insert(col.clone(), merge);
    }
    rules
}

fn ragged_merge_row() -> HyperbytedbError {
    HyperbytedbError::Internal("ragged shard merge row".into())
}

fn merge_rows_with_rules(
    parts: Vec<QueryResponse>,
    statement_id: u32,
    rules: &HashMap<String, ColumnMerge>,
    row_key_distinct_cols: &HashSet<String>,
) -> Result<QueryResponse, HyperbytedbError> {
    let mut rows: HashMap<String, (SeriesResult, Vec<Value>)> = HashMap::new();

    for part in parts {
        for stmt_result in part.results {
            let Some(series_list) = stmt_result.series else {
                continue;
            };
            for series in series_list {
                for row in &series.values {
                    let key = series_row_key(&series, row, row_key_distinct_cols)?;
                    match rows.entry(key) {
                        Entry::Occupied(mut e) => {
                            let (existing_series, merged_row) = e.get_mut();
                            *merged_row =
                                merge_row(&existing_series.columns, rules, merged_row, row)?;
                        }
                        Entry::Vacant(e) => {
                            e.insert((series.clone(), row.clone()));
                        }
                    }
                }
            }
        }
    }

    let mut by_series: HashMap<String, SeriesResult> = HashMap::new();
    for (_, (series, row)) in rows {
        let series_key = format!(
            "{}:{:?}",
            series.name,
            series.tags.as_ref().map(|t| {
                let mut pairs: Vec<_> = t.iter().collect();
                pairs.sort_by_key(|(k, _)| *k);
                pairs
            })
        );
        match by_series.entry(series_key) {
            Entry::Occupied(mut e) => e.get_mut().values.push(row),
            Entry::Vacant(e) => {
                e.insert(SeriesResult {
                    name: series.name,
                    tags: series.tags,
                    columns: series.columns,
                    values: vec![row],
                    partial: series.partial,
                });
            }
        }
    }

    let mut series: Vec<SeriesResult> = by_series.into_values().collect();
    series.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(QueryResponse::single(statement_id, series))
}

#[derive(Clone, Copy, Debug)]
enum ColumnMerge {
    Sum,
    Min,
    Max,
    First,
    Last,
    FirstByTime,
    LastByTime,
    DistinctKeep,
    Skip,
    Unsupported,
}

fn column_merge_rules(stmt: &SelectStatement) -> HashMap<String, ColumnMerge> {
    // Plan-less fallback for aggregate merges. Without a ShardedQueryPlan the
    // partial columns needed to combine MEAN/FIRST/LAST/STDDEV do not exist, so
    // those outputs are left untouched rather than combined with a rule that
    // would fabricate a plausible-but-wrong number. COUNT is genuinely
    // additive and stays mergeable.
    let mut rules = HashMap::new();
    for field in &stmt.fields {
        let Some(name) = select_output_field_name(field) else {
            continue;
        };
        let merge = if let Ok(combine) = rollup_combine_from_field(field) {
            match combine {
                RollupCombine::Sum => ColumnMerge::Sum,
                RollupCombine::Min => ColumnMerge::Min,
                RollupCombine::Max => ColumnMerge::Max,
                RollupCombine::First => ColumnMerge::First,
                RollupCombine::Last => ColumnMerge::Last,
            }
        } else if matches!(
            field.expr,
            Expr::Call(ref func) if func.name.eq_ignore_ascii_case("count")
        ) {
            ColumnMerge::Sum
        } else {
            ColumnMerge::Unsupported
        };
        rules.insert(name, merge);
    }
    rules
}

fn series_row_key(
    series: &SeriesResult,
    row: &[Value],
    row_key_distinct_cols: &HashSet<String>,
) -> Result<String, HyperbytedbError> {
    let mut key = series.name.clone();
    if let Some(tags) = &series.tags {
        let mut pairs: Vec<_> = tags.iter().collect();
        pairs.sort_by_key(|(k, _)| *k);
        for (k, v) in pairs {
            key.push('|');
            key.push_str(k);
            key.push('=');
            key.push_str(v);
        }
    }
    if let Some(idx) = series
        .columns
        .iter()
        .position(|c| c == "time" || c == "__time")
    {
        let cell = row.get(idx).ok_or_else(ragged_merge_row)?;
        key.push('|');
        key.push_str(&cell.to_string());
    }
    for col in row_key_distinct_cols {
        if let Some(idx) = series.columns.iter().position(|c| c == col) {
            let cell = row.get(idx).ok_or_else(ragged_merge_row)?;
            key.push('|');
            key.push_str(col);
            key.push('=');
            key.push_str(&cell.to_string());
        }
    }
    Ok(key)
}

fn json_as_f64(v: &Value) -> Option<f64> {
    v.as_f64()
        .or_else(|| v.as_i64().map(|i| i as f64))
        .or_else(|| v.as_u64().map(|u| u as f64))
}

fn json_as_i64(v: &Value) -> Option<i64> {
    v.as_i64()
        .or_else(|| v.as_u64().and_then(|u| i64::try_from(u).ok()))
}

fn json_as_count(v: &Value) -> i64 {
    if let Some(i) = json_as_i64(v) {
        return i;
    }
    json_as_f64(v)
        .filter(|f| f.is_finite())
        .map(|f| f as i64)
        .unwrap_or(0)
}

fn json_sortable_time(v: &Value) -> i64 {
    if let Some(n) = json_as_i64(v) {
        return n;
    }
    if let Some(s) = v.as_str() {
        if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(s) {
            return dt.timestamp_nanos_opt().unwrap_or(0);
        }
        if let Ok(ndt) = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S") {
            return ndt.and_utc().timestamp_nanos_opt().unwrap_or(0);
        }
        if let Ok(ndt) = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f") {
            return ndt.and_utc().timestamp_nanos_opt().unwrap_or(0);
        }
    }
    0
}

fn json_sum(left: &Value, right: &Value) -> Value {
    if let (Some(l), Some(r)) = (left.as_i64(), right.as_i64()) {
        return match l.checked_add(r) {
            Some(s) => json!(s),
            None => json!(l as f64 + r as f64),
        };
    }
    if let (Some(l), Some(r)) = (left.as_u64(), right.as_u64()) {
        return match l.checked_add(r) {
            Some(s) => json!(s),
            None => json!(l as f64 + r as f64),
        };
    }
    let l = json_as_f64(left).unwrap_or(0.0);
    let r = json_as_f64(right).unwrap_or(0.0);
    json!(l + r)
}

fn merge_values(
    left: &Value,
    right: &Value,
    rule: ColumnMerge,
    columns: &[String],
    col_idx: usize,
    left_row: &[Value],
    right_row: &[Value],
) -> Result<Value, HyperbytedbError> {
    match rule {
        ColumnMerge::DistinctKeep | ColumnMerge::Skip | ColumnMerge::Unsupported => {
            Ok(left.clone())
        }
        ColumnMerge::First => Ok(left.clone()),
        ColumnMerge::Last => Ok(right.clone()),
        ColumnMerge::Sum => Ok(json_sum(left, right)),
        ColumnMerge::Min => {
            let l = json_as_f64(left);
            let r = json_as_f64(right);
            Ok(match (l, r) {
                (Some(a), Some(b)) => json!(a.min(b)),
                (Some(a), None) => json!(a),
                (None, Some(b)) => json!(b),
                (None, None) => left.clone(),
            })
        }
        ColumnMerge::Max => {
            let l = json_as_f64(left);
            let r = json_as_f64(right);
            Ok(match (l, r) {
                (Some(a), Some(b)) => json!(a.max(b)),
                (Some(a), None) => json!(a),
                (None, Some(b)) => json!(b),
                (None, None) => left.clone(),
            })
        }
        ColumnMerge::FirstByTime => {
            let col = columns.get(col_idx).ok_or_else(ragged_merge_row)?;
            let time_col = format!("{col}__sel_time");
            pick_by_time(left, right, left_row, right_row, columns, &time_col, true)
        }
        ColumnMerge::LastByTime => {
            let col = columns.get(col_idx).ok_or_else(ragged_merge_row)?;
            let time_col = format!("{col}__sel_time");
            pick_by_time(left, right, left_row, right_row, columns, &time_col, false)
        }
    }
}

fn pick_by_time(
    left: &Value,
    right: &Value,
    left_row: &[Value],
    right_row: &[Value],
    columns: &[String],
    time_col: &str,
    pick_min: bool,
) -> Result<Value, HyperbytedbError> {
    let Some(time_idx) = columns.iter().position(|c| c == time_col) else {
        return Ok(left.clone());
    };
    let lt = json_as_i64(left_row.get(time_idx).ok_or_else(ragged_merge_row)?);
    let rt = json_as_i64(right_row.get(time_idx).ok_or_else(ragged_merge_row)?);
    Ok(match (lt, rt) {
        (Some(a), Some(b)) if a == b => left.clone(),
        (Some(a), Some(b)) if pick_min && a <= b => left.clone(),
        (Some(a), Some(b)) if !pick_min && a >= b => left.clone(),
        (Some(_), Some(_)) => right.clone(),
        (Some(_), None) => left.clone(),
        (None, Some(_)) => right.clone(),
        (None, None) => left.clone(),
    })
}

fn merge_row(
    columns: &[String],
    rules: &HashMap<String, ColumnMerge>,
    left: &[Value],
    right: &[Value],
) -> Result<Vec<Value>, HyperbytedbError> {
    if left.len() != columns.len() || right.len() != columns.len() {
        return Err(ragged_merge_row());
    }
    let mut out = left.to_vec();
    for (i, col) in columns.iter().enumerate() {
        let rule = rules.get(col).copied().unwrap_or(ColumnMerge::Unsupported);
        out[i] = merge_values(&out[i], &right[i], rule, columns, i, left, right)?;
    }
    Ok(out)
}

fn merge_aggregate_parts(
    parts: Vec<QueryResponse>,
    stmt: &SelectStatement,
    plan: Option<&ShardedQueryPlan>,
) -> Result<QueryResponse, HyperbytedbError> {
    if parts.is_empty() {
        return Ok(QueryResponse::empty(0));
    }
    if parts.len() == 1 {
        if let Some(part) = parts.into_iter().next() {
            return Ok(part);
        }
        return Ok(QueryResponse::empty(0));
    }

    let statement_id = parts[0]
        .results
        .first()
        .map(|r| r.statement_id)
        .unwrap_or(0);
    let rules = plan
        .map(|p| p.merge_rules.clone())
        .unwrap_or_else(|| column_merge_rules(stmt));
    let row_key_distinct_cols = plan
        .map(|p| p.row_key_distinct_cols.clone())
        .unwrap_or_default();

    merge_rows_with_rules(parts, statement_id, &rules, &row_key_distinct_cols)
}

fn finalize_sharded_aggregates(
    response: QueryResponse,
    plan: &ShardedQueryPlan,
) -> Result<QueryResponse, HyperbytedbError> {
    let Some(mut results) = response.results.into_iter().next() else {
        return Ok(QueryResponse::empty(0));
    };
    let Some(series_list) = results.series.take() else {
        return Ok(QueryResponse::single(results.statement_id, vec![]));
    };

    let mut finalized = Vec::with_capacity(series_list.len());
    for series in series_list {
        finalized.push(finalize_series_aggregates(series, plan)?);
    }

    Ok(QueryResponse::single(results.statement_id, finalized))
}

/// Resolved column indices for one finalizable aggregate output.
#[derive(Clone, Copy)]
enum AggregateFinalizer {
    Mean {
        sum_idx: usize,
        count_idx: usize,
    },
    Stddev {
        sum_idx: usize,
        sumsq_idx: usize,
        count_idx: usize,
    },
}

fn finalize_series_aggregates(
    mut series: SeriesResult,
    plan: &ShardedQueryPlan,
) -> Result<SeriesResult, HyperbytedbError> {
    if !plan.count_distinct_outputs.is_empty() {
        series = collapse_count_distinct(series, plan)?;
    }

    let drop_cols: HashSet<String> = plan
        .mean_partials
        .values()
        .flat_map(|p| [p.sum_col.clone(), p.count_col.clone()])
        .chain(
            plan.first_partials
                .values()
                .chain(plan.last_partials.values())
                .cloned(),
        )
        .chain(
            plan.stddev_partials
                .values()
                .flat_map(|p| [p.sum_col.clone(), p.sumsq_col.clone(), p.count_col.clone()]),
        )
        .collect();

    let mut new_columns = Vec::new();
    let mut finalizers: Vec<(String, AggregateFinalizer)> = Vec::new();
    for (output, partial) in &plan.mean_partials {
        let sum_idx = series.columns.iter().position(|c| c == &partial.sum_col);
        let count_idx = series.columns.iter().position(|c| c == &partial.count_col);
        if let (Some(si), Some(ci)) = (sum_idx, count_idx) {
            finalizers.push((
                output.clone(),
                AggregateFinalizer::Mean {
                    sum_idx: si,
                    count_idx: ci,
                },
            ));
            new_columns.push(output.clone());
        }
    }
    for (output, partial) in &plan.stddev_partials {
        let sum_idx = series.columns.iter().position(|c| c == &partial.sum_col);
        let sumsq_idx = series.columns.iter().position(|c| c == &partial.sumsq_col);
        let count_idx = series.columns.iter().position(|c| c == &partial.count_col);
        if let (Some(si), Some(sqi), Some(ci)) = (sum_idx, sumsq_idx, count_idx) {
            finalizers.push((
                output.clone(),
                AggregateFinalizer::Stddev {
                    sum_idx: si,
                    sumsq_idx: sqi,
                    count_idx: ci,
                },
            ));
            new_columns.push(output.clone());
        }
    }

    let keep_cols: Vec<(usize, String)> = series
        .columns
        .iter()
        .enumerate()
        .filter(|(_, c)| {
            // NOTE: count_distinct_outputs keys are the FINAL count columns
            // written by collapse_count_distinct above — they must be kept,
            // not dropped here.
            !drop_cols.contains(*c)
                && !plan.mean_partials.contains_key(*c)
                && !plan.stddev_partials.contains_key(*c)
        })
        .map(|(i, c)| (i, c.clone()))
        .collect();

    if finalizers.is_empty() && keep_cols.len() == series.columns.len() {
        return Ok(series);
    }

    let mut final_columns: Vec<String> = Vec::new();
    let mut value_builders: Vec<Vec<Value>> = Vec::new();

    for (idx, col) in &keep_cols {
        final_columns.push(col.clone());
        let mut col_values = Vec::with_capacity(series.values.len());
        for row in &series.values {
            col_values.push(row.get(*idx).cloned().ok_or_else(ragged_merge_row)?);
        }
        value_builders.push(col_values);
    }

    for (output, finalizer) in &finalizers {
        match *finalizer {
            AggregateFinalizer::Mean { sum_idx, count_idx } => {
                final_columns.push(output.clone());
                let mut means = Vec::with_capacity(series.values.len());
                for row in &series.values {
                    let sum =
                        json_as_f64(row.get(sum_idx).ok_or_else(ragged_merge_row)?).unwrap_or(0.0);
                    let count = json_as_count(row.get(count_idx).ok_or_else(ragged_merge_row)?);
                    means.push(if count == 0 || !sum.is_finite() {
                        Value::Null
                    } else {
                        json!(sum / count as f64)
                    });
                }
                value_builders.push(means);
            }
            AggregateFinalizer::Stddev {
                sum_idx,
                sumsq_idx,
                count_idx,
            } => {
                final_columns.push(output.clone());
                let mut stddevs = Vec::with_capacity(series.values.len());
                for row in &series.values {
                    let sum =
                        json_as_f64(row.get(sum_idx).ok_or_else(ragged_merge_row)?).unwrap_or(0.0);
                    let sumsq = json_as_f64(row.get(sumsq_idx).ok_or_else(ragged_merge_row)?)
                        .unwrap_or(0.0);
                    let count = json_as_count(row.get(count_idx).ok_or_else(ragged_merge_row)?);
                    if count < 2 || !sum.is_finite() || !sumsq.is_finite() {
                        stddevs.push(Value::Null);
                        continue;
                    }
                    let n = count as f64;
                    let var = (sumsq - (sum * sum) / n) / (n - 1.0);
                    stddevs.push(if !var.is_finite() {
                        Value::Null
                    } else if var <= 0.0 {
                        json!(0.0)
                    } else {
                        json!(var.sqrt())
                    });
                }
                value_builders.push(stddevs);
            }
        }
    }

    let row_count = series.values.len();
    let mut new_values = Vec::with_capacity(row_count);
    for row_idx in 0..row_count {
        let mut row = Vec::with_capacity(final_columns.len());
        for col_values in &value_builders {
            row.push(
                col_values
                    .get(row_idx)
                    .cloned()
                    .ok_or_else(ragged_merge_row)?,
            );
        }
        new_values.push(row);
    }

    series.columns = final_columns;
    series.values = new_values;
    Ok(series)
}

fn collapse_count_distinct(
    mut series: SeriesResult,
    plan: &ShardedQueryPlan,
) -> Result<SeriesResult, HyperbytedbError> {
    for (count_col, distinct_col) in &plan.count_distinct_outputs {
        let Some(distinct_idx) = series.columns.iter().position(|c| c == distinct_col) else {
            continue;
        };
        let time_idx = series
            .columns
            .iter()
            .position(|c| c == "time" || c == "__time");

        // One entry per output bucket, in first-seen order. The template row is
        // captured from the bucket itself — pairing groups with rows by index
        // would misalign columns whenever HashMap iteration order differs from
        // input row order.
        let mut group_order: Vec<String> = Vec::new();
        let mut groups: HashMap<String, (HashSet<String>, Vec<Value>)> = HashMap::new();
        for row in series.values.iter() {
            let mut key = String::new();
            if let Some(ti) = time_idx {
                key.push_str(&row.get(ti).ok_or_else(ragged_merge_row)?.to_string());
            }
            key.push('|');
            key.push_str(&format!("{:?}", series.tags));
            let entry = groups
                .entry(key.clone())
                .or_insert_with(|| (HashSet::new(), row.clone()));
            if !group_order.contains(&key) {
                group_order.push(key);
            }
            entry.0.insert(
                row.get(distinct_idx)
                    .ok_or_else(ragged_merge_row)?
                    .to_string(),
            );
        }

        let mut new_columns: Vec<String> = series
            .columns
            .iter()
            .filter(|c| *c != distinct_col)
            .cloned()
            .collect();
        let count_out_idx = match new_columns.iter().position(|c| c == count_col) {
            Some(i) => i,
            None => {
                new_columns.push(count_col.clone());
                new_columns.len() - 1
            }
        };

        let mut new_values = Vec::with_capacity(group_order.len());
        for key in group_order {
            let Some((distinct_set, template_row)) = groups.get(&key) else {
                continue;
            };
            let mut row: Vec<Value> = Vec::new();
            for (i, c) in series.columns.iter().enumerate() {
                if i != distinct_idx && *c != *count_col {
                    row.push(template_row.get(i).cloned().ok_or_else(ragged_merge_row)?);
                }
            }
            while row.len() <= count_out_idx {
                row.push(Value::Null);
            }
            row[count_out_idx] = json!(distinct_set.len());
            new_values.push(row);
        }

        series.columns = new_columns;
        series.values = new_values;
    }
    Ok(series)
}

fn apply_global_sort_limit_offset(
    response: QueryResponse,
    original: &SelectStatement,
    plan: &ShardedQueryPlan,
) -> Result<QueryResponse, HyperbytedbError> {
    let Some(mut results) = response.results.into_iter().next() else {
        return Ok(QueryResponse::empty(0));
    };
    let Some(mut series_list) = results.series.take() else {
        return Ok(QueryResponse::single(results.statement_id, vec![]));
    };

    if series_list.is_empty() {
        return Ok(QueryResponse::single(results.statement_id, series_list));
    }

    let time_desc = plan
        .saved_time_desc
        .unwrap_or_else(|| original.order_by.as_ref().is_some_and(|o| o.time_desc));

    let time_idx = series_list[0]
        .columns
        .iter()
        .position(|c| c == "time" || c == "__time");

    let mut flat: Vec<(usize, Vec<Value>)> = Vec::new();
    for (series_idx, series) in series_list.iter().enumerate() {
        for row in &series.values {
            flat.push((series_idx, row.clone()));
        }
    }

    flat.sort_by(|a, b| {
        if let Some(ti) = time_idx {
            let at = a.1.get(ti).map(json_sortable_time).unwrap_or(0);
            let bt = b.1.get(ti).map(json_sortable_time).unwrap_or(0);
            if time_desc { bt.cmp(&at) } else { at.cmp(&bt) }
        } else {
            let av = a.1.first().map(|v| v.to_string()).unwrap_or_default();
            let bv = b.1.first().map(|v| v.to_string()).unwrap_or_default();
            if time_desc { bv.cmp(&av) } else { av.cmp(&bv) }
        }
    });

    let offset = plan.saved_offset.unwrap_or(0) as usize;
    if offset > 0 {
        if offset >= flat.len() {
            flat.clear();
        } else {
            flat.drain(0..offset);
        }
    }
    if let Some(limit) = plan.saved_limit {
        let limit = limit as usize;
        if flat.len() > limit {
            flat.truncate(limit);
        }
    }

    for series in &mut series_list {
        series.values.clear();
    }
    for (series_idx, row) in flat {
        let series = series_list
            .get_mut(series_idx)
            .ok_or_else(ragged_merge_row)?;
        series.values.push(row);
    }
    series_list.retain(|s| !s.values.is_empty());

    Ok(QueryResponse::single(results.statement_id, series_list))
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::timeseriesql::ast::{
        Dimension, Duration, DurationUnit, Expr, Field, FunctionCall, GroupBy, SelectStatement,
    };
    use serde_json::json;

    #[test]
    fn merges_series_from_two_parts() {
        let a = QueryResponse::single(
            0,
            vec![SeriesResult {
                name: "cpu".into(),
                tags: Some(HashMap::from([("host".into(), "a".into())])),
                columns: vec!["time".into(), "value".into()],
                values: vec![vec![json!(1), json!(1.0)]],
                partial: None,
            }],
        );
        let b = QueryResponse::single(
            0,
            vec![SeriesResult {
                name: "cpu".into(),
                tags: Some(HashMap::from([("host".into(), "b".into())])),
                columns: vec!["time".into(), "value".into()],
                values: vec![vec![json!(2), json!(2.0)]],
                partial: None,
            }],
        );
        let merged = merge_query_results(vec![a, b]);
        assert_eq!(merged.results[0].series.as_ref().unwrap().len(), 2);
    }

    #[test]
    fn merges_global_count_across_regions() {
        let stmt = SelectStatement {
            fields: vec![Field {
                expr: Expr::Call(FunctionCall {
                    name: "count".into(),
                    args: vec![Expr::Identifier("value".into())],
                }),
                alias: None,
            }],
            into: None,
            from: vec![],
            condition: None,
            group_by: None,
            order_by: None,
            limit: None,
            offset: None,
            slimit: None,
            soffset: None,
            fill: None,
            timezone: None,
        };

        let partial = |n: i64| {
            QueryResponse::single(
                0,
                vec![SeriesResult {
                    name: "metrics".into(),
                    tags: None,
                    columns: vec!["count_value".into()],
                    values: vec![vec![json!(n)]],
                    partial: None,
                }],
            )
        };

        let merged =
            merge_sharded_query_results(vec![partial(5), partial(16)], &stmt, None).expect("merge");
        let series = merged.results[0].series.as_ref().unwrap();
        assert_eq!(series.len(), 1);
        assert_eq!(series[0].values.len(), 1);
        assert_eq!(series[0].values[0][0], json!(21));
    }

    #[test]
    fn merges_group_by_time_buckets() {
        let stmt = SelectStatement {
            fields: vec![Field {
                expr: Expr::Call(FunctionCall {
                    name: "count".into(),
                    args: vec![Expr::Identifier("value".into())],
                }),
                alias: None,
            }],
            into: None,
            from: vec![],
            condition: None,
            group_by: Some(GroupBy {
                dimensions: vec![Dimension::Time {
                    interval: Duration {
                        value: 1,
                        unit: DurationUnit::Hour,
                    },
                    offset: None,
                }],
            }),
            order_by: None,
            limit: None,
            offset: None,
            slimit: None,
            soffset: None,
            fill: None,
            timezone: None,
        };

        let partial = |n: i64| {
            QueryResponse::single(
                0,
                vec![SeriesResult {
                    name: "metrics".into(),
                    tags: None,
                    columns: vec!["time".into(), "count_value".into()],
                    values: vec![vec![json!(1000), json!(n)]],
                    partial: None,
                }],
            )
        };

        let merged =
            merge_sharded_query_results(vec![partial(3), partial(7)], &stmt, None).expect("merge");
        let series = merged.results[0].series.as_ref().unwrap();
        assert_eq!(series.len(), 1);
        assert_eq!(series[0].values.len(), 1);
        assert_eq!(series[0].values[0][1], json!(10));
    }

    #[test]
    fn merges_mean_via_sum_and_count_partials() {
        let stmt = SelectStatement {
            fields: vec![Field {
                expr: Expr::Call(FunctionCall {
                    name: "mean".into(),
                    args: vec![Expr::Identifier("value".into())],
                }),
                alias: None,
            }],
            into: None,
            from: vec![],
            condition: None,
            group_by: Some(GroupBy {
                dimensions: vec![Dimension::Time {
                    interval: Duration {
                        value: 1,
                        unit: DurationUnit::Hour,
                    },
                    offset: None,
                }],
            }),
            order_by: None,
            limit: None,
            offset: None,
            slimit: None,
            soffset: None,
            fill: None,
            timezone: None,
        };
        let plan = prepare_sharded_region_query(&stmt).unwrap();

        let partial = |sum: i64, count: i64| {
            QueryResponse::single(
                0,
                vec![SeriesResult {
                    name: "metrics".into(),
                    tags: None,
                    columns: vec!["time".into(), "sum_value".into(), "count_value".into()],
                    values: vec![vec![json!(1000), json!(sum), json!(count)]],
                    partial: None,
                }],
            )
        };

        let merged =
            merge_sharded_query_results(vec![partial(10, 2), partial(20, 3)], &stmt, Some(&plan))
                .expect("merge");
        let row = &merged.results[0].series.as_ref().unwrap()[0].values[0];
        assert_eq!(row[0], json!(1000));
        assert_eq!(row[1], json!(6.0));
    }

    #[test]
    fn merges_first_by_earliest_selection_time() {
        let stmt = SelectStatement {
            fields: vec![Field {
                expr: Expr::Call(FunctionCall {
                    name: "first".into(),
                    args: vec![Expr::Identifier("value".into())],
                }),
                alias: None,
            }],
            into: None,
            from: vec![],
            condition: None,
            group_by: Some(GroupBy {
                dimensions: vec![Dimension::Time {
                    interval: Duration {
                        value: 1,
                        unit: DurationUnit::Hour,
                    },
                    offset: None,
                }],
            }),
            order_by: None,
            limit: None,
            offset: None,
            slimit: None,
            soffset: None,
            fill: None,
            timezone: None,
        };
        let plan = prepare_sharded_region_query(&stmt).unwrap();

        let partial = |value: i64, sel_time: i64| {
            QueryResponse::single(
                0,
                vec![SeriesResult {
                    name: "metrics".into(),
                    tags: None,
                    columns: vec![
                        "time".into(),
                        "first_value".into(),
                        "first_value__sel_time".into(),
                    ],
                    values: vec![vec![json!(1000), json!(value), json!(sel_time)]],
                    partial: None,
                }],
            )
        };

        let merged = merge_sharded_query_results(
            vec![partial(10, 2000), partial(99, 500)],
            &stmt,
            Some(&plan),
        )
        .expect("merge");
        let row = &merged.results[0].series.as_ref().unwrap()[0].values[0];
        assert_eq!(row[1], json!(99));
    }

    #[test]
    fn merges_last_by_latest_selection_time() {
        let stmt = SelectStatement {
            fields: vec![Field {
                expr: Expr::Call(FunctionCall {
                    name: "last".into(),
                    args: vec![Expr::Identifier("value".into())],
                }),
                alias: None,
            }],
            into: None,
            from: vec![],
            condition: None,
            group_by: None,
            order_by: None,
            limit: None,
            offset: None,
            slimit: None,
            soffset: None,
            fill: None,
            timezone: None,
        };
        let plan = prepare_sharded_region_query(&stmt).unwrap();

        let partial = |value: i64, sel_time: i64| {
            QueryResponse::single(
                0,
                vec![SeriesResult {
                    name: "metrics".into(),
                    tags: None,
                    columns: vec!["last_value".into(), "last_value__sel_time".into()],
                    values: vec![vec![json!(value), json!(sel_time)]],
                    partial: None,
                }],
            )
        };

        let merged = merge_sharded_query_results(
            vec![partial(10, 1000), partial(99, 5000)],
            &stmt,
            Some(&plan),
        )
        .expect("merge");
        let row = &merged.results[0].series.as_ref().unwrap()[0].values[0];
        assert_eq!(row[0], json!(99));
    }

    #[test]
    fn rejects_percentile_on_multi_region() {
        let stmt = SelectStatement {
            fields: vec![Field {
                expr: Expr::Call(FunctionCall {
                    name: "percentile".into(),
                    args: vec![Expr::Identifier("value".into()), Expr::IntegerLiteral(95)],
                }),
                alias: None,
            }],
            into: None,
            from: vec![],
            condition: None,
            group_by: None,
            order_by: None,
            limit: None,
            offset: None,
            slimit: None,
            soffset: None,
            fill: None,
            timezone: None,
        };
        let err = prepare_sharded_region_query(&stmt).unwrap_err();
        assert!(err.to_string().contains(MULTI_REGION_UNSUPPORTED));
    }

    #[test]
    fn merges_stddev_from_partial_moments() {
        let stmt = SelectStatement {
            fields: vec![Field {
                expr: Expr::Call(FunctionCall {
                    name: "stddev".into(),
                    args: vec![Expr::Identifier("value".into())],
                }),
                alias: None,
            }],
            into: None,
            from: vec![],
            condition: None,
            group_by: None,
            order_by: None,
            limit: None,
            offset: None,
            slimit: None,
            soffset: None,
            fill: None,
            timezone: None,
        };
        let plan = prepare_sharded_region_query(&stmt).unwrap();

        // Region A: values 2,4 -> sum=6, sumsq=20, count=2
        // Region B: value 6 -> sum=6, sumsq=36, count=1
        // Combined: sum=12, sumsq=56, count=3, mean=4, sample var = (56 - 144/3)/2 = 4, stddev=2
        let partial = |sum: i64, sumsq: i64, count: i64| {
            QueryResponse::single(
                0,
                vec![SeriesResult {
                    name: "metrics".into(),
                    tags: None,
                    columns: vec![
                        "stddev_sum_value".into(),
                        "stddev_sumsq_value".into(),
                        "stddev_count_value".into(),
                    ],
                    values: vec![vec![json!(sum), json!(sumsq), json!(count)]],
                    partial: None,
                }],
            )
        };

        let merged = merge_sharded_query_results(
            vec![partial(6, 20, 2), partial(6, 36, 1)],
            &stmt,
            Some(&plan),
        )
        .expect("merge");
        let row = &merged.results[0].series.as_ref().unwrap()[0].values[0];
        let stddev = row[0].as_f64().unwrap();
        assert!(
            (stddev - 2.0).abs() < 0.001,
            "expected stddev 2.0, got {stddev}"
        );
    }

    #[test]
    fn prepare_desc_query_records_time_desc() {
        use crate::timeseriesql::parser::parse_query;

        let stmt = match parse_query("SELECT value FROM cpu ORDER BY time DESC LIMIT 2")
            .unwrap()
            .remove(0)
        {
            crate::timeseriesql::ast::Statement::Select(s) => s,
            _ => panic!("select"),
        };
        assert!(stmt.order_by.as_ref().unwrap().time_desc);
        let plan = prepare_sharded_region_query(&stmt).unwrap();
        assert_eq!(plan.saved_time_desc, Some(true));
        assert_eq!(plan.saved_limit, Some(2));
    }

    #[test]
    fn global_sort_applies_descending_order() {
        let stmt = SelectStatement {
            fields: vec![Field {
                expr: Expr::Identifier("value".into()),
                alias: None,
            }],
            into: None,
            from: vec![],
            condition: None,
            group_by: None,
            order_by: Some(crate::timeseriesql::ast::OrderBy { time_desc: false }),
            limit: Some(2),
            offset: Some(1),
            slimit: None,
            soffset: None,
            fill: None,
            timezone: None,
        };
        let plan = prepare_sharded_region_query(&stmt).unwrap();

        let partial = |values: Vec<i64>| {
            QueryResponse::single(
                0,
                vec![SeriesResult {
                    name: "cpu".into(),
                    tags: None,
                    columns: vec!["time".into(), "value".into()],
                    values: values
                        .into_iter()
                        .map(|v| vec![json!(v), json!(v * 10)])
                        .collect(),
                    partial: None,
                }],
            )
        };

        let merged = merge_sharded_query_results(
            vec![partial(vec![1000, 3000]), partial(vec![2000, 4000])],
            &stmt,
            Some(&plan),
        )
        .expect("merge");
        let values = &merged.results[0].series.as_ref().unwrap()[0].values;
        assert_eq!(values.len(), 2);
        assert_eq!(values[0][0], json!(2000));
        assert_eq!(values[1][0], json!(3000));
    }

    #[test]
    fn applies_global_limit_offset_desc_after_merge() {
        let stmt = SelectStatement {
            fields: vec![Field {
                expr: Expr::Identifier("value".into()),
                alias: None,
            }],
            into: None,
            from: vec![],
            condition: None,
            group_by: None,
            order_by: Some(crate::timeseriesql::ast::OrderBy { time_desc: true }),
            limit: Some(2),
            offset: None,
            slimit: None,
            soffset: None,
            fill: None,
            timezone: None,
        };
        let plan = prepare_sharded_region_query(&stmt).unwrap();

        let partial = |values: Vec<i64>| {
            QueryResponse::single(
                0,
                vec![SeriesResult {
                    name: "cpu".into(),
                    tags: None,
                    columns: vec!["time".into(), "value".into()],
                    values: values
                        .into_iter()
                        .map(|v| vec![json!(v), json!(v / 100_000_000)])
                        .collect(),
                    partial: None,
                }],
            )
        };

        let merged = merge_sharded_query_results(
            vec![
                partial(vec![1_000_000_000, 3_000_000_000]),
                partial(vec![2_000_000_000, 4_000_000_000]),
            ],
            &stmt,
            Some(&plan),
        )
        .expect("merge");
        let values = &merged.results[0].series.as_ref().unwrap()[0].values;
        assert_eq!(values.len(), 2);
        assert_eq!(values[0][1].as_f64().unwrap(), 40.0);
        assert_eq!(values[1][1].as_f64().unwrap(), 30.0);
    }

    #[test]
    fn applies_global_limit_offset_desc_with_rfc3339_times() {
        let stmt = SelectStatement {
            fields: vec![Field {
                expr: Expr::Identifier("value".into()),
                alias: None,
            }],
            into: None,
            from: vec![],
            condition: None,
            group_by: None,
            order_by: Some(crate::timeseriesql::ast::OrderBy { time_desc: true }),
            limit: Some(2),
            offset: None,
            slimit: None,
            soffset: None,
            fill: None,
            timezone: None,
        };
        let plan = prepare_sharded_region_query(&stmt).unwrap();

        let partial = |times: &[&str], values: &[f64]| {
            QueryResponse::single(
                0,
                vec![SeriesResult {
                    name: "cpu".into(),
                    tags: None,
                    columns: vec!["time".into(), "value".into()],
                    values: times
                        .iter()
                        .zip(values)
                        .map(|(t, v)| vec![json!(t), json!(v)])
                        .collect(),
                    partial: None,
                }],
            )
        };

        let merged = merge_sharded_query_results(
            vec![
                partial(
                    &["1970-01-01T00:00:01Z", "1970-01-01T00:00:02Z"],
                    &[10.0, 20.0],
                ),
                partial(
                    &["1970-01-01T00:00:03Z", "1970-01-01T00:00:04Z"],
                    &[30.0, 40.0],
                ),
            ],
            &stmt,
            Some(&plan),
        )
        .expect("merge");
        let values = &merged.results[0].series.as_ref().unwrap()[0].values;
        assert_eq!(values.len(), 2);
        assert_eq!(values[0][1].as_f64().unwrap(), 40.0);
        assert_eq!(values[1][1].as_f64().unwrap(), 30.0);
    }

    #[test]
    fn applies_global_limit_offset_desc_with_multi_tag_series() {
        let stmt = SelectStatement {
            fields: vec![Field {
                expr: Expr::Identifier("value".into()),
                alias: None,
            }],
            into: None,
            from: vec![],
            condition: None,
            group_by: None,
            order_by: Some(crate::timeseriesql::ast::OrderBy { time_desc: true }),
            limit: Some(2),
            offset: None,
            slimit: None,
            soffset: None,
            fill: None,
            timezone: None,
        };
        let plan = prepare_sharded_region_query(&stmt).unwrap();

        let partial = |host: &str, times: &[i64], values: &[f64]| {
            QueryResponse::single(
                0,
                vec![SeriesResult {
                    name: "cpu".into(),
                    tags: Some(HashMap::from([("host".into(), host.into())])),
                    columns: vec!["time".into(), "value".into()],
                    values: times
                        .iter()
                        .zip(values)
                        .map(|(t, v)| vec![json!(t), json!(v)])
                        .collect(),
                    partial: None,
                }],
            )
        };

        let merged = merge_sharded_query_results(
            vec![
                partial("host_low", &[1_000_000_000, 2_000_000_000], &[10.0, 20.0]),
                partial("host_high", &[3_000_000_000, 4_000_000_000], &[30.0, 40.0]),
            ],
            &stmt,
            Some(&plan),
        )
        .expect("merge");
        let mut values: Vec<f64> = merged.results[0]
            .series
            .as_ref()
            .unwrap()
            .iter()
            .flat_map(|s| s.values.iter().map(|row| row[1].as_f64().unwrap()))
            .collect();
        values.sort_by(|a, b| b.partial_cmp(a).unwrap());
        assert_eq!(values.len(), 2);
        assert_eq!(values[0], 40.0);
        assert_eq!(values[1], 30.0);
    }

    fn mv_meta_sum() -> MeasurementMeta {
        MeasurementMeta {
            name: "cpu_5m".into(),
            field_types: HashMap::from([("total".into(), 0)]),
            tag_keys: vec!["host".into()],
            field_rollups: HashMap::from([("total".into(), RollupCombine::Sum)]),
            mean_fields: HashMap::new(),
            materialized: true,
            materialized_rp: Some("autogen".into()),
        }
    }

    #[test]
    fn merge_materialized_sums_partials_with_same_time_and_tags() {
        let partial = |n: i64| {
            QueryResponse::single(
                0,
                vec![SeriesResult {
                    name: "cpu_5m".into(),
                    tags: Some(HashMap::from([("host".into(), "a".into())])),
                    columns: vec!["time".into(), "total".into()],
                    values: vec![vec![json!(1000), json!(n)]],
                    partial: None,
                }],
            )
        };
        let merged =
            merge_materialized_rollup_results(vec![partial(3), partial(7)], &mv_meta_sum())
                .expect("merge");
        let series = merged.results[0].series.as_ref().unwrap();
        assert_eq!(series.len(), 1);
        assert_eq!(series[0].values.len(), 1);
        assert_eq!(series[0].values[0][1], json!(10));
    }

    #[test]
    fn merge_materialized_keeps_distinct_tag_sets_separate() {
        let a = QueryResponse::single(
            0,
            vec![SeriesResult {
                name: "cpu_5m".into(),
                tags: Some(HashMap::from([("host".into(), "a".into())])),
                columns: vec!["time".into(), "total".into()],
                values: vec![vec![json!(1000), json!(1)]],
                partial: None,
            }],
        );
        let b = QueryResponse::single(
            0,
            vec![SeriesResult {
                name: "cpu_5m".into(),
                tags: Some(HashMap::from([("host".into(), "b".into())])),
                columns: vec!["time".into(), "total".into()],
                values: vec![vec![json!(1000), json!(2)]],
                partial: None,
            }],
        );
        let merged = merge_materialized_rollup_results(vec![a, b], &mv_meta_sum()).expect("merge");
        assert_eq!(merged.results[0].series.as_ref().unwrap().len(), 2);
    }

    #[test]
    fn merge_materialized_mean_storage_sums_sum_and_count_columns() {
        let meta = MeasurementMeta {
            name: "cpu_5m".into(),
            field_types: HashMap::from([("sum_value".into(), 0), ("count_value".into(), 0)]),
            tag_keys: vec!["host".into()],
            field_rollups: HashMap::from([
                ("sum_value".into(), RollupCombine::Sum),
                ("count_value".into(), RollupCombine::Sum),
            ]),
            mean_fields: HashMap::from([(
                "value".into(),
                MeanRollupField {
                    sum_col: "sum_value".into(),
                    count_col: "count_value".into(),
                },
            )]),
            materialized: true,
            materialized_rp: Some("autogen".into()),
        };
        let partial = |sum: i64, count: i64| {
            QueryResponse::single(
                0,
                vec![SeriesResult {
                    name: "cpu_5m".into(),
                    tags: Some(HashMap::from([("host".into(), "a".into())])),
                    columns: vec!["time".into(), "sum_value".into(), "count_value".into()],
                    values: vec![vec![json!(1000), json!(sum), json!(count)]],
                    partial: None,
                }],
            )
        };
        let merged = merge_materialized_rollup_results(vec![partial(10, 2), partial(20, 3)], &meta)
            .expect("merge");
        let row = &merged.results[0].series.as_ref().unwrap()[0].values[0];
        assert_eq!(row[1], json!(30));
        assert_eq!(row[2], json!(5));
    }

    #[test]
    fn merge_materialized_min_max() {
        let meta = MeasurementMeta {
            name: "cpu_5m".into(),
            field_types: HashMap::from([("min_v".into(), 0), ("max_v".into(), 0)]),
            tag_keys: vec![],
            field_rollups: HashMap::from([
                ("min_v".into(), RollupCombine::Min),
                ("max_v".into(), RollupCombine::Max),
            ]),
            mean_fields: HashMap::new(),
            materialized: true,
            materialized_rp: None,
        };
        let partial = |min_v: f64, max_v: f64| {
            QueryResponse::single(
                0,
                vec![SeriesResult {
                    name: "cpu_5m".into(),
                    tags: None,
                    columns: vec!["time".into(), "min_v".into(), "max_v".into()],
                    values: vec![vec![json!(1000), json!(min_v), json!(max_v)]],
                    partial: None,
                }],
            )
        };
        let merged =
            merge_materialized_rollup_results(vec![partial(5.0, 20.0), partial(3.0, 25.0)], &meta)
                .expect("merge");
        let row = &merged.results[0].series.as_ref().unwrap()[0].values[0];
        assert_eq!(row[1], json!(3.0));
        assert_eq!(row[2], json!(25.0));
    }

    #[test]
    fn count_distinct_keeps_template_row_aligned_with_its_group() {
        // Regression: groups were zipped with input rows by position, so a
        // group could inherit another bucket's template row (wrong time/tags).
        let stmt = SelectStatement {
            fields: vec![Field {
                expr: Expr::Call(FunctionCall {
                    name: "count".into(),
                    args: vec![Expr::Call(FunctionCall {
                        name: "distinct".into(),
                        args: vec![Expr::Identifier("value".into())],
                    })],
                }),
                alias: Some("count_value".into()),
            }],
            into: None,
            from: vec![],
            condition: None,
            group_by: Some(GroupBy {
                dimensions: vec![Dimension::Time {
                    interval: Duration {
                        value: 1,
                        unit: DurationUnit::Hour,
                    },
                    offset: None,
                }],
            }),
            order_by: None,
            limit: None,
            offset: None,
            slimit: None,
            soffset: None,
            fill: None,
            timezone: None,
        };
        let plan = prepare_sharded_region_query(&stmt).unwrap();

        let partial = |time: i64, distinct: &str| {
            QueryResponse::single(
                0,
                vec![SeriesResult {
                    name: "metrics".into(),
                    tags: None,
                    columns: vec!["time".into(), "distinct_value".into(), "count_value".into()],
                    values: vec![vec![json!(time), json!(distinct), json!(0)]],
                    partial: None,
                }],
            )
        };

        // Two regions, two time buckets each; bucket t=1000 has more rows than
        // t=2000 in part A so naive positional pairing would misalign.
        let merged = merge_sharded_query_results(
            vec![partial(1000, "a"), partial(2000, "b"), partial(1000, "c")],
            &stmt,
            Some(&plan),
        )
        .expect("merge");
        let series = merged.results[0].series.as_ref().unwrap();
        let mut by_time = std::collections::HashMap::new();
        for s in series {
            for row in &s.values {
                by_time.insert(row[0].as_i64().unwrap(), row[1].as_i64().unwrap());
            }
        }
        assert_eq!(
            by_time.get(&1000),
            Some(&2),
            "t=1000 must carry its own distinct set"
        );
        assert_eq!(
            by_time.get(&2000),
            Some(&1),
            "t=2000 must carry its own distinct set"
        );
    }

    #[test]
    fn plan_less_mean_merge_does_not_sum_region_means() {
        // Without partial columns there is no correct way to combine means;
        // the fallback must not fabricate a summed value.
        let stmt = SelectStatement {
            fields: vec![Field {
                expr: Expr::Call(FunctionCall {
                    name: "mean".into(),
                    args: vec![Expr::Identifier("value".into())],
                }),
                alias: None,
            }],
            into: None,
            from: vec![],
            condition: None,
            group_by: None,
            order_by: None,
            limit: None,
            offset: None,
            slimit: None,
            soffset: None,
            fill: None,
            timezone: None,
        };
        let partial = |v: f64| {
            QueryResponse::single(
                0,
                vec![SeriesResult {
                    name: "metrics".into(),
                    tags: None,
                    columns: vec!["mean_value".into()],
                    values: vec![vec![json!(v)]],
                    partial: None,
                }],
            )
        };
        let merged = merge_sharded_query_results(vec![partial(10.0), partial(20.0)], &stmt, None)
            .expect("merge");
        let row = &merged.results[0].series.as_ref().unwrap()[0].values[0];
        assert_ne!(
            row[0],
            json!(30.0),
            "plan-less merge must not sum means across regions"
        );
    }

    #[test]
    fn ragged_empty_row_with_time_column_is_error() {
        let stmt = SelectStatement {
            fields: vec![Field {
                expr: Expr::Call(FunctionCall {
                    name: "count".into(),
                    args: vec![Expr::Identifier("value".into())],
                }),
                alias: None,
            }],
            into: None,
            from: vec![],
            condition: None,
            group_by: None,
            order_by: None,
            limit: None,
            offset: None,
            slimit: None,
            soffset: None,
            fill: None,
            timezone: None,
        };
        let a = QueryResponse::single(
            0,
            vec![SeriesResult {
                name: "metrics".into(),
                tags: None,
                columns: vec!["time".into(), "count_value".into()],
                values: vec![vec![]],
                partial: None,
            }],
        );
        let b = QueryResponse::single(
            0,
            vec![SeriesResult {
                name: "metrics".into(),
                tags: None,
                columns: vec!["time".into(), "count_value".into()],
                values: vec![vec![json!(1), json!(1)]],
                partial: None,
            }],
        );
        let err = merge_sharded_query_results(vec![a, b], &stmt, None).unwrap_err();
        assert!(err.to_string().contains("ragged shard merge row"), "{err}");
    }

    #[test]
    fn mismatched_row_width_is_error() {
        let stmt = SelectStatement {
            fields: vec![Field {
                expr: Expr::Call(FunctionCall {
                    name: "sum".into(),
                    args: vec![Expr::Identifier("value".into())],
                }),
                alias: None,
            }],
            into: None,
            from: vec![],
            condition: None,
            group_by: None,
            order_by: None,
            limit: None,
            offset: None,
            slimit: None,
            soffset: None,
            fill: None,
            timezone: None,
        };
        let a = QueryResponse::single(
            0,
            vec![SeriesResult {
                name: "metrics".into(),
                tags: None,
                columns: vec!["sum_value".into()],
                values: vec![vec![json!(1), json!(2)]],
                partial: None,
            }],
        );
        let b = QueryResponse::single(
            0,
            vec![SeriesResult {
                name: "metrics".into(),
                tags: None,
                columns: vec!["sum_value".into()],
                values: vec![vec![json!(3)]],
                partial: None,
            }],
        );
        let err = merge_sharded_query_results(vec![a, b], &stmt, None).unwrap_err();
        assert!(err.to_string().contains("ragged shard merge row"), "{err}");
    }

    #[test]
    fn json_sum_does_not_wrap_i64_max() {
        let stmt = SelectStatement {
            fields: vec![Field {
                expr: Expr::Call(FunctionCall {
                    name: "sum".into(),
                    args: vec![Expr::Identifier("value".into())],
                }),
                alias: None,
            }],
            into: None,
            from: vec![],
            condition: None,
            group_by: None,
            order_by: None,
            limit: None,
            offset: None,
            slimit: None,
            soffset: None,
            fill: None,
            timezone: None,
        };
        let partial = |n: i64| {
            QueryResponse::single(
                0,
                vec![SeriesResult {
                    name: "metrics".into(),
                    tags: None,
                    columns: vec!["sum_value".into()],
                    values: vec![vec![json!(n)]],
                    partial: None,
                }],
            )
        };
        let merged = merge_sharded_query_results(vec![partial(i64::MAX), partial(1)], &stmt, None)
            .expect("merge");
        let row = &merged.results[0].series.as_ref().unwrap()[0].values[0];
        let n = row[0]
            .as_f64()
            .unwrap_or_else(|| row[0].as_i64().unwrap() as f64);
        assert!(
            (n - (i64::MAX as f64 + 1.0)).abs() < 1.0,
            "expected f64 fallback, got {row:?}"
        );
        assert!(
            row[0].as_i64() != Some(i64::MIN),
            "i64 must not wrap: {row:?}"
        );
    }
}
