use std::collections::HashMap;

use serde_json::{json, Value};

use crate::domain::query_result::{QueryResponse, SeriesResult};
use crate::domain::rollup::{rollup_combine_from_field, RollupCombine};
use crate::timeseriesql::ast::{Expr, SelectStatement};
use crate::timeseriesql::to_clickhouse::{select_has_true_aggregate, select_output_field_name};

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

    let statement_id = parts[0].results.first().map(|r| r.statement_id).unwrap_or(0);
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
                    merged
                        .entry(key)
                        .and_modify(|existing| {
                            existing.values.extend(series.values.clone());
                        })
                        .or_insert(series);
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
) -> QueryResponse {
    if !select_has_true_aggregate(stmt) {
        return merge_query_results(parts);
    }
    merge_aggregate_parts(parts, stmt)
}

#[derive(Clone, Copy)]
enum ColumnMerge {
    Sum,
    Min,
    Max,
    Mean,
    First,
    Last,
    Passthrough,
}

fn column_merge_rules(stmt: &SelectStatement) -> HashMap<String, ColumnMerge> {
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
            Expr::Call(ref func) if func.name.eq_ignore_ascii_case("mean")
        ) {
            ColumnMerge::Mean
        } else {
            ColumnMerge::Passthrough
        };
        rules.insert(name, merge);
    }
    rules
}

fn series_row_key(series: &SeriesResult, row: &[Value]) -> String {
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
        key.push('|');
        key.push_str(&row[idx].to_string());
    }
    key
}

fn json_as_f64(v: &Value) -> Option<f64> {
    v.as_f64()
        .or_else(|| v.as_i64().map(|i| i as f64))
        .or_else(|| v.as_u64().map(|u| u as f64))
}

fn json_sum(left: &Value, right: &Value) -> Value {
    if let (Some(l), Some(r)) = (left.as_i64(), right.as_i64()) {
        return json!(l + r);
    }
    if let (Some(l), Some(r)) = (left.as_u64(), right.as_u64()) {
        return json!(l + r);
    }
    let l = json_as_f64(left).unwrap_or(0.0);
    let r = json_as_f64(right).unwrap_or(0.0);
    json!(l + r)
}

fn merge_values(
    left: &Value,
    right: &Value,
    rule: ColumnMerge,
    mean_count: &mut u64,
) -> Value {
    match rule {
        ColumnMerge::Passthrough | ColumnMerge::First => left.clone(),
        ColumnMerge::Last => right.clone(),
        ColumnMerge::Sum => json_sum(left, right),
        ColumnMerge::Min => {
            let l = json_as_f64(left);
            let r = json_as_f64(right);
            match (l, r) {
                (Some(a), Some(b)) => serde_json::json!(a.min(b)),
                (Some(a), None) => serde_json::json!(a),
                (None, Some(b)) => serde_json::json!(b),
                (None, None) => left.clone(),
            }
        }
        ColumnMerge::Max => {
            let l = json_as_f64(left);
            let r = json_as_f64(right);
            match (l, r) {
                (Some(a), Some(b)) => serde_json::json!(a.max(b)),
                (Some(a), None) => serde_json::json!(a),
                (None, Some(b)) => serde_json::json!(b),
                (None, None) => left.clone(),
            }
        }
        ColumnMerge::Mean => {
            let l = json_as_f64(left).unwrap_or(0.0);
            let r = json_as_f64(right).unwrap_or(0.0);
            *mean_count = mean_count.saturating_add(1);
            serde_json::json!((l * (*mean_count as f64 - 1.0) + r) / *mean_count as f64)
        }
    }
}

fn merge_row(
    columns: &[String],
    rules: &HashMap<String, ColumnMerge>,
    left: &[Value],
    right: &[Value],
) -> Vec<Value> {
    let mut mean_counts = vec![1u64; columns.len()];
    let mut out = left.to_vec();
    for (i, col) in columns.iter().enumerate() {
        let rule = rules.get(col).copied().unwrap_or(ColumnMerge::Passthrough);
        out[i] = merge_values(&out[i], &right[i], rule, &mut mean_counts[i]);
    }
    out
}

fn merge_aggregate_parts(parts: Vec<QueryResponse>, stmt: &SelectStatement) -> QueryResponse {
    if parts.is_empty() {
        return QueryResponse::empty(0);
    }
    if parts.len() == 1 {
        if let Some(part) = parts.into_iter().next() {
            return part;
        }
        return QueryResponse::empty(0);
    }

    let statement_id = parts[0].results.first().map(|r| r.statement_id).unwrap_or(0);
    let rules = column_merge_rules(stmt);

    // row_key -> (SeriesResult template, merged row)
    let mut rows: HashMap<String, (SeriesResult, Vec<Value>)> = HashMap::new();

    for part in parts {
        for stmt_result in part.results {
            let Some(series_list) = stmt_result.series else {
                continue;
            };
            for series in series_list {
                for row in &series.values {
                    let key = series_row_key(&series, row);
                    rows.entry(key)
                        .and_modify(|(_, merged_row)| {
                            *merged_row =
                                merge_row(&series.columns, &rules, merged_row, row);
                        })
                        .or_insert_with(|| (series.clone(), row.clone()));
                }
            }
        }
    }

    // Group merged rows back into series buckets by name+tags.
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
        by_series
            .entry(series_key)
            .and_modify(|existing| existing.values.push(row.clone()))
            .or_insert_with(|| SeriesResult {
                name: series.name.clone(),
                tags: series.tags.clone(),
                columns: series.columns.clone(),
                values: vec![row],
                partial: series.partial,
            });
    }

    let mut series: Vec<SeriesResult> = by_series.into_values().collect();
    series.sort_by(|a, b| a.name.cmp(&b.name));
    QueryResponse::single(statement_id, series)
}

#[cfg(test)]
mod tests {
    use super::*;
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
        use crate::timeseriesql::ast::{Expr, Field, FunctionCall, SelectStatement};

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

        let merged = merge_sharded_query_results(vec![partial(5), partial(16)], &stmt);
        let series = merged.results[0].series.as_ref().unwrap();
        assert_eq!(series.len(), 1);
        assert_eq!(series[0].values.len(), 1);
        assert_eq!(series[0].values[0][0], json!(21));
    }

    #[test]
    fn merges_group_by_time_buckets() {
        use crate::timeseriesql::ast::{
            Dimension, Expr, Field, FunctionCall, GroupBy, SelectStatement,
        };

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
                    interval: crate::timeseriesql::ast::Duration {
                        value: 1,
                        unit: crate::timeseriesql::ast::DurationUnit::Hour,
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

        let merged = merge_sharded_query_results(vec![partial(3), partial(7)], &stmt);
        let series = merged.results[0].series.as_ref().unwrap();
        assert_eq!(series.len(), 1);
        assert_eq!(series[0].values.len(), 1);
        assert_eq!(series[0].values[0][1], json!(10));
    }
}
