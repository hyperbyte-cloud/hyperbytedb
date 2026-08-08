use crate::domain::chdb_naming::QuotedTableName;
use crate::domain::column_mapping::ColumnMapping;
use crate::domain::rollup::{aggregate_source_field_name, mean_rollup_column_names};
use crate::error::HyperbytedbError;
use crate::timeseriesql::ast::*;
use std::fmt::Write;

use super::coalesce::build_coalesced_fact_view_with_row_meta;
use super::conditions::{quote_phys_identifier, translate_expr};
use super::rename::rename_time_bucket_alias;
use super::select::{
    select_output_field_name, time_bucket_expr_on, translate_field, translate_inner,
};
use super::time_bounds::is_time_epoch_comparison;
use super::{SeriesJoin, validate_select_into};

/// Wrap a translated SELECT as `INSERT INTO <dest> SELECT ...`, renaming `__time` to `time`
/// for the destination measurement schema.
pub fn translate_select_into(
    stmt: &SelectStatement,
    dest_table: &QuotedTableName,
    source: &str,
    mapping: Option<&ColumnMapping>,
) -> Result<String, HyperbytedbError> {
    validate_select_into(stmt)?;
    let select_sql = translate_inner(stmt, source, mapping, None, None)?;
    let select_sql = rename_time_bucket_alias(&select_sql);
    Ok(format!("INSERT INTO {dest_table}\n{select_sql}"))
}

pub(super) fn translate_materialized_view_field(
    field: &Field,
    group_by: Option<&GroupBy>,
    mapping: &ColumnMapping,
) -> Result<String, HyperbytedbError> {
    if let Expr::Call(func) = &field.expr
        && func.name.eq_ignore_ascii_case("mean")
    {
        let source = aggregate_source_field_name(func)?;
        let col = mapping.physical_select_identifier(&source);
        let col_q = quote_phys_identifier(&col);
        let (sum_col, count_col) = mean_rollup_column_names(&source);
        return Ok(format!(
            "sum({col_q}) AS {}, count({col_q}) AS {}",
            quote_phys_identifier(&sum_col),
            quote_phys_identifier(&count_col)
        ));
    }
    translate_field(field, false, 0.0, group_by, Some(mapping))
}

/// Ensure coalesced MV source rows expose every field referenced in the SELECT.
pub(super) fn mapping_with_mv_aggregate_fields(
    mapping: &ColumnMapping,
    fields: &[Field],
) -> ColumnMapping {
    let mut expanded = mapping.clone();
    for field in fields {
        if let Expr::Call(func) = &field.expr
            && let Ok(source) = aggregate_source_field_name(func)
        {
            expanded
                .field_names
                .insert(mapping.physical_select_identifier(&source));
        }
    }
    expanded
}

/// ClickHouse `SELECT` body for a fact-table materialized view. Joins the source
/// series dimension, groups by the MV's `GROUP BY time(...)` bucket and tag
/// dimensions (dropping tags omitted from the GROUP BY, e.g. `server_id`), and
/// assigns a destination `series_id` via [`crate::domain::series::series_id_ch_sql`].
pub fn translate_materialized_view_select(
    stmt: &SelectStatement,
    source_fact: &QuotedTableName,
    source_series: &QuotedTableName,
    dest_measurement: &str,
    mapping: &ColumnMapping,
) -> Result<String, HyperbytedbError> {
    validate_select_into(stmt)?;
    let gb = stmt
        .group_by
        .as_ref()
        .ok_or_else(|| HyperbytedbError::QueryParse("MV requires GROUP BY".to_string()))?;
    let Some(Dimension::Time { interval, offset }) = gb.time_dimension() else {
        return Err(HyperbytedbError::QueryParse(
            "MV requires GROUP BY time(...)".to_string(),
        ));
    };
    let time_bucket = time_bucket_expr_on(
        "t.time",
        interval,
        offset.as_ref(),
        stmt.timezone.as_deref(),
    );

    let mut grouped_tags: Vec<String> = gb
        .tag_dimensions()
        .iter()
        .map(|s| (*s).to_string())
        .collect();
    grouped_tags.sort();

    let series_id_expr = crate::domain::series::series_id_ch_sql_for_tags(
        dest_measurement,
        &grouped_tags,
        |tag| quote_phys_identifier(&mapping.physical_tag_column_name(tag)),
        "s",
    );

    // Field columns must appear in sorted-by-name order to match the
    // destination fact table's DDL column order (build_create_table_sql
    // sorts fields by physical name). ClickHouse INSERT matches by position
    // when no explicit column list is given in the TO clause.
    // mean() expands to two columns (sum_col, count_col) — flatten them
    // individually so the interleaved sort is correct.
    let mut field_expr_by_name: std::collections::BTreeMap<String, String> =
        std::collections::BTreeMap::new();
    for field in &stmt.fields {
        if let Expr::Call(func) = &field.expr
            && func.name.eq_ignore_ascii_case("mean")
        {
            let source = aggregate_source_field_name(func)?;
            let col = mapping.physical_select_identifier(&source);
            let col_q = quote_phys_identifier(&col);
            let (sum_col, count_col) = mean_rollup_column_names(&source);
            let sum_expr = format!("sum({col_q}) AS {}", quote_phys_identifier(&sum_col));
            let count_expr = format!("count({col_q}) AS {}", quote_phys_identifier(&count_col));
            field_expr_by_name.insert(sum_col.clone(), sum_expr);
            field_expr_by_name.insert(count_col.clone(), count_expr);
        } else {
            let expr = translate_materialized_view_field(field, stmt.group_by.as_ref(), mapping)?;
            let name = select_output_field_name(field).ok_or_else(|| {
                HyperbytedbError::QueryParse(
                    "materialized view field requires a name or alias".to_string(),
                )
            })?;
            field_expr_by_name.insert(name, expr);
        }
    }
    let sorted_field_strs: Vec<String> = field_expr_by_name.into_values().collect();

    let mut select_parts = vec![
        format!("{time_bucket} AS time"),
        "any(t.`_mv_src_origin_node_id`) AS origin_node_id".to_string(),
        "max(t.`_mv_src_ingest_seq`) AS ingest_seq".to_string(),
        format!("min({series_id_expr}) AS series_id"),
    ];
    select_parts.extend(sorted_field_strs);

    let mut group_parts = vec![time_bucket.clone()];
    for tag in &grouped_tags {
        group_parts.push(format!(
            "s.{}",
            quote_phys_identifier(&mapping.physical_tag_column_name(tag))
        ));
    }

    let mut out = String::new();
    write!(out, "SELECT {}", select_parts.join(", "))?;
    let source_mapping = mapping_with_mv_aggregate_fields(mapping, &stmt.fields);
    let coalesced_source = build_coalesced_fact_view_with_row_meta(source_fact, &source_mapping);
    // ANY LEFT JOIN for consistency with the query path: fact rows whose series
    // row hasn't landed yet must not be silently dropped from the rollup.
    write!(
        out,
        "\nFROM {coalesced_source} AS t ANY LEFT JOIN {source_series} AS s ON t.`series_id` = s.`series_id`"
    )?;

    if let Some(ref cond) = stmt.condition {
        write!(out, "\nWHERE ")?;
        translate_expr(cond, &mut out, true, Some(mapping))?;
    }

    write!(out, "\nGROUP BY {}", group_parts.join(", "))?;
    Ok(out)
}

/// ClickHouse `SELECT` for the destination series-dimension MV: one row per
/// rolled-up tag combination (tags not listed in the MV GROUP BY are dropped).
///
/// `tag_name_mapping` controls how logical tag keys map to physical column
/// names (tag-field collision prefix). The source mapping uses the *source*
/// measurement's field names for collision detection, but the *destination*
/// series table may have a different set of field columns (MV aliases rename
/// fields), so callers should pass a dedicated mapping (or set of field names)
/// that reflects the destination schema for correct physical column naming.
pub fn translate_materialized_view_series_select(
    stmt: &SelectStatement,
    source_series: &QuotedTableName,
    dest_measurement: &str,
    mapping: &ColumnMapping,
    dest_field_names: Option<&std::collections::HashSet<String>>,
) -> Result<String, HyperbytedbError> {
    let gb = stmt
        .group_by
        .as_ref()
        .ok_or_else(|| HyperbytedbError::QueryParse("MV requires GROUP BY".to_string()))?;
    let mut grouped_tags: Vec<String> = gb
        .tag_dimensions()
        .iter()
        .map(|s| (*s).to_string())
        .collect();
    grouped_tags.sort();

    if grouped_tags.is_empty() {
        return Ok(format!(
            "SELECT min({}) AS series_id FROM {source_series} AS s GROUP BY tuple()",
            crate::domain::series::series_id_ch_sql(dest_measurement, &[] as &[String])
        ));
    }

    // Resolve physical tag column names: use destination field names when
    // provided (the destination series table's column naming depends on the
    // destination's field set, not the source's).
    let tag_phys_name = |tag: &str| -> String {
        match dest_field_names {
            Some(dfn) => {
                let fields: std::collections::HashSet<&str> =
                    dfn.iter().map(|s| s.as_str()).collect();
                crate::domain::chdb_naming::tag_column_name(tag, &fields)
            }
            None => mapping.physical_tag_column_name(tag),
        }
    };

    let series_id_expr = crate::domain::series::series_id_ch_sql_for_tags(
        dest_measurement,
        &grouped_tags,
        |tag| quote_phys_identifier(&tag_phys_name(tag)),
        "s",
    );

    let tag_cols: Vec<String> = grouped_tags
        .iter()
        .map(|tag| format!("s.{}", quote_phys_identifier(&tag_phys_name(tag))))
        .collect();

    let mut select_parts = vec![format!("min({series_id_expr}) AS series_id")];
    select_parts.extend(tag_cols.iter().cloned());

    let mut out = String::new();
    write!(out, "SELECT {}", select_parts.join(", "))?;
    write!(out, "\nFROM {source_series} AS s")?;
    write!(out, "\nGROUP BY {}", tag_cols.join(", "))?;
    Ok(out)
}

/// `INSERT INTO <dest> SELECT ...` for one-time MV backfill of historical data.
pub fn translate_materialized_view_backfill(
    stmt: &SelectStatement,
    dest_table: &QuotedTableName,
    source_fact: &QuotedTableName,
    source_series: &QuotedTableName,
    dest_measurement: &str,
    mapping: &ColumnMapping,
) -> Result<String, HyperbytedbError> {
    let select_sql = translate_materialized_view_select(
        stmt,
        source_fact,
        source_series,
        dest_measurement,
        mapping,
    )?;
    let insert_cols = materialized_view_dest_insert_columns(stmt)?;
    Ok(format!(
        "INSERT INTO {dest_table} ({insert_cols})\nSELECT {insert_cols}\nFROM (\n{select_sql}\n)"
    ))
}

/// Destination fact columns in physical DDL order (matches [`build_create_table_sql`]).
pub(super) fn materialized_view_dest_insert_columns(
    stmt: &SelectStatement,
) -> Result<String, HyperbytedbError> {
    let mut cols = vec![
        quote_phys_identifier("time"),
        quote_phys_identifier("origin_node_id"),
        quote_phys_identifier("ingest_seq"),
        quote_phys_identifier("series_id"),
    ];
    let mut field_names = materialized_view_dest_field_names(stmt)?;
    field_names.sort();
    cols.extend(field_names.into_iter().map(|n| quote_phys_identifier(&n)));
    Ok(cols.join(", "))
}

/// Output column names for MV destination fields (expands `mean()` to sum/count pairs).
pub(super) fn materialized_view_dest_field_names(
    stmt: &SelectStatement,
) -> Result<Vec<String>, HyperbytedbError> {
    let mut names = Vec::new();
    for field in &stmt.fields {
        if let Expr::Call(func) = &field.expr
            && func.name.eq_ignore_ascii_case("mean")
        {
            let source = aggregate_source_field_name(func)?;
            let (sum_col, count_col) = mean_rollup_column_names(&source);
            names.push(sum_col);
            names.push(count_col);
            continue;
        }
        names.push(select_output_field_name(field).ok_or_else(|| {
            HyperbytedbError::QueryParse(
                "materialized view field requires a name or alias".to_string(),
            )
        })?);
    }
    Ok(names)
}

/// Full `CREATE MATERIALIZED VIEW ... TO ... AS SELECT ...` DDL for the fact MV.
pub fn build_create_fact_materialized_view(
    mv_name: &QuotedTableName,
    dest_table: &QuotedTableName,
    select_sql: &str,
) -> String {
    format!("CREATE MATERIALIZED VIEW {mv_name} TO {dest_table} AS\n{select_sql}")
}

/// Full `CREATE MATERIALIZED VIEW ... TO ... AS SELECT ...` for the series MV.
pub fn build_create_series_materialized_view(
    mv_name: &QuotedTableName,
    dest_series: &QuotedTableName,
    select_sql: &str,
) -> String {
    format!("CREATE MATERIALIZED VIEW {mv_name} TO {dest_series} AS\n{select_sql}")
}

/// Like [`translate_select_into`], targeting a native MergeTree table source.
/// `series` lets a tag-grouped continuous query resolve tags from the source
/// measurement's dimension table.
pub fn translate_select_into_native(
    stmt: &SelectStatement,
    dest_table: &QuotedTableName,
    source_table: &QuotedTableName,
    mapping: Option<&ColumnMapping>,
    series: Option<SeriesJoin<'_>>,
) -> Result<String, HyperbytedbError> {
    validate_select_into(stmt)?;
    let select_sql = translate_inner(stmt, source_table.as_str(), mapping, series, None)?;
    let select_sql = rename_time_bucket_alias(&select_sql);
    Ok(format!("INSERT INTO {dest_table}\n{select_sql}"))
}

/// Remove user-supplied `time` comparisons from a WHERE clause. InfluxDB CQs
/// ignore user time ranges and inject their own window each run.
pub fn strip_time_predicates(condition: Option<Expr>) -> Option<Expr> {
    condition.and_then(strip_time_predicates_expr)
}

pub(super) fn strip_time_predicates_expr(expr: Expr) -> Option<Expr> {
    match expr {
        Expr::BinaryExpr(be) if matches!(be.op, BinaryOp::And) => {
            let left = strip_time_predicates_expr(be.left);
            let right = strip_time_predicates_expr(be.right);
            match (left, right) {
                (None, None) => None,
                (Some(l), None) => Some(l),
                (None, Some(r)) => Some(r),
                (Some(l), Some(r)) => Some(Expr::BinaryExpr(Box::new(BinaryExpr {
                    op: BinaryOp::And,
                    left: l,
                    right: r,
                }))),
            }
        }
        Expr::BinaryExpr(be) if is_time_epoch_comparison(&be) => None,
        other => Some(other),
    }
}

/// Build a WHERE clause for CQ coverage `[start, end)` in nanoseconds.
pub fn cq_time_window_condition(start_nanos: i64, end_nanos: i64) -> Expr {
    Expr::BinaryExpr(Box::new(BinaryExpr {
        op: BinaryOp::And,
        left: Expr::BinaryExpr(Box::new(BinaryExpr {
            op: BinaryOp::Gte,
            left: Expr::Identifier("time".to_string()),
            right: Expr::IntegerLiteral(start_nanos),
        })),
        right: Expr::BinaryExpr(Box::new(BinaryExpr {
            op: BinaryOp::Lt,
            left: Expr::Identifier("time".to_string()),
            right: Expr::IntegerLiteral(end_nanos),
        })),
    }))
}

/// Prepare a CQ inner SELECT for execution: strip user time bounds, inject the
/// computed coverage window, and optionally strip `fill()` (basic syntax).
pub fn prepare_cq_select(
    stmt: &SelectStatement,
    start_nanos: i64,
    end_nanos: i64,
    strip_fill: bool,
) -> SelectStatement {
    let mut prepared = stmt.clone();
    let window = cq_time_window_condition(start_nanos, end_nanos);
    let remaining = strip_time_predicates(prepared.condition.take());
    prepared.condition = Some(match remaining {
        Some(existing) => Expr::BinaryExpr(Box::new(BinaryExpr {
            op: BinaryOp::And,
            left: existing,
            right: window,
        })),
        None => window,
    });
    if strip_fill {
        prepared.fill = None;
    }
    prepared
}

/// `INSERT INTO <dest> SELECT ...` for a bounded CQ run against native tables.
pub fn translate_bounded_cq_into(
    stmt: &SelectStatement,
    dest_table: &QuotedTableName,
    source_table: &QuotedTableName,
    mapping: Option<&ColumnMapping>,
    series: Option<SeriesJoin<'_>>,
    start_nanos: i64,
    end_nanos: i64,
) -> Result<String, HyperbytedbError> {
    validate_select_into(stmt)?;
    let prepared = prepare_cq_select(stmt, start_nanos, end_nanos, false);
    let select_sql = translate_inner(
        &prepared,
        source_table.as_str(),
        mapping,
        series,
        Some((Some(start_nanos), Some(end_nanos))),
    )?;
    let select_sql = rename_time_bucket_alias(&select_sql);
    Ok(format!("INSERT INTO {dest_table}\n{select_sql}"))
}
