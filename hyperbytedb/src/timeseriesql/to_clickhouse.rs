use crate::domain::column_mapping::ColumnMapping;
use crate::domain::rollup::{RollupCombine, aggregate_source_field_name, mean_rollup_column_names};
use crate::error::HyperbytedbError;
use crate::timeseriesql::ast::*;
use std::fmt::Write;

/// Extract (min_time_nanos, max_time_nanos) from a WHERE clause, if present.
/// Returns `(Option<i64>, Option<i64>)`.
pub fn extract_time_bounds(condition: Option<&Expr>) -> (Option<i64>, Option<i64>) {
    let mut min_time: Option<i64> = None;
    let mut max_time: Option<i64> = None;

    if let Some(expr) = condition {
        collect_time_bounds(expr, &mut min_time, &mut max_time);
    }
    (min_time, max_time)
}

fn collect_time_bounds(expr: &Expr, min_time: &mut Option<i64>, max_time: &mut Option<i64>) {
    if let Expr::BinaryExpr(be) = expr {
        if matches!(be.op, BinaryOp::And) {
            collect_time_bounds(&be.left, min_time, max_time);
            collect_time_bounds(&be.right, min_time, max_time);
            return;
        }

        if !is_time_epoch_comparison(be) {
            return;
        }

        let (time_is_left, epoch_expr) = if is_time_identifier(&be.left) {
            (true, &be.right)
        } else {
            (false, &be.left)
        };

        let nanos = match epoch_expr {
            Expr::DurationLiteral(d) => d.to_nanos(),
            Expr::IntegerLiteral(n) => *n,
            _ => return,
        };

        // Normalize the operator so it's always `time <op> value`
        let effective_op = if time_is_left {
            &be.op
        } else {
            &match be.op {
                BinaryOp::Gt => BinaryOp::Lt,
                BinaryOp::Gte => BinaryOp::Lte,
                BinaryOp::Lt => BinaryOp::Gt,
                BinaryOp::Lte => BinaryOp::Gte,
                ref other => other.clone(),
            }
        };

        match effective_op {
            BinaryOp::Gte | BinaryOp::Gt | BinaryOp::Eq => {
                *min_time = Some(min_time.map_or(nanos, |cur| cur.min(nanos)));
            }
            _ => {}
        }
        match effective_op {
            BinaryOp::Lte | BinaryOp::Lt | BinaryOp::Eq => {
                *max_time = Some(max_time.map_or(nanos, |cur| cur.max(nanos)));
            }
            _ => {}
        }
    }
}

/// `SELECT ... INTO` requires `GROUP BY time(<interval>)` so results are bucketed
/// before writing to the destination measurement.
pub fn validate_select_into(stmt: &SelectStatement) -> Result<(), HyperbytedbError> {
    if stmt.into.is_none() {
        return Ok(());
    }
    let Some(gb) = stmt.group_by.as_ref() else {
        return Err(HyperbytedbError::QueryParse(
            "SELECT INTO requires GROUP BY time(<interval>)".to_string(),
        ));
    };
    if gb.time_dimension().is_none() {
        return Err(HyperbytedbError::QueryParse(
            "SELECT INTO requires GROUP BY time(<interval>)".to_string(),
        ));
    }
    Ok(())
}

/// The per-measurement series (tag dimension) table to join for tag resolution.
/// In the `series_id` layout the fact table no longer stores tag columns; when a
/// query references a tag we re-attach the tag columns from this table.
#[derive(Debug, Clone, Copy)]
pub struct SeriesJoin<'a> {
    /// Backtick-quoted `<db>_<rp>_<measurement>_series` table name.
    pub table: &'a str,
    /// Force the inline tag-rejoin view even when the query body references no
    /// tag. Set when tombstone predicates (spliced into WHERE post-translation)
    /// reference tag columns that must be present in the FROM source.
    pub force: bool,
    /// Physical column names that actually exist in the series table.
    /// When empty, all tags from the ColumnMapping are projected (backward-compat).
    pub tag_columns: &'a [String],
}

/// Translate against a native MergeTree table (or other pre-formatted FROM
/// source). This is the sole public translate entry for production queries.
///
/// When `series` is provided and the query references any tag, the fact table is
/// wrapped in an inline view that re-attaches the dimension table's tag columns
/// (see [`build_from_source`]); otherwise it is queried directly.
pub fn translate_native_table(
    stmt: &SelectStatement,
    table_source: &str,
    mapping: Option<&ColumnMapping>,
    series: Option<SeriesJoin<'_>>,
    time_bounds: Option<(Option<i64>, Option<i64>)>,
) -> Result<String, HyperbytedbError> {
    translate_inner(stmt, table_source, mapping, series, time_bounds)
}

fn translate_inner(
    stmt: &SelectStatement,
    from_source: &str,
    mapping: Option<&ColumnMapping>,
    series: Option<SeriesJoin<'_>>,
    time_bounds: Option<(Option<i64>, Option<i64>)>,
) -> Result<String, HyperbytedbError> {
    let mut out = String::new();

    // Only `fill(<number>)` coerces NULL aggregates to a numeric default in SQL.
    // `fill(null)` must leave NULL so JSON shows null, not 0.
    let use_ifnull_fill = matches!(stmt.fill, Some(FillOption::Value(_)));
    let needs_with_fill = matches!(
        stmt.fill,
        Some(FillOption::Null)
            | Some(FillOption::Value(_))
            | Some(FillOption::Previous)
            | Some(FillOption::Linear)
    );
    let fill_value = match &stmt.fill {
        Some(FillOption::Value(v)) => *v,
        Some(FillOption::Null) => 0.0,
        _ => 0.0,
    };

    // Collect field alias names for the INTERPOLATE clause. These must match the
    // output column names emitted by `translate_field` exactly — otherwise
    // `fill(previous)`/`fill(linear)` reference a non-existent identifier (e.g.
    // `INTERPOLATE (MEAN)` while the column is `mean_value`) and chDB errors out.
    let field_aliases: Vec<String> = stmt
        .fields
        .iter()
        .filter_map(select_output_field_name)
        .collect();

    // SELECT - prepend the time bucket column when GROUP BY time() is present
    write!(out, "SELECT ")?;
    let mut select_parts: Vec<String> = Vec::new();

    let has_group_by_time = stmt
        .group_by
        .as_ref()
        .and_then(|gb| gb.time_dimension())
        .is_some();

    if let Some(ref gb) = stmt.group_by {
        if let Some(Dimension::Time { interval, offset }) = gb.time_dimension() {
            let time_expr = time_bucket_expr(interval, offset.as_ref());
            // Use __time alias to avoid collision with the raw `time` column,
            // then rename back to `time` in the result parser.
            select_parts.push(format!("{} AS __time", time_expr));
        }

        // Include GROUP BY tag columns in SELECT so they appear in the result
        // and can be used to split rows into separate InfluxDB series.
        for tag in gb.tag_dimensions() {
            select_parts.push(select_tag_column_sql(tag, mapping));
        }
    }

    let has_aggregate = stmt.fields.iter().any(|f| expr_contains_call(&f.expr));
    let has_star = stmt
        .fields
        .iter()
        .any(|f| matches!(f.expr, Expr::Star | Expr::Wildcard));

    // Raw (non-aggregate) selects return one row per point and must carry the
    // point's `time` column, like InfluxDB. `SELECT *` already projects `time`,
    // and GROUP BY time() / aggregate queries get their time column elsewhere.
    let is_raw_select = !has_group_by_time && !has_star && !has_aggregate;
    if is_raw_select {
        select_parts.insert(0, quote_identifier("time"));
    }

    let field_strs: Vec<String> = stmt
        .fields
        .iter()
        .map(|f| {
            translate_field(
                f,
                use_ifnull_fill,
                fill_value,
                stmt.group_by.as_ref(),
                mapping,
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    select_parts.extend(field_strs);
    write!(out, "{}", select_parts.join(", "))?;

    // FROM <source> — wrapped in the tag-rejoin inline view when needed.
    let from = build_from_source(from_source, series, mapping, stmt);
    write!(out, "\nFROM {}", from)?;

    // WHERE
    if let Some(ref cond) = stmt.condition {
        write!(out, "\nWHERE ")?;
        translate_expr(cond, &mut out, true, mapping)?;
    }

    // GROUP BY
    if let Some(ref gb) = stmt.group_by {
        write!(out, "\nGROUP BY ")?;
        let mut gb_parts = Vec::new();

        if let Some(Dimension::Time { interval, offset }) = gb.time_dimension() {
            gb_parts.push(time_bucket_expr(interval, offset.as_ref()));
        }

        for tag in gb.tag_dimensions() {
            // Must match the SELECT expression: physical column name (handles the
            // `__tag__` collision prefix). Previously emitted the logical name,
            // which is wrong for collision-renamed tags.
            gb_parts.push(group_by_tag_sql(tag, mapping));
        }

        write!(out, "{}", gb_parts.join(", "))?;
    }

    // Compute time column expression for ORDER BY
    let time_col = stmt.group_by.as_ref().and_then(|gb| {
        if let Some(Dimension::Time { interval, offset }) = gb.time_dimension() {
            Some(time_bucket_expr(interval, offset.as_ref()))
        } else {
            None
        }
    });

    // InfluxDB orders every result by time ascending by default; an explicit
    // ORDER BY only changes the direction. Order whenever there is a time column
    // to sort on: GROUP BY time() buckets, or raw per-point selects (incl. `*`).
    // Aggregates without GROUP BY time() collapse to one row and need no ordering.
    let has_orderable_time = time_col.is_some() || (!has_aggregate && !has_group_by_time);
    let needs_order_by = has_orderable_time;
    if needs_order_by {
        write!(out, "\nORDER BY ")?;
        let time_desc = stmt.order_by.as_ref().is_some_and(|o| o.time_desc);
        let do_fill = needs_with_fill && time_col.is_some();

        // When filling a tag-grouped query, the tag columns must precede the
        // time-fill column in ORDER BY so ClickHouse fills each tag group
        // independently. Without this, WITH FILL fills globally: gap buckets are
        // emitted with empty tag values (a phantom all-NULL series) and the real
        // per-tag series is never filled — which surfaces as "no data" in Grafana.
        if do_fill && let Some(ref gb) = stmt.group_by {
            for tag in gb.tag_dimensions() {
                write!(out, "{} ASC, ", group_by_tag_sql(tag, mapping))?;
            }
        }

        if let Some(ref tc) = time_col {
            write!(out, "{}", tc)?;
        } else {
            write!(out, "time")?;
        }
        if time_desc {
            write!(out, " DESC")?;
        } else {
            write!(out, " ASC")?;
        }

        if do_fill {
            if let Some(ref gb) = stmt.group_by
                && let Some(Dimension::Time { interval, .. }) = gb.time_dimension()
            {
                let step = interval.to_clickhouse_interval();
                write!(out, " WITH FILL")?;
                if let Some((min_nanos, max_nanos)) = time_bounds
                    && let (Some(min), Some(max)) = (min_nanos, max_nanos)
                {
                    write!(
                        out,
                        " FROM toStartOfInterval({}, {}) TO toStartOfInterval({}, {})",
                        nanos_to_ch_timestamp(min),
                        step,
                        nanos_to_ch_timestamp(max),
                        step,
                    )?;
                }
                write!(out, " STEP {}", step)?;
            }

            // fill(previous): use INTERPOLATE to carry forward last known value
            if matches!(stmt.fill, Some(FillOption::Previous)) && !field_aliases.is_empty() {
                let interp_cols: Vec<String> =
                    field_aliases.iter().map(|a| quote_identifier(a)).collect();
                write!(out, " INTERPOLATE ({})", interp_cols.join(", "))?;
            }

            // fill(linear): use INTERPOLATE with linear expressions
            if matches!(stmt.fill, Some(FillOption::Linear)) && !field_aliases.is_empty() {
                let interp_cols: Vec<String> = field_aliases
                    .iter()
                    .map(|a| {
                        let q = quote_identifier(a);
                        format!("{q} AS {q}")
                    })
                    .collect();
                write!(out, " INTERPOLATE ({})", interp_cols.join(", "))?;
            }
        }
    }

    // LIMIT
    if let Some(limit) = stmt.limit {
        write!(out, "\nLIMIT {}", limit)?;
    }

    // OFFSET
    if let Some(offset) = stmt.offset {
        write!(out, "\nOFFSET {}", offset)?;
    }

    Ok(out)
}

/// Wrap a translated SELECT as `INSERT INTO <dest> SELECT ...`, renaming `__time` to `time`
/// for the destination measurement schema.
pub fn translate_select_into(
    stmt: &SelectStatement,
    dest_table: &str,
    source: &str,
    mapping: Option<&ColumnMapping>,
) -> Result<String, HyperbytedbError> {
    validate_select_into(stmt)?;
    let select_sql = translate_inner(stmt, source, mapping, None, None)?;
    let select_sql = select_sql.replace("__time", "time");
    Ok(format!("INSERT INTO {}\n{}", dest_table, select_sql))
}

fn translate_materialized_view_field(
    field: &Field,
    group_by: Option<&GroupBy>,
    mapping: &ColumnMapping,
) -> Result<String, HyperbytedbError> {
    if let Expr::Call(func) = &field.expr
        && func.name.eq_ignore_ascii_case("mean")
    {
        let source = aggregate_source_field_name(func)?;
        let col = mapping.physical_select_identifier(&source);
        let col_q = quote_identifier(&col);
        let (sum_col, count_col) = mean_rollup_column_names(&source);
        return Ok(format!(
            "sum({col_q}) AS {}, count({col_q}) AS {}",
            quote_identifier(&sum_col),
            quote_identifier(&count_col)
        ));
    }
    translate_field(field, false, 0.0, group_by, Some(mapping))
}

/// Ensure coalesced MV source rows expose every field referenced in the SELECT.
fn mapping_with_mv_aggregate_fields(mapping: &ColumnMapping, fields: &[Field]) -> ColumnMapping {
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
    source_fact: &str,
    source_series: &str,
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
    let time_bucket = time_bucket_expr_on("t.time", interval, offset.as_ref());

    let mut grouped_tags: Vec<String> = gb
        .tag_dimensions()
        .iter()
        .map(|s| (*s).to_string())
        .collect();
    grouped_tags.sort();

    let series_id_expr = crate::domain::series::series_id_ch_sql_for_tags(
        dest_measurement,
        &grouped_tags,
        |tag| quote_identifier(&mapping.tag_column_name(tag)),
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
            let col_q = quote_identifier(&col);
            let (sum_col, count_col) = mean_rollup_column_names(&source);
            let sum_expr = format!("sum({col_q}) AS {}", quote_identifier(&sum_col));
            let count_expr = format!("count({col_q}) AS {}", quote_identifier(&count_col));
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
            quote_identifier(&mapping.tag_column_name(tag))
        ));
    }

    let mut out = String::new();
    write!(out, "SELECT {}", select_parts.join(", "))?;
    let source_mapping = mapping_with_mv_aggregate_fields(mapping, &stmt.fields);
    let coalesced_source = build_coalesced_fact_view_with_row_meta(source_fact, &source_mapping);
    write!(
        out,
        "\nFROM {coalesced_source} AS t ANY INNER JOIN {source_series} AS s ON t.`series_id` = s.`series_id`"
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
    source_series: &str,
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
                crate::domain::column_mapping::tag_column_name(tag, &fields)
            }
            None => mapping.tag_column_name(tag),
        }
    };

    let series_id_expr = crate::domain::series::series_id_ch_sql_for_tags(
        dest_measurement,
        &grouped_tags,
        |tag| quote_identifier(&tag_phys_name(tag)),
        "s",
    );

    let tag_cols: Vec<String> = grouped_tags
        .iter()
        .map(|tag| format!("s.{}", quote_identifier(&tag_phys_name(tag))))
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
    dest_table: &str,
    source_fact: &str,
    source_series: &str,
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
fn materialized_view_dest_insert_columns(
    stmt: &SelectStatement,
) -> Result<String, HyperbytedbError> {
    let mut cols = vec![
        quote_identifier("time"),
        quote_identifier("origin_node_id"),
        quote_identifier("ingest_seq"),
        quote_identifier("series_id"),
    ];
    let mut field_names = materialized_view_dest_field_names(stmt)?;
    field_names.sort();
    cols.extend(field_names.into_iter().map(|n| quote_identifier(&n)));
    Ok(cols.join(", "))
}

/// Output column names for MV destination fields (expands `mean()` to sum/count pairs).
fn materialized_view_dest_field_names(
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
    mv_name: &str,
    dest_table: &str,
    select_sql: &str,
) -> String {
    format!("CREATE MATERIALIZED VIEW {mv_name} TO {dest_table} AS\n{select_sql}")
}

/// Full `CREATE MATERIALIZED VIEW ... TO ... AS SELECT ...` for the series MV.
pub fn build_create_series_materialized_view(
    mv_name: &str,
    dest_series: &str,
    select_sql: &str,
) -> String {
    format!("CREATE MATERIALIZED VIEW {mv_name} TO {dest_series} AS\n{select_sql}")
}

/// Like [`translate_select_into`], targeting a native MergeTree table source.
/// `series` lets a tag-grouped continuous query resolve tags from the source
/// measurement's dimension table.
pub fn translate_select_into_native(
    stmt: &SelectStatement,
    dest_table: &str,
    source_table: &str,
    mapping: Option<&ColumnMapping>,
    series: Option<SeriesJoin<'_>>,
) -> Result<String, HyperbytedbError> {
    validate_select_into(stmt)?;
    let select_sql = translate_inner(stmt, source_table, mapping, series, None)?;
    let select_sql = select_sql.replace("__time", "time");
    Ok(format!("INSERT INTO {}\n{}", dest_table, select_sql))
}

/// Like translate, but uses a custom source expression instead of file() - used for subqueries.
pub fn translate_with_source(
    stmt: &SelectStatement,
    source: &str,
) -> Result<String, HyperbytedbError> {
    translate_inner(stmt, source, None, None, None)
}

/// Whether `expr` references a tag (so the query needs the series join). Treats a
/// name present in `tag_keys` as a tag even if it also collides with a field name
/// — over-inclusive is safe (the field still resolves via `t.*`).
fn expr_references_tag(expr: &Expr, m: &ColumnMapping) -> bool {
    match expr {
        Expr::Identifier(name) => m.tag_keys.contains(name),
        Expr::FieldRef { name, typ } => {
            matches!(typ, Some(FieldType::Tag)) || m.tag_keys.contains(name)
        }
        Expr::BinaryExpr(be) => {
            expr_references_tag(&be.left, m) || expr_references_tag(&be.right, m)
        }
        Expr::UnaryExpr(_, e) => expr_references_tag(e, m),
        Expr::Call(fc) => fc.args.iter().any(|a| expr_references_tag(a, m)),
        _ => false,
    }
}

/// Whether the query references any tag (in SELECT, WHERE, or GROUP BY) — or uses
/// `SELECT *`, which in InfluxDB includes tags. Determines whether the series
/// dimension table must be joined.
fn query_references_tag(stmt: &SelectStatement, m: &ColumnMapping) -> bool {
    if stmt
        .group_by
        .as_ref()
        .is_some_and(|gb| gb.references_tags())
    {
        return true;
    }
    if stmt
        .fields
        .iter()
        .any(|f| matches!(f.expr, Expr::Star | Expr::Wildcard) || expr_references_tag(&f.expr, m))
    {
        return true;
    }
    stmt.condition
        .as_ref()
        .is_some_and(|c| expr_references_tag(c, m))
}

/// Build a query-time view that collapses duplicate `(series_id, time)` rows
/// to the single row with the highest `ingest_seq`.
///
/// Partial Telegraf lines are merged at ingest (`coalesce_points_and_origins`);
/// at query time we must not merge fields independently across rows — per-field
/// `argMaxIf` can stitch a correct `available` from one row with a corrupt
/// `used_percent` from another (e.g. async replication writing a second row
/// for the same instant), which produces nonsense Grafana percentages.
pub fn build_coalesced_fact_view(fact_table: &str, mapping: &ColumnMapping) -> String {
    build_coalesced_fact_view_impl(fact_table, mapping, false)
}

/// Like [`build_coalesced_fact_view`], but preserves `ingest_seq` / `origin_node_id` for
/// downstream aggregates (materialized view source dedup).
pub fn build_coalesced_fact_view_with_row_meta(
    fact_table: &str,
    mapping: &ColumnMapping,
) -> String {
    build_coalesced_fact_view_impl(fact_table, mapping, true)
}

fn build_coalesced_fact_view_impl(
    fact_table: &str,
    mapping: &ColumnMapping,
    include_row_metadata: bool,
) -> String {
    let mut field_cols: Vec<&String> = mapping.field_names.iter().collect();
    field_cols.sort();
    let field_aggs: Vec<String> = field_cols
        .iter()
        .map(|f| {
            let q = quote_identifier(f);
            let agg = match mapping.field_rollups.get(*f) {
                Some(RollupCombine::Sum) => format!("sum({q})"),
                Some(RollupCombine::Min) => format!("min({q})"),
                Some(RollupCombine::Max) => format!("max({q})"),
                Some(RollupCombine::First) => format!("argMin({q}, `time`)"),
                Some(RollupCombine::Last) | None => format!("argMax({q}, `ingest_seq`)"),
            };
            format!("{agg} AS {q}")
        })
        .collect();
    let select_fields = if field_aggs.is_empty() {
        String::new()
    } else {
        format!(", {}", field_aggs.join(", "))
    };
    let row_meta = if include_row_metadata {
        ", max(`ingest_seq`) AS `_mv_src_ingest_seq`, any(`origin_node_id`) AS `_mv_src_origin_node_id`"
    } else {
        ""
    };
    format!(
        "(SELECT `series_id`, `time`{row_meta}{select_fields} FROM {fact_table} GROUP BY `series_id`, `time`)"
    )
}

/// Build the FROM source. When `mapping` is present the fact table is wrapped in
/// a coalesced view so partial-field rows merge before aggregation. When `series`
/// is set and the query references a tag, the coalesced fact table is wrapped in
/// an inline view that re-attaches the tag columns from the dimension table.
/// `ANY LEFT JOIN` takes at most one matching dimension row (so pre-merge duplicate
/// `ReplacingMergeTree` series rows can't fan out fact rows) and preserves fact
/// rows whose series row is briefly missing. Tag columns are exposed under their
/// physical names, so the rest of the translator — which already references tags
/// by physical name — is unchanged.
fn build_from_source(
    fact_table: &str,
    series: Option<SeriesJoin<'_>>,
    mapping: Option<&ColumnMapping>,
    stmt: &SelectStatement,
) -> String {
    let fact = match mapping {
        Some(m) => build_coalesced_fact_view(fact_table, m),
        None => fact_table.to_string(),
    };
    let (Some(sj), Some(m)) = (series, mapping) else {
        return fact;
    };
    if !sj.force && !query_references_tag(stmt, m) {
        return fact;
    }
    let mut tag_cols: Vec<String> = m.tag_keys.iter().map(|t| m.tag_column_name(t)).collect();
    if tag_cols.is_empty() {
        return fact;
    }
    // Only project tag columns that actually exist in the series table.
    // MV destinations may have a subset of source tags (GROUP BY columns only).
    if !sj.tag_columns.is_empty() {
        tag_cols.retain(|c| sj.tag_columns.contains(c));
    }
    if tag_cols.is_empty() {
        return fact;
    }
    tag_cols.sort();
    let projected = tag_cols
        .iter()
        .map(|c| format!("s.{}", quote_identifier(c)))
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "(SELECT t.*, {projected} FROM {fact} AS t ANY LEFT JOIN {series} AS s ON t.`series_id` = s.`series_id`)",
        series = sj.table,
    )
}

/// GROUP BY expression for a tag: the physical column name, matching the SELECT
/// side. Without a mapping, falls back to the logical name (unchanged behaviour).
fn group_by_tag_sql(tag: &str, mapping: Option<&ColumnMapping>) -> String {
    match mapping {
        Some(m) => quote_identifier(&m.tag_column_name(tag)),
        None => quote_identifier(tag),
    }
}

fn time_bucket_expr(interval: &Duration, offset: Option<&Duration>) -> String {
    time_bucket_expr_on("time", interval, offset)
}

fn time_bucket_expr_on(time_col: &str, interval: &Duration, offset: Option<&Duration>) -> String {
    let interval_str = interval.to_clickhouse_interval();
    if let Some(off) = offset {
        let off_str = off.to_clickhouse_interval();
        format!(
            "toStartOfInterval({time_col} - {}, {}) + {}",
            off_str, interval_str, off_str
        )
    } else {
        format!("toStartOfInterval({time_col}, {})", interval_str)
    }
}

fn select_tag_column_sql(tag: &str, mapping: Option<&ColumnMapping>) -> String {
    let Some(m) = mapping else {
        return quote_identifier(tag);
    };
    let phys = m.tag_column_name(tag);
    if phys == tag {
        quote_identifier(tag)
    } else {
        format!("{} AS {}", quote_identifier(&phys), quote_identifier(tag))
    }
}

fn translate_field(
    field: &Field,
    use_fill: bool,
    fill_value: f64,
    group_by: Option<&GroupBy>,
    mapping: Option<&ColumnMapping>,
) -> Result<String, HyperbytedbError> {
    let sql = translate_field_expr(&field.expr, use_fill, fill_value, group_by, mapping)?;
    let alias = field
        .alias
        .clone()
        .or_else(|| default_field_alias(&field.expr));
    Ok(match alias {
        Some(a) => format!("{} AS {}", sql, quote_identifier(&a)),
        None => sql,
    })
}

/// Output column name for a SELECT field (explicit alias or Influx-style default).
#[must_use]
pub fn select_output_field_name(field: &Field) -> Option<String> {
    field
        .alias
        .clone()
        .or_else(|| default_field_alias(&field.expr))
}

/// Generate a default column alias matching InfluxDB conventions.
/// Single-arg aggregates include the field name for uniqueness:
/// `mean("usage_idle")` → `"mean_usage_idle"`, `count("x")` → `"count_x"`.
/// No-arg calls use just the function name: `count()` → `"count"`.
/// Non-call expressions get no alias.
fn default_field_alias(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Call(func) => {
            let base = func.name.to_lowercase();
            if let Some(Expr::Identifier(field_name)) = func.args.first() {
                Some(format!("{}_{}", base, field_name))
            } else {
                Some(base)
            }
        }
        _ => None,
    }
}

/// Whether an expression tree contains a function call (aggregate, selector, or
/// transform). Used to distinguish raw per-point selects from aggregate queries.
fn expr_contains_call(expr: &Expr) -> bool {
    match expr {
        Expr::Call(_) => true,
        Expr::BinaryExpr(be) => expr_contains_call(&be.left) || expr_contains_call(&be.right),
        Expr::UnaryExpr(_, e) => expr_contains_call(e),
        _ => false,
    }
}

fn translate_field_expr(
    expr: &Expr,
    use_fill: bool,
    fill_value: f64,
    group_by: Option<&GroupBy>,
    mapping: Option<&ColumnMapping>,
) -> Result<String, HyperbytedbError> {
    match expr {
        Expr::Star => Ok("*".to_string()),
        Expr::Identifier(name) => {
            let col = mapping
                .map(|m| m.physical_select_identifier(name))
                .unwrap_or_else(|| name.clone());
            Ok(quote_identifier(&col))
        }
        Expr::FieldRef { name, .. } => {
            let col = mapping
                .map(|m| m.physical_select_identifier(name))
                .unwrap_or_else(|| name.clone());
            Ok(quote_identifier(&col))
        }
        Expr::Call(func) => translate_aggregate_call(func, use_fill, fill_value, group_by, mapping),
        Expr::BinaryExpr(be) => translate_binary_expr(be, use_fill, fill_value, group_by, mapping),
        Expr::UnaryExpr(op, e) => {
            let inner = translate_field_expr(e, use_fill, fill_value, group_by, mapping)?;
            Ok(match op {
                UnaryOp::Neg => format!("(-{})", inner),
                UnaryOp::Not => format!("(NOT {})", inner),
            })
        }
        Expr::StringLiteral(s) => Ok(quote_string(s)),
        Expr::IntegerLiteral(n) => Ok(n.to_string()),
        Expr::FloatLiteral(f) => Ok(f.to_string()),
        Expr::BooleanLiteral(b) => Ok(if *b { "true" } else { "false" }.to_string()),
        Expr::DurationLiteral(d) => Ok(d.to_clickhouse_interval()),
        Expr::TimeLiteral(s) => Ok(quote_string(s)),
        Expr::Regex(r) => Ok(format!(
            "'{}'",
            r.replace('\\', "\\\\").replace('\'', "\\'")
        )),
        Expr::Wildcard => Ok("*".to_string()),
        Expr::Now => Ok("now64()".to_string()),
    }
}

fn translate_binary_expr(
    be: &BinaryExpr,
    use_fill: bool,
    fill_value: f64,
    group_by: Option<&GroupBy>,
    mapping: Option<&ColumnMapping>,
) -> Result<String, HyperbytedbError> {
    let left = translate_field_expr(&be.left, use_fill, fill_value, group_by, mapping)?;
    let right = translate_field_expr(&be.right, use_fill, fill_value, group_by, mapping)?;
    Ok(format!(
        "({} {} {})",
        left,
        binary_op_to_clickhouse(&be.op),
        right
    ))
}

fn translate_aggregate_call(
    func: &FunctionCall,
    use_fill: bool,
    fill_value: f64,
    group_by: Option<&GroupBy>,
    mapping: Option<&ColumnMapping>,
) -> Result<String, HyperbytedbError> {
    let name_upper = func.name.to_uppercase();
    let wrap_fill = |s: String| -> String {
        if use_fill && group_by.is_some() {
            format!("ifNull({}, {})", s, format_float(fill_value))
        } else {
            s
        }
    };

    let result = match name_upper.as_str() {
        "MEAN" => {
            let arg = get_single_arg(func, "MEAN")?;
            if let Some(m) = mapping
                && let Expr::Identifier(name) | Expr::FieldRef { name, .. } = arg
                && let Some(mean_def) = m.mean_fields.get(name)
            {
                let sum_q = quote_identifier(&mean_def.sum_col);
                let count_q = quote_identifier(&mean_def.count_col);
                return Ok(wrap_fill(format!(
                    "(sum({sum_q}) / nullIf(sum({count_q}), 0))"
                )));
            }
            let f = translate_aggregate_arg(arg, mapping)?;
            wrap_fill(format!("avg({})", f))
        }
        "MEDIAN" => {
            let arg = get_single_arg(func, "MEDIAN")?;
            let f = translate_aggregate_arg(arg, mapping)?;
            wrap_fill(format!("median({})", f))
        }
        "COUNT" => {
            let arg = get_single_arg(func, "COUNT")?;
            let f = translate_aggregate_arg(arg, mapping)?;
            wrap_fill(format!("count({})", f))
        }
        "SUM" => {
            let arg = get_single_arg(func, "SUM")?;
            let f = translate_aggregate_arg(arg, mapping)?;
            wrap_fill(format!("sum({})", f))
        }
        "MIN" => {
            let arg = get_single_arg(func, "MIN")?;
            let f = translate_aggregate_arg(arg, mapping)?;
            wrap_fill(format!("min({})", f))
        }
        "MAX" => {
            let arg = get_single_arg(func, "MAX")?;
            let f = translate_aggregate_arg(arg, mapping)?;
            wrap_fill(format!("max({})", f))
        }
        "FIRST" => {
            let arg = get_single_arg(func, "FIRST")?;
            let f = translate_aggregate_arg(arg, mapping)?;
            wrap_fill(format!("argMin({}, time)", f))
        }
        "LAST" => {
            let arg = get_single_arg(func, "LAST")?;
            let f = translate_aggregate_arg(arg, mapping)?;
            wrap_fill(format!("argMax({}, time)", f))
        }
        "PERCENTILE" => {
            let (field_arg, pct_arg) = get_two_args(func, "PERCENTILE")?;
            let f = translate_aggregate_arg(field_arg, mapping)?;
            let pct = match &pct_arg {
                Expr::IntegerLiteral(n) => (*n as f64) / 100.0,
                Expr::FloatLiteral(f) => *f / 100.0,
                _ => {
                    return Err(HyperbytedbError::QueryParse(format!(
                        "PERCENTILE second argument must be numeric, got {:?}",
                        pct_arg
                    )));
                }
            };
            wrap_fill(format!("quantile({})({})", format_float(pct), f))
        }
        "SPREAD" => {
            let arg = get_single_arg(func, "SPREAD")?;
            let f = translate_aggregate_arg(arg, mapping)?;
            wrap_fill(format!("(max({}) - min({}))", f, f))
        }
        "STDDEV" => {
            let arg = get_single_arg(func, "STDDEV")?;
            let f = translate_aggregate_arg(arg, mapping)?;
            wrap_fill(format!("stddevPop({})", f))
        }
        "MODE" => {
            let arg = get_single_arg(func, "MODE")?;
            let f = translate_aggregate_arg(arg, mapping)?;
            wrap_fill(format!("topKWeighted(1)({}, 1)", f))
        }
        "DISTINCT" => {
            let arg = get_single_arg(func, "DISTINCT")?;
            let f = translate_aggregate_arg(arg, mapping)?;
            format!("DISTINCT {}", f)
        }
        "DERIVATIVE" | "NON_NEGATIVE_DERIVATIVE" => {
            let field_arg = get_single_arg(func, &name_upper)?;
            let f = translate_field_or_nested(field_arg, group_by, mapping)?;
            let window = build_window_clause(group_by, mapping);
            let unit_nanos: i64 = if func.args.len() >= 2 {
                match &func.args[1] {
                    Expr::DurationLiteral(d) => d.to_nanos(),
                    _ => 1_000_000_000,
                }
            } else {
                1_000_000_000
            };
            let unit_seconds = format_float(unit_nanos as f64 / 1_000_000_000.0);
            let delta_value = format!("({f} - lagInFrame({f}, 1) {window})");
            // Use toFloat64() to get Unix timestamps as seconds (Float64)
            // for correct arithmetic regardless of DateTime/DateTime64 type.
            let time_ref = window_time_ref(group_by);
            let delta_time =
                format!("(toFloat64({time_ref}) - toFloat64(lagInFrame({time_ref}, 1) {window}))");
            let deriv = format!("{delta_value} / ({delta_time} / {unit_seconds})");
            if name_upper == "NON_NEGATIVE_DERIVATIVE" {
                format!("if(({deriv}) >= 0, ({deriv}), NULL)")
            } else {
                deriv
            }
        }
        "DIFFERENCE" | "NON_NEGATIVE_DIFFERENCE" => {
            let arg = get_single_arg(func, &name_upper)?;
            let f = translate_field_or_nested(arg, group_by, mapping)?;
            let window = build_window_clause(group_by, mapping);
            let diff = format!("({f} - lagInFrame({f}, 1) {window})");
            if name_upper == "NON_NEGATIVE_DIFFERENCE" {
                format!("if({diff} >= 0, {diff}, NULL)")
            } else {
                diff
            }
        }
        "MOVING_AVERAGE" => {
            let (field_arg, n_arg) = get_two_args(func, "MOVING_AVERAGE")?;
            let f = translate_field_or_nested(field_arg, group_by, mapping)?;
            let time_ref = window_time_ref(group_by);
            let n = match &n_arg {
                Expr::IntegerLiteral(n) => *n,
                _ => {
                    return Err(HyperbytedbError::QueryParse(
                        "MOVING_AVERAGE second argument must be integer".to_string(),
                    ));
                }
            };
            let partition_tags: Vec<&str> =
                group_by.map(|gb| gb.tag_dimensions()).unwrap_or_default();
            let partition_clause = if partition_tags.is_empty() {
                String::new()
            } else {
                let p = partition_tags
                    .iter()
                    .map(|t| {
                        let phys = mapping
                            .map(|m| m.tag_column_name(t))
                            .unwrap_or_else(|| t.to_string());
                        quote_identifier(&phys)
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("PARTITION BY {p} ")
            };
            format!(
                "avg({f}) OVER ({partition_clause}ORDER BY {time_ref} ROWS BETWEEN {preceding} PRECEDING AND CURRENT ROW)",
                preceding = n - 1
            )
        }
        "CUMULATIVE_SUM" => {
            let arg = get_single_arg(func, "CUMULATIVE_SUM")?;
            let f = translate_field_or_nested(arg, group_by, mapping)?;
            let time_ref = window_time_ref(group_by);
            let partition_tags: Vec<&str> =
                group_by.map(|gb| gb.tag_dimensions()).unwrap_or_default();
            let partition_clause = if partition_tags.is_empty() {
                String::new()
            } else {
                let p = partition_tags
                    .iter()
                    .map(|t| {
                        let phys = mapping
                            .map(|m| m.tag_column_name(t))
                            .unwrap_or_else(|| t.to_string());
                        quote_identifier(&phys)
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("PARTITION BY {p} ")
            };
            format!(
                "sum({f}) OVER ({partition_clause}ORDER BY {time_ref} ROWS UNBOUNDED PRECEDING)"
            )
        }
        "ELAPSED" => {
            let _field_arg = get_single_arg(func, "ELAPSED")?;
            let time_ref = window_time_ref(group_by);
            let window = build_window_clause(group_by, mapping);
            let unit_nanos: i64 = if func.args.len() >= 2 {
                match &func.args[1] {
                    Expr::DurationLiteral(d) => d.to_nanos(),
                    _ => 1_000_000_000,
                }
            } else {
                1_000_000_000
            };
            let unit_seconds = format_float(unit_nanos as f64 / 1_000_000_000.0);
            format!(
                "((toFloat64({time_ref}) - toFloat64(lagInFrame({time_ref}, 1) {window})) / {unit_seconds})"
            )
        }
        _ => {
            return Err(HyperbytedbError::QueryParse(format!(
                "unsupported aggregate function: {}",
                func.name
            )));
        }
    };

    Ok(result)
}

fn translate_aggregate_arg(
    expr: &Expr,
    mapping: Option<&ColumnMapping>,
) -> Result<String, HyperbytedbError> {
    match expr {
        Expr::Identifier(name) | Expr::FieldRef { name, .. } => {
            let col = mapping
                .map(|m| m.physical_select_identifier(name))
                .unwrap_or_else(|| name.clone());
            Ok(quote_identifier(&col))
        }
        Expr::Star => Ok("*".to_string()),
        _ => Err(HyperbytedbError::QueryParse(format!(
            "aggregate argument must be identifier or *, got {:?}",
            expr
        ))),
    }
}

/// Translate the first argument of a transform function (derivative, difference, etc.).
/// Accepts either a plain identifier or a nested aggregate like mean("reads").
fn translate_field_or_nested(
    expr: &Expr,
    group_by: Option<&GroupBy>,
    mapping: Option<&ColumnMapping>,
) -> Result<String, HyperbytedbError> {
    match expr {
        Expr::Call(inner_func) => {
            translate_aggregate_call(inner_func, false, 0.0, group_by, mapping)
        }
        _ => translate_aggregate_arg(expr, mapping),
    }
}

/// Return the time column reference for window function ORDER BY clauses.
/// Uses `__time` (the time bucket alias) when GROUP BY time() is present,
/// raw `time` otherwise.
fn window_time_ref(group_by: Option<&GroupBy>) -> &'static str {
    if group_by.and_then(|gb| gb.time_dimension()).is_some() {
        "__time"
    } else {
        "time"
    }
}

/// Build the OVER (...) window clause for transform functions.
/// Includes PARTITION BY for GROUP BY tag dimensions so that window
/// functions (lagInFrame, etc.) operate within each series independently.
fn build_window_clause(group_by: Option<&GroupBy>, mapping: Option<&ColumnMapping>) -> String {
    let time_ref = window_time_ref(group_by);
    let partition_tags: Vec<&str> = group_by.map(|gb| gb.tag_dimensions()).unwrap_or_default();

    if partition_tags.is_empty() {
        format!("OVER (ORDER BY {time_ref})")
    } else {
        let partition = partition_tags
            .iter()
            .map(|t| {
                let phys = mapping
                    .map(|m| m.tag_column_name(t))
                    .unwrap_or_else(|| t.to_string());
                quote_identifier(&phys)
            })
            .collect::<Vec<_>>()
            .join(", ");
        format!("OVER (PARTITION BY {partition} ORDER BY {time_ref})")
    }
}

fn get_single_arg<'a>(func: &'a FunctionCall, name: &str) -> Result<&'a Expr, HyperbytedbError> {
    func.args.first().ok_or_else(|| {
        HyperbytedbError::QueryParse(format!("{} requires exactly one argument", name))
    })
}

fn get_two_args<'a>(
    func: &'a FunctionCall,
    name: &str,
) -> Result<(&'a Expr, &'a Expr), HyperbytedbError> {
    if func.args.len() < 2 {
        return Err(HyperbytedbError::QueryParse(format!(
            "{} requires exactly two arguments",
            name
        )));
    }
    Ok((&func.args[0], &func.args[1]))
}

/// Translate a WHERE condition expression to ClickHouse SQL.
/// Used by the DELETE statement handler to serialize tombstone predicates.
pub fn translate_condition(expr: &Expr, out: &mut String) -> Result<(), HyperbytedbError> {
    translate_expr(expr, out, true, None)
}

/// Like [`translate_condition`] but resolves tag identifiers to their physical
/// column names. Used to store tombstone predicates so the spliced WHERE clause
/// matches the tag columns exposed by the series-rejoin inline view.
pub fn translate_condition_with_mapping(
    expr: &Expr,
    mapping: Option<&ColumnMapping>,
    out: &mut String,
) -> Result<(), HyperbytedbError> {
    translate_expr(expr, out, true, mapping)
}

fn tag_field_collision(m: &ColumnMapping, name: &str) -> bool {
    m.tag_keys.contains(name) && m.field_names.contains(name)
}

fn is_where_literal(e: &Expr) -> bool {
    matches!(
        e,
        Expr::IntegerLiteral(_)
            | Expr::FloatLiteral(_)
            | Expr::StringLiteral(_)
            | Expr::BooleanLiteral(_)
    )
}

fn where_identifier_physical_name(m: &ColumnMapping, name: &str, other: &Expr) -> String {
    if !tag_field_collision(m, name) {
        return quote_identifier(name);
    }
    match other {
        Expr::IntegerLiteral(_) | Expr::FloatLiteral(_) | Expr::BooleanLiteral(_) => {
            quote_identifier(name)
        }
        Expr::StringLiteral(_) | Expr::Regex(_) => quote_identifier(&m.tag_column_name(name)),
        _ => quote_identifier(&m.tag_column_name(name)),
    }
}

fn regex_match_column_name(
    left: &Expr,
    mapping: Option<&ColumnMapping>,
) -> Result<String, HyperbytedbError> {
    match left {
        Expr::FieldRef {
            name,
            typ: Some(FieldType::Tag),
        } => {
            let col = mapping
                .map(|m| m.tag_column_name(name))
                .unwrap_or_else(|| name.clone());
            Ok(quote_identifier(&col))
        }
        Expr::FieldRef {
            name,
            typ: Some(FieldType::Field),
        } => Ok(quote_identifier(name)),
        Expr::FieldRef { name, typ: None } => {
            let col = mapping
                .map(|m| m.tag_column_name(name))
                .unwrap_or_else(|| name.clone());
            Ok(quote_identifier(&col))
        }
        Expr::Identifier(n) => {
            let col = if let Some(m) = mapping {
                if tag_field_collision(m, n) {
                    m.tag_column_name(n)
                } else {
                    m.physical_select_identifier(n)
                }
            } else {
                n.clone()
            };
            Ok(quote_identifier(&col))
        }
        _ => Err(HyperbytedbError::QueryParse(
            "regex operator =~ / !~ requires identifier and regex".to_string(),
        )),
    }
}

fn try_translate_where_binary_expr(
    be: &BinaryExpr,
    out: &mut String,
    m: &ColumnMapping,
) -> Result<bool, HyperbytedbError> {
    let (name, lit, id_on_left) = match (&be.left, &be.right) {
        (Expr::Identifier(n), rhs) if is_where_literal(rhs) => (n.as_str(), rhs, true),
        (Expr::FieldRef { name, typ: None }, rhs) if is_where_literal(rhs) => {
            (name.as_str(), rhs, true)
        }
        (lhs, Expr::Identifier(n)) if is_where_literal(lhs) => (n.as_str(), lhs, false),
        (lhs, Expr::FieldRef { name, typ: None }) if is_where_literal(lhs) => {
            (name.as_str(), lhs, false)
        }
        _ => return Ok(false),
    };
    if matches!(be.op, BinaryOp::And | BinaryOp::Or) {
        return Ok(false);
    }
    if !tag_field_collision(m, name) {
        return Ok(false);
    }
    let col = where_identifier_physical_name(m, name, lit);
    if id_on_left {
        write!(out, "{}", col)?;
        write!(out, " {} ", binary_op_to_clickhouse(&be.op))?;
        translate_expr(lit, out, true, Some(m))?;
    } else {
        translate_expr(lit, out, true, Some(m))?;
        write!(out, " {} ", binary_op_to_clickhouse(&be.op))?;
        write!(out, "{}", col)?;
    }
    Ok(true)
}

fn translate_expr(
    expr: &Expr,
    out: &mut String,
    in_where: bool,
    mapping: Option<&ColumnMapping>,
) -> Result<(), HyperbytedbError> {
    match expr {
        Expr::Identifier(name) => {
            if in_where && name.to_lowercase() == "time" {
                write!(out, "time")?;
            } else if in_where {
                if let Some(m) = mapping {
                    if tag_field_collision(m, name) {
                        write!(out, "{}", quote_identifier(&m.tag_column_name(name)))?;
                    } else {
                        write!(out, "{}", quote_identifier(name))?;
                    }
                } else {
                    write!(out, "{}", quote_identifier(name))?;
                }
            } else {
                write!(out, "{}", quote_identifier(name))?;
            }
        }
        Expr::FieldRef { name, typ } => {
            let s = match typ {
                Some(FieldType::Tag) => {
                    if let Some(m) = mapping {
                        quote_identifier(&m.tag_column_name(name))
                    } else {
                        quote_identifier(name)
                    }
                }
                Some(FieldType::Field) => quote_identifier(name),
                None => {
                    if let Some(m) = mapping {
                        if tag_field_collision(m, name) {
                            quote_identifier(&m.tag_column_name(name))
                        } else {
                            quote_identifier(name)
                        }
                    } else {
                        quote_identifier(name)
                    }
                }
            };
            write!(out, "{}", s)?;
        }
        Expr::Now => write!(out, "now64()")?,
        Expr::DurationLiteral(d) => write!(out, "{}", d.to_clickhouse_interval())?,
        Expr::BinaryExpr(be) => {
            write!(out, "(")?;
            if matches!(be.op, BinaryOp::RegexMatch | BinaryOp::RegexNotMatch) {
                let pattern = match (&be.left, &be.right) {
                    (_, Expr::Regex(p)) => p.clone(),
                    _ => {
                        return Err(HyperbytedbError::QueryParse(
                            "regex operator =~ / !~ requires identifier and regex".to_string(),
                        ));
                    }
                };
                let col = regex_match_column_name(&be.left, mapping)?;
                let escaped = pattern.replace('\\', "\\\\").replace('\'', "\\'");
                if be.op == BinaryOp::RegexMatch {
                    write!(out, "match({}, '{}')", col, escaped)?;
                } else {
                    write!(out, "NOT match({}, '{}')", col, escaped)?;
                }
            } else {
                let is_logical = matches!(be.op, BinaryOp::And | BinaryOp::Or);
                if is_logical {
                    translate_expr(&be.left, out, in_where, mapping)?;
                    let op_str = match be.op {
                        BinaryOp::And => "AND",
                        BinaryOp::Or => "OR",
                        _ => {
                            return Err(HyperbytedbError::QueryParse(
                                "internal: expected AND/OR in logical binary expression"
                                    .to_string(),
                            ));
                        }
                    };
                    write!(out, " {} ", op_str)?;
                    translate_expr(&be.right, out, in_where, mapping)?;
                } else if in_where && is_time_epoch_comparison(be) {
                    translate_time_epoch_comparison(be, out)?;
                } else {
                    let handled = if let Some(m) = mapping {
                        if in_where {
                            try_translate_where_binary_expr(be, out, m)?
                        } else {
                            false
                        }
                    } else {
                        false
                    };
                    if !handled {
                        translate_expr(&be.left, out, in_where, mapping)?;
                        write!(out, " {} ", binary_op_to_clickhouse(&be.op))?;
                        translate_expr(&be.right, out, in_where, mapping)?;
                    }
                }
            }
            write!(out, ")")?;
        }
        Expr::StringLiteral(s) => write!(out, "{}", quote_string(s))?,
        Expr::IntegerLiteral(n) => write!(out, "{}", n)?,
        Expr::FloatLiteral(f) => write!(out, "{}", format_float(*f))?,
        Expr::BooleanLiteral(b) => write!(out, "{}", if *b { "true" } else { "false" })?,
        Expr::TimeLiteral(s) => write!(out, "{}", quote_string(s))?,
        Expr::Regex(r) => write!(out, "'{}'", r.replace('\\', "\\\\").replace('\'', "\\'"))?,
        Expr::UnaryExpr(UnaryOp::Not, e) => {
            write!(out, "NOT ")?;
            translate_expr(e, out, in_where, mapping)?;
        }
        Expr::UnaryExpr(UnaryOp::Neg, e) => {
            write!(out, "-")?;
            translate_expr(e, out, in_where, mapping)?;
        }
        _ => {
            return Err(HyperbytedbError::QueryParse(format!(
                "unsupported expression in WHERE: {:?}",
                expr
            )));
        }
    }
    Ok(())
}

fn is_time_identifier(expr: &Expr) -> bool {
    matches!(expr, Expr::Identifier(n) if n.to_lowercase() == "time")
}

/// Detect `time <cmp> <epoch_value>` where epoch_value is a DurationLiteral
/// (e.g., `1772462462777ms`) or a bare IntegerLiteral (nanosecond epoch).
fn is_time_epoch_comparison(be: &BinaryExpr) -> bool {
    if !matches!(
        be.op,
        BinaryOp::Eq | BinaryOp::Neq | BinaryOp::Lt | BinaryOp::Lte | BinaryOp::Gt | BinaryOp::Gte
    ) {
        return false;
    }

    let (is_left_time, rhs) = if is_time_identifier(&be.left) {
        (true, &be.right)
    } else if is_time_identifier(&be.right) {
        (true, &be.left)
    } else {
        (false, &be.right)
    };

    if !is_left_time {
        return false;
    }

    matches!(rhs, Expr::DurationLiteral(_) | Expr::IntegerLiteral(_))
}

/// Translate `time >= 1772462462777ms` → `time >= fromUnixTimestamp64Milli(1772462462777)`
fn translate_time_epoch_comparison(
    be: &BinaryExpr,
    out: &mut String,
) -> Result<(), HyperbytedbError> {
    let (time_side_is_left, epoch_expr) = if is_time_identifier(&be.left) {
        (true, &be.right)
    } else {
        (false, &be.left)
    };

    let ts_sql = match epoch_expr {
        Expr::DurationLiteral(d) => epoch_duration_to_timestamp(d),
        Expr::IntegerLiteral(n) => format!("fromUnixTimestamp64Nano({})", n),
        _ => {
            return Err(HyperbytedbError::QueryParse(
                "expected duration or integer epoch beside time in comparison".to_string(),
            ));
        }
    };

    if time_side_is_left {
        write!(out, "time {} {}", binary_op_to_clickhouse(&be.op), ts_sql)?;
    } else {
        write!(out, "{} {} time", ts_sql, binary_op_to_clickhouse(&be.op))?;
    }
    Ok(())
}

fn epoch_duration_to_timestamp(d: &Duration) -> String {
    match d.unit {
        DurationUnit::Second => format!("fromUnixTimestamp({})", d.value),
        DurationUnit::Millisecond => format!("fromUnixTimestamp64Milli({})", d.value),
        DurationUnit::Microsecond => format!("fromUnixTimestamp64Micro({})", d.value),
        DurationUnit::Nanosecond => format!("fromUnixTimestamp64Nano({})", d.value),
        _ => {
            let nanos = d.to_nanos();
            nanos_to_ch_timestamp(nanos)
        }
    }
}

fn nanos_to_ch_timestamp(nanos: i64) -> String {
    format!("fromUnixTimestamp64Nano({nanos})")
}

fn binary_op_to_clickhouse(op: &BinaryOp) -> &'static str {
    match op {
        BinaryOp::Add => "+",
        BinaryOp::Sub => "-",
        BinaryOp::Mul => "*",
        BinaryOp::Div => "/",
        BinaryOp::Mod => "%",
        BinaryOp::Eq => "=",
        BinaryOp::Neq => "!=",
        BinaryOp::Lt => "<",
        BinaryOp::Lte => "<=",
        BinaryOp::Gt => ">",
        BinaryOp::Gte => ">=",
        BinaryOp::And => "AND",
        BinaryOp::Or => "OR",
        BinaryOp::RegexMatch => "~",
        BinaryOp::RegexNotMatch => "!~",
    }
}

fn quote_identifier(name: &str) -> String {
    format!("\"{}\"", name.replace('\\', "\\\\").replace('"', "\\\""))
}

fn quote_string(s: &str) -> String {
    format!("'{}'", s.replace('\\', "\\\\").replace('\'', "\\'"))
}

fn format_float(f: f64) -> String {
    if f.fract() == 0.0 && f.is_finite() {
        format!("{}", f as i64)
    } else {
        format!("{}", f)
    }
}

/// Remove user-supplied `time` comparisons from a WHERE clause. InfluxDB CQs
/// ignore user time ranges and inject their own window each run.
pub fn strip_time_predicates(condition: Option<Expr>) -> Option<Expr> {
    condition.and_then(strip_time_predicates_expr)
}

fn strip_time_predicates_expr(expr: Expr) -> Option<Expr> {
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
    dest_table: &str,
    source_table: &str,
    mapping: Option<&ColumnMapping>,
    series: Option<SeriesJoin<'_>>,
    start_nanos: i64,
    end_nanos: i64,
) -> Result<String, HyperbytedbError> {
    validate_select_into(stmt)?;
    let prepared = prepare_cq_select(stmt, start_nanos, end_nanos, false);
    let select_sql = translate_inner(
        &prepared,
        source_table,
        mapping,
        series,
        Some((Some(start_nanos), Some(end_nanos))),
    )?;
    let select_sql = select_sql.replace("__time", "time");
    Ok(format!("INSERT INTO {dest_table}\n{select_sql}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::timeseriesql::parser;

    const TEST_TABLE: &str = "`mydb_autogen_cpu`";

    fn translate_test(stmt: &SelectStatement) -> String {
        translate_native_table(stmt, TEST_TABLE, None, None, None).unwrap()
    }

    const SERIES_TABLE: &str = "`mydb_autogen_cpu_series`";

    /// Mapping with `host` as a tag and `usage_idle` as a field (no collision).
    fn cpu_mapping() -> ColumnMapping {
        ColumnMapping {
            tag_keys: ["host", "region"].into_iter().map(String::from).collect(),
            field_names: ["usage_idle"].into_iter().map(String::from).collect(),
            ..Default::default()
        }
    }

    /// Translate with a series join available (force = false).
    fn translate_series(stmt: &SelectStatement, m: &ColumnMapping) -> String {
        translate_native_table(
            stmt,
            TEST_TABLE,
            Some(m),
            Some(SeriesJoin {
                table: SERIES_TABLE,
                force: false,
                tag_columns: &[],
            }),
            None,
        )
        .unwrap()
    }

    fn parse_select(q: &str) -> SelectStatement {
        let stmts = parser::parse_query(q).unwrap();
        match stmts.into_iter().next().unwrap() {
            Statement::Select(s) => s,
            _ => panic!("expected SELECT statement"),
        }
    }

    #[test]
    fn test_select_star() {
        let stmt = parse_select("SELECT * FROM cpu");
        let sql = translate_test(&stmt);
        assert!(sql.contains("SELECT *"));
        assert!(sql.contains("FROM `mydb_autogen_cpu`"));
    }

    #[test]
    fn test_mean() {
        let stmt = parse_select(r#"SELECT mean("value") FROM cpu"#);
        let sql = translate_test(&stmt);
        assert!(sql.contains("avg(\"value\")"));
    }

    #[test]
    fn test_median_count_sum_min_max() {
        let stmt =
            parse_select(r#"SELECT median("x"), count("x"), sum("x"), min("x"), max("x") FROM m"#);
        let sql = translate_test(&stmt);
        assert!(sql.contains("median(\"x\")"));
        assert!(sql.contains("count(\"x\")"));
        assert!(sql.contains("sum(\"x\")"));
        assert!(sql.contains("min(\"x\")"));
        assert!(sql.contains("max(\"x\")"));
    }

    #[test]
    fn test_first_last() {
        let stmt = parse_select(r#"SELECT first("v"), last("v") FROM m"#);
        let sql = translate_test(&stmt);
        assert!(sql.contains("argMin(\"v\", time)"));
        assert!(sql.contains("argMax(\"v\", time)"));
    }

    #[test]
    fn test_percentile() {
        let stmt = parse_select(r#"SELECT percentile("value", 95) FROM m"#);
        let sql = translate_test(&stmt);
        assert!(sql.contains("quantile(0.95)(\"value\")"));
    }

    #[test]
    fn test_spread_stddev_mode_distinct() {
        let stmt =
            parse_select(r#"SELECT spread("v"), stddev("v"), mode("v"), distinct("v") FROM m"#);
        let sql = translate_test(&stmt);
        assert!(sql.contains("(max(\"v\") - min(\"v\"))"));
        assert!(sql.contains("stddevPop(\"v\")"));
        assert!(sql.contains("topKWeighted(1)(\"v\", 1)"));
        assert!(sql.contains("DISTINCT \"v\""));
    }

    #[test]
    fn test_where_time_and_tag() {
        let stmt =
            parse_select(r#"SELECT * FROM cpu WHERE "host" = 'server01' AND time > now() - 1h"#);
        let sql = translate_test(&stmt);
        assert!(sql.contains("WHERE"));
        assert!(sql.contains("host"));
        assert!(sql.contains("server01"));
        assert!(sql.contains("time"));
        assert!(sql.contains("now64()"));
        assert!(sql.contains("INTERVAL 1 HOUR"));
    }

    #[test]
    fn test_where_regex() {
        let stmt = parse_select(r#"SELECT * FROM m WHERE "region" =~ /us-.*/"#);
        let sql = translate_test(&stmt);
        assert!(sql.contains("match"));
        assert!(sql.contains("us-.*"));
    }

    #[test]
    fn test_group_by_time() {
        let stmt = parse_select(r#"SELECT mean("value") FROM cpu GROUP BY time(5m)"#);
        let sql = translate_test(&stmt);
        assert!(sql.contains("GROUP BY"));
        assert!(sql.contains("toStartOfInterval(time, INTERVAL 5 MINUTE)"));
    }

    #[test]
    fn test_group_by_time_with_offset() {
        let stmt = parse_select(r#"SELECT mean("value") FROM cpu GROUP BY time(1h, 15m)"#);
        let sql = translate_test(&stmt);
        assert!(sql.contains(
            "toStartOfInterval(time - INTERVAL 15 MINUTE, INTERVAL 1 HOUR) + INTERVAL 15 MINUTE"
        ));
    }

    #[test]
    fn test_group_by_time_and_tags() {
        let stmt =
            parse_select(r#"SELECT mean("value") FROM cpu GROUP BY time(5m), "host", "region""#);
        let sql = translate_test(&stmt);
        assert!(sql.contains("toStartOfInterval(time, INTERVAL 5 MINUTE)"));
        assert!(sql.contains("\"host\""));
        assert!(sql.contains("\"region\""));
        // Tag columns must appear in SELECT for result splitting
        let select_line = sql.lines().next().unwrap();
        assert!(
            select_line.contains("\"host\""),
            "SELECT must include tag columns, got: {}",
            select_line
        );
        assert!(
            select_line.contains("\"region\""),
            "SELECT must include tag columns, got: {}",
            select_line
        );
    }

    #[test]
    fn test_fill_null() {
        let stmt = parse_select(r#"SELECT mean("value") FROM cpu GROUP BY time(5m) fill(null)"#);
        let sql = translate_test(&stmt);
        assert!(
            !sql.contains("ifNull"),
            "fill(null) must not coerce NULL to 0, got: {sql}"
        );
        assert!(sql.contains("avg(\"value\")"));
        assert!(sql.contains("WITH FILL STEP INTERVAL 5 MINUTE"));
    }

    #[test]
    fn test_fill_null_with_time_bounds_uses_from_to() {
        let stmt = parse_select(
            r#"SELECT mean("load1") FROM "system" WHERE time >= 1781541739132ms AND time <= 1781552539132ms GROUP BY time(10s) fill(null)"#,
        );
        let min = 1_781_541_739_132_000_000i64;
        let max = 1_781_552_539_132_000_000i64;
        let sql =
            translate_native_table(&stmt, TEST_TABLE, None, None, Some((Some(min), Some(max))))
                .unwrap();
        assert!(
            sql.contains("WITH FILL FROM toStartOfInterval(fromUnixTimestamp64Nano(1781541739132000000), INTERVAL 10 SECOND)"),
            "expected FROM bound aligned to bucket, got: {sql}"
        );
        assert!(
            sql.contains("TO toStartOfInterval(fromUnixTimestamp64Nano(1781552539132000000), INTERVAL 10 SECOND)"),
            "expected TO bound aligned to bucket, got: {sql}"
        );
        assert!(
            sql.contains("STEP INTERVAL 10 SECOND"),
            "expected STEP after FROM/TO, got: {sql}"
        );
    }

    #[test]
    fn test_fill_with_group_by_tag_orders_tag_before_time() {
        // fill() + GROUP BY tag must order the tag column *before* the
        // time-fill column so ClickHouse fills each tag group independently.
        // Otherwise WITH FILL emits gap rows with an empty tag value (a phantom
        // all-NULL series) and never fills the real per-tag series.
        let stmt = parse_select(
            r#"SELECT mean("usage_idle") FROM cpu GROUP BY time(10s), "host" fill(null)"#,
        );
        let sql = translate_series(&stmt, &cpu_mapping());
        assert!(
            sql.contains(
                "ORDER BY \"host\" ASC, toStartOfInterval(time, INTERVAL 10 SECOND) ASC WITH FILL"
            ),
            "tag must precede the time-fill column in ORDER BY, got: {sql}"
        );
    }

    #[test]
    fn test_raw_select_projects_time_and_orders_ascending() {
        // Raw (non-aggregate) selects must carry `time` and default to time ASC,
        // matching InfluxDB. Without this, points come back in storage order.
        let stmt = parse_select(r#"SELECT "load1", "load5" FROM system"#);
        let sql = translate_test(&stmt);
        assert!(
            sql.starts_with("SELECT \"time\","),
            "raw select must project time first, got: {sql}"
        );
        assert!(
            sql.contains("ORDER BY time ASC"),
            "raw select defaults to time ASC, got: {sql}"
        );
    }

    #[test]
    fn test_group_by_time_defaults_to_order_by_time_ascending() {
        let stmt = parse_select(r#"SELECT mean("value") FROM cpu GROUP BY time(5m)"#);
        let sql = translate_test(&stmt);
        assert!(
            sql.contains("ORDER BY toStartOfInterval(time, INTERVAL 5 MINUTE) ASC"),
            "GROUP BY time defaults to time ASC, got: {sql}"
        );
    }

    #[test]
    fn test_aggregate_without_group_by_time_has_no_order_by() {
        // Collapses to a single row — no ORDER BY (and no raw `time` column).
        let stmt = parse_select(r#"SELECT mean("value") FROM cpu"#);
        let sql = translate_test(&stmt);
        assert!(!sql.contains("ORDER BY"), "got: {sql}");
        assert!(!sql.contains("\"time\""), "no raw time column, got: {sql}");
    }

    #[test]
    fn test_select_star_orders_by_time_without_duplicate_time() {
        let stmt = parse_select("SELECT * FROM cpu");
        let sql = translate_test(&stmt);
        assert!(sql.starts_with("SELECT *"), "got: {sql}");
        assert!(sql.contains("ORDER BY time ASC"), "got: {sql}");
    }

    #[test]
    fn test_fill_value() {
        let stmt = parse_select(r#"SELECT mean("value") FROM cpu GROUP BY time(5m) fill(0)"#);
        let sql = translate_test(&stmt);
        assert!(sql.contains("ifNull(avg(\"value\"), 0)"));
        assert!(sql.contains("WITH FILL"));
    }

    #[test]
    fn test_fill_none() {
        let stmt = parse_select(r#"SELECT mean("value") FROM cpu GROUP BY time(5m) fill(none)"#);
        let sql = translate_test(&stmt);
        assert!(!sql.contains("ifNull"));
        assert!(!sql.contains("WITH FILL"));
    }

    #[test]
    fn test_limit_offset() {
        let stmt = parse_select("SELECT * FROM cpu LIMIT 10 OFFSET 5");
        let sql = translate_test(&stmt);
        assert!(sql.contains("LIMIT 10"));
        assert!(sql.contains("OFFSET 5"));
    }

    #[test]
    fn test_order_by_desc() {
        let stmt =
            parse_select(r#"SELECT mean("value") FROM cpu GROUP BY time(5m) ORDER BY time DESC"#);
        let sql = translate_test(&stmt);
        assert!(sql.contains("ORDER BY"));
        assert!(sql.contains("DESC"));
    }

    #[test]
    fn test_derivative() {
        let stmt = parse_select(r#"SELECT derivative("value", 1s) FROM cpu"#);
        let sql = translate_test(&stmt);
        assert!(
            sql.contains("lagInFrame"),
            "expected lagInFrame, got: {sql}"
        );
        assert!(
            sql.contains("toFloat64"),
            "expected toFloat64 time conversion, got: {sql}"
        );
        assert!(
            !sql.contains("PARTITION BY"),
            "no tags = no PARTITION BY, got: {sql}"
        );
    }

    #[test]
    fn test_non_negative_derivative() {
        let stmt = parse_select(r#"SELECT non_negative_derivative("value", 1s) FROM cpu"#);
        let sql = translate_test(&stmt);
        assert!(
            sql.contains("if("),
            "expected if() for non-negative check, got: {sql}"
        );
        assert!(sql.contains(">= 0"), "expected >= 0 check, got: {sql}");
        assert!(
            sql.contains("lagInFrame"),
            "expected lagInFrame, got: {sql}"
        );
        assert!(
            sql.contains("toFloat64"),
            "expected toFloat64 time conversion, got: {sql}"
        );
    }

    #[test]
    fn test_difference() {
        let stmt = parse_select(r#"SELECT difference("value") FROM cpu"#);
        let sql = translate_test(&stmt);
        assert!(sql.contains("lagInFrame"));
        assert!(!sql.contains("if("));
    }

    #[test]
    fn test_nested_aggregate_in_derivative() {
        let stmt = parse_select(
            r#"SELECT non_negative_derivative(mean("reads"), 1s) FROM "diskio" WHERE time >= 1000ms GROUP BY time(10s), "host" fill(null)"#,
        );
        let sql = translate_test(&stmt);
        assert!(
            sql.contains("avg(\"reads\")"),
            "expected avg(reads), got: {sql}"
        );
        assert!(
            sql.contains("ORDER BY __time"),
            "expected ORDER BY __time, got: {sql}"
        );
        assert!(
            sql.contains("lagInFrame"),
            "expected lagInFrame, got: {sql}"
        );
        assert!(
            sql.contains(">= 0"),
            "expected non-negative check, got: {sql}"
        );
        assert!(
            sql.contains("PARTITION BY \"host\""),
            "GROUP BY tag must produce PARTITION BY in window clause, got: {sql}"
        );
        assert!(
            sql.contains("toFloat64"),
            "expected toFloat64 time conversion, got: {sql}"
        );
        let select_line = sql.lines().next().unwrap();
        assert!(
            select_line.contains("\"host\""),
            "expected host in SELECT, got: {select_line}"
        );
    }

    #[test]
    fn test_derivative_with_nested_first() {
        let stmt = parse_select(
            r#"SELECT derivative(first("bytes_recv"), 1s) * 8 FROM net GROUP BY time(10s) fill(null)"#,
        );
        let sql = translate_test(&stmt);
        // first() → argMin(field, time)
        assert!(
            sql.contains("argMin(\"bytes_recv\", time)"),
            "expected argMin, got: {sql}"
        );
        assert!(
            sql.contains("ORDER BY __time"),
            "expected ORDER BY __time, got: {sql}"
        );
    }

    #[test]
    fn test_moving_average() {
        let stmt = parse_select(r#"SELECT moving_average("value", 5) FROM cpu"#);
        let sql = translate_test(&stmt);
        assert!(sql.contains("avg(\"value\") OVER"));
        assert!(sql.contains("ROWS BETWEEN 4 PRECEDING AND CURRENT ROW"));
    }

    #[test]
    fn test_cumulative_sum() {
        let stmt = parse_select(r#"SELECT cumulative_sum("value") FROM cpu"#);
        let sql = translate_test(&stmt);
        assert!(sql.contains("sum(\"value\") OVER"));
        assert!(sql.contains("ROWS UNBOUNDED PRECEDING"));
    }

    #[test]
    fn test_elapsed() {
        let stmt = parse_select(r#"SELECT elapsed("value", 1s) FROM cpu"#);
        let sql = translate_test(&stmt);
        assert!(
            sql.contains("lagInFrame(time, 1)"),
            "expected lagInFrame, got: {sql}"
        );
        assert!(
            sql.contains("toFloat64"),
            "expected toFloat64 time conversion, got: {sql}"
        );
    }

    #[test]
    fn test_fill_previous() {
        let stmt = parse_select(
            r#"SELECT mean("value") AS avg_val FROM cpu GROUP BY time(5m) fill(previous)"#,
        );
        let sql = translate_test(&stmt);
        assert!(sql.contains("WITH FILL STEP INTERVAL 5 MINUTE"));
        assert!(sql.contains("INTERPOLATE"));
        assert!(sql.contains("\"avg_val\""));
        assert!(!sql.contains("ifNull"));
    }

    #[test]
    fn test_fill_linear() {
        let stmt = parse_select(
            r#"SELECT mean("value") AS avg_val FROM cpu GROUP BY time(5m) fill(linear)"#,
        );
        let sql = translate_test(&stmt);
        assert!(sql.contains("WITH FILL STEP INTERVAL 5 MINUTE"));
        assert!(sql.contains("INTERPOLATE"));
        assert!(sql.contains("\"avg_val\" AS \"avg_val\""));
        assert!(!sql.contains("ifNull"));
    }

    #[test]
    fn test_grafana_tag_annotation() {
        let stmt = parse_select(
            r#"SELECT mean("usage_idle") FROM cpu WHERE time >= 1000ms AND time <= 2000ms GROUP BY time(1s), "host"::tag"#,
        );
        let sql = translate_test(&stmt);
        assert!(sql.contains("GROUP BY"));
        assert!(
            sql.contains("\"host\""),
            "should strip ::tag suffix, got: {sql}"
        );
        assert!(
            !sql.contains("::tag"),
            "should not contain ::tag, got: {sql}"
        );
    }

    #[test]
    fn test_epoch_ms_time_comparison() {
        let stmt = parse_select(
            r#"SELECT * FROM cpu WHERE time >= 1772462462777ms AND time <= 1772466062777ms"#,
        );
        let sql = translate_test(&stmt);
        assert!(
            sql.contains("fromUnixTimestamp64Milli(1772462462777)"),
            "should convert ms epoch to timestamp, got: {sql}"
        );
        assert!(
            sql.contains("fromUnixTimestamp64Milli(1772466062777)"),
            "should convert ms epoch to timestamp, got: {sql}"
        );
        assert!(
            !sql.contains("INTERVAL"),
            "should not use INTERVAL for epoch timestamps, got: {sql}"
        );
    }

    #[test]
    fn test_epoch_ns_time_comparison() {
        let stmt = parse_select(r#"SELECT * FROM cpu WHERE time >= 1772462462777000000"#);
        let sql = translate_test(&stmt);
        assert!(
            sql.contains("fromUnixTimestamp64Nano(1772462462777000000)"),
            "bare integer should become nanosecond timestamp, got: {sql}"
        );
    }

    #[test]
    fn test_non_negative_derivative_with_multiple_tags() {
        let stmt = parse_select(
            r#"SELECT non_negative_derivative(mean("read_bytes"), 1s) AS "Reads", non_negative_derivative(mean("write_bytes"), 1s) AS "Writes" FROM "diskio" WHERE "host" =~ /^(8a8b7bfef1c0)$/ AND time >= 1772542183541ms AND time <= 1772542483541ms GROUP BY time(1s), "host", "name" fill(null)"#,
        );
        let sql = translate_test(&stmt);
        assert!(
            sql.contains(r#"PARTITION BY "host", "name""#),
            "window must PARTITION BY all GROUP BY tags to avoid cross-series derivative, got: {sql}"
        );
        assert!(
            sql.contains("toFloat64"),
            "time diff must use toFloat64 for correct arithmetic, got: {sql}"
        );
        assert!(
            sql.contains("avg(\"read_bytes\")"),
            "expected avg(read_bytes), got: {sql}"
        );
        assert!(
            sql.contains("avg(\"write_bytes\")"),
            "expected avg(write_bytes), got: {sql}"
        );
        assert!(
            sql.contains(">= 0"),
            "expected non-negative check, got: {sql}"
        );
        assert!(
            sql.contains("AS \"Reads\""),
            "expected Reads alias, got: {sql}"
        );
        assert!(
            sql.contains("AS \"Writes\""),
            "expected Writes alias, got: {sql}"
        );
    }

    #[test]
    fn test_difference_with_tags_has_partition_by() {
        let stmt = parse_select(
            r#"SELECT difference(mean("value")) FROM cpu GROUP BY time(10s), "host", "region""#,
        );
        let sql = translate_test(&stmt);
        assert!(
            sql.contains(r#"PARTITION BY "host", "region""#),
            "difference window must PARTITION BY tags, got: {sql}"
        );
    }

    #[test]
    fn test_moving_average_with_tags_has_partition_by() {
        let stmt = parse_select(
            r#"SELECT moving_average(mean("value"), 5) FROM cpu GROUP BY time(10s), "host""#,
        );
        let sql = translate_test(&stmt);
        assert!(
            sql.contains(r#"PARTITION BY "host""#),
            "moving_average window must PARTITION BY tags, got: {sql}"
        );
    }

    #[test]
    fn test_cumulative_sum_with_tags_has_partition_by() {
        let stmt = parse_select(
            r#"SELECT cumulative_sum(mean("value")) FROM cpu GROUP BY time(10s), "host""#,
        );
        let sql = translate_test(&stmt);
        assert!(
            sql.contains(r#"PARTITION BY "host""#),
            "cumulative_sum window must PARTITION BY tags, got: {sql}"
        );
    }

    #[test]
    fn test_non_negative_difference_divided_by_constant() {
        let stmt = parse_select(
            r#"SELECT NON_NEGATIVE_DIFFERENCE(mean("packets_recv"))/10 AS "in", NON_NEGATIVE_DIFFERENCE(mean("packets_sent"))/10 AS "out" FROM "net" WHERE "host" =~ /^(telegraf-664c6bf94-pgt7t)$/ AND "interface" =~ /(vlan|eth|bond).*/ AND time >= 1772706604176ms AND time <= 1772706904176ms GROUP BY time(1s), "host", "interface" fill(null)"#,
        );
        let sql = translate_test(&stmt);
        assert!(
            sql.contains("lagInFrame"),
            "expected lagInFrame for difference, got: {sql}"
        );
        assert!(sql.contains("/ 10"), "expected division by 10, got: {sql}");
        assert!(sql.contains("AS \"in\""), "expected alias 'in', got: {sql}");
        assert!(
            sql.contains("AS \"out\""),
            "expected alias 'out', got: {sql}"
        );
        assert!(
            !sql.contains("NON_NEGATIVE_DIFFERENCE"),
            "should not contain raw TimeseriesQL function name in output SQL, got: {sql}"
        );
    }

    #[test]
    fn test_derivative_unit_conversion() {
        let stmt = parse_select(r#"SELECT derivative("value", 1ms) FROM cpu GROUP BY time(10s)"#);
        let sql = translate_test(&stmt);
        assert!(
            sql.contains("/ 0.001"),
            "1ms unit should divide time diff by 0.001 seconds, got: {sql}"
        );
    }

    #[test]
    fn test_relative_time_still_uses_interval() {
        let stmt = parse_select(r#"SELECT * FROM cpu WHERE time > now() - 1h"#);
        let sql = translate_test(&stmt);
        assert!(sql.contains("now64()"), "should keep now64(), got: {sql}");
        assert!(
            sql.contains("INTERVAL 1 HOUR"),
            "relative duration should stay as interval, got: {sql}"
        );
    }

    #[test]
    fn test_translate_select_into() {
        let q = r#"SELECT mean("value") INTO "cpu_1h" FROM "cpu" WHERE "host" = 'server01' GROUP BY time(1h), "host""#;
        let stmt = parse_select(q);
        let sql = translate_select_into(&stmt, "`mydb_autogen_cpu_1h`", "`mydb_autogen_cpu`", None)
            .unwrap();
        assert!(sql.starts_with("INSERT INTO `mydb_autogen_cpu_1h`"));
        assert!(sql.contains("SELECT "));
        assert!(sql.contains("time"));
        assert!(!sql.contains("__time"));
        assert!(sql.contains("avg(\"value\")"));
        assert!(sql.contains("GROUP BY"));
        assert!(sql.contains("toStartOfInterval(time, INTERVAL 1 HOUR)"));
    }

    #[test]
    fn test_select_into_requires_group_by_time() {
        let q = r#"SELECT mean("value") INTO "cpu_1h" FROM "cpu""#;
        let stmt = parse_select(q);
        assert!(translate_select_into(&stmt, "`dest`", "`source`", None).is_err());
    }

    #[test]
    fn test_translate_materialized_view_select() {
        let q = r#"SELECT mean("value") INTO "cpu_5m" FROM "cpu" GROUP BY time(5m), "host""#;
        let stmt = parse_select(q);
        let map = cpu_mapping();
        let sql = translate_materialized_view_select(
            &stmt,
            "`mydb_autogen_cpu`",
            "`mydb_autogen_cpu_series`",
            "cpu_5m",
            &map,
        )
        .unwrap();
        assert!(sql.starts_with("SELECT "));
        assert!(sql.contains("toStartOfInterval(t.time, INTERVAL 5 MINUTE) AS time"));
        assert!(sql.contains("any(t.`_mv_src_origin_node_id`) AS origin_node_id"));
        assert!(sql.contains("max(t.`_mv_src_ingest_seq`) AS ingest_seq"));
        assert!(sql.contains("sipHash64("));
        assert!(sql.contains("AS \"count_value\""));
        assert!(sql.contains("AS \"sum_value\""));
        assert!(!sql.contains("avg(\"value\")"));
        assert!(
            sql.contains("argMax(\"value\", `ingest_seq`)"),
            "MV source should coalesce duplicate raw rows before aggregating"
        );
        assert!(
            sql.contains(
                "FROM (SELECT `series_id`, `time`, max(`ingest_seq`) AS `_mv_src_ingest_seq`"
            ),
            "MV should read from coalesced source subquery, got: {sql}"
        );
        assert!(sql.contains("AS t ANY INNER JOIN `mydb_autogen_cpu_series` AS s"));
        assert!(sql.contains("GROUP BY toStartOfInterval(t.time, INTERVAL 5 MINUTE)"));
        assert!(sql.contains("s.\"host\""));
        assert!(!sql.contains("INSERT INTO"));
        // Field columns must appear in sorted-by-name order (count < sum).
        let count_pos = sql.find("AS \"count_value\"").unwrap();
        let sum_pos = sql.find("AS \"sum_value\"").unwrap();
        assert!(
            count_pos < sum_pos,
            "fields should be sorted: count_value before sum_value, got: {}..{}",
            count_pos,
            sum_pos
        );
    }

    #[test]
    fn materialized_view_backfill_orders_insert_columns_by_physical_name() {
        let q = r#"SELECT sum("players") AS "players", sum("max_players") AS "maxplayers", sum("cpu") AS "cpu" INTO "server_stats_1m" FROM "server_stats" GROUP BY time(1m), "host""#;
        let stmt = parse_select(q);
        let map = cpu_mapping();
        let sql = translate_materialized_view_backfill(
            &stmt,
            "`dest`",
            "`source`",
            "`source_series`",
            "server_stats_1m",
            &map,
        )
        .unwrap();
        assert!(
            sql.starts_with(
                "INSERT INTO `dest` (\"time\", \"origin_node_id\", \"ingest_seq\", \"series_id\", \"cpu\", \"maxplayers\", \"players\")"
            ),
            "backfill must name columns in DDL order, got: {sql}"
        );
        assert!(
            sql.contains("SELECT \"time\", \"origin_node_id\", \"ingest_seq\", \"series_id\", \"cpu\", \"maxplayers\", \"players\"\nFROM ("),
            "backfill outer SELECT must match INSERT column order, got: {sql}"
        );
    }

    #[test]
    fn rollup_fact_view_uses_sum_for_additive_fields() {
        use crate::domain::rollup::RollupCombine;

        let mut map = cpu_mapping();
        map.field_rollups
            .insert("usage_idle".to_string(), RollupCombine::Sum);
        let sql = build_coalesced_fact_view(TEST_TABLE, &map);
        assert!(
            sql.contains("sum(\"usage_idle\") AS \"usage_idle\""),
            "rollup fields should merge with sum(), got: {sql}"
        );
        assert!(
            !sql.contains("argMax(\"usage_idle\""),
            "rollup sum fields must not use argMax, got: {sql}"
        );
    }

    #[test]
    fn raw_fact_view_still_uses_argmax_without_rollups() {
        let map = cpu_mapping();
        let sql = build_coalesced_fact_view(TEST_TABLE, &map);
        assert!(
            sql.contains("argMax(\"usage_idle\", `ingest_seq`)"),
            "raw measurements should keep argMax coalesce, got: {sql}"
        );
    }

    #[test]
    fn mean_on_rollup_measurement_rewrites_to_sum_over_count() {
        use crate::domain::rollup::{MeanRollupField, RollupCombine};

        let mut map = cpu_mapping();
        map.mean_fields.insert(
            "value".to_string(),
            MeanRollupField {
                sum_col: "sum_value".to_string(),
                count_col: "count_value".to_string(),
            },
        );
        map.field_rollups
            .insert("sum_value".to_string(), RollupCombine::Sum);
        map.field_rollups
            .insert("count_value".to_string(), RollupCombine::Sum);

        let stmt = parse_select(r#"SELECT mean("value") FROM cpu GROUP BY time(5m), "host""#);
        let sql = translate_native_table(
            &stmt,
            TEST_TABLE,
            Some(&map),
            Some(SeriesJoin {
                table: SERIES_TABLE,
                force: false,
                tag_columns: &[],
            }),
            None,
        )
        .unwrap();
        assert!(
            sql.contains("sum(\"sum_value\") / nullIf(sum(\"count_value\"), 0)"),
            "expected weighted mean rewrite, got: {sql}"
        );
    }

    #[test]
    fn test_tag_field_collision_uses_column_mapping() {
        let stmt = parse_select(r#"SELECT mean("cpu") FROM m GROUP BY cpu"#);
        let mut map = ColumnMapping::default();
        map.tag_keys.insert("cpu".into());
        map.field_names.insert("cpu".into());
        let sql = translate_native_table(
            &stmt,
            TEST_TABLE,
            Some(&map),
            Some(SeriesJoin {
                table: SERIES_TABLE,
                force: false,
                tag_columns: &[],
            }),
            None,
        )
        .unwrap();
        assert!(
            sql.contains("__tag__cpu"),
            "tag column should be prefixed when it collides with a field, got: {sql}"
        );
        assert!(
            sql.contains("avg(\"cpu\")"),
            "aggregate should use field column name, got: {sql}"
        );
        assert!(
            sql.contains("GROUP BY \"__tag__cpu\""),
            "GROUP BY must use the physical tag column to match SELECT, got: {sql}"
        );
    }

    // --- series_id layout: tag resolution via the dimension-table inline view ---

    #[test]
    fn series_field_only_query_has_no_join() {
        // No tag referenced → coalesced fact view, no series dimension join.
        let stmt = parse_select(r#"SELECT mean("usage_idle") FROM cpu WHERE time > 0"#);
        let sql = translate_series(&stmt, &cpu_mapping());
        assert!(
            !sql.contains("JOIN") && !sql.contains("_series"),
            "field-only query should not join the series table, got: {sql}"
        );
        assert!(
            sql.contains("argMax(\"usage_idle\", `ingest_seq`)"),
            "field-only query should collapse duplicate rows by ingest_seq, got: {sql}"
        );
        assert!(sql.contains("FROM `mydb_autogen_cpu`"), "got: {sql}");
    }

    #[test]
    fn telegraf_cpu_multi_field_query_coalesces_partial_rows() {
        let stmt = parse_select(
            r#"SELECT mean("usage_guest") AS "Usage Guest", mean("usage_idle") AS "Usage Idle", mean("usage_user") AS "Usage User" FROM "cpu" WHERE "host" =~ /^(d2ddee27a9f4)$/ AND "cpu" = 'cpu-total' AND time >= 1780922276152ms and time <= 1780925876152ms GROUP BY time(2s), "host" fill(null)"#,
        );
        let mut map = ColumnMapping::default();
        map.tag_keys.insert("host".into());
        map.tag_keys.insert("cpu".into());
        for f in [
            "usage_guest",
            "usage_idle",
            "usage_user",
            "usage_system",
            "usage_iowait",
        ] {
            map.field_names.insert(f.into());
        }
        let sql = translate_native_table(
            &stmt,
            TEST_TABLE,
            Some(&map),
            Some(SeriesJoin {
                table: SERIES_TABLE,
                force: false,
                tag_columns: &[],
            }),
            None,
        )
        .unwrap();
        assert!(
            sql.contains("argMax(\"usage_idle\", `ingest_seq`)"),
            "expected coalesced fact view, got: {sql}"
        );
        assert!(
            sql.contains("ANY LEFT JOIN `mydb_autogen_cpu_series` AS s"),
            "tag filter should join series table, got: {sql}"
        );
        assert!(sql.contains("avg(\"usage_idle\")"), "got: {sql}");
        assert!(
            sql.contains("toStartOfInterval(time, INTERVAL 2 SECOND)"),
            "got: {sql}"
        );
    }

    #[test]
    fn series_where_tag_filter_joins_dimension() {
        let stmt = parse_select(r#"SELECT mean("usage_idle") FROM cpu WHERE "host" = 'h1'"#);
        let sql = translate_series(&stmt, &cpu_mapping());
        assert!(
            sql.contains("ANY LEFT JOIN `mydb_autogen_cpu_series` AS s"),
            "tag filter should join the series table, got: {sql}"
        );
        assert!(
            sql.contains("t.`series_id` = s.`series_id`"),
            "join key should be series_id, got: {sql}"
        );
        // The tag predicate resolves against the joined view's tag column.
        assert!(sql.contains("\"host\" = 'h1'"), "got: {sql}");
    }

    #[test]
    fn series_group_by_all_tags_expands_to_measurement_tags() {
        let mut stmt = parse_select(r#"SELECT mean("usage_idle") FROM cpu GROUP BY time(5m), *"#);
        let gb = stmt.group_by.as_ref().unwrap().clone();
        let (expanded_gb, tags) = gb.expand_all_tags(&["host".to_string(), "region".to_string()]);
        stmt.group_by = Some(expanded_gb);
        assert_eq!(tags, vec!["host", "region"]);
        let sql = translate_series(&stmt, &cpu_mapping());
        assert!(sql.contains("ANY LEFT JOIN"), "got: {sql}");
        assert!(sql.contains("\"host\""), "got: {sql}");
        assert!(sql.contains("\"region\""), "got: {sql}");
        assert!(!sql.contains("`*`"), "got: {sql}");
    }

    #[test]
    fn series_group_by_tag_projects_and_groups_physical() {
        let stmt = parse_select(r#"SELECT mean("usage_idle") FROM cpu GROUP BY time(5m), "host""#);
        let sql = translate_series(&stmt, &cpu_mapping());
        assert!(sql.contains("ANY LEFT JOIN"), "got: {sql}");
        // host is non-colliding, so physical == logical.
        assert!(
            sql.contains("\"host\""),
            "tag projected/grouped, got: {sql}"
        );
        assert!(sql.contains("GROUP BY"), "got: {sql}");
        assert!(sql.contains("avg(\"usage_idle\")"), "got: {sql}");
    }

    #[test]
    fn series_view_exposes_only_tag_columns_from_dimension() {
        let stmt = parse_select(r#"SELECT mean("usage_idle") FROM cpu GROUP BY "host""#);
        let sql = translate_series(&stmt, &cpu_mapping());
        // Inline view selects t.* plus the dimension's tag columns (sorted).
        assert!(
            sql.contains("SELECT t.*, s.\"host\", s.\"region\""),
            "view should re-attach tag columns, got: {sql}"
        );
    }

    #[test]
    fn series_force_join_without_tag_reference() {
        // force=true (e.g. a tombstone references a tag) joins even a field-only body.
        let stmt = parse_select(r#"SELECT mean("usage_idle") FROM cpu WHERE time > 0"#);
        let m = cpu_mapping();
        let sql = translate_native_table(
            &stmt,
            TEST_TABLE,
            Some(&m),
            Some(SeriesJoin {
                table: SERIES_TABLE,
                force: true,
                tag_columns: &[],
            }),
            None,
        )
        .unwrap();
        assert!(
            sql.contains("ANY LEFT JOIN"),
            "force should join, got: {sql}"
        );
    }

    #[test]
    fn mv_series_select_uses_dest_field_names_for_tag_prefix() {
        // Tag "host" collides with a field only in the destination, not the source.
        // Source mapping treats "host" as non-colliding (source field_names is
        // {"usage_idle"}), so tag_column_name("host") returns "host".
        // Destination has field "host", so dest_field_names = {"host", "usage_idle"},
        // and tag_column_name("host") should return "__tag__host".
        let stmt = parse_select(
            r#"SELECT mean("usage_idle") INTO "dest" FROM "cpu" GROUP BY time(5m), "host""#,
        );
        let mut src_mapping = cpu_mapping();
        src_mapping.tag_keys.insert("host".to_string());

        let dest_field_names: std::collections::HashSet<String> =
            ["host".to_string(), "usage_idle".to_string()].into();

        let sql = translate_materialized_view_series_select(
            &stmt,
            "`source_series`",
            "dest",
            &src_mapping,
            Some(&dest_field_names),
        )
        .unwrap();

        assert!(
            sql.contains("__tag__host"),
            "tag 'host' should be prefixed when dest has colliding field, got: {sql}"
        );
    }

    #[test]
    fn mv_series_select_uses_source_names_when_no_dest_field_names() {
        let stmt = parse_select(
            r#"SELECT mean("usage_idle") INTO "dest" FROM "cpu" GROUP BY time(5m), "host""#,
        );
        let mut src_mapping = cpu_mapping();
        src_mapping.tag_keys.insert("host".to_string());

        let sql = translate_materialized_view_series_select(
            &stmt,
            "`source_series`",
            "dest",
            &src_mapping,
            None,
        )
        .unwrap();

        // Without dest field names, source mapping says "host" doesn't collide
        // (cpu_mapping has only "usage_idle" as field).
        assert!(
            sql.contains("\"host\""),
            "tag 'host' should NOT be prefixed when dest_field_names is None, got: {sql}"
        );
        assert!(
            !sql.contains("__tag__host"),
            "tag 'host' should NOT be prefixed without dest_field_names, got: {sql}"
        );
    }
}
