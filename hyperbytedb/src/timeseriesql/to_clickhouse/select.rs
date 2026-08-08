use crate::domain::chdb_naming::QuotedTableName;
use crate::domain::column_mapping::ColumnMapping;
use crate::error::HyperbytedbError;
use crate::timeseriesql::ast::*;
use std::fmt::Write;

use super::SeriesJoin;
use super::aggregates::{
    expr_contains_aggregate, expr_contains_raw_transform, translate_aggregate_call,
    translate_binary_expr,
};
use super::coalesce::build_coalesced_fact_view;
use super::conditions::{
    format_float, nanos_to_ch_timestamp, quote_identifier, quote_phys_identifier, quote_string,
    translate_expr,
};

pub fn translate_native_table(
    stmt: &SelectStatement,
    table_source: &str,
    mapping: Option<&ColumnMapping>,
    series: Option<SeriesJoin<'_>>,
    time_bounds: Option<(Option<i64>, Option<i64>)>,
) -> Result<String, HyperbytedbError> {
    translate_inner(stmt, table_source, mapping, series, time_bounds)
}

pub(super) fn translate_inner(
    stmt: &SelectStatement,
    from_source: &str,
    mapping: Option<&ColumnMapping>,
    series: Option<SeriesJoin<'_>>,
    time_bounds: Option<(Option<i64>, Option<i64>)>,
) -> Result<String, HyperbytedbError> {
    let mut out = String::new();

    // InfluxQL treats a GROUP BY time() query without an explicit fill() as
    // fill(null): every bucket in the queried range is emitted, with NULL
    // aggregates for empty buckets. Writes (`SELECT ... INTO` / CQ runs) keep
    // the absent-fill case as "no fill" so synthetic NULL rows are never
    // inserted into the destination.
    let effective_fill = match (&stmt.fill, &stmt.into) {
        (Some(f), _) => f.clone(),
        (None, Some(_)) => FillOption::None,
        (None, None) => FillOption::Null,
    };

    // Only `fill(<number>)` coerces NULL aggregates to a numeric default in SQL.
    // `fill(null)` must leave NULL so JSON shows null, not 0.
    let use_ifnull_fill = matches!(effective_fill, FillOption::Value(_));
    let needs_with_fill = !matches!(effective_fill, FillOption::None);
    let fill_value = match &effective_fill {
        FillOption::Value(v) => *v,
        _ => 0.0,
    };

    // tz() flows into every bucketing expression (SELECT / GROUP BY / ORDER BY
    // and the WITH FILL grid anchors) so buckets align on local-time boundaries,
    // including 23/25-hour DST days.
    let tz = stmt.timezone.as_deref();

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
            let time_expr = time_bucket_expr(interval, offset.as_ref(), tz);
            // Use __time alias to avoid collision with the raw `time` column,
            // then rename back to `time` in the result parser.
            select_parts.push(format!("{} AS __time", time_expr));
        }

        // Include GROUP BY tag columns in SELECT so they appear in the result
        // and can be used to split rows into separate InfluxDB series.
        for tag in gb.tag_dimensions() {
            select_parts.push(select_tag_column_sql(tag, mapping)?);
        }
    }

    let has_aggregate = stmt.fields.iter().any(|f| expr_contains_call(&f.expr));
    let has_star = stmt
        .fields
        .iter()
        .any(|f| matches!(f.expr, Expr::Star | Expr::Wildcard));
    // True aggregates collapse rows; bare window transforms (difference("v"),
    // moving_average("v", n), ...) stay per-point and must keep the raw `time`
    // column and per-point ordering like raw selects.
    let has_true_aggregate = select_has_true_aggregate(stmt);
    let has_raw_transform = stmt
        .fields
        .iter()
        .any(|f| expr_contains_raw_transform(&f.expr));

    // Raw (non-aggregate) selects return one row per point and must carry the
    // point's `time` column, like InfluxDB. `SELECT *` already projects `time`,
    // and GROUP BY time() / aggregate queries get their time column elsewhere.
    let is_raw_select = !has_group_by_time && !has_star && !has_aggregate;
    let projects_point_time =
        is_raw_select || (has_raw_transform && !has_group_by_time && !has_star);
    if projects_point_time {
        select_parts.insert(0, quote_phys_identifier("time"));
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
        .collect::<Result<Vec<String>, HyperbytedbError>>()?;
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
        let mut gb_parts = Vec::new();

        if let Some(Dimension::Time { interval, offset }) = gb.time_dimension() {
            gb_parts.push(time_bucket_expr(interval, offset.as_ref(), tz));
        }

        // Tag dimensions only group the SQL when a true aggregate is present.
        // Raw selects / bare window transforms keep one row per point: their
        // tag columns stay projected (for per-series splitting in the result
        // parser and PARTITION BY in window clauses) but grouping by them
        // would be NOT_AN_AGGREGATE in ClickHouse.
        if has_true_aggregate {
            for tag in gb.tag_dimensions() {
                // Must match the SELECT expression: physical column name (handles the
                // `__tag__` collision prefix). Previously emitted the logical name,
                // which is wrong for collision-renamed tags.
                gb_parts.push(group_by_tag_sql(tag, mapping)?);
            }
        }

        if !gb_parts.is_empty() {
            write!(out, "\nGROUP BY {}", gb_parts.join(", "))?;
        }
    }

    // Compute time column expression for ORDER BY
    let time_col = stmt.group_by.as_ref().and_then(|gb| {
        if let Some(Dimension::Time { interval, offset }) = gb.time_dimension() {
            Some(time_bucket_expr(interval, offset.as_ref(), tz))
        } else {
            None
        }
    });

    // InfluxDB orders every result by time ascending by default; an explicit
    // ORDER BY only changes the direction. Order whenever there is a time column
    // to sort on: GROUP BY time() buckets, raw per-point selects (incl. `*`), or
    // bare window transforms (which are per-point and project raw `time`).
    // Aggregates without GROUP BY time() collapse to one row and need no ordering.
    let has_orderable_time =
        time_col.is_some() || (!has_aggregate && !has_group_by_time) || projects_point_time;
    let time_desc = stmt.order_by.as_ref().is_some_and(|o| o.time_desc);
    let do_fill = needs_with_fill && time_col.is_some();
    // ClickHouse WITH FILL on a DESC-ordered column never matches the ascending
    // FROM/TO anchors we emit, so no fill rows are generated. Fill ascending in
    // this (inner) SELECT and re-order descending in a wrapper below.
    let wrap_desc_fill = time_desc && do_fill;

    if has_orderable_time {
        write!(out, "\nORDER BY ")?;

        // When filling a tag-grouped query, the tag columns must precede the
        // time-fill column in ORDER BY so ClickHouse fills each tag group
        // independently. Without this, WITH FILL fills globally: gap buckets are
        // emitted with empty tag values (a phantom all-NULL series) and the real
        // per-tag series is never filled — which surfaces as "no data" in Grafana.
        if do_fill && let Some(ref gb) = stmt.group_by {
            for tag in gb.tag_dimensions() {
                write!(out, "{} ASC, ", group_by_tag_sql(tag, mapping)?)?;
            }
        }

        if let Some(ref tc) = time_col {
            write!(out, "{}", tc)?;
        } else {
            write!(out, "time")?;
        }
        if time_desc && !wrap_desc_fill {
            write!(out, " DESC")?;
        } else {
            write!(out, " ASC")?;
        }

        if do_fill
            && let Some(ref gb) = stmt.group_by
            && let Some(Dimension::Time { interval, offset }) = gb.time_dimension()
        {
            let step = interval.to_clickhouse_interval();
            write!(out, " WITH FILL")?;
            if let Some((min_nanos, max_nanos)) = time_bounds
                && let (Some(min), Some(max)) = (min_nanos, max_nanos)
            {
                // The grid anchors must use the same bucket shape (offset +
                // timezone) as the bucket expression, or the generated grid
                // interleaves phantom buckets. `WITH FILL ... TO` is exclusive,
                // so extend one step past the bucket containing the upper WHERE
                // bound to emit the final bucket.
                let from_anchor =
                    time_bucket_expr_on(&nanos_to_ch_timestamp(min), interval, offset.as_ref(), tz);
                let to_anchor =
                    time_bucket_expr_on(&nanos_to_ch_timestamp(max), interval, offset.as_ref(), tz);
                write!(out, " FROM {from_anchor} TO {to_anchor} + {step}")?;
            }
            write!(out, " STEP {}", step)?;

            match effective_fill {
                // fill(previous): use INTERPOLATE to carry forward last known value
                FillOption::Previous if !field_aliases.is_empty() => {
                    let interp_cols: Vec<String> = field_aliases
                        .iter()
                        .map(|a| quote_identifier(a))
                        .collect::<Result<Vec<String>, HyperbytedbError>>()?;
                    write!(out, " INTERPOLATE ({})", interp_cols.join(", "))?;
                }
                // fill(linear): use INTERPOLATE with linear expressions
                FillOption::Linear if !field_aliases.is_empty() => {
                    let interp_cols: Vec<String> = field_aliases
                        .iter()
                        .map(|a| -> Result<String, HyperbytedbError> {
                            let q = quote_identifier(a)?;
                            Ok(format!("{q} AS {q}"))
                        })
                        .collect::<Result<Vec<String>, HyperbytedbError>>()?;
                    write!(out, " INTERPOLATE ({})", interp_cols.join(", "))?;
                }
                // fill(<number>): WITH FILL-generated rows get column defaults
                // (NULL) that the ifNull() around the aggregate can't reach; a
                // constant INTERPOLATE expression sets generated rows —
                // including leading gaps — to the fill value.
                FillOption::Value(v) if !field_aliases.is_empty() => {
                    let interp_cols: Vec<String> = field_aliases
                        .iter()
                        .map(|a| Ok(format!("{} AS {}", quote_identifier(a)?, format_float(v))))
                        .collect::<Result<Vec<String>, HyperbytedbError>>()?;
                    write!(out, " INTERPOLATE ({})", interp_cols.join(", "))?;
                }
                _ => {}
            }
        }
    }

    // GROUP BY tag dimensions carry InfluxQL per-series LIMIT semantics and
    // outer ordering. These are the logical (output) column names.
    let tag_dims: Vec<&str> = stmt
        .group_by
        .as_ref()
        .map(|gb| gb.tag_dimensions())
        .unwrap_or_default();

    if wrap_desc_fill {
        // Re-order the ascending filled grid descending, tags first (matching
        // the tag-first fill ordering above). Outer clauses stay on the `)`
        // line so tombstone WHERE-splicing targets only the inner query.
        let mut order_parts: Vec<String> = tag_dims
            .iter()
            .map(|t| Ok(format!("{} ASC", quote_identifier(t)?)))
            .collect::<Result<Vec<String>, HyperbytedbError>>()?;
        order_parts.push("__time DESC".to_string());
        out = format!(
            "SELECT * FROM (\n{out}\n) ORDER BY {}",
            order_parts.join(", ")
        );
    } else if has_raw_transform && !has_group_by_time {
        // InfluxQL omits rows where a per-point window transform has no value
        // yet (difference/derivative/elapsed first point, moving_average until
        // the window is full). Those surface as NULL transform outputs here;
        // filter them in a wrapper. Rows where every named transform output is
        // NULL are dropped — in InfluxDB a point with a null input field would
        // not exist in that field's series at all.
        let transform_aliases: Vec<String> = stmt
            .fields
            .iter()
            .filter(|f| expr_contains_raw_transform(&f.expr))
            .filter_map(select_output_field_name)
            .collect();
        if !transform_aliases.is_empty() {
            let cond = transform_aliases
                .iter()
                .map(|a| Ok(format!("{} IS NOT NULL", quote_identifier(a)?)))
                .collect::<Result<Vec<String>, HyperbytedbError>>()?
                .join(" OR ");
            let dir = if time_desc { "DESC" } else { "ASC" };
            out = format!(
                "SELECT * FROM (\n{out}\n) WHERE {cond} ORDER BY {} {dir}",
                quote_phys_identifier("time")
            );
        }
    }

    // LIMIT / OFFSET — InfluxQL LIMIT/OFFSET paginate points *per series*; with
    // tag dimensions in GROUP BY that maps to ClickHouse `LIMIT [m,] n BY tags`.
    // Without tag grouping the whole result is one series, so plain LIMIT works.
    if !tag_dims.is_empty() && stmt.limit.is_some() {
        let by_cols = tag_dims
            .iter()
            .map(|t| quote_identifier(t))
            .collect::<Result<Vec<String>, HyperbytedbError>>()?
            .join(", ");
        let limit = stmt.limit.unwrap_or(0);
        match stmt.offset {
            Some(offset) => write!(out, "\nLIMIT {offset}, {limit} BY ({by_cols})")?,
            None => write!(out, "\nLIMIT {limit} BY ({by_cols})")?,
        }
    } else {
        if let Some(limit) = stmt.limit {
            write!(out, "\nLIMIT {}", limit)?;
        }
        if let Some(offset) = stmt.offset {
            write!(out, "\nOFFSET {}", offset)?;
        }
    }

    Ok(out)
}

pub fn translate_with_source(
    stmt: &SelectStatement,
    source: &str,
) -> Result<String, HyperbytedbError> {
    translate_inner(stmt, source, None, None, None)
}
pub(super) fn expr_references_tag(expr: &Expr, m: &ColumnMapping) -> bool {
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
pub(super) fn query_references_tag(stmt: &SelectStatement, m: &ColumnMapping) -> bool {
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

/// Build the FROM source. When `mapping` is present the fact table is wrapped in
/// a coalesced view so partial-field rows merge before aggregation. When `series`
/// is set and the query references a tag, the coalesced fact table is wrapped in
/// an inline view that re-attaches the tag columns from the dimension table.
/// `ANY LEFT JOIN` takes at most one matching dimension row (so pre-merge duplicate
/// `ReplacingMergeTree` series rows can't fan out fact rows) and preserves fact
/// rows whose series row is briefly missing. Tag columns are exposed under their
/// physical names, so the rest of the translator — which already references tags
/// by physical name — is unchanged.
pub(super) fn build_from_source(
    fact_table: &str,
    series: Option<SeriesJoin<'_>>,
    mapping: Option<&ColumnMapping>,
    stmt: &SelectStatement,
) -> String {
    let fact = match mapping {
        Some(m) => {
            build_coalesced_fact_view(&QuotedTableName::new_quoted(fact_table.to_string()), m)
        }
        None => fact_table.to_string(),
    };
    let (Some(sj), Some(m)) = (series, mapping) else {
        return fact;
    };
    if !sj.force && !query_references_tag(stmt, m) {
        return fact;
    }
    let mut tag_cols: Vec<String> = m
        .tag_keys
        .iter()
        .map(|t| m.physical_tag_column_name(t))
        .collect();
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
        .map(|c| format!("s.{}", quote_phys_identifier(c)))
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "(SELECT t.*, {projected} FROM {fact} AS t ANY LEFT JOIN {series} AS s ON t.`series_id` = s.`series_id`)",
        series = sj.table,
    )
}
pub(super) fn group_by_tag_sql(
    tag: &str,
    mapping: Option<&ColumnMapping>,
) -> Result<String, HyperbytedbError> {
    match mapping {
        Some(m) => Ok(quote_phys_identifier(&m.physical_tag_column_name(tag))),
        None => quote_identifier(tag),
    }
}

pub(super) fn time_bucket_expr(
    interval: &Duration,
    offset: Option<&Duration>,
    tz: Option<&str>,
) -> String {
    time_bucket_expr_on("time", interval, offset, tz)
}

/// Bucketing expression over an arbitrary time expression. `tz` (from `tz()`)
/// makes `toStartOfInterval` bucket on local-time boundaries in that zone,
/// which is what keeps `GROUP BY time(1d)` correct across 23/25-hour DST days.
pub(super) fn time_bucket_expr_on(
    time_col: &str,
    interval: &Duration,
    offset: Option<&Duration>,
    tz: Option<&str>,
) -> String {
    let interval_str = interval.to_clickhouse_interval();
    let tz_arg = tz
        .map(|t| format!(", {}", quote_string(t)))
        .unwrap_or_default();
    if let Some(off) = offset {
        let off_str = off.to_clickhouse_interval();
        format!(
            "toStartOfInterval({time_col} - {}, {}{tz_arg}) + {}",
            off_str, interval_str, off_str
        )
    } else {
        format!("toStartOfInterval({time_col}, {}{tz_arg})", interval_str)
    }
}

pub(super) fn select_tag_column_sql(
    tag: &str,
    mapping: Option<&ColumnMapping>,
) -> Result<String, HyperbytedbError> {
    let Some(m) = mapping else {
        return quote_identifier(tag);
    };
    let phys = m.physical_tag_column_name(tag);
    if phys == tag {
        quote_identifier(tag)
    } else {
        Ok(format!(
            "{} AS {}",
            quote_phys_identifier(&phys),
            quote_identifier(tag)?
        ))
    }
}

pub(super) fn translate_field(
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
        Some(a) => format!("{} AS {}", sql, quote_identifier(&a)?),
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
pub(super) fn default_field_alias(expr: &Expr) -> Option<String> {
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

pub(super) fn translate_field_expr(
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
            Ok(quote_phys_identifier(&col))
        }
        Expr::FieldRef { name, .. } => {
            let col = mapping
                .map(|m| m.physical_select_identifier(name))
                .unwrap_or_else(|| name.clone());
            Ok(quote_phys_identifier(&col))
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

/// Whether any SELECT field contains a row-collapsing aggregate (not a bare window transform).
pub fn select_has_true_aggregate(stmt: &SelectStatement) -> bool {
    stmt.fields.iter().any(|f| expr_contains_aggregate(&f.expr))
}
