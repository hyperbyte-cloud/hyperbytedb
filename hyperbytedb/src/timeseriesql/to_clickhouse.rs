use crate::domain::column_mapping::ColumnMapping;
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
) -> Result<String, HyperbytedbError> {
    translate_inner(stmt, table_source, mapping, series)
}

fn translate_inner(
    stmt: &SelectStatement,
    from_source: &str,
    mapping: Option<&ColumnMapping>,
    series: Option<SeriesJoin<'_>>,
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

    // Collect field alias names for INTERPOLATE clause
    let field_aliases: Vec<String> = stmt
        .fields
        .iter()
        .map(|f| {
            f.alias.clone().unwrap_or_else(|| match &f.expr {
                Expr::Identifier(n) => n.clone(),
                Expr::Call(fc) => fc.name.clone(),
                _ => String::new(),
            })
        })
        .filter(|s| !s.is_empty())
        .collect();

    // SELECT - prepend the time bucket column when GROUP BY time() is present
    write!(out, "SELECT ")?;
    let mut select_parts: Vec<String> = Vec::new();

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

    // Add ORDER BY (explicit or implicit for fill)
    let needs_order_by = stmt.order_by.is_some() || (needs_with_fill && time_col.is_some());
    if needs_order_by {
        write!(out, "\nORDER BY ")?;
        let time_desc = stmt.order_by.as_ref().is_some_and(|o| o.time_desc);
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

        if needs_with_fill && time_col.is_some() {
            if let Some(ref gb) = stmt.group_by
                && let Some(Dimension::Time { interval, .. }) = gb.time_dimension()
            {
                let step = interval.to_clickhouse_interval();
                write!(out, " WITH FILL STEP {}", step)?;
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
    let select_sql = translate_inner(stmt, source, mapping, None)?;
    let select_sql = select_sql.replace("__time", "time");
    Ok(format!("INSERT INTO {}\n{}", dest_table, select_sql))
}

/// ClickHouse `SELECT` body for a fact-table materialized view. Groups by
/// `series_id` and the time bucket (not tag columns) and emits the system
/// columns required by the destination fact table schema.
pub fn translate_materialized_view_select(
    stmt: &SelectStatement,
    source_fact: &str,
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
    let time_bucket = time_bucket_expr(interval, offset.as_ref());

    let field_strs: Vec<String> = stmt
        .fields
        .iter()
        .map(|f| translate_field(f, false, 0.0, stmt.group_by.as_ref(), Some(mapping)))
        .collect::<Result<Vec<_>, _>>()?;

    let mut select_parts = vec![
        format!("{time_bucket} AS time"),
        "any(origin_node_id) AS origin_node_id".to_string(),
        "max(ingest_seq) AS ingest_seq".to_string(),
        "series_id".to_string(),
    ];
    select_parts.extend(field_strs);

    let mut out = String::new();
    write!(out, "SELECT {}", select_parts.join(", "))?;
    write!(out, "\nFROM {source_fact}")?;

    if let Some(ref cond) = stmt.condition {
        write!(out, "\nWHERE ")?;
        translate_expr(cond, &mut out, true, Some(mapping))?;
    }

    // GROUP BY the `time` alias — ClickHouse rejects grouping on the raw `time`
    // column expression when the SELECT output column is also named `time`.
    write!(out, "\nGROUP BY series_id, time")?;
    Ok(out)
}

/// `INSERT INTO <dest> SELECT ...` for one-time MV backfill of historical data.
pub fn translate_materialized_view_backfill(
    stmt: &SelectStatement,
    dest_table: &str,
    source_fact: &str,
    mapping: &ColumnMapping,
) -> Result<String, HyperbytedbError> {
    let select_sql = translate_materialized_view_select(stmt, source_fact, mapping)?;
    Ok(format!("INSERT INTO {dest_table}\n{select_sql}"))
}

/// Full `CREATE MATERIALIZED VIEW ... TO ... AS SELECT ...` DDL for the fact MV.
pub fn build_create_fact_materialized_view(
    mv_name: &str,
    dest_table: &str,
    select_sql: &str,
) -> String {
    format!("CREATE MATERIALIZED VIEW {mv_name} TO {dest_table} AS\n{select_sql}")
}

/// Full `CREATE MATERIALIZED VIEW ... TO ... AS SELECT * FROM ...` for series sync.
pub fn build_create_series_materialized_view(
    mv_name: &str,
    dest_series: &str,
    source_series: &str,
) -> String {
    format!("CREATE MATERIALIZED VIEW {mv_name} TO {dest_series} AS\nSELECT * FROM {source_series}")
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
    let select_sql = translate_inner(stmt, source_table, mapping, series)?;
    let select_sql = select_sql.replace("__time", "time");
    Ok(format!("INSERT INTO {}\n{}", dest_table, select_sql))
}

/// Like translate, but uses a custom source expression instead of file() - used for subqueries.
pub fn translate_with_source(
    stmt: &SelectStatement,
    source: &str,
) -> Result<String, HyperbytedbError> {
    translate_inner(stmt, source, None, None)
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

/// Build a query-time view that merges sparse partial rows sharing
/// `(series_id, time)` — the read-path counterpart to ingest coalescing.
/// Without this, `ReplacingMergeTree(ingest_seq)` keeps only the highest-seq
/// whole row, dropping fields from other partial writes.
pub fn build_coalesced_fact_view(fact_table: &str, mapping: &ColumnMapping) -> String {
    let mut field_cols: Vec<&String> = mapping.field_names.iter().collect();
    field_cols.sort();
    let field_aggs: Vec<String> = field_cols
        .iter()
        .map(|f| {
            let q = quote_identifier(f);
            format!("argMaxIf({q}, `ingest_seq`, isNotNull({q})) AS {q}")
        })
        .collect();
    let select_fields = if field_aggs.is_empty() {
        String::new()
    } else {
        format!(", {}", field_aggs.join(", "))
    };
    format!(
        "(SELECT `series_id`, `time`{select_fields} FROM {fact_table} GROUP BY `series_id`, `time`)"
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
    let interval_str = interval.to_clickhouse_interval();
    if let Some(off) = offset {
        let off_str = off.to_clickhouse_interval();
        format!(
            "toStartOfInterval(time - {}, {}) + {}",
            off_str, interval_str, off_str
        )
    } else {
        format!("toStartOfInterval(time, {})", interval_str)
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
            format!("fromUnixTimestamp64Nano({})", nanos)
        }
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::timeseriesql::parser;

    const TEST_TABLE: &str = "`mydb_autogen_cpu`";

    fn translate_test(stmt: &SelectStatement) -> String {
        translate_native_table(stmt, TEST_TABLE, None, None).unwrap()
    }

    const SERIES_TABLE: &str = "`mydb_autogen_cpu_series`";

    /// Mapping with `host` as a tag and `usage_idle` as a field (no collision).
    fn cpu_mapping() -> ColumnMapping {
        ColumnMapping {
            tag_keys: ["host", "region"].into_iter().map(String::from).collect(),
            field_names: ["usage_idle"].into_iter().map(String::from).collect(),
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
            }),
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
        let q = r#"SELECT mean("value") INTO "cpu_5m" FROM "cpu" GROUP BY time(5m), *"#;
        let stmt = parse_select(q);
        let map = cpu_mapping();
        let sql = translate_materialized_view_select(&stmt, "`mydb_autogen_cpu`", &map).unwrap();
        assert!(sql.starts_with("SELECT "));
        assert!(sql.contains("toStartOfInterval(time, INTERVAL 5 MINUTE) AS time"));
        assert!(sql.contains("any(origin_node_id) AS origin_node_id"));
        assert!(sql.contains("max(ingest_seq) AS ingest_seq"));
        assert!(sql.contains("series_id"));
        assert!(sql.contains("avg(\"value\")"));
        assert!(sql.contains("FROM `mydb_autogen_cpu`"));
        assert!(sql.contains("GROUP BY series_id, time"));
        assert!(!sql.contains("INSERT INTO"));
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
            }),
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
            sql.contains("argMaxIf(\"usage_idle\", `ingest_seq`, isNotNull(\"usage_idle\"))"),
            "field-only query should coalesce partial rows, got: {sql}"
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
            }),
        )
        .unwrap();
        assert!(
            sql.contains("argMaxIf(\"usage_idle\", `ingest_seq`, isNotNull(\"usage_idle\"))"),
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
            }),
        )
        .unwrap();
        assert!(
            sql.contains("ANY LEFT JOIN"),
            "force should join, got: {sql}"
        );
    }
}
