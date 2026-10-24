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
use super::coalesce::build_coalesced_fact_view_filtered;
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

/// [`translate_native_table`] without ORDER BY, for a sharded region's
/// partial when the coordinator sorts (and fills) the merged result. Sorting
/// ~35k region rows by tag and time was half of each region query on n3.
/// The caller must have removed fill and any region LIMIT, which need the
/// order.
pub fn translate_native_table_unordered(
    stmt: &SelectStatement,
    table_source: &str,
    mapping: Option<&ColumnMapping>,
    series: Option<SeriesJoin<'_>>,
    time_bounds: Option<(Option<i64>, Option<i64>)>,
) -> Result<String, HyperbytedbError> {
    translate_inner_ordered(stmt, table_source, mapping, series, time_bounds, false)
}

pub(super) fn translate_inner(
    stmt: &SelectStatement,
    from_source: &str,
    mapping: Option<&ColumnMapping>,
    series: Option<SeriesJoin<'_>>,
    time_bounds: Option<(Option<i64>, Option<i64>)>,
) -> Result<String, HyperbytedbError> {
    translate_inner_ordered(stmt, from_source, mapping, series, time_bounds, true)
}

fn translate_inner_ordered(
    stmt: &SelectStatement,
    from_source: &str,
    mapping: Option<&ColumnMapping>,
    series: Option<SeriesJoin<'_>>,
    time_bounds: Option<(Option<i64>, Option<i64>)>,
    ordered: bool,
) -> Result<String, HyperbytedbError> {
    // Every computed column gets its InfluxQL name before anything reads it,
    // so `mean(a), mean(b)` is `mean`, `mean_1` in the SQL and the merge alike.
    let named = with_output_names(stmt);
    let stmt = &named;
    if let Some(sel) = super::selectors::multi_selector(stmt)? {
        return super::selectors::translate_multi_selector(
            stmt,
            &sel,
            from_source,
            mapping,
            series,
        );
    }
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
    // As InfluxDB: raw points cannot be bucketed. ClickHouse would reject the
    // SQL with NOT_AN_AGGREGATE, which surfaced as a chDB error.
    if has_group_by_time && !has_aggregate {
        return Err(HyperbytedbError::QueryParse(
            "GROUP BY requires at least one aggregate function".to_string(),
        ));
    }
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
    // A lone selector reports the selected point's own time, like InfluxDB;
    // other aggregates without GROUP BY time() get theirs from the caller.
    if !has_group_by_time && let Some(point_time) = lone_selector_time_sql(stmt, mapping)? {
        select_parts.insert(0, format!("{point_time} AS __time"));
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
    let source = build_from_source(from_source, series, mapping, stmt);
    write!(out, "\nFROM {}", source.from)?;

    // WHERE
    if let Some(ref cond) = source.outer_condition {
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
    let has_orderable_time = ordered
        && (time_col.is_some() || (!has_aggregate && !has_group_by_time) || projects_point_time);
    let time_desc = stmt.order_by.as_ref().is_some_and(|o| o.time_desc);
    let do_fill = ordered && needs_with_fill && time_col.is_some();
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

/// `sub_sql` (the translation of `sub`) as an outer query's FROM source,
/// with a `time` column as InfluxDB gives a subquery's points: the bucket
/// for GROUP BY time(), the selected point's time for a lone selector, else
/// the WHERE clause's lower bound (`time_min`, epoch 0 without one).
///
/// Renaming the `__time` alias to `time` inside `sub_sql` made ClickHouse
/// read `time` in its GROUP BY as the bucket, not the column, so every
/// subquery over an aggregate failed (NOT_AN_AGGREGATE, or no `time`).
pub fn subquery_source(
    sub: &SelectStatement,
    sub_sql: &str,
    time_min: Option<i64>,
    mapping: Option<&ColumnMapping>,
) -> Result<String, HyperbytedbError> {
    let named = with_output_names(sub);
    let has_group_by_time = named
        .group_by
        .as_ref()
        .and_then(|gb| gb.time_dimension())
        .is_some();
    let has_star = named
        .fields
        .iter()
        .any(|f| matches!(f.expr, Expr::Star | Expr::Wildcard));
    // TOP/BOTTOM/SAMPLE keep their points' own `time`.
    if super::selectors::multi_selector(&named)?.is_some() {
        return Ok(format!("({sub_sql})"));
    }
    if has_group_by_time || lone_selector_time_sql(&named, mapping)?.is_some() {
        return Ok(format!(
            "(SELECT * EXCEPT (`__time`), `__time` AS `time` FROM ({sub_sql}))"
        ));
    }
    if select_has_true_aggregate(&named) && !has_star {
        let stamp = nanos_to_ch_timestamp(time_min.unwrap_or(0));
        return Ok(format!("(SELECT *, {stamp} AS `time` FROM ({sub_sql}))"));
    }
    // Raw points (and per-point transforms) carry their own `time`.
    Ok(format!("({sub_sql})"))
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
) -> FromSource {
    let pushed = mapping.map(|m| pushdown_filter(stmt, m, series.as_ref()));
    let unchanged = |from: String| FromSource {
        from,
        outer_condition: stmt.condition.clone(),
    };
    let fact = match mapping {
        Some(m) => build_coalesced_fact_view_filtered(
            &QuotedTableName::new_quoted(fact_table.to_string()),
            m,
            pushed.as_ref().and_then(|p| p.fact.as_deref()),
        ),
        None => fact_table.to_string(),
    };
    let (Some(sj), Some(m)) = (series, mapping) else {
        return unchanged(fact);
    };
    if !sj.force && !query_references_tag(stmt, m) {
        return unchanged(fact);
    }
    // Every tag the query uses was pushed into the fact filter as a series
    // lookup, and none is output or grouped by: the join would only add
    // columns nobody reads. On n3 data it was 20 of a one-series last()'s
    // 33 ms. The pushed terms leave the outer WHERE with it, since their
    // columns are not there; the series lookup already applied them exactly.
    if let Some(p) = pushed.as_ref().filter(|p| !p.tag_terms.is_empty())
        && !sj.force
        && !output_references_tag(stmt, m)
    {
        let remaining = condition_without_terms(stmt.condition.as_ref(), &p.tag_terms);
        if !remaining
            .as_ref()
            .is_some_and(|c| expr_references_tag(c, m))
        {
            return FromSource {
                from: fact,
                outer_condition: remaining,
            };
        }
    }
    let mut tag_cols: Vec<String> = m
        .tag_keys
        .iter()
        .map(|t| m.physical_tag_column_name(t))
        .collect();
    if tag_cols.is_empty() {
        return unchanged(fact);
    }
    // Only project tag columns that actually exist in the series table.
    // MV destinations may have a subset of source tags (GROUP BY columns only).
    if !sj.tag_columns.is_empty() {
        tag_cols.retain(|c| sj.tag_columns.contains(c));
    }
    if tag_cols.is_empty() {
        return unchanged(fact);
    }
    tag_cols.sort();
    let projected = tag_cols
        .iter()
        .map(|c| format!("s.{}", quote_phys_identifier(c)))
        .collect::<Vec<_>>()
        .join(", ");
    // With a tag selector pushed, every result row satisfies it, so the join
    // needs only the selected series. Joining the whole series table built a
    // hash of every series on the node for each region query: ~110 ms of CPU
    // for a one-series last() on n3.
    let series_side = match pushed.as_ref().and_then(|p| p.series.as_deref()) {
        Some(selector) => format!("(SELECT * FROM {} WHERE {selector})", sj.table),
        None => sj.table.to_string(),
    };
    unchanged(format!(
        "(SELECT t.*, {projected} FROM {fact} AS t ANY LEFT JOIN {series_side} AS s ON t.`series_id` = s.`series_id`)",
    ))
}

/// The FROM source and the condition the outer WHERE should apply to it.
pub(super) struct FromSource {
    pub from: String,
    pub outer_condition: Option<Expr>,
}

/// Whether the query outputs or groups by a tag (`SELECT *` includes tags).
fn output_references_tag(stmt: &SelectStatement, m: &ColumnMapping) -> bool {
    stmt.group_by
        .as_ref()
        .is_some_and(|gb| gb.references_tags())
        || stmt.fields.iter().any(|f| {
            matches!(f.expr, Expr::Star | Expr::Wildcard) || expr_references_tag(&f.expr, m)
        })
}

/// `cond` without its top-level AND terms at `drop` (indices in
/// [`and_terms`] order), or None when nothing is left.
fn condition_without_terms(cond: Option<&Expr>, drop: &[usize]) -> Option<Expr> {
    let mut terms = Vec::new();
    and_terms(cond?, &mut terms);
    terms
        .into_iter()
        .enumerate()
        .filter(|(i, _)| !drop.contains(i))
        .map(|(_, t)| t.clone())
        .reduce(|left, right| {
            Expr::BinaryExpr(Box::new(BinaryExpr {
                left,
                op: BinaryOp::And,
                right,
            }))
        })
}

/// Filters [`pushdown_filter`] moves ahead of the coalesce and the join.
struct Pushdown {
    /// For the fact rows, before they are grouped.
    fact: Option<String>,
    /// The pushed tag selectors, for the series table.
    series: Option<String>,
    /// Indices (in [`and_terms`] order) of the condition terms the series
    /// lookup applies exactly.
    tag_terms: Vec<usize>,
}
/// The part of the WHERE clause that can run on fact rows before they are
/// coalesced: top-level AND terms on `time` alone, and tag selectors (see
/// [`tag_selector`]), turned into a `series_id` lookup in the series table.
///
/// Without this every query coalesced the whole fact table and joined every
/// series before applying its WHERE: one series' `last` took as long as a
/// count over 90M points (~1.2 s on n3). The outer WHERE still applies in
/// full, so pushing a subset of terms never changes the result. Only
/// selectors that cannot match '' are pushed: a fact row whose series row is
/// missing reads its tags as '' through the LEFT JOIN, which `host = ''` or
/// `host != 'x'` would match but a series-table lookup would not.
fn pushdown_filter(
    stmt: &SelectStatement,
    m: &ColumnMapping,
    series: Option<&SeriesJoin<'_>>,
) -> Pushdown {
    let Some(cond) = stmt.condition.as_ref() else {
        return Pushdown {
            fact: None,
            series: None,
            tag_terms: Vec::new(),
        };
    };
    let mut terms = Vec::new();
    and_terms(cond, &mut terms);
    let mut time_parts = Vec::new();
    let mut tag_parts = Vec::new();
    let mut tag_terms = Vec::new();
    for (index, term) in terms.into_iter().enumerate() {
        let mut sql = String::new();
        if is_time_only_comparison(term) {
            if translate_expr(term, &mut sql, true, Some(m)).is_ok() {
                time_parts.push(sql);
            }
        } else if let Some(sj) = series {
            let mut tags = Vec::new();
            if tag_selector(term, m, &mut tags)
                && tags.iter().all(|t| {
                    sj.tag_columns.is_empty()
                        || sj.tag_columns.contains(&m.physical_tag_column_name(t))
                })
                && translate_expr(term, &mut sql, true, Some(m)).is_ok()
            {
                tag_parts.push(sql);
                tag_terms.push(index);
            }
        }
    }
    let selector = (!tag_parts.is_empty()).then(|| tag_parts.join(" AND "));
    if let (Some(sj), Some(sel)) = (series, &selector) {
        time_parts.push(format!(
            "`series_id` IN (SELECT `series_id` FROM {} WHERE {sel})",
            sj.table
        ));
    }
    Pushdown {
        fact: (!time_parts.is_empty()).then(|| time_parts.join(" AND ")),
        series: selector,
        tag_terms,
    }
}

/// Flatten top-level `AND`s into their terms.
fn and_terms<'e>(e: &'e Expr, out: &mut Vec<&'e Expr>) {
    match e {
        Expr::BinaryExpr(be) if be.op == BinaryOp::And => {
            and_terms(&be.left, out);
            and_terms(&be.right, out);
        }
        other => out.push(other),
    }
}

/// A comparison whose only column is `time`, e.g. `time >= now() - 1h`.
fn is_time_only_comparison(e: &Expr) -> bool {
    fn constant_or_time(e: &Expr, saw_time: &mut bool) -> bool {
        match e {
            Expr::Identifier(n) | Expr::FieldRef { name: n, .. } => {
                let is_time = n.eq_ignore_ascii_case("time");
                *saw_time |= is_time;
                is_time
            }
            Expr::StringLiteral(_)
            | Expr::IntegerLiteral(_)
            | Expr::FloatLiteral(_)
            | Expr::DurationLiteral(_)
            | Expr::TimeLiteral(_)
            | Expr::Now => true,
            Expr::UnaryExpr(UnaryOp::Neg, inner) => constant_or_time(inner, saw_time),
            Expr::BinaryExpr(be) if matches!(be.op, BinaryOp::Add | BinaryOp::Sub) => {
                constant_or_time(&be.left, saw_time) && constant_or_time(&be.right, saw_time)
            }
            _ => false,
        }
    }
    let Expr::BinaryExpr(be) = e else {
        return false;
    };
    if !matches!(
        be.op,
        BinaryOp::Eq | BinaryOp::Lt | BinaryOp::Lte | BinaryOp::Gt | BinaryOp::Gte
    ) {
        return false;
    }
    let mut saw_time = false;
    constant_or_time(&be.left, &mut saw_time)
        && constant_or_time(&be.right, &mut saw_time)
        && saw_time
}

/// Whether `e` selects series by tag values alone, matching no series whose
/// tags are all '' (how a fact row with a missing series row reads through
/// the LEFT JOIN): `tag = '<non-empty>'`, `tag =~ /re/` for a regex that
/// cannot match '', or an OR of those. Grafana's multi-value variables send
/// `host =~ /^(a|b|c)$/`. Pushes the names of the tags involved onto `tags`.
fn tag_selector<'e>(e: &'e Expr, m: &ColumnMapping, tags: &mut Vec<&'e str>) -> bool {
    let Expr::BinaryExpr(be) = e else {
        return false;
    };
    let tag_name = |x: &'e Expr| match x {
        Expr::Identifier(n)
        | Expr::FieldRef {
            name: n,
            typ: None | Some(FieldType::Tag),
        } if m.tag_keys.contains(n) && !m.field_names.contains(n) => Some(n.as_str()),
        _ => None,
    };
    match be.op {
        BinaryOp::Or => tag_selector(&be.left, m, tags) && tag_selector(&be.right, m, tags),
        BinaryOp::Eq => {
            let non_empty = |x: &Expr| matches!(x, Expr::StringLiteral(v) if !v.is_empty());
            let tag = match (tag_name(&be.left), tag_name(&be.right)) {
                (Some(t), None) if non_empty(&be.right) => t,
                (None, Some(t)) if non_empty(&be.left) => t,
                _ => return false,
            };
            tags.push(tag);
            true
        }
        BinaryOp::RegexMatch => {
            let (Some(tag), Expr::Regex(pattern)) = (tag_name(&be.left), &be.right) else {
                return false;
            };
            // Unparseable here: do not risk it.
            if !regex::Regex::new(pattern).is_ok_and(|r| !r.is_match("")) {
                return false;
            }
            tags.push(tag);
            true
        }
        _ => false,
    }
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

/// InfluxQL's name for a field without an alias (`Field.Name()` in InfluxDB):
/// a call is named after its function (`mean("v")` → `mean`), an expression
/// after the fields and calls in it joined by `_` (`"a" + "b"` → `a_b`,
/// `mean("v") * 2` → `mean`), a field reference after the field.
///
/// A bare field reference gets no alias: its column already has that name.
pub(super) fn default_field_alias(expr: &Expr) -> Option<String> {
    if matches!(expr, Expr::Identifier(_) | Expr::FieldRef { .. }) {
        return None;
    }
    influxql_name(expr)
}

fn influxql_name(expr: &Expr) -> Option<String> {
    let mut names = Vec::new();
    expr_name_parts(expr, &mut names);
    (!names.is_empty()).then(|| names.join("_"))
}

fn expr_name_parts(expr: &Expr, names: &mut Vec<String>) {
    match expr {
        Expr::Call(func) => names.push(func.name.to_lowercase()),
        Expr::Identifier(name) | Expr::FieldRef { name, .. } => names.push(name.clone()),
        Expr::BinaryExpr(be) => {
            expr_name_parts(&be.left, names);
            expr_name_parts(&be.right, names);
        }
        Expr::UnaryExpr(_, inner) => expr_name_parts(inner, names),
        _ => {}
    }
}

/// The pre-InfluxQL default column name: `mean("v")` → `mean_v`. Only for
/// materialized views stored before aliases were required, whose columns were
/// created under these names.
#[must_use]
pub fn legacy_field_alias(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Call(func) => {
            let base = func.name.to_lowercase();
            if let Some(Expr::Identifier(field_name)) = func.args.first() {
                Some(format!("{base}_{field_name}"))
            } else {
                Some(base)
            }
        }
        _ => None,
    }
}

/// `stmt` with InfluxQL column names on every computed field, as InfluxDB's
/// `SelectStatement.ColumnNames`: aliases are kept; other fields take their
/// default name, and a name already taken gets `_1`, `_2`, ...
///
/// Plain field references keep no alias (their column is the field itself)
/// but still take their name. Idempotent.
#[must_use]
pub fn with_output_names(stmt: &SelectStatement) -> SelectStatement {
    let mut out = stmt.clone();
    let mut taken: std::collections::HashMap<String, usize> = out
        .fields
        .iter()
        .filter_map(|f| f.alias.clone())
        .map(|a| (a, 1))
        .collect();
    for field in &mut out.fields {
        if field.alias.is_some() {
            continue;
        }
        let Some(name) = influxql_name(&field.expr) else {
            continue;
        };
        let plain = matches!(field.expr, Expr::Identifier(_) | Expr::FieldRef { .. });
        let resolved = match taken.get(&name).copied() {
            None => name.clone(),
            Some(mut n) => loop {
                let candidate = format!("{name}_{n}");
                if !taken.contains_key(&candidate) {
                    taken.insert(name.clone(), n + 1);
                    break candidate;
                }
                n += 1;
            },
        };
        *taken.entry(resolved.clone()).or_insert(0) += 1;
        if !plain || resolved != name {
            field.alias = Some(resolved);
        }
    }
    out
}

/// Selectors whose result is one stored point, so InfluxDB reports its time.
fn point_selector(name: &str) -> bool {
    matches!(
        name.to_uppercase().as_str(),
        "FIRST" | "LAST" | "MAX" | "MIN"
    )
}

/// The selected point's time for a statement whose only call is a bare
/// FIRST/LAST/MAX/MIN, or None.
fn lone_selector_time_sql(
    stmt: &SelectStatement,
    mapping: Option<&ColumnMapping>,
) -> Result<Option<String>, HyperbytedbError> {
    let calls: Vec<&FunctionCall> = stmt
        .fields
        .iter()
        .filter(|f| expr_contains_call(&f.expr))
        .filter_map(|f| match &f.expr {
            Expr::Call(c) => Some(c),
            _ => None,
        })
        .collect();
    let call_fields = stmt
        .fields
        .iter()
        .filter(|f| expr_contains_call(&f.expr))
        .count();
    let [func] = calls.as_slice() else {
        return Ok(None);
    };
    if call_fields != 1 || !point_selector(&func.name) || func.args.len() != 1 {
        return Ok(None);
    }
    let arg = super::aggregates::translate_aggregate_arg(&func.args[0], mapping)?;
    Ok(Some(match func.name.to_uppercase().as_str() {
        "FIRST" => "min(time)".to_string(),
        "LAST" => "max(time)".to_string(),
        "MAX" => format!("argMax(time, {arg})"),
        _ => format!("argMin(time, {arg})"),
    }))
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
