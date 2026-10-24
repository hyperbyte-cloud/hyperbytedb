//! TOP, BOTTOM and SAMPLE: selectors that return up to N stored points per
//! group (GROUP BY time() bucket and tags), each at its own time, as InfluxDB.
//!
//! The SQL ranks points in a window per group and keeps the first N. It also
//! returns two hidden columns, [`BUCKET_COL`] and [`TIME_NS_COL`], so a
//! sharded query's coordinator can rank the regions' rows again
//! ([`crate::domain::sharding::multi_selector`]); they are removed before the
//! result is returned.

use std::fmt::Write;

use crate::domain::column_mapping::ColumnMapping;
use crate::error::HyperbytedbError;
use crate::timeseriesql::ast::*;

use super::SeriesJoin;
use super::aggregates::translate_aggregate_arg;
use super::conditions::{quote_identifier, translate_expr};
use super::select::{
    build_from_source, select_output_field_name, select_tag_column_sql, time_bucket_expr,
    translate_field, with_output_names,
};

/// Each row's group bucket, in nanoseconds (0 without GROUP BY time()).
pub const BUCKET_COL: &str = "__bucket";
/// Each row's point time, in nanoseconds.
pub const TIME_NS_COL: &str = "__tns";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SelectorKind {
    Top,
    Bottom,
    Sample,
}

/// A statement's TOP, BOTTOM or SAMPLE call, parsed.
#[derive(Debug, Clone)]
pub struct MultiSelector {
    pub kind: SelectorKind,
    /// Output column of the selected values (`top` unless aliased).
    pub output: String,
    /// The field ranked.
    pub field: String,
    /// TOP/BOTTOM's tag arguments: at most one point per combination.
    pub tags: Vec<String>,
    pub n: u64,
}

fn kind_of(name: &str) -> Option<SelectorKind> {
    match name.to_ascii_uppercase().as_str() {
        "TOP" => Some(SelectorKind::Top),
        "BOTTOM" => Some(SelectorKind::Bottom),
        "SAMPLE" => Some(SelectorKind::Sample),
        _ => None,
    }
}

fn contains_multi_selector(expr: &Expr) -> bool {
    match expr {
        Expr::Call(fc) => {
            kind_of(&fc.name).is_some() || fc.args.iter().any(contains_multi_selector)
        }
        Expr::BinaryExpr(be) => {
            contains_multi_selector(&be.left) || contains_multi_selector(&be.right)
        }
        Expr::UnaryExpr(_, e) => contains_multi_selector(e),
        _ => false,
    }
}

fn ident(expr: &Expr) -> Option<&str> {
    match expr {
        Expr::Identifier(n) | Expr::FieldRef { name: n, .. } => Some(n),
        _ => None,
    }
}

/// The statement's TOP, BOTTOM or SAMPLE, or None without one. Errors as
/// InfluxDB does when it is combined with other functions or misused.
pub fn multi_selector(stmt: &SelectStatement) -> Result<Option<MultiSelector>, HyperbytedbError> {
    if !stmt.fields.iter().any(|f| contains_multi_selector(&f.expr)) {
        return Ok(None);
    }
    let named = with_output_names(stmt);
    let mut found = None;
    for field in &named.fields {
        match &field.expr {
            Expr::Call(fc) if kind_of(&fc.name).is_some() && found.is_none() => {
                found = Some((fc, select_output_field_name(field)));
            }
            Expr::Identifier(_) | Expr::FieldRef { .. } => {}
            Expr::Call(fc) if kind_of(&fc.name).is_some() => {
                return Err(combined_error(&fc.name));
            }
            _ => {
                let name = named
                    .fields
                    .iter()
                    .find_map(|f| match &f.expr {
                        Expr::Call(fc) if kind_of(&fc.name).is_some() => Some(fc.name.clone()),
                        _ => None,
                    })
                    .unwrap_or_else(|| "top".to_string());
                return Err(combined_error(&name));
            }
        }
    }
    let Some((fc, output)) = found else {
        // Nested inside another expression, e.g. `top(v, 2) * 2`.
        return Err(HyperbytedbError::QueryParse(
            "top(), bottom() and sample() cannot be used inside an expression".to_string(),
        ));
    };
    let kind = kind_of(&fc.name).unwrap_or(SelectorKind::Top);
    let lname = fc.name.to_ascii_lowercase();
    let min_args = 2;
    if fc.args.len() < min_args || (kind == SelectorKind::Sample && fc.args.len() != 2) {
        return Err(HyperbytedbError::QueryParse(format!(
            "invalid number of arguments for {lname}, expected {}, got {}",
            if kind == SelectorKind::Sample {
                "2".to_string()
            } else {
                "at least 2".to_string()
            },
            fc.args.len()
        )));
    }
    let field = ident(&fc.args[0]).ok_or_else(|| {
        HyperbytedbError::QueryParse(format!(
            "expected first argument to be a field in {lname}(), found {:?}",
            fc.args[0]
        ))
    })?;
    let n = match fc.args.last() {
        Some(Expr::IntegerLiteral(n)) if *n >= 1 => *n as u64,
        Some(Expr::IntegerLiteral(n)) => {
            return Err(HyperbytedbError::QueryParse(format!(
                "limit ({n}) in {lname} function must be at least 1"
            )));
        }
        other => {
            return Err(HyperbytedbError::QueryParse(format!(
                "expected integer as last argument in {lname}(), found {other:?}"
            )));
        }
    };
    let tags = fc.args[1..fc.args.len() - 1]
        .iter()
        .map(|a| {
            ident(a).map(str::to_string).ok_or_else(|| {
                HyperbytedbError::QueryParse(format!(
                    "only fields or tags are allowed in {lname}(), found {a:?}"
                ))
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Some(MultiSelector {
        kind,
        output: output.unwrap_or(lname),
        field: field.to_string(),
        tags,
        n,
    }))
}

fn combined_error(name: &str) -> HyperbytedbError {
    HyperbytedbError::QueryParse(format!(
        "selector function {}() cannot be combined with other functions",
        name.to_ascii_lowercase()
    ))
}

/// The SQL for a statement with a TOP, BOTTOM or SAMPLE call `sel`.
pub(super) fn translate_multi_selector(
    stmt: &SelectStatement,
    sel: &MultiSelector,
    from_source: &str,
    mapping: Option<&ColumnMapping>,
    series: Option<SeriesJoin<'_>>,
) -> Result<String, HyperbytedbError> {
    let named = with_output_names(stmt);
    let stmt = &named;
    let tz = stmt.timezone.as_deref();
    let gb_tags: Vec<&str> = stmt
        .group_by
        .as_ref()
        .map(|gb| gb.tag_dimensions())
        .unwrap_or_default();
    let bucket = match stmt.group_by.as_ref().and_then(|gb| gb.time_dimension()) {
        Some(Dimension::Time { interval, offset }) => format!(
            "toUnixTimestamp64Nano(toDateTime64({}, 9))",
            time_bucket_expr(interval, offset.as_ref(), tz)
        ),
        _ => "toInt64(0)".to_string(),
    };
    let value = translate_aggregate_arg(&Expr::Identifier(sel.field.clone()), mapping)?;
    let out = quote_identifier(&sel.output)?;
    let time = "`time`";

    // Points, with the value under the selector's name and every column the
    // result needs.
    let mut cols = vec![
        time.to_string(),
        format!("toUnixTimestamp64Nano(toDateTime64({time}, 9)) AS `{TIME_NS_COL}`"),
        format!("{bucket} AS `{BUCKET_COL}`"),
        format!("{value} AS {out}"),
    ];
    let mut seen: Vec<String> = vec![sel.output.clone()];
    for tag in gb_tags
        .iter()
        .copied()
        .chain(sel.tags.iter().map(String::as_str))
    {
        if !seen.iter().any(|s| s == tag) {
            cols.push(select_tag_column_sql(tag, mapping)?);
            seen.push(tag.to_string());
        }
    }
    for field in &stmt.fields {
        if let Expr::Identifier(n) | Expr::FieldRef { name: n, .. } = &field.expr {
            let name = field.alias.clone().unwrap_or_else(|| n.clone());
            if n != "time" && !seen.contains(&name) {
                cols.push(translate_field(
                    field,
                    false,
                    0.0,
                    stmt.group_by.as_ref(),
                    mapping,
                )?);
                seen.push(name);
            }
        }
    }
    // Laid out as the other translations, so the region and tombstone
    // predicates spliced in later (by line: `\nWHERE `, and into the inner
    // query of a `SELECT * FROM (\n...\n) ` wrapper) filter the points
    // before they are ranked.
    let source = build_from_source(from_source, series, mapping, stmt);
    let mut points = format!("SELECT {}\nFROM {}", cols.join(", "), source.from);
    write!(points, "\nWHERE {value} IS NOT NULL")?;
    if let Some(cond) = source.outer_condition.as_ref() {
        points.push_str(" AND (");
        translate_expr(cond, &mut points, true, mapping)?;
        points.push(')');
    }

    let group: Vec<String> = std::iter::once(format!("`{BUCKET_COL}`"))
        .chain(
            gb_tags
                .iter()
                .map(|t| quote_identifier(t))
                .collect::<Result<Vec<_>, _>>()?,
        )
        .collect();
    let order = match sel.kind {
        SelectorKind::Top => format!("{out} DESC, {time} ASC"),
        SelectorKind::Bottom => format!("{out} ASC, {time} ASC"),
        SelectorKind::Sample => "rand64()".to_string(),
    };
    // TOP/BOTTOM's tag arguments: each combination's best point first.
    if !sel.tags.is_empty() {
        let mut per_tag = group.clone();
        for t in &sel.tags {
            let q = quote_identifier(t)?;
            if !per_tag.contains(&q) {
                per_tag.push(q);
            }
        }
        points = format!(
            "SELECT * FROM (\n{points}\n) QUALIFY row_number() OVER (PARTITION BY {} ORDER BY {order}) = 1",
            per_tag.join(", ")
        );
    }
    let mut sql = format!(
        "SELECT * FROM (\n{points}\n) QUALIFY row_number() OVER (PARTITION BY {} ORDER BY {order}) <= {}",
        group.join(", "),
        sel.n
    );

    // As a raw select: series by tag, each in time order. On the `) ` line:
    // the splicers look for clauses only inside the wrapper.
    let desc = stmt.order_by.as_ref().is_some_and(|o| o.time_desc);
    let mut order_by: Vec<String> = gb_tags
        .iter()
        .map(|t| Ok(format!("{} ASC", quote_identifier(t)?)))
        .collect::<Result<_, HyperbytedbError>>()?;
    order_by.push(format!("{time} {}", if desc { "DESC" } else { "ASC" }));
    write!(sql, " ORDER BY {}", order_by.join(", "))?;
    if let Some(limit) = stmt.limit {
        let offset = stmt.offset.unwrap_or(0);
        if gb_tags.is_empty() {
            write!(sql, " LIMIT {offset}, {limit}")?;
        } else {
            let by = gb_tags
                .iter()
                .map(|t| quote_identifier(t))
                .collect::<Result<Vec<_>, _>>()?
                .join(", ");
            write!(sql, " LIMIT {offset}, {limit} BY ({by})")?;
        }
    } else if let Some(offset) = stmt.offset {
        write!(sql, " OFFSET {offset}")?;
    }
    Ok(sql)
}
