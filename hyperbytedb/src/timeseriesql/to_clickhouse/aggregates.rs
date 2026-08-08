use crate::domain::column_mapping::ColumnMapping;
use crate::error::HyperbytedbError;
use crate::timeseriesql::ast::*;

use super::conditions::{binary_op_to_clickhouse, format_float, quote_phys_identifier};

use super::select::translate_field_expr;

pub(super) fn is_window_transform_call(name: &str) -> bool {
    matches!(
        name.to_ascii_uppercase().as_str(),
        "DERIVATIVE"
            | "NON_NEGATIVE_DERIVATIVE"
            | "DIFFERENCE"
            | "NON_NEGATIVE_DIFFERENCE"
            | "MOVING_AVERAGE"
            | "CUMULATIVE_SUM"
            | "ELAPSED"
    )
}

/// Whether an expression contains a row-collapsing aggregate. Window transforms
/// only count when they wrap a nested aggregate (e.g. `difference(mean(v))`).
pub(super) fn expr_contains_aggregate(expr: &Expr) -> bool {
    match expr {
        Expr::Call(fc) if is_window_transform_call(&fc.name) => {
            fc.args.first().is_some_and(|a| matches!(a, Expr::Call(_)))
        }
        Expr::Call(_) => true,
        Expr::BinaryExpr(be) => {
            expr_contains_aggregate(&be.left) || expr_contains_aggregate(&be.right)
        }
        Expr::UnaryExpr(_, e) => expr_contains_aggregate(e),
        _ => false,
    }
}

/// Whether an expression contains a window transform applied directly to a raw
/// field (no nested aggregate) — a per-point transform.
pub(super) fn expr_contains_raw_transform(expr: &Expr) -> bool {
    match expr {
        Expr::Call(fc) if is_window_transform_call(&fc.name) => {
            !fc.args.first().is_some_and(|a| matches!(a, Expr::Call(_)))
        }
        Expr::Call(_) => false,
        Expr::BinaryExpr(be) => {
            expr_contains_raw_transform(&be.left) || expr_contains_raw_transform(&be.right)
        }
        Expr::UnaryExpr(_, e) => expr_contains_raw_transform(e),
        _ => false,
    }
}
pub(super) fn translate_binary_expr(
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

pub(super) fn translate_aggregate_call(
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
                let sum_q = quote_phys_identifier(&mean_def.sum_col);
                let count_q = quote_phys_identifier(&mean_def.count_col);
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
            // InfluxQL median averages the two middle values on even counts;
            // quantileExactInclusive(0.5) matches that exactly (ClickHouse
            // `median` is sampling-based and approximate).
            wrap_fill(format!("quantileExactInclusive(0.5)({})", f))
        }
        "COUNT" => {
            let arg = get_single_arg(func, "COUNT")?;
            // count(distinct("v")) → exact distinct count.
            if let Expr::Call(inner) = arg
                && inner.name.eq_ignore_ascii_case("distinct")
            {
                let inner_arg = get_single_arg(inner, "DISTINCT")?;
                let f = translate_aggregate_arg(inner_arg, mapping)?;
                wrap_fill(format!("uniqExact({})", f))
            } else {
                let f = translate_aggregate_arg(arg, mapping)?;
                wrap_fill(format!("count({})", f))
            }
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
            // InfluxQL percentile is nearest-rank and returns an actual sample
            // (for [10,20,30,40] p50 = 20); quantileExactLow matches that.
            wrap_fill(format!("quantileExactLow({})({})", format_float(pct), f))
        }
        "SPREAD" => {
            let arg = get_single_arg(func, "SPREAD")?;
            let f = translate_aggregate_arg(arg, mapping)?;
            wrap_fill(format!("(max({}) - min({}))", f, f))
        }
        "STDDEV" => {
            let arg = get_single_arg(func, "STDDEV")?;
            let f = translate_aggregate_arg(arg, mapping)?;
            // InfluxQL stddev is the *sample* standard deviation.
            wrap_fill(format!("stddevSamp({})", f))
        }
        "MODE" => {
            let arg = get_single_arg(func, "MODE")?;
            let f = translate_aggregate_arg(arg, mapping)?;
            // topKWeighted returns an Array; unwrap to a scalar. Still
            // approximate and tie-breaking is unspecified, unlike InfluxQL's
            // lowest-value tie-break.
            wrap_fill(format!("arrayElement(topKWeighted(1)({}, 1), 1)", f))
        }
        "DISTINCT" => {
            let arg = get_single_arg(func, "DISTINCT")?;
            let f = translate_aggregate_arg(arg, mapping)?;
            // arrayJoin(groupUniqArray(...)) yields one row per distinct value
            // and — unlike `SELECT DISTINCT` — stays valid inside GROUP BY time().
            format!("arrayJoin(groupUniqArray({}))", f)
        }
        "DERIVATIVE" | "NON_NEGATIVE_DERIVATIVE" => {
            let field_arg = get_single_arg(func, &name_upper)?;
            let f = translate_field_or_nested(field_arg, group_by, mapping)?;
            let window = build_window_clause(group_by, mapping)?;
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
            let window = build_window_clause(group_by, mapping)?;
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
                            .map(|m| m.physical_tag_column_name(t))
                            .unwrap_or_else(|| t.to_string());
                        Ok(quote_phys_identifier(&phys))
                    })
                    .collect::<Result<Vec<String>, HyperbytedbError>>()?
                    .join(", ");
                format!("PARTITION BY {p} ")
            };
            // InfluxQL emits moving_average values only once the window holds N
            // points; gate on the frame's non-null count so shorter leading
            // frames yield NULL (filtered for per-point transforms).
            let frame = format!(
                "({partition_clause}ORDER BY {time_ref} ROWS BETWEEN {preceding} PRECEDING AND CURRENT ROW)",
                preceding = n - 1
            );
            format!("if(count({f}) OVER {frame} >= {n}, avg({f}) OVER {frame}, NULL)")
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
                            .map(|m| m.physical_tag_column_name(t))
                            .unwrap_or_else(|| t.to_string());
                        Ok(quote_phys_identifier(&phys))
                    })
                    .collect::<Result<Vec<String>, HyperbytedbError>>()?
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
            let window = build_window_clause(group_by, mapping)?;
            let unit_nanos: i64 = if func.args.len() >= 2 {
                match &func.args[1] {
                    Expr::DurationLiteral(d) => d.to_nanos(),
                    _ => 1_000_000_000,
                }
            } else {
                1_000_000_000
            };
            let unit_seconds = format_float(unit_nanos as f64 / 1_000_000_000.0);
            // toNullable: lagInFrame on the non-Nullable time column would
            // default to epoch 0 out-of-frame, making the first row a huge
            // elapsed value instead of NULL (InfluxQL omits the first point).
            format!(
                "((toFloat64({time_ref}) - toFloat64(lagInFrame(toNullable({time_ref}), 1) {window})) / {unit_seconds})"
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

pub(super) fn translate_aggregate_arg(
    expr: &Expr,
    mapping: Option<&ColumnMapping>,
) -> Result<String, HyperbytedbError> {
    match expr {
        Expr::Identifier(name) | Expr::FieldRef { name, .. } => {
            let col = mapping
                .map(|m| m.physical_select_identifier(name))
                .unwrap_or_else(|| name.clone());
            Ok(quote_phys_identifier(&col))
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
pub(super) fn translate_field_or_nested(
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
pub(super) fn window_time_ref(group_by: Option<&GroupBy>) -> &'static str {
    if group_by.and_then(|gb| gb.time_dimension()).is_some() {
        "__time"
    } else {
        "time"
    }
}

/// Build the OVER (...) window clause for transform functions.
/// Includes PARTITION BY for GROUP BY tag dimensions so that window
/// functions (lagInFrame, etc.) operate within each series independently.
pub(super) fn build_window_clause(
    group_by: Option<&GroupBy>,
    mapping: Option<&ColumnMapping>,
) -> Result<String, HyperbytedbError> {
    let time_ref = window_time_ref(group_by);
    let partition_tags: Vec<&str> = group_by.map(|gb| gb.tag_dimensions()).unwrap_or_default();

    if partition_tags.is_empty() {
        Ok(format!("OVER (ORDER BY {time_ref})"))
    } else {
        let partition = partition_tags
            .iter()
            .map(|t| {
                let phys = mapping
                    .map(|m| m.physical_tag_column_name(t))
                    .unwrap_or_else(|| t.to_string());
                Ok(quote_phys_identifier(&phys))
            })
            .collect::<Result<Vec<String>, HyperbytedbError>>()?
            .join(", ");
        Ok(format!(
            "OVER (PARTITION BY {partition} ORDER BY {time_ref})"
        ))
    }
}

pub(super) fn get_single_arg<'a>(
    func: &'a FunctionCall,
    name: &str,
) -> Result<&'a Expr, HyperbytedbError> {
    func.args.first().ok_or_else(|| {
        HyperbytedbError::QueryParse(format!("{} requires exactly one argument", name))
    })
}

pub(super) fn get_two_args<'a>(
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
