//! Facts about an InfluxQL AST that hold whatever the query backend: output
//! column names, whether a SELECT aggregates, the TOP/BOTTOM/SAMPLE
//! selector, and the merge columns shared by the translator and the
//! sharded merge. The domain uses these; it does not use SQL generation.

use crate::timeseriesql::ast::{Expr, Field, SelectStatement};

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
pub fn default_field_alias(expr: &Expr) -> Option<String> {
    if matches!(expr, Expr::Identifier(_) | Expr::FieldRef { .. }) {
        return None;
    }
    influxql_name(expr)
}

pub fn influxql_name(expr: &Expr) -> Option<String> {
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

/// Whether any SELECT field contains a row-collapsing aggregate (not a bare window transform).
pub fn select_has_true_aggregate(stmt: &SelectStatement) -> bool {
    stmt.fields.iter().any(|f| expr_contains_aggregate(&f.expr))
}

pub fn is_window_transform_call(name: &str) -> bool {
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
pub fn expr_contains_aggregate(expr: &Expr) -> bool {
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
pub fn expr_contains_raw_transform(expr: &Expr) -> bool {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::timeseriesql::ast::Statement;

    fn select(q: &str) -> crate::timeseriesql::ast::SelectStatement {
        match crate::timeseriesql::parse(q).unwrap().remove(0) {
            Statement::Select(s) => s,
            _ => panic!("not a select"),
        }
    }

    #[test]
    fn output_names_follow_influxql() {
        let s = select(r#"SELECT mean("v") * 2, "a" + "b", "v", mean("v") AS m FROM t"#);
        assert_eq!(
            select_output_field_name(&s.fields[0]).as_deref(),
            Some("mean")
        );
        assert_eq!(
            select_output_field_name(&s.fields[1]).as_deref(),
            Some("a_b")
        );
        assert_eq!(select_output_field_name(&s.fields[2]), None);
        assert_eq!(select_output_field_name(&s.fields[3]).as_deref(), Some("m"));
    }

    #[test]
    fn window_transforms_are_not_aggregates() {
        assert!(!select_has_true_aggregate(&select(
            r#"SELECT difference("v") FROM m"#
        )));
        assert!(select_has_true_aggregate(&select(
            r#"SELECT mean("v") FROM m"#
        )));
    }

    #[test]
    fn merge_column_names_are_stable() {
        assert_eq!(BUCKET_COL, "__bucket");
        assert_eq!(TIME_NS_COL, "__tns");
    }
}
