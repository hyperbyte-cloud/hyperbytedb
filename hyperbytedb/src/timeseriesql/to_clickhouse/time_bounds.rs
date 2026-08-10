use crate::timeseriesql::ast::{BinaryExpr, BinaryOp, Expr};

/// Extract `(min_time_nanos, max_time_nanos)` from a WHERE clause for WITH FILL anchoring.
pub fn extract_time_bounds(condition: Option<&Expr>) -> (Option<i64>, Option<i64>) {
    condition.map_or((None, None), bounds_for_expr)
}

/// Bounds for a single OR-free expression tree (AND of time comparisons).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct TimeBounds {
    min: Option<i64>,
    max: Option<i64>,
}

impl TimeBounds {
    fn intersect(self, other: Self) -> Self {
        Self {
            min: match (self.min, other.min) {
                (Some(a), Some(b)) => Some(a.max(b)),
                (Some(a), None) => Some(a),
                (None, Some(b)) => Some(b),
                (None, None) => None,
            },
            max: match (self.max, other.max) {
                (Some(a), Some(b)) => Some(a.min(b)),
                (Some(a), None) => Some(a),
                (None, Some(b)) => Some(b),
                (None, None) => None,
            },
        }
    }

    fn has_time_constraint(self) -> bool {
        self.min.is_some() || self.max.is_some()
    }
}

fn bounds_for_expr(expr: &Expr) -> (Option<i64>, Option<i64>) {
    let disjuncts = collect_or_disjuncts(expr);
    let mut disjunct_bounds = Vec::with_capacity(disjuncts.len());

    for disjunct in disjuncts {
        let b = bounds_for_and_tree(&disjunct);
        if !b.has_time_constraint() {
            // e.g. `host = 'x'` with no time predicate — time is unbounded on this branch.
            return (None, None);
        }
        disjunct_bounds.push(b);
    }

    if disjunct_bounds.is_empty() {
        return (None, None);
    }

    // OR envelope: only anchor a bound when every disjunct defines that side.
    // A disjunct with `time >= N` but no upper cap must not tighten max from
    // another disjunct; likewise for missing lower caps.
    let min = if disjunct_bounds.iter().all(|b| b.min.is_some()) {
        disjunct_bounds.iter().filter_map(|b| b.min).min()
    } else {
        None
    };
    let max = if disjunct_bounds.iter().all(|b| b.max.is_some()) {
        disjunct_bounds.iter().filter_map(|b| b.max).max()
    } else {
        None
    };

    (min, max)
}

fn collect_or_disjuncts(expr: &Expr) -> Vec<Expr> {
    match expr {
        Expr::BinaryExpr(be) if be.op == BinaryOp::Or => {
            let mut out = collect_or_disjuncts(&be.left);
            out.extend(collect_or_disjuncts(&be.right));
            out
        }
        other => vec![other.clone()],
    }
}

fn bounds_for_and_tree(expr: &Expr) -> TimeBounds {
    match expr {
        Expr::BinaryExpr(be) if be.op == BinaryOp::And => {
            bounds_for_and_tree(&be.left).intersect(bounds_for_and_tree(&be.right))
        }
        Expr::BinaryExpr(be) if is_time_epoch_comparison(be) => bounds_from_comparison(be),
        _ => TimeBounds::default(),
    }
}

fn bounds_from_comparison(be: &BinaryExpr) -> TimeBounds {
    let (time_is_left, epoch_expr) = if is_time_identifier(&be.left) {
        (true, &be.right)
    } else {
        (false, &be.left)
    };

    let nanos = match epoch_expr {
        Expr::DurationLiteral(d) => d.to_nanos(),
        Expr::IntegerLiteral(n) => *n,
        _ => return TimeBounds::default(),
    };

    let effective_op = if time_is_left {
        be.op.clone()
    } else {
        match be.op {
            BinaryOp::Gt => BinaryOp::Lt,
            BinaryOp::Gte => BinaryOp::Lte,
            BinaryOp::Lt => BinaryOp::Gt,
            BinaryOp::Lte => BinaryOp::Gte,
            ref other => other.clone(),
        }
    };

    let mut bounds = TimeBounds::default();
    match effective_op {
        BinaryOp::Gte | BinaryOp::Gt | BinaryOp::Eq => bounds.min = Some(nanos),
        _ => {}
    }
    match effective_op {
        BinaryOp::Lte | BinaryOp::Lt | BinaryOp::Eq => bounds.max = Some(nanos),
        _ => {}
    }
    bounds
}

pub(crate) fn is_time_identifier(expr: &Expr) -> bool {
    matches!(expr, Expr::Identifier(n) if n.to_lowercase() == "time")
}

/// Detect `time <cmp> <epoch_value>` where epoch_value is a duration or integer epoch.
pub(crate) fn is_time_epoch_comparison(be: &BinaryExpr) -> bool {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::timeseriesql::ast::Statement;
    use crate::timeseriesql::parser::parse_query;

    fn bounds(q: &str) -> (Option<i64>, Option<i64>) {
        let stmt = match parse_query(q).unwrap().remove(0) {
            Statement::Select(s) => s,
            _ => panic!("expected SELECT"),
        };
        extract_time_bounds(stmt.condition.as_ref())
    }

    #[test]
    fn intersects_anded_bounds() {
        let (min, max) = bounds(
            "SELECT * FROM m WHERE time >= 1000000000 AND time >= 3000000000 \
             AND time <= 9000000000 AND time <= 7000000000",
        );
        assert_eq!(min, Some(3_000_000_000));
        assert_eq!(max, Some(7_000_000_000));
    }

    #[test]
    fn or_of_bounded_ranges_uses_envelope() {
        let (min, max) = bounds(
            "SELECT * FROM m WHERE (time >= 100 AND time <= 500) OR (time >= 300 AND time <= 700)",
        );
        assert_eq!(min, Some(100));
        assert_eq!(max, Some(700));
    }

    #[test]
    fn or_with_time_unconstrained_branch_is_conservative() {
        let (min, max) = bounds("SELECT * FROM m WHERE time >= 100 OR \"host\" = 'x'");
        assert_eq!(min, None);
        assert_eq!(max, None);
    }

    #[test]
    fn or_of_lower_bounds_only() {
        let (min, max) = bounds("SELECT * FROM m WHERE time >= 100 OR time >= 300");
        assert_eq!(min, Some(100));
        assert_eq!(max, None);
    }

    #[test]
    fn or_partial_max_omitted_when_one_disjunct_unbounded_above() {
        let (min, max) = bounds(
            "SELECT * FROM m WHERE (time >= 100 AND \"host\" = 'x') \
             OR (time >= 300 AND time <= 700)",
        );
        assert_eq!(min, Some(100));
        assert_eq!(max, None);
    }

    #[test]
    fn or_partial_bounds_omitted_when_disjuncts_lack_opposite_side() {
        let (min, max) = bounds("SELECT * FROM m WHERE time <= 500 OR time >= 300");
        assert_eq!(min, None);
        assert_eq!(max, None);
    }
}
