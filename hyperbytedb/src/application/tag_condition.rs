//! A SHOW statement's WHERE, evaluated against one series' tags.
//!
//! `SHOW TAG VALUES ... WHERE "host" = 'a'` ignored its WHERE and listed
//! every value: Grafana's chained template variables (the CPUs of the chosen
//! host) got every CPU of every host. Conditions on tags are matched against
//! each series' tag set; a time condition matches every series, as in
//! InfluxDB's series index. A missing tag reads as the empty string, so
//! `"region" = ''` selects series without one, as in InfluxDB.

use std::collections::BTreeMap;

use crate::error::HyperbytedbError;
use crate::timeseriesql::ast::{BinaryExpr, BinaryOp, Expr, FieldType};

/// A compiled condition: regexes are built once, not per series.
#[derive(Debug)]
pub enum TagPredicate {
    True,
    And(Box<TagPredicate>, Box<TagPredicate>),
    Or(Box<TagPredicate>, Box<TagPredicate>),
    Equals {
        key: String,
        value: String,
        negate: bool,
    },
    Matches {
        key: String,
        regex: regex::Regex,
        negate: bool,
    },
}

impl TagPredicate {
    pub fn compile(expr: &Expr) -> Result<Self, HyperbytedbError> {
        let Expr::BinaryExpr(b) = expr else {
            return Err(unsupported(expr));
        };
        let BinaryExpr { left, op, right } = b.as_ref();
        match op {
            BinaryOp::And => Ok(Self::And(
                Box::new(Self::compile(left)?),
                Box::new(Self::compile(right)?),
            )),
            BinaryOp::Or => Ok(Self::Or(
                Box::new(Self::compile(left)?),
                Box::new(Self::compile(right)?),
            )),
            _ if is_time(left) || is_time(right) => Ok(Self::True),
            BinaryOp::Eq | BinaryOp::Neq => {
                let (key, value) = match (tag_name(left), string_literal(right)) {
                    (Some(k), Some(v)) => (k, v),
                    _ => match (tag_name(right), string_literal(left)) {
                        (Some(k), Some(v)) => (k, v),
                        _ => return Err(unsupported(expr)),
                    },
                };
                Ok(Self::Equals {
                    key,
                    value,
                    negate: *op == BinaryOp::Neq,
                })
            }
            BinaryOp::RegexMatch | BinaryOp::RegexNotMatch => {
                let (Some(key), Expr::Regex(pattern)) = (tag_name(left), right) else {
                    return Err(unsupported(expr));
                };
                let regex = regex::Regex::new(pattern).map_err(|e| {
                    HyperbytedbError::QueryParse(format!("invalid regex /{pattern}/: {e}"))
                })?;
                Ok(Self::Matches {
                    key,
                    regex,
                    negate: *op == BinaryOp::RegexNotMatch,
                })
            }
            _ => Err(unsupported(expr)),
        }
    }

    #[must_use]
    pub fn matches(&self, tags: &BTreeMap<String, String>) -> bool {
        let tag = |key: &str| tags.get(key).map_or("", String::as_str);
        match self {
            Self::True => true,
            Self::And(a, b) => a.matches(tags) && b.matches(tags),
            Self::Or(a, b) => a.matches(tags) || b.matches(tags),
            Self::Equals { key, value, negate } => (tag(key) == value) != *negate,
            Self::Matches { key, regex, negate } => regex.is_match(tag(key)) != *negate,
        }
    }
}

fn is_time(e: &Expr) -> bool {
    matches!(e, Expr::Identifier(n) | Expr::FieldRef { name: n, .. } if n.eq_ignore_ascii_case("time"))
}

/// A tag reference: a plain identifier or one cast `::tag`.
fn tag_name(e: &Expr) -> Option<String> {
    match e {
        Expr::Identifier(n) => Some(n.clone()),
        Expr::FieldRef {
            name,
            typ: None | Some(FieldType::Tag),
        } => Some(name.clone()),
        _ => None,
    }
}

fn string_literal(e: &Expr) -> Option<String> {
    match e {
        Expr::StringLiteral(s) => Some(s.clone()),
        _ => None,
    }
}

fn unsupported(expr: &Expr) -> HyperbytedbError {
    HyperbytedbError::QueryParse(format!(
        "unsupported condition in SHOW ... WHERE (tag comparisons with =, !=, =~, !~, AND, OR): {expr:?}"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pred(where_text: &str) -> TagPredicate {
        TagPredicate::compile(&crate::timeseriesql::parser::parse_expr_str(where_text).unwrap())
            .unwrap()
    }

    fn tags(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect()
    }

    #[test]
    fn tag_conditions_match_like_influxdb() {
        let a = tags(&[("host", "a"), ("cpu", "cpu0")]);
        let b = tags(&[("host", "b"), ("cpu", "cpu1")]);
        for (cond, want_a, want_b) in [
            (r#""host" = 'a'"#, true, false),
            (r#""host"::tag = 'a'"#, true, false),
            (r#"'a' = "host""#, true, false),
            (r#""host" != 'a'"#, false, true),
            (r#""host" =~ /^(a|c)$/"#, true, false),
            (r#""host" !~ /a/"#, false, true),
            (r#""host" = 'a' OR "cpu" = 'cpu1'"#, true, true),
            (r#""host" = 'a' AND "cpu" = 'cpu1'"#, false, false),
            (r#""region" = ''"#, true, true),
            (r#""host" = 'a' AND time > now() - 1h"#, true, false),
        ] {
            let p = pred(cond);
            assert_eq!((p.matches(&a), p.matches(&b)), (want_a, want_b), "{cond}");
        }
    }

    #[test]
    fn other_conditions_are_refused_not_ignored() {
        for cond in [r#""usage" > 5"#, r#""host" = 5"#] {
            let expr = crate::timeseriesql::parser::parse_expr_str(cond).unwrap();
            assert!(TagPredicate::compile(&expr).is_err(), "{cond}");
        }
    }
}
