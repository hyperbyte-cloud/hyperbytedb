use crate::domain::column_mapping::ColumnMapping;
use crate::error::HyperbytedbError;
use crate::timeseriesql::ast::*;
use std::fmt::Write;

use super::time_bounds::{is_time_epoch_comparison, is_time_identifier};

pub fn translate_condition(
    expr: &Expr,
    mapping: &ColumnMapping,
    out: &mut String,
) -> Result<(), HyperbytedbError> {
    translate_expr(expr, out, true, Some(mapping))
}

pub(super) fn tag_field_collision(m: &ColumnMapping, name: &str) -> bool {
    m.tag_keys.contains(name) && m.field_names.contains(name)
}

pub(super) fn is_where_literal(e: &Expr) -> bool {
    matches!(
        e,
        Expr::IntegerLiteral(_)
            | Expr::FloatLiteral(_)
            | Expr::StringLiteral(_)
            | Expr::BooleanLiteral(_)
    )
}

pub(super) fn where_identifier_physical_name(
    m: &ColumnMapping,
    name: &str,
    other: &Expr,
) -> Result<String, HyperbytedbError> {
    if !tag_field_collision(m, name) {
        return quote_identifier(name);
    }
    match other {
        Expr::IntegerLiteral(_) | Expr::FloatLiteral(_) | Expr::BooleanLiteral(_) => {
            quote_identifier(name)
        }
        Expr::StringLiteral(_) | Expr::Regex(_) => {
            Ok(quote_phys_identifier(&m.physical_tag_column_name(name)))
        }
        _ => Ok(quote_phys_identifier(&m.physical_tag_column_name(name))),
    }
}

pub(super) fn regex_match_column_name(
    left: &Expr,
    mapping: Option<&ColumnMapping>,
) -> Result<String, HyperbytedbError> {
    match left {
        Expr::FieldRef {
            name,
            typ: Some(FieldType::Tag),
        } => {
            let col = mapping
                .map(|m| m.physical_tag_column_name(name))
                .unwrap_or_else(|| name.clone());
            Ok(quote_phys_identifier(&col))
        }
        Expr::FieldRef {
            name,
            typ: Some(FieldType::Field),
        } => quote_identifier(name),
        Expr::FieldRef { name, typ: None } => {
            let col = mapping
                .map(|m| m.physical_tag_column_name(name))
                .unwrap_or_else(|| name.clone());
            Ok(quote_phys_identifier(&col))
        }
        Expr::Identifier(n) => {
            let col = if let Some(m) = mapping {
                if tag_field_collision(m, n) {
                    m.physical_tag_column_name(n)
                } else {
                    m.physical_select_identifier(n)
                }
            } else {
                n.clone()
            };
            Ok(quote_phys_identifier(&col))
        }
        _ => Err(HyperbytedbError::QueryParse(
            "regex operator =~ / !~ requires identifier and regex".to_string(),
        )),
    }
}

pub(super) fn try_translate_where_binary_expr(
    be: &BinaryExpr,
    out: &mut String,
    m: &ColumnMapping,
) -> Result<bool, HyperbytedbError> {
    let (name, lit, id_on_left, explicit_tag) = match (&be.left, &be.right) {
        (Expr::Identifier(n), rhs) if is_where_literal(rhs) => (n.as_str(), rhs, true, false),
        (Expr::FieldRef { name, typ: None }, rhs) if is_where_literal(rhs) => {
            (name.as_str(), rhs, true, false)
        }
        (
            Expr::FieldRef {
                name,
                typ: Some(FieldType::Tag),
            },
            rhs,
        ) if is_where_literal(rhs) => (name.as_str(), rhs, true, true),
        (lhs, Expr::Identifier(n)) if is_where_literal(lhs) => (n.as_str(), lhs, false, false),
        (lhs, Expr::FieldRef { name, typ: None }) if is_where_literal(lhs) => {
            (name.as_str(), lhs, false, false)
        }
        (
            lhs,
            Expr::FieldRef {
                name,
                typ: Some(FieldType::Tag),
            },
        ) if is_where_literal(lhs) => (name.as_str(), lhs, false, true),
        _ => return Ok(false),
    };
    if matches!(be.op, BinaryOp::And | BinaryOp::Or) {
        return Ok(false);
    }
    // Tags are strings; comparing one to a numeric literal never matches in
    // InfluxQL (and would be a type error in ClickHouse). Emit constant-false
    // so the query runs and returns an empty result.
    let is_pure_tag = explicit_tag || (m.tag_keys.contains(name) && !m.field_names.contains(name));
    if is_pure_tag && matches!(lit, Expr::IntegerLiteral(_) | Expr::FloatLiteral(_)) {
        write!(out, "1 = 0")?;
        return Ok(true);
    }
    if !tag_field_collision(m, name) {
        return Ok(false);
    }
    let col = where_identifier_physical_name(m, name, lit)?;
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

pub(super) fn translate_expr(
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
                        write!(
                            out,
                            "{}",
                            quote_phys_identifier(&m.physical_tag_column_name(name))
                        )?;
                    } else {
                        write!(out, "{}", quote_identifier(name)?)?;
                    }
                } else {
                    write!(out, "{}", quote_identifier(name)?)?;
                }
            } else {
                write!(out, "{}", quote_identifier(name)?)?;
            }
        }
        Expr::FieldRef { name, typ } => {
            let s = match typ {
                Some(FieldType::Tag) => {
                    if let Some(m) = mapping {
                        quote_phys_identifier(&m.physical_tag_column_name(name))
                    } else {
                        quote_identifier(name)?
                    }
                }
                Some(FieldType::Field) => quote_identifier(name)?,
                None => {
                    if let Some(m) = mapping {
                        if tag_field_collision(m, name) {
                            quote_phys_identifier(&m.physical_tag_column_name(name))
                        } else {
                            quote_identifier(name)?
                        }
                    } else {
                        quote_identifier(name)?
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

pub(super) fn translate_time_epoch_comparison(
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

pub(super) fn epoch_duration_to_timestamp(d: &Duration) -> String {
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

pub(super) fn nanos_to_ch_timestamp(nanos: i64) -> String {
    format!("fromUnixTimestamp64Nano({nanos})")
}

pub(super) fn binary_op_to_clickhouse(op: &BinaryOp) -> &'static str {
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

pub(super) fn quote_identifier(name: &str) -> Result<String, HyperbytedbError> {
    if name.chars().any(char::is_control) {
        return Err(HyperbytedbError::QueryParse(format!(
            "identifier contains control characters: {name:?}"
        )));
    }
    Ok(format!(
        "\"{}\"",
        name.replace('\\', "\\\\").replace('"', "\\\"")
    ))
}

/// Quote a physical column name from [`crate::domain::chdb_naming`] (already sanitized).
pub(super) fn quote_phys_identifier(name: &str) -> String {
    format!("\"{}\"", name.replace('\\', "\\\\").replace('"', "\\\""))
}

pub(super) fn quote_string(s: &str) -> String {
    format!("'{}'", s.replace('\\', "\\\\").replace('\'', "\\'"))
}

pub(super) fn format_float(f: f64) -> String {
    if f.fract() == 0.0 && f.is_finite() {
        format!("{}", f as i64)
    } else {
        format!("{}", f)
    }
}
