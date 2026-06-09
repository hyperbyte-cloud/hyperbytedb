use sha2::{Digest, Sha256};
use std::fmt::Write;

use super::ast::*;

pub fn stmt_type(stmt: &Statement) -> &'static str {
    match stmt {
        Statement::Select(_) => "SELECT",
        Statement::ShowDatabases => "SHOW",
        Statement::ShowMeasurements(_) => "SHOW",
        Statement::ShowTagKeys(_) => "SHOW",
        Statement::ShowTagValues(_) => "SHOW",
        Statement::ShowFieldKeys(_) => "SHOW",
        Statement::ShowSeries(_) => "SHOW",
        Statement::ShowRetentionPolicies(_) => "SHOW",
        Statement::ShowUsers => "SHOW",
        Statement::ShowContinuousQueries => "SHOW",
        Statement::ShowMaterializedViews => "SHOW",
        Statement::CreateDatabase(_) => "CREATE",
        Statement::CreateRetentionPolicyStmt { .. } => "CREATE",
        Statement::CreateUser { .. } => "CREATE",
        Statement::CreateContinuousQuery(_) => "CREATE",
        Statement::CreateMaterializedView(_) => "CREATE",
        Statement::DropDatabase(_) => "DROP",
        Statement::DropMeasurement(_) => "DROP",
        Statement::DropUser(_) => "DROP",
        Statement::DropRetentionPolicyStmt { .. } => "DROP",
        Statement::DropContinuousQuery { .. } => "DROP",
        Statement::DropMaterializedView { .. } => "DROP",
        Statement::AlterRetentionPolicyStmt { .. } => "ALTER",
        Statement::Delete(_) => "DELETE",
        Statement::SetPassword { .. } => "SET",
        Statement::Grant { .. } => "GRANT",
        Statement::Revoke { .. } => "REVOKE",
    }
}

/// Produce a normalized query string and its SHA-256 digest.
///
/// Literal values in WHERE conditions are replaced with `?` so that
/// structurally identical queries share the same digest regardless of
/// the concrete filter values used.
pub fn fingerprint(stmt: &Statement) -> (String, String) {
    let normalized = normalize_statement(stmt);
    let hash = Sha256::digest(normalized.as_bytes());
    let digest_hex = format!("{:x}", hash);
    let short_digest = digest_hex[..16.min(digest_hex.len())].to_string();
    (short_digest, normalized)
}

fn normalize_statement(stmt: &Statement) -> String {
    let mut out = String::new();
    match stmt {
        Statement::Select(sel) => normalize_select(&mut out, sel),
        Statement::ShowDatabases => out.push_str("show databases"),
        Statement::ShowMeasurements(s) => {
            out.push_str("show measurements");
            if let Some(ref db) = s.database {
                write!(out, " on {}", db.to_lowercase()).ok();
            }
            if let Some(ref cond) = s.condition {
                out.push_str(" where ");
                normalize_expr(&mut out, cond, true);
            }
        }
        Statement::ShowTagKeys(s) => {
            out.push_str("show tag keys");
            if let Some(ref m) = s.from {
                out.push_str(" from ");
                normalize_measurement(&mut out, m);
            }
        }
        Statement::ShowTagValues(s) => {
            out.push_str("show tag values");
            if let Some(ref m) = s.from {
                out.push_str(" from ");
                normalize_measurement(&mut out, m);
            }
            match &s.tag_key {
                TagKeySelector::All => out.push_str(" with key = *"),
                TagKeySelector::Eq(k) => {
                    write!(out, " with key = {}", k.to_lowercase()).ok();
                }
                TagKeySelector::Neq(k) => {
                    write!(out, " with key != {}", k.to_lowercase()).ok();
                }
                TagKeySelector::Regex(r) => {
                    write!(out, " with key =~ /{}/", r).ok();
                }
                TagKeySelector::In(keys) => {
                    let lower: Vec<String> = keys.iter().map(|k| k.to_lowercase()).collect();
                    write!(out, " with key in ({})", lower.join(", ")).ok();
                }
            }
        }
        Statement::ShowFieldKeys(s) => {
            out.push_str("show field keys");
            if let Some(ref m) = s.from {
                out.push_str(" from ");
                normalize_measurement(&mut out, m);
            }
        }
        Statement::ShowSeries(s) => {
            out.push_str("show series");
            if let Some(ref m) = s.from {
                out.push_str(" from ");
                normalize_measurement(&mut out, m);
            }
        }
        Statement::ShowRetentionPolicies(db) => {
            write!(out, "show retention policies on {}", db.to_lowercase()).ok();
        }
        Statement::ShowUsers => out.push_str("show users"),
        Statement::ShowContinuousQueries => out.push_str("show continuous queries"),
        Statement::ShowMaterializedViews => out.push_str("show materialized views"),
        Statement::CreateDatabase(_) => out.push_str("create database ?"),
        Statement::DropDatabase(_) => out.push_str("drop database ?"),
        Statement::DropMeasurement(_) => out.push_str("drop measurement ?"),
        Statement::CreateRetentionPolicyStmt { .. } => {
            out.push_str("create retention policy ?");
        }
        Statement::AlterRetentionPolicyStmt { .. } => {
            out.push_str("alter retention policy ?");
        }
        Statement::DropRetentionPolicyStmt { .. } => {
            out.push_str("drop retention policy ?");
        }
        Statement::CreateUser { .. } => out.push_str("create user ?"),
        Statement::DropUser(_) => out.push_str("drop user ?"),
        Statement::SetPassword { .. } => out.push_str("set password ?"),
        Statement::Grant { database, .. } => {
            if database.is_some() {
                out.push_str("grant all on ? to ?");
            } else {
                out.push_str("grant all privileges to ?");
            }
        }
        Statement::Revoke { database, .. } => {
            if database.is_some() {
                out.push_str("revoke all on ? from ?");
            } else {
                out.push_str("revoke all privileges from ?");
            }
        }
        Statement::Delete(del) => {
            write!(out, "delete from {}", del.from.to_lowercase()).ok();
            if let Some(ref cond) = del.condition {
                out.push_str(" where ");
                normalize_expr(&mut out, cond, true);
            }
        }
        Statement::CreateContinuousQuery(cq) => {
            write!(
                out,
                "create continuous query {} on {}",
                cq.name.to_lowercase(),
                cq.database.to_lowercase()
            )
            .ok();
        }
        Statement::DropContinuousQuery { name, db } => {
            write!(
                out,
                "drop continuous query {} on {}",
                name.to_lowercase(),
                db.to_lowercase()
            )
            .ok();
        }
        Statement::CreateMaterializedView(mv) => {
            write!(
                out,
                "create materialized view {} on {}",
                mv.name.to_lowercase(),
                mv.database.to_lowercase()
            )
            .ok();
        }
        Statement::DropMaterializedView { name, db } => {
            write!(
                out,
                "drop materialized view {} on {}",
                name.to_lowercase(),
                db.to_lowercase()
            )
            .ok();
        }
    }
    out
}

fn normalize_select(out: &mut String, sel: &SelectStatement) {
    out.push_str("select ");

    for (i, field) in sel.fields.iter().enumerate() {
        if i > 0 {
            out.push_str(", ");
        }
        normalize_expr(out, &field.expr, false);
        if let Some(ref alias) = field.alias {
            write!(out, " as {}", alias.to_lowercase()).ok();
        }
    }

    if let Some(ref into) = sel.into {
        out.push_str(" into ");
        normalize_measurement(out, into);
    }

    if !sel.from.is_empty() {
        out.push_str(" from ");
        for (i, src) in sel.from.iter().enumerate() {
            if i > 0 {
                out.push_str(", ");
            }
            match src {
                MeasurementSource::Concrete(m) => normalize_measurement(out, m),
                MeasurementSource::Subquery(sub) => {
                    out.push('(');
                    normalize_select(out, sub);
                    out.push(')');
                }
            }
        }
    }

    if let Some(ref cond) = sel.condition {
        out.push_str(" where ");
        normalize_expr(out, cond, true);
    }

    if let Some(ref gb) = sel.group_by {
        out.push_str(" group by ");
        for (i, dim) in gb.dimensions.iter().enumerate() {
            if i > 0 {
                out.push_str(", ");
            }
            match dim {
                Dimension::Time { interval, offset } => {
                    write!(out, "time({})", normalize_duration(interval)).ok();
                    if let Some(off) = offset {
                        write!(out, ", {}", normalize_duration(off)).ok();
                    }
                }
                Dimension::Tag(name) => {
                    write!(out, "{}", name.to_lowercase()).ok();
                }
                Dimension::Regex(r) => {
                    write!(out, "/{}/", r).ok();
                }
            }
        }
    }

    if let Some(ref fill) = sel.fill {
        match fill {
            FillOption::Null => out.push_str(" fill(null)"),
            FillOption::None => out.push_str(" fill(none)"),
            FillOption::Previous => out.push_str(" fill(previous)"),
            FillOption::Linear => out.push_str(" fill(linear)"),
            FillOption::Value(_) => out.push_str(" fill(?)"),
        }
    }

    if let Some(ref ob) = sel.order_by {
        if ob.time_desc {
            out.push_str(" order by time desc");
        } else {
            out.push_str(" order by time asc");
        }
    }

    if let Some(limit) = sel.limit {
        write!(out, " limit {}", limit).ok();
    }
    if let Some(offset) = sel.offset {
        write!(out, " offset {}", offset).ok();
    }
    if let Some(slimit) = sel.slimit {
        write!(out, " slimit {}", slimit).ok();
    }
    if let Some(soffset) = sel.soffset {
        write!(out, " soffset {}", soffset).ok();
    }
}

/// Normalize an expression. When `in_condition` is true, literal values
/// are replaced with `?` placeholders to produce a canonical form.
fn normalize_expr(out: &mut String, expr: &Expr, in_condition: bool) {
    match expr {
        Expr::Identifier(name) => {
            write!(out, "{}", name.to_lowercase()).ok();
        }
        Expr::Star | Expr::Wildcard => out.push('*'),
        Expr::StringLiteral(_) => {
            // Always redacted: even outside WHERE, a string literal is a value
            // we don't want to fingerprint on.
            out.push('?');
        }
        Expr::IntegerLiteral(v) => {
            if in_condition {
                out.push('?');
            } else {
                write!(out, "{}", v).ok();
            }
        }
        Expr::FloatLiteral(v) => {
            if in_condition {
                out.push('?');
            } else {
                write!(out, "{}", v).ok();
            }
        }
        Expr::BooleanLiteral(_) => {
            // Booleans are also values; redact regardless of context.
            out.push('?');
        }
        Expr::DurationLiteral(d) => {
            if in_condition {
                out.push('?');
            } else {
                write!(out, "{}", normalize_duration(d)).ok();
            }
        }
        Expr::TimeLiteral(_) => {
            out.push('?');
        }
        Expr::Regex(r) => {
            write!(out, "/{}/", r).ok();
        }
        Expr::Now => out.push_str("now()"),
        Expr::FieldRef { name, .. } => {
            write!(out, "{}", name.to_lowercase()).ok();
        }
        Expr::Call(call) => {
            write!(out, "{}(", call.name.to_lowercase()).ok();
            for (i, arg) in call.args.iter().enumerate() {
                if i > 0 {
                    out.push_str(", ");
                }
                normalize_expr(out, arg, false);
            }
            out.push(')');
        }
        Expr::BinaryExpr(bin) => {
            let child_in_cond = in_condition || matches!(bin.op, BinaryOp::And | BinaryOp::Or);

            normalize_expr(out, &bin.left, child_in_cond);
            let op_str = match bin.op {
                BinaryOp::Add => " + ",
                BinaryOp::Sub => " - ",
                BinaryOp::Mul => " * ",
                BinaryOp::Div => " / ",
                BinaryOp::Mod => " % ",
                BinaryOp::Eq => " = ",
                BinaryOp::Neq => " != ",
                BinaryOp::Lt => " < ",
                BinaryOp::Lte => " <= ",
                BinaryOp::Gt => " > ",
                BinaryOp::Gte => " >= ",
                BinaryOp::And => " and ",
                BinaryOp::Or => " or ",
                BinaryOp::RegexMatch => " =~ ",
                BinaryOp::RegexNotMatch => " !~ ",
            };
            out.push_str(op_str);
            normalize_expr(out, &bin.right, child_in_cond);
        }
        Expr::UnaryExpr(op, inner) => {
            match op {
                UnaryOp::Neg => out.push('-'),
                UnaryOp::Not => out.push_str("not "),
            }
            normalize_expr(out, inner, in_condition);
        }
    }
}

fn normalize_measurement(out: &mut String, m: &Measurement) {
    if let Some(ref db) = m.database {
        write!(out, "{}.", db.to_lowercase()).ok();
    }
    if let Some(ref rp) = m.retention_policy {
        write!(out, "{}.", rp.to_lowercase()).ok();
    }
    match &m.name {
        MeasurementName::Name(name) => {
            write!(out, "{}", name.to_lowercase()).ok();
        }
        MeasurementName::Regex(r) => {
            write!(out, "/{}/", r).ok();
        }
    }
}

fn normalize_duration(d: &Duration) -> String {
    let unit = match d.unit {
        DurationUnit::Nanosecond => "ns",
        DurationUnit::Microsecond => "u",
        DurationUnit::Millisecond => "ms",
        DurationUnit::Second => "s",
        DurationUnit::Minute => "m",
        DurationUnit::Hour => "h",
        DurationUnit::Day => "d",
        DurationUnit::Week => "w",
    };
    format!("{}{}", d.value, unit)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stmt_type_select() {
        let stmt = Statement::Select(SelectStatement {
            fields: vec![],
            into: None,
            from: vec![],
            condition: None,
            group_by: None,
            order_by: None,
            limit: None,
            offset: None,
            slimit: None,
            soffset: None,
            fill: None,
            timezone: None,
        });
        assert_eq!(stmt_type(&stmt), "SELECT");
    }

    #[test]
    fn test_stmt_type_show() {
        assert_eq!(stmt_type(&Statement::ShowDatabases), "SHOW");
    }

    #[test]
    fn test_fingerprint_show_databases() {
        let (d1, n1) = fingerprint(&Statement::ShowDatabases);
        let (d2, n2) = fingerprint(&Statement::ShowDatabases);
        assert_eq!(d1, d2);
        assert_eq!(n1, "show databases");
        assert_eq!(n2, "show databases");
    }

    #[test]
    fn test_fingerprint_select_normalizes_where_values() {
        let stmt1 = Statement::Select(SelectStatement {
            fields: vec![Field {
                expr: Expr::Call(FunctionCall {
                    name: "mean".to_string(),
                    args: vec![Expr::Identifier("usage".to_string())],
                }),
                alias: None,
            }],
            into: None,
            from: vec![MeasurementSource::Concrete(Measurement {
                database: None,
                retention_policy: None,
                name: MeasurementName::Name("cpu".to_string()),
            })],
            condition: Some(Expr::BinaryExpr(Box::new(BinaryExpr {
                left: Expr::Identifier("host".to_string()),
                op: BinaryOp::Eq,
                right: Expr::StringLiteral("web01".to_string()),
            }))),
            group_by: None,
            order_by: None,
            limit: None,
            offset: None,
            slimit: None,
            soffset: None,
            fill: None,
            timezone: None,
        });

        let stmt2 = Statement::Select(SelectStatement {
            fields: vec![Field {
                expr: Expr::Call(FunctionCall {
                    name: "mean".to_string(),
                    args: vec![Expr::Identifier("usage".to_string())],
                }),
                alias: None,
            }],
            into: None,
            from: vec![MeasurementSource::Concrete(Measurement {
                database: None,
                retention_policy: None,
                name: MeasurementName::Name("cpu".to_string()),
            })],
            condition: Some(Expr::BinaryExpr(Box::new(BinaryExpr {
                left: Expr::Identifier("host".to_string()),
                op: BinaryOp::Eq,
                right: Expr::StringLiteral("web02".to_string()),
            }))),
            group_by: None,
            order_by: None,
            limit: None,
            offset: None,
            slimit: None,
            soffset: None,
            fill: None,
            timezone: None,
        });

        let (d1, n1) = fingerprint(&stmt1);
        let (d2, n2) = fingerprint(&stmt2);
        assert_eq!(d1, d2, "same structure should produce same digest");
        assert_eq!(n1, n2);
        assert_eq!(n1, "select mean(usage) from cpu where host = ?");
    }
}
