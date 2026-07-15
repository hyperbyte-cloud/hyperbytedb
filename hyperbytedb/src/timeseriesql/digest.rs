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
        Statement::DropMeasurement { .. } => "DROP",
        Statement::DropSeries(_) => "DROP",
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

/// Redact credential literals before storing query text in the statement summary.
pub fn redact_credentials(query: &str) -> String {
    use regex::Regex;
    use std::sync::OnceLock;

    static PASSWORD_SINGLE: OnceLock<Regex> = OnceLock::new();
    static PASSWORD_DOUBLE: OnceLock<Regex> = OnceLock::new();
    static QUERY_CREDS: OnceLock<Regex> = OnceLock::new();

    let mut out = query.to_string();
    // Literal patterns; invalid regex is a programming error, not user input.
    let re = PASSWORD_SINGLE.get_or_init(|| {
        #[allow(clippy::expect_used)]
        Regex::new(r"(?i)(PASSWORD\s+)'[^']*'").expect("password single-quote re")
    });
    out = re.replace_all(&out, "$1'****'").into_owned();
    let re = PASSWORD_DOUBLE.get_or_init(|| {
        #[allow(clippy::expect_used)]
        Regex::new(r#"(?i)(PASSWORD\s+)"[^"]*""#).expect("password double-quote re")
    });
    out = re.replace_all(&out, r#"$1"****""#).into_owned();
    let re = QUERY_CREDS.get_or_init(|| {
        #[allow(clippy::expect_used)]
        Regex::new(r"(?i)([?&](?:u|p)=)[^&\s]+").expect("query creds re")
    });
    out = re.replace_all(&out, "$1****").into_owned();
    out
}

fn normalize_statement(stmt: &Statement) -> String {
    let mut out = String::new();
    match stmt {
        Statement::Select(sel) => normalize_select(&mut out, sel),
        Statement::ShowDatabases => out.push_str("show databases"),
        Statement::ShowMeasurements(s) => {
            out.push_str("show measurements");
            normalize_on_db(&mut out, &s.database);
            normalize_show_tail(&mut out, &s.condition, &s.limit, &s.offset);
        }
        Statement::ShowTagKeys(s) => {
            out.push_str("show tag keys");
            normalize_on_db(&mut out, &s.database);
            if let Some(ref m) = s.from {
                out.push_str(" from ");
                normalize_measurement(&mut out, m);
            }
            normalize_show_tail(&mut out, &s.condition, &s.limit, &s.offset);
        }
        Statement::ShowTagValues(s) => {
            out.push_str("show tag values");
            normalize_on_db(&mut out, &s.database);
            if let Some(ref m) = s.from {
                out.push_str(" from ");
                normalize_measurement(&mut out, m);
            }
            match &s.tag_key {
                TagKeySelector::All => out.push_str(" with key = *"),
                TagKeySelector::Eq(k) => {
                    write!(out, " with key = {}", k).ok();
                }
                TagKeySelector::Neq(k) => {
                    write!(out, " with key != {}", k).ok();
                }
                TagKeySelector::Regex(r) => {
                    write!(out, " with key =~ /{}/", r).ok();
                }
                TagKeySelector::In(keys) => {
                    write!(out, " with key in ({})", keys.join(", ")).ok();
                }
            }
            normalize_show_tail(&mut out, &s.condition, &s.limit, &s.offset);
        }
        Statement::ShowFieldKeys(s) => {
            out.push_str("show field keys");
            normalize_on_db(&mut out, &s.database);
            if let Some(ref m) = s.from {
                out.push_str(" from ");
                normalize_measurement(&mut out, m);
            }
        }
        Statement::ShowSeries(s) => {
            out.push_str("show series");
            normalize_on_db(&mut out, &s.database);
            if let Some(ref m) = s.from {
                out.push_str(" from ");
                normalize_measurement(&mut out, m);
            }
            normalize_show_tail(&mut out, &s.condition, &s.limit, &s.offset);
        }
        Statement::ShowRetentionPolicies(db) => {
            write!(out, "show retention policies on {}", db).ok();
        }
        Statement::ShowUsers => out.push_str("show users"),
        Statement::ShowContinuousQueries => out.push_str("show continuous queries"),
        Statement::ShowMaterializedViews => out.push_str("show materialized views"),
        Statement::CreateDatabase(_) => out.push_str("create database ?"),
        Statement::DropDatabase(_) => out.push_str("drop database ?"),
        Statement::DropMeasurement { .. } => out.push_str("drop measurement ?"),
        Statement::DropSeries(_) => out.push_str("drop series ?"),
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
            write!(out, "delete from {}", del.from).ok();
            if let Some(ref cond) = del.condition {
                out.push_str(" where ");
                normalize_expr(&mut out, cond, true);
            }
        }
        Statement::CreateContinuousQuery(cq) => {
            write!(
                out,
                "create continuous query {} on {}",
                cq.name, cq.database
            )
            .ok();
        }
        Statement::DropContinuousQuery { name, db } => {
            write!(out, "drop continuous query {} on {}", name, db).ok();
        }
        Statement::CreateMaterializedView(mv) => {
            write!(
                out,
                "create materialized view {} on {}",
                mv.name, mv.database
            )
            .ok();
            if mv.backfill_on_create {
                out.push_str(" with backfill");
            }
        }
        Statement::DropMaterializedView { name, db } => {
            write!(out, "drop materialized view {} on {}", name, db).ok();
        }
    }
    out
}

fn normalize_on_db(out: &mut String, database: &Option<String>) {
    if let Some(db) = database {
        write!(out, " on {}", db).ok();
    }
}

fn normalize_show_tail(
    out: &mut String,
    condition: &Option<Expr>,
    limit: &Option<u64>,
    offset: &Option<u64>,
) {
    if let Some(cond) = condition {
        out.push_str(" where ");
        normalize_expr(out, cond, true);
    }
    if let Some(l) = limit {
        write!(out, " limit {l}").ok();
    }
    if let Some(o) = offset {
        write!(out, " offset {o}").ok();
    }
}

fn normalize_select(out: &mut String, sel: &SelectStatement) {
    out.push_str("select ");

    for (i, field) in sel.fields.iter().enumerate() {
        if i > 0 {
            out.push_str(", ");
        }
        normalize_expr(out, &field.expr, false);
        if let Some(ref alias) = field.alias {
            write!(out, " as {}", alias).ok();
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
                    write!(out, "{}", name).ok();
                }
                Dimension::AllTags => {
                    out.push('*');
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
    if let Some(ref tz) = sel.timezone {
        write!(out, " tz({})", tz).ok();
    }
}

/// Normalize an expression. When `in_condition` is true, literal values
/// are replaced with `?` placeholders to produce a canonical form.
fn normalize_expr(out: &mut String, expr: &Expr, in_condition: bool) {
    match expr {
        // Identifiers keep their case: InfluxQL identifiers are case-sensitive,
        // so `CPU` and `cpu` are different series and must digest differently.
        Expr::Identifier(name) => {
            write!(out, "{}", name).ok();
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
            write!(out, "{}", name).ok();
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

            // Parenthesize so `(a + b) * c` and `a + b * c` digest differently.
            out.push('(');
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
            out.push(')');
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
        write!(out, "{}.", db).ok();
    }
    if let Some(ref rp) = m.retention_policy {
        write!(out, "{}.", rp).ok();
    }
    match &m.name {
        MeasurementName::Name(name) => {
            write!(out, "{}", name).ok();
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
        // WHERE binary expressions are now parenthesized so that structurally
        // distinct predicates (e.g. `(a + b) * c` vs `a + b * c`) no longer
        // collide on the same digest.
        assert_eq!(n1, "select mean(usage) from cpu where (host = ?)");
    }

    #[test]
    fn redact_credentials_masks_password_literals() {
        let raw = r#"CREATE USER "x" WITH PASSWORD 's3cret'"#;
        let redacted = redact_credentials(raw);
        assert!(!redacted.contains("s3cret"));
        assert!(redacted.contains("****"));
    }

    #[test]
    fn normalize_create_mv_without_backfill() {
        let stmt = crate::timeseriesql::parse(
            r#"CREATE MATERIALIZED VIEW "mv" ON "db" AS SELECT mean("v") FROM "m" GROUP BY time(1m)"#,
        )
        .unwrap()
        .remove(0);
        let (_, norm) = fingerprint(&stmt);
        assert_eq!(norm, "create materialized view mv on db");
        assert!(!norm.contains("with backfill"));
    }

    #[test]
    fn normalize_create_mv_with_backfill() {
        let stmt = crate::timeseriesql::parse(
            r#"CREATE MATERIALIZED VIEW "mv" ON "db" WITH BACKFILL AS SELECT mean("v") FROM "m" GROUP BY time(1m)"#,
        )
        .unwrap()
        .remove(0);
        let (_, norm) = fingerprint(&stmt);
        assert_eq!(norm, "create materialized view mv on db with backfill");
    }
}
