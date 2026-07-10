//! Token-driven InfluxQL DDL/SHOW/auth parser (InfluxDB 1.x compatibility).

use crate::error::HyperbytedbError;
use crate::timeseriesql::ast::*;
use crate::timeseriesql::lexer::{
    Token, TokenCursor, TokenKind, nanos_to_ast_duration, parse_duration_text, tokenize,
};

const MIN_RP_DURATION_NANOS: i64 = 3_600 * 1_000_000_000; // 1h

/// Parse a single non-SELECT statement from token stream.
pub fn parse_ddl_statement(input: &str) -> Result<Statement, HyperbytedbError> {
    let tokens = tokenize(input)?;
    let mut cur = TokenCursor::new(input, &tokens);
    let first = cur
        .peek()
        .ok_or_else(|| HyperbytedbError::QueryParse("empty statement".to_string()))?;
    let stmt = match &first.kind {
        TokenKind::Keyword(k) => match k.as_str() {
            "SHOW" => parse_show(&mut cur),
            "CREATE" => parse_create(&mut cur),
            "DROP" => parse_drop(&mut cur),
            "ALTER" => parse_alter(&mut cur),
            "DELETE" => parse_delete(&mut cur),
            "SET" => parse_set_password(&mut cur),
            "GRANT" => parse_grant(&mut cur),
            "REVOKE" => parse_revoke(&mut cur),
            other => Err(HyperbytedbError::QueryParse(format!(
                "unsupported statement: {other}"
            ))),
        },
        _ => Err(HyperbytedbError::QueryParse(format!(
            "expected statement keyword, found {:?}",
            first.kind
        ))),
    }?;
    // Trailing tokens mean an unsupported or misplaced clause. Silently
    // ignoring them turned e.g. `CREATE USER x WITH PASSWORD 'p' WITH ALL
    // PRIVILEGES` into a non-admin user and `SHOW MEASUREMENTS WITH
    // MEASUREMENT =~ /re/` into an unfiltered listing.
    if let Some(tok) = cur.peek() {
        return Err(HyperbytedbError::QueryParse(format!(
            "unexpected trailing input: '{}'",
            cur.remaining_from(tok).trim()
        )));
    }
    Ok(stmt)
}

fn parse_show(cur: &mut TokenCursor<'_>) -> Result<Statement, HyperbytedbError> {
    cur.expect_keyword("SHOW")?;
    let kw = cur
        .peek()
        .ok_or_else(|| HyperbytedbError::QueryParse("incomplete SHOW".to_string()))?;
    match &kw.kind {
        TokenKind::Keyword(k) => match k.as_str() {
            "DATABASES" => {
                cur.bump();
                Ok(Statement::ShowDatabases)
            }
            "RETENTION" => {
                cur.bump();
                cur.expect_keyword("POLICIES")?;
                let db = parse_optional_on_db(cur)?;
                Ok(Statement::ShowRetentionPolicies(db.unwrap_or_default()))
            }
            "USERS" => {
                cur.bump();
                Ok(Statement::ShowUsers)
            }
            "MEASUREMENTS" => {
                cur.bump();
                let mut stmt = ShowMeasurementsStatement {
                    database: parse_optional_on_db(cur)?,
                    condition: None,
                    limit: None,
                    offset: None,
                };
                // Previously this clause was silently dropped, listing every
                // measurement unfiltered. Reject loudly until implemented.
                if cur.match_keyword("WITH") {
                    return Err(HyperbytedbError::QueryParse(
                        "SHOW MEASUREMENTS WITH MEASUREMENT is not supported; \
                         use a WHERE clause instead"
                            .to_string(),
                    ));
                }
                parse_show_tail(
                    cur,
                    &mut stmt.database,
                    &mut stmt.condition,
                    &mut stmt.limit,
                    &mut stmt.offset,
                )?;
                Ok(Statement::ShowMeasurements(stmt))
            }
            "TAG" => {
                cur.bump();
                let sub = cur.take_ident()?;
                match sub.to_uppercase().as_str() {
                    "KEYS" => {
                        let (mut database, from) = parse_show_on_from(cur)?;
                        let mut condition = None;
                        let mut limit = None;
                        let mut offset = None;
                        parse_show_tail(
                            cur,
                            &mut database,
                            &mut condition,
                            &mut limit,
                            &mut offset,
                        )?;
                        Ok(Statement::ShowTagKeys(ShowTagKeysStatement {
                            database,
                            from,
                            condition,
                            limit,
                            offset,
                        }))
                    }
                    "VALUES" => {
                        let (mut database, from) = parse_show_on_from(cur)?;
                        let tag_key = parse_with_key(cur)?;
                        let mut condition = None;
                        let mut limit = None;
                        let mut offset = None;
                        parse_show_tail(
                            cur,
                            &mut database,
                            &mut condition,
                            &mut limit,
                            &mut offset,
                        )?;
                        Ok(Statement::ShowTagValues(ShowTagValuesStatement {
                            database,
                            from,
                            tag_key,
                            condition,
                            limit,
                            offset,
                        }))
                    }
                    _ => Err(HyperbytedbError::QueryParse(format!(
                        "unexpected SHOW TAG: {sub}"
                    ))),
                }
            }
            "FIELD" => {
                cur.bump();
                cur.expect_keyword("KEYS")?;
                let (database, from) = parse_show_on_from(cur)?;
                Ok(Statement::ShowFieldKeys(ShowFieldKeysStatement {
                    database,
                    from,
                }))
            }
            "SERIES" => {
                cur.bump();
                let (mut database, from) = parse_show_on_from(cur)?;
                let mut condition = None;
                let mut limit = None;
                let mut offset = None;
                parse_show_tail(cur, &mut database, &mut condition, &mut limit, &mut offset)?;
                Ok(Statement::ShowSeries(ShowSeriesStatement {
                    database,
                    from,
                    condition,
                    limit,
                    offset,
                }))
            }
            "CONTINUOUS" => {
                cur.bump();
                cur.expect_keyword("QUERIES")?;
                Ok(Statement::ShowContinuousQueries)
            }
            "MATERIALIZED" => {
                cur.bump();
                cur.expect_keyword("VIEWS")?;
                Ok(Statement::ShowMaterializedViews)
            }
            _ => Err(HyperbytedbError::QueryParse(format!(
                "unsupported SHOW: {k}"
            ))),
        },
        _ => Err(HyperbytedbError::QueryParse("incomplete SHOW".to_string())),
    }
}

fn parse_show_tail(
    cur: &mut TokenCursor<'_>,
    database: &mut Option<String>,
    condition: &mut Option<Expr>,
    limit: &mut Option<u64>,
    offset: &mut Option<u64>,
) -> Result<(), HyperbytedbError> {
    if database.is_none() {
        *database = parse_optional_on_db(cur)?;
    }
    if cur.match_keyword("WHERE") {
        let where_text = slice_remaining_clause(cur, &["LIMIT", "OFFSET"])?;
        *condition = Some(crate::timeseriesql::parser::parse_expr_str(&where_text)?);
    }
    if cur.match_keyword("LIMIT") {
        *limit = Some(parse_u64_token(cur)?);
    }
    if cur.match_keyword("OFFSET") {
        *offset = Some(parse_u64_token(cur)?);
    }
    Ok(())
}

fn parse_create(cur: &mut TokenCursor<'_>) -> Result<Statement, HyperbytedbError> {
    cur.expect_keyword("CREATE")?;
    let kw = cur.take_ident()?;
    match kw.to_uppercase().as_str() {
        "DATABASE" => parse_create_database(cur),
        "RETENTION" => {
            cur.expect_keyword("POLICY")?;
            parse_create_retention_policy(cur)
        }
        "USER" => parse_create_user(cur),
        "CONTINUOUS" => {
            cur.expect_keyword("QUERY")?;
            parse_create_continuous_query(cur)
        }
        "MATERIALIZED" => {
            cur.expect_keyword("VIEW")?;
            parse_create_materialized_view(cur)
        }
        _ => Err(HyperbytedbError::QueryParse(format!(
            "unsupported CREATE: {kw}"
        ))),
    }
}

fn parse_create_database(cur: &mut TokenCursor<'_>) -> Result<Statement, HyperbytedbError> {
    let name = cur.take_ident()?;
    let mut stmt = CreateDatabaseStatement {
        name,
        duration: None,
        replication: None,
        shard_duration: None,
        rp_name: None,
    };
    if cur.match_keyword("WITH") {
        while let Some(opt) = cur.peek() {
            match &opt.kind {
                TokenKind::Keyword(k) if k == "DURATION" => {
                    cur.bump();
                    stmt.duration = Some(parse_duration_token(cur)?);
                }
                TokenKind::Keyword(k) if k == "REPLICATION" => {
                    cur.bump();
                    stmt.replication = Some(parse_replication(cur)?);
                }
                TokenKind::Keyword(k) if k == "SHARD" => {
                    cur.bump();
                    cur.expect_keyword("DURATION")?;
                    stmt.shard_duration = Some(parse_duration_token(cur)?);
                }
                TokenKind::Keyword(k) if k == "NAME" => {
                    cur.bump();
                    stmt.rp_name = Some(cur.take_ident()?);
                }
                _ => break,
            }
        }
    }
    // Same rules as retention policies: zero means infinite (represented as
    // no duration → engine default), anything else must be at least 1h.
    if let Some(ref d) = stmt.duration {
        if d.to_nanos() == 0 {
            stmt.duration = None;
        } else {
            validate_rp_duration(d)?;
        }
    }
    if let Some(ref sd) = stmt.shard_duration {
        if sd.to_nanos() == 0 {
            stmt.shard_duration = None;
        } else {
            validate_rp_duration(sd)?;
        }
    }
    Ok(Statement::CreateDatabase(stmt))
}

fn parse_drop(cur: &mut TokenCursor<'_>) -> Result<Statement, HyperbytedbError> {
    cur.expect_keyword("DROP")?;
    let kw = cur.take_ident()?;
    match kw.to_uppercase().as_str() {
        "DATABASE" => Ok(Statement::DropDatabase(cur.take_ident()?)),
        "MEASUREMENT" => {
            let meas = parse_measurement_ref(cur)?;
            let name = match meas.name {
                MeasurementName::Name(n) => n,
                MeasurementName::Regex(p) => {
                    return Err(HyperbytedbError::QueryParse(format!(
                        "DROP MEASUREMENT does not support regex: /{p}/"
                    )));
                }
            };
            Ok(Statement::DropMeasurement {
                name,
                rp: meas.retention_policy,
            })
        }
        "SERIES" => parse_drop_series(cur),
        "RETENTION" => {
            cur.expect_keyword("POLICY")?;
            let name = cur.take_ident()?;
            let db = parse_on_db(cur)?;
            Ok(Statement::DropRetentionPolicyStmt { name, db })
        }
        "USER" => Ok(Statement::DropUser(cur.take_ident()?)),
        "CONTINUOUS" => {
            cur.expect_keyword("QUERY")?;
            let name = cur.take_ident()?;
            let db = parse_on_db(cur)?;
            Ok(Statement::DropContinuousQuery { name, db })
        }
        "MATERIALIZED" => {
            cur.expect_keyword("VIEW")?;
            let name = cur.take_ident()?;
            let db = parse_on_db(cur)?;
            Ok(Statement::DropMaterializedView { name, db })
        }
        _ => Err(HyperbytedbError::QueryParse(format!(
            "unsupported DROP: {kw}"
        ))),
    }
}

fn parse_drop_series(cur: &mut TokenCursor<'_>) -> Result<Statement, HyperbytedbError> {
    let mut from = None;
    let mut condition = None;
    if cur.match_keyword("FROM") {
        from = Some(parse_measurement_name(cur)?);
    }
    let database = parse_optional_on_db(cur)?;
    if cur.match_keyword("WHERE") {
        let where_text = slice_remaining_clause(cur, &[])?;
        condition = Some(crate::timeseriesql::parser::parse_expr_str(&where_text)?);
    }
    Ok(Statement::DropSeries(DropSeriesStatement {
        database,
        from,
        condition,
    }))
}

fn parse_alter(cur: &mut TokenCursor<'_>) -> Result<Statement, HyperbytedbError> {
    cur.expect_keyword("ALTER")?;
    cur.expect_keyword("RETENTION")?;
    cur.expect_keyword("POLICY")?;
    let name = cur.take_ident()?;
    let db = parse_on_db(cur)?;
    let mut duration = None;
    let mut replication = None;
    let mut shard_duration = None;
    let mut is_default = None;
    let mut changed = false;
    while let Some(tok) = cur.peek() {
        match &tok.kind {
            TokenKind::Keyword(k) if k == "DURATION" => {
                cur.bump();
                duration = Some(parse_duration_token(cur)?);
                changed = true;
            }
            TokenKind::Keyword(k) if k == "REPLICATION" => {
                cur.bump();
                replication = Some(parse_replication(cur)?);
                changed = true;
            }
            TokenKind::Keyword(k) if k == "SHARD" => {
                cur.bump();
                cur.expect_keyword("DURATION")?;
                shard_duration = Some(parse_duration_token(cur)?);
                changed = true;
            }
            TokenKind::Keyword(k) if k == "DEFAULT" => {
                cur.bump();
                is_default = Some(true);
                changed = true;
            }
            _ => break,
        }
    }
    if !changed {
        return Err(HyperbytedbError::QueryParse(
            "ALTER RETENTION POLICY requires at least one option".to_string(),
        ));
    }
    if let Some(ref d) = duration {
        validate_rp_duration(d)?;
    }
    if let Some(ref sd) = shard_duration {
        validate_rp_duration(sd)?;
    }
    Ok(Statement::AlterRetentionPolicyStmt {
        name,
        db,
        duration,
        replication,
        shard_duration,
        is_default,
    })
}

fn parse_delete(cur: &mut TokenCursor<'_>) -> Result<Statement, HyperbytedbError> {
    cur.expect_keyword("DELETE")?;
    let mut measurement = String::new();
    if cur.match_keyword("FROM") {
        measurement = cur.take_ident()?;
    }
    if cur.match_keyword("WHERE") {
        let where_text = slice_remaining_clause(cur, &[])?;
        let expr = crate::timeseriesql::parser::parse_expr_str(&where_text)?;
        validate_delete_predicate(&expr)?;
        if measurement.is_empty() {
            return Ok(Statement::Delete(DeleteStatement {
                from: String::new(),
                condition: Some(expr),
            }));
        }
        return Ok(Statement::Delete(DeleteStatement {
            from: measurement,
            condition: Some(expr),
        }));
    }
    if measurement.is_empty() {
        return Err(HyperbytedbError::QueryParse(
            "DELETE requires FROM or WHERE time predicate".to_string(),
        ));
    }
    Ok(Statement::Delete(DeleteStatement {
        from: measurement,
        condition: None,
    }))
}

fn validate_cq_mv_select(stmt: &SelectStatement) -> Result<(), HyperbytedbError> {
    let gb = stmt.group_by.as_ref().ok_or_else(|| {
        HyperbytedbError::QueryParse(
            "continuous query and materialized view queries require GROUP BY time(...)".to_string(),
        )
    })?;
    if gb.time_dimension().is_none() {
        return Err(HyperbytedbError::QueryParse(
            "GROUP BY must include a time(...) interval".to_string(),
        ));
    }
    Ok(())
}

fn validate_delete_predicate(expr: &Expr) -> Result<(), HyperbytedbError> {
    if !expr_is_time_only(expr) {
        return Err(HyperbytedbError::QueryParse(
            "only time predicates are supported in DELETE WHERE clause \
             (tag and field filters are not supported)"
                .to_string(),
        ));
    }
    Ok(())
}

fn expr_is_time_only(expr: &Expr) -> bool {
    match expr {
        Expr::BinaryExpr(b) => expr_is_time_only(&b.left) && expr_is_time_only(&b.right),
        Expr::UnaryExpr(_, inner) => expr_is_time_only(inner),
        Expr::Identifier(name) => name == "time",
        Expr::StringLiteral(_)
        | Expr::TimeLiteral(_)
        | Expr::DurationLiteral(_)
        | Expr::IntegerLiteral(_)
        | Expr::FloatLiteral(_) => true,
        Expr::Now => true,
        Expr::Call(fc) if fc.name.eq_ignore_ascii_case("now") => true,
        _ => false,
    }
}

fn parse_create_retention_policy(cur: &mut TokenCursor<'_>) -> Result<Statement, HyperbytedbError> {
    let name = cur.take_ident()?;
    let db = parse_on_db(cur)?;
    let mut duration = None;
    let mut replication = None;
    let mut shard_duration = None;
    let mut is_default = false;
    while let Some(tok) = cur.peek() {
        match &tok.kind {
            TokenKind::Keyword(k) if k == "DURATION" => {
                cur.bump();
                duration = Some(parse_duration_token(cur)?);
            }
            TokenKind::Keyword(k) if k == "REPLICATION" => {
                cur.bump();
                replication = Some(parse_replication(cur)?);
            }
            TokenKind::Keyword(k) if k == "SHARD" => {
                cur.bump();
                cur.expect_keyword("DURATION")?;
                shard_duration = Some(parse_duration_token(cur)?);
            }
            TokenKind::Keyword(k) if k == "DEFAULT" => {
                cur.bump();
                is_default = true;
            }
            _ => break,
        }
    }
    let duration = duration.ok_or_else(|| {
        HyperbytedbError::QueryParse("CREATE RETENTION POLICY requires DURATION".to_string())
    })?;
    let replication = replication.ok_or_else(|| {
        HyperbytedbError::QueryParse("CREATE RETENTION POLICY requires REPLICATION".to_string())
    })?;
    let duration_is_infinite = duration.value == 0 && duration.unit == DurationUnit::Second;
    if !duration_is_infinite {
        validate_rp_duration(&duration)?;
    }
    if let Some(ref sd) = shard_duration {
        validate_rp_duration(sd)?;
    }
    Ok(Statement::CreateRetentionPolicyStmt {
        name,
        db,
        duration: if duration_is_infinite {
            None
        } else {
            Some(duration)
        },
        replication,
        shard_duration,
        is_default,
    })
}

fn validate_rp_duration(d: &Duration) -> Result<(), HyperbytedbError> {
    let nanos = d.to_nanos();
    if nanos < 0 {
        return Err(HyperbytedbError::QueryParse(
            "retention policy duration must not be negative".to_string(),
        ));
    }
    if nanos > 0 && nanos < MIN_RP_DURATION_NANOS {
        return Err(HyperbytedbError::QueryParse(
            "retention policy duration must be at least 1h or infinite (0/INF)".to_string(),
        ));
    }
    Ok(())
}

fn parse_create_user(cur: &mut TokenCursor<'_>) -> Result<Statement, HyperbytedbError> {
    let username = cur.take_ident()?;
    let mut password = String::new();
    let mut admin = false;
    // InfluxQL: CREATE USER u WITH PASSWORD 'p' [WITH ALL PRIVILEGES] — the
    // clauses come as separate WITH blocks in either order. Consuming only the
    // first one silently created a non-admin user from the canonical admin
    // statement.
    while cur.match_keyword("WITH") {
        if cur.match_keyword("ALL") {
            let _ = cur.match_keyword("PRIVILEGES");
            admin = true;
        } else if cur.match_keyword("PASSWORD") {
            password = parse_password_value(cur)?;
        } else {
            return Err(HyperbytedbError::QueryParse(
                "expected PASSWORD or ALL PRIVILEGES after WITH".to_string(),
            ));
        }
    }
    Ok(Statement::CreateUser {
        username,
        password,
        admin,
    })
}

fn parse_set_password(cur: &mut TokenCursor<'_>) -> Result<Statement, HyperbytedbError> {
    cur.expect_keyword("SET")?;
    cur.expect_keyword("PASSWORD")?;
    let password = if cur.match_keyword("FOR") {
        let user = cur.take_ident()?;
        // `=` lexes as TokenKind::Eq, not a keyword, so match it directly.
        match cur.bump() {
            Some(Token {
                kind: TokenKind::Eq,
                ..
            }) => {}
            other => {
                return Err(HyperbytedbError::QueryParse(format!(
                    "expected '=' in SET PASSWORD, found {:?}",
                    other.map(|t| t.kind)
                )));
            }
        }
        let pw = parse_password_value(cur)?;
        return Ok(Statement::SetPassword {
            username: user,
            password: pw,
        });
    } else {
        parse_password_value(cur)?
    };
    if cur.match_keyword("FOR") {
        let user = cur.take_ident()?;
        return Ok(Statement::SetPassword {
            username: user,
            password,
        });
    }
    Err(HyperbytedbError::QueryParse(
        "expected SET PASSWORD FOR".to_string(),
    ))
}

fn parse_grant(cur: &mut TokenCursor<'_>) -> Result<Statement, HyperbytedbError> {
    cur.expect_keyword("GRANT")?;
    cur.expect_keyword("ALL")?;
    let _ = cur.match_keyword("PRIVILEGES");
    let database = if cur.match_keyword("ON") {
        Some(cur.take_ident()?)
    } else {
        None
    };
    cur.expect_keyword("TO")?;
    let username = cur.take_ident()?;
    Ok(Statement::Grant { username, database })
}

fn parse_revoke(cur: &mut TokenCursor<'_>) -> Result<Statement, HyperbytedbError> {
    cur.expect_keyword("REVOKE")?;
    cur.expect_keyword("ALL")?;
    let _ = cur.match_keyword("PRIVILEGES");
    let database = if cur.match_keyword("ON") {
        Some(cur.take_ident()?)
    } else {
        None
    };
    cur.expect_keyword("FROM")?;
    let username = cur.take_ident()?;
    Ok(Statement::Revoke { username, database })
}

fn parse_create_continuous_query(cur: &mut TokenCursor<'_>) -> Result<Statement, HyperbytedbError> {
    let name = cur.take_ident()?;
    let database = parse_on_db(cur)?;
    let mut resample_every = None;
    let mut resample_for = None;
    if cur.match_keyword("RESAMPLE") {
        if cur.match_keyword("EVERY") {
            resample_every = Some(parse_duration_token(cur)?);
        }
        if cur.match_keyword("FOR") {
            resample_for = Some(parse_duration_token(cur)?);
        }
    }
    let (raw_query, select_stmt) = extract_begin_end_select(cur)?;
    validate_cq_mv_select(&select_stmt)?;
    Ok(Statement::CreateContinuousQuery(
        CreateContinuousQueryStatement {
            name,
            database,
            query: select_stmt,
            raw_query,
            resample_every,
            resample_for,
        },
    ))
}

fn parse_create_materialized_view(
    cur: &mut TokenCursor<'_>,
) -> Result<Statement, HyperbytedbError> {
    let name = cur.take_ident()?;
    let database = parse_on_db(cur)?;
    let (raw_query, select_stmt) = if cur.match_keyword("AS") {
        let start = cur.peek().map(|t| t.start).unwrap_or(0);
        let inner = cur.input[start..].trim();
        let stmt = crate::timeseriesql::parser::parse_select_statement(inner)?;
        validate_cq_mv_select(&stmt)?;
        // The SELECT body was consumed as a raw slice; drain its tokens so the
        // trailing-input check in `parse_ddl_statement` doesn't fire. (Guard on
        // peek: `bump()` returns the Eof token without advancing.)
        while cur.peek().is_some() {
            cur.bump();
        }
        (inner.to_string(), stmt)
    } else {
        let (raw, stmt) = extract_begin_end_select(cur)?;
        validate_cq_mv_select(&stmt)?;
        (raw, stmt)
    };
    Ok(Statement::CreateMaterializedView(
        CreateMaterializedViewStatement {
            name,
            database,
            query: select_stmt,
            raw_query,
        },
    ))
}

fn extract_begin_end_select(
    cur: &mut TokenCursor<'_>,
) -> Result<(String, SelectStatement), HyperbytedbError> {
    let begin_tok = cur
        .peek()
        .ok_or_else(|| HyperbytedbError::QueryParse("expected BEGIN".to_string()))?
        .clone();
    cur.expect_keyword("BEGIN")?;
    let inner_start = begin_tok.end;
    let inner_end = find_matching_end(cur.input, inner_start)?;
    let inner = cur.input[inner_start..inner_end].trim();
    let stmt = crate::timeseriesql::parser::parse_select_statement(inner)?;
    while let Some(tok) = cur.peek() {
        if tok.start >= inner_end {
            break;
        }
        cur.bump();
    }
    cur.match_keyword("END");
    Ok((inner.to_string(), stmt))
}

fn find_matching_end(input: &str, after_begin: usize) -> Result<usize, HyperbytedbError> {
    let tokens = tokenize(input)?;
    let mut depth = 1i32;
    for tok in &tokens {
        if tok.start < after_begin {
            continue;
        }
        match &tok.kind {
            TokenKind::Keyword(k) if k == "BEGIN" => depth += 1,
            TokenKind::Keyword(k) if k == "END" => {
                depth -= 1;
                if depth == 0 {
                    return Ok(tok.start);
                }
            }
            _ => {}
        }
    }
    Err(HyperbytedbError::QueryParse(
        "missing END in CREATE CONTINUOUS QUERY".to_string(),
    ))
}

fn parse_on_db(cur: &mut TokenCursor<'_>) -> Result<String, HyperbytedbError> {
    cur.expect_keyword("ON")?;
    cur.take_ident()
}

fn parse_optional_on_db(cur: &mut TokenCursor<'_>) -> Result<Option<String>, HyperbytedbError> {
    if cur.match_keyword("ON") {
        Ok(Some(cur.take_ident()?))
    } else {
        Ok(None)
    }
}

fn parse_optional_from_measurement(
    cur: &mut TokenCursor<'_>,
) -> Result<Option<Measurement>, HyperbytedbError> {
    if cur.match_keyword("FROM") {
        Ok(Some(parse_measurement_ref(cur)?))
    } else {
        Ok(None)
    }
}

/// Parse `[ON db]` and `[FROM measurement]` in either order (InfluxQL's SHOW
/// grammar puts ON first; the previous FROM-then-ON parsing silently dropped
/// both clauses for the documented order).
fn parse_show_on_from(
    cur: &mut TokenCursor<'_>,
) -> Result<(Option<String>, Option<Measurement>), HyperbytedbError> {
    let mut database = None;
    let mut from = None;
    loop {
        if database.is_none()
            && let Some(db) = parse_optional_on_db(cur)?
        {
            database = Some(db);
            continue;
        }
        if from.is_none()
            && let Some(m) = parse_optional_from_measurement(cur)?
        {
            from = Some(m);
            continue;
        }
        break;
    }
    Ok((database, from))
}

fn parse_measurement_ref(cur: &mut TokenCursor<'_>) -> Result<Measurement, HyperbytedbError> {
    let first = cur.take_ident()?;
    if matches!(
        cur.peek(),
        Some(Token {
            kind: TokenKind::Dot,
            ..
        })
    ) {
        cur.bump();
        let second = cur.take_ident()?;
        if matches!(
            cur.peek(),
            Some(Token {
                kind: TokenKind::Dot,
                ..
            })
        ) {
            cur.bump();
            let third = cur.take_ident()?;
            Ok(Measurement {
                database: Some(first),
                retention_policy: Some(second),
                name: MeasurementName::Name(third),
            })
        } else {
            Ok(Measurement {
                database: None,
                retention_policy: Some(first),
                name: MeasurementName::Name(second),
            })
        }
    } else {
        Ok(Measurement {
            database: None,
            retention_policy: None,
            name: MeasurementName::Name(first),
        })
    }
}

fn parse_measurement_name(cur: &mut TokenCursor<'_>) -> Result<MeasurementName, HyperbytedbError> {
    let m = parse_measurement_ref(cur)?;
    Ok(m.name)
}

fn parse_with_key(cur: &mut TokenCursor<'_>) -> Result<TagKeySelector, HyperbytedbError> {
    if cur.match_keyword("WITH") {
        cur.expect_keyword("KEY")?;
        if matches!(
            cur.peek(),
            Some(Token {
                kind: TokenKind::Eq,
                ..
            })
        ) {
            cur.bump();
        }
        if matches!(
            cur.peek(),
            Some(Token {
                kind: TokenKind::Star,
                ..
            })
        ) {
            cur.bump();
            return Ok(TagKeySelector::All);
        }
        let key = cur.take_ident()?;
        return Ok(TagKeySelector::Eq(key));
    }
    Ok(TagKeySelector::All)
}

fn parse_duration_token(cur: &mut TokenCursor<'_>) -> Result<Duration, HyperbytedbError> {
    const INFINITE: Duration = Duration {
        value: 0,
        unit: DurationUnit::Second,
    };
    // Zero of any unit normalizes to the infinite sentinel (0s) so `DURATION
    // 0m` means infinite like InfluxDB, not a finite zero. Negative durations
    // are rejected outright: downstream retention math casts through u64, so
    // a negative duration wrapped into a cutoff in the future and deleted
    // every point in the measurement.
    let check = |n: i64| -> Result<Duration, HyperbytedbError> {
        if n < 0 {
            return Err(HyperbytedbError::QueryParse(
                "duration must not be negative".to_string(),
            ));
        }
        if n == 0 {
            return Ok(INFINITE);
        }
        Ok(nanos_to_ast_duration(n))
    };
    let tok = cur
        .bump()
        .ok_or_else(|| HyperbytedbError::QueryParse("expected duration".to_string()))?;
    match tok.kind {
        TokenKind::Duration { nanos: None } => Ok(INFINITE),
        TokenKind::Duration { nanos: Some(n) } => check(n),
        TokenKind::Ident(s) if s.eq_ignore_ascii_case("INF") => Ok(INFINITE),
        _ => {
            let text = cur.slice(&tok);
            match parse_duration_text(text)? {
                None => Ok(INFINITE),
                Some(n) => check(n),
            }
        }
    }
}

fn parse_replication(cur: &mut TokenCursor<'_>) -> Result<u32, HyperbytedbError> {
    let tok = cur
        .bump()
        .ok_or_else(|| HyperbytedbError::QueryParse("expected REPLICATION factor".to_string()))?;
    let n = match tok.kind {
        TokenKind::Number(v) => v,
        _ => cur
            .slice(&tok)
            .parse()
            .map_err(|_| HyperbytedbError::QueryParse("invalid REPLICATION".to_string()))?,
    };
    if n < 1 || n > u32::MAX as i64 {
        return Err(HyperbytedbError::QueryParse(
            "REPLICATION must be between 1 and 4294967295".to_string(),
        ));
    }
    Ok(n as u32)
}

fn parse_password_value(cur: &mut TokenCursor<'_>) -> Result<String, HyperbytedbError> {
    let tok = cur
        .bump()
        .ok_or_else(|| HyperbytedbError::QueryParse("expected password".to_string()))?;
    match tok.kind {
        TokenKind::StringLit(s) => Ok(s),
        TokenKind::Ident(s) => Ok(s),
        _ => Err(HyperbytedbError::QueryParse(
            "expected quoted password".to_string(),
        )),
    }
}

fn parse_u64_token(cur: &mut TokenCursor<'_>) -> Result<u64, HyperbytedbError> {
    let tok = cur
        .bump()
        .ok_or_else(|| HyperbytedbError::QueryParse("expected number".to_string()))?;
    match tok.kind {
        TokenKind::Number(v) if v >= 0 => Ok(v as u64),
        _ => Err(HyperbytedbError::QueryParse("invalid number".to_string())),
    }
}

fn slice_remaining_clause(
    cur: &mut TokenCursor<'_>,
    stop_kws: &[&str],
) -> Result<String, HyperbytedbError> {
    let start = cur.peek().map(|t| t.start).unwrap_or(cur.input.len());
    let mut end = cur.input.len();
    // Consume tokens up to (but not including) a stop keyword, leaving the
    // cursor on it so the caller can still parse trailing clauses such as
    // LIMIT/OFFSET. With no stop keywords the whole remainder is taken.
    while let Some(tok) = cur.peek() {
        if let TokenKind::Keyword(k) = &tok.kind
            && stop_kws.contains(&k.as_str())
        {
            end = tok.start;
            break;
        }
        cur.bump();
    }
    Ok(cur.input[start..end].trim().to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn set_password_for_with_eq() {
        let stmt = parse_ddl_statement(r#"SET PASSWORD FOR "alice" = 'secret'"#).unwrap();
        match stmt {
            Statement::SetPassword { username, password } => {
                assert_eq!(username, "alice");
                assert_eq!(password, "secret");
            }
            other => panic!("expected SetPassword, got {other:?}"),
        }
    }

    #[test]
    fn create_database_with_name_clause() {
        // Exercises both the NAME keyword and the WITH loop terminating at EOF.
        let stmt = parse_ddl_statement(
            r#"CREATE DATABASE "db" WITH DURATION 1w REPLICATION 1 NAME "myrp""#,
        )
        .unwrap();
        match stmt {
            Statement::CreateDatabase(s) => {
                assert_eq!(s.name, "db");
                assert_eq!(s.replication, Some(1));
                assert_eq!(s.rp_name.as_deref(), Some("myrp"));
            }
            other => panic!("expected CreateDatabase, got {other:?}"),
        }
    }

    #[test]
    fn create_database_with_options_at_eof() {
        // WITH options that run to end of input must not error "incomplete WITH".
        let stmt =
            parse_ddl_statement(r#"CREATE DATABASE "db" WITH DURATION 1w REPLICATION 1"#).unwrap();
        match stmt {
            Statement::CreateDatabase(s) => {
                assert_eq!(s.duration.map(|d| d.unit), Some(DurationUnit::Week));
                assert_eq!(s.replication, Some(1));
            }
            other => panic!("expected CreateDatabase, got {other:?}"),
        }
    }

    #[test]
    fn show_series_limit_zero() {
        let stmt = parse_ddl_statement("SHOW SERIES LIMIT 0").unwrap();
        match stmt {
            Statement::ShowSeries(s) => assert_eq!(s.limit, Some(0)),
            other => panic!("expected ShowSeries, got {other:?}"),
        }
    }

    #[test]
    fn show_measurements_offset_zero() {
        let stmt = parse_ddl_statement("SHOW MEASUREMENTS OFFSET 0").unwrap();
        match stmt {
            Statement::ShowMeasurements(s) => assert_eq!(s.offset, Some(0)),
            other => panic!("expected ShowMeasurements, got {other:?}"),
        }
    }

    #[test]
    fn show_series_where_then_limit() {
        // LIMIT after a WHERE clause must be captured, not consumed with the
        // WHERE text.
        let stmt = parse_ddl_statement(r#"SHOW SERIES WHERE "host" = 'a' LIMIT 5"#).unwrap();
        match stmt {
            Statement::ShowSeries(s) => {
                assert!(s.condition.is_some());
                assert_eq!(s.limit, Some(5));
            }
            other => panic!("expected ShowSeries, got {other:?}"),
        }
    }

    #[test]
    fn show_series_regex_where_then_limit() {
        // A regex predicate must tokenize cleanly (inner keyword-like content
        // skipped) and the trailing LIMIT must still be parsed.
        let stmt =
            parse_ddl_statement(r#"SHOW SERIES WHERE "host" =~ /^prod LIMIT/ LIMIT 3"#).unwrap();
        match stmt {
            Statement::ShowSeries(s) => {
                assert!(s.condition.is_some());
                assert_eq!(s.limit, Some(3));
            }
            other => panic!("expected ShowSeries, got {other:?}"),
        }
    }

    #[test]
    fn delete_where_with_division_does_not_abort() {
        // A lone `/` (division) in a predicate must not abort tokenization with
        // an "unterminated regex literal" error.
        let stmt = parse_ddl_statement("DELETE FROM cpu WHERE time > 1000/2").unwrap();
        assert!(matches!(stmt, Statement::Delete(_)));
    }

    #[test]
    fn drop_measurement_with_rp() {
        let stmt =
            parse_ddl_statement(r#"DROP MEASUREMENT "default_high"."server_stats""#).unwrap();
        match stmt {
            Statement::DropMeasurement { name, rp } => {
                assert_eq!(name, "server_stats");
                assert_eq!(rp.as_deref(), Some("default_high"));
            }
            other => panic!("expected DropMeasurement, got {other:?}"),
        }
    }

    #[test]
    fn drop_measurement_without_rp() {
        let stmt = parse_ddl_statement(r#"DROP MEASUREMENT "server_stats""#).unwrap();
        match stmt {
            Statement::DropMeasurement { name, rp } => {
                assert_eq!(name, "server_stats");
                assert!(rp.is_none());
            }
            other => panic!("expected DropMeasurement, got {other:?}"),
        }
    }

    #[test]
    fn drop_measurement_rejects_regex() {
        let result = parse_ddl_statement(r#"DROP MEASUREMENT /^server/"#);
        assert!(result.is_err());
    }

    #[test]
    fn create_user_password_then_all_privileges_is_admin() {
        let stmt =
            parse_ddl_statement("CREATE USER paul WITH PASSWORD 'pw' WITH ALL PRIVILEGES").unwrap();
        match stmt {
            Statement::CreateUser {
                username,
                password,
                admin,
            } => {
                assert_eq!(username, "paul");
                assert_eq!(password, "pw");
                assert!(admin, "second WITH block must set admin");
            }
            other => panic!("expected CreateUser, got {other:?}"),
        }
    }

    #[test]
    fn create_user_all_privileges_then_password_is_admin() {
        let stmt =
            parse_ddl_statement("CREATE USER paul WITH ALL PRIVILEGES WITH PASSWORD 'pw'").unwrap();
        match stmt {
            Statement::CreateUser {
                password, admin, ..
            } => {
                assert_eq!(password, "pw");
                assert!(admin);
            }
            other => panic!("expected CreateUser, got {other:?}"),
        }
    }

    #[test]
    fn negative_durations_rejected_everywhere() {
        for q in [
            "CREATE RETENTION POLICY rp ON db DURATION -1h REPLICATION 1",
            "CREATE RETENTION POLICY rp ON db DURATION 2h REPLICATION 1 SHARD DURATION -30m",
            "CREATE DATABASE db WITH DURATION -1h",
            "ALTER RETENTION POLICY rp ON db DURATION -1h",
        ] {
            assert!(
                parse_ddl_statement(q).is_err(),
                "negative duration must be rejected: {q}"
            );
        }
    }

    #[test]
    fn zero_duration_any_unit_means_infinite() {
        let stmt =
            parse_ddl_statement("CREATE RETENTION POLICY rp ON db DURATION 0m REPLICATION 1")
                .unwrap();
        match stmt {
            Statement::CreateRetentionPolicyStmt { duration, .. } => {
                assert!(duration.is_none(), "0m must mean infinite");
            }
            other => panic!("expected CreateRetentionPolicyStmt, got {other:?}"),
        }
        let stmt = parse_ddl_statement("CREATE DATABASE db WITH DURATION 0d").unwrap();
        match stmt {
            Statement::CreateDatabase(s) => {
                assert!(s.duration.is_none(), "0d must mean infinite");
            }
            other => panic!("expected CreateDatabase, got {other:?}"),
        }
    }

    #[test]
    fn show_statements_accept_on_before_from() {
        let stmt =
            parse_ddl_statement(r#"SHOW TAG VALUES ON mydb FROM cpu WITH KEY = "host""#).unwrap();
        match stmt {
            Statement::ShowTagValues(s) => {
                assert_eq!(s.database.as_deref(), Some("mydb"));
                assert!(s.from.is_some(), "FROM cpu must be captured");
                assert!(matches!(s.tag_key, TagKeySelector::Eq(ref k) if k == "host"));
            }
            other => panic!("expected ShowTagValues, got {other:?}"),
        }
        let stmt = parse_ddl_statement("SHOW TAG KEYS ON mydb FROM cpu").unwrap();
        match stmt {
            Statement::ShowTagKeys(s) => {
                assert_eq!(s.database.as_deref(), Some("mydb"));
                assert!(s.from.is_some());
            }
            other => panic!("expected ShowTagKeys, got {other:?}"),
        }
        let stmt = parse_ddl_statement("SHOW FIELD KEYS ON db FROM cpu").unwrap();
        match stmt {
            Statement::ShowFieldKeys(s) => {
                assert_eq!(s.database.as_deref(), Some("db"));
                assert!(s.from.is_some());
            }
            other => panic!("expected ShowFieldKeys, got {other:?}"),
        }
        let stmt = parse_ddl_statement(r#"SHOW SERIES ON db FROM cpu WHERE "host" = 'a'"#).unwrap();
        match stmt {
            Statement::ShowSeries(s) => {
                assert_eq!(s.database.as_deref(), Some("db"));
                assert!(s.from.is_some());
                assert!(s.condition.is_some(), "WHERE must be captured");
            }
            other => panic!("expected ShowSeries, got {other:?}"),
        }
    }

    #[test]
    fn trailing_garbage_rejected() {
        assert!(parse_ddl_statement("DROP DATABASE foo bar baz").is_err());
        let err = parse_ddl_statement("SHOW MEASUREMENTS WITH MEASUREMENT =~ /cpu/").unwrap_err();
        assert!(
            err.to_string().contains("not supported"),
            "WITH MEASUREMENT must fail loudly, got: {err}"
        );
    }

    #[test]
    fn replication_out_of_u32_range_rejected() {
        assert!(
            parse_ddl_statement(
                "CREATE RETENTION POLICY rp ON db DURATION 1h REPLICATION 4294967296"
            )
            .is_err()
        );
    }

    #[test]
    fn keyword_named_identifiers_keep_source_spelling() {
        match parse_ddl_statement("CREATE DATABASE offset").unwrap() {
            Statement::CreateDatabase(s) => assert_eq!(s.name, "offset"),
            other => panic!("expected CreateDatabase, got {other:?}"),
        }
        match parse_ddl_statement("DROP DATABASE Field").unwrap() {
            Statement::DropDatabase(name) => assert_eq!(name, "Field"),
            other => panic!("expected DropDatabase, got {other:?}"),
        }
    }

    #[test]
    fn create_materialized_view_as_select_survives_trailing_check() {
        let stmt = parse_ddl_statement(
            r#"CREATE MATERIALIZED VIEW mv ON db AS SELECT mean("v") FROM m GROUP BY time(5m)"#,
        );
        assert!(stmt.is_ok(), "AS-form MV must parse: {stmt:?}");
    }
}
