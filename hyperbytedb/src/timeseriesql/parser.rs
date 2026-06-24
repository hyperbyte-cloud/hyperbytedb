use crate::error::HyperbytedbError;
use crate::timeseriesql::ast::*;
use crate::timeseriesql::ddl_parser;
use crate::timeseriesql::lexer;

pub fn parse_query(input: &str) -> Result<Vec<Statement>, HyperbytedbError> {
    let input = input.trim();
    if input.is_empty() {
        return Err(HyperbytedbError::QueryParse("empty query".to_string()));
    }

    let statements: Vec<String> = lexer::split_statements(input)?;
    let mut result = Vec::new();

    for stmt_str in statements {
        let stmt_str = stmt_str.trim();
        if stmt_str.is_empty() {
            continue;
        }
        let stmt = parse_statement(stmt_str)?;
        result.push(stmt);
    }

    if result.is_empty() {
        return Err(HyperbytedbError::QueryParse("empty query".to_string()));
    }

    Ok(result)
}

/// Parse a SELECT statement body (used by CQ/MV DDL).
pub(crate) fn parse_select_statement(input: &str) -> Result<SelectStatement, HyperbytedbError> {
    match parse_select(input)? {
        Statement::Select(s) => Ok(s),
        _ => Err(HyperbytedbError::QueryParse(
            "expected SELECT statement".to_string(),
        )),
    }
}

/// Parse a WHERE/expression fragment (used by DDL).
pub(crate) fn parse_expr_str(input: &str) -> Result<Expr, HyperbytedbError> {
    parse_expr(input)
}

fn parse_statement(input: &str) -> Result<Statement, HyperbytedbError> {
    if starts_with_keyword(input, "SELECT") {
        parse_select(input)
    } else {
        ddl_parser::parse_ddl_statement(input)
    }
}

fn starts_with_keyword(input: &str, kw: &str) -> bool {
    let trimmed = input.trim_start();
    if trimmed.len() < kw.len() || !trimmed[..kw.len()].eq_ignore_ascii_case(kw) {
        return false;
    }
    !matches!(trimmed.as_bytes().get(kw.len()), Some(b) if b.is_ascii_alphanumeric() || *b == b'_')
}

fn parse_select(input: &str) -> Result<Statement, HyperbytedbError> {
    let trimmed = input.trim_start();
    let remaining = if trimmed.len() >= 6 && trimmed[..6].eq_ignore_ascii_case("SELECT") {
        trimmed[6..].trim()
    } else {
        input[6..].trim()
    };

    let mut stmt = SelectStatement {
        fields: Vec::new(),
        into: None,
        from: Vec::new(),
        condition: None,
        group_by: None,
        order_by: None,
        limit: None,
        offset: None,
        slimit: None,
        soffset: None,
        fill: None,
        timezone: None,
    };

    let parts = split_clauses(remaining);

    // Parse field list
    let fields_str = parts.get("fields").ok_or_else(|| {
        HyperbytedbError::QueryParse("expected field list after SELECT".to_string())
    })?;
    stmt.fields = parse_field_list(fields_str)?;

    // Parse INTO
    if let Some(into_str) = parts.get("into") {
        stmt.into = Some(parse_measurement(into_str.trim())?);
    }

    // Parse FROM
    if let Some(from_str) = parts.get("from") {
        stmt.from = parse_from_sources(from_str)?;
    } else {
        return Err(HyperbytedbError::QueryParse(
            "found EOF, expected FROM".to_string(),
        ));
    }

    // Parse WHERE — strip any trailing fill(...) that Grafana may attach without GROUP BY
    if let Some(where_str) = parts.get("where") {
        let (where_clean, standalone_fill) = strip_trailing_fill(where_str);
        stmt.condition = Some(parse_expr(&where_clean)?);
        if standalone_fill.is_some() {
            stmt.fill = standalone_fill;
        }
    }

    // Parse GROUP BY
    if let Some(gb_str) = parts.get("group_by") {
        let (group_by, fill) = parse_group_by_clause(gb_str)?;
        stmt.group_by = Some(group_by);
        if fill.is_some() {
            stmt.fill = fill;
        }
    }

    // Parse ORDER BY
    if let Some(ob_str) = parts.get("order_by") {
        stmt.order_by = Some(parse_order_by(ob_str)?);
    }

    // Parse LIMIT / OFFSET / SLIMIT / SOFFSET
    if let Some(v) = parts.get("limit") {
        stmt.limit = Some(
            v.trim()
                .parse::<u64>()
                .map_err(|_| HyperbytedbError::QueryParse(format!("invalid LIMIT: {v}")))?,
        );
    }
    if let Some(v) = parts.get("offset") {
        stmt.offset = Some(
            v.trim()
                .parse::<u64>()
                .map_err(|_| HyperbytedbError::QueryParse(format!("invalid OFFSET: {v}")))?,
        );
    }
    if let Some(v) = parts.get("slimit") {
        stmt.slimit = Some(
            v.trim()
                .parse::<u64>()
                .map_err(|_| HyperbytedbError::QueryParse(format!("invalid SLIMIT: {v}")))?,
        );
    }
    if let Some(v) = parts.get("soffset") {
        stmt.soffset = Some(
            v.trim()
                .parse::<u64>()
                .map_err(|_| HyperbytedbError::QueryParse(format!("invalid SOFFSET: {v}")))?,
        );
    }

    // Parse TZ
    if let Some(tz_str) = parts.get("tz") {
        stmt.timezone = Some(tz_str.trim().trim_matches('\'').to_string());
    }

    Ok(Statement::Select(stmt))
}

/// Split a SELECT body into clause segments using case-insensitive keyword matching.
fn split_clauses(input: &str) -> std::collections::HashMap<String, String> {
    let mut result = std::collections::HashMap::new();
    let upper = input.to_uppercase();

    let keywords = [
        "INTO", "FROM", "WHERE", "GROUP BY", "ORDER BY", "LIMIT", "OFFSET", "SLIMIT", "SOFFSET",
        "TZ",
    ];

    let mut positions: Vec<(usize, &str)> = Vec::new();
    for kw in &keywords {
        let mut search_from = 0;
        while let Some(pos) = find_keyword_position(&upper, kw, search_from) {
            positions.push((pos, kw));
            search_from = pos + kw.len();
        }
    }
    positions.sort_by_key(|(pos, _)| *pos);

    // Everything before first keyword is the fields
    let first_kw_pos = positions
        .first()
        .map(|(pos, _)| *pos)
        .unwrap_or(input.len());
    result.insert(
        "fields".to_string(),
        input[..first_kw_pos].trim().to_string(),
    );

    for (i, (pos, kw)) in positions.iter().enumerate() {
        let start = pos + kw.len();
        let end = if i + 1 < positions.len() {
            positions[i + 1].0
        } else {
            input.len()
        };
        let key = kw.to_lowercase().replace(' ', "_");
        result.insert(key, input[start..end].trim().to_string());
    }

    result
}

fn find_keyword_position(upper: &str, keyword: &str, start: usize) -> Option<usize> {
    let bytes = upper.as_bytes();
    let kw_bytes = keyword.as_bytes();
    let kw_len = kw_bytes.len();

    if start + kw_len > bytes.len() {
        return None;
    }

    for i in start..=(bytes.len() - kw_len) {
        if &bytes[i..i + kw_len] == kw_bytes {
            let before_ok = i == 0 || bytes[i - 1].is_ascii_whitespace() || bytes[i - 1] == b')';
            let after_ok = i + kw_len >= bytes.len()
                || bytes[i + kw_len].is_ascii_whitespace()
                || bytes[i + kw_len] == b'(';

            // Don't match inside quoted strings
            let in_quotes = !upper[..i].matches('"').count().is_multiple_of(2)
                || !upper[..i].matches('\'').count().is_multiple_of(2);

            if before_ok && after_ok && !in_quotes {
                return Some(i);
            }
        }
    }
    None
}

fn parse_field_list(input: &str) -> Result<Vec<Field>, HyperbytedbError> {
    let input = input.trim();
    if input == "*" {
        return Ok(vec![Field {
            expr: Expr::Star,
            alias: None,
        }]);
    }

    let parts = split_top_level_commas(input);
    let mut fields = Vec::new();

    for part in parts {
        let part = part.trim();
        fields.push(parse_field_expr(part)?);
    }

    Ok(fields)
}

fn split_top_level_commas(input: &str) -> Vec<&str> {
    let mut parts = Vec::new();
    let mut depth = 0;
    let mut last = 0;
    let mut in_single_quote = false;
    let mut in_double_quote = false;

    for (i, c) in input.char_indices() {
        match c {
            '\'' if !in_double_quote => in_single_quote = !in_single_quote,
            '"' if !in_single_quote => in_double_quote = !in_double_quote,
            '(' if !in_single_quote && !in_double_quote => depth += 1,
            ')' if !in_single_quote && !in_double_quote => depth -= 1,
            ',' if depth == 0 && !in_single_quote && !in_double_quote => {
                parts.push(&input[last..i]);
                last = i + 1;
            }
            _ => {}
        }
    }
    parts.push(&input[last..]);
    parts
}

fn parse_field_expr(input: &str) -> Result<Field, HyperbytedbError> {
    let input = input.trim();

    // Check for AS alias
    let (expr_str, alias) = if let Some(pos) = find_keyword_position(&input.to_uppercase(), "AS", 0)
    {
        let expr_part = input[..pos].trim();
        let alias_part = input[pos + 2..].trim().trim_matches('"');
        (expr_part, Some(alias_part.to_string()))
    } else {
        (input, None)
    };

    let expr = parse_expr(expr_str)?;
    Ok(Field { expr, alias })
}

pub fn parse_expr(input: &str) -> Result<Expr, HyperbytedbError> {
    let input = input.trim();

    // Regex literal must be checked early to avoid treating / as division
    if input.starts_with('/') && input.len() > 1 && input.ends_with('/') {
        return Ok(Expr::Regex(input[1..input.len() - 1].to_string()));
    }

    // Try to parse as binary expression with AND/OR
    if let Some(expr) = try_parse_logical_expr(input)? {
        return Ok(expr);
    }

    // Try comparison operators
    if let Some(expr) = try_parse_comparison_expr(input)? {
        return Ok(expr);
    }

    // Try arithmetic
    if let Some(expr) = try_parse_arithmetic_expr(input)? {
        return Ok(expr);
    }

    // Parse atom
    parse_atom(input)
}

fn try_parse_logical_expr(input: &str) -> Result<Option<Expr>, HyperbytedbError> {
    let upper = input.to_uppercase();
    // Find top-level AND/OR (not inside parens or quotes)
    for op_str in &["AND", "OR"] {
        if let Some(pos) = find_top_level_operator(&upper, op_str) {
            let left = parse_expr(&input[..pos])?;
            let right = parse_expr(&input[pos + op_str.len()..])?;
            let op = if *op_str == "AND" {
                BinaryOp::And
            } else {
                BinaryOp::Or
            };
            return Ok(Some(Expr::BinaryExpr(Box::new(BinaryExpr {
                left,
                op,
                right,
            }))));
        }
    }
    Ok(None)
}

fn try_parse_comparison_expr(input: &str) -> Result<Option<Expr>, HyperbytedbError> {
    let operators = [
        ("=~", BinaryOp::RegexMatch),
        ("!~", BinaryOp::RegexNotMatch),
        ("!=", BinaryOp::Neq),
        ("<>", BinaryOp::Neq),
        ("<=", BinaryOp::Lte),
        (">=", BinaryOp::Gte),
        ("=", BinaryOp::Eq),
        ("<", BinaryOp::Lt),
        (">", BinaryOp::Gt),
    ];

    for (op_str, op) in &operators {
        if let Some(pos) = find_top_level_operator(input, op_str) {
            let left = parse_expr(&input[..pos])?;
            let right = parse_expr(&input[pos + op_str.len()..])?;
            return Ok(Some(Expr::BinaryExpr(Box::new(BinaryExpr {
                left,
                op: op.clone(),
                right,
            }))));
        }
    }
    Ok(None)
}

fn try_parse_arithmetic_expr(input: &str) -> Result<Option<Expr>, HyperbytedbError> {
    for (op_char, op) in &[
        ('+', BinaryOp::Add),
        ('-', BinaryOp::Sub),
        ('*', BinaryOp::Mul),
        ('/', BinaryOp::Div),
    ] {
        let op_str = &op_char.to_string();
        let mut search_from = 0;
        while let Some(pos) = find_top_level_operator_from(input, op_str, search_from) {
            if pos == 0 {
                search_from = pos + 1;
                continue;
            }

            // For `-` and `+`: skip when preceded by another arithmetic operator
            // — that makes it unary negation/plus, not binary subtraction/addition.
            // e.g. `mean("x") * -1` → the `-` is unary, not `mean("x") *` minus `1`.
            if *op_char == '-' || *op_char == '+' {
                let left_trimmed = input[..pos].trim_end();
                if left_trimmed.is_empty() || left_trimmed.ends_with(|c: char| "+-*/(".contains(c))
                {
                    search_from = pos + 1;
                    continue;
                }
            }

            let left = parse_expr(&input[..pos])?;
            let right = parse_expr(&input[pos + 1..])?;
            return Ok(Some(Expr::BinaryExpr(Box::new(BinaryExpr {
                left,
                op: op.clone(),
                right,
            }))));
        }
    }
    Ok(None)
}

fn find_top_level_operator(input: &str, op: &str) -> Option<usize> {
    find_top_level_operator_from(input, op, 0)
}

fn slash_is_regex_start(bytes: &[u8], pos: usize) -> bool {
    let mut j = pos;
    while j > 0 && bytes[j - 1].is_ascii_whitespace() {
        j -= 1;
    }
    j == 0
        || !matches!(
            bytes[j - 1],
            b')' | b'0'..=b'9' | b'a'..=b'z' | b'A'..=b'Z' | b'_' | b'"' | b'\''
        )
}

fn find_top_level_operator_from(input: &str, op: &str, start: usize) -> Option<usize> {
    let mut depth: i32 = 0;
    let mut in_single_quote = false;
    let mut in_double_quote = false;
    let mut in_regex = false;
    let bytes = input.as_bytes();
    let op_bytes = op.as_bytes();

    if op_bytes.len() > bytes.len() {
        return None;
    }

    // Track quoting/depth state for bytes before `start`.
    for (idx, &b) in bytes[..start].iter().enumerate() {
        match b {
            b'\'' if !in_double_quote && !in_regex => in_single_quote = !in_single_quote,
            b'"' if !in_single_quote && !in_regex => in_double_quote = !in_double_quote,
            b'/' if !in_single_quote && !in_double_quote => {
                if in_regex {
                    in_regex = false;
                } else if slash_is_regex_start(bytes, idx) {
                    in_regex = true;
                }
            }
            b'(' if !in_single_quote && !in_double_quote && !in_regex => depth += 1,
            b')' if !in_single_quote && !in_double_quote && !in_regex => depth -= 1,
            _ => {}
        }
    }

    let mut i = start;
    while i < bytes.len() {
        match bytes[i] {
            b'\'' if !in_double_quote && !in_regex => in_single_quote = !in_single_quote,
            b'"' if !in_single_quote && !in_regex => in_double_quote = !in_double_quote,
            b'/' if !in_single_quote && !in_double_quote => {
                if in_regex {
                    in_regex = false;
                } else if slash_is_regex_start(bytes, i) {
                    in_regex = true;
                }
            }
            b'(' if !in_single_quote && !in_double_quote && !in_regex => depth += 1,
            b')' if !in_single_quote && !in_double_quote && !in_regex => depth -= 1,
            _ => {}
        }

        if depth == 0
            && !in_single_quote
            && !in_double_quote
            && !in_regex
            && i + op_bytes.len() <= bytes.len()
        {
            let candidate = if op.chars().all(|c| c.is_alphabetic()) {
                input[i..i + op_bytes.len()].to_uppercase() == op.to_uppercase()
            } else {
                &bytes[i..i + op_bytes.len()] == op_bytes
            };

            if candidate {
                if op.chars().all(|c| c.is_alphabetic()) {
                    let before_ok = i == 0 || bytes[i - 1].is_ascii_whitespace();
                    let after_ok = i + op_bytes.len() >= bytes.len()
                        || bytes[i + op_bytes.len()].is_ascii_whitespace();
                    if before_ok && after_ok {
                        return Some(i);
                    }
                } else {
                    return Some(i);
                }
            }
        }
        i += 1;
    }
    None
}

fn parse_atom(input: &str) -> Result<Expr, HyperbytedbError> {
    let input = input.trim();

    if input.is_empty() {
        return Err(HyperbytedbError::QueryParse(
            "unexpected empty expression".to_string(),
        ));
    }

    // Parenthesized expression
    if input.starts_with('(') && input.ends_with(')') {
        return parse_expr(&input[1..input.len() - 1]);
    }

    // Unary negation: -<expr> (only when the rest isn't a plain number)
    if input.starts_with('-') && input.len() > 1 {
        let rest = input[1..].trim();
        if rest.parse::<i64>().is_err() && rest.parse::<f64>().is_err() {
            let inner = parse_expr(rest)?;
            return Ok(Expr::BinaryExpr(Box::new(BinaryExpr {
                left: Expr::IntegerLiteral(0),
                op: BinaryOp::Sub,
                right: inner,
            })));
        }
    }

    // Star
    if input == "*" {
        return Ok(Expr::Star);
    }

    // now()
    if input.to_uppercase() == "NOW()" {
        return Ok(Expr::Now);
    }

    // Boolean literals
    match input.to_uppercase().as_str() {
        "TRUE" => return Ok(Expr::BooleanLiteral(true)),
        "FALSE" => return Ok(Expr::BooleanLiteral(false)),
        _ => {}
    }

    // Regex literal /pattern/
    if input.starts_with('/') && input.ends_with('/') && input.len() > 1 {
        return Ok(Expr::Regex(input[1..input.len() - 1].to_string()));
    }

    // String literal 'value'
    if input.starts_with('\'') && input.ends_with('\'') {
        let s = input[1..input.len() - 1].replace("\\'", "'");
        // Could be a time literal
        if s.contains('T') && s.contains('-') && (s.ends_with('Z') || s.contains('+')) {
            return Ok(Expr::TimeLiteral(s));
        }
        return Ok(Expr::StringLiteral(s));
    }

    // Function call: NAME(args...)
    if let Some(paren_pos) = input.find('(')
        && input.ends_with(')')
    {
        let func_name = input[..paren_pos].trim().to_uppercase();
        let args_str = &input[paren_pos + 1..input.len() - 1];
        let args = if args_str.trim().is_empty() {
            Vec::new()
        } else {
            split_top_level_commas(args_str)
                .iter()
                .map(|a| parse_expr(a))
                .collect::<Result<Vec<_>, _>>()?
        };
        return Ok(Expr::Call(FunctionCall {
            name: func_name,
            args,
        }));
    }

    // Duration literal: number followed by unit
    if let Some(dur) = try_parse_duration(input) {
        return Ok(Expr::DurationLiteral(dur));
    }

    // Numeric literals
    if let Ok(v) = input.parse::<i64>() {
        return Ok(Expr::IntegerLiteral(v));
    }
    if let Ok(v) = input.parse::<f64>() {
        return Ok(Expr::FloatLiteral(v));
    }

    // Quoted identifier "name"
    if input.starts_with('"') && input.ends_with('"') {
        let name = input[1..input.len() - 1].to_string();
        return Ok(Expr::Identifier(name));
    }

    // Identifier with ::field or ::tag suffix
    if input.contains("::") {
        let parts: Vec<&str> = input.splitn(2, "::").collect();
        let name = parts[0].trim_matches('"').to_string();
        let typ = match parts[1].to_lowercase().as_str() {
            "field" => Some(FieldType::Field),
            "tag" => Some(FieldType::Tag),
            _ => None,
        };
        return Ok(Expr::FieldRef { name, typ });
    }

    // Bare identifier
    Ok(Expr::Identifier(input.to_string()))
}

fn try_parse_duration(input: &str) -> Option<Duration> {
    let input = input.trim();
    let units = [
        ("ns", DurationUnit::Nanosecond),
        ("us", DurationUnit::Microsecond),
        ("µ", DurationUnit::Microsecond),
        ("u", DurationUnit::Microsecond),
        ("ms", DurationUnit::Millisecond),
        ("s", DurationUnit::Second),
        ("m", DurationUnit::Minute),
        ("h", DurationUnit::Hour),
        ("d", DurationUnit::Day),
        ("w", DurationUnit::Week),
    ];

    for (suffix, unit) in &units {
        if let Some(num_str) = input.strip_suffix(suffix)
            && let Ok(value) = num_str.parse::<i64>()
        {
            return Some(Duration {
                value,
                unit: unit.clone(),
            });
        }
    }
    None
}

fn parse_from_sources(input: &str) -> Result<Vec<MeasurementSource>, HyperbytedbError> {
    let input = input.trim();

    // Check for subquery: FROM (SELECT ...)
    if input.starts_with('(') && input.ends_with(')') {
        let inner = &input[1..input.len() - 1].trim();
        if inner.to_uppercase().starts_with("SELECT") {
            let stmt = parse_select(inner)?;
            if let Statement::Select(sub) = stmt {
                return Ok(vec![MeasurementSource::Subquery(Box::new(sub))]);
            }
        }
    }

    let parts = split_top_level_commas(input);
    let mut sources = Vec::new();

    for part in parts {
        let part = part.trim();
        sources.push(MeasurementSource::Concrete(parse_measurement(part)?));
    }

    Ok(sources)
}

fn parse_measurement(input: &str) -> Result<Measurement, HyperbytedbError> {
    let input = input.trim();

    // Regex measurement /pattern/
    if input.starts_with('/') && input.ends_with('/') {
        return Ok(Measurement {
            database: None,
            retention_policy: None,
            name: MeasurementName::Regex(input[1..input.len() - 1].to_string()),
        });
    }

    // Fully qualified: "db"."rp"."measurement" or db.rp.measurement
    let parts: Vec<&str> = input.split('.').collect();
    match parts.len() {
        1 => Ok(Measurement {
            database: None,
            retention_policy: None,
            name: MeasurementName::Name(unquote(parts[0])),
        }),
        2 => Ok(Measurement {
            database: None,
            retention_policy: Some(unquote(parts[0])),
            name: MeasurementName::Name(unquote(parts[1])),
        }),
        3 => Ok(Measurement {
            database: Some(unquote(parts[0])),
            retention_policy: Some(unquote(parts[1])),
            name: MeasurementName::Name(unquote(parts[2])),
        }),
        _ => Err(HyperbytedbError::QueryParse(format!(
            "invalid measurement reference: {input}"
        ))),
    }
}

fn unquote(s: &str) -> String {
    let s = s.trim();
    // Strip ::tag or ::field suffix first (Grafana sends "host"::tag)
    let s = if let Some(pos) = s.find("::") {
        &s[..pos]
    } else {
        s
    };
    let s = s.trim();
    if s.starts_with('"') && s.ends_with('"') && s.len() >= 2 {
        s[1..s.len() - 1].to_string()
    } else {
        s.to_string()
    }
}

fn parse_group_by_clause(input: &str) -> Result<(GroupBy, Option<FillOption>), HyperbytedbError> {
    let mut fill = None;
    let mut dims_str = input.to_string();

    // Check for fill() at end
    let upper = input.to_uppercase();
    if let Some(fill_pos) = upper.rfind("FILL(") {
        let fill_end = input[fill_pos..].find(')').map(|p| fill_pos + p + 1);
        if let Some(fill_end) = fill_end {
            let fill_str = &input[fill_pos + 5..fill_end - 1];
            fill = Some(parse_fill_option(fill_str)?);
            dims_str = input[..fill_pos].trim().to_string();
        }
    }

    let parts = split_top_level_commas(&dims_str);
    let mut dimensions = Vec::new();

    for part in parts {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }

        let upper_part = part.to_uppercase();
        if upper_part.starts_with("TIME(") && part.ends_with(')') {
            let args_str = &part[5..part.len() - 1];
            let args = split_top_level_commas(args_str);

            let interval = try_parse_duration(args[0].trim()).ok_or_else(|| {
                HyperbytedbError::QueryParse(format!("invalid duration in time(): {}", args[0]))
            })?;
            let offset = if args.len() > 1 {
                Some(try_parse_duration(args[1].trim()).ok_or_else(|| {
                    HyperbytedbError::QueryParse(format!("invalid offset in time(): {}", args[1]))
                })?)
            } else {
                None
            };

            dimensions.push(Dimension::Time { interval, offset });
        } else if part == "*" {
            dimensions.push(Dimension::AllTags);
        } else if part.starts_with('/') && part.ends_with('/') {
            dimensions.push(Dimension::Regex(part[1..part.len() - 1].to_string()));
        } else {
            dimensions.push(Dimension::Tag(unquote(part)));
        }
    }

    Ok((GroupBy { dimensions }, fill))
}

/// Strip a trailing `fill(...)` from a clause string (e.g. WHERE or ORDER BY)
/// that Grafana may send even without a GROUP BY clause.
fn strip_trailing_fill(input: &str) -> (String, Option<FillOption>) {
    let upper = input.to_uppercase();
    if let Some(pos) = upper.rfind("FILL(") {
        // Make sure FILL( isn't inside quotes
        let before_fill = &input[..pos];
        let in_quotes = !before_fill.matches('"').count().is_multiple_of(2)
            || !before_fill.matches('\'').count().is_multiple_of(2);
        if !in_quotes && let Some(close) = input[pos..].find(')') {
            let fill_inner = &input[pos + 5..pos + close];
            let rest = input[..pos].trim().to_string();
            if let Ok(f) = parse_fill_option(fill_inner) {
                return (rest, Some(f));
            }
        }
    }
    (input.to_string(), None)
}

fn parse_fill_option(input: &str) -> Result<FillOption, HyperbytedbError> {
    let input = input.trim().to_lowercase();
    // Strip outer fill(...) if present
    let inner = if input.starts_with("fill(") && input.ends_with(')') {
        &input[5..input.len() - 1]
    } else if input.starts_with('(') && input.ends_with(')') {
        &input[1..input.len() - 1]
    } else {
        &input
    };
    let inner = inner.trim();

    match inner {
        "null" => Ok(FillOption::Null),
        "none" => Ok(FillOption::None),
        "previous" => Ok(FillOption::Previous),
        "linear" => Ok(FillOption::Linear),
        other => {
            let val: f64 = other.parse().map_err(|_| {
                HyperbytedbError::QueryParse(format!("invalid fill value: {other}"))
            })?;
            Ok(FillOption::Value(val))
        }
    }
}

fn parse_order_by(input: &str) -> Result<OrderBy, HyperbytedbError> {
    let upper = input.trim().to_uppercase();
    let time_desc = upper.contains("DESC");
    Ok(OrderBy { time_desc })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_select_star() {
        let stmts = parse_query("SELECT * FROM cpu").unwrap();
        assert_eq!(stmts.len(), 1);
        match &stmts[0] {
            Statement::Select(s) => {
                assert_eq!(s.fields.len(), 1);
                assert!(matches!(s.fields[0].expr, Expr::Star));
                assert_eq!(s.from[0].name_str(), Some("cpu"));
            }
            _ => panic!("expected SELECT"),
        }
    }

    #[test]
    fn test_parse_show_databases() {
        let stmts = parse_query("SHOW DATABASES").unwrap();
        assert!(matches!(stmts[0], Statement::ShowDatabases));
    }

    #[test]
    fn test_parse_create_database() {
        let stmts = parse_query("CREATE DATABASE \"mydb\"").unwrap();
        match &stmts[0] {
            Statement::CreateDatabase(stmt) => assert_eq!(stmt.name, "mydb"),
            _ => panic!("expected CREATE DATABASE"),
        }
    }

    #[test]
    fn test_parse_select_with_where_group_by() {
        let q = r#"SELECT mean("value") FROM "cpu" WHERE "host" = 'server01' AND time > now() - 1h GROUP BY time(5m) fill(0)"#;
        let stmts = parse_query(q).unwrap();
        match &stmts[0] {
            Statement::Select(s) => {
                assert_eq!(s.fields.len(), 1);
                assert!(s.condition.is_some());
                assert!(s.group_by.is_some());
                let gb = s.group_by.as_ref().unwrap();
                assert!(gb.time_dimension().is_some());
                assert!(s.fill.is_some());
            }
            _ => panic!("expected SELECT"),
        }
    }

    #[test]
    fn test_parse_multiple_statements() {
        let q = "SHOW DATABASES; SELECT * FROM cpu";
        let stmts = parse_query(q).unwrap();
        assert_eq!(stmts.len(), 2);
    }

    #[test]
    fn test_parse_arithmetic_with_unary_negation() {
        let q = r#"SELECT mean("usage_idle") * -1 + 100 FROM "cpu" WHERE "host" = 'server01' GROUP BY time(10s) fill(null)"#;
        let stmts = parse_query(q).unwrap();
        match &stmts[0] {
            Statement::Select(s) => {
                assert_eq!(s.fields.len(), 1);
                // The field should parse as: (mean("usage_idle") * (-1)) + 100
                match &s.fields[0].expr {
                    Expr::BinaryExpr(outer) => {
                        assert_eq!(outer.op, BinaryOp::Add);
                        match &outer.right {
                            Expr::IntegerLiteral(100) => {}
                            other => panic!("expected IntegerLiteral(100), got {:?}", other),
                        }
                        match &outer.left {
                            Expr::BinaryExpr(inner) => {
                                assert_eq!(inner.op, BinaryOp::Mul);
                                match &inner.right {
                                    Expr::IntegerLiteral(-1) => {}
                                    other => panic!("expected IntegerLiteral(-1), got {:?}", other),
                                }
                            }
                            other => panic!("expected BinaryExpr(Mul), got {:?}", other),
                        }
                    }
                    other => panic!("expected BinaryExpr(Add), got {:?}", other),
                }
            }
            _ => panic!("expected SELECT"),
        }
    }

    #[test]
    fn test_parse_unary_negation_of_function() {
        let q = r#"SELECT -mean("value") FROM "cpu""#;
        let stmts = parse_query(q).unwrap();
        match &stmts[0] {
            Statement::Select(s) => {
                assert_eq!(s.fields.len(), 1);
                // -mean("value") → BinaryExpr(0 - mean("value"))
                match &s.fields[0].expr {
                    Expr::BinaryExpr(be) => {
                        assert_eq!(be.op, BinaryOp::Sub);
                        match &be.left {
                            Expr::IntegerLiteral(0) => {}
                            other => panic!("expected IntegerLiteral(0), got {:?}", other),
                        }
                        match &be.right {
                            Expr::Call(f) => assert_eq!(f.name, "MEAN"),
                            other => panic!("expected Call(MEAN), got {:?}", other),
                        }
                    }
                    other => panic!("expected BinaryExpr(Sub), got {:?}", other),
                }
            }
            _ => panic!("expected SELECT"),
        }
    }

    #[test]
    fn test_parse_fill_without_group_by() {
        let q = r#"SELECT last("uptime") FROM "system" WHERE "host" =~ /^server1$/ AND time >= 1772478673792ms and time <= 1772482273792ms fill(null)"#;
        let stmts = parse_query(q).unwrap();
        match &stmts[0] {
            Statement::Select(s) => {
                assert_eq!(s.fields.len(), 1);
                assert!(s.condition.is_some());
                assert!(s.group_by.is_none());
                assert!(matches!(s.fill, Some(FillOption::Null)));
            }
            _ => panic!("expected SELECT"),
        }
    }

    #[test]
    fn test_parse_delete_from() {
        let stmts = parse_query(r#"DELETE FROM "cpu""#).unwrap();
        match &stmts[0] {
            Statement::Delete(del) => {
                assert_eq!(del.from, "cpu");
                assert!(del.condition.is_none());
            }
            _ => panic!("expected DELETE"),
        }
    }

    #[test]
    fn test_parse_delete_with_time_condition() {
        let stmts =
            parse_query(r#"DELETE FROM "cpu" WHERE time < '2024-01-01T00:00:00Z'"#).unwrap();
        match &stmts[0] {
            Statement::Delete(del) => {
                assert_eq!(del.from, "cpu");
                assert!(del.condition.is_some());
                match del.condition.as_ref().unwrap() {
                    Expr::BinaryExpr(be) => {
                        assert_eq!(be.op, BinaryOp::Lt);
                    }
                    other => panic!("expected BinaryExpr, got {:?}", other),
                }
            }
            _ => panic!("expected DELETE"),
        }
    }

    #[test]
    fn test_parse_delete_with_tag_and_time() {
        let err =
            parse_query(r#"DELETE FROM "cpu" WHERE "host" = 'server01' AND time < now() - 7d"#)
                .unwrap_err();
        assert!(matches!(err, HyperbytedbError::QueryParse(_)));
        assert!(err.to_string().contains("fields not allowed"));
    }

    #[test]
    fn test_parse_delete_where_time_without_from() {
        let stmts = parse_query("DELETE WHERE time < now()").unwrap();
        match &stmts[0] {
            Statement::Delete(del) => {
                assert!(del.from.is_empty());
                assert!(del.condition.is_some());
            }
            _ => panic!("expected DELETE"),
        }
    }

    #[test]
    fn test_parse_delete_requires_measurement() {
        assert!(parse_query("DELETE FROM").is_err());
    }

    #[test]
    fn test_parse_create_continuous_query() {
        let q = r#"CREATE CONTINUOUS QUERY "cq_1h" ON "mydb" BEGIN SELECT mean("value") INTO "cpu_1h" FROM "cpu" GROUP BY time(1h) END"#;
        let stmts = parse_query(q).unwrap();
        match &stmts[0] {
            Statement::CreateContinuousQuery(cq) => {
                assert_eq!(cq.name, "cq_1h");
                assert_eq!(cq.database, "mydb");
                assert!(cq.raw_query.contains("SELECT"));
                assert!(cq.raw_query.contains("mean"));
                assert!(cq.resample_every.is_none());
                assert!(cq.resample_for.is_none());
            }
            _ => panic!("expected CREATE CONTINUOUS QUERY"),
        }
    }

    #[test]
    fn test_parse_create_continuous_query_with_resample() {
        let q = r#"CREATE CONTINUOUS QUERY "cq_5m" ON "mydb" RESAMPLE EVERY 5m FOR 1h BEGIN SELECT mean("value") INTO "cpu_5m" FROM "cpu" GROUP BY time(5m) END"#;
        let stmts = parse_query(q).unwrap();
        match &stmts[0] {
            Statement::CreateContinuousQuery(cq) => {
                assert_eq!(cq.name, "cq_5m");
                assert!(cq.resample_every.is_some());
                let every = cq.resample_every.as_ref().unwrap();
                assert_eq!(every.value, 5);
                assert_eq!(every.unit, DurationUnit::Minute);
                assert!(cq.resample_for.is_some());
                let for_dur = cq.resample_for.as_ref().unwrap();
                assert_eq!(for_dur.value, 1);
                assert_eq!(for_dur.unit, DurationUnit::Hour);
            }
            _ => panic!("expected CREATE CONTINUOUS QUERY"),
        }
    }

    #[test]
    fn test_parse_show_continuous_queries() {
        let stmts = parse_query("SHOW CONTINUOUS QUERIES").unwrap();
        assert!(matches!(stmts[0], Statement::ShowContinuousQueries));
    }

    #[test]
    fn test_parse_drop_continuous_query() {
        let stmts = parse_query(r#"DROP CONTINUOUS QUERY "cq_1h" ON "mydb""#).unwrap();
        match &stmts[0] {
            Statement::DropContinuousQuery { name, db } => {
                assert_eq!(name, "cq_1h");
                assert_eq!(db, "mydb");
            }
            _ => panic!("expected DROP CONTINUOUS QUERY"),
        }
    }

    #[test]
    fn test_parse_create_materialized_view() {
        let q = r#"CREATE MATERIALIZED VIEW "mv_5m" ON "mydb" AS SELECT mean("value") INTO "cpu_5m" FROM "cpu" GROUP BY time(5m), *"#;
        let stmts = parse_query(q).unwrap();
        match &stmts[0] {
            Statement::CreateMaterializedView(mv) => {
                assert_eq!(mv.name, "mv_5m");
                assert_eq!(mv.database, "mydb");
                assert!(mv.query.into.is_some());
                assert!(mv.query.group_by.is_some());
            }
            _ => panic!("expected CREATE MATERIALIZED VIEW"),
        }
    }

    #[test]
    fn test_parse_create_materialized_view_requires_group_by_time() {
        let q = r#"CREATE MATERIALIZED VIEW "mv" ON "mydb" AS SELECT mean("value") INTO "cpu_5m" FROM "cpu""#;
        assert!(parse_query(q).is_err());
    }

    #[test]
    fn test_parse_show_materialized_views() {
        let stmts = parse_query("SHOW MATERIALIZED VIEWS").unwrap();
        assert!(matches!(stmts[0], Statement::ShowMaterializedViews));
    }

    #[test]
    fn test_parse_drop_materialized_view() {
        let stmts = parse_query(r#"DROP MATERIALIZED VIEW "mv_5m" ON "mydb""#).unwrap();
        match &stmts[0] {
            Statement::DropMaterializedView { name, db } => {
                assert_eq!(name, "mv_5m");
                assert_eq!(db, "mydb");
            }
            _ => panic!("expected DROP MATERIALIZED VIEW"),
        }
    }

    #[test]
    fn test_parse_division_after_function_call() {
        let q = r#"SELECT NON_NEGATIVE_DIFFERENCE(mean("packets_recv"))/10 AS "in" FROM "net" GROUP BY time(1s)"#;
        let stmts = parse_query(q).unwrap();
        match &stmts[0] {
            Statement::Select(s) => {
                assert_eq!(s.fields.len(), 1);
                assert_eq!(s.fields[0].alias.as_deref(), Some("in"));
                match &s.fields[0].expr {
                    Expr::BinaryExpr(be) => {
                        assert_eq!(be.op, BinaryOp::Div);
                        match &be.left {
                            Expr::Call(f) => assert_eq!(f.name, "NON_NEGATIVE_DIFFERENCE"),
                            other => panic!("expected Call, got {:?}", other),
                        }
                        match &be.right {
                            Expr::IntegerLiteral(10) => {}
                            other => panic!("expected IntegerLiteral(10), got {:?}", other),
                        }
                    }
                    other => panic!("expected BinaryExpr(Div), got {:?}", other),
                }
            }
            _ => panic!("expected SELECT"),
        }
    }

    #[test]
    fn test_parse_division_with_spaces() {
        let q = r#"SELECT mean("value") / 10 FROM "cpu""#;
        let stmts = parse_query(q).unwrap();
        match &stmts[0] {
            Statement::Select(s) => match &s.fields[0].expr {
                Expr::BinaryExpr(be) => {
                    assert_eq!(be.op, BinaryOp::Div);
                }
                other => panic!("expected BinaryExpr(Div), got {:?}", other),
            },
            _ => panic!("expected SELECT"),
        }
    }

    #[test]
    fn test_division_does_not_break_regex_in_where() {
        let q = r#"SELECT mean("value") / 10 FROM "cpu" WHERE "host" =~ /^server.*/ AND time > now() - 1h GROUP BY time(1s)"#;
        let stmts = parse_query(q).unwrap();
        match &stmts[0] {
            Statement::Select(s) => {
                match &s.fields[0].expr {
                    Expr::BinaryExpr(be) => assert_eq!(be.op, BinaryOp::Div),
                    other => panic!("expected division, got {:?}", other),
                }
                assert!(s.condition.is_some());
            }
            _ => panic!("expected SELECT"),
        }
    }

    #[test]
    fn test_parse_select_into_with_group_by() {
        let q = r#"SELECT mean("value") INTO "cpu_1h" FROM "cpu" WHERE "host" = 'server01' GROUP BY time(1h), "host""#;
        let stmts = parse_query(q).unwrap();
        match &stmts[0] {
            Statement::Select(s) => {
                assert_eq!(s.fields.len(), 1);
                assert!(s.into.is_some());
                let into = s.into.as_ref().unwrap();
                assert_eq!(into.name_str(), Some("cpu_1h"));
                assert_eq!(s.from[0].name_str(), Some("cpu"));
                assert!(s.condition.is_some());
                assert!(s.group_by.as_ref().unwrap().time_dimension().is_some());
                assert_eq!(s.group_by.as_ref().unwrap().tag_dimensions(), vec!["host"]);
            }
            _ => panic!("expected SELECT"),
        }
    }

    #[test]
    fn test_parse_select_into_requires_group_by_time() {
        let q = r#"SELECT mean("value") INTO "cpu_1h" FROM "cpu""#;
        assert!(crate::timeseriesql::parse(q).is_err());
    }

    #[test]
    fn test_parse_select_multiple_from_measurements() {
        let stmts = parse_query(r#"SELECT * FROM "cpu", "memory""#).unwrap();
        match &stmts[0] {
            Statement::Select(s) => {
                assert_eq!(s.from.len(), 2);
                assert_eq!(s.from[0].name_str(), Some("cpu"));
                assert_eq!(s.from[1].name_str(), Some("memory"));
            }
            _ => panic!("expected SELECT"),
        }
    }

    #[test]
    fn test_parse_select_from_retention_policy_qualified_measurement() {
        let stmts =
            parse_query(r#"SELECT * FROM "default_high"."server_stats" WHERE time > now() - 1h"#)
                .unwrap();
        match &stmts[0] {
            Statement::Select(s) => {
                let m = match s.from.first().unwrap() {
                    MeasurementSource::Concrete(m) => m,
                    _ => panic!("expected concrete measurement"),
                };
                assert_eq!(m.retention_policy.as_deref(), Some("default_high"));
                assert_eq!(m.name_str(), Some("server_stats"));
            }
            _ => panic!("expected SELECT"),
        }
    }

    #[test]
    fn test_parse_create_retention_policy_default_high_name_is_not_default_modifier() {
        let stmts = parse_query(
            r#"CREATE RETENTION POLICY "default_high" ON "gameservers" DURATION 52w REPLICATION 1"#,
        )
        .unwrap();
        match &stmts[0] {
            Statement::CreateRetentionPolicyStmt {
                name, is_default, ..
            } => {
                assert_eq!(name, "default_high");
                assert!(!is_default);
            }
            _ => panic!("expected CREATE RETENTION POLICY"),
        }
    }

    #[test]
    fn test_parse_create_retention_policy_explicit_default_modifier() {
        let stmts = parse_query(
            r#"CREATE RETENTION POLICY "myrp" ON "gameservers" DURATION 30d REPLICATION 1 DEFAULT"#,
        )
        .unwrap();
        match &stmts[0] {
            Statement::CreateRetentionPolicyStmt { is_default, .. } => {
                assert!(*is_default);
            }
            _ => panic!("expected CREATE RETENTION POLICY"),
        }
    }

    #[test]
    fn test_parse_show_tag_keys_from_retention_policy() {
        let stmts = parse_query(r#"SHOW TAG KEYS FROM "default_high"."server_stats""#).unwrap();
        match &stmts[0] {
            Statement::ShowTagKeys(s) => {
                let m = s.from.as_ref().unwrap();
                assert_eq!(m.retention_policy.as_deref(), Some("default_high"));
                assert_eq!(m.name_str(), Some("server_stats"));
            }
            _ => panic!("expected SHOW TAG KEYS"),
        }
    }

    #[test]
    fn test_parse_group_by_all_tags() {
        let stmts = parse_query(r#"SELECT mean("value") FROM cpu GROUP BY time(5m), *"#).unwrap();
        match &stmts[0] {
            Statement::Select(s) => {
                let gb = s.group_by.as_ref().unwrap();
                assert!(gb.references_tags());
                assert!(gb.tag_dimensions().is_empty());
                assert!(
                    gb.dimensions
                        .iter()
                        .any(|d| matches!(d, Dimension::AllTags))
                );
            }
            _ => panic!("expected SELECT"),
        }
    }

    #[test]
    fn test_create_user_badmin_is_not_admin() {
        let stmts = parse_query(r#"CREATE USER "badmin" WITH PASSWORD 'secret'"#).unwrap();
        match &stmts[0] {
            Statement::CreateUser {
                username, admin, ..
            } => {
                assert_eq!(username, "badmin");
                assert!(!admin);
            }
            _ => panic!("expected CREATE USER"),
        }
    }

    #[test]
    fn test_create_rp_requires_replication() {
        let err = parse_query(r#"CREATE RETENTION POLICY "rp" ON "db" DURATION 24h"#).unwrap_err();
        assert!(err.to_string().contains("REPLICATION"));
    }

    #[test]
    fn test_create_rp_rejects_sub_one_hour_duration() {
        let err = parse_query(r#"CREATE RETENTION POLICY "rp" ON "db" DURATION 30m REPLICATION 1"#)
            .unwrap_err();
        assert!(err.to_string().contains("1h"));
    }

    #[test]
    fn test_create_cq_resample_not_confused_by_identifiers() {
        let q = r#"CREATE CONTINUOUS QUERY "cq" ON "forecast" RESAMPLE EVERY 1h FOR 2h BEGIN SELECT mean("v") FROM "backend" GROUP BY time(1h) END"#;
        let stmts = parse_query(q).unwrap();
        match &stmts[0] {
            Statement::CreateContinuousQuery(cq) => {
                assert_eq!(cq.database, "forecast");
                assert!(cq.resample_every.is_some());
                assert!(cq.resample_for.is_some());
            }
            _ => panic!("expected CREATE CONTINUOUS QUERY"),
        }
    }

    #[test]
    fn test_reject_single_quoted_identifier_in_create_database() {
        let err = parse_query("CREATE DATABASE 'mydb'").unwrap_err();
        assert!(err.to_string().contains("string literal"));
    }
}
