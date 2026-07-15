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
    // Compare bytes, not `&str` slices: `trimmed[..kw.len()]` can panic on a
    // non-char boundary when the input starts with multi-byte characters.
    let bytes = input.trim_start().as_bytes();
    if bytes.len() < kw.len() || !bytes[..kw.len()].eq_ignore_ascii_case(kw.as_bytes()) {
        return false;
    }
    !matches!(bytes.get(kw.len()), Some(b) if b.is_ascii_alphanumeric() || *b == b'_')
}

fn parse_select(input: &str) -> Result<Statement, HyperbytedbError> {
    let trimmed = input.trim_start();
    if trimmed.len() < 6 || !trimmed.as_bytes()[..6].eq_ignore_ascii_case(b"SELECT") {
        return Err(HyperbytedbError::QueryParse(
            "expected SELECT statement".to_string(),
        ));
    }
    let remaining = trimmed[6..].trim();

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

    let parts = split_clauses(remaining)?;

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

    // Parse TZ — the clause value arrives as `('America/New_York')`, so strip
    // the surrounding parens and quotes.
    if let Some(tz_str) = parts.get("tz") {
        let mut tz = tz_str.trim();
        if let Some(stripped) = tz.strip_prefix('(').and_then(|s| s.strip_suffix(')')) {
            tz = stripped.trim();
        }
        stmt.timezone = Some(tz.trim_matches('\'').to_string());
    }

    Ok(Statement::Select(stmt))
}

/// Per-character scan info produced by [`scan_chars`].
#[derive(Debug, Clone, Copy)]
struct ScannedChar {
    /// Byte offset of the character in the original input (valid for slicing).
    idx: usize,
    ch: char,
    /// Paren depth: 0 for top-level characters (the outermost parens
    /// themselves included), > 0 strictly inside parentheses.
    depth: i32,
    /// True when the character is part of a single-quoted string, a
    /// double-quoted identifier, or a regex literal (delimiters included).
    masked: bool,
}

/// Masking scanner shared by all SELECT-parsing string primitives.
///
/// Walks the ORIGINAL string char by char (never an uppercased copy, whose
/// byte offsets can diverge for chars like `ı`/`ﬁ`) and tracks:
/// - single-quoted string literals, honoring both `\'` and `''` escapes,
/// - double-quoted identifiers (`""` escape) as an independent state — a
///   quote char inside the other quote kind does not toggle,
/// - regex literals `/.../` (with `\/` escape), distinguished from division
///   by [`slash_is_regex_start`],
/// - parenthesis depth.
///
/// The output has exactly one entry per input char, in order.
fn scan_chars(input: &str) -> Vec<ScannedChar> {
    let chars: Vec<(usize, char)> = input.char_indices().collect();
    let mut out = Vec::with_capacity(chars.len());
    let mut depth: i32 = 0;
    let mut i = 0usize;
    while i < chars.len() {
        let (idx, ch) = chars[i];
        match ch {
            '\'' | '"' => {
                let quote = ch;
                out.push(ScannedChar {
                    idx,
                    ch,
                    depth,
                    masked: true,
                });
                i += 1;
                while i < chars.len() {
                    let (jdx, c) = chars[i];
                    out.push(ScannedChar {
                        idx: jdx,
                        ch: c,
                        depth,
                        masked: true,
                    });
                    i += 1;
                    if quote == '\'' && c == '\\' && i < chars.len() {
                        // Backslash escape (`\'`, `\\`) inside a string literal.
                        let (kdx, k) = chars[i];
                        out.push(ScannedChar {
                            idx: kdx,
                            ch: k,
                            depth,
                            masked: true,
                        });
                        i += 1;
                    } else if c == quote {
                        if i < chars.len() && chars[i].1 == quote {
                            // Doubled-quote escape: '' or "".
                            let (kdx, k) = chars[i];
                            out.push(ScannedChar {
                                idx: kdx,
                                ch: k,
                                depth,
                                masked: true,
                            });
                            i += 1;
                        } else {
                            break;
                        }
                    }
                }
            }
            '/' if slash_is_regex_start(&chars, i) => {
                out.push(ScannedChar {
                    idx,
                    ch,
                    depth,
                    masked: true,
                });
                i += 1;
                while i < chars.len() {
                    let (jdx, c) = chars[i];
                    out.push(ScannedChar {
                        idx: jdx,
                        ch: c,
                        depth,
                        masked: true,
                    });
                    i += 1;
                    if c == '\\' && i < chars.len() {
                        let (kdx, k) = chars[i];
                        out.push(ScannedChar {
                            idx: kdx,
                            ch: k,
                            depth,
                            masked: true,
                        });
                        i += 1;
                    } else if c == '/' {
                        break;
                    }
                }
            }
            '(' => {
                out.push(ScannedChar {
                    idx,
                    ch,
                    depth,
                    masked: false,
                });
                depth += 1;
                i += 1;
            }
            ')' => {
                depth -= 1;
                out.push(ScannedChar {
                    idx,
                    ch,
                    depth,
                    masked: false,
                });
                i += 1;
            }
            _ => {
                out.push(ScannedChar {
                    idx,
                    ch,
                    depth,
                    masked: false,
                });
                i += 1;
            }
        }
    }
    out
}

/// Whether a `/` at `chars[pos]` begins a regex literal rather than division.
/// Division follows an operand (identifier, number, `)` or a quoted value);
/// a regex follows start-of-input, an operator/comma/open paren, or a clause
/// keyword that puts the slash in operand position (`FROM /re/`,
/// `GROUP BY /re/`).
fn slash_is_regex_start(chars: &[(usize, char)], pos: usize) -> bool {
    let mut j = pos;
    while j > 0 && chars[j - 1].1.is_whitespace() {
        j -= 1;
    }
    if j == 0 {
        return true;
    }
    let prev = chars[j - 1].1;
    if matches!(prev, ')' | '"' | '\'') {
        return false;
    }
    if prev.is_alphanumeric() || prev == '_' {
        let end = j;
        let mut start = j;
        while start > 0 && (chars[start - 1].1.is_alphanumeric() || chars[start - 1].1 == '_') {
            start -= 1;
        }
        let word: String = chars[start..end].iter().map(|&(_, c)| c).collect();
        return ["FROM", "WHERE", "BY", "AND", "OR"]
            .iter()
            .any(|kw| word.eq_ignore_ascii_case(kw));
    }
    true
}

fn is_keyword_boundary_before(c: char) -> bool {
    c.is_whitespace() || matches!(c, ')' | '\'' | '"')
}

fn is_keyword_boundary_after(c: char) -> bool {
    c.is_whitespace() || matches!(c, '(' | '\'' | '"' | '/')
}

/// Match an ASCII `keyword` ("LIMIT", "GROUP BY", …) at scan index `i`,
/// case-insensitively on the original string. Two-word keywords accept any
/// whitespace run between the words. Only unmasked, top-level (paren depth 0)
/// text matches, and the keyword must be delimited by whitespace, a paren, or
/// a quote on either side (so `(a=1)AND(b=2)` works). Returns the matched
/// byte range `(start, end)`.
fn match_keyword_at(
    input: &str,
    scan: &[ScannedChar],
    i: usize,
    keyword: &str,
) -> Option<(usize, usize)> {
    let sc = scan[i];
    if sc.masked || sc.depth != 0 {
        return None;
    }
    if i > 0 && !is_keyword_boundary_before(scan[i - 1].ch) {
        return None;
    }

    let bytes = input.as_bytes();
    let mut words = keyword.split_ascii_whitespace();
    let first = words.next()?;
    let start = sc.idx;
    if start + first.len() > bytes.len()
        || !bytes[start..start + first.len()].eq_ignore_ascii_case(first.as_bytes())
    {
        return None;
    }
    // The matched region is ASCII, so scan indices advance one per byte.
    let mut j = i + first.len();
    for word in words {
        let ws_start = j;
        while j < scan.len() && scan[j].ch.is_whitespace() {
            j += 1;
        }
        if j == ws_start || j >= scan.len() {
            return None;
        }
        let word_start = scan[j].idx;
        if word_start + word.len() > bytes.len()
            || !bytes[word_start..word_start + word.len()].eq_ignore_ascii_case(word.as_bytes())
        {
            return None;
        }
        j += word.len();
    }
    if j < scan.len() && !is_keyword_boundary_after(scan[j].ch) {
        return None;
    }
    let end = if j < scan.len() {
        scan[j].idx
    } else {
        input.len()
    };
    Some((start, end))
}

/// Byte range of the first top-level occurrence of `keyword` in `input`.
fn find_keyword_position(
    input: &str,
    scan: &[ScannedChar],
    keyword: &str,
) -> Option<(usize, usize)> {
    (0..scan.len()).find_map(|i| match_keyword_at(input, scan, i, keyword))
}

/// Split a SELECT body into clause segments using case-insensitive keyword
/// matching on the original string. Keywords inside strings, quoted
/// identifiers, regex literals, or parentheses (subqueries) are ignored.
/// A repeated top-level clause keyword is a parse error.
fn split_clauses(
    input: &str,
) -> Result<std::collections::HashMap<String, String>, HyperbytedbError> {
    const CLAUSE_KEYWORDS: [(&str, &str); 10] = [
        ("INTO", "into"),
        ("FROM", "from"),
        ("WHERE", "where"),
        ("GROUP BY", "group_by"),
        ("ORDER BY", "order_by"),
        ("SLIMIT", "slimit"),
        ("SOFFSET", "soffset"),
        ("LIMIT", "limit"),
        ("OFFSET", "offset"),
        ("TZ", "tz"),
    ];

    let scan = scan_chars(input);
    // (keyword, key, keyword start byte, value start byte)
    let mut found: Vec<(&str, &str, usize, usize)> = Vec::new();
    let mut i = 0;
    while i < scan.len() {
        let matched = CLAUSE_KEYWORDS.iter().find_map(|(kw, key)| {
            match_keyword_at(input, &scan, i, kw).map(|(start, end)| (*kw, *key, start, end))
        });
        match matched {
            Some((kw, key, start, end)) => {
                if found.iter().any(|(_, k, _, _)| *k == key) {
                    return Err(HyperbytedbError::QueryParse(format!(
                        "duplicate {kw} clause in SELECT"
                    )));
                }
                found.push((kw, key, start, end));
                while i < scan.len() && scan[i].idx < end {
                    i += 1;
                }
            }
            None => i += 1,
        }
    }

    let mut result = std::collections::HashMap::new();
    // Everything before the first keyword is the fields
    let first_kw_pos = found.first().map(|(_, _, s, _)| *s).unwrap_or(input.len());
    result.insert(
        "fields".to_string(),
        input[..first_kw_pos].trim().to_string(),
    );
    for (n, (_, key, _, value_start)) in found.iter().enumerate() {
        let end = found
            .get(n + 1)
            .map(|(_, _, s, _)| *s)
            .unwrap_or(input.len());
        result.insert(
            (*key).to_string(),
            input[*value_start..end].trim().to_string(),
        );
    }
    Ok(result)
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
    let scan = scan_chars(input);
    let mut parts = Vec::new();
    let mut last = 0;
    for sc in &scan {
        if sc.ch == ',' && !sc.masked && sc.depth == 0 {
            parts.push(&input[last..sc.idx]);
            last = sc.idx + 1;
        }
    }
    parts.push(&input[last..]);
    parts
}

fn parse_field_expr(input: &str) -> Result<Field, HyperbytedbError> {
    let input = input.trim();

    // Check for AS alias
    let scan = scan_chars(input);
    let (expr_str, alias) = if let Some((pos, end)) = find_keyword_position(input, &scan, "AS") {
        let expr_part = input[..pos].trim();
        let alias_part = input[end..].trim().trim_matches('"');
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

    // Bare DISTINCT keyword: `DISTINCT "v"` is the same as `distinct("v")`.
    // Require whitespace after the keyword so the function-call form
    // `distinct("v")` (and e.g. `distinct("v") / 10`) keeps its usual path.
    if starts_with_keyword(input, "DISTINCT")
        && input
            .as_bytes()
            .get("DISTINCT".len())
            .is_some_and(|b| b.is_ascii_whitespace())
    {
        let rest = input["DISTINCT".len()..].trim();
        if !rest.is_empty() && !rest.starts_with(|c: char| "=<>!~+-*/%".contains(c)) {
            let arg = parse_expr(rest)?;
            return Ok(Expr::Call(FunctionCall {
                name: "DISTINCT".to_string(),
                args: vec![arg],
            }));
        }
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
    let scan = scan_chars(input);
    // OR has the lowest precedence in InfluxQL, so split at OR first: the
    // operator split earliest ends up at the root of the tree and binds
    // loosest. Which OR occurrence is split at is semantically neutral.
    for (kw, op) in [("OR", BinaryOp::Or), ("AND", BinaryOp::And)] {
        if let Some((pos, end)) = find_keyword_position(input, &scan, kw) {
            let left = parse_expr(&input[..pos])?;
            let right = parse_expr(&input[end..])?;
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

    let scan = scan_chars(input);
    for (op_str, op) in &operators {
        if let Some(pos) = find_top_level_operator(input, &scan, op_str) {
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
    // Lower-precedence level first, so it ends up at the root of the tree.
    // Within a level, split at the LAST top-level operator for left
    // associativity: `a - b - c` == `(a - b) - c` and `bytes/1024/1024` ==
    // `(bytes/1024)/1024`.
    let levels: [&[(char, BinaryOp)]; 2] = [
        &[('+', BinaryOp::Add), ('-', BinaryOp::Sub)],
        &[
            ('*', BinaryOp::Mul),
            ('/', BinaryOp::Div),
            ('%', BinaryOp::Mod),
        ],
    ];

    let scan = scan_chars(input);
    for level in levels {
        for sc in scan.iter().rev() {
            if sc.masked || sc.depth != 0 {
                continue;
            }
            let Some((_, op)) = level.iter().find(|(c, _)| *c == sc.ch) else {
                continue;
            };
            let pos = sc.idx;
            if pos == 0 {
                continue;
            }

            // For `-` and `+`: skip when preceded by another arithmetic operator
            // — that makes it unary negation/plus, not binary subtraction/addition.
            // e.g. `mean("x") * -1` → the `-` is unary, not `mean("x") *` minus `1`.
            if sc.ch == '-' || sc.ch == '+' {
                let left_trimmed = input[..pos].trim_end();
                if left_trimmed.is_empty() || left_trimmed.ends_with(|c: char| "+-*/%(".contains(c))
                {
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

/// Find the first top-level (unmasked, paren depth 0) occurrence of a
/// symbolic operator, refusing matches that are part of a longer operator
/// (`=` inside `>=`/`!=`/`=~`, `<` inside `<=`/`<>`, `>` inside `>=`/`<>`).
fn find_top_level_operator(input: &str, scan: &[ScannedChar], op: &str) -> Option<usize> {
    let bytes = input.as_bytes();
    let op_bytes = op.as_bytes();
    for sc in scan {
        if sc.masked || sc.depth != 0 {
            continue;
        }
        let i = sc.idx;
        if i + op_bytes.len() > bytes.len() || &bytes[i..i + op_bytes.len()] != op_bytes {
            continue;
        }
        let prev = i.checked_sub(1).map(|p| bytes[p]);
        let next = bytes.get(i + op_bytes.len()).copied();
        let standalone = match op {
            "=" => {
                !matches!(prev, Some(b'!' | b'<' | b'>' | b'='))
                    && !matches!(next, Some(b'~' | b'='))
            }
            "<" => !matches!(next, Some(b'=' | b'>')),
            ">" => !matches!(prev, Some(b'<')) && !matches!(next, Some(b'=')),
            _ => true,
        };
        if standalone {
            return Some(i);
        }
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

    // String literal 'value' — both `\'` and `''` escape a quote
    if input.starts_with('\'') && input.ends_with('\'') {
        let s = input[1..input.len() - 1]
            .replace("\\'", "'")
            .replace("''", "'");
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

    let scan = scan_chars(input);

    // Identifier with ::field or ::tag suffix
    if let Some(k) = (0..scan.len().saturating_sub(1)).find(|&k| {
        !scan[k].masked && scan[k].ch == ':' && !scan[k + 1].masked && scan[k + 1].ch == ':'
    }) {
        let pos = scan[k].idx;
        let name = input[..pos].trim().trim_matches('"').to_string();
        let typ = match input[pos + 2..].trim().to_lowercase().as_str() {
            "field" => Some(FieldType::Field),
            "tag" => Some(FieldType::Tag),
            other => {
                return Err(HyperbytedbError::QueryParse(format!(
                    "unsupported cast ::{other}: only ::field and ::tag casts are supported"
                )));
            }
        };
        return Ok(Expr::FieldRef { name, typ });
    }

    // Bitwise operators are not supported: fail loudly instead of silently
    // treating the whole expression as an identifier.
    if let Some(sc) = scan
        .iter()
        .find(|sc| !sc.masked && matches!(sc.ch, '&' | '|' | '^'))
    {
        return Err(HyperbytedbError::QueryParse(format!(
            "unsupported operator '{}' in expression: {input}",
            sc.ch
        )));
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
        let inner = input[1..input.len() - 1].trim();
        if starts_with_keyword(inner, "SELECT") {
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

    // Fully qualified: "db"."rp"."measurement" or db.rp.measurement — split
    // on dots outside quotes so `FROM "app.requests"` stays one measurement.
    let scan = scan_chars(input);
    let mut parts: Vec<&str> = Vec::new();
    let mut last = 0;
    for sc in &scan {
        if sc.ch == '.' && !sc.masked && sc.depth == 0 {
            parts.push(&input[last..sc.idx]);
            last = sc.idx + 1;
        }
    }
    parts.push(&input[last..]);
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

/// Byte offset of the last unmasked, top-level, ASCII-case-insensitive
/// occurrence of `needle` that does not continue an identifier.
fn rfind_top_level_ci(input: &str, scan: &[ScannedChar], needle: &str) -> Option<usize> {
    let bytes = input.as_bytes();
    let needle_bytes = needle.as_bytes();
    for (k, sc) in scan.iter().enumerate().rev() {
        if sc.masked || sc.depth != 0 {
            continue;
        }
        let i = sc.idx;
        if i + needle_bytes.len() > bytes.len()
            || !bytes[i..i + needle_bytes.len()].eq_ignore_ascii_case(needle_bytes)
        {
            continue;
        }
        if k > 0 {
            let prev = scan[k - 1].ch;
            if prev.is_alphanumeric() || prev == '_' || prev == '"' {
                continue;
            }
        }
        return Some(i);
    }
    None
}

fn parse_group_by_clause(input: &str) -> Result<(GroupBy, Option<FillOption>), HyperbytedbError> {
    let mut fill = None;
    let mut dims_str = input.to_string();

    // Check for fill() at end
    let scan = scan_chars(input);
    if let Some(fill_pos) = rfind_top_level_ci(input, &scan, "FILL(") {
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

        if part.len() >= 5
            && part.as_bytes()[..5].eq_ignore_ascii_case(b"TIME(")
            && part.ends_with(')')
        {
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
    let scan = scan_chars(input);
    if let Some(pos) = rfind_top_level_ci(input, &scan, "FILL(")
        && let Some(close) = input[pos..].find(')')
    {
        let fill_inner = &input[pos + 5..pos + close];
        let rest = input[..pos].trim().to_string();
        if let Ok(f) = parse_fill_option(fill_inner) {
            return (rest, Some(f));
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
        assert!(
            err.to_string()
                .contains("only time predicates are supported")
        );
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
                assert!(!mv.backfill_on_create);
                assert!(mv.query.into.is_some());
                assert!(mv.query.group_by.is_some());
            }
            _ => panic!("expected CREATE MATERIALIZED VIEW"),
        }
    }

    #[test]
    fn test_parse_create_materialized_view_with_backfill() {
        let q = r#"CREATE MATERIALIZED VIEW "mv_5m" ON "mydb" WITH BACKFILL AS SELECT mean("value") INTO "cpu_5m" FROM "cpu" GROUP BY time(5m), *"#;
        let stmts = parse_query(q).unwrap();
        match &stmts[0] {
            Statement::CreateMaterializedView(mv) => {
                assert_eq!(mv.name, "mv_5m");
                assert_eq!(mv.database, "mydb");
                assert!(mv.backfill_on_create);
            }
            _ => panic!("expected CREATE MATERIALIZED VIEW"),
        }
    }

    #[test]
    fn test_parse_create_materialized_view_with_without_backfill_keyword_fails() {
        let q = r#"CREATE MATERIALIZED VIEW "mv" ON "mydb" WITH AS SELECT mean("value") INTO "cpu_5m" FROM "cpu" GROUP BY time(5m), *"#;
        assert!(
            parse_query(q).is_err(),
            "WITH without BACKFILL should be a parse error"
        );
    }

    #[test]
    fn test_parse_create_materialized_view_with_backfill_begin_syntax() {
        let q = r#"CREATE MATERIALIZED VIEW "mv_1h" ON "mydb" WITH BACKFILL BEGIN SELECT mean("value") INTO "cpu_1h" FROM "cpu" GROUP BY time(1h), * END"#;
        let stmts = parse_query(q).unwrap();
        match &stmts[0] {
            Statement::CreateMaterializedView(mv) => {
                assert_eq!(mv.name, "mv_1h");
                assert!(mv.backfill_on_create);
                assert!(mv.query.group_by.is_some());
            }
            _ => panic!("expected CREATE MATERIALIZED VIEW"),
        }
    }

    #[test]
    fn test_parse_create_materialized_view_begin_syntax_without_backfill() {
        let q = r#"CREATE MATERIALIZED VIEW "mv_1h" ON "mydb" BEGIN SELECT mean("value") INTO "cpu_1h" FROM "cpu" GROUP BY time(1h), * END"#;
        let stmts = parse_query(q).unwrap();
        match &stmts[0] {
            Statement::CreateMaterializedView(mv) => {
                assert!(!mv.backfill_on_create);
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

    // --- masking-scanner / precedence regression tests ---

    fn select_stmt(q: &str) -> SelectStatement {
        match parse_query(q).unwrap().remove(0) {
            Statement::Select(s) => s,
            other => panic!("expected SELECT, got {:?}", other),
        }
    }

    #[test]
    fn test_or_binds_looser_than_and() {
        let s = select_stmt("SELECT * FROM cpu WHERE a = 1 AND b = 2 OR c = 3");
        match s.condition.as_ref().unwrap() {
            Expr::BinaryExpr(root) => {
                assert_eq!(root.op, BinaryOp::Or);
                match &root.left {
                    Expr::BinaryExpr(l) => assert_eq!(l.op, BinaryOp::And),
                    other => panic!("expected AND on the left, got {:?}", other),
                }
            }
            other => panic!("expected BinaryExpr, got {:?}", other),
        }

        let s = select_stmt("SELECT * FROM cpu WHERE a = 1 OR b = 2 AND c = 3");
        match s.condition.as_ref().unwrap() {
            Expr::BinaryExpr(root) => {
                assert_eq!(root.op, BinaryOp::Or);
                match &root.right {
                    Expr::BinaryExpr(r) => assert_eq!(r.op, BinaryOp::And),
                    other => panic!("expected AND on the right, got {:?}", other),
                }
            }
            other => panic!("expected BinaryExpr, got {:?}", other),
        }
    }

    #[test]
    fn test_subtraction_and_division_left_associative() {
        let s = select_stmt(r#"SELECT "bytes" / 1024 / 1024 FROM m"#);
        match &s.fields[0].expr {
            Expr::BinaryExpr(root) => {
                assert_eq!(root.op, BinaryOp::Div);
                match &root.right {
                    Expr::IntegerLiteral(1024) => {}
                    other => panic!("expected IntegerLiteral(1024), got {:?}", other),
                }
                match &root.left {
                    Expr::BinaryExpr(l) => assert_eq!(l.op, BinaryOp::Div),
                    other => panic!("expected inner division, got {:?}", other),
                }
            }
            other => panic!("expected BinaryExpr(Div), got {:?}", other),
        }

        let s = select_stmt(r#"SELECT "a" - "b" - "c" FROM m"#);
        match &s.fields[0].expr {
            Expr::BinaryExpr(root) => {
                assert_eq!(root.op, BinaryOp::Sub);
                match &root.right {
                    Expr::Identifier(c) => assert_eq!(c, "c"),
                    other => panic!("expected identifier c, got {:?}", other),
                }
                match &root.left {
                    Expr::BinaryExpr(l) => assert_eq!(l.op, BinaryOp::Sub),
                    other => panic!("expected inner subtraction, got {:?}", other),
                }
            }
            other => panic!("expected BinaryExpr(Sub), got {:?}", other),
        }
    }

    #[test]
    fn test_subquery_in_from() {
        let s = select_stmt(
            r#"SELECT max("usage") FROM (SELECT mean("value") AS "usage" FROM "cpu" WHERE "t" = 'a') WHERE "host" = 'b' LIMIT 10"#,
        );
        let sub = match &s.from[0] {
            MeasurementSource::Subquery(sub) => sub,
            other => panic!("expected subquery, got {:?}", other),
        };
        assert_eq!(sub.from[0].name_str(), Some("cpu"));
        assert!(sub.condition.is_some());
        assert_eq!(sub.fields[0].alias.as_deref(), Some("usage"));
        assert!(s.condition.is_some());
        assert_eq!(s.limit, Some(10));
    }

    #[test]
    fn test_duplicate_clause_is_error() {
        let err = parse_query("SELECT * FROM cpu WHERE a = 1 WHERE b = 2").unwrap_err();
        assert!(err.to_string().contains("duplicate WHERE"));
    }

    #[test]
    fn test_regex_hides_keywords_from_clause_splitter() {
        let s = select_stmt(r#"SELECT * FROM cpu WHERE host =~ /a from b/ LIMIT 5"#);
        assert_eq!(s.limit, Some(5));
        match s.condition.as_ref().unwrap() {
            Expr::BinaryExpr(be) => {
                assert_eq!(be.op, BinaryOp::RegexMatch);
                match &be.right {
                    Expr::Regex(r) => assert_eq!(r, "a from b"),
                    other => panic!("expected regex, got {:?}", other),
                }
            }
            other => panic!("expected BinaryExpr, got {:?}", other),
        }
    }

    #[test]
    fn test_regex_measurement_with_braces() {
        let s = select_stmt(r#"SELECT * FROM /^cpu[0-9]{1,3}$/ WHERE "region" = 'eu'"#);
        match &s.from[0] {
            MeasurementSource::Concrete(m) => match &m.name {
                MeasurementName::Regex(r) => assert_eq!(r, "^cpu[0-9]{1,3}$"),
                other => panic!("expected regex measurement, got {:?}", other),
            },
            other => panic!("expected concrete measurement, got {:?}", other),
        }
        assert!(s.condition.is_some());
    }

    #[test]
    fn test_escaped_quotes_in_string_literals() {
        let s = select_stmt(r#"SELECT * FROM logs WHERE msg = 'don\'t group by me'"#);
        assert!(s.group_by.is_none());
        match s.condition.as_ref().unwrap() {
            Expr::BinaryExpr(be) => match &be.right {
                Expr::StringLiteral(v) => assert_eq!(v, "don't group by me"),
                other => panic!("expected string literal, got {:?}", other),
            },
            other => panic!("expected BinaryExpr, got {:?}", other),
        }

        let s = select_stmt("SELECT * FROM logs WHERE msg = 'don''t limit me' LIMIT 3");
        assert_eq!(s.limit, Some(3));
        match s.condition.as_ref().unwrap() {
            Expr::BinaryExpr(be) => match &be.right {
                Expr::StringLiteral(v) => assert_eq!(v, "don't limit me"),
                other => panic!("expected string literal, got {:?}", other),
            },
            other => panic!("expected BinaryExpr, got {:?}", other),
        }
    }

    #[test]
    fn test_cross_quote_contamination() {
        let s = select_stmt(r#"SELECT "it's" FROM cpu"#);
        match &s.fields[0].expr {
            Expr::Identifier(name) => assert_eq!(name, "it's"),
            other => panic!("expected identifier, got {:?}", other),
        }
        assert_eq!(s.from[0].name_str(), Some("cpu"));

        let s = select_stmt(r#"SELECT * FROM cpu WHERE unit = '"' GROUP BY time(1m)"#);
        assert!(s.group_by.as_ref().unwrap().time_dimension().is_some());
        match s.condition.as_ref().unwrap() {
            Expr::BinaryExpr(be) => match &be.right {
                Expr::StringLiteral(v) => assert_eq!(v, "\""),
                other => panic!("expected string literal, got {:?}", other),
            },
            other => panic!("expected BinaryExpr, got {:?}", other),
        }
    }

    #[test]
    fn test_non_ascii_does_not_shift_byte_offsets() {
        let s = select_stmt("SELECT * FROM cpu WHERE city = 'ığdır' LIMIT 5");
        assert_eq!(s.limit, Some(5));
        match s.condition.as_ref().unwrap() {
            Expr::BinaryExpr(be) => match &be.right {
                Expr::StringLiteral(v) => assert_eq!(v, "ığdır"),
                other => panic!("expected string literal, got {:?}", other),
            },
            other => panic!("expected BinaryExpr, got {:?}", other),
        }

        // Goes through parse_select directly: lexer::split_statements (out of
        // scope for the parser fix) still has a byte-boundary panic on this
        // input (`rest[..kw.len()]` at lexer.rs:138).
        let s = match parse_select(r#"SELECT "ﬁx" FROM cpu"#).unwrap() {
            Statement::Select(s) => s,
            other => panic!("expected SELECT, got {:?}", other),
        };
        match &s.fields[0].expr {
            Expr::Identifier(name) => assert_eq!(name, "ﬁx"),
            other => panic!("expected identifier, got {:?}", other),
        }
        assert_eq!(s.from[0].name_str(), Some("cpu"));
    }

    #[test]
    fn test_fill_after_non_ascii_string() {
        let s = select_stmt(r#"SELECT last("v") FROM m WHERE city = 'ığdır' fill(null)"#);
        assert!(matches!(s.fill, Some(FillOption::Null)));
        assert!(s.condition.is_some());
    }

    #[test]
    fn test_quoted_measurement_with_dot() {
        let s = select_stmt(r#"SELECT * FROM "app.requests""#);
        let m = s.from[0].as_concrete().unwrap();
        assert!(m.database.is_none());
        assert!(m.retention_policy.is_none());
        assert_eq!(m.name_str(), Some("app.requests"));
    }

    #[test]
    fn test_bare_distinct_keyword() {
        let s = select_stmt(r#"SELECT DISTINCT "v" FROM cpu"#);
        match &s.fields[0].expr {
            Expr::Call(f) => {
                assert_eq!(f.name, "DISTINCT");
                assert_eq!(f.args.len(), 1);
                match &f.args[0] {
                    Expr::Identifier(name) => assert_eq!(name, "v"),
                    other => panic!("expected identifier arg, got {:?}", other),
                }
            }
            other => panic!("expected DISTINCT call, got {:?}", other),
        }
    }

    #[test]
    fn test_modulo_operator() {
        let s = select_stmt(r#"SELECT "a" % 2 FROM m"#);
        match &s.fields[0].expr {
            Expr::BinaryExpr(be) => assert_eq!(be.op, BinaryOp::Mod),
            other => panic!("expected BinaryExpr(Mod), got {:?}", other),
        }
    }

    #[test]
    fn test_bitwise_operators_are_loud_errors() {
        for q in [
            "SELECT a & b FROM m",
            "SELECT a | b FROM m",
            "SELECT a ^ b FROM m",
        ] {
            let err = parse_query(q).unwrap_err();
            assert!(
                err.to_string().contains("unsupported operator"),
                "query {q} gave: {err}"
            );
        }
    }

    #[test]
    fn test_unsupported_cast_is_error() {
        let err = parse_query(r#"SELECT "v"::integer FROM cpu"#).unwrap_err();
        assert!(err.to_string().contains("::field and ::tag"));

        let s = select_stmt(r#"SELECT "v"::field FROM cpu"#);
        match &s.fields[0].expr {
            Expr::FieldRef { name, typ } => {
                assert_eq!(name, "v");
                assert!(matches!(typ, Some(FieldType::Field)));
            }
            other => panic!("expected FieldRef, got {:?}", other),
        }
    }

    #[test]
    fn test_tz_clause_strips_parens_and_quotes() {
        let s = select_stmt("SELECT * FROM cpu TZ('America/New_York')");
        assert_eq!(s.timezone.as_deref(), Some("America/New_York"));
    }

    #[test]
    fn test_keywords_adjacent_to_parens() {
        let s = select_stmt(r#"SELECT * FROM cpu WHERE ("a" = 1)AND("b" = 2)"#);
        match s.condition.as_ref().unwrap() {
            Expr::BinaryExpr(root) => {
                assert_eq!(root.op, BinaryOp::And);
                for side in [&root.left, &root.right] {
                    match side {
                        Expr::BinaryExpr(be) => assert_eq!(be.op, BinaryOp::Eq),
                        other => panic!("expected Eq, got {:?}", other),
                    }
                }
            }
            other => panic!("expected BinaryExpr, got {:?}", other),
        }
    }

    #[test]
    fn test_two_word_keywords_with_flexible_whitespace() {
        let s =
            select_stmt("select mean(\"v\") from cpu group\n\tby time(1m) order   by time desc");
        assert!(s.group_by.as_ref().unwrap().time_dimension().is_some());
        assert!(s.order_by.as_ref().unwrap().time_desc);
    }

    #[test]
    fn test_group_by_regex_dimension() {
        let s = select_stmt(r#"SELECT mean("v") FROM cpu GROUP BY time(1m), /host.*/"#);
        let gb = s.group_by.as_ref().unwrap();
        assert!(gb.time_dimension().is_some());
        assert!(
            gb.dimensions
                .iter()
                .any(|d| matches!(d, Dimension::Regex(r) if r == "host.*"))
        );
    }
}
