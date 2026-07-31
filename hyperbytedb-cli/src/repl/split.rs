/// Split a TimeseriesQL batch on semicolons, respecting string and regex literals.
///
/// Mirrors server-side `timeseriesql::lexer::split_statements` closely enough
/// for REPL / `-execute` multi-statement batches.
pub fn split_statements(input: &str) -> Vec<String> {
    let mut statements = Vec::new();
    let mut start = 0usize;
    let mut i = 0usize;
    let bytes = input.as_bytes();
    let mut in_single = false;
    let mut in_double = false;
    let mut in_regex = false;
    let mut begin_depth = 0i32;
    let mut prev_sig: Option<char> = None;

    while i < bytes.len() {
        let c = bytes[i] as char;

        if in_regex {
            if c == '\\' {
                i += 2;
                continue;
            }
            if c == '/' {
                in_regex = false;
                prev_sig = Some('/');
            }
            i += 1;
            continue;
        }
        if in_single {
            if c == '\'' {
                if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                    i += 2;
                    continue;
                }
                in_single = false;
                prev_sig = Some('\'');
            }
            i += 1;
            continue;
        }
        if in_double {
            if c == '"' {
                if i + 1 < bytes.len() && bytes[i + 1] == b'"' {
                    i += 2;
                    continue;
                }
                in_double = false;
                prev_sig = Some('"');
            }
            i += 1;
            continue;
        }

        let at_word_boundary = i == 0 || !is_ident_continue(bytes[i - 1] as char);

        match c {
            '\'' => in_single = true,
            '"' => in_double = true,
            '/' if matches!(prev_sig, Some('~') | Some('(') | Some(',') | Some('=')) => {
                in_regex = true;
            }
            ';' if begin_depth == 0 => {
                let slice = input[start..i].trim();
                if !slice.is_empty() {
                    statements.push(slice.to_string());
                }
                start = i + 1;
            }
            _ if is_ident_start(c) && at_word_boundary && matches_keyword_at(input, i, "BEGIN") => {
                begin_depth += 1
            }
            _ if is_ident_start(c)
                && at_word_boundary
                && begin_depth > 0
                && matches_keyword_at(input, i, "END") =>
            {
                begin_depth -= 1;
            }
            _ => {}
        }
        if !c.is_whitespace() {
            prev_sig = Some(c);
        }
        i += 1;
    }

    let tail = input[start..].trim();
    if !tail.is_empty() {
        statements.push(tail.to_string());
    }
    statements
}

fn is_ident_start(c: char) -> bool {
    c.is_ascii_alphabetic() || c == '_'
}

fn is_ident_continue(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_'
}

fn matches_keyword_at(input: &str, start: usize, kw: &str) -> bool {
    let rest = input.as_bytes().get(start..);
    let Some(rest) = rest else {
        return false;
    };
    if rest.len() < kw.len() || !rest[..kw.len()].eq_ignore_ascii_case(kw.as_bytes()) {
        return false;
    }
    !matches!(rest.get(kw.len()), Some(b) if is_ident_continue(*b as char))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn splits_simple_statements() {
        let stmts = split_statements("SHOW DATABASES; SHOW MEASUREMENTS");
        assert_eq!(stmts, vec!["SHOW DATABASES", "SHOW MEASUREMENTS"]);
    }

    #[test]
    fn ignores_semicolon_in_single_quoted_string() {
        let stmts = split_statements("SELECT * FROM cpu WHERE msg = 'a;b'; SHOW DATABASES");
        assert_eq!(stmts.len(), 2);
        assert!(stmts[0].contains("'a;b'"));
        assert_eq!(stmts[1], "SHOW DATABASES");
    }

    #[test]
    fn ignores_semicolon_in_double_quoted_identifier() {
        let stmts = split_statements(r#"SELECT * FROM "meas;ure"; SHOW DATABASES"#);
        assert_eq!(stmts.len(), 2);
        assert!(stmts[0].contains(r#""meas;ure""#));
    }

    #[test]
    fn ignores_semicolon_in_regex() {
        let stmts = split_statements(r#"SELECT * FROM cpu WHERE host =~ /a;b/; SHOW DATABASES"#);
        assert_eq!(stmts.len(), 2);
        assert!(stmts[0].contains("/a;b/"));
        assert_eq!(stmts[1], "SHOW DATABASES");
    }

    #[test]
    fn begin_end_block_keeps_internal_semicolons() {
        let input = "BEGIN; SELECT 1; SELECT 2; END; SHOW DATABASES";
        let stmts = split_statements(input);
        assert_eq!(stmts.len(), 2);
        assert!(stmts[0].starts_with("BEGIN"));
        assert_eq!(stmts[1], "SHOW DATABASES");
    }
}
