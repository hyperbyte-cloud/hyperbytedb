//! Quote- and escape-aware InfluxQL lexer for DDL/SHOW/auth statements.
//!
//! SELECT expression parsing remains in [`super::parser`]; this lexer drives
//! statement splitting and token consumption for DDL grammars.

use crate::error::HyperbytedbError;
use crate::timeseriesql::ast::{Duration, DurationUnit};

/// Lexer token with source span.
#[derive(Debug, Clone, PartialEq)]
pub struct Token {
    pub kind: TokenKind,
    pub start: usize,
    pub end: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub enum TokenKind {
    /// Reserved word, canonical uppercase (e.g. `SELECT`, `BEGIN`).
    Keyword(String),
    /// Identifier: bare or double-quoted (case preserved).
    Ident(String),
    /// Single-quoted string literal.
    StringLit(String),
    /// Integer literal.
    Number(i64),
    /// Duration literal (compound supported); `None` nanos = infinite (0/INF).
    Duration {
        nanos: Option<i64>,
    },
    /// Punctuation / operators.
    LParen,
    RParen,
    Comma,
    Dot,
    Eq,
    Ne,
    Lt,
    Lte,
    Gt,
    Gte,
    Bang,
    Tilde,
    /// Regex-match operator `=~`.
    MatchRegex,
    /// Regex-not-match operator `!~`.
    NotMatchRegex,
    Plus,
    Minus,
    Semi,
    Star,
    /// Division `/` (also used wherever a `/` is not a regex delimiter).
    Slash,
    /// Regex literal /pattern/
    Regex(String),
    Eof,
}

/// Tokenize `input` into a flat stream (no statement splitting).
pub fn tokenize(input: &str) -> Result<Vec<Token>, HyperbytedbError> {
    let mut lexer = Lexer::new(input);
    let mut tokens = Vec::new();
    loop {
        let tok = lexer.next_token()?;
        let is_eof = tok.kind == TokenKind::Eof;
        tokens.push(tok);
        if is_eof {
            break;
        }
    }
    Ok(tokens)
}

/// Split multi-statement input on `;` outside quotes and BEGIN…END blocks.
pub fn split_statements(input: &str) -> Result<Vec<String>, HyperbytedbError> {
    let mut statements = Vec::new();
    let mut start = 0usize;
    let mut i = 0usize;
    let bytes = input.as_bytes();
    let mut in_single = false;
    let mut in_double = false;
    let mut in_regex = false;
    let mut begin_depth = 0i32;
    // Last significant (non-whitespace) char outside string/regex literals,
    // used to decide whether a `/` is in operand position (regex start).
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
            // `/` in operand position (after `=~`, `!~`, `(`, `,` or `=`)
            // starts a regex literal; a `;` inside it must not split.
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
    Ok(statements)
}

fn matches_keyword_at(input: &str, start: usize, kw: &str) -> bool {
    // Byte-wise compare: slicing `rest[..kw.len()]` panics when a multibyte
    // char straddles the boundary (e.g. an identifier containing `ﬁ`).
    let rest = input.as_bytes().get(start..);
    let Some(rest) = rest else { return false };
    if rest.len() < kw.len() || !rest[..kw.len()].eq_ignore_ascii_case(kw.as_bytes()) {
        return false;
    }
    !matches!(rest.get(kw.len()), Some(b) if is_ident_continue(*b as char))
}

/// Sum compound duration text (e.g. `1h30m`, `0`, `INF`) to nanoseconds.
/// Returns `None` for infinite.
pub fn parse_duration_text(input: &str) -> Result<Option<i64>, HyperbytedbError> {
    let s = input.trim();
    if s.is_empty() {
        return Err(HyperbytedbError::QueryParse("empty duration".to_string()));
    }
    let upper = s.to_uppercase();
    if upper == "INF" || upper == "INFINITY" || s == "0" || s == "0s" {
        return Ok(None);
    }

    let units: &[(&str, i64)] = &[
        ("ns", 1),
        ("us", 1_000),
        ("µs", 1_000),
        ("u", 1_000),
        ("ms", 1_000_000),
        ("s", 1_000_000_000),
        ("m", 60 * 1_000_000_000),
        ("h", 3_600 * 1_000_000_000),
        ("d", 86_400 * 1_000_000_000),
        ("w", 7 * 86_400 * 1_000_000_000),
    ];

    let mut total: i64 = 0;
    let mut i = 0;
    let bytes = s.as_bytes();
    while i < bytes.len() {
        if bytes[i].is_ascii_whitespace() {
            i += 1;
            continue;
        }
        let start = i;
        if bytes[i] == b'-' || bytes[i] == b'+' {
            i += 1;
        }
        while i < bytes.len() && bytes[i].is_ascii_digit() {
            i += 1;
        }
        if i == start || (i == start + 1 && !bytes[start].is_ascii_digit()) {
            return Err(HyperbytedbError::QueryParse(format!(
                "invalid duration: {input}"
            )));
        }
        let num_str = &s[start..i];
        let value: i64 = num_str
            .parse()
            .map_err(|_| HyperbytedbError::QueryParse(format!("invalid duration: {input}")))?;

        let mut matched = false;
        for (suffix, mult) in units {
            let su = suffix.as_bytes();
            if i + su.len() <= bytes.len() && &bytes[i..i + su.len()] == su {
                total = total.saturating_add(value.saturating_mul(*mult));
                i += su.len();
                matched = true;
                break;
            }
        }
        if !matched {
            return Err(HyperbytedbError::QueryParse(format!(
                "invalid duration unit in: {input}"
            )));
        }
    }
    Ok(Some(total))
}

/// Convert nanoseconds to a single-term AST Duration (largest exact unit).
pub fn nanos_to_ast_duration(nanos: i64) -> Duration {
    const H: i64 = 3_600 * 1_000_000_000;
    const D: i64 = 86_400 * 1_000_000_000;
    const W: i64 = 7 * D;
    const M: i64 = 60 * 1_000_000_000;
    const S: i64 = 1_000_000_000;
    if nanos % W == 0 {
        return Duration {
            value: nanos / W,
            unit: DurationUnit::Week,
        };
    }
    if nanos % D == 0 {
        return Duration {
            value: nanos / D,
            unit: DurationUnit::Day,
        };
    }
    if nanos % H == 0 {
        return Duration {
            value: nanos / H,
            unit: DurationUnit::Hour,
        };
    }
    if nanos % M == 0 {
        return Duration {
            value: nanos / M,
            unit: DurationUnit::Minute,
        };
    }
    if nanos % S == 0 {
        return Duration {
            value: nanos / S,
            unit: DurationUnit::Second,
        };
    }
    Duration {
        value: nanos,
        unit: DurationUnit::Nanosecond,
    }
}

/// Token cursor for recursive-descent DDL parsing.
pub struct TokenCursor<'a> {
    pub input: &'a str,
    pub tokens: &'a [Token],
    pub pos: usize,
}

impl<'a> TokenCursor<'a> {
    pub fn new(input: &'a str, tokens: &'a [Token]) -> Self {
        Self {
            input,
            tokens,
            pos: 0,
        }
    }

    pub fn peek(&self) -> Option<&Token> {
        self.tokens
            .get(self.pos)
            .filter(|t| t.kind != TokenKind::Eof)
    }

    pub fn bump(&mut self) -> Option<Token> {
        let t = self.tokens.get(self.pos)?.clone();
        if t.kind != TokenKind::Eof {
            self.pos += 1;
        }
        Some(t)
    }

    pub fn expect_keyword(&mut self, kw: &str) -> Result<(), HyperbytedbError> {
        match self.bump() {
            Some(Token {
                kind: TokenKind::Keyword(k),
                ..
            }) if k == kw => Ok(()),
            Some(t) => Err(HyperbytedbError::QueryParse(format!(
                "expected {kw}, found {:?}",
                t.kind
            ))),
            None => Err(HyperbytedbError::QueryParse(format!(
                "expected {kw}, found EOF"
            ))),
        }
    }

    pub fn match_keyword(&mut self, kw: &str) -> bool {
        if matches!(
            self.peek(),
            Some(Token {
                kind: TokenKind::Keyword(k),
                ..
            }) if k == kw
        ) {
            self.bump();
            true
        } else {
            false
        }
    }

    pub fn take_ident(&mut self) -> Result<String, HyperbytedbError> {
        match self.bump() {
            Some(Token {
                kind: TokenKind::Ident(s),
                ..
            }) => Ok(s),
            // An unquoted identifier that happens to be a keyword (e.g. a
            // database named `offset`) must keep its source spelling — the
            // token kind carries the canonicalized UPPERCASE form, which would
            // otherwise leak into stored names.
            Some(
                tok @ Token {
                    kind: TokenKind::Keyword(_),
                    ..
                },
            ) => Ok(self.input[tok.start..tok.end].to_string()),
            Some(Token {
                kind: TokenKind::StringLit(_),
                ..
            }) => Err(HyperbytedbError::QueryParse(
                "expected identifier, found string literal".to_string(),
            )),
            Some(t) => Err(HyperbytedbError::QueryParse(format!(
                "expected identifier, found {:?}",
                t.kind
            ))),
            None => Err(HyperbytedbError::QueryParse(
                "expected identifier, found EOF".to_string(),
            )),
        }
    }

    pub fn slice(&self, tok: &Token) -> &str {
        &self.input[tok.start..tok.end]
    }

    pub fn remaining_from(&self, tok: &Token) -> &str {
        &self.input[tok.start..]
    }
}

struct Lexer<'a> {
    input: &'a str,
    chars: Vec<(usize, char)>,
    pos: usize,
    /// Kind of the previously emitted token, used to decide whether a `/`
    /// begins a regex literal (operand position after `=~`/`!~`) or division.
    last_kind: Option<TokenKind>,
}

impl<'a> Lexer<'a> {
    fn new(input: &'a str) -> Self {
        Self {
            input,
            chars: input.char_indices().collect(),
            pos: 0,
            last_kind: None,
        }
    }

    fn peek_char(&self) -> Option<(usize, char)> {
        self.chars.get(self.pos).copied()
    }

    fn bump_char(&mut self) -> Option<(usize, char)> {
        let c = self.chars.get(self.pos).copied();
        if c.is_some() {
            self.pos += 1;
        }
        c
    }

    fn skip_whitespace(&mut self) {
        while let Some((_, c)) = self.peek_char() {
            if c.is_whitespace() {
                self.bump_char();
            } else {
                break;
            }
        }
    }

    fn next_token(&mut self) -> Result<Token, HyperbytedbError> {
        let tok = self.scan_token()?;
        self.last_kind = Some(tok.kind.clone());
        Ok(tok)
    }

    fn scan_token(&mut self) -> Result<Token, HyperbytedbError> {
        self.skip_whitespace();
        let (start, c) = match self.peek_char() {
            Some(v) => v,
            None => {
                return Ok(Token {
                    kind: TokenKind::Eof,
                    start: self.input.len(),
                    end: self.input.len(),
                });
            }
        };

        match c {
            '(' => {
                self.bump_char();
                Ok(Token {
                    kind: TokenKind::LParen,
                    start,
                    end: start + 1,
                })
            }
            ')' => {
                self.bump_char();
                Ok(Token {
                    kind: TokenKind::RParen,
                    start,
                    end: start + 1,
                })
            }
            ',' => {
                self.bump_char();
                Ok(Token {
                    kind: TokenKind::Comma,
                    start,
                    end: start + 1,
                })
            }
            '.' => {
                self.bump_char();
                Ok(Token {
                    kind: TokenKind::Dot,
                    start,
                    end: start + 1,
                })
            }
            '=' => {
                self.bump_char();
                if matches!(self.peek_char(), Some((_, '~'))) {
                    self.bump_char();
                    Ok(Token {
                        kind: TokenKind::MatchRegex,
                        start,
                        end: start + 2,
                    })
                } else {
                    Ok(Token {
                        kind: TokenKind::Eq,
                        start,
                        end: start + 1,
                    })
                }
            }
            '<' => {
                self.bump_char();
                if matches!(self.peek_char(), Some((_, '='))) {
                    self.bump_char();
                    Ok(Token {
                        kind: TokenKind::Lte,
                        start,
                        end: start + 2,
                    })
                } else {
                    Ok(Token {
                        kind: TokenKind::Lt,
                        start,
                        end: start + 1,
                    })
                }
            }
            '>' => {
                self.bump_char();
                if matches!(self.peek_char(), Some((_, '='))) {
                    self.bump_char();
                    Ok(Token {
                        kind: TokenKind::Gte,
                        start,
                        end: start + 2,
                    })
                } else {
                    Ok(Token {
                        kind: TokenKind::Gt,
                        start,
                        end: start + 1,
                    })
                }
            }
            '!' => {
                self.bump_char();
                if matches!(self.peek_char(), Some((_, '='))) {
                    self.bump_char();
                    Ok(Token {
                        kind: TokenKind::Ne,
                        start,
                        end: start + 2,
                    })
                } else if matches!(self.peek_char(), Some((_, '~'))) {
                    self.bump_char();
                    Ok(Token {
                        kind: TokenKind::NotMatchRegex,
                        start,
                        end: start + 2,
                    })
                } else {
                    Ok(Token {
                        kind: TokenKind::Bang,
                        start,
                        end: start + 1,
                    })
                }
            }
            '~' => {
                self.bump_char();
                Ok(Token {
                    kind: TokenKind::Tilde,
                    start,
                    end: start + 1,
                })
            }
            ';' => {
                self.bump_char();
                Ok(Token {
                    kind: TokenKind::Semi,
                    start,
                    end: start + 1,
                })
            }
            '*' => {
                self.bump_char();
                Ok(Token {
                    kind: TokenKind::Star,
                    start,
                    end: start + 1,
                })
            }
            '\'' => self.read_string_lit(start),
            '"' => self.read_ident_quoted(start),
            '/' => {
                // A `/` only starts a regex literal in operand position, i.e.
                // immediately after a regex-match operator. Anywhere else it is
                // division; treating every `/` as a regex made a lone `/` (e.g.
                // arithmetic in a WHERE clause) abort tokenization of the whole
                // statement.
                if matches!(
                    self.last_kind,
                    Some(TokenKind::MatchRegex) | Some(TokenKind::NotMatchRegex)
                ) {
                    self.read_regex(start)
                } else {
                    self.bump_char();
                    Ok(Token {
                        kind: TokenKind::Slash,
                        start,
                        end: start + 1,
                    })
                }
            }
            '-' | '+' => {
                if self.looks_like_number() {
                    self.read_number_or_duration(start)
                } else {
                    self.bump_char();
                    Ok(Token {
                        kind: if c == '-' {
                            TokenKind::Minus
                        } else {
                            TokenKind::Plus
                        },
                        start,
                        end: start + 1,
                    })
                }
            }
            _ if c.is_ascii_digit() => self.read_number_or_duration(start),
            _ if is_ident_start(c) => self.read_ident_or_keyword(start),
            _ => Err(HyperbytedbError::QueryParse(format!(
                "unexpected character '{c}' at position {start}"
            ))),
        }
    }

    fn looks_like_number(&self) -> bool {
        self.chars
            .get(self.pos + 1)
            .map(|(_, c)| c.is_ascii_digit())
            .unwrap_or(false)
    }

    fn read_string_lit(&mut self, start: usize) -> Result<Token, HyperbytedbError> {
        self.bump_char(); // opening '
        let mut out = String::new();
        while let Some((_, c)) = self.peek_char() {
            self.bump_char();
            if c == '\'' {
                if matches!(self.peek_char(), Some((_, '\''))) {
                    self.bump_char();
                    out.push('\'');
                    continue;
                }
                let end = self
                    .chars
                    .get(self.pos - 1)
                    .map(|(i, _)| i + 1)
                    .unwrap_or(start);
                return Ok(Token {
                    kind: TokenKind::StringLit(out),
                    start,
                    end,
                });
            }
            out.push(c);
        }
        Err(HyperbytedbError::QueryParse(
            "unterminated string literal".to_string(),
        ))
    }

    fn read_ident_quoted(&mut self, start: usize) -> Result<Token, HyperbytedbError> {
        self.bump_char();
        let mut out = String::new();
        while let Some((_, c)) = self.peek_char() {
            self.bump_char();
            if c == '"' {
                if matches!(self.peek_char(), Some((_, '"'))) {
                    self.bump_char();
                    out.push('"');
                    continue;
                }
                let end = self
                    .chars
                    .get(self.pos - 1)
                    .map(|(i, _)| i + 1)
                    .unwrap_or(start);
                return Ok(Token {
                    kind: TokenKind::Ident(out),
                    start,
                    end,
                });
            }
            out.push(c);
        }
        Err(HyperbytedbError::QueryParse(
            "unterminated quoted identifier".to_string(),
        ))
    }

    fn read_regex(&mut self, start: usize) -> Result<Token, HyperbytedbError> {
        self.bump_char();
        let mut out = String::new();
        while let Some((_, c)) = self.peek_char() {
            self.bump_char();
            if c == '/' {
                let end = self
                    .chars
                    .get(self.pos - 1)
                    .map(|(i, _)| i + 1)
                    .unwrap_or(start);
                return Ok(Token {
                    kind: TokenKind::Regex(out),
                    start,
                    end,
                });
            }
            out.push(c);
        }
        Err(HyperbytedbError::QueryParse(
            "unterminated regex literal".to_string(),
        ))
    }

    fn read_number_or_duration(&mut self, start: usize) -> Result<Token, HyperbytedbError> {
        let rest = &self.input[start..];
        let mut best: Option<(Option<i64>, usize)> = None;
        for end in 1..=rest.len() {
            if let Ok(nanos) = parse_duration_text(&rest[..end]) {
                let slice = rest[..end].trim();
                // Only a unit-bearing literal (e.g. `0s`, `1h30m`, `5µs`) is a
                // duration here. A bare integer — including `0` — is a Number;
                // the "0 == infinite" rule is applied where a duration is
                // grammatically expected (see ddl_parser::parse_duration_token),
                // so `LIMIT 0`/`OFFSET 0` still get a real number token.
                let is_duration = slice.chars().any(|c| c.is_ascii_alphabetic() || c == 'µ');
                if is_duration {
                    best = Some((nanos, end));
                }
            }
        }
        if let Some((nanos, consumed)) = best {
            let byte_end = start + consumed;
            while self.pos < self.chars.len() && self.chars[self.pos].0 < byte_end {
                self.pos += 1;
            }
            return Ok(Token {
                kind: TokenKind::Duration { nanos },
                start,
                end: byte_end,
            });
        }

        let mut end_pos = self.pos;
        if matches!(self.peek_char(), Some((_, '-' | '+'))) {
            end_pos += 1;
            self.bump_char();
        }
        while matches!(self.peek_char(), Some((_, c)) if c.is_ascii_digit()) {
            end_pos += 1;
            self.bump_char();
        }
        let text = &self.input[start
            ..self
                .chars
                .get(end_pos)
                .map(|(i, _)| *i)
                .unwrap_or(self.input.len())];
        let value: i64 = text
            .parse()
            .map_err(|_| HyperbytedbError::QueryParse(format!("invalid number: {text}")))?;
        self.pos = end_pos;
        Ok(Token {
            kind: TokenKind::Number(value),
            start,
            end: self
                .chars
                .get(end_pos)
                .map(|(i, _)| *i)
                .unwrap_or(self.input.len()),
        })
    }

    fn read_ident_or_keyword(&mut self, start: usize) -> Result<Token, HyperbytedbError> {
        let mut end_pos = self.pos;
        while let Some((_, c)) = self.peek_char() {
            if is_ident_continue(c) {
                end_pos += 1;
                self.bump_char();
            } else {
                break;
            }
        }
        let end = self
            .chars
            .get(end_pos)
            .map(|(i, _)| *i)
            .unwrap_or(self.input.len());
        let text = &self.input[start..end];
        let upper = text.to_uppercase();
        if is_keyword(&upper) {
            Ok(Token {
                kind: TokenKind::Keyword(upper),
                start,
                end,
            })
        } else {
            Ok(Token {
                kind: TokenKind::Ident(text.to_string()),
                start,
                end,
            })
        }
    }
}

fn is_ident_start(c: char) -> bool {
    c.is_ascii_alphabetic() || c == '_'
}

fn is_ident_continue(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_'
}

fn is_keyword(word: &str) -> bool {
    matches!(
        word,
        "SELECT"
            | "SHOW"
            | "CREATE"
            | "DROP"
            | "DELETE"
            | "ALTER"
            | "SET"
            | "GRANT"
            | "REVOKE"
            | "DATABASE"
            | "DATABASES"
            | "RETENTION"
            | "POLICIES"
            | "POLICY"
            | "MEASUREMENT"
            | "MEASUREMENTS"
            | "SERIES"
            | "USER"
            | "USERS"
            | "CONTINUOUS"
            | "QUERIES"
            | "QUERY"
            | "MATERIALIZED"
            | "VIEW"
            | "VIEWS"
            | "FROM"
            | "WHERE"
            | "ON"
            | "WITH"
            | "PASSWORD"
            | "PRIVILEGES"
            | "ALL"
            | "DURATION"
            | "REPLICATION"
            | "SHARD"
            | "DEFAULT"
            | "BEGIN"
            | "END"
            | "RESAMPLE"
            | "BACKFILL"
            | "EVERY"
            | "FOR"
            | "KEY"
            | "KEYS"
            | "VALUES"
            | "FIELD"
            | "FIELDS"
            | "TAG"
            | "NAME"
            | "LIMIT"
            | "OFFSET"
            | "AS"
            | "TO"
            | "AND"
            | "OR"
            | "NOT"
            | "IN"
            | "INF"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn kinds(input: &str) -> Vec<TokenKind> {
        tokenize(input)
            .unwrap()
            .into_iter()
            .filter(|t| t.kind != TokenKind::Eof)
            .map(|t| t.kind)
            .collect()
    }

    #[test]
    fn quoted_ident_with_spaces() {
        let toks = tokenize(r#"CREATE DATABASE "my db""#).unwrap();
        assert!(matches!(
            toks[2].kind,
            TokenKind::Ident(ref s) if s == "my db"
        ));
    }

    #[test]
    fn compound_duration() {
        let toks = tokenize("DURATION 1h30m").unwrap();
        assert!(matches!(
            toks[1].kind,
            TokenKind::Duration { nanos: Some(n) } if n == (3600 + 1800) * 1_000_000_000
        ));
    }

    #[test]
    fn infinite_duration() {
        assert!(parse_duration_text("INF").unwrap().is_none());
        assert!(parse_duration_text("0").unwrap().is_none());
    }

    #[test]
    fn statement_split_inside_cq() {
        let input = r#"CREATE CONTINUOUS QUERY "cq" ON "db" BEGIN SELECT mean("x") FROM "m"; END"#;
        let stmts = split_statements(input).unwrap();
        assert_eq!(stmts.len(), 1);
    }

    #[test]
    fn statement_split_multiple() {
        let input = "CREATE DATABASE a; CREATE DATABASE b";
        let stmts = split_statements(input).unwrap();
        assert_eq!(stmts.len(), 2);
    }

    #[test]
    fn default_high_not_keyword_default() {
        let toks = tokenize(r#"CREATE RETENTION POLICY "default_high" ON "db""#).unwrap();
        assert!(
            !toks
                .iter()
                .any(|t| matches!(&t.kind, TokenKind::Keyword(k) if k == "DEFAULT"))
        );
    }

    #[test]
    fn bare_zero_is_a_number() {
        // `0` must be a Number (so LIMIT 0 works); only unit-bearing `0s` is a
        // duration.
        assert_eq!(kinds("0"), vec![TokenKind::Number(0)]);
        assert_eq!(kinds("100"), vec![TokenKind::Number(100)]);
        assert_eq!(kinds("0s"), vec![TokenKind::Duration { nanos: None }]);
    }

    #[test]
    fn name_is_a_keyword() {
        assert_eq!(kinds("NAME"), vec![TokenKind::Keyword("NAME".to_string())]);
    }

    #[test]
    fn regex_operators_and_literal() {
        assert_eq!(
            kinds("=~ /foo/"),
            vec![TokenKind::MatchRegex, TokenKind::Regex("foo".to_string())]
        );
        assert_eq!(
            kinds("!~ /bar/"),
            vec![
                TokenKind::NotMatchRegex,
                TokenKind::Regex("bar".to_string())
            ]
        );
    }

    #[test]
    fn lone_slash_is_division_not_regex() {
        // A `/` outside operand position is division and must not consume the
        // rest of the input as an (unterminated) regex.
        assert_eq!(
            kinds("10/2"),
            vec![
                TokenKind::Number(10),
                TokenKind::Slash,
                TokenKind::Number(2)
            ]
        );
    }

    #[test]
    fn regex_swallows_keyword_like_content() {
        // The inner LIMIT belongs to the regex; only the trailing one is a
        // keyword token.
        let limits = tokenize(r#""host" =~ /a LIMIT b/ LIMIT 5"#)
            .unwrap()
            .into_iter()
            .filter(|t| matches!(&t.kind, TokenKind::Keyword(k) if k == "LIMIT"))
            .count();
        assert_eq!(limits, 1);
    }

    #[test]
    fn split_statements_ignores_semicolon_in_regex() {
        let stmts = split_statements(r#"SELECT * FROM cpu WHERE host =~ /a;b/"#).unwrap();
        assert_eq!(stmts.len(), 1, "`;` inside a regex literal must not split");
    }

    #[test]
    fn split_statements_begin_needs_word_boundary() {
        let stmts = split_statements("SELECT * FROM tx_begin; SELECT * FROM cpu").unwrap();
        assert_eq!(
            stmts.len(),
            2,
            "`begin` suffix of an identifier must not open a block: {stmts:?}"
        );
        assert_eq!(stmts[0], "SELECT * FROM tx_begin");
    }

    #[test]
    fn split_statements_still_respects_begin_end_blocks() {
        let stmts = split_statements(
            "CREATE CONTINUOUS QUERY cq ON db BEGIN SELECT mean(v) INTO m2 FROM m1 GROUP BY time(30m) END; SHOW DATABASES",
        )
        .unwrap();
        assert_eq!(stmts.len(), 2);
        assert!(stmts[0].ends_with("END"));
    }
}
