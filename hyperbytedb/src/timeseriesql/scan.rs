//! Quote-, regex-, and paren-aware masking scanner shared by SELECT parsing,
//! statement splitting, and DDL tokenization.

use crate::error::HyperbytedbError;

/// Per-character scan info produced by [`scan_chars`].
#[derive(Debug, Clone, Copy)]
pub struct ScannedChar {
    /// Byte offset of the character in the original input (valid for slicing).
    pub idx: usize,
    pub ch: char,
    /// Paren depth: 0 for top-level characters (the outermost parens
    /// themselves included), > 0 strictly inside parentheses.
    pub depth: i32,
    /// True when the character is part of a single-quoted string, a
    /// double-quoted identifier, or a regex literal (delimiters included).
    pub masked: bool,
}

/// Masking scanner shared by all InfluxQL string primitives.
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
pub fn scan_chars(input: &str) -> Result<Vec<ScannedChar>, HyperbytedbError> {
    let chars: Vec<(usize, char)> = input.char_indices().collect();
    let mut out = Vec::with_capacity(chars.len());
    let mut depth: u32 = 0;
    let mut i = 0usize;
    while i < chars.len() {
        let (idx, ch) = chars[i];
        match ch {
            '\'' | '"' => {
                let quote = ch;
                out.push(ScannedChar {
                    idx,
                    ch,
                    depth: depth as i32,
                    masked: true,
                });
                i += 1;
                while i < chars.len() {
                    let (jdx, c) = chars[i];
                    out.push(ScannedChar {
                        idx: jdx,
                        ch: c,
                        depth: depth as i32,
                        masked: true,
                    });
                    i += 1;
                    if quote == '\'' && c == '\\' && i < chars.len() {
                        let (kdx, k) = chars[i];
                        out.push(ScannedChar {
                            idx: kdx,
                            ch: k,
                            depth: depth as i32,
                            masked: true,
                        });
                        i += 1;
                    } else if c == quote {
                        if i < chars.len() && chars[i].1 == quote {
                            let (kdx, k) = chars[i];
                            out.push(ScannedChar {
                                idx: kdx,
                                ch: k,
                                depth: depth as i32,
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
                    depth: depth as i32,
                    masked: true,
                });
                i += 1;
                while i < chars.len() {
                    let (jdx, c) = chars[i];
                    out.push(ScannedChar {
                        idx: jdx,
                        ch: c,
                        depth: depth as i32,
                        masked: true,
                    });
                    i += 1;
                    if c == '\\' && i < chars.len() {
                        let (kdx, k) = chars[i];
                        out.push(ScannedChar {
                            idx: kdx,
                            ch: k,
                            depth: depth as i32,
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
                    depth: depth as i32,
                    masked: false,
                });
                depth += 1;
                i += 1;
            }
            ')' => {
                if depth == 0 {
                    return Err(HyperbytedbError::QueryParse(format!(
                        "unbalanced ')' in expression: {input}"
                    )));
                }
                depth -= 1;
                out.push(ScannedChar {
                    idx,
                    ch,
                    depth: depth as i32,
                    masked: false,
                });
                i += 1;
            }
            _ => {
                out.push(ScannedChar {
                    idx,
                    ch,
                    depth: depth as i32,
                    masked: false,
                });
                i += 1;
            }
        }
    }
    if depth != 0 {
        return Err(HyperbytedbError::QueryParse(format!(
            "unclosed '(' in expression: {input}"
        )));
    }
    Ok(out)
}

/// Whether a `/` at `chars[pos]` begins a regex literal rather than division.
pub fn slash_is_regex_start(chars: &[(usize, char)], pos: usize) -> bool {
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

/// Whether a `/` at `byte_pos` in `input` begins a regex literal.
pub fn is_regex_start_at(input: &str, byte_pos: usize) -> bool {
    let chars: Vec<(usize, char)> = input.char_indices().collect();
    let Some(pos) = chars.iter().position(|(idx, _)| *idx == byte_pos) else {
        return false;
    };
    slash_is_regex_start(&chars, pos)
}

pub(crate) fn is_keyword_boundary_before(c: char) -> bool {
    c.is_whitespace() || matches!(c, ')' | '\'' | '"')
}

pub(crate) fn is_keyword_boundary_after(c: char) -> bool {
    c.is_whitespace() || matches!(c, '(' | '\'' | '"' | '/')
}

/// Match an ASCII `keyword` at scan index `i`, case-insensitively on the original string.
pub fn match_keyword_at(
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
pub fn find_keyword_position(
    input: &str,
    scan: &[ScannedChar],
    keyword: &str,
) -> Option<(usize, usize)> {
    (0..scan.len()).find_map(|i| match_keyword_at(input, scan, i, keyword))
}

/// Find the first top-level symbolic operator occurrence.
pub fn find_top_level_operator(input: &str, scan: &[ScannedChar], op: &str) -> Option<usize> {
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

/// Byte offset of the last unmasked, top-level case-insensitive `needle`.
pub fn rfind_top_level_ci(input: &str, scan: &[ScannedChar], needle: &str) -> Option<usize> {
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

/// Split on top-level commas outside quotes, regex literals, and parentheses.
pub fn split_top_level_commas(input: &str) -> Result<Vec<&str>, HyperbytedbError> {
    let scan = scan_chars(input)?;
    let mut parts = Vec::new();
    let mut last = 0;
    for sc in &scan {
        if sc.ch == ',' && !sc.masked && sc.depth == 0 {
            parts.push(&input[last..sc.idx]);
            last = sc.idx + 1;
        }
    }
    parts.push(&input[last..]);
    Ok(parts)
}

/// Scan entry covering `byte_pos`, if any.
pub fn scanned_at(scan: &[ScannedChar], byte_pos: usize) -> Option<&ScannedChar> {
    scan.iter().rev().find(|sc| sc.idx <= byte_pos)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn division_is_not_regex() {
        let input = "10/2";
        let scan = scan_chars(input).unwrap();
        assert!(scan.iter().any(|sc| sc.ch == '/' && !sc.masked));
    }

    #[test]
    fn regex_after_match_operator_is_masked() {
        let input = "=~ /foo/";
        let scan = scan_chars(input).unwrap();
        assert!(scan.iter().all(|sc| sc.ch == '/'
            || sc.ch == 'f'
            || sc.ch == 'o'
            || sc.ch == '~'
            || sc.ch == '='
            || sc.masked
            || sc.ch.is_whitespace()));
        let slashes: Vec<_> = scan.iter().filter(|sc| sc.ch == '/').collect();
        assert_eq!(slashes.len(), 2);
        assert!(slashes.iter().all(|sc| sc.masked));
    }

    #[test]
    fn regex_after_from_keyword_is_masked() {
        let input = "FROM /^cpu/";
        let scan = scan_chars(input).unwrap();
        let slash = scan.iter().find(|sc| sc.ch == '/').unwrap();
        assert!(slash.masked);
    }

    #[test]
    fn is_regex_start_at_matches_scan_chars() {
        let input = "SELECT * FROM /^cpu/";
        let slash_pos = input.find('/').unwrap();
        assert!(is_regex_start_at(input, slash_pos));
        assert!(!is_regex_start_at("10/2", 2));
        assert!(is_regex_start_at("=~ /foo/", "=~ /foo/".find('/').unwrap()));
    }

    #[test]
    fn semicolon_inside_regex_is_masked() {
        let input = r#"host =~ /a;b/"#;
        let scan = scan_chars(input).unwrap();
        let semi = scan.iter().find(|sc| sc.ch == ';').unwrap();
        assert!(semi.masked);
    }
}
