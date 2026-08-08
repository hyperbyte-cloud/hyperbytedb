/// Rename the internal `__time` bucket alias to `time`, for INSERT ... SELECT
/// destinations and subquery FROM sources. Only standalone `__time` tokens are
/// rewritten (bare, `"__time"`, or `` `__time` ``); identifiers that merely
/// contain the substring (e.g. `"cpu__time"`) are preserved.
#[must_use]
pub fn rename_time_bucket_alias(sql: &str) -> String {
    let bytes = sql.as_bytes();
    let is_ident = |c: u8| c.is_ascii_alphanumeric() || c == b'_';
    let mut out = String::with_capacity(sql.len());
    let mut last = 0usize;
    for (pos, _) in sql.match_indices("__time") {
        if pos < last {
            continue;
        }
        let prev = if pos == 0 {
            None
        } else {
            Some(bytes[pos - 1])
        };
        let next = bytes.get(pos + "__time".len()).copied();
        let exact_quoted = matches!(
            (prev, next),
            (Some(b'"'), Some(b'"')) | (Some(b'`'), Some(b'`'))
        );
        let bare = prev.is_none_or(|c| !is_ident(c) && c != b'"' && c != b'`')
            && next.is_none_or(|c| !is_ident(c) && c != b'"' && c != b'`');
        if exact_quoted || bare {
            out.push_str(&sql[last..pos]);
            out.push_str("time");
            last = pos + "__time".len();
        }
    }
    out.push_str(&sql[last..]);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rename_time_bucket_alias_is_token_precise() {
        assert_eq!(
            rename_time_bucket_alias("toStartOfInterval(time, INTERVAL 1 MINUTE) AS __time"),
            "toStartOfInterval(time, INTERVAL 1 MINUTE) AS time"
        );
        assert_eq!(
            rename_time_bucket_alias("ORDER BY __time DESC"),
            "ORDER BY time DESC"
        );
        assert_eq!(rename_time_bucket_alias("\"__time\""), "\"time\"");
        assert_eq!(rename_time_bucket_alias("`__time`"), "`time`");
        assert_eq!(
            rename_time_bucket_alias("\"cpu__time\" AS __time"),
            "\"cpu__time\" AS time"
        );
        assert_eq!(rename_time_bucket_alias("lag__timer"), "lag__timer");
    }
}
