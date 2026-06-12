//! InfluxDB v1-style argv normalization and query decoding.
//!
//! The legacy `influx` client accepts single-dash long flags such as `-host` and
//! `-database`. clap treats `-host` as `-h` (help) plus a stray `ost` argument,
//! so we rewrite multi-character single-dash flags to `--long` form before parsing.

use crate::error::{CliError, Result};
use percent_encoding::percent_decode_str;

/// Rewrite Influx-style `-flag` tokens to clap-compatible `--flag` form.
pub fn normalize_influx_style_args<I, S>(args: I) -> Vec<String>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    args.into_iter()
        .map(|arg| {
            let arg = arg.as_ref();
            if !arg.starts_with('-') || arg.starts_with("--") {
                return arg.to_string();
            }

            let rest = &arg[1..];
            // Keep true short flags (`-e`, `-d`, `-h`, …).
            if rest.len() <= 1 {
                return arg.to_string();
            }

            if rest
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
            {
                format!("--{rest}")
            } else {
                arg.to_string()
            }
        })
        .collect()
}

/// Parse curl-style `--data-urlencode` values (`q=SELECT ...` or percent-encoded).
pub fn decode_data_urlencode_query(value: &str) -> Result<String> {
    let q = value.strip_prefix("q=").unwrap_or(value);
    if q.contains('%') {
        percent_decode_str(q)
            .decode_utf8()
            .map_err(|e| CliError::Other(format!("invalid URL encoding in query: {e}")))
            .map(|s| s.into_owned())
    } else {
        Ok(q.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::{decode_data_urlencode_query, normalize_influx_style_args};

    fn norm(args: &[&str]) -> Vec<String> {
        normalize_influx_style_args(args.iter().copied())
    }

    #[test]
    fn rewrites_single_dash_long_flags() {
        assert_eq!(
            norm(&["-host", "localhost", "-execute", "SHOW DATABASES"]),
            vec![
                "--host".to_string(),
                "localhost".to_string(),
                "--execute".to_string(),
                "SHOW DATABASES".to_string(),
            ]
        );
    }

    #[test]
    fn preserves_short_flags() {
        assert_eq!(
            norm(&["-e", "SHOW DATABASES", "-d", "mydb"]),
            vec![
                "-e".to_string(),
                "SHOW DATABASES".to_string(),
                "-d".to_string(),
                "mydb".to_string(),
            ]
        );
    }

    #[test]
    fn rewrites_subcommand_flags() {
        assert_eq!(
            norm(&["write", "-database", "mydb", "-data-binary", "cpu value=1"]),
            vec![
                "write".to_string(),
                "--database".to_string(),
                "mydb".to_string(),
                "--data-binary".to_string(),
                "cpu value=1".to_string(),
            ]
        );
    }

    #[test]
    fn decodes_percent_encoded_query() {
        assert_eq!(
            decode_data_urlencode_query("q=SELECT%20*%20FROM%20cpu").unwrap(),
            "SELECT * FROM cpu"
        );
    }

    #[test]
    fn leaves_plain_query_unchanged() {
        assert_eq!(
            decode_data_urlencode_query("q=SELECT * FROM cpu").unwrap(),
            "SELECT * FROM cpu"
        );
    }

    #[test]
    fn leaves_double_dash_flags_unchanged() {
        assert_eq!(
            norm(&["--host", "localhost", "--data-urlencode", "q=SELECT 1"]),
            vec![
                "--host".to_string(),
                "localhost".to_string(),
                "--data-urlencode".to_string(),
                "q=SELECT 1".to_string(),
            ]
        );
    }
}
