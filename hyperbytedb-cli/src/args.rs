//! InfluxDB v1-style argv normalization.
//!
//! The legacy `influx` client accepts single-dash long flags such as `-host` and
//! `-database`. clap treats `-host` as `-h` (help) plus a stray `ost` argument,
//! so we rewrite multi-character single-dash flags to `--long` form before parsing.

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

#[cfg(test)]
mod tests {
    use super::normalize_influx_style_args;

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
