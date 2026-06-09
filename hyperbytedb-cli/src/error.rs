use serde_json::Value;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum CliError {
    #[error("connection failed: {0}")]
    Connection(String),

    #[error("authentication failed: {0}")]
    Auth(String),

    #[error("authorization denied: {0}")]
    Forbidden(String),

    #[error("query error: {0}")]
    Query(String),

    #[error("write error: {0}")]
    Write(String),

    #[error("HTTP {status}: {body}")]
    Http { status: u16, body: String },

    #[error("configuration error: {0}")]
    Config(String),

    #[error("import error: {0}")]
    Import(String),

    #[error("export error: {0}")]
    Export(String),

    #[error("{0}")]
    Other(String),
}

impl CliError {
    pub fn from_status(status: reqwest::StatusCode, body: &str) -> Self {
        let message = parse_json_error(body);
        match status.as_u16() {
            401 => Self::Auth(message),
            403 => Self::Forbidden(message),
            _ => Self::Http {
                status: status.as_u16(),
                body: message,
            },
        }
    }
}

/// Extract a human-readable message from HyperbyteDB / Influx-style JSON error bodies.
///
/// Handles:
/// - `{"error":"message"}`
/// - `{"results":[{"statement_id":1,"error":"message"}]}`
pub fn parse_json_error(body: &str) -> String {
    let trimmed = body.trim();
    if trimmed.is_empty() {
        return "(empty response)".to_string();
    }

    let Ok(value) = serde_json::from_str::<Value>(trimmed) else {
        return trimmed.to_string();
    };

    if let Some(err) = value.get("error").and_then(Value::as_str) {
        return err.to_string();
    }

    if let Some(results) = value.get("results").and_then(Value::as_array) {
        let errors: Vec<String> = results
            .iter()
            .filter_map(|result| {
                let err = result.get("error")?.as_str()?;
                let id = result
                    .get("statement_id")
                    .and_then(Value::as_u64)
                    .unwrap_or(0);
                if results.len() > 1 {
                    Some(format!("statement {id}: {err}"))
                } else {
                    Some(err.to_string())
                }
            })
            .collect();
        if !errors.is_empty() {
            return errors.join("; ");
        }
    }

    if value.is_object() {
        serde_json::to_string_pretty(&value).unwrap_or_else(|_| trimmed.to_string())
    } else {
        trimmed.to_string()
    }
}

pub type Result<T> = std::result::Result<T, CliError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_top_level_error_field() {
        let msg = parse_json_error(r#"{"error":"database not found: mydb"}"#);
        assert_eq!(msg, "database not found: mydb");
    }

    #[test]
    fn parse_results_array_error() {
        let msg = parse_json_error(
            r#"{"results":[{"statement_id":0,"series":[],"error":"syntax error at line 1"}]}"#,
        );
        assert_eq!(msg, "syntax error at line 1");
    }

    #[test]
    fn parse_multiple_statement_errors() {
        let msg = parse_json_error(
            r#"{"results":[
                {"statement_id":0,"error":"first failed"},
                {"statement_id":1,"error":"second failed"}
            ]}"#,
        );
        assert_eq!(msg, "statement 0: first failed; statement 1: second failed");
    }

    #[test]
    fn parse_non_json_passthrough() {
        let msg = parse_json_error("plain text error");
        assert_eq!(msg, "plain text error");
    }

    #[test]
    fn from_status_parses_json_body() {
        let err = CliError::from_status(
            reqwest::StatusCode::BAD_REQUEST,
            r#"{"error":"query parse: missing GROUP BY"}"#,
        );
        assert!(matches!(err, CliError::Http { status: 400, .. }));
        assert_eq!(err.to_string(), "HTTP 400: query parse: missing GROUP BY");
    }
}
