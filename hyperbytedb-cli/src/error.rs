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
        match status.as_u16() {
            401 => Self::Auth(body.to_string()),
            403 => Self::Forbidden(body.to_string()),
            _ => Self::Http {
                status: status.as_u16(),
                body: body.to_string(),
            },
        }
    }
}

pub type Result<T> = std::result::Result<T, CliError>;
