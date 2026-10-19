use thiserror::Error;

/// What can go wrong. A refused write is [`Error::Write`] and is not retried;
/// batches before it have been written.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum Error {
    /// The server could not be reached (after the last retry).
    #[error("{method} {path}: {source}")]
    Connection {
        method: &'static str,
        path: &'static str,
        #[source]
        source: reqwest::Error,
    },
    /// An HTTP error status after the last retry (5xx, 429, ...).
    #[error("HTTP {status}: {message}")]
    Server { status: u16, message: String },
    /// 401 or 403: missing or wrong credentials, or no privilege.
    #[error("HTTP {status}: {message}")]
    Auth { status: u16, message: String },
    /// A write the server refused (400: bad line protocol, a field type
    /// conflict, a cardinality limit, ...).
    #[error("HTTP {status}: {message}")]
    Write { status: u16, message: String },
    /// A statement's error, as the server returned it.
    #[error("query: {0}")]
    Query(String),
    /// A point line protocol cannot express (a newline in a name, ...).
    #[error("invalid point: {0}")]
    InvalidPoint(String),
    /// A response that is not what the API returns.
    #[error("decoding the response: {0}")]
    Decode(String),
    /// A write that failed after `written` of its lines were stored (earlier
    /// batches, or half of a batch split after a 413). Sending the whole
    /// write again would store those twice.
    #[error("{source} (after {written} lines were written)")]
    WriteInterrupted {
        written: usize,
        #[source]
        source: Box<Error>,
    },
}

impl Error {
    /// Whether trying again later may succeed: the server could not be
    /// reached, or answered 429 or 5xx after every retry.
    pub fn is_retryable(&self) -> bool {
        match self {
            Error::Connection { .. } => true,
            Error::Server { status, .. } => crate::client::retryable(*status),
            Error::WriteInterrupted { source, .. } => source.is_retryable(),
            _ => false,
        }
    }

    /// The HTTP status, when the server answered with an error.
    pub fn status(&self) -> Option<u16> {
        match self {
            Error::Server { status, .. }
            | Error::Auth { status, .. }
            | Error::Write { status, .. } => Some(*status),
            Error::WriteInterrupted { source, .. } => source.status(),
            _ => None,
        }
    }

    /// Lines stored before a write failed: non-zero only for
    /// [`Error::WriteInterrupted`].
    pub fn written(&self) -> usize {
        match self {
            Error::WriteInterrupted { written, .. } => *written,
            _ => 0,
        }
    }
}

pub type Result<T, E = Error> = std::result::Result<T, E>;
