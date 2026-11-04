//! How a call to a peer failed: the peer answered with an error status, or
//! it could not be reached. Callers act on the status (409 stale epoch,
//! 429/503 back-pressure, 404/405 an older peer), so it is kept.

use std::fmt;

use crate::error::HyperbytedbError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PeerRpcError {
    /// The peer answered with a non-success status.
    Status { code: u16, body: String },
    /// The request did not complete (connect, send or read failed, or it
    /// timed out).
    Unreachable(String),
}

impl PeerRpcError {
    /// The peer's status code, if it answered.
    #[must_use]
    pub fn status(&self) -> Option<u16> {
        match self {
            Self::Status { code, .. } => Some(*code),
            Self::Unreachable(_) => None,
        }
    }
}

impl fmt::Display for PeerRpcError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Status { code, body } => write!(f, "HTTP {code}: {body}"),
            Self::Unreachable(e) => f.write_str(e),
        }
    }
}

impl std::error::Error for PeerRpcError {}

impl From<PeerRpcError> for HyperbytedbError {
    fn from(e: PeerRpcError) -> Self {
        HyperbytedbError::PeerUnreachable(e.to_string())
    }
}
