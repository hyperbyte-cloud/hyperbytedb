use std::fmt;

use thiserror::Error;

/// Error payload that preserves an optional `source()` chain for ops debugging.
#[derive(Debug)]
pub struct ChainedError {
    message: String,
    source: Option<Box<dyn std::error::Error + Send + Sync + 'static>>,
}

impl ChainedError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            source: None,
        }
    }

    pub fn from_error<E: std::error::Error + Send + Sync + 'static>(source: E) -> Self {
        Self {
            message: source.to_string(),
            source: Some(Box::new(source)),
        }
    }

    pub fn with_context<E: std::error::Error + Send + Sync + 'static>(
        context: impl Into<String>,
        source: E,
    ) -> Self {
        Self {
            message: format!("{}: {source}", context.into()),
            source: Some(Box::new(source)),
        }
    }
}

impl fmt::Display for ChainedError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for ChainedError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source.as_ref().map(|e| e.as_ref() as _)
    }
}

impl From<String> for ChainedError {
    fn from(message: String) -> Self {
        Self::new(message)
    }
}

impl From<&str> for ChainedError {
    fn from(message: &str) -> Self {
        Self::new(message)
    }
}

#[derive(Error, Debug)]
pub enum HyperbytedbError {
    #[error("database not found: \"{0}\"")]
    DatabaseNotFound(String),

    #[error("retention policy not found: {0}")]
    RetentionPolicyNotFound(String),

    #[error(
        "field type conflict: input field \"{field}\" on measurement \"{measurement}\" is type {got}, already exists as type {expected}"
    )]
    FieldTypeConflict {
        field: String,
        measurement: String,
        got: String,
        expected: String,
    },

    #[error("unable to parse '{line}': {reason}")]
    LineProtocolParse { line: String, reason: String },

    #[error("unable to parse msgpack write body: {reason}")]
    MsgpackParse { reason: String },

    #[error("unable to parse columnar msgpack write body: {reason}")]
    ColumnarMsgpackParse { reason: String },

    #[error("wall clock not available for implicit timestamp on line protocol point")]
    WallClockTimestampUnavailable,

    #[error("error parsing query: {0}")]
    QueryParse(String),

    #[error("authorization failed")]
    AuthFailed,

    #[error("forbidden: {0}")]
    Forbidden(String),

    #[error("database is required")]
    DatabaseRequired,

    #[error("missing required parameter: {0}")]
    MissingParameter(String),

    #[error("WAL error: {0}")]
    Wal(#[source] ChainedError),

    #[error("storage error: {0}")]
    Storage(#[source] ChainedError),

    #[error("chdb error: {0}")]
    Chdb(#[source] ChainedError),

    #[error("metadata error: {0}")]
    Metadata(#[source] ChainedError),

    #[error(
        "cardinality limit exceeded: measurement \"{measurement}\" tag \"{tag_key}\" has {current} values (limit: {limit})"
    )]
    CardinalityExceeded {
        measurement: String,
        tag_key: String,
        current: usize,
        limit: usize,
    },

    #[error("request exceeds maximum point count: {count} points (limit: {limit})")]
    RequestPointLimitExceeded { count: usize, limit: usize },

    #[error("request payload too large: {0}")]
    PayloadTooLarge(String),

    #[error("insufficient storage: {0}")]
    InsufficientStorage(String),

    #[error("WAL backpressure: write queue full for {timeout_ms}ms")]
    WalBackpressure { timeout_ms: u64 },

    #[error(
        "query timeout exceeded; earlier statements in a multi-statement batch may already be committed"
    )]
    QueryTimeout,

    #[error("cluster unavailable: {0}")]
    ClusterUnavailable(String),

    #[error("peer unreachable: {0}")]
    PeerUnreachable(String),

    #[error("sync failed: {0}")]
    SyncFailed(String),

    #[error("replication timeout: {0}")]
    ReplicationTimeout(String),

    #[error(
        "replication quorum timeout: {acks_received}/{required} peer acks received within {timeout_ms}ms"
    )]
    ReplicationQuorumTimeout {
        acks_received: usize,
        required: usize,
        timeout_ms: u64,
    },

    #[error("internal error: {0}")]
    Internal(#[source] ChainedError),
}

// RocksDB errors are mapped per subsystem (e.g. `Wal` in [`crate::adapters::wal::rocksdb_wal`],
// `Metadata` in [`crate::adapters::metadata::rocksdb_meta`]) to avoid mislabeling raft/metadata as WAL.

impl From<std::fmt::Error> for HyperbytedbError {
    fn from(e: std::fmt::Error) -> Self {
        HyperbytedbError::Internal(ChainedError::from_error(e))
    }
}

impl From<std::io::Error> for HyperbytedbError {
    fn from(e: std::io::Error) -> Self {
        HyperbytedbError::Storage(ChainedError::from_error(e))
    }
}

impl From<bincode::Error> for HyperbytedbError {
    fn from(e: bincode::Error) -> Self {
        HyperbytedbError::Internal(ChainedError::from_error(e))
    }
}

#[cfg(test)]
mod tests {
    use super::{ChainedError, HyperbytedbError};
    use std::error::Error;

    #[test]
    fn chained_error_preserves_io_source() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "missing file");
        let chained = ChainedError::from_error(io_err);
        let wal = HyperbytedbError::Wal(chained);
        assert!(wal.source().is_some());
        assert!(wal.source().unwrap().source().is_some());
    }

    #[test]
    fn message_only_chained_error_has_no_source() {
        let err = HyperbytedbError::Wal(ChainedError::new("wal column family not found"));
        assert!(err.source().is_some());
        assert!(err.source().unwrap().source().is_none());
    }
}
