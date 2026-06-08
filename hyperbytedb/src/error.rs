use thiserror::Error;

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
    Wal(String),

    #[error("storage error: {0}")]
    Storage(String),

    #[error("chdb error: {0}")]
    Chdb(String),

    #[error("metadata error: {0}")]
    Metadata(String),

    #[error(
        "cardinality limit exceeded: measurement \"{measurement}\" tag \"{tag_key}\" has {current} values (limit: {limit})"
    )]
    CardinalityExceeded {
        measurement: String,
        tag_key: String,
        current: usize,
        limit: usize,
    },

    #[error("query timeout exceeded")]
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
    Internal(String),
}

// RocksDB errors are mapped per subsystem (e.g. `Wal` in [`crate::adapters::wal::rocksdb_wal`],
// `Metadata` in [`crate::adapters::metadata::rocksdb_meta`]) to avoid mislabeling raft/metadata as WAL.

impl From<std::fmt::Error> for HyperbytedbError {
    fn from(e: std::fmt::Error) -> Self {
        HyperbytedbError::Internal(e.to_string())
    }
}

impl From<std::io::Error> for HyperbytedbError {
    fn from(e: std::io::Error) -> Self {
        HyperbytedbError::Storage(e.to_string())
    }
}

impl From<bincode::Error> for HyperbytedbError {
    fn from(e: bincode::Error) -> Self {
        HyperbytedbError::Internal(e.to_string())
    }
}
