use serde::{Deserialize, Serialize};

use crate::domain::point::Point;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct WalEntry {
    pub database: String,
    pub retention_policy: String,
    pub points: Vec<Point>,
    /// Node that originated this write. 0 means the local node.
    pub origin_node_id: u64,
}

/// A row delete that ran when the WAL was flushed through `flushed_through`.
///
/// A restarted node flushes the WAL entries it still holds again, which puts
/// back rows a delete removed. Replaying the delete once the entries up to
/// `flushed_through` are flushed again removes them a second time, and the
/// entries after it, written after the delete, are flushed on top.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeleteMarker {
    pub flushed_through: u64,
    pub database: String,
    pub retention_policy: String,
    pub measurement: String,
    pub predicate: String,
}
