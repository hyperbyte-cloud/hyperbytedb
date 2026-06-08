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
