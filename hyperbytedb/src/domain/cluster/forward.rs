//! Writes a coordinator owes to other region primaries.

use serde::{Deserialize, Serialize};

/// One queued forward: points (binary frames) for one database and
/// retention policy, re-partitioned against the current shard map when sent.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ForwardEntry {
    pub db: String,
    pub rp: String,
    pub precision: Option<String>,
    /// When the client write was accepted. Kept across retries so the queue's
    /// age reports how far primaries trail, not how recently a retry landed.
    pub enqueued_ms: u64,
    /// Not handed out before this time (retry backoff).
    pub not_before_ms: u64,
    pub attempts: u32,
    pub body: Vec<u8>,
}
