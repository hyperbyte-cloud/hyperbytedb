//! Region data transfer protocol types.

use serde::{Deserialize, Serialize};

use super::types::ShardEpoch;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TransferPhase {
    /// Destination imports payload.
    Push,
    /// Source acknowledges completion and may drop vacated range.
    Ack,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardTransferPayload {
    pub db: String,
    pub rp: String,
    pub measurement: String,
    pub region_id: u64,
    pub start: u64,
    pub end: u64,
    pub epoch: ShardEpoch,
    pub phase: TransferPhase,
    /// Line-protocol points for `[start, end)` when phase = Push.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub body: Option<Vec<u8>>,
    pub source_node_id: u64,
}

impl ShardTransferPayload {
    #[allow(clippy::too_many_arguments)]
    pub fn push(
        db: impl Into<String>,
        rp: impl Into<String>,
        measurement: impl Into<String>,
        region_id: u64,
        start: u64,
        end: u64,
        epoch: ShardEpoch,
        body: Vec<u8>,
        source_node_id: u64,
    ) -> Self {
        Self {
            db: db.into(),
            rp: rp.into(),
            measurement: measurement.into(),
            region_id,
            start,
            end,
            epoch,
            phase: TransferPhase::Push,
            body: Some(body),
            source_node_id,
        }
    }
}
