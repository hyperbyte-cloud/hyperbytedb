//! Region data transfer protocol types.

use serde::{Deserialize, Serialize};

use super::types::ShardEpoch;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TransferPhase {
    /// Destination imports payload.
    #[serde(alias = "Push")]
    Push,
    /// Source acknowledges completion and may drop vacated range.
    #[serde(alias = "Ack")]
    Ack,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
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
    /// Correlates Push chunks and the final Ack for one transfer attempt.
    #[serde(default)]
    pub transfer_id: u64,
    /// Monotonic chunk index within `transfer_id` (Push only).
    #[serde(default)]
    pub seq: u64,
    /// True on the last Push chunk, or on Ack.
    #[serde(default)]
    pub done: bool,
    /// Pre-commit staging push (split optimization): the destination applies
    /// rows for `[start, end)` WITHOUT requiring a committed region match,
    /// because the Split that will own this range has not committed yet.
    /// Restricted to known cluster members by the receiving handler.
    #[serde(default)]
    pub stage: bool,
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
        transfer_id: u64,
        seq: u64,
        done: bool,
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
            transfer_id,
            seq,
            done,
            stage: false,
        }
    }

    /// Pre-commit staging push for a not-yet-committed split child range.
    #[allow(clippy::too_many_arguments)]
    pub fn stage_push(
        db: impl Into<String>,
        rp: impl Into<String>,
        measurement: impl Into<String>,
        region_id: u64,
        start: u64,
        end: u64,
        epoch: ShardEpoch,
        body: Vec<u8>,
        source_node_id: u64,
        transfer_id: u64,
        seq: u64,
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
            transfer_id,
            seq,
            done: false,
            stage: true,
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn ack(
        db: impl Into<String>,
        rp: impl Into<String>,
        measurement: impl Into<String>,
        region_id: u64,
        start: u64,
        end: u64,
        epoch: ShardEpoch,
        source_node_id: u64,
        transfer_id: u64,
    ) -> Self {
        Self {
            db: db.into(),
            rp: rp.into(),
            measurement: measurement.into(),
            region_id,
            start,
            end,
            epoch,
            phase: TransferPhase::Ack,
            body: None,
            source_node_id,
            transfer_id,
            seq: 0,
            done: true,
            stage: false,
        }
    }
}

/// Ask the node currently holding `[start, end)` to push its local rows to
/// `dest_primary`. Used after a Split when the Raft leader is neither
/// the old primary nor the new child primary — only the data holder can export.
///
/// `drop_source` controls whether the source copy is deleted after a verified
/// push: `true` when the range's ownership genuinely moved away from the
/// source (split re-homing), `false` for primary moves that keep ownership
/// ambiguous until a subsequent map op commits (pre-merge staging).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ShardRehomeRequest {
    pub db: String,
    pub rp: String,
    pub measurement: String,
    pub start: u64,
    pub end: u64,
    pub epoch: ShardEpoch,
    pub dest_primary: u64,
    #[serde(default)]
    pub drop_source: bool,
    /// When true, the destination is not yet a committed peer (`AddPeer`
    /// staging). Uses the stage transfer that skips dest ownership checks.
    #[serde(default)]
    pub stage: bool,
}

#[cfg(test)]
mod tests {
    use super::TransferPhase;

    #[test]
    fn transfer_phase_decodes_pascal_and_snake_case() {
        assert_eq!(
            serde_json::from_str::<TransferPhase>(r#""Push""#).unwrap(),
            TransferPhase::Push
        );
        assert_eq!(
            serde_json::from_str::<TransferPhase>(r#""Ack""#).unwrap(),
            TransferPhase::Ack
        );
        assert_eq!(
            serde_json::from_str::<TransferPhase>(r#""push""#).unwrap(),
            TransferPhase::Push
        );
        assert_eq!(
            serde_json::from_str::<TransferPhase>(r#""ack""#).unwrap(),
            TransferPhase::Ack
        );
        assert_eq!(
            serde_json::to_string(&TransferPhase::Push).unwrap(),
            r#""push""#
        );
    }
}
