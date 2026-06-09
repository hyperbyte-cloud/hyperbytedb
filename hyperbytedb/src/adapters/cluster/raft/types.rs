use serde::{Deserialize, Serialize};

use crate::domain::cluster::membership::NodeState;
use crate::domain::cluster::types::MutationRequest;

/// Application-level request that goes through Raft consensus.
/// Only cluster coordination and schema DDL use Raft -- high-throughput
/// time-series writes stay on the existing fan-out data plane.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ClusterRequest {
    SetNodeState { node_id: u64, state: NodeState },
    SchemaMutation(Box<MutationRequest>),
}

/// Response returned after a ClusterRequest is committed and applied.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterResponse {
    pub ok: bool,
    pub message: Option<String>,
}

impl ClusterResponse {
    pub fn success() -> Self {
        Self {
            ok: true,
            message: None,
        }
    }

    pub fn with_message(msg: impl Into<String>) -> Self {
        Self {
            ok: true,
            message: Some(msg.into()),
        }
    }

    pub fn error(msg: impl Into<String>) -> Self {
        Self {
            ok: false,
            message: Some(msg.into()),
        }
    }
}
