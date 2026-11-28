//! Requests that go through Raft consensus and their responses. Their JSON
//! is the Raft log's wire format: keep it stable across versions.

use serde::{Deserialize, Serialize};

use crate::domain::cluster::membership::NodeState;
use crate::domain::cluster::types::MutationRequest;
use crate::domain::sharding::ShardMapOp;

/// Application-level request that goes through Raft consensus.
/// Only cluster coordination and schema DDL use Raft -- high-throughput
/// time-series writes stay on the existing fan-out data plane.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ClusterRequest {
    SetNodeState { node_id: u64, state: NodeState },
    SchemaMutation(Box<MutationRequest>),
    ShardMapMutation(Box<ShardMapOp>),
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

    pub fn error(msg: impl Into<String>) -> Self {
        Self {
            ok: false,
            message: Some(msg.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::cluster::membership::NodeState;
    use crate::domain::cluster::types::MutationRequest;
    use crate::domain::sharding::ShardMapOp;

    /// Raft log entries written by older nodes must still decode: the JSON
    /// of every variant is pinned to what it was before the types moved
    /// out of the Raft adapter.
    #[test]
    fn cluster_request_json_is_unchanged() {
        let cases = [
            (
                ClusterRequest::SetNodeState {
                    node_id: 3,
                    state: NodeState::Active,
                },
                r#"{"SetNodeState":{"node_id":3,"state":"Active"}}"#,
            ),
            (
                ClusterRequest::SchemaMutation(Box::new(MutationRequest::DropDatabase(
                    "db".into(),
                ))),
                r#"{"SchemaMutation":{"DropDatabase":"db"}}"#,
            ),
            (
                ClusterRequest::ShardMapMutation(Box::new(ShardMapOp::DropSpace {
                    db: "db".into(),
                    rp: Some("autogen".into()),
                    measurement: None,
                })),
                r#"{"ShardMapMutation":{"DropSpace":{"db":"db","rp":"autogen","measurement":null}}}"#,
            ),
        ];
        for (req, json) in cases {
            assert_eq!(serde_json::to_string(&req).unwrap(), json);
            let back: ClusterRequest = serde_json::from_str(json).unwrap();
            assert_eq!(serde_json::to_string(&back).unwrap(), json);
        }
        assert_eq!(
            serde_json::to_string(&ClusterResponse::error("x")).unwrap(),
            r#"{"ok":false,"message":"x"}"#
        );
    }
}
