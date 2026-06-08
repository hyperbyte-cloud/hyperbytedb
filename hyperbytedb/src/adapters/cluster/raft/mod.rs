pub mod log_store;
pub mod network;
pub mod state_machine;
pub mod types;

use std::io::Cursor;

use openraft::TokioRuntime;

use self::types::{ClusterRequest, ClusterResponse};

openraft::declare_raft_types!(
    pub TypeConfig:
        D            = ClusterRequest,
        R            = ClusterResponse,
        NodeId       = u64,
        Node         = openraft::BasicNode,
        Entry        = openraft::Entry<TypeConfig>,
        SnapshotData = Cursor<Vec<u8>>,
        Responder    = openraft::impls::OneshotResponder<TypeConfig>,
        AsyncRuntime = TokioRuntime,
);

pub type HyperbytedbRaft = openraft::Raft<TypeConfig>;
pub type RaftAdaptor = openraft::storage::Adaptor<TypeConfig, log_store::RaftStore>;
