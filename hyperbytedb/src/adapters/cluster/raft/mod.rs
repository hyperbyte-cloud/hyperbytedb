pub mod log_store;
#[cfg(madsim)]
pub mod madsim_runtime;
pub mod network;
pub mod state_machine;
pub mod types;

use std::io::Cursor;

// openraft's tasks, timers, clock and RNG run on madsim's deterministic
// runtime in the simulation rig, and on tokio everywhere else.
#[cfg(madsim)]
use self::madsim_runtime::MadsimRuntime as RaftRuntime;
#[cfg(not(madsim))]
use openraft::TokioRuntime as RaftRuntime;

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
        AsyncRuntime = RaftRuntime,
);

pub type HyperbytedbRaft = openraft::Raft<TypeConfig>;
pub type RaftAdaptor = openraft::storage::Adaptor<TypeConfig, log_store::RaftStore>;
