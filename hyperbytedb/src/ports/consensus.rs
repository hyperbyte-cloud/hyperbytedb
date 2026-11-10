//! Proposing cluster-wide changes (schema mutations, shard-map ops) to the
//! replicated log.

use crate::domain::cluster::raft_request::{ClusterRequest, ClusterResponse};
use crate::error::HyperbytedbError;
use crate::ports::leadership::LeadershipPort;

#[async_trait::async_trait]
pub trait ConsensusPort: LeadershipPort {
    /// Commit `req`, on this node when it leads, else on the leader. The
    /// response says whether the apply accepted it: a committed op can
    /// still be refused (a stale epoch), so callers check `ok`.
    async fn propose(&self, req: ClusterRequest) -> Result<ClusterResponse, HyperbytedbError>;
}
