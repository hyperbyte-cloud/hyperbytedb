//! Calls to peer nodes, one typed method per internal endpoint. The
//! application uses this instead of building HTTP requests itself.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures::stream::BoxStream;

use crate::domain::cluster::peer_rpc::{PeerRpcError, ShardWriteTarget};
use crate::domain::sharding::types::{
    ShardBootstrapRequest, ShardDeleteRequest, ShardMetadataRequest, ShardQueryRequest,
    ShardWriteBatchResponse,
};

/// A peer's reply body as it arrives.
pub type ByteStream = BoxStream<'static, Result<Bytes, PeerRpcError>>;

#[async_trait::async_trait]
pub trait PeerRpc: Send + Sync {
    /// `POST /internal/shard/bootstrap`.
    async fn shard_bootstrap(
        &self,
        addr: &str,
        req: &ShardBootstrapRequest,
        timeout: Duration,
    ) -> Result<(), PeerRpcError>;

    /// `POST /internal/shard/write`: line protocol for one region.
    async fn shard_write(
        &self,
        addr: &str,
        target: &ShardWriteTarget,
        body: Bytes,
        timeout: Duration,
    ) -> Result<(), PeerRpcError>;

    /// `POST /internal/shard/write-batch`: a bincode `ShardWriteBatch`.
    async fn shard_write_batch(
        &self,
        addr: &str,
        body: Bytes,
        timeout: Duration,
    ) -> Result<ShardWriteBatchResponse, PeerRpcError>;

    /// `POST /internal/shard/query`: the Arrow IPC reply, as it streams.
    async fn shard_query(
        &self,
        addr: &str,
        req: &ShardQueryRequest,
        timeout: Duration,
    ) -> Result<ByteStream, PeerRpcError>;

    /// `POST /internal/shard/metadata`: the JSON reply body.
    async fn shard_metadata(
        &self,
        addr: &str,
        req: &ShardMetadataRequest,
        timeout: Duration,
    ) -> Result<Bytes, PeerRpcError>;

    /// `POST /internal/shard/delete`.
    async fn shard_delete(
        &self,
        addr: &str,
        req: &ShardDeleteRequest,
        timeout: Duration,
    ) -> Result<(), PeerRpcError>;
}

pub type SharedPeerRpc = Arc<dyn PeerRpc>;
