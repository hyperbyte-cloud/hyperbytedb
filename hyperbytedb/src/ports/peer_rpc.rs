//! Calls to peer nodes, one typed method per internal endpoint. The
//! application uses this instead of building HTTP requests itself.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures::stream::BoxStream;

use crate::domain::cluster::membership::SharedMembership;
use crate::domain::cluster::peer_rpc::{PeerRpcError, ShardWriteTarget};
use crate::domain::sharding::transfer::{ShardRehomeRequest, ShardTransferPayload};
use crate::domain::sharding::types::{
    MvHandoffRequest, RegionHeartbeat, ShardBootstrapRequest, ShardDeleteRequest,
    ShardMetadataRequest, ShardMvBackfillRequest, ShardQueryRequest, ShardWriteBatchResponse,
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

    /// A transfer push: the raw route first (`/internal/shard/transfer/raw`,
    /// the line protocol as the body), and on 404 (an older peer) the JSON
    /// route. Returns the reply text.
    async fn shard_transfer_push(
        &self,
        addr: &str,
        payload: ShardTransferPayload,
        timeout: Duration,
    ) -> Result<String, PeerRpcError>;

    /// `POST /internal/shard/transfer` with an ack payload.
    async fn shard_transfer_ack(
        &self,
        addr: &str,
        payload: &ShardTransferPayload,
        timeout: Duration,
    ) -> Result<(), PeerRpcError>;

    /// `POST /internal/shard/mv-backfill`.
    async fn shard_mv_backfill(
        &self,
        addr: &str,
        req: &ShardMvBackfillRequest,
        timeout: Duration,
    ) -> Result<(), PeerRpcError>;

    /// `POST /internal/mv/handoff`, to the departing node.
    async fn mv_handoff(
        &self,
        addr: &str,
        req: &MvHandoffRequest,
        timeout: Duration,
    ) -> Result<(), PeerRpcError>;

    /// `POST /internal/shard/heartbeat`, to the leader.
    async fn shard_heartbeat(
        &self,
        addr: &str,
        hb: &RegionHeartbeat,
        timeout: Duration,
    ) -> Result<(), PeerRpcError>;

    /// `POST /internal/shard/rehome` with progress streaming: waits as long
    /// as the peer keeps sending (up to `max`), gives up after `idle` of
    /// silence. Returns the final JSON body (which carries its own status).
    async fn shard_rehome(
        &self,
        addr: &str,
        req: &ShardRehomeRequest,
        idle: Duration,
        max: Duration,
    ) -> Result<serde_json::Value, PeerRpcError>;

    /// `GET /internal/sync/manifest`.
    async fn sync_manifest(
        &self,
        addr: &str,
        timeout: Duration,
    ) -> Result<crate::domain::cluster::sync::SyncManifest, PeerRpcError>;

    /// `GET /internal/shard/map`.
    async fn shard_map(
        &self,
        addr: &str,
        timeout: Duration,
    ) -> Result<crate::domain::sharding::ShardMap, PeerRpcError>;
}

pub type SharedPeerRpc = Arc<dyn PeerRpc>;

/// The port plus the membership its callers resolve node ids against.
#[derive(Clone)]
pub struct Peers {
    pub rpc: SharedPeerRpc,
    pub membership: SharedMembership,
}
