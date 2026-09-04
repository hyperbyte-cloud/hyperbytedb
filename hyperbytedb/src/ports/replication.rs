use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;

use crate::domain::cluster::types::MutationRequest;
use crate::error::HyperbytedbError;

/// One outbound replication batch (Influx line protocol body + routing metadata).
pub struct OutboundReplicationBatch {
    pub database: String,
    pub retention_policy: String,
    pub precision: Option<String>,
    pub body: Vec<u8>,
    pub wal_seq: u64,
    /// When set, replicate only to these node ids (sharding region peers).
    pub target_node_ids: Option<Vec<u64>>,
}

/// Outbound write/mutation replication to cluster peers.
#[async_trait]
pub trait ReplicationPort: Send + Sync {
    fn replicate_write(
        self: Arc<Self>,
        batch: OutboundReplicationBatch,
    ) -> Result<(), HyperbytedbError>;

    async fn replicate_write_sync(
        self: Arc<Self>,
        batch: OutboundReplicationBatch,
        required_acks: usize,
        timeout: Duration,
    ) -> Result<(), HyperbytedbError>;

    fn replicate_mutation(self: Arc<Self>, req: MutationRequest, target_node_ids: Option<Vec<u64>>);

    async fn replicate_mutation_sync(
        self: Arc<Self>,
        req: MutationRequest,
    ) -> Result<(), HyperbytedbError>;

    async fn active_peer_count(&self, self_node_id: u64) -> usize;
}
