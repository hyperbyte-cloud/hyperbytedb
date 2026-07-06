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
}

/// Outbound write/mutation replication to cluster peers.
#[async_trait]
pub trait ReplicationPort: Send + Sync {
    fn replicate_write(self: Arc<Self>, batch: OutboundReplicationBatch);

    async fn replicate_write_sync(
        self: Arc<Self>,
        batch: OutboundReplicationBatch,
        required_acks: usize,
        timeout: Duration,
    ) -> Result<(), HyperbytedbError>;

    fn replicate_mutation(self: Arc<Self>, req: MutationRequest);

    async fn active_peer_count(&self, self_node_id: u64) -> usize;
}
