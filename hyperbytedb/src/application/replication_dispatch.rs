use std::sync::Arc;
use std::time::Duration;

use crate::config::{ReplicationConfig, ReplicationMode, SyncQuorumMinAcks};
use crate::error::HyperbytedbError;
use crate::ports::replication::{OutboundReplicationBatch, ReplicationPort};

/// Fan out a WAL-backed write batch to peers using the configured replication mode.
pub async fn dispatch_outbound_replication(
    replication: Arc<dyn ReplicationPort>,
    node_id: u64,
    replication_config: &ReplicationConfig,
    batch: OutboundReplicationBatch,
) -> Result<(), HyperbytedbError> {
    match replication_config.mode {
        ReplicationMode::Async => {
            replication.replicate_write(batch);
            Ok(())
        }
        ReplicationMode::SyncQuorum => {
            let peer_count = replication.active_peer_count(node_id).await;
            let min_acks: SyncQuorumMinAcks = replication_config.sync_quorum.min_acks;
            let required = min_acks.resolve(peer_count);
            let timeout = Duration::from_millis(replication_config.ack_timeout_ms);
            replication
                .replicate_write_sync(batch, required, timeout)
                .await
        }
    }
}
