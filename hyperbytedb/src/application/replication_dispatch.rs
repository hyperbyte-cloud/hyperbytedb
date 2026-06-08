use std::sync::Arc;
use std::time::Duration;

use crate::application::system_trace::{self, PhaseTimer};
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
    let mode = match replication_config.mode {
        ReplicationMode::Async => "async",
        ReplicationMode::SyncQuorum => "sync_quorum",
    };
    let span = system_trace::replication_dispatch_span(batch.wal_seq, mode);
    let _guard = span.enter();
    let total_start = system_trace::start_timer();
    let mut dispatch_pt = PhaseTimer::start();

    let result = match replication_config.mode {
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
    };

    dispatch_pt.record_phase_final("dispatch_us");
    let msg = if result.is_ok() {
        "replication dispatch complete"
    } else {
        "replication dispatch failed"
    };
    system_trace::finish_span(&span, total_start, msg);
    result
}
