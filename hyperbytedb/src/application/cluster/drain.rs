use metrics::{counter, gauge};
use std::sync::Arc;
use std::time::Duration;

use crate::adapters::cluster::replication_log::ReplicationLog;
use crate::domain::cluster::membership::{NodeState, SharedMembership};
use crate::error::HyperbytedbError;
use crate::ports::flush::FlushPort;
use crate::ports::wal::WalPort;

pub struct DrainService {
    node_id: u64,
    membership: SharedMembership,
    flush_service: Arc<dyn FlushPort>,
    replication_log: Arc<ReplicationLog>,
    wal: Arc<dyn WalPort>,
}

impl DrainService {
    pub fn new(
        node_id: u64,
        membership: SharedMembership,
        flush_service: Arc<dyn FlushPort>,
        replication_log: Arc<ReplicationLog>,
        wal: Arc<dyn WalPort>,
    ) -> Self {
        Self {
            node_id,
            membership,
            flush_service,
            replication_log,
            wal,
        }
    }

    /// Execute the full drain procedure for graceful scale-down.
    /// 1. Set node state to Draining (rejects new writes)
    /// 2. Flush all WAL entries into native chDB tables
    /// 3. Wait for all peers to acknowledge replication
    /// 4. Notify peers of leave
    /// 5. Set node state to Leaving
    pub async fn drain(&self) -> Result<(), HyperbytedbError> {
        counter!("hyperbytedb_drain_total").increment(1);
        gauge!("hyperbytedb_cluster_node_state").set(4.0); // Draining
        tracing::info!(node_id = self.node_id, "starting drain procedure");

        {
            let mut m = self.membership.write().await;
            m.set_state(self.node_id, NodeState::Draining);
            tracing::debug!("node state set to Draining, rejecting new writes");
        }

        tracing::debug!("flushing all WAL entries to chDB");
        self.flush_service.drain().await?;

        tracing::debug!("waiting for peer replication acknowledgments");
        self.wait_for_replication_acks().await?;

        self.notify_peers_leave().await;

        {
            let mut m = self.membership.write().await;
            m.set_state(self.node_id, NodeState::Leaving);
            gauge!("hyperbytedb_cluster_node_state").set(5.0); // Leaving
            tracing::debug!("node state set to Leaving");
        }

        tracing::info!("drain procedure complete");
        Ok(())
    }

    async fn wait_for_replication_acks(&self) -> Result<(), HyperbytedbError> {
        let local_wal_seq = self.wal.last_sequence().await?;
        let local_mutation_seq = self.replication_log.last_mutation_seq();

        // Must stay in sync with the operator preStop poll window (drainAckWaitSecs).
        let max_wait = Duration::from_secs(90);
        let start = std::time::Instant::now();

        loop {
            if start.elapsed() > max_wait {
                tracing::warn!("timed out waiting for replication acks");
                break;
            }

            let peers = {
                let m = self.membership.read().await;
                m.active_peers(self.node_id)
                    .iter()
                    .map(|n| n.node_id)
                    .collect::<Vec<_>>()
            };

            let mut all_acked = true;
            for peer_id in &peers {
                let wal_ack = self.replication_log.get_wal_ack(*peer_id)?;
                let mutation_ack = self.replication_log.get_mutation_ack(*peer_id)?;

                if wal_ack < local_wal_seq || mutation_ack < local_mutation_seq {
                    all_acked = false;
                    tracing::debug!(
                        peer_id = peer_id,
                        wal_ack = wal_ack,
                        local_wal = local_wal_seq,
                        mutation_ack = mutation_ack,
                        local_mutation = local_mutation_seq,
                        "waiting for peer ack"
                    );
                }
            }

            if all_acked {
                tracing::debug!("all peers acknowledged replication");
                break;
            }

            tokio::time::sleep(Duration::from_secs(2)).await;
        }

        Ok(())
    }

    async fn notify_peers_leave(&self) {
        let peers = {
            let m = self.membership.read().await;
            m.active_peers(self.node_id)
                .iter()
                .map(|n| n.addr.clone())
                .collect::<Vec<_>>()
        };

        let client = match reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
            .build()
        {
            Ok(c) => c,
            Err(e) => {
                tracing::error!(error = %e, "failed to build leave-notify client");
                return;
            }
        };

        let leave_req = crate::domain::cluster::sync::LeaveRequest {
            node_id: self.node_id,
        };

        for peer_addr in &peers {
            let url = format!("http://{}/internal/membership/leave", peer_addr);
            match client.post(&url).json(&leave_req).send().await {
                Ok(resp) if resp.status().is_success() => {
                    tracing::debug!(peer = %peer_addr, "notified peer of leave");
                }
                Ok(resp) => {
                    tracing::warn!(
                        peer = %peer_addr,
                        status = %resp.status(),
                        "peer leave notification failed"
                    );
                }
                Err(e) => {
                    tracing::warn!(
                        peer = %peer_addr,
                        error = %e,
                        "failed to notify peer of leave"
                    );
                }
            }
        }
    }
}
