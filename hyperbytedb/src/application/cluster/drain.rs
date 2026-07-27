use metrics::{counter, gauge};
use std::sync::Arc;
use std::time::Duration;

use crate::adapters::cluster::peer_client::PeerClient;
use crate::adapters::cluster::raft::HyperbytedbRaft;
use crate::adapters::cluster::replication_log::ReplicationLog;
use crate::application::shard_routing::ShardRoutingContext;
use crate::application::shard_transfer::run_region_transfer;
use crate::domain::cluster::membership::{NodeState, SharedMembership};
use crate::domain::sharding::ShardMapOp;
use crate::error::HyperbytedbError;
use crate::ports::flush::FlushPort;
use crate::ports::metadata::MetadataPort;
use crate::ports::points_sink::PointsSinkPort;
use crate::ports::sharding::ShardMapPort;
use crate::ports::wal::WalPort;

pub struct DrainService {
    node_id: u64,
    membership: SharedMembership,
    flush_service: Arc<dyn FlushPort>,
    replication_log: Arc<ReplicationLog>,
    wal: Arc<dyn WalPort>,
    shard_routing: Option<Arc<ShardRoutingContext>>,
    peer_client: Option<Arc<PeerClient>>,
    metadata: Option<Arc<dyn MetadataPort>>,
    points_sink: Option<Arc<dyn PointsSinkPort>>,
    raft: Option<HyperbytedbRaft>,
    max_points_per_request: usize,
}

impl DrainService {
    #[allow(clippy::too_many_arguments)]
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
            shard_routing: None,
            peer_client: None,
            metadata: None,
            points_sink: None,
            raft: None,
            max_points_per_request: 0,
        }
    }

    pub fn with_sharding(
        mut self,
        ctx: Arc<ShardRoutingContext>,
        peer_client: Arc<PeerClient>,
        metadata: Arc<dyn MetadataPort>,
        points_sink: Arc<dyn PointsSinkPort>,
        raft: HyperbytedbRaft,
        max_points_per_request: usize,
    ) -> Self {
        self.shard_routing = Some(ctx);
        self.peer_client = Some(peer_client);
        self.metadata = Some(metadata);
        self.points_sink = Some(points_sink);
        self.raft = Some(raft);
        self.max_points_per_request = max_points_per_request;
        self
    }

    /// Execute the full drain procedure for graceful scale-down.
    pub async fn drain(&self) -> Result<(), HyperbytedbError> {
        counter!("hyperbytedb_drain_total").increment(1);
        gauge!("hyperbytedb_cluster_node_state").set(4.0);
        tracing::info!(node_id = self.node_id, "starting drain procedure");

        {
            let mut m = self.membership.write().await;
            m.set_state(self.node_id, NodeState::Draining);
        }

        if let Err(e) = self.shard_handoff().await {
            tracing::warn!(error = %e, "shard handoff during drain failed");
        }

        self.flush_service.drain().await?;
        self.wait_for_replication_acks().await?;
        self.notify_peers_leave().await;

        {
            let mut m = self.membership.write().await;
            m.set_state(self.node_id, NodeState::Leaving);
            gauge!("hyperbytedb_cluster_node_state").set(5.0);
        }

        tracing::info!("drain procedure complete");
        Ok(())
    }

    async fn shard_handoff(&self) -> Result<(), HyperbytedbError> {
        let ctx = match self.shard_routing.as_ref() {
            Some(c) => c,
            None => return Ok(()),
        };
        let pc = self
            .peer_client
            .as_ref()
            .ok_or_else(|| HyperbytedbError::ClusterUnavailable("no peer client".into()))?;
        let metadata = self
            .metadata
            .as_ref()
            .ok_or_else(|| HyperbytedbError::ClusterUnavailable("no metadata".into()))?;
        let raft = self
            .raft
            .as_ref()
            .ok_or_else(|| HyperbytedbError::ClusterUnavailable("no raft".into()))?;

        let map = ctx.shard_map.snapshot().await?;
        for space in map.spaces.values() {
            for region in &space.regions {
                if region.primary != self.node_id {
                    continue;
                }
                let new_primary = region
                    .peers
                    .iter()
                    .copied()
                    .find(|id| *id != self.node_id)
                    .ok_or_else(|| {
                        HyperbytedbError::ShardMap("no alternate primary for drain handoff".into())
                    })?;

                run_region_transfer(
                    pc,
                    metadata,
                    &self.wal,
                    self.points_sink.as_ref(),
                    self.node_id,
                    &space.key,
                    region,
                    new_primary,
                    self.max_points_per_request.max(1),
                )
                .await?;

                use crate::adapters::cluster::raft::types::ClusterRequest;
                let tp = ShardMapOp::TransferPrimary {
                    key: space.key.clone(),
                    region_id: region.region_id,
                    new_primary,
                    epoch: region.epoch.clone(),
                };
                raft.client_write(ClusterRequest::ShardMapMutation(Box::new(tp)))
                    .await
                    .map_err(|e| HyperbytedbError::ShardMap(e.to_string()))?;

                let mp = ShardMapOp::MovePeer {
                    key: space.key.clone(),
                    region_id: region.region_id,
                    from_peer: self.node_id,
                    to_peer: new_primary,
                    epoch: region.epoch.clone(),
                };
                raft.client_write(ClusterRequest::ShardMapMutation(Box::new(mp)))
                    .await
                    .map_err(|e| HyperbytedbError::ShardMap(e.to_string()))?;
            }
        }
        Ok(())
    }

    async fn wait_for_replication_acks(&self) -> Result<(), HyperbytedbError> {
        let local_wal_seq = self.wal.last_sequence().await?;
        let local_mutation_seq = self.replication_log.last_mutation_seq();
        let max_wait = Duration::from_secs(90);
        let start = std::time::Instant::now();

        loop {
            if start.elapsed() > max_wait {
                tracing::warn!("timed out waiting for replication acks");
                return Err(HyperbytedbError::Internal(crate::error::ChainedError::new(
                    "timed out waiting for peer replication acks during drain",
                )));
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
                }
            }

            if all_acked {
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
            Err(_) => return,
        };

        let leave_req = crate::domain::cluster::sync::LeaveRequest {
            node_id: self.node_id,
        };

        for peer_addr in &peers {
            let url = format!("http://{}/internal/membership/leave", peer_addr);
            let _ = client.post(&url).json(&leave_req).send().await;
        }
    }
}
