use metrics::{counter, gauge};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
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
use crate::ports::query::QueryPort;
use crate::ports::sharding::ShardMapPort;
use crate::ports::wal::WalPort;

/// Runs the two node-lifecycle procedures that hand off this node's primary
/// regions before it stops serving: [`DrainService::drain`] (a restart --
/// the node keeps its seat and its data) and [`DrainService::decommission`]
/// (a permanent departure -- an evacuation scheduler, not this service,
/// evacuates regions and declares the node `Leaving`).
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
    query_port: Option<Arc<dyn QueryPort>>,
    raft: Option<HyperbytedbRaft>,
    max_points_per_request: usize,
    /// Set once [`DrainService::drain`] has run to completion (or determined
    /// there was nothing to do because the node was already in a terminal
    /// lifecycle state). Lets the shutdown sequence skip a redundant drain
    /// when the Kubernetes preStop hook already ran one -- checking local
    /// state is no longer possible for this now that a successful drain ends
    /// at `Draining`, not `Leaving`, indistinguishable from "never drained".
    drained: AtomicBool,
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
            query_port: None,
            raft: None,
            max_points_per_request: 0,
            drained: AtomicBool::new(false),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn with_sharding(
        mut self,
        ctx: Arc<ShardRoutingContext>,
        peer_client: Arc<PeerClient>,
        metadata: Arc<dyn MetadataPort>,
        points_sink: Arc<dyn PointsSinkPort>,
        query_port: Arc<dyn QueryPort>,
        raft: HyperbytedbRaft,
        max_points_per_request: usize,
    ) -> Self {
        self.shard_routing = Some(ctx);
        self.peer_client = Some(peer_client);
        self.metadata = Some(metadata);
        self.points_sink = Some(points_sink);
        self.query_port = Some(query_port);
        self.raft = Some(raft);
        self.max_points_per_request = max_points_per_request;
        self
    }

    /// True once [`DrainService::drain`] has completed (successfully or as a
    /// guarded no-op) at least once. Used by the shutdown sequence to avoid
    /// re-running drain when the preStop hook already did.
    #[must_use]
    pub fn has_drained(&self) -> bool {
        self.drained.load(Ordering::SeqCst)
    }

    /// Execute the drain procedure for a restart: hand primaries off so
    /// writes stop landing here, but keep this node's seat in every region's
    /// peer set and keep its local data, so restarting it resumes serving
    /// with nothing to re-stage. Ends in `Draining`, never `Leaving`.
    ///
    /// A no-op (besides logging) when the node is already `Decommissioning`
    /// or `Leaving`: [`crate::domain::cluster::membership::ClusterMembership::transition`]
    /// refuses to downgrade a terminal lifecycle state back to `Draining`, so
    /// the Kubernetes preStop hook calling `/internal/drain` on a pod the
    /// operator already decommissioned cannot re-admit it mid-evacuation.
    pub async fn drain(&self) -> Result<(), HyperbytedbError> {
        counter!("hyperbytedb_drain_total").increment(1);

        {
            let mut m = self.membership.write().await;
            if !m.transition(self.node_id, NodeState::Draining) {
                let current = m.get_node(self.node_id).map(|n| n.state);
                tracing::info!(
                    node_id = self.node_id,
                    ?current,
                    "drain skipped: node already in a terminal lifecycle state"
                );
                self.drained.store(true, Ordering::SeqCst);
                return Ok(());
            }
        }
        gauge!("hyperbytedb_cluster_node_state").set(4.0);
        tracing::info!(node_id = self.node_id, "starting drain procedure");

        if let Err(e) = self.transfer_primaries().await {
            tracing::warn!(error = %e, "primary handoff during drain failed");
        }

        self.flush_service.drain().await?;
        self.wait_for_replication_acks().await?;

        self.drained.store(true, Ordering::SeqCst);
        tracing::info!("drain procedure complete");
        Ok(())
    }

    /// Execute the decommission procedure for a permanent departure: mark the
    /// node `Decommissioning`, hand off primaries, flush and wait for
    /// replication acks, then return. Never declares `Leaving` itself -- a
    /// separate evacuation scheduler (not part of this service) evacuates
    /// the node's regions and is the only thing that writes `Leaving` for a
    /// decommissioning node.
    ///
    /// Like [`DrainService::drain`], a no-op (besides logging) when the node
    /// is already `Decommissioning` or `Leaving`.
    pub async fn decommission(&self) -> Result<(), HyperbytedbError> {
        counter!("hyperbytedb_decommission_total").increment(1);

        {
            let mut m = self.membership.write().await;
            if !m.transition(self.node_id, NodeState::Decommissioning) {
                let current = m.get_node(self.node_id).map(|n| n.state);
                tracing::info!(
                    node_id = self.node_id,
                    ?current,
                    "decommission skipped: node already in a terminal lifecycle state"
                );
                return Ok(());
            }
        }
        // 6.0: one past Leaving (5.0) in the ad hoc node-state gauge scale
        // used across bootstrap.rs / drain.rs / peer_handlers.rs.
        gauge!("hyperbytedb_cluster_node_state").set(6.0);
        tracing::info!(node_id = self.node_id, "starting decommission procedure");

        if let Err(e) = self.transfer_primaries().await {
            tracing::warn!(error = %e, "primary handoff during decommission failed");
        }

        self.flush_service.drain().await?;
        self.wait_for_replication_acks().await?;

        tracing::info!(
            node_id = self.node_id,
            "decommission handoff complete; region evacuation and final removal \
             are the shard scheduler's job"
        );
        Ok(())
    }

    /// Hand off the primary role for every region this node currently leads.
    ///
    /// Proposes only [`ShardMapOp::TransferPrimary`] -- never `MovePeer` --
    /// and passes `drop_source: false` into [`run_region_transfer`]. Shared by
    /// [`DrainService::drain`] (the seat and local copy must stay: the node
    /// is coming back) and [`DrainService::decommission`] (giving up the seat
    /// and dropping the local copy is the evacuation scheduler's job, run
    /// only once a replacement primary and full peer set are established).
    async fn transfer_primaries(&self) -> Result<(), HyperbytedbError> {
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
                        HyperbytedbError::ShardMap("no alternate primary for handoff".into())
                    })?;

                run_region_transfer(
                    pc,
                    metadata,
                    &self.wal,
                    self.query_port.as_ref(),
                    self.points_sink.as_ref(),
                    self.node_id,
                    &space.key,
                    region,
                    new_primary,
                    self.max_points_per_request.max(1),
                    // Only the crown moves here. The seat (and this node's
                    // local copy) stays -- see the doc comment above.
                    false,
                )
                .await?;

                use crate::adapters::cluster::raft::types::ClusterRequest;
                let tp = ShardMapOp::TransferPrimary {
                    key: space.key.clone(),
                    region_id: region.region_id,
                    new_primary,
                    epoch: region.epoch,
                };
                raft.client_write(ClusterRequest::ShardMapMutation(Box::new(tp)))
                    .await
                    .map_err(|e| HyperbytedbError::ShardMap(e.to_string().into()))?;
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapters::wal::rocksdb_wal::RocksDbWal;
    use crate::domain::cluster::membership::{ClusterMembership, NodeInfo, new_shared};
    use async_trait::async_trait;

    struct NoopFlush;

    #[async_trait]
    impl FlushPort for NoopFlush {
        async fn drain(&self) -> Result<(), HyperbytedbError> {
            Ok(())
        }
    }

    fn make_node(id: u64, state: NodeState) -> NodeInfo {
        NodeInfo {
            node_id: id,
            addr: format!("127.0.0.1:{}", 8080 + id),
            state,
            joined_at: 0,
            last_heartbeat: 0,
            needs_sync: false,
            consecutive_misses: 0,
        }
    }

    /// A `DrainService` with no sharding wired up (`shard_routing: None`), so
    /// `transfer_primaries` is a no-op and only the lifecycle-state and
    /// replication-ack-wait behaviour is exercised. `active_peers` on a
    /// single-node membership is empty, so `wait_for_replication_acks`
    /// returns immediately without needing a real peer.
    fn make_service(dir: &tempfile::TempDir, initial_state: NodeState) -> DrainService {
        let membership = ClusterMembership::new();
        let shared = new_shared(membership);
        {
            let mut m = shared.try_write().expect("uncontended in test setup");
            m.add_node(make_node(1, initial_state));
        }

        let wal = RocksDbWal::open(dir.path().join("wal")).expect("open test wal");
        let replication_log =
            ReplicationLog::open(dir.path().join("repl")).expect("open test replication log");

        DrainService::new(
            1,
            shared,
            Arc::new(NoopFlush),
            Arc::new(replication_log),
            Arc::new(wal),
        )
    }

    #[tokio::test]
    async fn drain_ends_in_draining_not_leaving() {
        let dir = tempfile::tempdir().expect("tempdir");
        let svc = make_service(&dir, NodeState::Active);

        svc.drain().await.expect("drain succeeds");

        let m = svc.membership.read().await;
        assert_eq!(m.get_node(1).map(|n| n.state), Some(NodeState::Draining));
        assert!(svc.has_drained());
    }

    #[tokio::test]
    async fn decommission_ends_in_decommissioning_not_leaving() {
        let dir = tempfile::tempdir().expect("tempdir");
        let svc = make_service(&dir, NodeState::Active);

        svc.decommission().await.expect("decommission succeeds");

        let m = svc.membership.read().await;
        assert_eq!(
            m.get_node(1).map(|n| n.state),
            Some(NodeState::Decommissioning)
        );
    }

    #[tokio::test]
    async fn drain_does_not_resurrect_or_downgrade_a_decommissioning_node() {
        // The Kubernetes preStop hook calls /internal/drain on every pod,
        // including one the operator already decommissioned. Without the
        // transition() guard this call would clobber Decommissioning back to
        // Draining, re-admitting the node as a placement candidate mid
        // evacuation.
        let dir = tempfile::tempdir().expect("tempdir");
        let svc = make_service(&dir, NodeState::Decommissioning);

        svc.drain().await.expect("drain is a guarded no-op");

        let m = svc.membership.read().await;
        assert_eq!(
            m.get_node(1).map(|n| n.state),
            Some(NodeState::Decommissioning),
            "drain must not downgrade a decommissioning node"
        );
        drop(m);
        assert!(
            svc.has_drained(),
            "shutdown must still treat this as drained so it does not retry"
        );
    }

    #[tokio::test]
    async fn decommission_then_drain_stays_decommissioning() {
        let dir = tempfile::tempdir().expect("tempdir");
        let svc = make_service(&dir, NodeState::Active);

        svc.decommission().await.expect("decommission succeeds");
        svc.drain().await.expect("drain is a guarded no-op");

        let m = svc.membership.read().await;
        assert_eq!(
            m.get_node(1).map(|n| n.state),
            Some(NodeState::Decommissioning)
        );
    }

    #[tokio::test]
    async fn decommission_is_idempotent() {
        let dir = tempfile::tempdir().expect("tempdir");
        let svc = make_service(&dir, NodeState::Active);

        svc.decommission().await.expect("first decommission");
        svc.decommission()
            .await
            .expect("second decommission is a no-op");

        let m = svc.membership.read().await;
        assert_eq!(
            m.get_node(1).map(|n| n.state),
            Some(NodeState::Decommissioning)
        );
    }

    #[tokio::test]
    async fn has_drained_is_false_before_drain_runs() {
        let dir = tempfile::tempdir().expect("tempdir");
        let svc = make_service(&dir, NodeState::Active);
        assert!(!svc.has_drained());
    }
}
