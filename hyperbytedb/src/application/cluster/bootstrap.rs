use std::sync::Arc;

use crate::adapters::cluster::hinted_handoff::HintedHandoff;
use crate::adapters::cluster::peer_client::PeerClient;
use crate::adapters::cluster::raft::{HyperbytedbRaft, TypeConfig};
use crate::adapters::cluster::replication_log::ReplicationLog;
use crate::adapters::cluster::sync_client::SyncClient;
use crate::config::ClusterConfig;
use crate::domain::cluster::membership::{
    ClusterMembership, NodeInfo, NodeState, SharedMembership,
};
use crate::ports::metadata::MetadataPort;
use crate::ports::points_sink::PointsSinkPort;
use crate::ports::wal::WalPort;

pub struct ClusterBootstrap {
    pub peer_client: Arc<PeerClient>,
    pub membership: SharedMembership,
    pub replication_log: Arc<ReplicationLog>,
    pub hinted_handoff: Arc<HintedHandoff>,
    pub peer_addrs: Vec<String>,
}

impl ClusterBootstrap {
    pub fn init(config: &ClusterConfig, max_hints_per_peer: u64) -> anyhow::Result<Self> {
        std::fs::create_dir_all(&config.replication_log_dir)?;
        let repl_log = Arc::new(
            ReplicationLog::open(&config.replication_log_dir)
                .map_err(|e| anyhow::anyhow!("failed to open replication log: {e}"))?,
        );

        let now = chrono::Utc::now().timestamp();
        let mut cluster_membership = ClusterMembership::new();
        cluster_membership.add_node(NodeInfo {
            node_id: config.node_id,
            addr: config.cluster_addr.clone(),
            state: NodeState::Active,
            joined_at: now,
            last_heartbeat: now,
            needs_sync: false,
        });

        let shared_membership = crate::domain::cluster::membership::new_shared(cluster_membership);

        let peer_addrs: Vec<String> = config
            .peer_list()
            .into_iter()
            .filter(|p| p != &config.cluster_addr)
            .collect();

        if !peer_addrs.is_empty() {
            tracing::warn!(
                "cluster.peers is set; this is deprecated. Prefer driving \
                 membership via the /cluster/membership/add-node API \
                 (the Kubernetes operator does this automatically)."
            );
        }

        tracing::info!(
            node_id = config.node_id,
            cluster_addr = %config.cluster_addr,
            peer_addrs = ?peer_addrs,
            "initializing cluster (raft-managed membership)"
        );

        let hh = Arc::new(
            HintedHandoff::new(repl_log.db().clone(), max_hints_per_peer)
                .map_err(|e| anyhow::anyhow!("failed to create hinted handoff store: {e}"))?,
        );

        let pc = Arc::new(
            PeerClient::new(
                config.node_id,
                config.cluster_addr.clone(),
                shared_membership.clone(),
                repl_log.clone(),
                config.replication_max_retries,
                config.replication_queue_depth,
                config.replication_max_inflight_batches,
                config.replication_max_coalesce_body_bytes,
            )
            .with_hinted_handoff(hh.clone()),
        );

        Ok(Self {
            peer_client: pc,
            membership: shared_membership,
            replication_log: repl_log,
            hinted_handoff: hh,
            peer_addrs,
        })
    }

    pub async fn run_startup_sync(
        &self,
        config: &ClusterConfig,
        metadata: &Arc<dyn MetadataPort>,
        wal: &Arc<dyn WalPort>,
        points_sink: Option<Arc<dyn PointsSinkPort>>,
        max_points_per_request: usize,
    ) -> anyhow::Result<()> {
        {
            let mut m = self.membership.write().await;
            m.set_state(config.node_id, NodeState::Syncing);
        }
        metrics::gauge!("hyperbytedb_cluster_node_state").set(3.0);

        tracing::info!("startup phase: syncing with cluster before accepting traffic");

        let sync_client = SyncClient::with_points_sink(
            config.node_id,
            config.cluster_addr.clone(),
            self.membership.clone(),
            metadata.clone(),
            wal.clone(),
            points_sink,
            max_points_per_request,
            self.peer_addrs.clone(),
        );

        let has_data = metadata
            .list_databases()
            .await
            .map(|dbs| !dbs.is_empty())
            .unwrap_or(false);
        let wal_seq = wal.last_sequence().await.unwrap_or(0);

        const MAX_RETRIES: u32 = 5;
        let is_new_node = !has_data && wal_seq == 0;

        // When the cluster is operator-driven, this node may start up with no
        // knowledge of any peers — neither in static config nor in shared
        // membership (Raft hasn't propagated membership yet because we have
        // not been added as a learner). In that case there's nothing to sync
        // from. Mark Active+needs_sync so /health passes (allowing the
        // operator to discover us and add us as a Raft learner) and rely on
        // the leader's replication monitor to trigger a re-sync once we are
        // visible in the membership.
        let known_peer_count = {
            let m = self.membership.read().await;
            m.all_peers(config.node_id).len()
        };
        if self.peer_addrs.is_empty() && known_peer_count == 0 {
            let mut m = self.membership.write().await;
            m.set_state(config.node_id, NodeState::Active);
            // Only flag for re-sync if we have nothing to lose — bootstrap
            // node (id 1) starts empty by design and is the source of truth.
            if is_new_node && config.node_id != 1 {
                m.set_needs_sync(config.node_id, true);
            }
            metrics::gauge!("hyperbytedb_cluster_node_state").set(1.0);
            tracing::info!(
                node_id = config.node_id,
                "no peers known yet (operator-driven cluster); marking node Active and waiting to be added by leader"
            );
            return Ok(());
        }

        if is_new_node {
            tracing::info!("new node detected, attempting initial sync from cluster");
        } else {
            tracing::info!(
                wal_seq = wal_seq,
                "existing data detected, attempting reconnect sync"
            );
        }

        let mut sync_succeeded = false;
        for attempt in 1..=MAX_RETRIES {
            let result = if is_new_node {
                sync_client.join_and_sync().await
            } else {
                sync_client.reconnect_sync().await
            };

            match result {
                Ok(true) => {
                    tracing::info!(attempt = attempt, "sync completed successfully");
                    sync_succeeded = true;
                    break;
                }
                Ok(false) => {
                    tracing::debug!(
                        attempt = attempt,
                        "no sync needed (first node or already current)"
                    );
                    sync_succeeded = true;
                    break;
                }
                Err(e) => {
                    if attempt == MAX_RETRIES {
                        tracing::error!(
                            error = %e,
                            attempts = MAX_RETRIES,
                            "startup sync failed after all retries, node may have incomplete data"
                        );
                        metrics::counter!("hyperbytedb_startup_sync_failures_total").increment(1);
                    } else {
                        let backoff = std::time::Duration::from_secs(2u64.pow(attempt));
                        tracing::warn!(
                            error = %e,
                            attempt = attempt,
                            max_retries = MAX_RETRIES,
                            backoff_secs = backoff.as_secs(),
                            "startup sync attempt failed, retrying"
                        );
                        tokio::time::sleep(backoff).await;
                    }
                }
            }
        }

        {
            let mut m = self.membership.write().await;
            if sync_succeeded {
                m.set_state(config.node_id, NodeState::Active);
                metrics::gauge!("hyperbytedb_cluster_node_state").set(1.0);
                tracing::info!("startup phase: sync complete, node is Active");
            } else {
                // Transition to Active but flag for leader-driven re-sync.
                // Staying in Syncing forever would block readiness and
                // prevent the leader from reaching us to trigger a re-sync.
                m.set_state(config.node_id, NodeState::Active);
                m.set_needs_sync(config.node_id, true);
                metrics::gauge!("hyperbytedb_cluster_node_state").set(1.0);
                tracing::warn!(
                    "startup phase: sync failed, node is Active but flagged for re-sync"
                );
            }
        }

        Ok(())
    }

    pub async fn start_raft(
        &self,
        config: &ClusterConfig,
        metadata: Arc<dyn MetadataPort>,
        mv_service: Arc<crate::application::materialized_view_service::MaterializedViewService>,
    ) -> Option<HyperbytedbRaft> {
        use crate::adapters::cluster::raft::log_store::RaftStore;
        use crate::adapters::cluster::raft::network::Network;
        use openraft::Config as RaftConfig;
        use openraft::storage::Adaptor;

        let raft_store = match RaftStore::open(&config.raft_dir, self.membership.clone()) {
            Ok(store) => store.with_metadata(metadata).with_mv_service(mv_service),
            Err(e) => {
                tracing::error!(error = %e, "failed to open raft store");
                return None;
            }
        };
        // Push the persisted Raft membership into the data-plane SharedMembership
        // BEFORE handing the store to openraft. Without this, a restarted leader's
        // log is fully applied so no Membership entries replay during catch-up
        // and the SharedMembership stays empty, silently turning every /write
        // into a no-op for replication fan-out.
        raft_store
            .hydrate_shared_membership_from_state_machine()
            .await;
        let (log_store, state_machine) = Adaptor::<TypeConfig, RaftStore>::new(raft_store);

        let network = Network::new();

        let raft_config = match (RaftConfig {
            heartbeat_interval: config.raft_heartbeat_interval_ms.unwrap_or(300),
            election_timeout_min: config.raft_election_timeout_ms.unwrap_or(1000),
            election_timeout_max: config.raft_election_timeout_ms.unwrap_or(1000) * 2,
            snapshot_policy: openraft::SnapshotPolicy::LogsSinceLast(
                config.raft_snapshot_threshold.unwrap_or(1000) as u64,
            ),
            ..Default::default()
        })
        .validate()
        {
            Ok(c) => Arc::new(c),
            Err(e) => {
                tracing::error!(error = %e, "invalid raft config; cluster will not start");
                return None;
            }
        };

        match HyperbytedbRaft::new(
            config.node_id,
            raft_config,
            network,
            log_store,
            state_machine,
        )
        .await
        {
            Ok(raft) => {
                tracing::info!(
                    node_id = config.node_id,
                    "raft consensus engine initialized"
                );

                if config.node_id == 1 {
                    use std::collections::BTreeMap;
                    let mut members = BTreeMap::new();
                    members.insert(
                        config.node_id,
                        openraft::BasicNode::new(config.cluster_addr.clone()),
                    );
                    if let Err(e) = raft.initialize(members).await {
                        tracing::debug!(error = %e, "raft already initialized (expected on restart)");
                    }
                }

                Some(raft)
            }
            Err(e) => {
                tracing::error!(error = %e, "failed to initialize raft");
                None
            }
        }
    }
}
