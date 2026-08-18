use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use metrics::counter;
use tokio::sync::watch;

use crate::adapters::cluster::peer_client::PeerClient;
use crate::adapters::cluster::raft::HyperbytedbRaft;
use crate::adapters::sharding::rocksdb_shard_map::RocksDbShardMap;
use crate::application::shard_peer_resolution::is_active_peer;
use crate::application::shard_transfer::{
    complete_region_transfer, push_region_transfer_data, run_region_transfer,
};
use crate::config::ShardingConfig;
use crate::domain::cluster::membership::{NodeState, SharedMembership};
use crate::domain::sharding::{MeasurementKey, ShardMapOp, ShardRegion};
use crate::error::HyperbytedbError;
use crate::ports::metadata::MetadataPort;
use crate::ports::points_sink::PointsSinkPort;
use crate::ports::query::QueryPort;
use crate::ports::sharding::ShardMapPort;
use crate::ports::wal::WalPort;

type RegionHeartbeatRow = (u64, u64, u64, u64, u64);

fn max_region_peer_heartbeat_stats(hb: &[RegionHeartbeatRow], region: &ShardRegion) -> (u64, u64) {
    hb.iter()
        .filter(|(region_id, node_id, ..)| {
            *region_id == region.region_id && region.peers.contains(node_id)
        })
        .fold((0u64, 0u64), |acc, (_, _, series_count, _, write_qps)| {
            (acc.0.max(*series_count), acc.1.max(*write_qps))
        })
}

pub struct ShardScheduler {
    shard_map: Arc<RocksDbShardMap>,
    membership: SharedMembership,
    raft: HyperbytedbRaft,
    peer_client: Option<Arc<PeerClient>>,
    metadata: Arc<dyn MetadataPort>,
    wal: Arc<dyn WalPort>,
    query_port: Option<Arc<dyn QueryPort>>,
    points_sink: Option<Arc<dyn PointsSinkPort>>,
    node_id: u64,
    config: ShardingConfig,
    max_points_per_request: usize,
    heartbeats: tokio::sync::RwLock<Vec<RegionHeartbeatRow>>,
    /// Regions with an in-flight operator (split/merge/rebalance/failover/transfer).
    region_operators: tokio::sync::RwLock<HashMap<u64, ()>>,
    /// (db, rp, measurement, region_id) -> unix secs when primary first seen unhealthy
    unhealthy_primaries: tokio::sync::RwLock<HashMap<(String, String, String, u64), u64>>,
    #[cfg(test)]
    test_force_leader: bool,
    #[cfg(test)]
    test_propose_sink: Option<Arc<std::sync::Mutex<Vec<ShardMapOp>>>>,
}

impl ShardScheduler {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        shard_map: Arc<RocksDbShardMap>,
        membership: SharedMembership,
        raft: HyperbytedbRaft,
        peer_client: Option<Arc<PeerClient>>,
        metadata: Arc<dyn MetadataPort>,
        wal: Arc<dyn WalPort>,
        query_port: Option<Arc<dyn QueryPort>>,
        points_sink: Option<Arc<dyn PointsSinkPort>>,
        node_id: u64,
        config: ShardingConfig,
        max_points_per_request: usize,
    ) -> Self {
        Self {
            shard_map,
            membership,
            raft,
            peer_client,
            metadata,
            wal,
            query_port,
            points_sink,
            node_id,
            config,
            max_points_per_request,
            heartbeats: tokio::sync::RwLock::new(Vec::new()),
            region_operators: tokio::sync::RwLock::new(HashMap::new()),
            unhealthy_primaries: tokio::sync::RwLock::new(HashMap::new()),
            #[cfg(test)]
            test_force_leader: false,
            #[cfg(test)]
            test_propose_sink: None,
        }
    }

    #[cfg(test)]
    pub fn with_test_force_leader(mut self, v: bool) -> Self {
        self.test_force_leader = v;
        self
    }

    #[cfg(test)]
    pub fn with_test_propose_sink(mut self, sink: Arc<std::sync::Mutex<Vec<ShardMapOp>>>) -> Self {
        self.test_propose_sink = Some(sink);
        self
    }

    #[cfg(test)]
    pub async fn tick_once_for_test(&self) -> Result<(), HyperbytedbError> {
        self.tick().await
    }

    #[cfg(test)]
    pub async fn try_failover_for_test(
        &self,
        key: &MeasurementKey,
        region: &ShardRegion,
        now: u64,
    ) -> Result<(), HyperbytedbError> {
        self.try_failover_unhealthy_primary(key, region, now).await
    }

    pub async fn record_heartbeat(
        &self,
        region_id: u64,
        node_id: u64,
        series_count: u64,
        approx_bytes: u64,
        write_qps: u64,
    ) {
        let mut hb = self.heartbeats.write().await;
        hb.retain(|(r, n, _, _, _)| !(*r == region_id && *n == node_id));
        hb.push((region_id, node_id, series_count, approx_bytes, write_qps));
        metrics::gauge!(
            "hyperbytedb_shard_heartbeat_series_count",
            "region_id" => region_id.to_string(),
            "node_id" => node_id.to_string(),
        )
        .set(series_count as f64);
    }

    fn is_leader(&self) -> bool {
        #[cfg(test)]
        if self.test_force_leader {
            return true;
        }
        let metrics = self.raft.metrics().borrow().clone();
        metrics.current_leader == Some(self.node_id)
    }

    pub async fn run(&self, interval: Duration, mut shutdown_rx: watch::Receiver<bool>) {
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    if !self.is_leader() {
                        continue;
                    }
                    if let Err(e) = self.tick().await {
                        tracing::warn!(error = %e, "shard scheduler tick error");
                    }
                }
                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        break;
                    }
                }
            }
        }
    }

    async fn acquire_operator(&self, region_id: u64) -> bool {
        let limit = self.config.schedule_limit.max(1);
        let mut ops = self.region_operators.write().await;
        if ops.len() >= limit || ops.contains_key(&region_id) {
            return false;
        }
        ops.insert(region_id, ());
        true
    }

    async fn release_operator(&self, region_id: u64) {
        self.region_operators.write().await.remove(&region_id);
    }

    async fn region_has_operator(&self, region_id: u64) -> bool {
        self.region_operators.read().await.contains_key(&region_id)
    }

    async fn tick(&self) -> Result<(), HyperbytedbError> {
        let map = self.shard_map.snapshot().await?;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let hb = self.heartbeats.read().await.clone();

        for (key, space) in &map.spaces {
            metrics::gauge!(
                "hyperbytedb_shard_regions",
                "db" => key.db.clone(),
                "rp" => key.rp.clone(),
                "measurement" => key.measurement.clone(),
            )
            .set(space.regions.len() as f64);

            for (idx, region) in space.regions.iter().enumerate() {
                let (series_count, write_qps) = max_region_peer_heartbeat_stats(&hb, region);

                if self.region_has_operator(region.region_id).await {
                    continue;
                }

                if !self.acquire_operator(region.region_id).await {
                    continue;
                }

                let should_split = series_count > self.config.region_max_series
                    || (series_count > self.config.region_split_series
                        && now.saturating_sub(region.last_split_at)
                            >= self.config.split_merge_interval_secs);

                if should_split {
                    let key = space.key.clone();
                    let region = region.clone();
                    if self.try_split(&key, &region, series_count).await.is_ok() {
                        self.release_operator(region.region_id).await;
                        continue;
                    }
                }

                if idx + 1 < space.regions.len() {
                    let right = &space.regions[idx + 1];
                    let (right_series, _) = max_region_peer_heartbeat_stats(&hb, right);
                    if series_count < self.config.region_merge_series
                        && right_series < self.config.region_merge_series
                        && now.saturating_sub(region.last_split_at)
                            >= self.config.split_merge_interval_secs
                        && now.saturating_sub(right.last_split_at)
                            >= self.config.split_merge_interval_secs
                    {
                        let key = space.key.clone();
                        if self.try_merge(&key, region, right.clone()).await.is_ok() {
                            self.release_operator(region.region_id).await;
                            continue;
                        }
                    }
                }

                if self.config.load_split_qps_threshold > 0
                    && write_qps > self.config.load_split_qps_threshold
                    && now.saturating_sub(region.last_split_at)
                        >= self.config.split_merge_interval_secs
                {
                    let key = space.key.clone();
                    let region = region.clone();
                    if self.try_split(&key, &region, series_count).await.is_ok() {
                        self.release_operator(region.region_id).await;
                        continue;
                    }
                }

                if let Err(e) = self.try_rebalance(&space.key, region, &hb).await {
                    tracing::debug!(error = %e, region_id = region.region_id, "rebalance skipped");
                }

                if let Err(e) = self
                    .try_failover_unhealthy_primary(&space.key, region, now)
                    .await
                {
                    tracing::debug!(error = %e, region_id = region.region_id, "failover skipped");
                }

                self.release_operator(region.region_id).await;
            }
        }
        Ok(())
    }

    async fn try_failover_unhealthy_primary(
        &self,
        key: &MeasurementKey,
        region: &ShardRegion,
        now: u64,
    ) -> Result<(), HyperbytedbError> {
        let map_key = (
            key.db.clone(),
            key.rp.clone(),
            key.measurement.clone(),
            region.region_id,
        );

        let membership = self.membership.read().await;
        let primary_unhealthy = !is_active_peer(&membership, region.primary);
        drop(membership);

        if !primary_unhealthy {
            self.unhealthy_primaries.write().await.remove(&map_key);
            return Ok(());
        }

        let first_seen = {
            let mut unhealthy = self.unhealthy_primaries.write().await;
            *unhealthy.entry(map_key.clone()).or_insert(now)
        };

        if now.saturating_sub(first_seen) < self.config.primary_failover_after_secs {
            return Ok(());
        }

        let new_primary = pick_best_primary(
            &self.membership,
            region,
            self.node_id,
            self.peer_client.as_deref(),
        )
        .await;
        let Some(new_primary) = new_primary else {
            counter!(
                "hyperbytedb_shard_primary_failover_skipped_total",
                "reason" => "no_alternate"
            )
            .increment(1);
            return Ok(());
        };

        let op = ShardMapOp::TransferPrimary {
            key: key.clone(),
            region_id: region.region_id,
            new_primary,
            epoch: region.epoch,
        };
        self.propose(op).await?;
        counter!("hyperbytedb_shard_primary_failover_total").increment(1);
        self.unhealthy_primaries.write().await.remove(&map_key);
        Ok(())
    }

    async fn try_split(
        &self,
        key: &MeasurementKey,
        region: &ShardRegion,
        series_count: u64,
    ) -> Result<(), HyperbytedbError> {
        if region.end.saturating_sub(region.start) <= 1 {
            return Ok(());
        }
        let map = self.shard_map.snapshot().await?;
        if let Some(space) = map.space(&key.db, &key.rp, &key.measurement)
            && space.regions.len() >= self.config.max_regions_per_measurement
        {
            tracing::warn!(
                region_id = region.region_id,
                series_count,
                start = region.start,
                end = region.end,
                region_count = space.regions.len(),
                max_regions = self.config.max_regions_per_measurement,
                "shard split refused: region cap reached"
            );
            return Ok(());
        }
        let split_key = self.compute_split_key(key, region).await?;
        tracing::info!(
            region_id = region.region_id,
            series_count,
            start = region.start,
            end = region.end,
            split_key,
            "proposing shard region split"
        );
        let child_epoch = region.epoch.bump_version();
        let mut left = region.clone();
        left.end = split_key;
        left.epoch = child_epoch;
        left.last_split_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let mut right = region.clone();
        right.region_id = map.next_region_id;
        right.start = split_key;
        right.epoch = child_epoch;
        right.last_split_at = left.last_split_at;
        if let Some(new_primary) = pick_best_primary(
            &self.membership,
            region,
            self.node_id,
            self.peer_client.as_deref(),
        )
        .await
        {
            right.primary = new_primary;
        }

        let mut transfer_id = 0u64;
        if let Some(pc) = self.peer_client.as_ref()
            && region.primary == self.node_id
            && right.primary != self.node_id
        {
            transfer_id = match push_region_transfer_data(
                pc,
                &self.metadata,
                &self.wal,
                self.query_port.as_ref(),
                self.node_id,
                key,
                &right,
                right.primary,
                self.max_points_per_request,
            )
            .await
            {
                Ok(id) => id,
                Err(e) => {
                    counter!("hyperbytedb_shard_transfer_failures_total").increment(1);
                    tracing::warn!(
                        region_id = region.region_id,
                        error = %e,
                        "shard split transfer failed; leaving region unsplit"
                    );
                    return Err(e);
                }
            };
        }

        let op = ShardMapOp::Split {
            key: key.clone(),
            region_id: region.region_id,
            split_key,
            epoch: region.epoch,
            left: left.clone(),
            right: right.clone(),
        };
        self.propose(op).await?;
        counter!("hyperbytedb_shard_splits_total").increment(1);

        if transfer_id != 0
            && let Some(pc) = self.peer_client.as_ref()
        {
            complete_region_transfer(
                pc,
                &self.metadata,
                self.points_sink.as_ref(),
                self.node_id,
                key,
                &right,
                right.primary,
                transfer_id,
            )
            .await?;
        }
        Ok(())
    }

    async fn compute_split_key(
        &self,
        key: &MeasurementKey,
        region: &ShardRegion,
    ) -> Result<u64, HyperbytedbError> {
        let fallback = region.start + (region.end - region.start) / 2;
        let split_key = match self
            .metadata
            .median_series_id_in_range(&key.db, &key.rp, &key.measurement, region.start, region.end)
            .await
        {
            Ok(Some(id)) => id,
            _ => fallback,
        };
        Ok(split_key
            .max(region.start.saturating_add(1))
            .min(region.end.saturating_sub(1)))
    }

    async fn try_merge(
        &self,
        key: &MeasurementKey,
        left: &ShardRegion,
        right: ShardRegion,
    ) -> Result<(), HyperbytedbError> {
        if left.end != right.start {
            return Ok(());
        }
        let mut merged = left.clone();
        merged.end = right.end;
        merged.epoch = left.epoch.bump_version();
        merged.last_split_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        if let Some(pc) = self.peer_client.as_ref()
            && right.primary == self.node_id
            && merged.primary != self.node_id
        {
            run_region_transfer(
                pc,
                &self.metadata,
                &self.wal,
                self.query_port.as_ref(),
                self.points_sink.as_ref(),
                self.node_id,
                key,
                &right,
                merged.primary,
                self.max_points_per_request,
            )
            .await?;
        }

        let op = ShardMapOp::Merge {
            key: key.clone(),
            left_region_id: left.region_id,
            right_region_id: right.region_id,
            epoch: left.epoch,
            merged,
        };
        self.propose(op).await?;
        counter!("hyperbytedb_shard_merges_total").increment(1);
        Ok(())
    }

    async fn try_rebalance(
        &self,
        key: &MeasurementKey,
        region: &ShardRegion,
        hb: &[(u64, u64, u64, u64, u64)],
    ) -> Result<(), HyperbytedbError> {
        let mut loads: Vec<(u64, u64)> = region
            .peers
            .iter()
            .map(|node| {
                let bytes = hb
                    .iter()
                    .find(|(r, n, _, _, _)| *r == region.region_id && *n == *node)
                    .map(|(_, _, _, b, _)| *b)
                    .unwrap_or(0);
                (*node, bytes)
            })
            .collect();
        if loads.len() < 2 {
            return Ok(());
        }
        loads.sort_by_key(|(_, b)| *b);
        let (light_node, light_bytes) = loads[0];
        let (heavy_node, heavy_bytes) = loads[loads.len() - 1];
        if heavy_bytes < light_bytes.saturating_mul(2).max(1) {
            return Ok(());
        }
        if heavy_node != region.primary {
            return Ok(());
        }
        let new_primary = light_node;
        if new_primary == region.primary {
            return Ok(());
        }

        if let Some(pc) = self.peer_client.as_ref() {
            run_region_transfer(
                pc,
                &self.metadata,
                &self.wal,
                self.query_port.as_ref(),
                self.points_sink.as_ref(),
                self.node_id,
                key,
                region,
                new_primary,
                self.max_points_per_request,
            )
            .await?;
        }

        let op = ShardMapOp::TransferPrimary {
            key: key.clone(),
            region_id: region.region_id,
            new_primary,
            epoch: region.epoch,
        };
        self.propose(op).await?;
        counter!("hyperbytedb_shard_rebalances_total").increment(1);
        Ok(())
    }

    async fn propose(&self, op: ShardMapOp) -> Result<(), HyperbytedbError> {
        #[cfg(test)]
        if let Some(sink) = &self.test_propose_sink {
            sink.lock().unwrap().push(op);
            return Ok(());
        }
        use crate::adapters::cluster::raft::types::ClusterRequest;
        let req = ClusterRequest::ShardMapMutation(Box::new(op));
        self.raft
            .client_write(req)
            .await
            .map_err(|e| HyperbytedbError::ShardMap(e.to_string()))?;
        Ok(())
    }
}

async fn pick_best_primary(
    membership: &SharedMembership,
    region: &ShardRegion,
    self_id: u64,
    peer_client: Option<&PeerClient>,
) -> Option<u64> {
    let m = membership.read().await;
    let candidates: Vec<u64> = region
        .peers
        .iter()
        .copied()
        .filter(|id| *id != region.primary)
        .filter(|id| {
            *id == self_id
                || m.get_node(*id)
                    .is_some_and(|n| n.state == NodeState::Active)
        })
        .collect();
    drop(m);

    if candidates.is_empty() {
        return None;
    }

    if let Some(pc) = peer_client {
        let mut best = candidates[0];
        let mut best_wm = 0u64;
        for candidate in candidates {
            let wm = peer_region_wal_watermark(pc, candidate, region.region_id).await;
            if wm > best_wm || (wm == best_wm && candidate == self_id) {
                best_wm = wm;
                best = candidate;
            }
        }
        return Some(best);
    }

    pick_alternate_primary(membership, region, self_id).await
}

async fn peer_region_wal_watermark(peer_client: &PeerClient, peer_id: u64, region_id: u64) -> u64 {
    let membership = peer_client.membership().read().await;
    let Some(node) = membership.get_node(peer_id) else {
        return 0;
    };
    let addr = node.addr.clone();
    drop(membership);

    let url = format!("http://{addr}/internal/sync/manifest");
    let Ok(resp) = peer_client.http_client().get(&url).send().await else {
        return 0;
    };
    let Ok(body) = resp.text().await else {
        return 0;
    };
    let Ok(manifest) = serde_json::from_str::<crate::domain::cluster::sync::SyncManifest>(&body)
    else {
        return 0;
    };
    manifest
        .databases
        .iter()
        .flat_map(|db| db.measurements.iter())
        .flat_map(|meas| meas.region_watermarks.iter())
        .find(|rw| rw.region_id == region_id)
        .map(|rw| rw.wal_watermark)
        .unwrap_or(manifest.wal_last_seq)
}

async fn pick_alternate_primary(
    membership: &SharedMembership,
    region: &ShardRegion,
    self_id: u64,
) -> Option<u64> {
    let m = membership.read().await;
    region
        .peers
        .iter()
        .copied()
        .find(|id| peer_is_active_alternate(&m, region, *id, self_id))
}

fn peer_is_active_alternate(
    membership: &crate::domain::cluster::membership::ClusterMembership,
    region: &ShardRegion,
    peer_id: u64,
    self_id: u64,
) -> bool {
    peer_id != region.primary
        && peer_id != self_id
        && membership
            .get_node(peer_id)
            .is_some_and(|n| n.state == NodeState::Active)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::cluster::membership::{ClusterMembership, NodeInfo, new_shared};
    use crate::domain::sharding::ShardEpoch;

    fn node(id: u64, state: NodeState) -> NodeInfo {
        NodeInfo {
            node_id: id,
            addr: format!("127.0.0.1:{id}"),
            state,
            joined_at: 0,
            last_heartbeat: 0,
            needs_sync: false,
        }
    }

    #[test]
    fn max_region_peer_heartbeat_stats_uses_max_across_peers() {
        let region = ShardRegion {
            region_id: 1,
            start: 0,
            end: u64::MAX,
            epoch: ShardEpoch::default(),
            peers: vec![1, 2],
            primary: 1,
            last_split_at: 0,
        };
        let hb = vec![(1, 1, 0, 0, 0), (1, 2, 130, 0, 42), (2, 3, 999, 0, 0)];
        assert_eq!(max_region_peer_heartbeat_stats(&hb, &region), (130, 42));
    }

    #[tokio::test]
    async fn pick_alternate_primary_requires_active_peer() {
        let mut m = ClusterMembership::new();
        m.add_node(node(1, NodeState::Disconnected));
        m.add_node(node(2, NodeState::Active));
        m.add_node(node(3, NodeState::Draining));
        let membership = new_shared(m);

        let region = ShardRegion {
            region_id: 1,
            start: 0,
            end: u64::MAX,
            epoch: ShardEpoch::default(),
            peers: vec![1, 2, 3],
            primary: 1,
            last_split_at: 0,
        };

        assert_eq!(
            pick_alternate_primary(&membership, &region, 99).await,
            Some(2)
        );
    }

    #[tokio::test]
    async fn pick_alternate_primary_none_when_only_primary_in_membership() {
        let mut m = ClusterMembership::new();
        m.add_node(node(2, NodeState::Active));
        let membership = new_shared(m);

        let region = ShardRegion {
            region_id: 1,
            start: 0,
            end: u64::MAX,
            epoch: ShardEpoch::default(),
            peers: vec![1, 2],
            primary: 1,
            last_split_at: 0,
        };

        // Peer 2 is active but self_id excludes it; peer 1 is not in membership.
        assert_eq!(pick_alternate_primary(&membership, &region, 2).await, None);
    }

    #[tokio::test]
    async fn pick_alternate_primary_none_when_no_active_alternate() {
        let mut m = ClusterMembership::new();
        m.add_node(node(1, NodeState::Disconnected));
        m.add_node(node(2, NodeState::Leaving));
        let membership = new_shared(m);

        let region = ShardRegion {
            region_id: 1,
            start: 0,
            end: u64::MAX,
            epoch: ShardEpoch::default(),
            peers: vec![1, 2],
            primary: 1,
            last_split_at: 0,
        };

        assert_eq!(pick_alternate_primary(&membership, &region, 99).await, None);
    }

    #[tokio::test]
    #[serial_test::serial(chdb)]
    async fn try_failover_proposes_transfer_primary() {
        use std::sync::Arc;
        use std::time::Duration;

        use crate::adapters::chdb::native_adapter::ChdbNativeAdapter;
        use crate::adapters::chdb::query_adapter::ChdbQueryAdapter;
        use crate::adapters::chdb::session::SharedSession;
        use crate::adapters::metadata::rocksdb_meta::RocksDbMetadata;
        use crate::adapters::sharding::rocksdb_shard_map::RocksDbShardMap;
        use crate::adapters::wal::rocksdb_wal::RocksDbWal;
        use crate::application::cluster::bootstrap::ClusterBootstrap;
        use crate::application::materialized_view_service::MaterializedViewService;
        use crate::domain::sharding::{MeasurementKey, ShardLocationCache, ShardMapOp};
        use crate::ports::points_sink::PointsSinkPort;

        let dir = tempfile::tempdir().unwrap();
        let meta_dir = dir.path().join("meta");
        let wal_dir = dir.path().join("wal");
        let chdb_dir = dir.path().join("chdb");
        for p in [&meta_dir, &wal_dir, &chdb_dir] {
            std::fs::create_dir_all(p).unwrap();
        }

        let chdb = SharedSession::new_eager(chdb_dir.to_str().unwrap(), 1).unwrap();
        let chdb_adapter = Arc::new(ChdbQueryAdapter::from_shared(chdb.clone(), 0));
        let sink: Arc<dyn PointsSinkPort> = Arc::new(ChdbNativeAdapter::new(chdb));
        let wal = Arc::new(RocksDbWal::open(&wal_dir).unwrap());
        let metadata = Arc::new(RocksDbMetadata::open(&meta_dir).unwrap());
        let mv_service = Arc::new(MaterializedViewService::new(
            metadata.clone(),
            chdb_adapter,
            sink.clone(),
        ));

        let mut cluster_cfg = crate::config::HyperbytedbConfig::load(None)
            .unwrap()
            .cluster;
        cluster_cfg.enabled = true;
        cluster_cfg.node_id = 1;
        cluster_cfg.cluster_addr = "127.0.0.1:18086".into();
        cluster_cfg.replication_log_dir = dir.path().join("repl").to_string_lossy().into();
        cluster_cfg.raft_dir = dir.path().join("raft").to_string_lossy().into();
        cluster_cfg.raft_heartbeat_interval_ms = Some(200);
        cluster_cfg.raft_election_timeout_ms = Some(500);

        let bootstrap = ClusterBootstrap::init(&cluster_cfg, 1000).unwrap();

        let shard_map = Arc::new(RocksDbShardMap::open(&meta_dir, true, 1).unwrap());
        let location_cache = Arc::new(ShardLocationCache::new());
        let raft = bootstrap
            .start_raft(
                &cluster_cfg,
                metadata.clone(),
                mv_service,
                sink.clone(),
                wal.clone(),
                Some((shard_map.clone(), location_cache.clone())),
            )
            .await
            .unwrap();

        {
            let mut m = bootstrap.membership.write().await;
            m.add_node(node(2, NodeState::Active));
        }

        {
            let mut m = bootstrap.membership.write().await;
            m.add_node(node(2, NodeState::Active));
        }

        tokio::time::sleep(Duration::from_millis(200)).await;

        let key = MeasurementKey::new("db", "autogen", "cpu");
        let region = ShardRegion {
            region_id: 1,
            start: 0,
            end: u64::MAX,
            epoch: ShardEpoch::default(),
            peers: vec![1, 2],
            primary: 1,
            last_split_at: 0,
        };
        shard_map
            .apply_op(ShardMapOp::BootstrapMeasurement {
                key: key.clone(),
                region: region.clone(),
            })
            .await
            .unwrap();

        let sharding = ShardingConfig {
            primary_failover_after_secs: 1,
            ..Default::default()
        };

        let proposals = Arc::new(std::sync::Mutex::new(Vec::new()));
        let scheduler = ShardScheduler::new(
            shard_map,
            bootstrap.membership.clone(),
            raft,
            None,
            metadata,
            wal,
            None,
            Some(sink),
            99,
            sharding,
            0,
        )
        .with_test_force_leader(true)
        .with_test_propose_sink(proposals.clone());

        bootstrap
            .membership
            .write()
            .await
            .set_state(1, NodeState::Disconnected);
        {
            let mut m = bootstrap.membership.write().await;
            if m.get_node(2).is_none() {
                m.add_node(node(2, NodeState::Active));
            }
            assert!(m.get_node(2).is_some(), "peer 2 must be in membership");
        }
        assert_eq!(
            pick_alternate_primary(&bootstrap.membership, &region, 99).await,
            Some(2)
        );

        scheduler
            .try_failover_for_test(&key, &region, 100)
            .await
            .unwrap();
        scheduler
            .try_failover_for_test(&key, &region, 102)
            .await
            .unwrap();

        let captured = proposals.lock().unwrap();
        assert!(!captured.is_empty(), "no proposals captured: {captured:?}");
        assert!(
            captured
                .iter()
                .any(|op| matches!(op, ShardMapOp::TransferPrimary { new_primary: 2, .. }))
        );
    }

    #[tokio::test]
    #[serial_test::serial(chdb)]
    async fn tick_proposes_split_when_series_count_exceeds_threshold() {
        use std::sync::Arc;
        use std::time::Duration;

        use crate::adapters::chdb::native_adapter::ChdbNativeAdapter;
        use crate::adapters::chdb::query_adapter::ChdbQueryAdapter;
        use crate::adapters::chdb::session::SharedSession;
        use crate::adapters::metadata::rocksdb_meta::RocksDbMetadata;
        use crate::adapters::sharding::rocksdb_shard_map::RocksDbShardMap;
        use crate::adapters::wal::rocksdb_wal::RocksDbWal;
        use crate::application::cluster::bootstrap::ClusterBootstrap;
        use crate::application::materialized_view_service::MaterializedViewService;
        use crate::domain::sharding::{MeasurementKey, ShardLocationCache, ShardMapOp};
        use crate::ports::points_sink::PointsSinkPort;

        let dir = tempfile::tempdir().unwrap();
        let meta_dir = dir.path().join("meta");
        let wal_dir = dir.path().join("wal");
        let chdb_dir = dir.path().join("chdb");
        for p in [&meta_dir, &wal_dir, &chdb_dir] {
            std::fs::create_dir_all(p).unwrap();
        }

        let chdb = SharedSession::new_eager(chdb_dir.to_str().unwrap(), 1).unwrap();
        let chdb_adapter = Arc::new(ChdbQueryAdapter::from_shared(chdb.clone(), 0));
        let sink: Arc<dyn PointsSinkPort> = Arc::new(ChdbNativeAdapter::new(chdb));
        let wal = Arc::new(RocksDbWal::open(&wal_dir).unwrap());
        let metadata = Arc::new(RocksDbMetadata::open(&meta_dir).unwrap());
        let mv_service = Arc::new(MaterializedViewService::new(
            metadata.clone(),
            chdb_adapter,
            sink.clone(),
        ));

        let mut cluster_cfg = crate::config::HyperbytedbConfig::load(None)
            .unwrap()
            .cluster;
        cluster_cfg.enabled = true;
        cluster_cfg.node_id = 1;
        cluster_cfg.cluster_addr = "127.0.0.1:18087".into();
        cluster_cfg.replication_log_dir = dir.path().join("repl").to_string_lossy().into();
        cluster_cfg.raft_dir = dir.path().join("raft").to_string_lossy().into();
        cluster_cfg.raft_heartbeat_interval_ms = Some(200);
        cluster_cfg.raft_election_timeout_ms = Some(500);

        let bootstrap = ClusterBootstrap::init(&cluster_cfg, 1000).unwrap();
        let shard_map = Arc::new(RocksDbShardMap::open(&meta_dir, true, 1).unwrap());
        let location_cache = Arc::new(ShardLocationCache::new());
        let raft = bootstrap
            .start_raft(
                &cluster_cfg,
                metadata.clone(),
                mv_service,
                sink.clone(),
                wal.clone(),
                Some((shard_map.clone(), location_cache.clone())),
            )
            .await
            .unwrap();

        tokio::time::sleep(Duration::from_millis(200)).await;

        let key = MeasurementKey::new("db", "autogen", "cpu");
        let region = ShardRegion {
            region_id: 1,
            start: 0,
            end: u64::MAX,
            epoch: ShardEpoch::default(),
            peers: vec![1],
            primary: 1,
            last_split_at: 0,
        };
        shard_map
            .apply_op(ShardMapOp::BootstrapMeasurement {
                key: key.clone(),
                region: region.clone(),
            })
            .await
            .unwrap();

        let sharding = ShardingConfig {
            region_split_series: 10,
            region_max_series: 1_000,
            split_merge_interval_secs: 0,
            ..Default::default()
        };

        let proposals = Arc::new(std::sync::Mutex::new(Vec::new()));
        let scheduler = ShardScheduler::new(
            shard_map,
            bootstrap.membership.clone(),
            raft,
            None,
            metadata,
            wal,
            None,
            Some(sink),
            1,
            sharding,
            0,
        )
        .with_test_force_leader(true)
        .with_test_propose_sink(proposals.clone());

        scheduler.record_heartbeat(1, 1, 50, 0, 0).await;
        scheduler.tick_once_for_test().await.unwrap();

        let captured = proposals.lock().unwrap();
        assert!(
            captured
                .iter()
                .any(|op| matches!(op, ShardMapOp::Split { .. })),
            "expected Split proposal when series_count exceeds threshold: {captured:?}"
        );
    }
}
