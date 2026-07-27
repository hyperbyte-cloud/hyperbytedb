use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use metrics::counter;
use tokio::sync::watch;

use crate::adapters::cluster::peer_client::PeerClient;
use crate::adapters::cluster::raft::HyperbytedbRaft;
use crate::adapters::sharding::rocksdb_shard_map::RocksDbShardMap;
use crate::application::shard_transfer::run_region_transfer;
use crate::config::ShardingConfig;
use crate::domain::cluster::membership::SharedMembership;
use crate::domain::sharding::{
    MeasurementKey, ShardEpoch, ShardMapOp, ShardRegion,
};
use crate::error::HyperbytedbError;
use crate::ports::metadata::MetadataPort;
use crate::ports::points_sink::PointsSinkPort;
use crate::ports::sharding::ShardMapPort;
use crate::ports::wal::WalPort;

pub struct ShardScheduler {
    shard_map: Arc<RocksDbShardMap>,
    membership: SharedMembership,
    raft: HyperbytedbRaft,
    peer_client: Option<Arc<PeerClient>>,
    metadata: Arc<dyn MetadataPort>,
    wal: Arc<dyn WalPort>,
    points_sink: Option<Arc<dyn PointsSinkPort>>,
    node_id: u64,
    config: ShardingConfig,
    max_points_per_request: usize,
    heartbeats: tokio::sync::RwLock<Vec<(u64, u64, u64, u64, u64)>>, // region, node, series, bytes, qps
    in_flight_ops: tokio::sync::RwLock<usize>,
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
            points_sink,
            node_id,
            config,
            max_points_per_request,
            heartbeats: tokio::sync::RwLock::new(Vec::new()),
            in_flight_ops: tokio::sync::RwLock::new(0),
        }
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
    }

    fn is_leader(&self) -> bool {
        let metrics = self.raft.metrics().borrow().clone();
        metrics.current_leader == Some(self.node_id)
    }

    pub async fn run(
        &self,
        interval: Duration,
        mut shutdown_rx: watch::Receiver<bool>,
    ) {
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

    async fn acquire_op_slot(&self) -> bool {
        let limit = self.config.schedule_limit.max(1);
        let mut in_flight = self.in_flight_ops.write().await;
        if *in_flight >= limit {
            return false;
        }
        *in_flight += 1;
        true
    }

    async fn release_op_slot(&self) {
        let mut in_flight = self.in_flight_ops.write().await;
        *in_flight = in_flight.saturating_sub(1);
    }

    async fn tick(&self) -> Result<(), HyperbytedbError> {
        let map = self.shard_map.snapshot().await?;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let hb = self.heartbeats.read().await.clone();

        for (_key, space) in &map.spaces {
            for (idx, region) in space.regions.iter().enumerate() {
                let stats = hb
                    .iter()
                    .find(|(r, n, _, _, _)| *r == region.region_id && region.peers.contains(n));
                let series_count = stats.map(|(_, _, c, _, _)| *c).unwrap_or(0);
                let write_qps = stats.map(|(_, _, _, _, q)| *q).unwrap_or(0);

                if !self.acquire_op_slot().await {
                    continue;
                }

                if series_count > self.config.region_max_series {
                    let key = space.key.clone();
                    let region = region.clone();
                    if self.try_split(&key, &region, series_count).await.is_ok() {
                        self.release_op_slot().await;
                        continue;
                    }
                } else if series_count > self.config.region_split_series
                    && now.saturating_sub(region.last_split_at) >= self.config.split_merge_interval_secs
                {
                    let key = space.key.clone();
                    let region = region.clone();
                    if self.try_split(&key, &region, series_count).await.is_ok() {
                        self.release_op_slot().await;
                        continue;
                    }
                }

                if idx + 1 < space.regions.len() {
                    let right = &space.regions[idx + 1];
                    let right_stats = hb
                        .iter()
                        .find(|(r, n, _, _, _)| *r == right.region_id && right.peers.contains(n));
                    let right_series = right_stats.map(|(_, _, c, _, _)| *c).unwrap_or(0);
                    if series_count < self.config.region_merge_series
                        && right_series < self.config.region_merge_series
                        && now.saturating_sub(region.last_split_at)
                            >= self.config.split_merge_interval_secs
                        && now.saturating_sub(right.last_split_at)
                            >= self.config.split_merge_interval_secs
                    {
                        let key = space.key.clone();
                        if self
                            .try_merge(&key, region, right.clone())
                            .await
                            .is_ok()
                        {
                            self.release_op_slot().await;
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
                        self.release_op_slot().await;
                        continue;
                    }
                }

                if let Err(e) = self.try_rebalance(&space.key, region, &hb).await {
                    tracing::debug!(error = %e, region_id = region.region_id, "rebalance skipped");
                }

                self.release_op_slot().await;
            }
        }
        Ok(())
    }

    async fn try_split(
        &self,
        key: &MeasurementKey,
        region: &ShardRegion,
        _series_count: u64,
    ) -> Result<(), HyperbytedbError> {
        if region.end.saturating_sub(region.start) <= 1 {
            return Ok(());
        }
        let split_key = region.start + (region.end - region.start) / 2;
        let mut left = region.clone();
        left.end = split_key;
        left.epoch = left.epoch.bump_version();
        left.last_split_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let mut right = region.clone();
        right.region_id = region.region_id + map_next_region_id(&self.shard_map).await?;
        right.start = split_key;
        right.epoch = ShardEpoch::default().bump_version();
        right.last_split_at = left.last_split_at;
        if let Some(new_primary) = pick_alternate_primary(&self.membership, region, self.node_id).await
        {
            right.primary = new_primary;
        }

        let op = ShardMapOp::Split {
            key: key.clone(),
            region_id: region.region_id,
            split_key,
            left: left.clone(),
            right: right.clone(),
        };
        self.propose(op).await?;
        counter!("hyperbytedb_shard_splits_total").increment(1);

        if let Some(pc) = self.peer_client.as_ref()
            && region.primary == self.node_id
            && right.primary != self.node_id
        {
            let _ = run_region_transfer(
                pc,
                &self.metadata,
                &self.wal,
                self.points_sink.as_ref(),
                self.node_id,
                key,
                &right,
                right.primary,
                self.max_points_per_request,
            )
            .await;
        }
        Ok(())
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
            epoch: region.epoch.clone(),
        };
        self.propose(op).await?;
        counter!("hyperbytedb_shard_rebalances_total").increment(1);
        Ok(())
    }

    async fn propose(&self, op: ShardMapOp) -> Result<(), HyperbytedbError> {
        use crate::adapters::cluster::raft::types::ClusterRequest;
        let req = ClusterRequest::ShardMapMutation(Box::new(op));
        self.raft
            .client_write(req)
            .await
            .map_err(|e| HyperbytedbError::ShardMap(e.to_string()))?;
        Ok(())
    }
}

async fn map_next_region_id(shard_map: &RocksDbShardMap) -> Result<u64, HyperbytedbError> {
    let map = shard_map.snapshot().await?;
    let max = map
        .spaces
        .values()
        .flat_map(|s| s.regions.iter().map(|r| r.region_id))
        .max()
        .unwrap_or(0);
    Ok(max + 1)
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
        .find(|id| *id != region.primary && *id != self_id && m.get_node(*id).is_some())
        .or_else(|| {
            region
                .peers
                .iter()
                .copied()
                .find(|id| *id != region.primary && m.get_node(*id).is_some())
        })
}
