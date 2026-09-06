use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use metrics::counter;
use tokio::sync::watch;

use crate::adapters::cluster::peer_client::PeerClient;
use crate::adapters::cluster::raft::HyperbytedbRaft;
use crate::adapters::sharding::rocksdb_shard_map::RocksDbShardMap;
use crate::application::shard_peer_resolution::is_active_peer;
use crate::application::shard_routing::effective_replication_factor;
use crate::application::shard_transfer::{
    complete_region_transfer, push_region_transfer_data, run_region_transfer,
    stage_region_transfer_data,
};
use crate::config::ShardingConfig;
use crate::domain::cluster::membership::{NodeState, SharedMembership};
use crate::domain::sharding::{
    MeasurementKey, ShardMap, ShardMapOp, ShardRegion, ShardRehomeRequest,
};
use crate::error::HyperbytedbError;
use crate::ports::metadata::MetadataPort;
use crate::ports::points_sink::PointsSinkPort;
use crate::ports::query::QueryPort;
use crate::ports::sharding::ShardMapPort;
use crate::ports::wal::WalPort;

type RegionHeartbeatRow = (u64, u64, u64, u64, u64, u64);

/// Entries older than this multiple of the report interval are ignored for
/// scheduling decisions — otherwise stats from permanently-dead nodes would
/// drive splits/rebalances forever.
const HEARTBEAT_TTL_INTERVALS: u64 = 3;

/// Warn once a region has gone this many consecutive ticks with candidates
/// available but none caught up to the committed map.
const PLACEMENT_STALL_WARN_TICKS: u32 = 5;

/// Split only after the region's cooldown has elapsed. `last_split_at == 0`
/// means "never split" (bootstrap must stamp wall-clock time); treating 0 as
/// "long ago" let a single hot region binary-split every tick (1→2→4→8).
fn region_ready_to_split(
    series_count: u64,
    last_split_at: u64,
    now: u64,
    split_series: u64,
    max_series: u64,
    cooldown_secs: u64,
) -> bool {
    if last_split_at == 0 {
        return false;
    }
    if now.saturating_sub(last_split_at) < cooldown_secs {
        return false;
    }
    series_count > max_series || series_count > split_series
}

fn max_region_peer_heartbeat_stats(hb: &[RegionHeartbeatRow], region: &ShardRegion) -> (u64, u64) {
    hb.iter()
        .filter(|(region_id, node_id, _, _, _, _)| {
            *region_id == region.region_id && region.peers.contains(node_id)
        })
        .fold(
            (0u64, 0u64),
            |acc, (_, _, series_count, _, write_qps, _)| {
                (acc.0.max(*series_count), acc.1.max(*write_qps))
            },
        )
}

/// One outstanding post-commit split-transfer movement awaiting verified
/// re-push. Ordering detail only — durable intent lives in the shard map's
/// `transfer_verified` flag, so leadership change or process restart rebuilds
/// the queue from a map scan.
#[derive(Debug, Clone)]
pub(crate) struct PendingTransfer {
    pub key: MeasurementKey,
    pub region_id: u64,
    pub start: u64,
    pub end: u64,
    /// Source that provably held the rows at split time (the old primary).
    /// Candidate order at drain time is: current primary (authoritative
    /// owner), then this node, then remaining live peers.
    pub recorded_source: u64,
    pub resolve_at_drain: bool,
    pub ticks_without_progress: u32,
    /// Parked entries stop consuming retries; the map flag stays false so
    /// the debt remains visible and re-enqueued after restart.
    pub parked: bool,
    pub first_seen: i64,
}

/// Ticks without applied-count progress before a stuck entry escalates.
const RECONCILE_PARK_AFTER_TICKS: u32 = 10;
/// Minimum debt age (persisted via `transfer_first_seen`) before parking.
const RECONCILE_MIN_AGE_SECS: i64 = 300;

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
    /// Post-commit split-transfer reconciliation queue. Idempotent per
    /// (key, region_id); rebuilt from the map scan on leadership change.
    reconciliation_queue: std::sync::Mutex<Vec<PendingTransfer>>,
    /// Materialized-view rollup destinations (SummingMergeTree spaces),
    /// refreshed from metadata each drain. Re-delivery there would
    /// permanently corrupt additive aggregates, so those spaces are never
    /// enqueued — their primary changes are provisioned by the MV backfill
    /// path.
    rollup_dests: tokio::sync::RwLock<HashMap<MeasurementKey, ()>>,
    /// Peer `map_version` probes for the tick in progress, keyed by node id,
    /// cleared when each tick starts. `/internal/shard/map` serializes every
    /// space and region in the cluster to answer one `u64`, so probing per
    /// region cost O(regions x map size) bytes on every tick of a join.
    peer_map_versions: tokio::sync::RwLock<HashMap<u64, u64>>,
    /// Consecutive ticks a region skipped placement with no caught-up
    /// candidate, keyed by region id. A cluster-wide placement stall is
    /// otherwise only visible at `debug!`.
    placement_stalls: tokio::sync::RwLock<HashMap<u64, u32>>,
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
            reconciliation_queue: std::sync::Mutex::new(Vec::new()),
            rollup_dests: tokio::sync::RwLock::new(HashMap::new()),
            peer_map_versions: tokio::sync::RwLock::new(HashMap::new()),
            placement_stalls: tokio::sync::RwLock::new(HashMap::new()),
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

    /// `peer`'s committed `map_version`, probed at most once per tick.
    async fn cached_peer_map_version(
        &self,
        peer: u64,
        addr: &str,
        client: &reqwest::Client,
    ) -> Result<u64, HyperbytedbError> {
        if let Some(v) = self.peer_map_versions.read().await.get(&peer) {
            return Ok(*v);
        }
        let version = fetch_peer_map_version(client, addr).await?;
        self.peer_map_versions.write().await.insert(peer, version);
        Ok(version)
    }

    /// Record that `region_id` could not place a replica this tick, and warn
    /// once the stall has persisted. A placement blocked forever by a lagging
    /// candidate is otherwise silent above `debug!`.
    async fn note_placement_stall(&self, region_id: u64, candidates: usize) {
        let mut stalls = self.placement_stalls.write().await;
        let count = stalls.entry(region_id).or_insert(0);
        *count = count.saturating_add(1);
        if *count == PLACEMENT_STALL_WARN_TICKS {
            counter!("hyperbytedb_shard_placement_stalled_total").increment(1);
            tracing::warn!(
                region_id,
                candidates,
                ticks = *count,
                "region has no caught-up replica candidate; placement stalled"
            );
        }
    }

    async fn clear_placement_stall(&self, region_id: u64) {
        self.placement_stalls.write().await.remove(&region_id);
    }

    /// True when `key` is a rollup (SummingMergeTree) destination.
    ///
    /// `apply_transfer_push` has no idempotence guard, so a staged region that
    /// fails to commit its map op is staged again on the next tick. A
    /// `ReplacingMergeTree` measurement collapses the redelivery; an additive
    /// rollup destination sums it twice and is corrupted permanently. Contended
    /// lock counts as "yes" — skipping a placement costs a tick, guessing wrong
    /// costs the data.
    async fn is_rollup_dest(&self, key: &MeasurementKey) -> bool {
        match self.rollup_dests.try_read() {
            Ok(set) => set.contains_key(key),
            Err(_) => true,
        }
    }

    #[cfg(test)]
    async fn seed_rollup_dest(&self, key: &MeasurementKey) {
        self.rollup_dests.write().await.insert(key.clone(), ());
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
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        let mut hb = self.heartbeats.write().await;
        hb.retain(|(r, n, _, _, _, _)| !(*r == region_id && *n == node_id));
        hb.push((
            region_id,
            node_id,
            series_count,
            approx_bytes,
            write_qps,
            now,
        ));
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
        // Peer map versions are only valid for the tick that probed them.
        self.peer_map_versions.write().await.clear();
        let map = self.shard_map.snapshot().await?;
        self.drain_reconciliation(&map).await;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let ttl_secs = self
            .config
            .heartbeat_interval_secs
            .saturating_mul(HEARTBEAT_TTL_INTERVALS)
            .max(1);
        // Expire stale reporters so dead-node stats stop driving decisions.
        let hb: Vec<RegionHeartbeatRow> = self
            .heartbeats
            .read()
            .await
            .iter()
            .filter(|(.., at)| now.saturating_sub(*at) <= ttl_secs)
            .copied()
            .collect();

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
                metrics::gauge!(
                    "hyperbytedb_shard_heartbeat_series_count",
                    "region_id" => region.region_id.to_string(),
                )
                .set(series_count as f64);

                if self.region_has_operator(region.region_id).await {
                    continue;
                }

                if !self.acquire_operator(region.region_id).await {
                    continue;
                }

                let should_split = region_ready_to_split(
                    series_count,
                    region.last_split_at,
                    now,
                    self.config.region_split_series,
                    self.config.region_max_series,
                    self.config.split_merge_interval_secs,
                );

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

                // A placement that commits changes the region's epoch, peers or
                // primary, which makes the `region` borrowed from this tick's
                // snapshot stale. Everything below reads that borrow, and
                // `try_rebalance` would spend a full region copy before its
                // proposal failed the epoch CAS — so yield the region and pick
                // it up fresh on the next tick.
                let mut placed = false;
                for step in ["live", "idle", "primary"] {
                    let outcome = match step {
                        "live" => self.try_place_live_member(&space.key, region).await,
                        "idle" => self.try_place_idle_member(&space.key, region).await,
                        _ => self.try_place_primary(&space.key, region).await,
                    };
                    match outcome {
                        Ok(true) => {
                            placed = true;
                            break;
                        }
                        Ok(false) => {}
                        Err(e) => {
                            tracing::debug!(error = %e, region_id = region.region_id, step, "placement skipped");
                        }
                    }
                }
                if placed {
                    self.release_operator(region.region_id).await;
                    continue;
                }

                if let Err(e) = self.try_rebalance(&space.key, region, &hb).await {
                    tracing::debug!(error = %e, region_id = region.region_id, "rebalance skipped");
                }

                if let Err(e) = self.try_replace_dead_peer(&space.key, region).await {
                    tracing::debug!(error = %e, region_id = region.region_id, "peer heal skipped");
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

        // Data-aware guard: never hand the primary role to a peer whose WAL
        // watermark lags the best known for this region. Moving primary to a
        // data-less node converts a visible availability blip into silent
        // empty reads; blocking keeps reads routed at whoever holds the data.
        if let Some(pc) = self.peer_client.as_ref() {
            let candidate_wm = peer_region_wal_watermark(pc, new_primary, region.region_id).await;
            let mut max_wm = candidate_wm;
            for p in &region.peers {
                let wm = peer_region_wal_watermark(pc, *p, region.region_id).await;
                max_wm = max_wm.max(wm);
            }
            if !failover_watermark_safe(candidate_wm, max_wm) {
                counter!(
                    "hyperbytedb_shard_primary_failover_skipped_total",
                    "reason" => "watermark_lag"
                )
                .increment(1);
                return Ok(());
            }
        }

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
        // Lifecycle guard: never fan outstanding transfer debt out across a
        // re-split (round-3 N2).
        if region.transfer_outstanding() {
            tracing::debug!(
                region_id = region.region_id,
                "shard split deferred: transfer debt outstanding on parent"
            );
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

        if right.primary != region.primary {
            right.transfer_verified = Some(false);
            if right.transfer_first_seen.is_none() {
                right.transfer_first_seen = Some(now_unix());
            }
        }
        if left.primary != region.primary {
            left.transfer_verified = Some(false);
            if left.transfer_first_seen.is_none() {
                left.transfer_first_seen = Some(now_unix());
            }
        }

        // Pre-commit staging (best-effort): when this node holds the historical
        // rows, push the future right-child range to its new primary BEFORE the
        // Split commits, shrinking the empty-read window to commit latency.
        // Rows land via a `stage` transfer that skips ownership checks; the
        // post-commit verified re-push below remains authoritative and cleans
        // up the source copy.
        if region.primary == self.node_id
            && right.primary != self.node_id
            && let Some(pc) = self.peer_client.as_ref()
        {
            match stage_region_transfer_data(
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
                Ok(o) => {
                    counter!("hyperbytedb_shard_stage_total").increment(1);
                    tracing::info!(
                        region_id = region.region_id,
                        staged = o.exported,
                        "split child data staged pre-commit"
                    );
                }
                Err(e) => {
                    tracing::warn!(
                        region_id = region.region_id,
                        error = %e,
                        "split pre-commit staging failed; committing split anyway"
                    );
                }
            }
        }

        let op = ShardMapOp::Split {
            key: key.clone(),
            region_id: region.region_id,
            split_key,
            epoch: region.epoch,
            left: left.clone(),
            right: right.clone(),
        };
        if let Err(e) = self.propose(op).await {
            tracing::warn!(
                region_id = region.region_id,
                error = %e,
                "shard split propose failed"
            );
            return Err(e);
        }
        counter!("hyperbytedb_shard_splits_total").increment(1);

        // Region ids are reallocated at apply time, so the committed right
        // child may differ from the proposed one. Resolve it by range — the
        // transfer payload must carry the committed id/epoch or the
        // destination rejects it. Unresolved lookups enqueue as
        // resolve-at-drain markers; the proposed child (possibly wrong
        // epoch/id) is never baked into a queue entry.
        let committed = self
            .committed_child_for_range(key, right.start, right.end)
            .await;
        let resolve_at_drain = committed.is_none();
        let right = match committed {
            Some(c) => c,
            None => {
                tracing::warn!(
                    region_id = region.region_id,
                    start = right.start,
                    end = right.end,
                    "split commit lookup failed; queueing resolve-at-drain marker"
                );
                right
            }
        };

        // Ownership unchanged (e.g. no active alternate for the child primary):
        // the data already lives on the surviving primary.
        if right.primary == region.primary {
            return Ok(());
        }

        let recorded_source = region.primary;
        if region.primary == self.node_id {
            // This node is the old primary and holds the historical rows.
            let Some(pc) = self.peer_client.as_ref() else {
                return Ok(());
            };
            match push_and_drop_range(
                pc,
                &self.metadata,
                &self.wal,
                self.query_port.as_ref(),
                self.points_sink.as_ref(),
                self.node_id,
                key,
                &right,
                right.primary,
                self.max_points_per_request,
            )
            .await
            {
                Ok(()) => {
                    if let Err(e) = self.propose_clear_verified(key, &right).await {
                        tracing::warn!(
                            region_id = right.region_id,
                            error = %e,
                            "transfer verified but ClearVerified proposal failed"
                        );
                        self.enqueue_reconciliation(PendingTransfer {
                            key: key.clone(),
                            region_id: right.region_id,
                            start: right.start,
                            end: right.end,
                            recorded_source,
                            resolve_at_drain,
                            ticks_without_progress: 0,
                            parked: false,
                            first_seen: right.transfer_first_seen.unwrap_or_else(now_unix),
                        });
                    }
                }
                Err(e) => {
                    counter!("hyperbytedb_shard_transfer_failures_total").increment(1);
                    tracing::warn!(
                        region_id = right.region_id,
                        error = %e,
                        "shard split post-transfer failed; split committed"
                    );
                    self.enqueue_reconciliation(PendingTransfer {
                        key: key.clone(),
                        region_id: right.region_id,
                        start: right.start,
                        end: right.end,
                        recorded_source,
                        resolve_at_drain,
                        ticks_without_progress: 0,
                        parked: false,
                        first_seen: right.transfer_first_seen.unwrap_or_else(now_unix),
                    });
                }
            }
        } else if let Some(pc) = self.peer_client.as_ref() {
            // Leader is neither source nor destination: only the old primary
            // can export historical rows, so ask it to re-home them.
            match request_region_rehome(
                pc,
                &self.membership,
                region.primary,
                key,
                &right,
                right.primary,
                true,
            )
            .await
            {
                Ok(()) => {
                    counter!("hyperbytedb_shard_rehome_total").increment(1);
                    if let Err(e) = self.propose_clear_verified(key, &right).await {
                        tracing::warn!(
                            region_id = right.region_id,
                            error = %e,
                            "rehome verified but ClearVerified proposal failed"
                        );
                        self.enqueue_reconciliation(PendingTransfer {
                            key: key.clone(),
                            region_id: right.region_id,
                            start: right.start,
                            end: right.end,
                            recorded_source,
                            resolve_at_drain,
                            ticks_without_progress: 0,
                            parked: false,
                            first_seen: right.transfer_first_seen.unwrap_or_else(now_unix),
                        });
                    }
                }
                Err(e) => {
                    counter!("hyperbytedb_shard_rehome_failures_total").increment(1);
                    tracing::warn!(
                        region_id = right.region_id,
                        old_primary = region.primary,
                        error = %e,
                        "split rehome RPC failed; split committed"
                    );
                    self.enqueue_reconciliation(PendingTransfer {
                        key: key.clone(),
                        region_id: right.region_id,
                        start: right.start,
                        end: right.end,
                        recorded_source,
                        resolve_at_drain,
                        ticks_without_progress: 0,
                        parked: false,
                        first_seen: right.transfer_first_seen.unwrap_or_else(now_unix),
                    });
                }
            }
        }
        Ok(())
    }

    fn enqueue_reconciliation(&self, entry: PendingTransfer) {
        match self.rollup_dests.try_read() {
            Ok(set) if set.contains_key(&entry.key) => {
                // SummingMergeTree destinations must never receive re-delivered
                // rows (additive aggregates would permanently double-count).
                // Their primary changes are provisioned by the MV backfill path.
                tracing::info!(
                    db = %entry.key.db,
                    rp = %entry.key.rp,
                    measurement = %entry.key.measurement,
                    region_id = entry.region_id,
                    "rollup destination space excluded from transfer reconciliation"
                );
                return;
            }
            Ok(_) => {}
            Err(_) => {
                tracing::warn!(
                    db = %entry.key.db,
                    rp = %entry.key.rp,
                    measurement = %entry.key.measurement,
                    region_id = entry.region_id,
                    "rollup dest set contended; skipping transfer enqueue"
                );
                return;
            }
        }
        let mut q = self
            .reconciliation_queue
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        if let Some(existing) = q
            .iter_mut()
            .find(|e| e.key == entry.key && e.region_id == entry.region_id)
        {
            existing.recorded_source = entry.recorded_source;
            existing.resolve_at_drain = false;
            existing.parked = false;
            return;
        }
        q.push(entry);
    }

    /// Propose clearing a verified split-transfer debt. No-op success when
    /// the region carries no outstanding flag. Gated behind
    /// `transfer_clear_proposals_enabled` for mixed-version rolling upgrades:
    /// old binaries cannot decode the `ClearVerified` op from the Raft log.
    async fn propose_clear_verified(
        &self,
        key: &MeasurementKey,
        region: &ShardRegion,
    ) -> Result<(), HyperbytedbError> {
        if !region.transfer_outstanding() {
            return Ok(());
        }
        if !self.config.transfer_clear_proposals_enabled {
            return Err(HyperbytedbError::ShardMap(
                "ClearVerified proposals disabled (upgrade window)".into(),
            ));
        }
        self.propose(ShardMapOp::ClearVerified {
            key: key.clone(),
            region_id: region.region_id,
            epoch: region.epoch,
        })
        .await
    }

    /// Locate the committed child region matching a proposed `[start, end)`
    /// range in this node's shard map snapshot.
    async fn committed_child_for_range(
        &self,
        key: &MeasurementKey,
        start: u64,
        end: u64,
    ) -> Option<ShardRegion> {
        let map = self.shard_map.snapshot().await.ok()?;
        map.space(&key.db, &key.rp, &key.measurement)?
            .region_with_range(start, end)
            .cloned()
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
        // Lifecycle gate BEFORE staging (round-3 N3): merging a region with
        // outstanding transfer debt would consolidate possibly-incomplete
        // rows and silently destroy the durable-intent guarantee. Staging
        // below pulls from the right child's primary, so the check must come
        // first.
        if left.transfer_outstanding() || right.transfer_outstanding() {
            tracing::debug!(
                left_region_id = left.region_id,
                right_region_id = right.region_id,
                "shard merge deferred: transfer debt outstanding"
            );
            return Ok(());
        }
        let mut merged = left.clone();
        merged.end = right.end;
        merged.epoch = left.epoch.bump_version();
        merged.last_split_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        // Stage the right half onto the merged owner BEFORE committing: after
        // a split, the right child's rows may live solely on its own primary,
        // and merging ownership without moving data strands them (reads go to
        // `merged.primary` only). The transfer must not depend on who the
        // leader is — only on where the data lives.
        if right.primary != merged.primary {
            if right.primary == self.node_id {
                let Some(pc) = self.peer_client.as_ref() else {
                    return Ok(());
                };
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
                    // Merged range keeps a single owner; retain the source copy
                    // as a stale replica rather than dropping before
                    // ownership is committed.
                    false,
                )
                .await?;
            } else if let Some(pc) = self.peer_client.as_ref() {
                // Leader is neither holder nor destination: have the current
                // holder stage its copy onto the future owner. Keep the source
                // copy (drop_source=false) so a rejected Merge proposal cannot
                // strand the data.
                request_region_rehome(
                    pc,
                    &self.membership,
                    right.primary,
                    key,
                    &right,
                    merged.primary,
                    false,
                )
                .await?;
            }
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

    /// Place a live Active member that is not yet a region peer, when
    /// effective RF has room. Stages rows onto the joiner, then commits
    /// `AddPeer` — never commit-then-stage (that's heal `MovePeer`).
    async fn try_place_live_member(
        &self,
        key: &MeasurementKey,
        region: &ShardRegion,
    ) -> Result<bool, HyperbytedbError> {
        if !self.config.add_peer_proposals_enabled || self.is_rollup_dest(key).await {
            return Ok(false);
        }

        // Resolve the peer client before probing the joiner: without one there
        // is no way to stage rows, so the round trip below would be wasted.
        let Some(pc) = self.peer_client.as_ref() else {
            return Ok(false);
        };

        // Re-read the region: `drain_reconciliation` runs before this loop and
        // can bump epochs, so the caller's snapshot may already be stale.
        // Acting on it stages a whole region copy that the epoch CAS then
        // rejects.
        let map = self.shard_map.snapshot().await?;
        let Some(region) = current_region(&map, key, region.region_id) else {
            return Ok(false);
        };

        let candidates: Vec<(u64, String)> = {
            let m = self.membership.read().await;
            let ids: Vec<u64> = m.active_peers(0).into_iter().map(|n| n.node_id).collect();
            live_replica_candidates(region, &ids, self.config.replication_factor)
                .into_iter()
                .filter_map(|id| m.get_node(id).map(|n| (id, n.addr.clone())))
                .collect()
        };
        if candidates.is_empty() {
            self.clear_placement_stall(region.region_id).await;
            return Ok(false);
        }

        // Take the first candidate that has caught up. Stopping at the first
        // candidate outright let one lagging node block every region forever,
        // because the choice was made before the probe and never advanced.
        let cluster_ver = map.map_version;
        let mut chosen = None;
        for (id, addr) in &candidates {
            let ver = match self
                .cached_peer_map_version(*id, addr, pc.http_client())
                .await
            {
                Ok(v) => v,
                Err(e) => {
                    tracing::debug!(error = %e, candidate = id, "map version probe failed");
                    continue;
                }
            };
            if joiner_map_caught_up(cluster_ver, ver) {
                chosen = Some(*id);
                break;
            }
            tracing::debug!(
                region_id = region.region_id,
                candidate = id,
                cluster_ver,
                candidate_ver = ver,
                "candidate map not caught up"
            );
        }
        let Some(joiner) = chosen else {
            self.note_placement_stall(region.region_id, candidates.len())
                .await;
            return Ok(false);
        };
        self.clear_placement_stall(region.region_id).await;

        if region.primary == self.node_id {
            let outcome = stage_region_transfer_data(
                pc,
                &self.metadata,
                &self.wal,
                self.query_port.as_ref(),
                self.node_id,
                key,
                region,
                joiner,
                self.max_points_per_request,
            )
            .await?;
            if !outcome.verified() {
                return Err(HyperbytedbError::ShardMap(
                    format!(
                        "live placement stage unverified: exported={} applied={}",
                        outcome.exported, outcome.applied
                    )
                    .into(),
                ));
            }
        } else {
            request_region_stage(
                pc.as_ref(),
                &self.membership,
                region.primary,
                key,
                region,
                joiner,
            )
            .await?;
        }

        self.propose(ShardMapOp::AddPeer {
            key: key.clone(),
            region_id: region.region_id,
            to_peer: joiner,
            epoch: region.epoch,
        })
        .await?;
        counter!("hyperbytedb_shard_live_placements_total").increment(1);
        Ok(true)
    }

    /// Rebalance one replica slot onto the latest-joined member.
    ///
    /// `try_place_live_member` only fires while a region is below effective RF.
    /// At 3 nodes and RF 3 every region is already complete, so a 4th process
    /// needs an existing peer to step aside instead. Rows are staged onto the
    /// target before `MovePeer` commits — same stage-then-commit ordering, so
    /// no peer is ever published holding nothing. `MovePeer` appends the
    /// target, which makes it the region's newest peer and therefore the next
    /// primary-placement candidate.
    ///
    /// # This is not join-triggered
    ///
    /// The target is the active member with the greatest `joined_at`, which is
    /// whoever joined last — there is no recency window, so "last" may mean
    /// months ago. The rule fires whenever that member is under-loaded relative
    /// to some region's non-primary peer, which means a cluster that has been
    /// stable but unbalanced will start moving replicas on the first tick after
    /// an upgrade, not in response to any join.
    ///
    /// Movement is bounded and convergent — it stops as soon as memberships
    /// even out — and each copy is staged and verified before the map changes,
    /// so this is a scheduling surprise rather than a correctness risk. It is
    /// accepted while sharding is beta and stays deliberately ungated; revisit
    /// before sharding graduates, when an unannounced rebalance on upgrade
    /// stops being acceptable.
    async fn try_place_idle_member(
        &self,
        key: &MeasurementKey,
        region: &ShardRegion,
    ) -> Result<bool, HyperbytedbError> {
        if self.is_rollup_dest(key).await {
            return Ok(false);
        }

        let Some(pc) = self.peer_client.as_ref() else {
            return Ok(false);
        };

        let latest_member = {
            let m = self.membership.read().await;
            m.active_peers(0)
                .into_iter()
                .max_by_key(|n| (n.joined_at, n.node_id))
                .map(|n| (n.node_id, n.addr.clone()))
        };
        let Some((latest_member, addr)) = latest_member else {
            return Ok(false);
        };

        // Re-read the region: an `AddPeer` earlier in this same tick leaves the
        // caller's snapshot stale, and acting on it would stage a full copy
        // only for the proposal to bounce off the epoch CAS.
        let map = self.shard_map.snapshot().await?;
        let Some(region) = current_region(&map, key, region.region_id) else {
            return Ok(false);
        };
        let memberships = region_memberships(&map);
        let Some(displaced) = idle_member_replica_swap(region, latest_member, &memberships) else {
            return Ok(false);
        };

        let joiner_ver = self
            .cached_peer_map_version(latest_member, &addr, pc.http_client())
            .await?;
        if !joiner_map_caught_up(map.map_version, joiner_ver) {
            tracing::debug!(
                region_id = region.region_id,
                latest_member,
                cluster_ver = map.map_version,
                joiner_ver,
                "skip idle placement: rebalance target map not caught up"
            );
            return Ok(false);
        }

        if region.primary == self.node_id {
            let outcome = stage_region_transfer_data(
                pc,
                &self.metadata,
                &self.wal,
                self.query_port.as_ref(),
                self.node_id,
                key,
                region,
                latest_member,
                self.max_points_per_request,
            )
            .await?;
            if !outcome.verified() {
                return Err(HyperbytedbError::ShardMap(
                    format!(
                        "idle placement stage unverified: exported={} applied={}",
                        outcome.exported, outcome.applied
                    )
                    .into(),
                ));
            }
        } else {
            request_region_stage(
                pc.as_ref(),
                &self.membership,
                region.primary,
                key,
                region,
                latest_member,
            )
            .await?;
        }

        tracing::info!(
            region_id = region.region_id,
            displaced,
            latest_member,
            "rebalancing region replica onto latest-joined member"
        );
        self.propose(ShardMapOp::MovePeer {
            key: key.clone(),
            region_id: region.region_id,
            from_peer: displaced,
            to_peer: latest_member,
            epoch: region.epoch,
        })
        .await?;
        counter!("hyperbytedb_shard_idle_placements_total").increment(1);
        Ok(true)
    }

    /// Hand a region's primary to its newest peer so a joiner starts taking
    /// writes instead of serving only as a replica.
    ///
    /// Rows move and are verified *before* `TransferPrimary` commits, the same
    /// ordering drain and rebalance use: an empty primary must never own a
    /// range. `try_rebalance` cannot do this job — it reacts to a 2x byte-load
    /// imbalance between peers, and a joiner that has just been staged holds a
    /// copy of the same rows, so the imbalance it looks for never appears.
    async fn try_place_primary(
        &self,
        key: &MeasurementKey,
        region: &ShardRegion,
    ) -> Result<bool, HyperbytedbError> {
        if self.is_rollup_dest(key).await {
            return Ok(false);
        }

        let Some(pc) = self.peer_client.as_ref() else {
            return Ok(false);
        };

        // Re-read the region: a peer placement earlier in this same tick leaves
        // the caller's snapshot stale, and a stale epoch would fail the
        // `TransferPrimary` proposal only after a full region copy had run.
        let map = self.shard_map.snapshot().await?;
        let Some(region) = current_region(&map, key, region.region_id) else {
            return Ok(false);
        };
        let counts = primary_counts(&map);
        let active: Vec<u64> = {
            let m = self.membership.read().await;
            m.active_peers(0).into_iter().map(|n| n.node_id).collect()
        };
        let Some(new_primary) = primary_placement_candidate(region, &counts, &active) else {
            return Ok(false);
        };

        // Source from whoever holds the authoritative rows. Exporting from the
        // leader when leadership and primary diverge finds nothing, and a
        // 0-exported/0-applied transfer "verifies" vacuously onto an empty node.
        if region.primary == self.node_id {
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
                // Placement keeps the same range and only changes who leads it;
                // keep the source copy as a replica rather than stranding data
                // if the ownership change later fails.
                false,
            )
            .await?;
        } else {
            request_region_rehome(
                pc,
                &self.membership,
                region.primary,
                key,
                region,
                new_primary,
                false,
            )
            .await?;
        }

        tracing::info!(
            region_id = region.region_id,
            from = region.primary,
            to = new_primary,
            "placing region primary on newest peer"
        );
        self.propose(ShardMapOp::TransferPrimary {
            key: key.clone(),
            region_id: region.region_id,
            new_primary,
            epoch: region.epoch,
        })
        .await?;
        counter!("hyperbytedb_shard_primary_placements_total").increment(1);
        Ok(true)
    }

    async fn try_rebalance(
        &self,
        key: &MeasurementKey,
        region: &ShardRegion,
        hb: &[(u64, u64, u64, u64, u64, u64)],
    ) -> Result<(), HyperbytedbError> {
        // Only peers that have actually reported can be scored. A peer with no
        // live heartbeat row is unmeasured, not empty: scoring it as 0 bytes
        // made any freshly added replica look infinitely lighter than the
        // primary and handed it ownership on the strength of missing telemetry.
        let mut loads: Vec<(u64, u64)> = region
            .peers
            .iter()
            .filter_map(|node| {
                hb.iter()
                    .find(|(r, n, _, _, _, _)| *r == region.region_id && *n == *node)
                    .map(|(_, _, _, b, _, _)| (*node, *b))
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
            // The transfer MUST source from the region's current primary — the
            // node that actually holds authoritative rows. Exporting from the
            // leader instead finds zero rows whenever leadership and primary
            // diverge, the 0==0 "verification" passes vacuously, and ownership
            // commits onto a data-less node (silent empty reads).
            if region.primary == self.node_id {
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
                    // Rebalance keeps the same owned range, just a new primary:
                    // retain the stale replica copy on the source instead of
                    // dropping it (dropping strands data if the ownership
                    // change later fails).
                    false,
                )
                .await?;
            } else {
                request_region_rehome(
                    pc,
                    &self.membership,
                    region.primary,
                    key,
                    region,
                    new_primary,
                    false,
                )
                .await?;
            }
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

    /// Replace one permanently-inactive region peer with a healthy member
    /// (replica-set healing). One replacement per region per tick; the epoch
    /// bump from each MovePeer naturally serializes further replacements.
    ///
    /// Data is staged onto the new peer AFTER the MovePeer commits (the
    /// transfer destination requires the receiver to be a committed region
    /// peer). Until staged, the new peer is only consulted if the primary is
    /// unreachable — scatter order tries the primary first — so the brief
    /// staging window does not surface empty reads.
    async fn try_replace_dead_peer(
        &self,
        key: &MeasurementKey,
        region: &ShardRegion,
    ) -> Result<(), HyperbytedbError> {
        if !self.config.peer_heal_enabled {
            return Ok(());
        }
        let Some(pc) = self.peer_client.as_ref() else {
            return Ok(());
        };

        let dead_peer = {
            let m = self.membership.read().await;
            region
                .peers
                .iter()
                .copied()
                .find(|id| *id != region.primary && !is_active_peer(&m, *id))
        };
        let Some(dead_peer) = dead_peer else {
            return Ok(());
        };

        // Prefer the active member carrying the fewest region memberships.
        let map = self.shard_map.snapshot().await?;
        let mut load: HashMap<u64, usize> = HashMap::new();
        for space in map.spaces.values() {
            for r in &space.regions {
                for p in &r.peers {
                    *load.entry(*p).or_insert(0) += 1;
                }
            }
        }
        let replacement = {
            let m = self.membership.read().await;
            let mut candidates: Vec<u64> = m
                .active_peers(0)
                .into_iter()
                .map(|n| n.node_id)
                .filter(|id| !region.peers.contains(id))
                .collect();
            drop(m);
            candidates.sort_by_key(|id| (load.get(id).copied().unwrap_or(0), *id));
            candidates.into_iter().next()
        };
        let Some(replacement) = replacement else {
            counter!(
                "hyperbytedb_shard_peer_heal_skipped_total",
                "reason" => "no_replacement"
            )
            .increment(1);
            return Ok(());
        };

        tracing::info!(
            region_id = region.region_id,
            dead_peer,
            replacement,
            "proposing shard peer replacement"
        );
        self.propose(ShardMapOp::MovePeer {
            key: key.clone(),
            region_id: region.region_id,
            from_peer: dead_peer,
            to_peer: replacement,
            epoch: region.epoch,
        })
        .await?;
        counter!("hyperbytedb_shard_peer_heals_total").increment(1);

        // Stage data onto the new peer from whoever holds authoritative rows.
        let fresh = self
            .committed_child_for_range(key, region.start, region.end)
            .await
            .unwrap_or_else(|| region.clone());
        if fresh.primary == self.node_id {
            run_region_transfer(
                pc,
                &self.metadata,
                &self.wal,
                self.query_port.as_ref(),
                self.points_sink.as_ref(),
                self.node_id,
                key,
                &fresh,
                replacement,
                self.max_points_per_request,
                // The new peer is an added replica; keep every other copy.
                false,
            )
            .await?;
        } else {
            request_region_rehome(
                pc,
                &self.membership,
                fresh.primary,
                key,
                &fresh,
                replacement,
                false,
            )
            .await?;
        }
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
            .map_err(|e| HyperbytedbError::ShardMap(e.to_string().into()))?;
        Ok(())
    }
}

/// Re-home timeout: transfers are bulk data movement, not scatter queries.
const REHOME_TIMEOUT_SECS: u64 = 300;

/// Data-aware failover guard: the candidate must hold a caught-up copy.
///
/// `max_peer_watermark` is the highest WAL watermark observed across all
/// region peers (unreachable peers report 0, so it reflects living nodes).
/// Blocking failover when every known watermark is behind keeps reads routed
/// at the node holding the data instead of silently returning empty results.
fn failover_watermark_safe(candidate_watermark: u64, max_peer_watermark: u64) -> bool {
    candidate_watermark > 0 || max_peer_watermark == 0
}

/// Region data movement onto a joiner starts only after it has caught up to
/// the cluster's committed `map_version`. Staging rows against a lagging map
/// would apply under the wrong epoch / peer set.
///
/// Ahead is fine, and has to be: the cluster version is read from a snapshot
/// taken a moment before the probe, so a joiner that applied an op in between
/// legitimately reports a higher number. Requiring equality rejected that node
/// and — because the candidate was chosen before the probe and never advanced
/// — stalled replica growth for every region indefinitely.
#[must_use]
pub fn joiner_map_caught_up(cluster_map_version: u64, joiner_map_version: u64) -> bool {
    joiner_map_version >= cluster_map_version
}

/// Members eligible to become a replica of `region`, lowest id first.
///
/// Returns every candidate rather than just the best one so the caller can
/// advance past a node that fails its catch-up probe. Returning a single
/// candidate meant one permanently-lagging node was re-chosen every tick and
/// blocked replica growth cluster-wide.
#[must_use]
pub fn live_replica_candidates(
    region: &ShardRegion,
    active_member_ids: &[u64],
    configured_rf: usize,
) -> Vec<u64> {
    if region.transfer_outstanding() {
        return Vec::new();
    }
    let target = effective_replication_factor(configured_rf, active_member_ids.len());
    if region.peers.len() >= target {
        return Vec::new();
    }
    let mut candidates: Vec<u64> = active_member_ids
        .iter()
        .copied()
        .filter(|id| !region.peers.contains(id))
        .collect();
    candidates.sort_unstable();
    candidates
}

/// Next live member to add as a replica, or `None` when RF is full, the
/// region carries transfer debt, or every Active member is already a peer.
#[must_use]
pub fn live_replica_candidate(
    region: &ShardRegion,
    active_member_ids: &[u64],
    configured_rf: usize,
) -> Option<u64> {
    if region.transfer_outstanding() {
        return None;
    }
    let target = effective_replication_factor(configured_rf, active_member_ids.len());
    if region.peers.len() >= target {
        return None;
    }
    active_member_ids
        .iter()
        .copied()
        .filter(|id| !region.peers.contains(id))
        .min()
}

/// Count the regions each node is primary for, across every space in the map.
#[must_use]
pub fn primary_counts(map: &ShardMap) -> HashMap<u64, usize> {
    let mut counts: HashMap<u64, usize> = HashMap::new();
    for space in map.spaces.values() {
        for region in &space.regions {
            *counts.entry(region.primary).or_insert(0) += 1;
        }
    }
    counts
}

/// The committed state of `region_id` in `map`, or `None` if it is gone (split,
/// merged, or its measurement dropped since the caller's snapshot).
#[must_use]
pub fn current_region<'a>(
    map: &'a ShardMap,
    key: &MeasurementKey,
    region_id: u64,
) -> Option<&'a ShardRegion> {
    map.spaces
        .get(key)?
        .regions
        .iter()
        .find(|r| r.region_id == region_id)
}

/// Count the regions each node is a peer of, across every space in the map.
#[must_use]
pub fn region_memberships(map: &ShardMap) -> HashMap<u64, usize> {
    let mut counts: HashMap<u64, usize> = HashMap::new();
    for space in map.spaces.values() {
        for region in &space.regions {
            for peer in &region.peers {
                *counts.entry(*peer).or_insert(0) += 1;
            }
        }
    }
    counts
}

/// Non-primary peer that should yield its replica slot to `latest_member`, or
/// `None` when this region is already well placed.
///
/// An RF-complete region never triggers `AddPeer`, so a member that is not
/// already a peer would own nothing — every region legitimately has its full
/// replica count, and pure load balancing has no reason to disturb them. This
/// rule breaks that tie in one direction only: `latest_member` may displace a
/// peer, and only one carrying strictly more region memberships than it.
///
/// `latest_member` must be the active member with the greatest `joined_at`.
/// The caller establishes that, and it is what makes the swap converge: the
/// identity is stable until membership itself changes, so the node just
/// displaced cannot reclaim the slot — it is not the latest member. Passing an
/// arbitrary node, or the least-loaded one, forfeits that and lets a slot trade
/// back and forth indefinitely.
///
/// Note this is *ordering*, not recency: the greatest `joined_at` is simply
/// whichever member joined last, whether that was seconds or months ago. See
/// [`ShardScheduler::try_place_idle_member`] for what that means in practice.
#[must_use]
pub fn idle_member_replica_swap(
    region: &ShardRegion,
    latest_member: u64,
    memberships: &HashMap<u64, usize>,
) -> Option<u64> {
    if region.transfer_outstanding() || region.peers.contains(&latest_member) {
        return None;
    }
    let latest_member_load = memberships.get(&latest_member).copied().unwrap_or(0);
    region
        .peers
        .iter()
        .copied()
        .filter(|p| *p != region.primary)
        .map(|p| (memberships.get(&p).copied().unwrap_or(0), p))
        .filter(|(load, _)| *load > latest_member_load)
        .max()
        .map(|(_, peer)| peer)
}

/// Node that should take this region's primary so a newly added replica starts
/// serving writes, or `None` when the primary is already well placed.
///
/// The candidate is the region's newest peer — `AddPeer` appends, and region
/// sorting only orders by range, so `peers.last()` is the most recently placed
/// replica. It takes the primary only while it carries strictly fewer primaries
/// than the current one, which is what stops a join from stampeding every
/// region's primary onto the joiner and what makes the rule terminate: once the
/// newest peer *is* the primary the rule no longer applies, so there is no
/// oscillation back to the previous owner.
#[must_use]
pub fn primary_placement_candidate(
    region: &ShardRegion,
    primary_counts: &HashMap<u64, usize>,
    active_member_ids: &[u64],
) -> Option<u64> {
    if region.transfer_outstanding() {
        return None;
    }
    let newest = *region.peers.last()?;
    if newest == region.primary || !active_member_ids.contains(&newest) {
        return None;
    }
    let current = primary_counts.get(&region.primary).copied().unwrap_or(0);
    let candidate = primary_counts.get(&newest).copied().unwrap_or(0);
    (candidate < current).then_some(newest)
}

/// Read a peer's committed `map_version` via `/internal/shard/map`.
pub async fn fetch_peer_map_version(
    client: &reqwest::Client,
    peer_addr: &str,
) -> Result<u64, HyperbytedbError> {
    let map =
        crate::adapters::cluster::sync_client::fetch_shard_map(client.clone(), peer_addr).await?;
    Ok(map.map_version)
}

#[allow(clippy::too_many_arguments)]
async fn push_and_drop_range(
    peer_client: &Arc<PeerClient>,
    metadata: &Arc<dyn MetadataPort>,
    wal: &Arc<dyn WalPort>,
    query_port: Option<&Arc<dyn QueryPort>>,
    sink: Option<&Arc<dyn PointsSinkPort>>,
    source_node_id: u64,
    key: &MeasurementKey,
    range: &ShardRegion,
    dest_primary: u64,
    max_points: usize,
) -> Result<(), HyperbytedbError> {
    // push_region_transfer_data fails unless the destination confirmed every
    // exported point; complete_region_transfer then acks and drops the source
    // copy. The range is owned by another region after a split, so dropping is
    // safe here — and only here.
    let outcome = push_region_transfer_data(
        peer_client,
        metadata,
        wal,
        query_port,
        source_node_id,
        key,
        range,
        dest_primary,
        max_points,
    )
    .await?;
    if outcome.transfer_id == 0 {
        // transfer_id == 0 ⇔ source == destination: ownership moved between
        // resolution and execution. This is a collision signal for the
        // reconciliation queue — never silent success, and it must not
        // authorize dropping the range from the node that owns it.
        return Err(HyperbytedbError::TransferCollision);
    }
    complete_region_transfer(
        peer_client,
        metadata,
        sink,
        source_node_id,
        key,
        range,
        dest_primary,
        outcome.transfer_id,
        true,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn request_region_rehome(
    peer_client: &PeerClient,
    membership: &SharedMembership,
    target_node: u64,
    key: &MeasurementKey,
    range: &ShardRegion,
    dest_primary: u64,
    drop_source: bool,
) -> Result<(), HyperbytedbError> {
    let addr = {
        let m = membership.read().await;
        m.get_node(target_node).map(|n| n.addr.clone())
    }
    .ok_or_else(|| {
        HyperbytedbError::PeerUnreachable(format!("rehome target node {target_node} unknown"))
    })?;
    let req = ShardRehomeRequest {
        db: key.db.clone(),
        rp: key.rp.clone(),
        measurement: key.measurement.clone(),
        start: range.start,
        end: range.end,
        epoch: range.epoch,
        dest_primary,
        drop_source,
        stage: false,
    };
    let url = format!("http://{addr}/internal/shard/rehome");
    let resp = peer_client
        .http_client()
        .post(&url)
        .json(&req)
        .timeout(Duration::from_secs(REHOME_TIMEOUT_SECS))
        .send()
        .await
        .map_err(|e| HyperbytedbError::PeerUnreachable(e.to_string()))?;
    if !resp.status().is_success() {
        return Err(HyperbytedbError::TransferRejected {
            status: resp.status().as_u16(),
        });
    }
    Ok(())
}

/// Ask the current primary to stage `[start, end)` onto a joiner that is
/// not yet a committed peer.
async fn request_region_stage(
    peer_client: &PeerClient,
    membership: &SharedMembership,
    target_node: u64,
    key: &MeasurementKey,
    range: &ShardRegion,
    dest: u64,
) -> Result<(), HyperbytedbError> {
    let addr = {
        let m = membership.read().await;
        m.get_node(target_node).map(|n| n.addr.clone())
    }
    .ok_or_else(|| {
        HyperbytedbError::PeerUnreachable(format!("stage target node {target_node} unknown"))
    })?;
    let req = ShardRehomeRequest {
        db: key.db.clone(),
        rp: key.rp.clone(),
        measurement: key.measurement.clone(),
        start: range.start,
        end: range.end,
        epoch: range.epoch,
        dest_primary: dest,
        drop_source: false,
        stage: true,
    };
    let url = format!("http://{addr}/internal/shard/rehome");
    let resp = peer_client
        .http_client()
        .post(&url)
        .json(&req)
        .timeout(Duration::from_secs(REHOME_TIMEOUT_SECS))
        .send()
        .await
        .map_err(|e| HyperbytedbError::PeerUnreachable(e.to_string()))?;
    if !resp.status().is_success() {
        return Err(HyperbytedbError::TransferRejected {
            status: resp.status().as_u16(),
        });
    }
    Ok(())
}

/// Reconciliation disposition for a failed post-commit transfer movement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TransferDisposition {
    /// Transient (transport, apply lag 404/409, 5xx): retry next drain.
    Retryable,
    /// Source/destination identity collapsed or moved (400 dest-is-self,
    /// collision): re-resolve roles and retry.
    Reresolve,
    /// Nothing to retry (logic error): park with alarm.
    Park,
}

pub(crate) fn classify_transfer_error(err: &HyperbytedbError) -> TransferDisposition {
    match err {
        HyperbytedbError::PeerUnreachable(_) | HyperbytedbError::ReplicationTimeout(_) => {
            TransferDisposition::Retryable
        }
        HyperbytedbError::TransferRejected { status } => match status {
            400 => TransferDisposition::Reresolve,
            404 | 409 => TransferDisposition::Retryable,
            s if *s >= 500 => TransferDisposition::Retryable,
            _ => TransferDisposition::Park,
        },
        HyperbytedbError::TransferCollision => TransferDisposition::Reresolve,
        // Map-state errors are typically transient snapshots; bounded by the
        // progress-based parking below.
        HyperbytedbError::ShardMap(_) => TransferDisposition::Retryable,
        _ => TransferDisposition::Park,
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
    let mut total_region_watermarks = 0usize;
    let mut region_wm: Option<u64> = None;
    for db in &manifest.databases {
        for meas in &db.measurements {
            for rw in &meas.region_watermarks {
                total_region_watermarks += 1;
                if rw.region_id == region_id {
                    region_wm = Some(rw.wal_watermark);
                }
            }
        }
    }
    match region_wm {
        Some(wm) => wm,
        // No watermark for this region: fall back to the node-wide sequence
        // ONLY when the peer reports no region watermarks at all (fresh or
        // legacy node). Otherwise absence means this node holds no data for
        // the region — reporting its global seq would wrongly bless a
        // data-less candidate as caught-up during failover.
        None if total_region_watermarks == 0 => manifest.wal_last_seq,
        None => 0,
    }
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

fn now_unix() -> i64 {
    chrono::Utc::now().timestamp()
}

impl ShardScheduler {
    /// Rebuild reconciliation entries from the committed map scan: any
    /// flagged child without an in-memory entry gets one. Makes leader change
    /// and process restart equivalent — durable intent lives in the map.
    fn rebuild_queue_from_map(&self, map: &super::super::domain::sharding::types::ShardMap) {
        let dests = match self.rollup_dests.try_read() {
            Ok(guard) => guard,
            Err(_) => {
                tracing::warn!("rollup dest set contended; skipping transfer queue rebuild");
                return;
            }
        };
        let mut q = self
            .reconciliation_queue
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        for space in map.spaces.values() {
            if dests.contains_key(&space.key) {
                continue;
            }
            for region in &space.regions {
                if !region.transfer_outstanding() {
                    continue;
                }
                let exists = q
                    .iter()
                    .any(|e| e.key == space.key && e.region_id == region.region_id);
                if exists {
                    continue;
                }
                // The old primary is unknown post-restart; the current primary
                // is both destination and best initial source guess.
                q.push(PendingTransfer {
                    key: space.key.clone(),
                    region_id: region.region_id,
                    start: region.start,
                    end: region.end,
                    recorded_source: region.primary,
                    resolve_at_drain: false,
                    ticks_without_progress: 0,
                    parked: false,
                    first_seen: region.transfer_first_seen.unwrap_or_else(now_unix),
                });
            }
        }
    }

    /// Drain due split-transfer reconciliation entries. Called once per
    /// leader tick, before scheduling heuristics.
    async fn drain_reconciliation(&self, map: &super::super::domain::sharding::types::ShardMap) {
        // Refresh rollup-destination knowledge for the exclusion check.
        match self.metadata.list_all_materialized_views().await {
            Ok(defs) => {
                let mut set = self.rollup_dests.write().await;
                set.clear();
                for def in defs {
                    set.insert(
                        MeasurementKey::new(def.dest_db, def.dest_rp, def.dest_measurement),
                        (),
                    );
                }
            }
            Err(e) => {
                tracing::debug!(error = %e, "rollup-dest refresh failed; using cached set");
            }
        }

        self.rebuild_queue_from_map(map);

        let pending = self.pending_transfers();
        metrics::gauge!("hyperbytedb_shard_transfer_queue_pending")
            .set(pending.iter().filter(|e| !e.parked).count() as f64);
        let now_ts = now_unix();
        let oldest_age = pending
            .iter()
            .map(|e| now_ts - e.first_seen)
            .max()
            .unwrap_or(0);
        metrics::gauge!("hyperbytedb_shard_transfer_queue_oldest_age_secs")
            .set(oldest_age.max(0) as f64);

        for entry in pending {
            let Some(space) = map.space(&entry.key.db, &entry.key.rp, &entry.key.measurement)
            else {
                self.remove_reconciliation(&entry.key, entry.region_id);
                continue;
            };
            let region = space
                .regions
                .iter()
                .find(|r| {
                    r.region_id == entry.region_id
                        || (entry.resolve_at_drain && r.start == entry.start && r.end == entry.end)
                })
                .cloned();
            let Some(region) = region else {
                // Region vanished (merged away / manual surgery): debt gone.
                self.remove_reconciliation(&entry.key, entry.region_id);
                continue;
            };
            if !region.transfer_outstanding() {
                // Cleared elsewhere.
                self.remove_reconciliation(&entry.key, region.region_id);
                continue;
            }
            if entry.parked || self.region_has_operator(region.region_id).await {
                continue;
            }
            if !self.acquire_operator(region.region_id).await {
                continue;
            }

            let outcome = self
                .attempt_reconciliation(&entry.key, &region, &entry)
                .await;
            self.release_operator(region.region_id).await;

            match outcome {
                ReconcileOutcome::Cleared => {
                    self.remove_reconciliation(&entry.key, region.region_id);
                }
                ReconcileOutcome::Stalled => {
                    let mut q = self
                        .reconciliation_queue
                        .lock()
                        .unwrap_or_else(|p| p.into_inner());
                    if let Some(e) = q
                        .iter_mut()
                        .find(|e| e.key == entry.key && e.region_id == region.region_id)
                    {
                        e.ticks_without_progress += 1;
                        let age = now_ts - e.first_seen;
                        if e.ticks_without_progress >= RECONCILE_PARK_AFTER_TICKS
                            && age >= RECONCILE_MIN_AGE_SECS
                        {
                            e.parked = true;
                            counter!("hyperbytedb_shard_transfer_parked_total").increment(1);
                            tracing::warn!(
                                db = %entry.key.db,
                                measurement = %entry.key.measurement,
                                region_id = region.region_id,
                                age_secs = age,
                                "split-transfer reconciliation parked; operator action required"
                            );
                        }
                    }
                }
                ReconcileOutcome::Escalate => {
                    match self.escalate_stuck_transfer(&entry.key, &region).await {
                        Ok(()) => {
                            let mut q = self
                                .reconciliation_queue
                                .lock()
                                .unwrap_or_else(|p| p.into_inner());
                            if let Some(e) = q
                                .iter_mut()
                                .find(|e| e.key == entry.key && e.region_id == region.region_id)
                            {
                                // Ownership moved via TransferPrimary; give the
                                // new topology a fresh retry budget.
                                e.ticks_without_progress = 0;
                            }
                        }
                        Err(err) => {
                            tracing::warn!(
                                region_id = region.region_id,
                                error = %err,
                                "reconciliation escalation failed"
                            );
                            let mut q = self
                                .reconciliation_queue
                                .lock()
                                .unwrap_or_else(|p| p.into_inner());
                            if let Some(e) = q
                                .iter_mut()
                                .find(|e| e.key == entry.key && e.region_id == region.region_id)
                            {
                                e.parked = true;
                                counter!("hyperbytedb_shard_transfer_parked_total").increment(1);
                            }
                        }
                    }
                }
            }
        }
    }

    /// One reconciliation attempt against the current committed region state.
    async fn attempt_reconciliation(
        &self,
        key: &MeasurementKey,
        region: &ShardRegion,
        entry: &PendingTransfer,
    ) -> ReconcileOutcome {
        let dest = region.primary;
        let Some(pc) = self.peer_client.as_ref() else {
            return ReconcileOutcome::Stalled;
        };

        // Authoritative-source order: the node that provably held the rows at
        // split time first, then remaining live peers with region coverage
        // (watermark > 0). The destination itself is never a source.
        let m = self.membership.read().await;
        let mut sources: Vec<u64> = vec![entry.recorded_source];
        sources.extend(region.peers.iter().copied());
        sources.retain(|id| {
            *id != dest
                && (*id == self.node_id
                    || m.get_node(*id)
                        .is_some_and(|n| n.state == NodeState::Active))
        });
        drop(m);

        for source in sources {
            // Coverage proof for non-recorded sources: a peer whose region
            // watermark is 0 holds no rows for this region and would verify
            // vacuously (the 0==0 hazard documented on rebalance).
            if source != entry.recorded_source && source != self.node_id {
                let wm = peer_region_wal_watermark(pc, source, region.region_id).await;
                if wm == 0 {
                    continue;
                }
            }

            let result = if source == self.node_id {
                push_and_drop_range(
                    pc,
                    &self.metadata,
                    &self.wal,
                    self.query_port.as_ref(),
                    self.points_sink.as_ref(),
                    self.node_id,
                    key,
                    region,
                    dest,
                    self.max_points_per_request,
                )
                .await
            } else {
                request_region_rehome(pc, &self.membership, source, key, region, dest, true).await
            };

            match result {
                Ok(()) => {
                    counter!("hyperbytedb_shard_transfer_retries_total").increment(1);
                    return match self.propose_clear_verified(key, region).await {
                        Ok(()) => ReconcileOutcome::Cleared,
                        Err(e) => {
                            tracing::warn!(
                                region_id = region.region_id,
                                error = %e,
                                "verified re-push complete; ClearVerified proposal failed"
                            );
                            // Keep the entry so verification is not lost; do
                            // NOT re-push every tick against a flag we could
                            // not clear.
                            ReconcileOutcome::Stalled
                        }
                    };
                }
                Err(e) => {
                    tracing::debug!(
                        region_id = region.region_id,
                        source_node = source,
                        error = %e,
                        "reconciliation movement attempt failed"
                    );
                    match classify_transfer_error(&e) {
                        TransferDisposition::Reresolve => continue,
                        TransferDisposition::Retryable => {
                            counter!("hyperbytedb_shard_transfer_retries_total").increment(1);
                            return ReconcileOutcome::Stalled;
                        }
                        TransferDisposition::Park => {
                            return ReconcileOutcome::Escalate;
                        }
                    }
                }
            }
        }
        ReconcileOutcome::Stalled
    }

    /// Break a stuck transfer by moving ownership to another live peer via
    /// `TransferPrimary`. The existing transfer-primary path stages rows from
    /// the current authoritative primary before committing ownership, so this
    /// cannot commit a data-less node (mirrors `try_rebalance` ordering).
    async fn escalate_stuck_transfer(
        &self,
        key: &MeasurementKey,
        region: &ShardRegion,
    ) -> Result<(), HyperbytedbError> {
        let Some(new_primary) =
            pick_alternate_primary(&self.membership, region, self.node_id).await
        else {
            return Err(HyperbytedbError::PeerUnreachable(
                "no live alternate peer for reconciliation escalation".into(),
            ));
        };
        tracing::warn!(
            db = %key.db,
            measurement = %key.measurement,
            region_id = region.region_id,
            new_primary,
            "escalating stuck split-transfer via TransferPrimary"
        );
        self.propose(ShardMapOp::TransferPrimary {
            key: key.clone(),
            region_id: region.region_id,
            new_primary,
            epoch: region.epoch,
        })
        .await
    }

    fn remove_reconciliation(&self, key: &MeasurementKey, region_id: u64) {
        let mut q = self
            .reconciliation_queue
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        q.retain(|e| !(e.key == *key && e.region_id == region_id));
    }

    fn pending_transfers(&self) -> Vec<PendingTransfer> {
        self.reconciliation_queue
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone()
    }
}

enum ReconcileOutcome {
    /// Verified and the map flag cleared: drop the entry.
    Cleared,
    /// No progress this tick; bounded retry continues.
    Stalled,
    /// Terminal/repeatedly-stuck: move ownership or park.
    Escalate,
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
    use crate::adapters::sharding::rocksdb_shard_map::RocksDbShardMap;
    use crate::domain::cluster::membership::{ClusterMembership, NodeInfo, new_shared};
    use crate::domain::sharding::ShardEpoch;
    use crate::domain::sharding::ShardLocationCache;

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
            transfer_verified: None,
            transfer_first_seen: None,
        };
        let hb = vec![
            (1, 1, 0, 0, 0, 0),
            (1, 2, 130, 0, 42, 0),
            (2, 3, 999, 0, 0, 0),
        ];
        assert_eq!(max_region_peer_heartbeat_stats(&hb, &region), (130, 42));
    }

    #[test]
    fn unsplit_region_does_not_split_while_last_split_at_is_zero() {
        assert!(!region_ready_to_split(1_000, 0, 1_000_000, 5, 10, 30));
    }

    #[test]
    fn split_requires_cooldown_even_above_max_series() {
        assert!(!region_ready_to_split(1_000, 100, 120, 5, 10, 30));
        assert!(region_ready_to_split(1_000, 100, 130, 5, 10, 30));
    }

    #[test]
    fn split_fires_after_cooldown_when_above_target() {
        assert!(region_ready_to_split(6, 100, 130, 5, 10, 30));
        assert!(!region_ready_to_split(5, 100, 130, 5, 10, 30));
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
            transfer_verified: None,
            transfer_first_seen: None,
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
            transfer_verified: None,
            transfer_first_seen: None,
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
            transfer_verified: None,
            transfer_first_seen: None,
        };

        assert_eq!(pick_alternate_primary(&membership, &region, 99).await, None);
    }

    #[test]
    fn failover_watermark_guard_blocks_dataless_candidates() {
        // Caught-up candidate: safe.
        assert!(failover_watermark_safe(100, 100));
        // Behind but holding data: allowed — async watermarks always lag and
        // this is still the best living copy.
        assert!(failover_watermark_safe(50, 100));
        // Candidate has no data while some peer does: blocked — this is the
        // post-split strand where the new primary never received the rows.
        assert!(!failover_watermark_safe(0, 42));
        // Nobody has data yet (fresh region): allow.
        assert!(failover_watermark_safe(0, 0));
    }

    #[test]
    fn joiner_map_catchup_blocks_a_lagging_joiner() {
        assert!(joiner_map_caught_up(3, 3));
        assert!(!joiner_map_caught_up(3, 0));
        assert!(!joiner_map_caught_up(3, 2));
        // Ahead is allowed. The gate exists to stop staging against a *lagging*
        // map; a joiner that applied an op after the cluster snapshot was read
        // is not lagging, and rejecting it stalled placement permanently.
        assert!(joiner_map_caught_up(3, 4));
    }

    fn sample_region_peers(peers: Vec<u64>, primary: u64) -> ShardRegion {
        ShardRegion {
            region_id: 1,
            start: 0,
            end: u64::MAX,
            epoch: ShardEpoch::default(),
            peers,
            primary,
            last_split_at: 0,
            transfer_verified: None,
            transfer_first_seen: None,
        }
    }

    #[test]
    fn live_replica_candidate_picks_lowest_id_joiner() {
        let region = sample_region_peers(vec![1], 1);
        assert_eq!(live_replica_candidate(&region, &[1, 2], 3), Some(2));
    }

    #[test]
    fn live_replica_candidate_none_when_rf_full() {
        let region = sample_region_peers(vec![1, 2, 3], 1);
        assert_eq!(live_replica_candidate(&region, &[1, 2, 3, 4], 3), None);
    }

    #[test]
    fn live_replica_candidate_none_when_already_peer() {
        let region = sample_region_peers(vec![1, 2], 1);
        assert_eq!(live_replica_candidate(&region, &[1, 2], 3), None);
    }

    #[test]
    fn catch_up_accepts_a_candidate_that_is_ahead() {
        assert!(joiner_map_caught_up(5, 5), "equal is caught up");
        // The cluster version comes from a snapshot taken before the probe, so
        // a candidate that applied an op in between is legitimately ahead.
        // Rejecting it stalled placement permanently.
        assert!(joiner_map_caught_up(5, 6), "ahead is caught up");
        assert!(!joiner_map_caught_up(5, 4), "behind is not");
    }

    #[test]
    fn live_replica_candidates_returns_every_option_in_order() {
        let region = sample_region_peers(vec![1], 1);
        assert_eq!(
            live_replica_candidates(&region, &[1, 3, 2], 3),
            vec![2, 3],
            "all non-peers, lowest first, so a lagging one can be skipped"
        );
    }

    #[test]
    fn live_replica_candidates_empty_when_rf_full_or_indebted() {
        let full = sample_region_peers(vec![1, 2, 3], 1);
        assert!(live_replica_candidates(&full, &[1, 2, 3, 4], 3).is_empty());

        let mut indebted = sample_region_peers(vec![1], 1);
        indebted.transfer_verified = Some(false);
        assert!(live_replica_candidates(&indebted, &[1, 2], 3).is_empty());
    }

    #[test]
    fn live_replica_candidate_none_with_transfer_debt() {
        let mut region = sample_region_peers(vec![1], 1);
        region.transfer_verified = Some(false);
        assert_eq!(live_replica_candidate(&region, &[1, 2], 3), None);
    }

    /// Build a map whose single space holds `regions`, for primary counting.
    fn map_of(regions: Vec<ShardRegion>) -> ShardMap {
        let key = MeasurementKey::new("db", "autogen", "cpu");
        let mut map = ShardMap::default();
        map.spaces.insert(
            key.clone(),
            crate::domain::sharding::MeasurementShardSpace { key, regions },
        );
        map
    }

    #[test]
    fn primary_counts_tallies_every_space() {
        let map = map_of(vec![
            sample_region_peers(vec![1, 2], 1),
            sample_region_peers(vec![1, 2], 2),
            sample_region_peers(vec![1, 2], 1),
        ]);
        let counts = primary_counts(&map);
        assert_eq!(counts.get(&1), Some(&2));
        assert_eq!(counts.get(&2), Some(&1));
    }

    #[test]
    fn primary_placement_hands_only_region_to_the_joiner() {
        let region = sample_region_peers(vec![1, 2], 1);
        let counts = primary_counts(&map_of(vec![region.clone()]));
        assert_eq!(
            primary_placement_candidate(&region, &counts, &[1, 2]),
            Some(2),
            "a joiner with no primaries must take the region so writes land on it"
        );
    }

    /// The rule must terminate: after the joiner takes the primary, evaluating
    /// the same region again must not hand it straight back.
    #[test]
    fn primary_placement_does_not_oscillate() {
        let placed = sample_region_peers(vec![1, 2], 2);
        let counts = primary_counts(&map_of(vec![placed.clone()]));
        assert_eq!(
            primary_placement_candidate(&placed, &counts, &[1, 2]),
            None,
            "newest peer already holds the primary; nothing left to place"
        );
    }

    #[test]
    fn primary_placement_stops_at_an_even_split() {
        // Four regions, primaries already 2/2 across the pair.
        let regions = vec![
            sample_region_peers(vec![1, 2], 1),
            sample_region_peers(vec![1, 2], 1),
            sample_region_peers(vec![1, 2], 2),
            sample_region_peers(vec![1, 2], 2),
        ];
        let counts = primary_counts(&map_of(regions.clone()));
        assert_eq!(
            primary_placement_candidate(&regions[0], &counts, &[1, 2]),
            None,
            "moving another primary would only invert the imbalance"
        );
    }

    #[test]
    fn primary_placement_drains_a_lopsided_owner() {
        let regions = vec![
            sample_region_peers(vec![1, 2], 1),
            sample_region_peers(vec![1, 2], 1),
            sample_region_peers(vec![1, 2], 1),
        ];
        let counts = primary_counts(&map_of(regions.clone()));
        assert_eq!(
            primary_placement_candidate(&regions[0], &counts, &[1, 2]),
            Some(2)
        );
    }

    #[test]
    fn primary_placement_skips_inactive_and_indebted() {
        let region = sample_region_peers(vec![1, 2], 1);
        let counts = primary_counts(&map_of(vec![region.clone()]));
        assert_eq!(
            primary_placement_candidate(&region, &counts, &[1]),
            None,
            "joiner is not an active member"
        );

        let mut indebted = region.clone();
        indebted.transfer_verified = Some(false);
        assert_eq!(
            primary_placement_candidate(&indebted, &counts, &[1, 2]),
            None,
            "no ownership mutation while transfer debt is outstanding"
        );
    }

    #[test]
    fn idle_member_displaces_a_loaded_replica_not_the_primary() {
        let region = sample_region_peers(vec![1, 2, 3], 1);
        let memberships = region_memberships(&map_of(vec![region.clone()]));
        let displaced =
            idle_member_replica_swap(&region, 4, &memberships).expect("latest member takes a slot");
        assert_ne!(displaced, 1, "the primary must never be displaced");
        assert!(displaced == 2 || displaced == 3);
    }

    /// The swap must converge. `latest_member` stays the same node for as long as
    /// membership is unchanged, so once it holds the slot the rule stops firing
    /// and the slot cannot trade back and forth.
    #[test]
    fn idle_member_swap_converges() {
        let before = sample_region_peers(vec![1, 2, 3], 1);
        let displaced = idle_member_replica_swap(
            &before,
            4,
            &region_memberships(&map_of(vec![before.clone()])),
        )
        .expect("first pass moves the latest member in");

        let mut after = before.clone();
        after.peers.retain(|p| *p != displaced);
        after.peers.push(4);
        assert_eq!(
            idle_member_replica_swap(&after, 4, &region_memberships(&map_of(vec![after.clone()]))),
            None,
            "second pass with the same latest member must be a no-op"
        );
    }

    #[test]
    fn idle_member_swap_skips_debt_and_balanced_regions() {
        let mut indebted = sample_region_peers(vec![1, 2, 3], 1);
        indebted.transfer_verified = Some(false);
        let memberships = region_memberships(&map_of(vec![indebted.clone()]));
        assert_eq!(
            idle_member_replica_swap(&indebted, 4, &memberships),
            None,
            "no peer mutation while transfer debt is outstanding"
        );

        // Newcomer already carries as many memberships as every replica here.
        let region = sample_region_peers(vec![1, 2, 3], 1);
        let balanced = region_memberships(&map_of(vec![
            region.clone(),
            sample_region_peers(vec![4, 5, 6], 4),
        ]));
        assert_eq!(idle_member_replica_swap(&region, 4, &balanced), None);
    }

    #[test]
    fn primary_placement_never_moves_off_the_newest_peer() {
        // Node 3 is both newest and primary while node 1 carries none. Evening
        // that out is byte-load rebalance's job, not placement's — placement
        // exists to give a joiner work, and node 3 already has it.
        let region = sample_region_peers(vec![1, 2, 3], 3);
        let counts = primary_counts(&map_of(vec![region.clone()]));
        assert_eq!(
            primary_placement_candidate(&region, &counts, &[1, 2, 3]),
            None
        );
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

        let shard_map = Arc::new(RocksDbShardMap::open(&meta_dir, true).unwrap());
        let location_cache = Arc::new(ShardLocationCache::new());
        let raft = bootstrap
            .start_raft(
                &cluster_cfg,
                metadata.clone(),
                mv_service,
                sink.clone(),
                wal.clone(),
                Some((shard_map.clone(), location_cache.clone())),
                None,
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
            transfer_verified: None,
            transfer_first_seen: None,
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
        let shard_map = Arc::new(RocksDbShardMap::open(&meta_dir, true).unwrap());
        let location_cache = Arc::new(ShardLocationCache::new());
        let raft = bootstrap
            .start_raft(
                &cluster_cfg,
                metadata.clone(),
                mv_service,
                sink.clone(),
                wal.clone(),
                Some((shard_map.clone(), location_cache.clone())),
                None,
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
            last_split_at: 1,
            transfer_verified: None,
            transfer_first_seen: None,
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

    // ── reconciliation queue (S.1–S.7) ──────────────────────────────────

    fn pending_entry(key: &MeasurementKey, region_id: u64, first_seen: i64) -> PendingTransfer {
        PendingTransfer {
            key: key.clone(),
            region_id,
            start: 0,
            end: u64::MAX,
            recorded_source: 1,
            resolve_at_drain: false,
            ticks_without_progress: 0,
            parked: false,
            first_seen,
        }
    }

    #[test]
    fn classify_transfer_error_dispositions() {
        use HyperbytedbError::{PeerUnreachable, ShardMap, TransferCollision, TransferRejected};
        assert_eq!(
            classify_transfer_error(&PeerUnreachable("down".into())),
            TransferDisposition::Retryable
        );
        assert_eq!(
            classify_transfer_error(&TransferRejected { status: 404 }),
            TransferDisposition::Retryable
        );
        assert_eq!(
            classify_transfer_error(&TransferRejected { status: 409 }),
            TransferDisposition::Retryable
        );
        assert_eq!(
            classify_transfer_error(&TransferRejected { status: 503 }),
            TransferDisposition::Retryable
        );
        assert_eq!(
            classify_transfer_error(&TransferRejected { status: 400 }),
            TransferDisposition::Reresolve
        );
        assert_eq!(
            classify_transfer_error(&TransferCollision),
            TransferDisposition::Reresolve
        );
        assert_eq!(
            classify_transfer_error(&TransferRejected { status: 403 }),
            TransferDisposition::Park
        );
        assert_eq!(
            classify_transfer_error(&ShardMap("snapshot".into())),
            TransferDisposition::Retryable
        );
    }

    #[test]
    fn enqueue_is_idempotent_per_key_and_region() {
        // Exercise the queue merge semantics through a manually-built slice:
        // identical (key, region_id) updates in place instead of duplicating.
        let mut q: Vec<PendingTransfer> = Vec::new();
        let key = MeasurementKey::new("db", "autogen", "cpu");
        let e1 = pending_entry(&key, 7, 100);
        let e2 = pending_entry(&key, 7, 200);
        q.push(e1);
        if let Some(existing) = q.iter_mut().find(|e| e.key == key && e.region_id == 7) {
            existing.recorded_source = e2.recorded_source;
            existing.resolve_at_drain = false;
            existing.parked = false;
        } else {
            q.push(e2);
        }
        assert_eq!(q.len(), 1);
        assert_eq!(q[0].recorded_source, 1);
    }

    #[tokio::test]
    #[serial_test::serial(chdb)]
    async fn drain_parks_stuck_debt_after_progress_bound() {
        // Full harness: single-node raft cluster with no peer client, so the
        // movement attempt always stalls — driving the entry to parking.
        let harness = ReconcileTestHarness::new().await;
        let key = MeasurementKey::new("db", "autogen", "cpu");
        let old = now_unix() - RECONCILE_MIN_AGE_SECS - 10;
        let flagged = flagged_test_region(1, old);
        harness
            .shard_map
            .apply_op(ShardMapOp::BootstrapMeasurement {
                key: key.clone(),
                region: flagged,
            })
            .await
            .unwrap();

        for _ in 0..=RECONCILE_PARK_AFTER_TICKS {
            harness.scheduler.tick_once_for_test().await.unwrap();
        }

        let q = harness.scheduler.reconciliation_queue.lock().unwrap();
        assert_eq!(q.len(), 1, "parked debt stays visible in the queue");
        assert!(
            q[0].parked,
            "entry must park after K stalled ticks past min-age"
        );
    }

    #[tokio::test]
    #[serial_test::serial(chdb)]
    async fn rebuild_enqueues_flagged_children_from_map_scan() {
        // Simulates leadership change: fresh scheduler over a map that
        // already carries a flagged child must enqueue it without any prior
        // in-memory state.
        let harness = ReconcileTestHarness::new().await;
        let key = MeasurementKey::new("db", "autogen", "cpu");
        harness
            .shard_map
            .apply_op(ShardMapOp::BootstrapMeasurement {
                key: key.clone(),
                region: flagged_test_region(1, now_unix()),
            })
            .await
            .unwrap();

        harness.scheduler.tick_once_for_test().await.unwrap();

        let q = harness.scheduler.reconciliation_queue.lock().unwrap();
        assert_eq!(q.len(), 1, "map-scan rebuild must enqueue flagged child");
    }

    #[tokio::test]
    #[serial_test::serial(chdb)]
    async fn rollup_destinations_are_excluded_from_reconciliation() {
        let harness = ReconcileTestHarness::new().await;
        let key = MeasurementKey::new("db", "autogen", "rollup_dest");
        harness.scheduler.seed_rollup_dest(&key).await;

        harness
            .scheduler
            .enqueue_reconciliation(pending_entry(&key, 3, now_unix()));

        let q = harness.scheduler.reconciliation_queue.lock().unwrap();
        assert!(q.is_empty(), "SummingMergeTree spaces must never enqueue");
    }

    /// Placement stages rows and `apply_transfer_push` is not idempotent, so a
    /// placement that stages and then fails its proposal re-delivers on the
    /// next tick. `SummingMergeTree` destinations would sum the redelivery.
    #[tokio::test]
    #[serial_test::serial(chdb)]
    async fn placement_refuses_rollup_destinations() {
        let harness = ReconcileTestHarness::new().await;
        let rollup = MeasurementKey::new("db", "autogen", "rollup_dest");
        let raw = MeasurementKey::new("db", "autogen", "cpu");
        harness.scheduler.seed_rollup_dest(&rollup).await;

        assert!(
            harness.scheduler.is_rollup_dest(&rollup).await,
            "seeded rollup destination must be refused by every placement path"
        );
        assert!(
            !harness.scheduler.is_rollup_dest(&raw).await,
            "raw ReplacingMergeTree measurements stay eligible"
        );
    }

    #[tokio::test]
    #[serial_test::serial(chdb)]
    async fn rebuild_skips_flagged_rollup_destinations() {
        let harness = ReconcileTestHarness::new().await;
        let key = MeasurementKey::new("db", "autogen", "rollup_dest");
        harness.scheduler.seed_rollup_dest(&key).await;
        harness
            .shard_map
            .apply_op(ShardMapOp::BootstrapMeasurement {
                key: key.clone(),
                region: flagged_test_region(1, now_unix()),
            })
            .await
            .unwrap();

        let map = harness.shard_map.snapshot().await.unwrap();
        harness.scheduler.rebuild_queue_from_map(&map);

        let q = harness.scheduler.reconciliation_queue.lock().unwrap();
        assert!(
            q.is_empty(),
            "flagged rollup dest must not enter the transfer queue on rebuild"
        );
    }

    #[tokio::test]
    #[serial_test::serial(chdb)]
    async fn split_and_merge_are_refused_while_transfer_outstanding() {
        let harness = ReconcileTestHarness::new().await;
        let key = MeasurementKey::new("db", "autogen", "cpu");
        let flagged = flagged_test_region(1, now_unix());
        harness
            .shard_map
            .apply_op(ShardMapOp::BootstrapMeasurement {
                key: key.clone(),
                region: flagged.clone(),
            })
            .await
            .unwrap();

        let proposals = Arc::new(std::sync::Mutex::new(Vec::<ShardMapOp>::new()));
        *harness.proposals.lock().unwrap() = Vec::new();

        harness
            .scheduler
            .try_split(&key, &flagged, 999_999)
            .await
            .unwrap();
        harness
            .scheduler
            .try_merge(&key, &flagged, flagged_test_region(2, now_unix()))
            .await
            .unwrap();

        let captured = proposals.lock().unwrap();
        assert!(
            captured.is_empty(),
            "no shard ops may propose while transfer debt is outstanding: {captured:?}"
        );
    }

    /// Minimal standalone harness for reconciliation tests: real RocksDB shard
    /// map + scheduler internals without a full raft bootstrap where possible.
    /// Minimal stand-in for a joiner: answers the map-version probe with
    /// `map_version`, and accepts a staging push by echoing the line count it
    /// received. Returns the address it bound to.
    async fn spawn_mock_peer(map_version: u64) -> (String, Arc<std::sync::atomic::AtomicU64>) {
        use axum::routing::{get, post};
        let probes = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let probe_counter = probes.clone();
        let app = axum::Router::new()
            .route(
                "/internal/shard/map",
                get(move || {
                    let probes = probe_counter.clone();
                    async move {
                        probes.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        axum::Json(serde_json::json!({
                            "map_version": map_version,
                            "next_region_id": 2,
                            "spaces": [],
                        }))
                    }
                }),
            )
            .route(
                "/internal/shard/transfer",
                post(|body: axum::body::Bytes| async move {
                    let payload: serde_json::Value =
                        serde_json::from_slice(&body).unwrap_or_default();
                    let applied = payload
                        .get("body")
                        .and_then(|b| b.as_array())
                        .map(|bytes| {
                            let raw: Vec<u8> = bytes
                                .iter()
                                .filter_map(|v| v.as_u64().map(|n| n as u8))
                                .collect();
                            String::from_utf8_lossy(&raw)
                                .lines()
                                .filter(|l| !l.trim().is_empty())
                                .count() as u64
                        })
                        .unwrap_or(0);
                    axum::Json(serde_json::json!({ "applied": applied }))
                }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        (addr, probes)
    }

    struct ReconcileTestHarness {
        _dir: tempfile::TempDir,
        shard_map: Arc<RocksDbShardMap>,
        scheduler: ShardScheduler,
        proposals: Arc<std::sync::Mutex<Vec<ShardMapOp>>>,
    }

    fn flagged_test_region(region_id: u64, first_seen: i64) -> ShardRegion {
        ShardRegion {
            region_id,
            start: 0,
            end: u64::MAX,
            epoch: ShardEpoch::default(),
            peers: vec![1],
            primary: 1,
            last_split_at: 0,
            transfer_verified: Some(false),
            transfer_first_seen: Some(first_seen),
        }
    }

    /// Harness whose scheduler owns the region and can actually reach a peer,
    /// so `tick()` runs the placement steps end to end.
    struct PlacementTestHarness {
        _dir: tempfile::TempDir,
        scheduler: ShardScheduler,
        proposals: Arc<std::sync::Mutex<Vec<ShardMapOp>>>,
        /// `/internal/shard/map` hit counts, keyed by mock peer node id.
        probes: HashMap<u64, Arc<std::sync::atomic::AtomicU64>>,
    }

    impl PlacementTestHarness {
        /// `peers` is `(node_id, map_version_delta)`; a negative delta makes
        /// that peer report a map behind the cluster's. `regions` is how many
        /// measurements to bootstrap, each one region owned by this node.
        async fn new(
            add_peer_proposals_enabled: bool,
            peers: &[(u64, i64)],
            regions: usize,
        ) -> Self {
            use crate::adapters::chdb::native_adapter::ChdbNativeAdapter;
            use crate::adapters::chdb::query_adapter::ChdbQueryAdapter;
            use crate::adapters::chdb::session::SharedSession;
            use crate::adapters::cluster::peer_client::PeerClient;
            use crate::adapters::cluster::replication_log::ReplicationLog;
            use crate::adapters::metadata::rocksdb_meta::RocksDbMetadata;
            use crate::adapters::wal::rocksdb_wal::RocksDbWal;
            use crate::application::cluster::bootstrap::ClusterBootstrap;
            use crate::application::materialized_view_service::MaterializedViewService;
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
            cluster_cfg.cluster_addr = "127.0.0.1:18100".into();
            cluster_cfg.replication_log_dir = dir.path().join("repl").to_string_lossy().into();
            cluster_cfg.raft_dir = dir.path().join("raft").to_string_lossy().into();
            cluster_cfg.raft_heartbeat_interval_ms = Some(200);
            cluster_cfg.raft_election_timeout_ms = Some(500);

            let bootstrap = ClusterBootstrap::init(&cluster_cfg, 1000).unwrap();
            let shard_map = Arc::new(RocksDbShardMap::open(&meta_dir, true).unwrap());
            let location_cache = Arc::new(ShardLocationCache::new());
            let raft = bootstrap
                .start_raft(
                    &cluster_cfg,
                    metadata.clone(),
                    mv_service,
                    sink.clone(),
                    wal.clone(),
                    Some((shard_map.clone(), location_cache)),
                    None,
                )
                .await
                .unwrap();
            tokio::time::sleep(std::time::Duration::from_millis(150)).await;

            // Regions owned by this node, each one peer short of effective RF.
            for i in 0..regions.max(1) {
                let mut region = sample_region_peers(vec![1], 1);
                region.region_id = (i + 1) as u64;
                shard_map
                    .apply_op(ShardMapOp::BootstrapMeasurement {
                        key: MeasurementKey::new("db", "autogen", format!("cpu{i}")),
                        region,
                    })
                    .await
                    .unwrap();
            }
            let map_version = shard_map.snapshot().await.unwrap().map_version;

            let mut probes = HashMap::new();
            {
                let mut m = bootstrap.membership.write().await;
                m.add_node(NodeInfo {
                    node_id: 1,
                    addr: "127.0.0.1:18100".into(),
                    state: NodeState::Active,
                    joined_at: 0,
                    last_heartbeat: 0,
                    needs_sync: false,
                });
                for (id, delta) in peers {
                    let reported = map_version.saturating_add_signed(*delta);
                    let (addr, counter) = spawn_mock_peer(reported).await;
                    probes.insert(*id, counter);
                    m.add_node(NodeInfo {
                        node_id: *id,
                        addr,
                        state: NodeState::Active,
                        joined_at: *id as i64,
                        last_heartbeat: 0,
                        needs_sync: false,
                    });
                }
            }

            let peer_client = Arc::new(PeerClient::new(
                1,
                "127.0.0.1:18100".into(),
                bootstrap.membership.clone(),
                Arc::new(ReplicationLog::open(dir.path().join("repl-peer")).unwrap()),
                2,
                8192,
                8,
                8 * 1024 * 1024,
            ));

            let sharding = crate::config::ShardingConfig {
                replication_factor: 3,
                add_peer_proposals_enabled,
                ..Default::default()
            };
            let proposals = Arc::new(std::sync::Mutex::new(Vec::<ShardMapOp>::new()));
            let scheduler = ShardScheduler::new(
                shard_map,
                bootstrap.membership.clone(),
                raft,
                Some(peer_client),
                metadata,
                wal,
                None,
                Some(sink),
                1,
                sharding,
                10_000,
            )
            .with_test_force_leader(true)
            .with_test_propose_sink(proposals.clone());

            Self {
                _dir: dir,
                scheduler,
                proposals,
                probes,
            }
        }

        fn probe_count(&self, peer: u64) -> u64 {
            self.probes[&peer].load(std::sync::atomic::Ordering::Relaxed)
        }
    }

    /// Guards the tick wiring. Without this, deleting the three placement calls
    /// from `tick()` leaves every other test in the suite green.
    #[tokio::test]
    #[serial_test::serial(chdb)]
    async fn tick_places_a_live_joiner_as_a_region_peer() {
        let harness = PlacementTestHarness::new(true, &[(2, 0)], 1).await;
        harness.scheduler.tick_once_for_test().await.unwrap();
        let ops = harness.proposals.lock().unwrap();
        assert!(
            ops.iter()
                .any(|op| matches!(op, ShardMapOp::AddPeer { to_peer: 2, .. })),
            "tick must propose AddPeer to place the live joiner, got {ops:?}"
        );
    }

    /// H.2: one lagging candidate must not block the others. Node 2 reports a
    /// map behind the cluster and is chosen first by id; placement has to move
    /// on to node 3 instead of stalling here every tick forever.
    #[tokio::test]
    #[serial_test::serial(chdb)]
    async fn placement_skips_a_lagging_candidate_for_a_caught_up_one() {
        let harness = PlacementTestHarness::new(true, &[(2, -1), (3, 0)], 1).await;
        harness.scheduler.tick_once_for_test().await.unwrap();
        let ops = harness.proposals.lock().unwrap();
        assert!(
            ops.iter()
                .any(|op| matches!(op, ShardMapOp::AddPeer { to_peer: 3, .. })),
            "must place onto the caught-up candidate, got {ops:?}"
        );
        assert!(
            !ops.iter()
                .any(|op| matches!(op, ShardMapOp::AddPeer { to_peer: 2, .. })),
            "must not place onto the lagging candidate, got {ops:?}"
        );
    }

    /// H.4: `/internal/shard/map` serializes the whole cluster map to answer
    /// one `u64`. Probing per region made a join cost O(regions x map size)
    /// bytes per tick; one probe per peer per tick is the contract.
    #[tokio::test]
    #[serial_test::serial(chdb)]
    async fn map_version_is_probed_once_per_peer_per_tick() {
        let harness = PlacementTestHarness::new(true, &[(2, -1)], 4).await;
        harness.scheduler.tick_once_for_test().await.unwrap();
        assert_eq!(
            harness.probe_count(2),
            1,
            "four regions must share one probe, not one probe each"
        );
    }

    /// The upgrade gate has to hold at the tick, not just in config: a leader
    /// that proposes `AddPeer` mid-rolling-restart wedges un-upgraded voters.
    #[tokio::test]
    #[serial_test::serial(chdb)]
    async fn tick_withholds_add_peer_while_the_upgrade_gate_is_closed() {
        let harness = PlacementTestHarness::new(false, &[(2, 0)], 1).await;
        harness.scheduler.tick_once_for_test().await.unwrap();
        let ops = harness.proposals.lock().unwrap();
        assert!(
            !ops.iter()
                .any(|op| matches!(op, ShardMapOp::AddPeer { .. })),
            "AddPeer must not be proposed while the gate is closed, got {ops:?}"
        );
    }

    impl ReconcileTestHarness {
        async fn new() -> Self {
            use crate::adapters::chdb::native_adapter::ChdbNativeAdapter;
            use crate::adapters::chdb::query_adapter::ChdbQueryAdapter;
            use crate::adapters::chdb::session::SharedSession;
            use crate::adapters::metadata::rocksdb_meta::RocksDbMetadata;
            use crate::adapters::wal::rocksdb_wal::RocksDbWal;
            use crate::application::cluster::bootstrap::ClusterBootstrap;
            use crate::application::materialized_view_service::MaterializedViewService;
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
            cluster_cfg.cluster_addr = "127.0.0.1:18099".into();
            cluster_cfg.replication_log_dir = dir.path().join("repl").to_string_lossy().into();
            cluster_cfg.raft_dir = dir.path().join("raft").to_string_lossy().into();
            cluster_cfg.raft_heartbeat_interval_ms = Some(200);
            cluster_cfg.raft_election_timeout_ms = Some(500);

            let bootstrap = ClusterBootstrap::init(&cluster_cfg, 1000).unwrap();
            let shard_map = Arc::new(RocksDbShardMap::open(&meta_dir, true).unwrap());
            let location_cache = Arc::new(ShardLocationCache::new());
            let raft = bootstrap
                .start_raft(
                    &cluster_cfg,
                    metadata.clone(),
                    mv_service,
                    sink.clone(),
                    wal.clone(),
                    Some((shard_map.clone(), location_cache)),
                    None,
                )
                .await
                .unwrap();

            tokio::time::sleep(std::time::Duration::from_millis(150)).await;

            let sharding = crate::config::ShardingConfig::default();
            let proposals = Arc::new(std::sync::Mutex::new(Vec::<ShardMapOp>::new()));
            // peer_client intentionally None: attempts stall deterministically.
            let scheduler = ShardScheduler::new(
                shard_map.clone(),
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

            Self {
                _dir: dir,
                shard_map,
                scheduler,
                proposals,
            }
        }
    }
}
