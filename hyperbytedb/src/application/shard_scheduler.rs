use std::collections::{HashMap, HashSet};
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
    TRANSFER_CHUNK_POINTS, complete_region_transfer, push_region_transfer_data,
    run_region_transfer, stage_region_transfer_data,
};
use crate::config::ShardingConfig;
use crate::domain::cluster::membership::{
    NodeInfo, NodeState, SharedMembership, holds_placement, is_departing, is_serving,
};
use crate::domain::sharding::placement::{
    MoveTier, PlacementStep, RegionPlanInput, estimate_region_weights, move_rate_bytes_per_sec,
    move_tier, next_placement_step, plan_placement, target_placement,
};
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

/// Why a split attempt did nothing. Carried out of `try_split` so the region
/// pass can tell "a split is now proposed, this region's shape is about to
/// change" from "nothing happened here" — conflating the two is what made a
/// capped measurement skip placement, heal and failover forever (G1).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SplitRefusal {
    /// Range is one key wide; there is no split point left.
    RangeTooNarrow,
    /// Parent still owes a transfer; splitting would fan the debt out.
    TransferDebt,
    /// The measurement is already at its region cap.
    RegionCap,
}

impl SplitRefusal {
    fn as_str(self) -> &'static str {
        match self {
            Self::RangeTooNarrow => "range_too_narrow",
            Self::TransferDebt => "transfer_debt",
            Self::RegionCap => "region_cap",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SplitOutcome {
    /// A `Split` op was proposed and committed; the region is now two regions.
    Proposed,
    Refused(SplitRefusal),
}

impl SplitOutcome {
    fn is_proposed(self) -> bool {
        matches!(self, Self::Proposed)
    }
}

#[derive(Clone)]
struct PendingPlacement {
    key: MeasurementKey,
    region_id: u64,
    tier: MoveTier,
    /// Best known size of the region, for the in-flight byte budget.
    approx_bytes: u64,
}

/// Entries older than this multiple of the report interval are ignored for
/// scheduling decisions — otherwise stats from permanently-dead nodes would
/// drive splits/rebalances forever.
const HEARTBEAT_TTL_INTERVALS: u64 = 3;

/// Warn once a region has gone this many consecutive ticks with candidates
/// available but none caught up to the committed map.
const PLACEMENT_STALL_WARN_TICKS: u32 = 5;

/// One shared slot per probe key, so concurrent movers that all want the same
/// peer's answer run the probe once and the rest await that one result.
///
/// A plain "read cache, miss, probe, write cache" is single-flight only while
/// callers are serialised. The movers are not: N of them start together, all
/// miss, and all probe — turning a per-tick cache into a per-mover stampede
/// against the same peer.
type ProbeCell<T> = Arc<tokio::sync::OnceCell<T>>;

/// Per-tick probe cache: peer id -> its reported map version, once fetched.
type PeerMapVersions = tokio::sync::RwLock<HashMap<u64, ProbeCell<Option<(u64, u64)>>>>;

/// Region cap per measurement scales with the candidate count, floored by
/// `max_regions_per_measurement`. Splits are metadata-only, so the cost of a
/// region is a map entry and a heartbeat row, not a copy.
const REGIONS_PER_CANDIDATE: usize = 32;

/// A region keeps its primary while that node stays within this many percent
/// of its fair share of primary load; see `balance_primaries`.
const PRIMARY_BALANCE_TOLERANCE_PCT: u64 = 10;

fn peer_disconnected_past_deadline(node: &NodeInfo, now: i64, timeout_secs: u64) -> bool {
    node.state == NodeState::Disconnected
        && now.saturating_sub(node.last_heartbeat) as u64 >= timeout_secs
}

/// After the first failure, repeat the convergence warning every this many
/// consecutive ticks. Every region failing every tick would otherwise be a
/// log flood; never warning at all was the bug.
const PLACEMENT_ERROR_WARN_EVERY: u32 = 30;

/// Split only after the region's cooldown has elapsed. `last_split_at == 0`
/// means "never split" (bootstrap must stamp wall-clock time); treating 0 as
/// "long ago" let a single hot region binary-split every tick (1→2→4→8).
fn region_ready_to_split(
    series_count: u64,
    last_split_at: u64,
    now: u64,
    split_series: u64,
    max_series: u64,
    split_cooldown_secs: u64,
) -> bool {
    if series_count > max_series {
        return true;
    }
    if last_split_at == 0 {
        return false;
    }
    if now.saturating_sub(last_split_at) < split_cooldown_secs {
        return false;
    }
    series_count > split_series
}

/// Split point for `[start, end)`: a region peer's median, else this node's
/// own median (only offered when it holds the region), else the midpoint.
/// Always strictly inside the range.
fn choose_split_key(
    start: u64,
    end: u64,
    peer_hint: Option<u64>,
    local_median: Option<u64>,
) -> u64 {
    let inside = |k: &u64| *k > start && *k < end;
    peer_hint
        .filter(inside)
        .or(local_median.filter(inside))
        .unwrap_or(start + (end - start) / 2)
        .max(start.saturating_add(1))
        .min(end.saturating_sub(1))
}

/// Most learners a node may be receiving at once for balance alone.
///
/// Balance moves are optional, and each learner costs its destination a full
/// region copy plus every replicated write from the moment it joins. At N=12
/// on n3 one node was sent 17 at once; under write load the copies could not
/// finish, and that node paced the cluster for the whole run. Moves that
/// restore replication or evacuate a node are not capped.
const MAX_BALANCE_LEARNERS_PER_NODE: usize = 2;

/// How many regions `node` is currently a learner of.
fn learners_on(map: &ShardMap, node: u64) -> usize {
    map.spaces
        .values()
        .flat_map(|s| &s.regions)
        .filter(|r| r.staging_peers.contains(&node))
        .count()
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

/// Largest reported size for `region` across its peers, in bytes.
fn max_region_peer_heartbeat_bytes(hb: &[RegionHeartbeatRow], region: &ShardRegion) -> u64 {
    hb.iter()
        .filter(|(region_id, node_id, ..)| {
            *region_id == region.region_id && region.peers.contains(node_id)
        })
        .map(|(_, _, _, approx_bytes, _, _)| *approx_bytes)
        .max()
        .unwrap_or(0)
}

/// One outstanding post-commit split-transfer movement awaiting verified
/// re-push. Ordering detail only — durable intent lives in the shard map's
/// `transfer_verified` flag, so leadership change or process restart rebuilds
/// the queue from a map scan.
#[derive(Debug, Clone)]
/// Dead since splits became metadata-only: the split was the only caller,
/// and nothing creates transfer debt any more. Retained deliberately —
/// removing the reconciliation machinery is a large deletion that deserves
/// its own review, not a side effect of an unrelated change.
#[allow(dead_code)]
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
#[allow(dead_code)]
const RECONCILE_PARK_AFTER_TICKS: u32 = 10;
/// Minimum debt age (persisted via `transfer_first_seen`) before parking.
#[allow(dead_code)]
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
    /// (region_id, reporter) -> that peer's median series id for the region.
    split_hints: tokio::sync::RwLock<HashMap<(u64, u64), u64>>,
    /// region_id -> planned target (primary first), balancing copies and
    /// primaries across nodes; refreshed each tick. See `plan_placement`.
    placement_plan: tokio::sync::RwLock<HashMap<u64, Vec<u64>>>,
    /// Regions with an in-flight operator (split/merge/rebalance/failover/transfer).
    region_operators: tokio::sync::RwLock<HashMap<u64, ()>>,
    /// region_id -> the attempt sequence number of its last convergence move.
    /// Sorting the mover queue by this is what makes it round-robin: without
    /// it the queue was ordered by tier alone, so map order decided everything
    /// and the same few regions were retried every tick forever.
    placement_last_attempted: tokio::sync::RwLock<HashMap<u64, u64>>,
    placement_attempt_seq: std::sync::atomic::AtomicU64,
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
    ///
    /// The value is `(peer map_version, cluster map_version when probed)`. The
    /// second half is load-bearing: a placement committing earlier in the same
    /// tick advances the cluster version, and judging a cached probe against
    /// the newer number rejects a peer that never lagged — a permanent
    /// "no caught-up replica candidate" stall on a cluster whose maps all
    /// agree and whose probes all return 200.
    ///
    /// `None` caches a *failed* probe for the rest of the tick. An unreachable
    /// node is still a placement candidate — `holds_placement` deliberately
    /// keeps `Disconnected` — so without this a restarting node is re-probed
    /// once per region at up to `connect_timeout` each, and enough regions
    /// means the tick never finishes.
    peer_map_versions: PeerMapVersions,
    /// Consecutive ticks a region skipped placement with no caught-up
    /// candidate, keyed by region id. A cluster-wide placement stall is
    /// otherwise only visible at `debug!`.
    placement_stalls: tokio::sync::RwLock<HashMap<u64, u32>>,
    /// Consecutive ticks a region's placement convergence returned an error,
    /// keyed by region id. Throttles the warning without hiding it.
    placement_errors: tokio::sync::RwLock<HashMap<u64, u32>>,
    /// Learners `(region_id, peer)` seen outstanding on the previous tick.
    ///
    /// The convergence path stages and promotes a learner inline, so one that
    /// is still outstanding on the NEXT tick is one whose inline promote
    /// failed. Re-staging every outstanding learner on every tick instead
    /// re-copies regions that are merely mid-flight, and that competes with
    /// evacuation for the same movement budget.
    learners_seen: tokio::sync::RwLock<HashSet<(u64, u64)>>,
    /// True while [`ShardScheduler::run`]'s background mover pool is working.
    /// Ticks that find it busy plan (splits, heals, failover) but leave their
    /// placements for a later tick.
    movers_busy: std::sync::atomic::AtomicBool,
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
            split_hints: tokio::sync::RwLock::new(HashMap::new()),
            placement_plan: tokio::sync::RwLock::new(HashMap::new()),
            region_operators: tokio::sync::RwLock::new(HashMap::new()),
            placement_last_attempted: tokio::sync::RwLock::new(HashMap::new()),
            placement_attempt_seq: std::sync::atomic::AtomicU64::new(0),
            unhealthy_primaries: tokio::sync::RwLock::new(HashMap::new()),
            reconciliation_queue: std::sync::Mutex::new(Vec::new()),
            rollup_dests: tokio::sync::RwLock::new(HashMap::new()),
            peer_map_versions: tokio::sync::RwLock::new(HashMap::new()),
            placement_stalls: tokio::sync::RwLock::new(HashMap::new()),
            placement_errors: tokio::sync::RwLock::new(HashMap::new()),
            learners_seen: tokio::sync::RwLock::new(HashSet::new()),
            movers_busy: std::sync::atomic::AtomicBool::new(false),
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

    /// `peer`'s committed `map_version`, probed at most once per tick, paired
    /// with the cluster `map_version` the probe was taken against. `None` if
    /// the probe failed — also remembered for the rest of the tick.
    ///
    /// A probe only ever says "this peer had applied up to X when asked". The
    /// caller must judge it against the cluster version at that moment, not
    /// against one a later commit in the same tick moved underneath it.
    async fn cached_peer_map_version(
        &self,
        peer: u64,
        addr: &str,
        cluster_ver: u64,
        client: &reqwest::Client,
    ) -> Option<(u64, u64)> {
        let cell = self.probe_cell(&self.peer_map_versions, peer).await;
        *cell
            .get_or_init(|| self.probe_peer_map_version(peer, addr, cluster_ver, client))
            .await
    }

    /// The shared slot for `key`, created on first use. Held only long enough
    /// to clone the `Arc` — never across the probe itself.
    async fn probe_cell<K: Copy + Eq + std::hash::Hash, T>(
        &self,
        cache: &tokio::sync::RwLock<HashMap<K, ProbeCell<T>>>,
        key: K,
    ) -> ProbeCell<T> {
        if let Some(cell) = cache.read().await.get(&key) {
            return cell.clone();
        }
        cache
            .write()
            .await
            .entry(key)
            .or_insert_with(|| Arc::new(tokio::sync::OnceCell::new()))
            .clone()
    }

    async fn probe_peer_map_version(
        &self,
        peer: u64,
        addr: &str,
        cluster_ver: u64,
        client: &reqwest::Client,
    ) -> Option<(u64, u64)> {
        match fetch_peer_map_version(client, addr).await {
            Ok(version) => Some((version, cluster_ver)),
            Err(e) => {
                // Warn, not debug: the shipped image pins the log level to
                // `info` (`ENV HYPERBYTEDB__LOGGING__LEVEL=info`, which the
                // env overlay applies over the ConfigMap), so a candidate that
                // can never be probed was previously invisible in production.
                // Caching the failure caps this at one line per peer per tick.
                tracing::warn!(
                    error = %e,
                    candidate = peer,
                    "map version probe failed; candidate skipped for placement this tick"
                );
                None
            }
        }
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

    /// Record that `region_id`'s placement convergence failed, and warn.
    ///
    /// Warns on the first failure, then every `PLACEMENT_ERROR_WARN_EVERY`
    /// consecutive ticks. Convergence carries the staging error -- the one
    /// signal that says WHY a region will not reach its replication factor --
    /// and it was logged at `debug!`, which the shipped image's pinned `info`
    /// level discards. A cluster could fail to place every region, every tick,
    /// in complete silence.
    async fn note_placement_error(&self, region_id: u64, err: &HyperbytedbError) {
        counter!("hyperbytedb_shard_placement_errors_total").increment(1);
        let mut errs = self.placement_errors.write().await;
        let count = errs.entry(region_id).or_insert(0);
        *count = count.saturating_add(1);
        if *count == 1 || *count % PLACEMENT_ERROR_WARN_EVERY == 0 {
            tracing::warn!(
                error = %err,
                region_id,
                consecutive = *count,
                "placement convergence failed"
            );
        }
    }

    async fn clear_placement_error(&self, region_id: u64) {
        self.placement_errors.write().await.remove(&region_id);
    }

    /// Drop per-region counters for regions the map no longer has.
    ///
    /// `placement_stalls` and `placement_errors` are keyed by `region_id` and
    /// cleared only when a region converges. A region that splits, merges or
    /// has its measurement dropped never converges again, and `next_region_id`
    /// only ever increases -- so without this the maps grow for the life of
    /// the process on any cluster that reshapes regions, which is every
    /// sharded cluster under load.
    async fn prune_region_counters(&self, map: &ShardMap) {
        let live: HashSet<u64> = map
            .spaces
            .values()
            .flat_map(|space| space.regions.iter().map(|r| r.region_id))
            .collect();
        self.placement_stalls
            .write()
            .await
            .retain(|id, _| live.contains(id));
        self.placement_errors
            .write()
            .await
            .retain(|id, _| live.contains(id));
        // Splits and merges churn region ids, so the round-robin cursor would
        // otherwise grow without bound.
        self.placement_last_attempted
            .write()
            .await
            .retain(|id, _| live.contains(id));
    }

    /// Finish any learner left outstanding by an earlier tick.
    ///
    /// Nothing else will: once the peer is in `peers`, `next_placement_step`
    /// considers the region converged and never revisits it, so a learner whose
    /// PromotePeer failed -- a moved epoch, a leadership change mid-flight --
    /// stays a learner forever, receiving writes and serving nothing. One run
    /// added 10 learners and promoted 6.
    ///
    /// Re-staging before promoting is deliberate: the learner has been
    /// receiving replicated writes since it joined, so this only has to close
    /// whatever the first copy missed, and staging is idempotent at the
    /// destination.
    async fn try_promote_learners(&self, map: &ShardMap) {
        // Only act on a learner that was ALSO outstanding last tick. The
        // convergence path stages and promotes inline, so a learner seen once
        // is usually just mid-flight; re-staging it immediately re-copies the
        // region for nothing and competes with evacuation for the movement
        // budget. One left over on two consecutive ticks is one whose inline
        // promote actually failed.
        let current: HashSet<(u64, u64)> = map
            .spaces
            .values()
            .flat_map(|space| {
                space
                    .regions
                    .iter()
                    .flat_map(|r| r.staging_peers.iter().map(move |p| (r.region_id, *p)))
            })
            .collect();
        let stale: HashSet<(u64, u64)> = {
            let seen = self.learners_seen.read().await;
            current.intersection(&seen).copied().collect()
        };
        *self.learners_seen.write().await = current;
        if stale.is_empty() {
            return;
        }

        for space in map.spaces.values() {
            for region in &space.regions {
                for peer in region.staging_peers.clone() {
                    if !stale.contains(&(region.region_id, peer)) {
                        continue;
                    }
                    // A mover still copying onto it: ticks no longer wait for
                    // moves, so a learner seen twice is not necessarily stale.
                    if self.region_has_operator(region.region_id).await {
                        continue;
                    }
                    if self
                        .stage_region_onto(&space.key, region, peer, 0)
                        .await
                        .is_err()
                    {
                        continue;
                    }
                    let fresh = match self.shard_map.snapshot().await {
                        Ok(m) => m,
                        Err(_) => continue,
                    };
                    let Some(current) = current_region(&fresh, &space.key, region.region_id) else {
                        continue;
                    };
                    if self
                        .propose(ShardMapOp::PromotePeer {
                            key: space.key.clone(),
                            region_id: region.region_id,
                            peer,
                            epoch: current.epoch,
                        })
                        .await
                        .is_ok()
                    {
                        counter!("hyperbytedb_shard_learners_promoted_total").increment(1);
                    }
                }
            }
        }
    }

    /// Cut over any staging region whose data has finished moving.
    ///
    /// A split publishes its child NOT serving: until this fires the donor
    /// still holds and serves the range, so readers never see a region that is
    /// missing rows. Activation is gated on the movement being verified --
    /// `transfer_verified` is the reconciliation machinery's own completion
    /// signal -- so the child only becomes routable once it actually holds the
    /// data. A child that kept the donor's primary carries no debt and
    /// activates on the next tick.
    ///
    /// The flip is one committed op, so every node's location cache applies it
    /// in the same apply: no node sees a half-applied handover.
    #[allow(dead_code)]
    async fn try_activate_staged_regions(&self, map: &ShardMap) {
        for space in map.spaces.values() {
            for region in &space.regions {
                if region.is_serving() {
                    continue;
                }
                if region.transfer_outstanding() {
                    continue;
                }
                match self
                    .propose(ShardMapOp::ActivateRegion {
                        key: space.key.clone(),
                        region_id: region.region_id,
                        epoch: region.epoch,
                    })
                    .await
                {
                    Ok(()) => {
                        counter!("hyperbytedb_shard_region_activations_total").increment(1);
                        tracing::info!(
                            region_id = region.region_id,
                            primary = region.primary,
                            "staging region cut over to serving"
                        );
                    }
                    Err(e) => {
                        tracing::warn!(
                            region_id = region.region_id,
                            error = %e,
                            "staging region cutover failed; will retry"
                        );
                    }
                }
            }
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

    /// The series count this scheduler holds for `(region_id, node_id)`, or
    /// `None` if it never recorded one. Test support for the heartbeat
    /// handler, which has no other externally visible effect.
    pub async fn recorded_heartbeat_series(&self, region_id: u64, node_id: u64) -> Option<u64> {
        self.heartbeats
            .read()
            .await
            .iter()
            .find(|(r, n, ..)| *r == region_id && *n == node_id)
            .map(|(_, _, series, ..)| *series)
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

    /// Remember (or forget) a peer's split-key hint for a region.
    pub async fn record_split_hint(&self, region_id: u64, node_id: u64, hint: Option<u64>) {
        let mut hints = self.split_hints.write().await;
        match hint {
            Some(k) => {
                hints.insert((region_id, node_id), k);
            }
            None => {
                hints.remove(&(region_id, node_id));
            }
        }
    }

    /// The split hint from the region peer reporting the most series: the
    /// same peer whose count triggered the split.
    async fn peer_split_hint(&self, region: &ShardRegion) -> Option<u64> {
        let hb = self.heartbeats.read().await;
        let reporter = hb
            .iter()
            .filter(|(r, n, ..)| *r == region.region_id && region.peers.contains(n))
            .max_by_key(|(_, _, series, ..)| *series)
            .map(|(_, n, ..)| *n)?;
        drop(hb);
        self.split_hints
            .read()
            .await
            .get(&(region.region_id, reporter))
            .copied()
    }

    fn is_leader(&self) -> bool {
        #[cfg(test)]
        if self.test_force_leader {
            return true;
        }
        let metrics = self.raft.metrics().borrow().clone();
        metrics.current_leader == Some(self.node_id)
    }

    /// Tick on `interval` while leader. Placement moves run in a background
    /// pool, so a tick never waits on a data copy: splits, heals and failover
    /// keep their cadence while regions move. A tick used to await every move
    /// it queued, and on n3 a new database's second split round waited 3.5
    /// minutes behind moves of an older database's regions.
    pub async fn run(self: Arc<Self>, interval: Duration, mut shutdown_rx: watch::Receiver<bool>) {
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    if !self.is_leader() {
                        continue;
                    }
                    match self.plan_tick().await {
                        Ok(pending) => {
                            self.retire_evacuated_nodes().await;
                            self.start_background_placements(pending);
                        }
                        Err(e) => tracing::warn!(error = %e, "shard scheduler tick error"),
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

    /// Run `pending` in a background mover pool, unless the previous pool is
    /// still working: those regions are planned again on a later tick.
    fn start_background_placements(self: &Arc<Self>, pending: Vec<PendingPlacement>) {
        use std::sync::atomic::Ordering;
        if pending.is_empty() || self.movers_busy.swap(true, Ordering::AcqRel) {
            return;
        }
        let this = Arc::clone(self);
        tokio::spawn(async move {
            this.run_pending_placements(pending).await;
            this.movers_busy.store(false, Ordering::Release);
        });
    }

    /// Claim `region_id` for one operator (split, merge, heal, move). Only
    /// exclusivity: how many moves run at once is the mover pool's
    /// `schedule_limit`. Capping this table at `schedule_limit` as well let
    /// four background movers lock the planning pass out of every region.
    async fn acquire_operator(&self, region_id: u64) -> bool {
        let mut ops = self.region_operators.write().await;
        if ops.contains_key(&region_id) {
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

    /// Recompute every region's target so that both the copies each node
    /// holds and the regions it leads are even. Rollup destinations are left
    /// out: their placement is not driven from here, and counting them would
    /// skew everyone else's share.
    async fn refresh_placement_plan(&self, map: &ShardMap) {
        let candidates = self.active_candidate_ids().await;
        if candidates.is_empty() {
            return;
        }
        let rf = effective_replication_factor(self.config.replication_factor, candidates.len());
        let mut inputs = Vec::new();
        {
            let hb = self.heartbeats.read().await;
            for space in map.spaces.values() {
                if self.is_rollup_dest(&space.key).await {
                    continue;
                }
                let reported: Vec<(u64, u64)> = space
                    .regions
                    .iter()
                    .map(|r| {
                        (
                            r.end.saturating_sub(r.start),
                            max_region_peer_heartbeat_stats(&hb, r).0,
                        )
                    })
                    .collect();
                let weights = estimate_region_weights(&reported);
                for (r, weight) in space.regions.iter().zip(weights) {
                    inputs.push(RegionPlanInput {
                        region_id: r.region_id,
                        // Established copies only. A learner whose copy has
                        // not finished holds nothing yet; counting it made
                        // the plan keep it and drop a full copy instead.
                        current_peers: r
                            .peers
                            .iter()
                            .copied()
                            .filter(|p| !r.staging_peers.contains(p))
                            .collect(),
                        current_primary: r.primary,
                        weight,
                        hrw: target_placement(&space.key, r.start, &candidates, candidates.len()),
                    });
                }
            }
        }
        let plan = plan_placement(&inputs, rf, PRIMARY_BALANCE_TOLERANCE_PCT);
        *self.placement_plan.write().await = plan;
    }

    /// A region's target: the balanced plan when it is current for these
    /// candidates, otherwise plain HRW (e.g. before the first plan, or when
    /// membership changed since it was made).
    async fn balanced_target(
        &self,
        key: &MeasurementKey,
        region: &ShardRegion,
        candidates: &[u64],
        rf: usize,
    ) -> Vec<u64> {
        if let Some(t) = self.placement_plan.read().await.get(&region.region_id)
            && t.len() == rf
            && t.iter().all(|n| candidates.contains(n))
        {
            return t.clone();
        }
        target_placement(key, region.start, candidates, rf)
    }

    /// One full scheduler pass with its placements run inline. [`Self::run`]
    /// plans the same way but moves data in the background.
    #[cfg(test)]
    async fn tick(&self) -> Result<(), HyperbytedbError> {
        let pending = self.plan_tick().await?;
        self.run_pending_placements(pending).await;
        // After the movers, so a node that finished evacuating this tick is
        // retired in the same tick rather than waiting for the next.
        self.retire_evacuated_nodes().await;
        Ok(())
    }

    /// Splits, merges, heals and failover for every region, plus the
    /// placement moves it wants, returned instead of run.
    async fn plan_tick(&self) -> Result<Vec<PendingPlacement>, HyperbytedbError> {
        // Peer map versions are only valid for the tick that probed them.
        self.peer_map_versions.write().await.clear();
        let map = self.shard_map.snapshot().await?;
        self.prune_region_counters(&map).await;
        self.refresh_placement_plan(&map).await;
        // Learners left behind by a failed promote would otherwise never be
        // revisited: once in `peers`, the region looks converged.
        self.try_promote_learners(&map).await;
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

        let mut pending_placements: Vec<PendingPlacement> = Vec::new();

        for (key, space) in &map.spaces {
            metrics::gauge!(
                "hyperbytedb_shard_regions",
                "db" => key.db.clone(),
                "rp" => key.rp.clone(),
                "measurement" => key.measurement.clone(),
            )
            .set(space.regions.len() as f64);

            let mut cap_refused = 0usize;

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

                // Availability before shape. Heal and failover run first so a
                // region that cannot split — capped, too narrow, or carrying
                // transfer debt — still gets a live primary and RF restored.
                // While these ran last, a measurement at its cap left every
                // region unhealable for as long as it stayed at the cap.
                if let Err(e) = self.try_replace_dead_peer(&space.key, region).await {
                    tracing::debug!(error = %e, region_id = region.region_id, "peer heal skipped");
                }

                if let Err(e) = self
                    .try_failover_unhealthy_primary(&space.key, region, now)
                    .await
                {
                    tracing::debug!(error = %e, region_id = region.region_id, "failover skipped");
                }

                let should_split = region_ready_to_split(
                    series_count,
                    region.last_split_at,
                    now,
                    self.config.region_split_series,
                    self.config.region_max_series,
                    self.config.region_split_cooldown_secs,
                );

                // Only a *proposed* split ends the region's pass: it replaced
                // this region with two children, so placement on the region we
                // are holding is meaningless. A refusal leaves the region
                // exactly as it was, and the pass must carry on.
                if should_split {
                    let key = space.key.clone();
                    let region = region.clone();
                    match self.try_split(&key, &region, series_count).await {
                        Ok(outcome) if outcome.is_proposed() => {
                            self.release_operator(region.region_id).await;
                            continue;
                        }
                        Ok(SplitOutcome::Refused(SplitRefusal::RegionCap)) => {
                            cap_refused += 1;
                        }
                        Ok(_) => {}
                        Err(e) => {
                            tracing::debug!(
                                error = %e,
                                region_id = region.region_id,
                                "shard split failed"
                            );
                        }
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
                    match self.try_split(&key, &region, series_count).await {
                        Ok(outcome) if outcome.is_proposed() => {
                            self.release_operator(region.region_id).await;
                            continue;
                        }
                        Ok(SplitOutcome::Refused(SplitRefusal::RegionCap)) => {
                            cap_refused += 1;
                        }
                        Ok(_) => {}
                        Err(e) => {
                            tracing::debug!(
                                error = %e,
                                region_id = region.region_id,
                                "shard load split failed"
                            );
                        }
                    }
                }

                if let Some(tier) = self.preview_placement_tier(&space.key, region).await {
                    let approx_bytes = max_region_peer_heartbeat_bytes(&hb, region);
                    pending_placements.push(PendingPlacement {
                        key: space.key.clone(),
                        region_id: region.region_id,
                        tier,
                        approx_bytes,
                    });
                }

                self.release_operator(region.region_id).await;
            }

            // One line per measurement per tick, not one per region: at the cap
            // every region refuses on every tick, which drowned the log.
            if cap_refused > 0 {
                tracing::warn!(
                    db = %key.db,
                    rp = %key.rp,
                    measurement = %key.measurement,
                    region_count = space.regions.len(),
                    regions_refused = cap_refused,
                    "shard splits refused: measurement at region cap"
                );
            }
        }

        Ok(pending_placements)
    }

    /// Whether this region has a placement step pending (cheap; no data movement).
    async fn preview_placement_tier(
        &self,
        key: &MeasurementKey,
        region: &ShardRegion,
    ) -> Option<MoveTier> {
        if self.is_rollup_dest(key).await || self.peer_client.is_none() {
            return None;
        }
        let map = self.shard_map.snapshot().await.ok()?;
        let region = current_region(&map, key, region.region_id)?;
        let candidates = self.active_candidate_ids().await;
        if candidates.is_empty() {
            return None;
        }
        let rf = effective_replication_factor(self.config.replication_factor, candidates.len());
        let target = self.balanced_target(key, region, &candidates, rf).await;
        next_placement_step(region, &target)?;
        let departing = self.departing_node_ids().await;
        let unhealthy = self.unhealthy_region_peers(region).await;
        Some(move_tier(region, rf, &departing, &unhealthy))
    }

    /// Drain the planner's queue with a bounded pool of movers.
    ///
    /// Was: sort by tier, `take(schedule_limit)`, and `.await` each full copy
    /// inline. That is head-of-line blocking with a fixed order — at one tier
    /// the same first `schedule_limit` regions in map order were retried every
    /// tick, and every region behind them waited on a copy that had nothing to
    /// do with them. On the kind cluster that was 15 placements in five hours.
    ///
    /// Now: every queued region is attempted each tick, `schedule_limit` at a
    /// time, ordered by tier (safety first) and then least-recently-attempted
    /// so the movement budget rotates across regions instead of pinning to
    /// whichever ones sort first. Each mover holds the region operator for the
    /// whole `AddPeer -> stage -> PromotePeer` sequence, so no other part of
    /// the tick can touch a region mid-move.
    async fn run_pending_placements(&self, mut pending: Vec<PendingPlacement>) {
        use futures::stream::StreamExt;

        if pending.is_empty() {
            return;
        }
        let last_attempted = self.placement_last_attempted.read().await.clone();
        pending.sort_by_key(|p| {
            (
                p.tier,
                last_attempted.get(&p.region_id).copied().unwrap_or(0),
                p.region_id,
            )
        });

        let limit = self.config.schedule_limit.max(1);

        // Bound the bytes in flight, not just the number of movers.
        //
        // When a move staged a whole region in memory, four concurrent movers
        // of large regions OOM-killed a 4Gi leader on the kind cluster. Moves
        // now stream one chunk at a time, so each is charged a chunk (see
        // `mover_inflight_permits`); charging the whole region serialised
        // every move of a region over the budget, on n3 one at a time.
        //
        // Permits are MiB so the counts stay small. A move larger than the
        // whole budget still runs, alone — never refuse a move outright.
        let budget_mib = (self.config.movement_max_inflight_bytes / (1024 * 1024)).max(1) as usize;
        let byte_budget = Arc::new(tokio::sync::Semaphore::new(budget_mib));

        futures::stream::iter(pending)
            .for_each_concurrent(limit, |job| {
                let byte_budget = Arc::clone(&byte_budget);
                async move {
                    let want = mover_inflight_permits(job.approx_bytes, budget_mib);
                    let _permit = match byte_budget.acquire_many_owned(want as u32).await {
                        Ok(p) => p,
                        Err(_) => return,
                    };
                    // One mover per region. A region the planning pass is still
                    // holding is simply left for the next tick.
                    if !self.acquire_operator(job.region_id).await {
                        return;
                    }
                    self.note_placement_attempt(job.region_id).await;
                    self.run_one_placement(&job).await;
                    self.release_operator(job.region_id).await;
                }
            })
            .await;
    }

    async fn run_one_placement(&self, job: &PendingPlacement) {
        let Ok(map) = self.shard_map.snapshot().await else {
            return;
        };
        let Some(region) = current_region(&map, &job.key, job.region_id) else {
            return;
        };
        match self.try_converge_placement(&job.key, region).await {
            Ok(true) => {
                self.clear_placement_error(job.region_id).await;
            }
            Ok(false) => {}
            Err(e) => {
                // Isolated to this region on purpose: a region that cannot be
                // moved must not cost the other queued regions their tick.
                self.note_placement_error(job.region_id, &e).await;
            }
        }
    }

    async fn note_placement_attempt(&self, region_id: u64) {
        let seq = self
            .placement_attempt_seq
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            .saturating_add(1);
        self.placement_last_attempted
            .write()
            .await
            .insert(region_id, seq);
    }

    /// Per-region progress for every holder of `region`, from the heartbeat
    /// stream.
    ///
    /// This replaces the manifest watermark as the input to primary selection,
    /// and the difference is the whole point: `sync_manifest` emits a region
    /// watermark ONLY where the reporting node is that region's primary, so a
    /// candidate reported 0 for every region it merely replicates. `0` there
    /// means "I am not the primary", not "I hold no data" — so every candidate
    /// looked infinitely behind and `promote_skipped_total{watermark_lag}`
    /// blocked primacy from ever moving. Measured on the kind cluster: 2,431
    /// skips, one node primarying all 192 regions.
    ///
    /// The heartbeat path already carries the right quantity. Every node in
    /// `region.peers` reports its series count for that region — learners
    /// included, since `AddPeer` puts them in `peers` — so this is per-region
    /// and comparable across replicas. `None` means no fresh heartbeat: not
    /// heard from, which is distinct from a reported zero.
    async fn region_progress_for_peers(&self, region: &ShardRegion) -> HashMap<u64, Option<u64>> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let ttl = self
            .config
            .heartbeat_interval_secs
            .saturating_mul(HEARTBEAT_TTL_INTERVALS)
            .max(1);
        let hb = self.heartbeats.read().await;
        region
            .peers
            .iter()
            .copied()
            .map(|peer| {
                let progress = hb
                    .iter()
                    .filter(|(r, n, .., at)| {
                        *r == region.region_id && *n == peer && now.saturating_sub(*at) <= ttl
                    })
                    .map(|(_, _, series, ..)| *series)
                    .max();
                (peer, progress)
            })
            .collect()
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
            // No serving peer survives. If a learner does, promote it and hand
            // it primacy in the same tick rather than leaving the region with
            // no target at all.
            //
            // The watermark guard below is deliberately NOT applied here:
            // availability outranks it when the alternative is an
            // indefinitely unserved region, and a learner populated by bulk
            // transfer legitimately reports a low position anyway. The gap is
            // logged instead of silently deciding.
            if let Some(learner) =
                pick_learner_survivor(&self.membership, region, self.node_id).await
            {
                tracing::warn!(
                    region_id = region.region_id,
                    learner,
                    dead_primary = region.primary,
                    "promoting learner-only survivor: region has no serving peer"
                );
                self.propose(ShardMapOp::PromotePeer {
                    key: key.clone(),
                    region_id: region.region_id,
                    peer: learner,
                    epoch: region.epoch,
                })
                .await?;
                counter!("hyperbytedb_shard_learner_survivor_promoted_total").increment(1);

                // Re-read: PromotePeer moved the epoch, and TransferPrimary
                // must carry the current one.
                let after = self.shard_map.snapshot().await?;
                let Some(current) = current_region(&after, key, region.region_id) else {
                    return Ok(());
                };
                self.propose(ShardMapOp::TransferPrimary {
                    key: key.clone(),
                    region_id: current.region_id,
                    new_primary: learner,
                    epoch: current.epoch,
                })
                .await?;
                counter!("hyperbytedb_shard_primary_failover_total").increment(1);
                self.unhealthy_primaries.write().await.remove(&map_key);
                return Ok(());
            }
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
        if self.peer_client.is_some() {
            let progress = self.region_progress_for_peers(region).await;
            // `flatten` drops unknowns rather than reading them as 0: only
            // progress we actually heard take part in the comparison.
            let candidate_wm = progress.get(&new_primary).copied().flatten();
            let max_wm = progress.values().copied().flatten().max();
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

    /// Propose a split, or say why not. A `Refused` outcome means the region
    /// is unchanged and the rest of its pass (placement, heal, failover) must
    /// still run — only `Proposed` invalidates the region we are holding.
    async fn try_split(
        &self,
        key: &MeasurementKey,
        region: &ShardRegion,
        series_count: u64,
    ) -> Result<SplitOutcome, HyperbytedbError> {
        if region.end.saturating_sub(region.start) <= 1 {
            return Ok(self.refuse_split(SplitRefusal::RangeTooNarrow));
        }
        // Lifecycle guard: never fan outstanding transfer debt out across a
        // re-split (round-3 N2).
        if region.transfer_outstanding() {
            tracing::debug!(
                region_id = region.region_id,
                "shard split deferred: transfer debt outstanding on parent"
            );
            return Ok(self.refuse_split(SplitRefusal::TransferDebt));
        }
        let map = self.shard_map.snapshot().await?;
        let candidates = self.active_candidate_ids().await;
        let cap = region_cap(self.config.max_regions_per_measurement, candidates.len());
        if let Some(space) = map.space(&key.db, &key.rp, &key.measurement)
            && space.regions.len() >= cap
        {
            // Debug, not warn: at the cap this fires for every region on every
            // tick. The tick logs one warn per measurement instead.
            tracing::debug!(
                region_id = region.region_id,
                series_count,
                start = region.start,
                end = region.end,
                region_count = space.regions.len(),
                max_regions = cap,
                "shard split refused: region cap reached"
            );
            return Ok(self.refuse_split(SplitRefusal::RegionCap));
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
        // TiKV-style: a split moves no data.
        //
        // `right` is a clone of the parent, so it inherits the parent's peers
        // AND primary. The rows for its range are already on exactly those
        // nodes, so the split is metadata only: propose it, and both children
        // are immediately correct and routable. This is what
        // `exec_batch_split` does in TiKV -- it clones the parent's peers into
        // each new region and copies no KV data, because a region's placement
        // IS its Raft group's stores.
        //
        // Re-placing the right child HERE is what made splits lossy. HRW hashes
        // it to a different node, which forced a one-shot bulk copy, which
        // needed a staging state and a cutover -- and the writes that landed on
        // the donor between the copy and the cutover were never forwarded.
        // Measured at 33 lost writes per run, almost exactly one Raft
        // round-trip of ingest.
        //
        // Placement is convergence's job, not the split's. `try_converge_placement`
        // walks the child to its HRW target afterwards by staging a peer BEFORE
        // committing `AddPeer` and then transferring the primary -- and a
        // committed peer receives ongoing writes through normal replication, so
        // there is no "copy finished" instant that can be stale. That is the
        // analogue of TiKV's learner catching up on the Raft log before PD
        // promotes it.
        //
        // The tradeoff, accepted deliberately: load does not spread until
        // convergence runs, rather than instantly at split time.

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

        Ok(SplitOutcome::Proposed)
    }

    fn refuse_split(&self, reason: SplitRefusal) -> SplitOutcome {
        counter!(
            "hyperbytedb_shard_split_refused_total",
            "reason" => reason.as_str()
        )
        .increment(1);
        SplitOutcome::Refused(reason)
    }

    /// Propose clearing a verified split-transfer debt. No-op success when
    /// the region carries no outstanding flag. Gated behind
    /// `transfer_clear_proposals_enabled` for mixed-version rolling upgrades:
    /// old binaries cannot decode the `ClearVerified` op from the Raft log.
    // Unreachable in production since splits became metadata-only: the split
    // was its only caller. Nothing now creates transfer debt either --
    // apply-time normalization flags a child only when its primary differs
    // from the parent's, which a TiKV-style split never does, and
    // TransferPrimary does not set it. So the reconciliation queue, its drain,
    // and ClearVerified are all currently dead paths.
    //
    // Retained rather than deleted in the same change as the behaviour switch:
    // removing that machinery is a large, separate deletion that deserves its
    // own review. The test below still covers the rollup-destination exclusion.
    #[allow(dead_code)]
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

    #[allow(dead_code)]
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

    async fn compute_split_key(
        &self,
        key: &MeasurementKey,
        region: &ShardRegion,
    ) -> Result<u64, HyperbytedbError> {
        let peer_hint = self.peer_split_hint(region).await;
        // The local index is complete only where this node holds the region.
        let local_median = if region.peers.contains(&self.node_id) {
            self.metadata
                .median_series_id_in_range(
                    &key.db,
                    &key.rp,
                    &key.measurement,
                    region.start,
                    region.end,
                )
                .await
                .ok()
                .flatten()
        } else {
            None
        };
        Ok(choose_split_key(
            region.start,
            region.end,
            peer_hint,
            local_median,
        ))
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
    /// Node ids eligible to hold region data.
    ///
    /// `holds_placement`: `Active | Draining | Disconnected`. A restarting node
    /// keeps its regions — excluding it would make every rolling upgrade
    /// evacuate and refill the cluster, since the preStop hook drains every
    /// pod, not just scale-down targets. A briefly unreachable node keeps them
    /// too; the dead-node timeout handles real death.
    ///
    /// `Decommissioning` and `Leaving` are excluded, which is exactly what
    /// makes convergence evacuate them without a dedicated rule.
    async fn active_candidate_ids(&self) -> Vec<u64> {
        let now = chrono::Utc::now().timestamp();
        let timeout = self.config.dead_node_timeout_secs;
        let m = self.membership.read().await;
        let mut ids: Vec<u64> = m
            .nodes
            .values()
            .filter(|n| holds_placement(n) && !peer_disconnected_past_deadline(n, now, timeout))
            .map(|n| n.node_id)
            .collect();
        drop(m);
        ids.sort_unstable();
        ids.dedup();
        ids
    }

    /// Node ids being permanently removed. NOT `Draining` — that is a restart.
    async fn departing_node_ids(&self) -> Vec<u64> {
        let m = self.membership.read().await;
        let ids = m
            .nodes
            .values()
            .filter(|n| is_departing(n))
            .map(|n| n.node_id)
            .collect();
        drop(m);
        ids
    }

    /// Peers of `region` whose node is not serving, for movement-tier urgency.
    async fn unhealthy_region_peers(&self, region: &ShardRegion) -> Vec<u64> {
        let m = self.membership.read().await;
        let ids = region
            .peers
            .iter()
            .copied()
            .filter(|p| m.get_node(*p).is_none_or(|n| !is_serving(n)))
            .collect();
        drop(m);
        ids
    }

    /// True when no region anywhere in the map still lists `node_id`.
    ///
    /// A `Decommissioning` node stays in membership until this holds, so
    /// operators can watch evacuation progress and nothing removes a node that
    /// still owns rows. This is the ONLY thing that may move a decommissioning
    /// node to `Leaving`: `drain` reaches its terminal state on a 90s-capped
    /// replication wait, which says nothing about whether regions have moved.
    async fn decommission_complete(&self, node_id: u64) -> bool {
        let Ok(map) = self.shard_map.snapshot().await else {
            return false;
        };
        !map.spaces
            .values()
            .flat_map(|s| s.regions.iter())
            .any(|r| r.peers.contains(&node_id))
    }

    /// Move a fully evacuated `Decommissioning` node to `Leaving`.
    async fn retire_evacuated_nodes(&self) {
        for node_id in self.departing_node_ids().await {
            let still_decommissioning = {
                let m = self.membership.read().await;
                let v = m
                    .get_node(node_id)
                    .is_some_and(|n| n.state == NodeState::Decommissioning);
                drop(m);
                v
            };
            if !still_decommissioning {
                continue;
            }
            if self.decommission_complete(node_id).await {
                let mut m = self.membership.write().await;
                let moved = m.transition(node_id, NodeState::Leaving);
                drop(m);
                if moved {
                    counter!("hyperbytedb_shard_decommission_completed_total").increment(1);
                    tracing::info!(node_id, "decommission complete; node marked Leaving");
                }
            }
        }
    }

    /// Move one region one step toward its target placement.
    ///
    /// Replaces `try_place_live_member`, `try_place_idle_member` and
    /// `try_place_primary`. Those keyed on recency to prove termination
    /// without a cooldown; with a pure target function termination is trivial —
    /// each step strictly reduces the difference, and `next_placement_step`
    /// returns `None` at the fixed point.
    ///
    /// Returns `Ok(true)` when a step committed, so the caller yields the
    /// region: a commit bumps the epoch and invalidates this tick's snapshot.
    async fn try_converge_placement(
        &self,
        key: &MeasurementKey,
        region: &ShardRegion,
    ) -> Result<bool, HyperbytedbError> {
        // `apply_transfer_push` is not idempotent, so a re-staged
        // SummingMergeTree destination sums twice, permanently and silently.
        if self.is_rollup_dest(key).await {
            return Ok(false);
        }
        // No peer client means nothing can be staged; skip quietly rather than
        // erroring every tick.
        if self.peer_client.is_none() {
            return Ok(false);
        }
        // Re-read: `drain_reconciliation` runs before the tick loop and can
        // bump epochs, so the caller's snapshot may already be stale.
        let map = self.shard_map.snapshot().await?;
        metrics::gauge!("hyperbytedb_shard_placement_map_version").set(map.map_version as f64);
        let membership_version = self.membership.read().await.version;
        metrics::gauge!("hyperbytedb_shard_placement_membership_version")
            .set(membership_version as f64);
        let Some(region) = current_region(&map, key, region.region_id) else {
            return Ok(false);
        };
        let candidates = self.active_candidate_ids().await;
        // Absent is not empty. No candidates means membership is unreadable,
        // not that the region is converged; falling through would return the
        // same `None` as a healthy region and clear the stall counter.
        if candidates.is_empty() {
            self.note_placement_stall(region.region_id, 0).await;
            return Ok(false);
        }
        let rf = effective_replication_factor(self.config.replication_factor, candidates.len());
        let target = self.balanced_target(key, region, &candidates, rf).await;
        let Some(step) = next_placement_step(region, &target) else {
            self.clear_placement_stall(region.region_id).await;
            return Ok(false);
        };
        // After the converged check, never before: gating first would skip the
        // `None` branch for already-converged regions once a tick's budget was
        // spent, leaving their stall counters uncleared and reporting healthy
        // regions as stalled under exactly the load stall detection exists for.
        let departing = self.departing_node_ids().await;
        let unhealthy = self.unhealthy_region_peers(region).await;
        let tier = move_tier(region, rf, &departing, &unhealthy);
        // Paced on the sender, chunk by chunk, at an even share of the
        // cluster-wide cap. This was a per-tick allowance checked only before
        // a move started; one region larger than the allowance spent it, so
        // balance moves ran one per tick however long the tick took.
        let rate = move_rate_bytes_per_sec(
            tier,
            self.config.movement_budget_bytes_per_sec,
            self.config.schedule_limit,
        );
        match step {
            PlacementStep::Add(_) => {
                if !self.config.add_peer_proposals_enabled {
                    return Ok(false);
                }
                // Every target member the region is missing, in target order —
                // not just the one `next_placement_step` named. All of them
                // belong in the region, so any may be staged first, and
                // committing to one before probing let a single lagging node
                // block the region forever (review findings #9/#10).
                let missing: Vec<(u64, String)> = {
                    let m = self.membership.read().await;
                    target
                        .iter()
                        .filter(|t| !region.peers.contains(t))
                        .filter(|t| {
                            tier != MoveTier::Convergence
                                || learners_on(&map, **t) < MAX_BALANCE_LEARNERS_PER_NODE
                        })
                        .filter_map(|t| m.get_node(*t).map(|n| (*t, n.addr.clone())))
                        .collect()
                };
                let Some(to_peer) = self
                    .first_caught_up(region.region_id, map.map_version, &missing)
                    .await
                else {
                    return Ok(false);
                };
                // Commit membership FIRST, as a learner, then copy.
                //
                // Staging before committing leaves a gap: the peer is not a
                // replication target until AddPeer applies, so every write
                // landing between the snapshot and the commit reaches the
                // primary alone. A later TransferPrimary then hands primacy to
                // a node missing exactly those writes, and since aggregates use
                // PrimaryRead they simply vanish. Measured at 628 lost writes
                // in one 3-minute run, concentrated at the end when
                // convergence was busiest.
                //
                // The reason it was written that way is real -- a committed
                // peer holding no rows gets used by scatter and returns silent
                // empty results -- and `staging_peers` is what resolves it: the
                // learner is a member, so replication flows to it immediately,
                // but `resolve_region_peers` refuses it until it is promoted.
                //
                // This is TiKV's AddLearner -> snapshot -> PromoteLearner.
                self.propose(ShardMapOp::AddPeer {
                    key: key.clone(),
                    region_id: region.region_id,
                    to_peer,
                    epoch: region.epoch,
                    staging: true,
                })
                .await?;
                counter!("hyperbytedb_shard_learners_added_total").increment(1);

                // Re-read: AddPeer bumped the epoch, and PromotePeer must carry
                // the current one.
                let after_add = self.shard_map.snapshot().await?;
                let Some(after_region) = current_region(&after_add, key, region.region_id) else {
                    return Ok(true);
                };
                self.stage_region_onto(key, after_region, to_peer, rate)
                    .await?;

                let promoted = self.shard_map.snapshot().await?;
                let Some(current) = current_region(&promoted, key, region.region_id) else {
                    // Region vanished mid-flight; the learner is harmless -- it
                    // receives writes and serves nothing.
                    return Ok(true);
                };
                self.propose(ShardMapOp::PromotePeer {
                    key: key.clone(),
                    region_id: region.region_id,
                    peer: to_peer,
                    epoch: current.epoch,
                })
                .await?;
                counter!("hyperbytedb_shard_learners_promoted_total").increment(1);
            }
            PlacementStep::Promote(new_primary) => {
                // Same watermark guard the emergency failover path has used all
                // along -- planned promotion needs it just as much, and did not
                // have it.
                //
                // Replication is asynchronous, so a peer that joined recently
                // can trail the primary. Handing it primacy strands whatever it
                // has not received yet, and because aggregates resolve with
                // PrimaryRead those rows simply stop being counted. That is the
                // residual this suite kept measuring after the learner ordering
                // closed the join gap: 123 of 112980.
                // Not while evacuating. A node being decommissioned must hand
                // its primaries off, and a peer populated by bulk transfer
                // legitimately has NO WAL entries for the region -- watermark
                // 0 -- so this guard reads it as infinitely behind and defers
                // forever. That deadlocked decommission: the departing node
                // sat in `decommissioning` until the suite timed out, with
                // decommission_completed_total stuck at 0.
                //
                // The guard is a rebalancing heuristic; evacuation outranks it.
                let evacuating = departing.contains(&region.primary);
                if evacuating {
                    tracing::info!(
                        region_id = region.region_id,
                        new_primary,
                        departing_primary = region.primary,
                        "promoting despite watermark guard: primary is departing"
                    );
                }
                if !evacuating && self.peer_client.is_some() {
                    let progress = self.region_progress_for_peers(region).await;
                    let candidate_p = progress.get(&new_primary).copied().flatten();
                    let best_p = progress.values().copied().flatten().max();
                    if !promote_progress_safe(
                        candidate_p,
                        best_p,
                        self.config.promote_min_progress_pct,
                    ) {
                        counter!(
                            "hyperbytedb_shard_promote_skipped_total",
                            "reason" => "region_progress"
                        )
                        .increment(1);
                        tracing::info!(
                            region_id = region.region_id,
                            new_primary,
                            candidate_progress = ?candidate_p,
                            best_progress = ?best_p,
                            "planned promotion deferred: candidate behind"
                        );
                        return Ok(false);
                    }
                }
                self.propose(ShardMapOp::TransferPrimary {
                    key: key.clone(),
                    region_id: region.region_id,
                    new_primary,
                    epoch: region.epoch,
                })
                .await?;
            }
            PlacementStep::Remove(from_peer) => {
                if !self.config.remove_peer_proposals_enabled {
                    return Ok(false);
                }
                self.propose(ShardMapOp::RemovePeer {
                    key: key.clone(),
                    region_id: region.region_id,
                    from_peer,
                    epoch: region.epoch,
                })
                .await?;
            }
        }
        self.clear_placement_stall(region.region_id).await;
        counter!("hyperbytedb_shard_placements_total").increment(1);
        Ok(true)
    }

    /// Copy a region's rows onto `to_peer`, from whichever node holds them.
    ///
    /// Staging always precedes the map commit. Heal's commit-then-stage
    /// ordering would publish a peer holding no rows, and scatter reads fall
    /// back to replicas, so that surfaces as silent empty results rather than
    /// an error.
    /// Returns the number of points staged, for movement-budget accounting.
    /// The remote branch reports 0: the source node accounts for those bytes,
    /// not the leader that asked for the transfer.
    /// Copy `region` onto `to_peer`, paced at `max_bytes_per_sec` (0 = unpaced).
    async fn stage_region_onto(
        &self,
        key: &MeasurementKey,
        region: &ShardRegion,
        to_peer: u64,
        max_bytes_per_sec: u64,
    ) -> Result<u64, HyperbytedbError> {
        let Some(pc) = self.peer_client.as_ref() else {
            return Err(HyperbytedbError::ShardMap("no peer client".into()));
        };
        if region.primary == self.node_id {
            let outcome = stage_region_transfer_data(
                pc,
                &self.metadata,
                &self.wal,
                self.query_port.as_ref(),
                self.node_id,
                key,
                region,
                to_peer,
                self.max_points_per_request,
                max_bytes_per_sec,
            )
            .await?;
            if !outcome.verified() {
                return Err(HyperbytedbError::ShardMap(
                    format!(
                        "placement stage unverified: exported={} applied={}",
                        outcome.exported, outcome.applied
                    )
                    .into(),
                ));
            }
            Ok(outcome.bytes)
        } else {
            // The remote staging path had no failure signal whatsoever: no
            // counter, and its only error log was the `debug!` in the tick. A
            // leader failing to stage onto every region's primary was silent.
            match request_region_stage(
                pc.as_ref(),
                &self.membership,
                region.primary,
                key,
                region,
                to_peer,
                max_bytes_per_sec,
            )
            .await
            {
                Ok(staged_bytes) => {
                    counter!("hyperbytedb_shard_remote_stage_total").increment(1);
                    Ok(staged_bytes)
                }
                Err(e) => {
                    counter!("hyperbytedb_shard_remote_stage_failures_total").increment(1);
                    Err(e)
                }
            }
        }
    }

    /// First candidate whose shard map has caught up to `cluster_ver`.
    ///
    /// Walks candidates rather than committing to one up front: choosing
    /// before probing let a single lagging node block every region forever.
    /// A persistent stall warns and increments
    /// `hyperbytedb_shard_placement_stalled_total`.
    async fn first_caught_up(
        &self,
        region_id: u64,
        cluster_ver: u64,
        candidates: &[(u64, String)],
    ) -> Option<u64> {
        let pc = self.peer_client.as_ref()?;
        // Preserve the original accounting: no candidates is not a stall.
        if candidates.is_empty() {
            self.clear_placement_stall(region_id).await;
            return None;
        }
        for (id, addr) in candidates {
            let Some((ver, probed_against)) = self
                .cached_peer_map_version(*id, addr, cluster_ver, pc.http_client())
                .await
            else {
                continue;
            };
            // `probed_against`, not `cluster_ver`: the probe answers for the
            // map version that was current when it was taken. A placement that
            // committed later in this same tick must not retroactively make a
            // caught-up peer look behind.
            if joiner_map_caught_up(probed_against, ver) {
                self.clear_placement_stall(region_id).await;
                return Some(*id);
            }
            tracing::debug!(
                region_id,
                candidate = id,
                cluster_ver,
                probed_against,
                candidate_ver = ver,
                "candidate map not caught up"
            );
        }
        self.note_placement_stall(region_id, candidates.len()).await;
        None
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
        if self.peer_client.is_none() {
            return Ok(());
        }

        let now = chrono::Utc::now().timestamp();
        let dead_timeout = self.config.dead_node_timeout_secs;
        let dead_peer = {
            let m = self.membership.read().await;
            region.peers.iter().copied().find(|id| {
                if *id == region.primary {
                    return false;
                }
                match m.get_node(*id) {
                    None => true,
                    Some(n) if is_departing(n) || n.state == NodeState::Draining => false,
                    Some(n) if n.state == NodeState::Decommissioning => false,
                    Some(n) if n.state == NodeState::Active => false,
                    Some(n) => peer_disconnected_past_deadline(n, now, dead_timeout),
                }
            })
        };
        let Some(dead_peer) = dead_peer else {
            return Ok(());
        };

        if !self.config.remove_peer_proposals_enabled {
            return Ok(());
        }

        // A dead peer is dropped first, not swapped. There is no live source to
        // stage from, so preserving its membership only delays RF restoration —
        // the deliberate asymmetry with drain, which has a live source and so
        // restores before it removes.
        //
        // RemovePeer refuses the primary, so a dead primary is left to
        // `try_failover_unhealthy_primary`, which runs immediately after this
        // in the tick; the peer is removed on a later pass once it no longer
        // owns the region.
        if region.primary == dead_peer {
            tracing::debug!(
                region_id = region.region_id,
                dead_peer,
                "dead peer is the primary; deferring to failover before removal"
            );
            return Ok(());
        }

        tracing::info!(
            region_id = region.region_id,
            dead_peer,
            "removing dead region peer; convergence will restore RF"
        );
        self.propose(ShardMapOp::RemovePeer {
            key: key.clone(),
            region_id: region.region_id,
            from_peer: dead_peer,
            epoch: region.epoch,
        })
        .await?;
        counter!("hyperbytedb_shard_peer_heals_total").increment(1);
        // The region is now below effective RF, so convergence classifies it
        // RfViolation — top tier, exempt from the movement budget — and stages
        // a replacement chosen by the placement function rather than one picked
        // here that the hash may disagree with.
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

/// What one move holds in memory at once: a transfer chunk, never the whole
/// region. An estimate, costed like the heartbeat's region size (128 B a row)
/// times the four copies a chunk exists as: export text and decoded points on
/// the sender, the line-protocol body, and the receiver's parse.
const MOVE_INFLIGHT_BYTES: u64 = TRANSFER_CHUNK_POINTS as u64 * 128 * 4;

/// MiB permits one move takes from the mover pool's in-flight budget.
fn mover_inflight_permits(approx_bytes: u64, budget_mib: usize) -> usize {
    ((approx_bytes.min(MOVE_INFLIGHT_BYTES) / (1024 * 1024)).max(1) as usize).min(budget_mib)
}

/// Data-aware failover guard: the candidate must hold a caught-up copy.
///
/// `max_peer_watermark` is the highest WAL watermark observed across all
/// region peers (unreachable peers report 0, so it reflects living nodes).
/// Blocking failover when every known watermark is behind keeps reads routed
/// at the node holding the data instead of silently returning empty results.
/// How many regions a single measurement may hold. Scales with the cluster so
/// the cap stops being what decides balance — 128 regions over 50 nodes is a
/// placement straitjacket — while `configured_floor` keeps a one-node dev
/// cluster bounded. Splits are metadata-only, so a region costs a map entry and
/// a heartbeat row, not a copy.
#[must_use]
fn region_cap(configured_floor: usize, candidates: usize) -> usize {
    configured_floor.max(candidates.saturating_mul(REGIONS_PER_CANDIDATE).max(1))
}

/// Emergency failover: the primary is already gone, so the only question is
/// whether this candidate is *known* to hold nothing.
///
/// Was `candidate > 0 || max == 0`, which conflated three different facts into
/// the number 0 — "holds nothing", "could not be probed", and (under
/// per-region reporting) "holds this region but it has had no writes". A node
/// that primaried nothing reported 0 and was excluded from promotion forever;
/// that is `promote_skipped_total{watermark_lag}` on the kind cluster.
///
/// The guard's real purpose is narrow and worth keeping: do not hand primacy
/// to a replica that demonstrably never received the rows, because reads would
/// then return silently empty rather than failing. So it blocks exactly that
/// case and nothing else.
///
/// Lag alone does not block here. The region is already unserved, replication
/// is asynchronous so every replica lags, and refusing to promote a
/// behind-but-populated replica trades a degraded region for an unavailable
/// one. An unknown position also allows: it cannot prove the candidate is
/// empty, and the region has no better option.
#[must_use]
fn failover_watermark_safe(candidate: Option<u64>, best: Option<u64>) -> bool {
    !(candidate == Some(0) && best.is_some_and(|b| b > 0))
}

/// Planned promotion during rebalancing, where the current primary is alive
/// and there is no urgency — so the bar is higher than for failover.
///
/// An unknown progress defers rather than proceeds: nothing is broken while we
/// wait, and promoting a node we could not measure is the one move that can
/// silently strand data.
///
/// The bound is *relative*, not an absolute row count, because heartbeats are
/// sampled on an interval. Under continuous writes a perfectly healthy replica
/// is always some thousands of series behind whatever the primary reported a
/// moment ago, so any fixed allowance either blocks everything or permits
/// everything depending on write rate. A percentage floor asks the question
/// the guard actually cares about — "is this replica broadly keeping up, or is
/// it essentially empty?" — at any rate.
#[must_use]
fn promote_progress_safe(candidate: Option<u64>, best: Option<u64>, min_pct: u64) -> bool {
    let Some(candidate) = candidate else {
        return false;
    };
    let Some(best) = best else {
        return true;
    };
    if best == 0 {
        return true;
    }
    candidate.saturating_mul(100) >= best.saturating_mul(min_pct.min(100))
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
#[allow(dead_code)]
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
        region_id: range.region_id,
        epoch: range.epoch,
        dest_primary,
        drop_source,
        stage: false,
        max_bytes_per_sec: 0,
        dest_skip_wal_through: 0,
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
        let status = resp.status().as_u16();
        return Err(HyperbytedbError::TransferRejected {
            status,
            reason: crate::application::shard_transfer::rejection_reason(resp).await,
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
    max_bytes_per_sec: u64,
) -> Result<u64, HyperbytedbError> {
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
        region_id: range.region_id,
        epoch: range.epoch,
        dest_primary: dest,
        drop_source: false,
        stage: true,
        max_bytes_per_sec,
        // Accepted and ignored by the receiver; retained only for wire compat.
        dest_skip_wal_through: 0,
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
        let status = resp.status().as_u16();
        return Err(HyperbytedbError::TransferRejected {
            status,
            reason: crate::application::shard_transfer::rejection_reason(resp).await,
        });
    }
    let body: serde_json::Value = resp
        .json()
        .await
        .map_err(|e| HyperbytedbError::PeerUnreachable(e.to_string()))?;
    // Bytes, not points: the budget is a bandwidth throttle. An older peer that
    // does not report `bytes` contributes 0 rather than a point count —
    // under-charging is a convergence-rate cost, while charging points as if
    // they were bytes is a correctness one.
    Ok(body.get("bytes").and_then(|v| v.as_u64()).unwrap_or(0))
}

/// Reconciliation disposition for a failed post-commit transfer movement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub(crate) enum TransferDisposition {
    /// Transient (transport, apply lag 404/409, 5xx): retry next drain.
    Retryable,
    /// Source/destination identity collapsed or moved (400 dest-is-self,
    /// collision): re-resolve roles and retry.
    Reresolve,
    /// Nothing to retry (logic error): park with alarm.
    Park,
}

#[allow(dead_code)]
pub(crate) fn classify_transfer_error(err: &HyperbytedbError) -> TransferDisposition {
    match err {
        HyperbytedbError::PeerUnreachable(_) | HyperbytedbError::ReplicationTimeout(_) => {
            TransferDisposition::Retryable
        }
        HyperbytedbError::TransferRejected { status, .. } => match status {
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
        // A learner cannot be made primary: `TransferPrimary` apply rejects it
        // outright. Offering one here spent the failover on a proposal that
        // could only fail, and the region stayed unserved.
        .filter(|id| region.is_serving_peer(*id))
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
        let wms = futures::future::join_all(candidates.iter().map(|&candidate| async move {
            (
                candidate,
                peer_region_wal_watermark(pc, candidate, region.region_id).await,
            )
        }))
        .await;
        let mut best = candidates[0];
        let mut best_wm: Option<u64> = None;
        for (candidate, wm) in wms {
            // `None` never displaces a known position: a node we could not ask
            // is not evidence of progress.
            let better = match (wm, best_wm) {
                (Some(_), None) => true,
                (Some(w), Some(b)) => w > b || (w == b && candidate == self_id),
                (None, _) => false,
            };
            if better {
                best_wm = wm;
                best = candidate;
            }
        }
        return Some(best);
    }

    pick_alternate_primary(membership, region, self_id).await
}

/// The peer's replication position for `region_id`, or `None` when the peer
/// could not be asked.
///
/// `None` and `Some(0)` are different facts and must stay different: "I could
/// not reach this node" is not "this node has applied nothing". Collapsing
/// them let one timed-out probe read as a data-less candidate, which the
/// promotion guard then acted on.
async fn peer_region_wal_watermark(
    peer_client: &PeerClient,
    peer_id: u64,
    region_id: u64,
) -> Option<u64> {
    let membership = peer_client.membership().read().await;
    let addr = membership.get_node(peer_id)?.addr.clone();
    drop(membership);

    let url = format!("http://{addr}/internal/sync/manifest");
    let Ok(resp) = peer_client.http_client().get(&url).send().await else {
        return None;
    };
    let Ok(body) = resp.text().await else {
        return None;
    };
    let Ok(manifest) = serde_json::from_str::<crate::domain::cluster::sync::SyncManifest>(&body)
    else {
        return None;
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
        Some(wm) => Some(wm),
        // No watermark for this region: fall back to the node-wide sequence
        // ONLY when the peer reports no region watermarks at all (fresh or
        // legacy node). Otherwise absence means this node holds no data for
        // the region — reporting its global seq would wrongly bless a
        // data-less candidate as caught-up during failover.
        None if total_region_watermarks == 0 => Some(manifest.wal_last_seq),
        // Answered, and holds nothing for this region. A real zero.
        None => Some(0),
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

#[allow(dead_code)]
fn now_unix() -> i64 {
    chrono::Utc::now().timestamp()
}

impl ShardScheduler {
    /// Rebuild reconciliation entries from the committed map scan: any
    /// flagged child without an in-memory entry gets one. Makes leader change
    /// and process restart equivalent — durable intent lives in the map.
    #[allow(dead_code)]
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
    #[allow(dead_code)]
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
    #[allow(dead_code)]
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
                // Unknown counts as "no proof of coverage" here, same as a
                // real zero: this path is choosing a source to copy FROM, so
                // the conservative answer is to skip it.
                let wm = peer_region_wal_watermark(pc, source, region.region_id).await;
                if wm.unwrap_or(0) == 0 {
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
                // `region.is_serving()`, not `true`: a staging region's range is
                // still served by its donor, so the donor must keep its rows
                // until cutover. Dropping first is what left 13 staging regions
                // serving from a donor that had already deleted their rows.
                // After activation the donor's leftover rows are inert -- every
                // region query injects a `series_id` range predicate -- and are
                // reclaimed by a later GC.
                request_region_rehome(
                    pc,
                    &self.membership,
                    source,
                    key,
                    region,
                    dest,
                    region.is_serving(),
                )
                .await
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
    #[allow(dead_code)]
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

    #[allow(dead_code)]
    fn remove_reconciliation(&self, key: &MeasurementKey, region_id: u64) {
        let mut q = self
            .reconciliation_queue
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        q.retain(|e| !(e.key == *key && e.region_id == region_id));
    }

    #[allow(dead_code)]
    fn pending_transfers(&self) -> Vec<PendingTransfer> {
        self.reconciliation_queue
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone()
    }
}

#[allow(dead_code)]
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
        && region.is_serving_peer(peer_id)
        && membership
            .get_node(peer_id)
            .is_some_and(|n| n.state == NodeState::Active)
}

/// An active learner holding this region, when no serving peer is left.
///
/// A region whose primary dies leaving only a learner has no write or read
/// target at all (`resolve_region_peers` refuses a learner), and failover had
/// no candidate to offer, so it stalled indefinitely. The learner has been
/// receiving replication since `AddPeer`, so promoting it is the difference
/// between a degraded region and an unavailable one.
async fn pick_learner_survivor(
    membership: &SharedMembership,
    region: &ShardRegion,
    self_id: u64,
) -> Option<u64> {
    let m = membership.read().await;
    region
        .staging_peers
        .iter()
        .copied()
        .filter(|id| *id != region.primary)
        .find(|id| {
            *id == self_id
                || m.get_node(*id)
                    .is_some_and(|n| n.state == NodeState::Active)
        })
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
            consecutive_misses: 0,
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
            state: Default::default(),
            staged_from: None,
            staging_peers: Vec::new(),
        };
        let hb = vec![
            (1, 1, 0, 0, 0, 0),
            (1, 2, 130, 0, 42, 0),
            (2, 3, 999, 0, 0, 0),
        ];
        assert_eq!(max_region_peer_heartbeat_stats(&hb, &region), (130, 42));
    }

    #[test]
    fn unsplit_region_waits_for_first_split_stamp_until_above_max_series() {
        assert!(region_ready_to_split(11, 0, 1_000_000, 5, 10, 30));
        assert!(!region_ready_to_split(6, 0, 1_000_000, 5, 10, 30));
    }

    #[test]
    fn split_fires_immediately_above_max_series() {
        assert!(region_ready_to_split(1_000, 100, 120, 5, 10, 30));
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
            state: Default::default(),
            staged_from: None,
            staging_peers: Vec::new(),
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
            state: Default::default(),
            staged_from: None,
            staging_peers: Vec::new(),
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
            state: Default::default(),
            staged_from: None,
            staging_peers: Vec::new(),
        };

        assert_eq!(pick_alternate_primary(&membership, &region, 99).await, None);
    }

    #[test]
    fn failover_watermark_guard_blocks_dataless_candidates() {
        // Caught-up candidate: safe.
        assert!(failover_watermark_safe(Some(100), Some(100)));
        // Behind but holding data: allowed — async watermarks always lag and
        // this is still the best living copy.
        assert!(failover_watermark_safe(Some(50), Some(100)));
        // Far behind but populated: still allowed. The region is already
        // unserved; a degraded primary beats no primary.
        assert!(failover_watermark_safe(Some(1), Some(5_000_000)));
        // Candidate has no data while some peer does: blocked — this is the
        // post-split strand where the new primary never received the rows.
        assert!(!failover_watermark_safe(Some(0), Some(42)));
        // Nobody has data yet (fresh region): allow.
        assert!(failover_watermark_safe(Some(0), Some(0)));
        // Unknown is not zero. A probe we could not complete must not read as
        // "holds nothing" — that is what excluded healthy nodes permanently.
        assert!(failover_watermark_safe(None, Some(42)));
        assert!(failover_watermark_safe(Some(7), None));
    }

    /// Planned promotion is the unhurried case, so it is allowed to be strict
    /// where failover cannot be: the current primary is alive, nothing is
    /// broken while we wait, and both "too far behind" and "could not measure"
    /// are reasons to defer rather than risk stranding rows.
    #[test]
    fn planned_promotion_defers_where_failover_proceeds() {
        assert!(promote_progress_safe(Some(100), Some(100), 50));
        assert!(promote_progress_safe(Some(60), Some(100), 50));
        assert!(
            !promote_progress_safe(Some(1), Some(5_000_000), 50),
            "a materially behind replica must not be handed primacy by choice"
        );
        assert!(
            !promote_progress_safe(None, Some(100), 50),
            "an unmeasurable candidate must defer, not proceed"
        );
        assert!(
            promote_progress_safe(Some(5), None, 50),
            "nothing known to be ahead: allow"
        );
        // The two predicates deliberately disagree on the same inputs; that
        // difference is the design, not an oversight.
        assert!(failover_watermark_safe(Some(1), Some(5_000_000)));
        assert!(failover_watermark_safe(None, Some(100)));
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
            state: Default::default(),
            staged_from: None,
            staging_peers: Vec::new(),
        }
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
            state: Default::default(),
            staged_from: None,
            staging_peers: Vec::new(),
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
            state: Default::default(),
            staged_from: None,
            staging_peers: Vec::new(),
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

    /// A split moves no data: both children inherit the parent's placement.
    ///
    /// TiKV-style. `exec_batch_split` clones the parent's peers into each new
    /// region and copies no KV data, because a region's placement IS its Raft
    /// group's stores. Re-placing the right child at split time is what forced
    /// a bulk copy, which needed a staging state and a cutover, and the writes
    /// that landed on the donor in between were never forwarded -- 33 lost per
    /// run, about one Raft round-trip of ingest.
    ///
    /// Here the donor (node 9) is not in membership, so any HRW re-placement
    /// would have to move the child somewhere reachable. It must not: the child
    /// keeps node 9 as primary, carries the parent's peers, and is immediately
    /// routable. Moving it to its hash target is convergence's job.
    #[tokio::test]
    #[serial_test::serial(chdb)]
    async fn a_split_moves_no_data_and_both_children_serve() {
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
        cluster_cfg.cluster_addr = "127.0.0.1:18097".into();
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
        // Donor is node 9, which is not in membership and so cannot be asked
        // to stage; node 1 is the only placement candidate.
        let region = ShardRegion {
            region_id: 1,
            start: 0,
            end: u64::MAX,
            epoch: ShardEpoch::default(),
            peers: vec![9, 1],
            primary: 9,
            last_split_at: 1,
            transfer_verified: None,
            transfer_first_seen: None,
            state: Default::default(),
            staged_from: None,
            staging_peers: Vec::new(),
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
        let split = captured
            .iter()
            .find_map(|op| match op {
                ShardMapOp::Split { right, .. } => Some(right),
                _ => None,
            })
            .expect("the split must still happen");

        assert_eq!(
            split.primary, 9,
            "the child must inherit the parent's primary -- re-placing it here \
             is what forced a copy: {split:?}"
        );
        assert_eq!(
            split.peers,
            vec![9, 1],
            "the child must inherit the parent's peers: {split:?}"
        );
        assert!(
            split.is_serving(),
            "a split moves no data, so the child is routable immediately: {split:?}"
        );
        assert_eq!(split.staged_from, None, "nothing is staged by a split");
        assert!(
            !split.transfer_outstanding(),
            "a split creates no transfer debt: {split:?}"
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
            classify_transfer_error(&TransferRejected {
                status: 404,
                reason: "test".into()
            }),
            TransferDisposition::Retryable
        );
        assert_eq!(
            classify_transfer_error(&TransferRejected {
                status: 409,
                reason: "test".into()
            }),
            TransferDisposition::Retryable
        );
        assert_eq!(
            classify_transfer_error(&TransferRejected {
                status: 503,
                reason: "test".into()
            }),
            TransferDisposition::Retryable
        );
        assert_eq!(
            classify_transfer_error(&TransferRejected {
                status: 400,
                reason: "test".into(),
            }),
            TransferDisposition::Reresolve
        );
        assert_eq!(
            classify_transfer_error(&TransferCollision),
            TransferDisposition::Reresolve
        );
        assert_eq!(
            classify_transfer_error(&TransferRejected {
                status: 403,
                reason: "test".into(),
            }),
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

    #[test]
    fn split_key_prefers_the_peer_median_and_stays_inside_the_range() {
        let (s, e) = (1_000u64, 2_000u64);
        assert_eq!(choose_split_key(s, e, Some(1_600), Some(1_010)), 1_600);
        // A hint outside the range (stale, from before a split) is ignored.
        assert_eq!(choose_split_key(s, e, Some(5_000), Some(1_400)), 1_400);
        assert_eq!(choose_split_key(s, e, Some(s), None), 1_500);
        assert_eq!(choose_split_key(s, e, None, None), 1_500);
        assert_eq!(choose_split_key(0, 2, None, None), 1);
    }

    /// Through the scheduler: the leader (node 99) is not a region peer, so
    /// its own index must not pick the split point. At N=6 on n3 it did, and
    /// a 225k-series region split 20+ times into slivers.
    #[tokio::test]
    #[serial_test::serial(chdb)]
    async fn a_non_peer_leader_splits_on_the_peer_reported_median() {
        let harness = ReconcileTestHarness::new().await;
        let key = MeasurementKey::new("db", "autogen", "cpu");
        let mut region = flagged_test_region(1, now_unix());
        region.start = 0;
        region.end = u64::MAX;
        region.peers = vec![1, 2];
        region.primary = 1;
        let hint = u64::MAX / 3;
        let s = &harness.scheduler;
        s.record_heartbeat(1, 1, 225_000, 0, 0).await;
        s.record_heartbeat(1, 2, 224_000, 0, 0).await;
        s.record_split_hint(1, 1, Some(hint)).await;
        s.record_split_hint(1, 2, Some(u64::MAX / 4)).await;

        let got = s.compute_split_key(&key, &region).await.unwrap();
        assert_eq!(
            got, hint,
            "must use the hint from the peer with the most series"
        );
    }

    #[tokio::test]
    #[serial_test::serial(chdb)]
    async fn reconciliation_queue_is_inert_after_metadata_only_splits() {
        let harness = ReconcileTestHarness::new().await;
        let key = MeasurementKey::new("db", "autogen", "cpu");
        let old = now_unix() - RECONCILE_MIN_AGE_SECS - 10;
        harness
            .shard_map
            .apply_op(ShardMapOp::BootstrapMeasurement {
                key: key.clone(),
                region: flagged_test_region(1, old),
            })
            .await
            .unwrap();

        for _ in 0..=RECONCILE_PARK_AFTER_TICKS {
            harness.scheduler.tick_once_for_test().await.unwrap();
        }

        let q = harness.scheduler.reconciliation_queue.lock().unwrap();
        assert!(
            q.is_empty(),
            "legacy reconciliation drain is disabled; flagged children are not enqueued"
        );
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

    /// Like [`spawn_mock_peer`], but answers every probe from `version` as it
    /// stands at that moment — a peer that keeps up with the cluster rather
    /// than one frozen at a fixed number.
    async fn spawn_tracking_mock_peer(
        version: Arc<std::sync::atomic::AtomicU64>,
    ) -> (String, Arc<std::sync::atomic::AtomicU64>) {
        use axum::routing::get;
        let probes = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let probe_counter = probes.clone();
        let app = axum::Router::new().route(
            "/internal/shard/map",
            get(move || {
                let probes = probe_counter.clone();
                let version = version.clone();
                async move {
                    probes.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    axum::Json(serde_json::json!({
                        "map_version": version.load(std::sync::atomic::Ordering::Relaxed),
                        "next_region_id": 2,
                        "spaces": [],
                    }))
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        (addr, probes)
    }

    /// A peer that is reachable but cannot answer — the cheap, deterministic
    /// stand-in for an unreachable one. `fetch_shard_map` turns any non-2xx
    /// into an error on the same path a connect failure takes, and unlike a
    /// closed port it can count how many times it was asked.
    async fn spawn_failing_mock_peer() -> (String, Arc<std::sync::atomic::AtomicU64>) {
        use axum::routing::get;
        let probes = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let probe_counter = probes.clone();
        let app = axum::Router::new().route(
            "/internal/shard/map",
            get(move || {
                let probes = probe_counter.clone();
                async move {
                    probes.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    axum::http::StatusCode::INTERNAL_SERVER_ERROR
                }
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
            state: Default::default(),
            staged_from: None,
            staging_peers: Vec::new(),
        }
    }

    /// Harness whose scheduler owns the region and can actually reach a peer,
    /// so `tick()` runs the placement steps end to end.
    struct PlacementTestHarness {
        _dir: tempfile::TempDir,
        scheduler: Arc<ShardScheduler>,
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
            Self::new_tuned(add_peer_proposals_enabled, peers, regions, |_| {}).await
        }

        async fn new_tuned(
            add_peer_proposals_enabled: bool,
            peers: &[(u64, i64)],
            regions: usize,
            tune: impl FnOnce(&mut crate::config::ShardingConfig),
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
                    consecutive_misses: 0,
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
                        consecutive_misses: 0,
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

            let mut sharding = crate::config::ShardingConfig {
                replication_factor: 3,
                add_peer_proposals_enabled,
                ..Default::default()
            };
            tune(&mut sharding);
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
                scheduler: Arc::new(scheduler),
                proposals,
                probes,
            }
        }

        /// Unflushed rows for measurement `cpu{measurement}`, one series each,
        /// so a move of its region has `points` lines to push.
        async fn write_points(&self, measurement: usize, points: usize) {
            use crate::domain::point::{FieldValue, Point};
            let points = (0..points)
                .map(|i| Point {
                    measurement: format!("cpu{measurement}"),
                    tags: [("host".to_string(), format!("h{i}"))].into(),
                    fields: [("v".to_string(), FieldValue::Float(1.0))].into(),
                    timestamp: 1_000_000_000 + i as i64,
                })
                .collect();
            self.scheduler
                .wal
                .append(crate::ports::wal::WalEntry {
                    database: "db".into(),
                    retention_policy: "autogen".into(),
                    points,
                    origin_node_id: 0,
                })
                .await
                .unwrap();
        }

        /// The measurement `new(.., regions: 1)` bootstraps: `cpu0`.
        fn key() -> MeasurementKey {
            MeasurementKey::new("db", "autogen", "cpu0")
        }

        async fn region(&self) -> ShardRegion {
            self.scheduler
                .shard_map
                .snapshot()
                .await
                .unwrap()
                .spaces
                .get(&Self::key())
                .and_then(|s| s.regions.first().cloned())
                .expect("harness bootstrapped one region")
        }

        async fn add_peer_to_region(&self, peer: u64) {
            let region = self.region().await;
            self.scheduler
                .shard_map
                .apply_op(ShardMapOp::AddPeer {
                    key: Self::key(),
                    region_id: region.region_id,
                    to_peer: peer,
                    epoch: region.epoch,
                    staging: false,
                })
                .await
                .unwrap();
        }

        async fn set_node_state(&self, node_id: u64, state: NodeState) {
            let mut m = self.scheduler.membership.write().await;
            m.set_state(node_id, state);
        }

        async fn space_regions(&self, key: &MeasurementKey) -> Vec<ShardRegion> {
            self.scheduler
                .shard_map
                .snapshot()
                .await
                .unwrap()
                .spaces
                .get(key)
                .map(|s| s.regions.clone())
                .unwrap_or_default()
        }

        fn probe_count(&self, peer: u64) -> u64 {
            self.probes[&peer].load(std::sync::atomic::Ordering::Relaxed)
        }
    }

    /// Six regions on node 1 at RF 1 with node 2 in the cluster. Moving one
    /// to node 2 is balance only. `seed_learners` regions already have node 2
    /// as a learner. Returns the AddPeer-to-2 proposals of one tick.
    async fn balance_adds_to_node_2(seed_learners: usize) -> usize {
        // Node 2 reports a map ahead of the seeds below, so it counts as
        // caught up and only the cap can stop an AddPeer to it.
        let harness = PlacementTestHarness::new_tuned(true, &[(2, 16)], 6, |c| {
            c.replication_factor = 1;
        })
        .await;
        for i in 0..seed_learners {
            let mut seed = sample_region_peers(vec![1, 2], 1);
            seed.region_id = 9_000 + i as u64;
            seed.staging_peers = vec![2];
            harness
                .scheduler
                .shard_map
                .apply_op(ShardMapOp::BootstrapMeasurement {
                    key: MeasurementKey::new("db", "autogen", format!("learning{i}")),
                    region: seed,
                })
                .await
                .unwrap();
        }
        harness.scheduler.tick_once_for_test().await.unwrap();
        harness
            .proposals
            .lock()
            .unwrap()
            .iter()
            .filter(|op| matches!(op, ShardMapOp::AddPeer { to_peer: 2, .. }))
            .count()
    }

    #[tokio::test]
    #[serial_test::serial(chdb)]
    async fn balance_moves_a_region_onto_an_empty_node() {
        assert!(balance_adds_to_node_2(0).await > 0);
    }

    /// Balance may not pile learners onto one node: at N=12 on n3 one node
    /// was sent 17 at once and paced the whole cluster.
    #[tokio::test]
    #[serial_test::serial(chdb)]
    async fn balance_moves_stop_at_the_learner_cap_for_a_node() {
        assert_eq!(
            balance_adds_to_node_2(MAX_BALANCE_LEARNERS_PER_NODE).await,
            0,
            "node 2 is at its learner cap"
        );
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

    /// A placement that commits partway through a tick advances the cluster's
    /// `map_version`, but `peer_map_versions` still holds the probe taken
    /// before that commit. Every region processed later in the same tick then
    /// compares the *new* cluster version against the *old* probe and rejects
    /// a peer that is perfectly caught up.
    ///
    /// That is the stall signature that cannot be explained from outside: by
    /// the time anyone samples, every node's `/internal/shard/map` reports the
    /// same version and every probe returns 200 — yet `first_caught_up`
    /// returned `None` and the region was recorded as having "no caught-up
    /// replica candidate". The probe must be judged against the map version it
    /// was taken against, not one this tick moved underneath it.
    #[tokio::test]
    #[serial_test::serial(chdb)]
    async fn a_commit_mid_tick_must_not_reject_a_peer_that_kept_up() {
        let harness = PlacementTestHarness::new(true, &[(2, 0)], 2).await;
        let sched = &harness.scheduler;

        let reported = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let (addr, probes) = spawn_tracking_mock_peer(reported.clone()).await;
        {
            let mut m = sched.membership.write().await;
            m.nodes
                .get_mut(&2)
                .expect("harness put peer 2 in membership")
                .addr = addr.clone();
        }

        // Start of a tick: the probe cache is cleared, exactly as `tick` does.
        sched.peer_map_versions.write().await.clear();
        let before = sched.shard_map.snapshot().await.unwrap().map_version;
        reported.store(before, std::sync::atomic::Ordering::Relaxed);

        // First region of the tick: peer 2 is caught up, so it is chosen and
        // its version is cached for the rest of the tick.
        assert_eq!(
            sched.first_caught_up(1, before, &[(2, addr.clone())]).await,
            Some(2),
            "a caught-up peer must be accepted"
        );

        // That placement commits. The cluster map version advances, and the
        // peer applies the same op, so it is still caught up.
        let region = harness.region().await;
        sched
            .shard_map
            .apply_op(ShardMapOp::AddPeer {
                key: PlacementTestHarness::key(),
                region_id: region.region_id,
                to_peer: 2,
                epoch: region.epoch,
                staging: false,
            })
            .await
            .unwrap();
        let after = sched.shard_map.snapshot().await.unwrap().map_version;
        assert_eq!(
            after,
            before + 1,
            "the commit must advance the cluster map version"
        );
        reported.store(after, std::sync::atomic::Ordering::Relaxed);

        // Second region, same tick, same peer.
        assert_eq!(
            sched.first_caught_up(2, after, &[(2, addr)]).await,
            Some(2),
            "a peer that applied this tick's own commit must not be rejected \
             because its probe was cached before that commit"
        );
        assert_eq!(
            probes.load(std::sync::atomic::Ordering::Relaxed),
            1,
            "the one-probe-per-peer-per-tick contract must still hold"
        );
    }

    /// A candidate that cannot be probed must be given up on for the whole
    /// tick, not retried once per region.
    ///
    /// `holds_placement` deliberately keeps `Disconnected` nodes as placement
    /// candidates, so a restarting node stays in `missing` for every region
    /// that targets it. Each probe costs up to `connect_timeout` (3s) against
    /// a dead address; at `max_regions_per_measurement = 128` that is several
    /// minutes of blocking inside a 10s tick, and the scheduler stops doing
    /// anything else at exactly the moment the cluster needs it to heal. The
    /// field's own doc comment already promised "at most once per tick" —
    /// before this it held only for probes that succeeded.
    #[tokio::test]
    #[serial_test::serial(chdb)]
    async fn a_failed_probe_is_not_retried_for_every_region_in_the_tick() {
        let harness = PlacementTestHarness::new(true, &[(2, 0)], 2).await;
        let sched = &harness.scheduler;

        let (addr, probes) = spawn_failing_mock_peer().await;
        {
            let mut m = sched.membership.write().await;
            m.nodes
                .get_mut(&2)
                .expect("harness put peer 2 in membership")
                .addr = addr.clone();
        }
        sched.peer_map_versions.write().await.clear();
        let ver = sched.shard_map.snapshot().await.unwrap().map_version;

        for region_id in 1..=5 {
            assert_eq!(
                sched
                    .first_caught_up(region_id, ver, &[(2, addr.clone())])
                    .await,
                None,
                "an unprobeable candidate cannot be chosen"
            );
        }

        assert_eq!(
            probes.load(std::sync::atomic::Ordering::Relaxed),
            1,
            "a failed probe must be remembered for the rest of the tick"
        );

        // The next tick clears the cache and is free to try again: this is a
        // per-tick suppression, not a permanent blacklist.
        sched.peer_map_versions.write().await.clear();
        assert_eq!(sched.first_caught_up(1, ver, &[(2, addr)]).await, None);
        assert_eq!(
            probes.load(std::sync::atomic::Ordering::Relaxed),
            2,
            "a new tick must re-probe the candidate"
        );
    }

    /// Per-region counters must not outlive their regions.
    ///
    /// Both maps are cleared only when a region converges, but a region that
    /// splits, merges or has its measurement dropped never converges again --
    /// and `next_region_id` only ever increases, so entries for dead regions
    /// accumulate for the life of the process. On a sharded cluster under load
    /// regions reshape constantly, so this grows without bound.
    #[tokio::test]
    #[serial_test::serial(chdb)]
    async fn counters_for_vanished_regions_are_pruned() {
        let harness = PlacementTestHarness::new(true, &[(2, 0)], 2).await;
        let sched = &harness.scheduler;

        let live_id = harness.region().await.region_id;
        let dead_id = live_id + 9_999;

        sched.note_placement_stall(live_id, 1).await;
        sched.note_placement_stall(dead_id, 1).await;
        let err = HyperbytedbError::ShardMap("boom".into());
        sched.note_placement_error(live_id, &err).await;
        sched.note_placement_error(dead_id, &err).await;

        // Through the tick, not by calling prune directly: the wiring is half
        // the behaviour, and a test that calls the helper itself still passes
        // when the tick stops calling it.
        sched.tick_once_for_test().await.unwrap();

        let stalls = sched.placement_stalls.read().await;
        let errs = sched.placement_errors.read().await;
        // Only the vanished region is asserted. The live one's counters are
        // legitimately cleared by convergence during the same tick, so
        // asserting it survives would be testing convergence, not pruning.
        // `dead_id` is not in the map at all, so nothing but the prune can
        // remove it -- which is exactly what makes this discriminate.
        assert!(
            !stalls.contains_key(&dead_id) && !errs.contains_key(&dead_id),
            "a region the map no longer has must not keep counters: \
             stalls={stalls:?} errors={errs:?}"
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

    #[tokio::test]
    #[serial_test::serial(chdb)]
    async fn decommission_completes_only_once_no_region_references_the_node() {
        let h = PlacementTestHarness::new(true, &[(2, 0)], 1).await;
        let owner = h.scheduler.node_id;
        assert!(
            !h.scheduler.decommission_complete(owner).await,
            "the owning node still holds a region"
        );
        assert!(
            h.scheduler.decommission_complete(9999).await,
            "a node holding nothing must read as evacuated"
        );
    }

    #[tokio::test]
    #[serial_test::serial(chdb)]
    async fn a_departing_peer_is_not_treated_as_dead() {
        // Regression: is_active_peer tests `== Active`, so a Decommissioning
        // node reads as dead and the drop-first heal path removed it without
        // staging its rows anywhere. Scale-down lost ~24% of points in the
        // kind e2e before this guard.
        let h = PlacementTestHarness::new(true, &[(2, 0)], 1).await;
        let key = PlacementTestHarness::key();
        h.add_peer_to_region(2).await;
        h.set_node_state(2, NodeState::Decommissioning).await;

        let region = h.region().await;
        h.scheduler
            .try_replace_dead_peer(&key, &region)
            .await
            .unwrap();

        let ops = h.proposals.lock().unwrap();
        assert!(
            !ops.iter()
                .any(|op| matches!(op, ShardMapOp::RemovePeer { from_peer: 2, .. })),
            "a decommissioning peer was dropped by the heal path, losing its \
             only copy; convergence must evacuate it instead. ops={ops:?}"
        );
    }
    #[test]
    fn region_cap_scales_with_the_candidate_count() {
        // The configured value is a floor, not a ceiling: it keeps a one-node
        // dev cluster bounded and stops being the binding constraint as soon
        // as the cluster is big enough for the cap to decide balance.
        assert_eq!(region_cap(128, 1), 128, "floor wins on a small cluster");
        assert_eq!(region_cap(128, 4), 128);
        assert_eq!(region_cap(128, 6), 192, "6 nodes outgrow the 128 floor");
        assert_eq!(region_cap(128, 50), 1_600);
        // Degenerate inputs must not produce a zero cap, which would refuse
        // every split forever.
        assert_eq!(region_cap(0, 0), 1);
        assert_eq!(region_cap(0, usize::MAX), usize::MAX);
    }

    /// Builds a measurement sitting at its region cap, every region over
    /// `region_max_series` so a split is attempted — and refused — for each
    /// one on every tick. `primary` is the node each region is primaried by.
    async fn build_capped_space(
        harness: &PlacementTestHarness,
        key: &MeasurementKey,
        peers: Vec<u64>,
        primary: u64,
        regions: usize,
    ) -> Vec<ShardRegion> {
        let mut seed = sample_region_peers(peers, primary);
        seed.region_id = 1_000;
        seed.last_split_at = 1;
        harness
            .scheduler
            .shard_map
            .apply_op(ShardMapOp::BootstrapMeasurement {
                key: key.clone(),
                region: seed,
            })
            .await
            .unwrap();

        // Metadata-only splits: exactly what the scheduler itself produces.
        while harness.space_regions(key).await.len() < regions {
            let parent = harness
                .space_regions(key)
                .await
                .into_iter()
                .max_by_key(|r| r.end.saturating_sub(r.start))
                .unwrap();
            let split_key = parent.start + (parent.end - parent.start) / 2;
            let child_epoch = parent.epoch.bump_version();
            let mut left = parent.clone();
            left.end = split_key;
            left.epoch = child_epoch;
            left.last_split_at = 1;
            let mut right = parent.clone();
            right.start = split_key;
            right.epoch = child_epoch;
            right.last_split_at = 1;
            harness
                .scheduler
                .shard_map
                .apply_op(ShardMapOp::Split {
                    key: key.clone(),
                    region_id: parent.region_id,
                    split_key,
                    epoch: parent.epoch,
                    left,
                    right,
                })
                .await
                .unwrap();
        }

        let regions = harness.space_regions(key).await;
        for region in &regions {
            harness
                .scheduler
                .record_heartbeat(region.region_id, 1, 1_000, 0, 0)
                .await;
        }
        regions
    }

    /// H5/H6 wiring: the scheduler's plan evens both copies and primaries
    /// across nodes, each region's target is the plan, and the tick starts
    /// moving primacy off the overloaded node.
    #[tokio::test]
    #[serial_test::serial(chdb)]
    async fn primaries_are_balanced_across_nodes() {
        let harness = PlacementTestHarness::new_tuned(true, &[(2, 1_000), (3, 1_000)], 0, |c| {
            c.replication_factor = 2;
        })
        .await;
        let key = MeasurementKey::new("db", "autogen", "balanced");
        let nodes = [1u64, 2, 3];
        let target_for = |start: u64| {
            let t = target_placement(&key, start, &nodes, 2);
            // Node 1 leads wherever it is a peer: the skew HRW produced on n3.
            let primary = if t.contains(&1) { 1 } else { t[0] };
            (t, primary)
        };
        let (t0, p0) = target_for(0);
        let mut seed = sample_region_peers(t0, p0);
        seed.region_id = 5_000;
        seed.last_split_at = 1;
        harness
            .scheduler
            .shard_map
            .apply_op(ShardMapOp::BootstrapMeasurement {
                key: key.clone(),
                region: seed,
            })
            .await
            .unwrap();
        while harness.space_regions(&key).await.len() < 12 {
            let parent = harness
                .space_regions(&key)
                .await
                .into_iter()
                .max_by_key(|r| r.end.saturating_sub(r.start))
                .unwrap();
            let split_key = parent.start + (parent.end - parent.start) / 2;
            let child_epoch = parent.epoch.bump_version();
            let mut left = parent.clone();
            left.end = split_key;
            left.epoch = child_epoch;
            let mut right = parent.clone();
            right.start = split_key;
            right.epoch = child_epoch;
            for child in [&mut left, &mut right] {
                let (t, p) = target_for(child.start);
                child.peers = t;
                child.primary = p;
            }
            harness
                .scheduler
                .shard_map
                .apply_op(ShardMapOp::Split {
                    key: key.clone(),
                    region_id: parent.region_id,
                    split_key,
                    epoch: parent.epoch,
                    left,
                    right,
                })
                .await
                .unwrap();
        }
        let regions = harness.space_regions(&key).await;
        for r in &regions {
            for p in &r.peers {
                harness
                    .scheduler
                    .record_heartbeat(r.region_id, *p, 50_000, 0, 0)
                    .await;
            }
        }
        let before = regions.iter().filter(|r| r.primary == 1).count();
        assert!(
            before >= 6,
            "fixture must be skewed onto node 1: {before} of 12"
        );

        harness.scheduler.tick_once_for_test().await.unwrap();

        let plan = harness.scheduler.placement_plan.read().await.clone();
        let mut per: HashMap<u64, usize> = HashMap::new();
        let mut copies: HashMap<u64, usize> = HashMap::new();
        for r in &regions {
            let planned = &plan[&r.region_id];
            assert_eq!(planned.len(), 2, "RF respected");
            *per.entry(planned[0]).or_default() += 1;
            for n in planned {
                *copies.entry(*n).or_default() += 1;
            }
            let target = harness.scheduler.balanced_target(&key, r, &nodes, 2).await;
            assert_eq!(&target, planned, "target must be the plan");
        }
        // 24 copies and 12 primaries over 3 nodes.
        assert!(copies.values().all(|&n| (7..=9).contains(&n)), "{copies:?}");
        assert!(per.values().all(|&n| (3..=5).contains(&n)), "{per:?}");

        let ops = harness.proposals.lock().unwrap();
        let moves: Vec<u64> = ops
            .iter()
            .filter_map(|op| match op {
                ShardMapOp::TransferPrimary {
                    key: k,
                    new_primary,
                    ..
                } if *k == key => Some(*new_primary),
                _ => None,
            })
            .collect();
        assert!(
            !moves.is_empty(),
            "the tick must start moving primacy: {ops:?}"
        );
        assert!(
            moves.iter().all(|n| *n != 1),
            "only away from node 1: {moves:?}"
        );
    }

    /// Same shape, but no heartbeats: every region reads as empty, which makes
    /// adjacent pairs merge candidates rather than split candidates.
    async fn build_capped_space_without_heartbeats(
        harness: &PlacementTestHarness,
        key: &MeasurementKey,
        peers: Vec<u64>,
        primary: u64,
        regions: usize,
    ) -> Vec<ShardRegion> {
        let built = build_capped_space(harness, key, peers, primary, regions).await;
        harness.scheduler.heartbeats.write().await.clear();
        built
    }

    fn capped_tuning(c: &mut crate::config::ShardingConfig) {
        c.max_regions_per_measurement = 1;
        c.region_split_series = 5;
        // Must stay below `region_split_series` (config invariant) and below
        // the series these regions report, or the merge path claims the pass
        // before placement and the test proves nothing.
        c.region_merge_series = 1;
        c.region_max_series = 10;
        c.region_split_cooldown_secs = 0;
        c.split_merge_interval_secs = 0;
        c.primary_failover_after_secs = 0;
        c.peer_heal_enabled = true;
    }

    /// G1, half one: safety must not queue behind a split refusal.
    ///
    /// Heal and failover used to run *after* split/merge in the region pass,
    /// and `try_split` signalled a refusal as `Ok(())`, which the tick read as
    /// "split proposed" and `continue`d on. A measurement at its region cap
    /// therefore never failed over — on the kind cluster, 126 of 128 regions
    /// stayed pinned to one node for five hours.
    #[tokio::test]
    #[serial_test::serial(chdb)]
    async fn a_capped_oversized_region_still_fails_over() {
        let harness = PlacementTestHarness::new_tuned(true, &[(2, 0)], 1, capped_tuning).await;
        let key = MeasurementKey::new("db", "autogen", "capped");
        // Node 2 primaries every region and is about to die; node 1 (self) is
        // the only surviving peer, so it is the only failover target.
        let regions = build_capped_space(&harness, &key, vec![1, 2], 2, 40).await;
        {
            let mut m = harness.scheduler.membership.write().await;
            let addr = m.get_node(2).map(|n| n.addr.clone()).unwrap_or_default();
            m.add_node(NodeInfo {
                node_id: 2,
                addr,
                state: NodeState::Disconnected,
                joined_at: 2,
                last_heartbeat: 0,
                needs_sync: false,
                consecutive_misses: 9,
            });
        }

        // Node 2 is dead, so it is no longer a placement candidate: the cap is
        // 32 and the space holds 40 regions.
        assert!(
            regions.len() >= region_cap(1, 1),
            "test needs the space at its cap: {} regions, cap {}",
            regions.len(),
            region_cap(1, 1)
        );
        assert_eq!(
            harness
                .scheduler
                .try_split(&key, &regions[0], 1_000)
                .await
                .unwrap(),
            SplitOutcome::Refused(SplitRefusal::RegionCap),
            "at the cap the split must be refused — and say why"
        );

        harness.scheduler.tick_once_for_test().await.unwrap();

        let ops = harness.proposals.lock().unwrap();
        assert!(
            !ops.iter()
                .any(|op| matches!(op, ShardMapOp::Split { key: k, .. } if *k == key)),
            "the space is at its cap; no split may be proposed: {ops:?}"
        );
        assert!(
            ops.iter().any(|op| matches!(
                op,
                ShardMapOp::TransferPrimary { key: k, new_primary: 1, .. } if *k == key
            )),
            "a capped, oversized region whose primary is dead must still fail \
             over: {ops:?}"
        );
    }

    /// G1, half two: a refused split must not swallow the placement preview.
    ///
    /// Placement runs after split in the region pass, so this is what the
    /// `SplitOutcome::Proposed`-only `continue` guards. Every region here is
    /// over `region_max_series` and the space is at its cap, so the old code
    /// `continue`d past placement for all of them — forever, since the cap
    /// never clears on its own.
    #[tokio::test]
    #[serial_test::serial(chdb)]
    async fn a_capped_oversized_region_still_gets_a_placement_step() {
        let harness =
            // The mock peers report a map version well ahead of the one
            // captured at harness build time: this test applies ~100 splits
            // afterwards, and a joiner behind the committed map is (correctly)
            // refused as a staging target, which would mask the thing under
            // test.
            PlacementTestHarness::new_tuned(true, &[(2, 1_000), (3, 1_000)], 1, capped_tuning)
                .await;
        let key = MeasurementKey::new("db", "autogen", "capped");
        // Primaried by this node, one peer short of RF: the placement step is
        // an AddPeer, which no other path in this tick proposes for this space.
        let regions = build_capped_space(&harness, &key, vec![1], 1, 100).await;
        assert!(
            regions.len() >= region_cap(1, 3),
            "test needs the space at its cap: {} regions, cap {}",
            regions.len(),
            region_cap(1, 3)
        );

        harness.scheduler.tick_once_for_test().await.unwrap();

        let ops = harness.proposals.lock().unwrap();
        assert!(
            !ops.iter()
                .any(|op| matches!(op, ShardMapOp::Split { key: k, .. } if *k == key)),
            "the space is at its cap; no split may be proposed: {ops:?}"
        );
        assert!(
            ops.iter()
                .any(|op| matches!(op, ShardMapOp::AddPeer { key: k, .. } if *k == key)),
            "a capped, oversized region must still converge towards its \
             placement target; the refused split used to skip it: {ops:?}"
        );
    }
    /// G1, half three: a *successful* merge must not swallow failover either.
    ///
    /// Merge still ends the region's pass — correctly, the region is about to
    /// stop existing — so the only protection for a mergeable region whose
    /// primary is dead is that heal and failover run *before* split/merge.
    /// With them at the end of the pass, the left region of every mergeable
    /// pair went un-failed-over for as long as it kept merging.
    #[tokio::test]
    #[serial_test::serial(chdb)]
    async fn a_mergeable_region_with_a_dead_primary_still_fails_over() {
        let harness = PlacementTestHarness::new_tuned(true, &[(2, 0)], 1, |c| {
            c.split_merge_interval_secs = 0;
            c.primary_failover_after_secs = 0;
            c.peer_heal_enabled = true;
        })
        .await;
        let key = MeasurementKey::new("db", "autogen", "mergeable");
        // No heartbeats recorded, so both regions read as empty and the pair
        // is a merge candidate. Node 2 primaries them and is dead.
        let regions = build_capped_space_without_heartbeats(&harness, &key, vec![1, 2], 2, 2).await;
        let left = regions[0].region_id;
        {
            let mut m = harness.scheduler.membership.write().await;
            let addr = m.get_node(2).map(|n| n.addr.clone()).unwrap_or_default();
            m.add_node(NodeInfo {
                node_id: 2,
                addr,
                state: NodeState::Disconnected,
                joined_at: 2,
                last_heartbeat: 0,
                needs_sync: false,
                consecutive_misses: 9,
            });
        }

        harness.scheduler.tick_once_for_test().await.unwrap();

        let ops = harness.proposals.lock().unwrap();
        assert!(
            ops.iter().any(|op| matches!(
                op,
                ShardMapOp::TransferPrimary { key: k, region_id, new_primary: 1, .. }
                    if *k == key && *region_id == left
            )),
            "the left region of the merge pair lost its primary; failover must \
             run before the merge claims the pass: {ops:?}"
        );
    }
    /// G4: the mover queue must drain, not stop at `schedule_limit`.
    ///
    /// `run_pending_placements` sorted by tier, took the first
    /// `schedule_limit` jobs and awaited each full copy inline. With one tier
    /// and a stable map order that is the same four regions every tick, and
    /// every other region waiting behind a copy that has nothing to do with
    /// it — 15 placements in five hours on the kind cluster.
    ///
    /// Node 3 reports a map far behind the cluster, so it is refused as a
    /// staging target. Regions whose HRW target needs node 3 and nothing else
    /// are genuinely stuck jobs sitting in the queue; they must not cost the
    /// other regions their tick.
    #[tokio::test]
    #[serial_test::serial(chdb)]
    async fn the_mover_queue_drains_past_schedule_limit_and_past_stuck_regions() {
        const REGIONS: usize = 20;
        let harness = PlacementTestHarness::new_tuned(true, &[(2, 0), (3, -1_000)], REGIONS, |c| {
            c.schedule_limit = 4;
            // RF 2 over three candidates, so a region whose target is
            // {self, node 3} has no healthy alternative to fall through to.
            c.replication_factor = 2;
        })
        .await;

        harness.scheduler.tick_once_for_test().await.unwrap();

        // Which regions are stuck is not guessed: it is the target the
        // scheduler itself used this tick (the balanced plan), so this stays
        // honest when placement changes.
        let candidates = vec![1u64, 2, 3];
        let rf = effective_replication_factor(2, candidates.len());
        let mut expect_placed: Vec<u64> = Vec::new();
        let mut expect_stuck: Vec<u64> = Vec::new();
        for i in 0..REGIONS {
            let key = MeasurementKey::new("db", "autogen", format!("cpu{i}"));
            let region_id = (i + 1) as u64;
            let region = harness
                .space_regions(&key)
                .await
                .into_iter()
                .find(|r| r.region_id == region_id)
                .unwrap();
            let target = harness
                .scheduler
                .balanced_target(&key, &region, &candidates, rf)
                .await;
            // `missing` is every target member the region lacks, in target
            // order; the first caught-up one wins. Only node 2 is caught up.
            if target.contains(&2) {
                expect_placed.push(region_id);
            } else if target.contains(&3) {
                expect_stuck.push(region_id);
            }
        }
        assert!(
            !expect_placed.is_empty() && !expect_stuck.is_empty(),
            "the fixture needs both outcomes present: placed={expect_placed:?} \
             stuck={expect_stuck:?}"
        );

        // The cursor records one entry per job the movers actually ran, which
        // is exactly "did the queue drain".
        let attempted = harness.scheduler.placement_last_attempted.read().await;
        assert_eq!(
            attempted.len(),
            REGIONS,
            "every queued region must be attempted in the tick, not the first \
             schedule_limit of them: attempted={:?}",
            attempted.keys().collect::<Vec<_>>()
        );
        drop(attempted);

        let placed: std::collections::BTreeSet<u64> = harness
            .proposals
            .lock()
            .unwrap()
            .iter()
            .filter_map(|op| match op {
                ShardMapOp::AddPeer { region_id, .. } => Some(*region_id),
                _ => None,
            })
            .collect();
        for region_id in &expect_placed {
            assert!(
                placed.contains(region_id),
                "region {region_id} has a healthy target and must be placed \
                 even though other regions in the queue are stuck: \
                 placed={placed:?}"
            );
        }
        let stalls = harness.scheduler.placement_stalls.read().await;
        for region_id in &expect_stuck {
            assert!(
                stalls.contains_key(region_id),
                "region {region_id} has only a lagging target and must be \
                 recorded as stalled, not silently dropped from the queue: \
                 stalls={:?}",
                stalls.keys().collect::<Vec<_>>()
            );
        }
    }

    /// The round-robin cursor is per region id, so splits and merges would
    /// leak entries into it forever without the prune.
    #[tokio::test]
    #[serial_test::serial(chdb)]
    async fn the_round_robin_cursor_is_pruned_with_its_region() {
        let harness = PlacementTestHarness::new(true, &[(2, 0)], 1).await;
        let sched = &harness.scheduler;
        let dead_id = harness.region().await.region_id + 9_999;
        sched.note_placement_attempt(dead_id).await;

        sched.tick_once_for_test().await.unwrap();

        let cursor = sched.placement_last_attempted.read().await;
        assert!(
            !cursor.contains_key(&dead_id),
            "a region the map no longer has must not keep a round-robin \
             cursor entry: {cursor:?}"
        );
    }
    /// G7, half one: a learner must never be offered as a failover target.
    ///
    /// `TransferPrimary` apply rejects a learner outright
    /// (`ops.rs:541-544`), so proposing one spent the failover on a call that
    /// could only fail and left the region unserved for another tick.
    #[tokio::test]
    async fn a_learner_is_not_a_failover_candidate() {
        let mut m = ClusterMembership::new();
        m.add_node(node(1, NodeState::Disconnected));
        m.add_node(node(2, NodeState::Active));
        m.add_node(node(3, NodeState::Active));
        let membership = crate::domain::cluster::membership::new_shared(m);

        // Node 2 is a learner; node 3 is a committed peer.
        let mut region = sample_region_peers(vec![1, 2, 3], 1);
        region.staging_peers = vec![2];

        assert_eq!(
            pick_best_primary(&membership, &region, 9, None).await,
            Some(3),
            "the committed peer must win over the learner"
        );

        // With the committed peer gone, there is no serving candidate at all —
        // the learner must not be substituted.
        let mut learner_only = sample_region_peers(vec![1, 2], 1);
        learner_only.staging_peers = vec![2];
        assert_eq!(
            pick_best_primary(&membership, &learner_only, 9, None).await,
            None,
            "a learner must not be offered as a primary"
        );

        // The watermark path is the one production actually takes, and it
        // selects from its own candidate list rather than falling through to
        // `pick_alternate_primary`. Unreachable peers all probe as 0, so
        // without the learner filter the first candidate — the learner — wins.
        let dir = tempfile::tempdir().unwrap();
        let pc = PeerClient::new(
            9,
            "127.0.0.1:1".into(),
            membership.clone(),
            Arc::new(
                crate::adapters::cluster::replication_log::ReplicationLog::open(dir.path())
                    .unwrap(),
            ),
            1,
            8192,
            2,
            8 * 1024 * 1024,
        );
        assert_eq!(
            pick_best_primary(&membership, &learner_only, 9, Some(&pc)).await,
            None,
            "the watermark path must exclude learners too"
        );
        assert_eq!(
            pick_best_primary(&membership, &region, 9, Some(&pc)).await,
            Some(3),
            "and must still pick the committed peer"
        );
    }

    /// G7, half two: a learner-only survivor is promoted rather than stranded.
    ///
    /// A region left with a dead primary and one learner has no write or read
    /// target — `resolve_region_peers` refuses a learner — and failover had no
    /// candidate, so it stalled forever. It must promote the learner and hand
    /// it primacy.
    #[tokio::test]
    #[serial_test::serial(chdb)]
    async fn a_learner_only_survivor_is_promoted_and_takes_primacy() {
        let harness = PlacementTestHarness::new_tuned(true, &[(2, 0)], 1, |c| {
            c.primary_failover_after_secs = 0;
        })
        .await;
        let key = MeasurementKey::new("db", "autogen", "learner_only");
        let mut seed = sample_region_peers(vec![1, 2], 1);
        seed.region_id = 4_100;
        // Node 2 holds the region only as a learner; node 1 primaries it.
        seed.staging_peers = vec![2];
        harness
            .scheduler
            .shard_map
            .apply_op(ShardMapOp::BootstrapMeasurement {
                key: key.clone(),
                region: seed,
            })
            .await
            .unwrap();

        // The primary (this node) goes away, leaving only the learner.
        {
            let mut m = harness.scheduler.membership.write().await;
            m.set_state(1, NodeState::Disconnected);
        }

        let region = harness.space_regions(&key).await.remove(0);
        harness
            .scheduler
            .try_failover_unhealthy_primary(&key, &region, now_unix() as u64)
            .await
            .unwrap();

        let ops = harness.proposals.lock().unwrap();
        assert!(
            ops.iter().any(|op| matches!(
                op,
                ShardMapOp::PromotePeer { key: k, peer: 2, .. } if *k == key
            )),
            "the learner must be promoted before it can hold primacy: {ops:?}"
        );
        assert!(
            ops.iter().any(|op| matches!(
                op,
                ShardMapOp::TransferPrimary { key: k, new_primary: 2, .. } if *k == key
            )),
            "primacy must then move to the promoted survivor, or the region \
             stays unserved: {ops:?}"
        );
    }
    /// B.5b: the promote bound has to be relative, because heartbeats are
    /// sampled on an interval.
    ///
    /// The kind cluster writes a new series per point at roughly 450/s with a
    /// 10s heartbeat interval, so a perfectly healthy replica is ~4,500 series
    /// behind whatever the primary last reported. Any absolute allowance is
    /// therefore either blocking (small) or meaningless (large) depending
    /// purely on write rate — which is how `promote_skipped_total` reached
    /// 2,431 and pinned all 192 regions to one node.
    #[test]
    fn the_promote_bound_survives_heartbeat_staleness() {
        // Healthy replica, one heartbeat interval behind at a high write rate.
        assert!(
            promote_progress_safe(Some(95_500), Some(100_000), 50),
            "a replica one sampling interval behind must still be promotable"
        );
        // Essentially empty: this is what the guard exists to catch.
        assert!(!promote_progress_safe(Some(10), Some(100_000), 50));
        assert!(!promote_progress_safe(Some(0), Some(100_000), 50));
        // Not heard from is not "empty", but it still defers for a *planned*
        // move, because nothing is broken while we wait.
        assert!(!promote_progress_safe(None, Some(100_000), 50));
        // Nothing written yet anywhere: allow, or a fresh region can never
        // move its primary.
        assert!(promote_progress_safe(Some(0), Some(0), 50));
        assert!(promote_progress_safe(Some(0), None, 50));
        // The absolute-allowance failure mode, stated as a test so it cannot
        // quietly come back: 4,500 behind out of 100,000 is healthy, and any
        // fixed bound in the tens would reject it.
        let healthy_lag = 100_000u64 - 95_500;
        assert!(
            healthy_lag > 64,
            "a fixed allowance of 64 would reject this"
        );
        assert!(promote_progress_safe(Some(95_500), Some(100_000), 50));
    }
    /// A move is charged what it holds at once, one transfer chunk, so the
    /// default budget runs four moves of large regions together. Charging the
    /// whole region ran every move over 256 MiB alone: on n3, 45 moves of a
    /// kept database took over 30 minutes.
    #[test]
    fn a_move_is_charged_one_chunk_not_its_region() {
        let budget_mib = 256usize;
        let chunk_mib = (MOVE_INFLIGHT_BYTES / (1024 * 1024)) as usize;

        // Small regions barely draw on the budget.
        assert_eq!(mover_inflight_permits(4 * 1024 * 1024, budget_mib), 4);

        // A large region costs one chunk, so four of them fit.
        let large = mover_inflight_permits(650 * 1024 * 1024, budget_mib);
        assert_eq!(large, chunk_mib);
        assert!(
            budget_mib / large >= 4,
            "four large moves fit: {large} MiB each"
        );

        // A region with no heartbeat yet still costs something.
        assert_eq!(mover_inflight_permits(0, budget_mib), 1);

        // A budget smaller than a chunk is clamped, NOT refused: the move runs
        // alone. Asking for more permits than the semaphore holds would block
        // that region forever.
        assert_eq!(mover_inflight_permits(u64::MAX, 4), 4);
    }

    /// Large regions move together, each at its share of the cap.
    ///
    /// Charging a move its whole region ran every move of a region over the
    /// in-flight budget alone, and the per-tick allowance stopped balance
    /// moves after the first big one. Every region here reports 1 GiB, so
    /// the old charge would run these moves strictly one after another.
    #[tokio::test]
    #[serial_test::serial(chdb)]
    async fn large_moves_run_together_each_paced_to_its_share() {
        const REGIONS: usize = 8;
        const MOVERS: usize = 8;
        const POINTS: usize = 2_000;
        let harness = PlacementTestHarness::new_tuned(true, &[(2, 0)], REGIONS, |c| {
            c.schedule_limit = MOVERS;
            c.replication_factor = 1;
            // Each mover's share is sized below so one move takes >= 1 s.
            c.movement_budget_bytes_per_sec = (MOVERS * 20 * 1024) as u64;
        })
        .await;
        for i in 0..REGIONS {
            harness.write_points(i, POINTS).await;
            harness
                .scheduler
                .record_heartbeat((i + 1) as u64, 1, POINTS as u64, 1 << 30, 0)
                .await;
        }

        let started = std::time::Instant::now();
        harness.scheduler.tick_once_for_test().await.unwrap();
        let elapsed = started.elapsed();

        let staged: Vec<u64> = harness
            .proposals
            .lock()
            .unwrap()
            .iter()
            .filter_map(|op| match op {
                ShardMapOp::PromotePeer { region_id, .. } => Some(*region_id),
                _ => None,
            })
            .collect();
        assert!(
            (2..=MOVERS).contains(&staged.len()),
            "the fixture needs 2..={MOVERS} moves in one wave, got {staged:?}"
        );
        // Each move pushes > 20 KiB at a 20 KiB/s share: paced to >= 1 s.
        assert!(
            elapsed >= std::time::Duration::from_secs(1),
            "moves were not paced: {elapsed:?}"
        );
        // Together, not one after another (serial would be >= moves seconds).
        assert!(
            elapsed < std::time::Duration::from_millis(1_000 * staged.len() as u64 - 300),
            "{} moves ran one at a time: {elapsed:?}",
            staged.len()
        );
    }

    /// A move in flight must not lock the planning pass out of other regions.
    ///
    /// The operator table was capped at `schedule_limit`. With movers running
    /// in the background holding their regions, that cap would stop every
    /// split, merge and heal until the moves finished.
    #[tokio::test]
    #[serial_test::serial(chdb)]
    async fn a_split_is_planned_while_movers_hold_other_regions() {
        let harness = PlacementTestHarness::new_tuned(true, &[(2, 0)], 2, |c| {
            c.schedule_limit = 1;
            c.replication_factor = 1;
        })
        .await;
        // Region 2 is mid-move: its mover holds the only schedule_limit slot.
        assert!(harness.scheduler.acquire_operator(2).await);
        harness
            .scheduler
            .record_heartbeat(1, 1, 1_000_000, 0, 0)
            .await;

        harness.scheduler.plan_tick().await.unwrap();

        let proposals = harness.proposals.lock().unwrap();
        assert!(
            proposals
                .iter()
                .any(|op| matches!(op, ShardMapOp::Split { region_id: 1, .. })),
            "region 1 is over region_max_series and must split while region 2 \
             moves: {proposals:?}"
        );
    }

    /// `run` must keep planning while its movers copy data.
    ///
    /// Ticks used to await every move they queued, so on n3 a new database's
    /// second split round waited 3.5 minutes behind moves of another
    /// database's regions.
    #[tokio::test]
    #[serial_test::serial(chdb)]
    async fn run_keeps_splitting_while_a_move_is_in_flight() {
        const REGIONS: usize = 8;
        let harness = PlacementTestHarness::new_tuned(true, &[(2, 0)], REGIONS, |c| {
            c.schedule_limit = 1;
            c.replication_factor = 1;
            // One mover at 4 KiB/s: a move of ~50 KiB takes many seconds.
            c.movement_budget_bytes_per_sec = 4 * 1024;
        })
        .await;
        for i in 0..REGIONS {
            harness.write_points(i, 2_000).await;
        }

        let (tx, rx) = watch::channel(false);
        let sched = harness.scheduler.clone();
        let run = tokio::spawn(async move {
            sched.run(Duration::from_millis(100), rx).await;
        });
        // Let the first move start, then make another region need a split.
        tokio::time::sleep(Duration::from_millis(400)).await;
        assert!(
            harness
                .scheduler
                .movers_busy
                .load(std::sync::atomic::Ordering::Acquire),
            "a move should be in flight"
        );
        // Any region but the one the single mover holds.
        let held: Vec<u64> = harness
            .scheduler
            .region_operators
            .read()
            .await
            .keys()
            .copied()
            .collect();
        let split_region = (1..=REGIONS as u64).find(|r| !held.contains(r)).unwrap();
        harness
            .scheduler
            .record_heartbeat(split_region, 1, 1_000_000, 0, 0)
            .await;
        tokio::time::sleep(Duration::from_millis(600)).await;

        let split = harness.proposals.lock().unwrap().iter().any(
            |op| matches!(op, ShardMapOp::Split { region_id, .. } if *region_id == split_region),
        );
        let still_moving = harness
            .scheduler
            .movers_busy
            .load(std::sync::atomic::Ordering::Acquire);
        let _ = tx.send(true);
        run.abort();
        assert!(
            still_moving,
            "the move finished too early to prove anything"
        );
        assert!(split, "no split proposed while a move was in flight");
    }

    /// A learner whose copy is still running is not stale.
    ///
    /// Learners seen on two ticks in a row were re-staged as failed promotes.
    /// Once ticks stopped waiting for moves, that re-copied regions whose
    /// mover was still copying.
    #[tokio::test]
    #[serial_test::serial(chdb)]
    async fn a_learner_mid_move_is_not_restaged() {
        let harness = PlacementTestHarness::new(true, &[(2, 0)], 1).await;
        let region = harness.region().await;
        harness
            .scheduler
            .shard_map
            .apply_op(ShardMapOp::AddPeer {
                key: PlacementTestHarness::key(),
                region_id: region.region_id,
                to_peer: 2,
                epoch: region.epoch,
                staging: true,
            })
            .await
            .unwrap();
        let promoted = |h: &PlacementTestHarness| {
            h.proposals
                .lock()
                .unwrap()
                .iter()
                .any(|op| matches!(op, ShardMapOp::PromotePeer { peer: 2, .. }))
        };

        assert!(harness.scheduler.acquire_operator(region.region_id).await);
        for _ in 0..2 {
            let map = harness.scheduler.shard_map.snapshot().await.unwrap();
            harness.scheduler.try_promote_learners(&map).await;
        }
        assert!(
            !promoted(&harness),
            "re-staged a learner its mover still holds"
        );

        harness.scheduler.release_operator(region.region_id).await;
        let map = harness.scheduler.shard_map.snapshot().await.unwrap();
        harness.scheduler.try_promote_learners(&map).await;
        assert!(
            promoted(&harness),
            "a learner left over once its mover is gone must still be promoted"
        );
    }
}
