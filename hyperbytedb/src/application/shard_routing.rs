//! Shard routing helpers: bootstrap, partition, forward.

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use metrics::{counter, histogram};

use crate::adapters::cluster::peer_client::PeerClient;
use crate::adapters::cluster::raft::types::{ClusterRequest, ClusterResponse};
use crate::adapters::sharding::rocksdb_shard_map::RocksDbShardMap;
use crate::application::runtime::RegionWriteStats;
use crate::application::shard_peer_resolution::{
    RegionTargetRole, ScatterKind, active_region_peer_targets, is_active_peer, peer_addr,
    resolve_region_peers,
};
use crate::config::ShardingConfig;
use crate::domain::point::Point;
use crate::domain::series::series_id_for_point;
use crate::domain::sharding::ShardLocationCache;
use crate::domain::sharding::{
    MeasurementKey, ShardBootstrapRequest, ShardEpoch, ShardMapOp, ShardRegion, ShardWriteRequest,
};
use crate::error::HyperbytedbError;
use crate::ports::metadata::MetadataPort;
use crate::ports::sharding::ShardMapPort;

pub struct ShardRoutingContext {
    pub shard_map: Arc<RocksDbShardMap>,
    pub location_cache: Arc<ShardLocationCache>,
    pub config: ShardingConfig,
    pub node_id: u64,
    pub peer_client: Arc<PeerClient>,
    pub region_write_stats: Arc<RegionWriteStats>,
}

pub struct PointBuckets {
    pub local: Vec<Point>,
    /// region_id -> points for regions this node is not a peer of
    pub forward: HashMap<u64, Vec<Point>>,
}

/// Try remote peers in Active-only order until one succeeds.
pub async fn scatter_to_region_peers<T, F, Fut>(
    ctx: &ShardRoutingContext,
    region: &ShardRegion,
    role: RegionTargetRole,
    scatter_kind: ScatterKind,
    mut attempt_one: F,
) -> Result<T, HyperbytedbError>
where
    F: FnMut(u64, &str, Duration) -> Fut,
    Fut: Future<Output = Result<T, HyperbytedbError>>,
{
    let membership = ctx.peer_client.membership().read().await;
    let mut candidates = resolve_region_peers(region, ctx.node_id, &membership, role);
    candidates.retain(|id| *id != ctx.node_id);

    let max = ctx.config.scatter_max_peer_attempts.max(1);
    candidates.truncate(max);

    let peer_addrs: Vec<(u64, String)> = candidates
        .into_iter()
        .filter_map(|peer_id| peer_addr(&membership, peer_id).map(|addr| (peer_id, addr)))
        .collect();
    drop(membership);

    if peer_addrs.is_empty() {
        return Err(HyperbytedbError::PeerUnreachable(format!(
            "no active peers for region {}",
            region.region_id
        )));
    }

    let timeout = Duration::from_millis(ctx.config.scatter_peer_timeout_ms.max(1));
    let mut last_err: Option<HyperbytedbError> = None;

    for (attempts, (peer_id, addr)) in peer_addrs.into_iter().enumerate() {
        let attempt_no = attempts as u64 + 1;
        match attempt_one(peer_id, &addr, timeout).await {
            Ok(v) => {
                if attempt_no > 1 {
                    counter!(
                        "hyperbytedb_shard_scatter_fallback_total",
                        "kind" => scatter_kind.as_str(),
                    )
                    .increment(1);
                }
                histogram!("hyperbytedb_shard_scatter_peer_attempts").record(attempt_no as f64);
                return Ok(v);
            }
            Err(HyperbytedbError::StaleShardEpoch { .. }) => {
                return Err(HyperbytedbError::StaleShardEpoch {
                    region_id: region.region_id,
                });
            }
            Err(e) => last_err = Some(e),
        }
    }

    Err(last_err.unwrap_or_else(|| {
        HyperbytedbError::PeerUnreachable(format!(
            "all active peers failed for region {}",
            region.region_id
        ))
    }))
}

pub async fn refresh_location_cache(ctx: &ShardRoutingContext) -> Result<(), HyperbytedbError> {
    let map = ctx.shard_map.snapshot().await?;
    ctx.location_cache.refresh_from_map(&map);
    Ok(())
}

/// Reload a region from the shard map after stale epoch (post cache refresh).
pub async fn reload_region(
    ctx: &ShardRoutingContext,
    db: &str,
    rp: &str,
    measurement: &str,
    region_id: u64,
) -> Result<ShardRegion, HyperbytedbError> {
    refresh_location_cache(ctx).await?;
    let map = ctx.shard_map.snapshot().await?;
    map.space(db, rp, measurement)
        .and_then(|space| {
            space
                .regions
                .iter()
                .find(|r| r.region_id == region_id)
                .cloned()
        })
        .ok_or_else(|| HyperbytedbError::ShardMap("region missing after refresh".into()))
}

pub async fn ensure_measurement_bootstrapped(
    ctx: &ShardRoutingContext,
    db: &str,
    rp: &str,
    measurement: &str,
    is_raft_leader: bool,
    leader_addr: Option<&str>,
) -> Result<(), HyperbytedbError> {
    let timeout = Duration::from_millis(ctx.config.bootstrap_timeout_ms);
    if measurement_bootstrapped(ctx, db, rp, measurement).await? {
        return Ok(());
    }

    if is_raft_leader {
        let op = build_bootstrap_op(ctx, db, rp, measurement).await?;
        if let Some(addr) = leader_addr {
            propose_shard_map_op_via_raft(addr, op).await?;
        } else {
            // No leader address resolves: this is raft-less sharding (async
            // replication without OpenRaft), where this node's shard map IS
            // the authoritative placement authority — apply locally. Real
            // Raft deployments never land here: RaftLeaderCallbacks falls
            // back to the configured cluster address whenever leadership is
            // held locally.
            bootstrap_measurement_local(ctx, db, rp, measurement).await?;
        }
    } else if let Some(addr) = leader_addr {
        let url = format!("http://{addr}/internal/shard/bootstrap");
        let req = ShardBootstrapRequest {
            db: db.to_string(),
            rp: rp.to_string(),
            measurement: measurement.to_string(),
        };
        let resp = ctx
            .peer_client
            .http_client()
            .post(&url)
            .json(&req)
            .timeout(timeout)
            .send()
            .await
            .map_err(|e| HyperbytedbError::PeerUnreachable(e.to_string()))?;
        if !resp.status().is_success() {
            return Err(HyperbytedbError::ShardMap(
                format!("bootstrap failed: {}", resp.status()).into(),
            ));
        }
    } else {
        return Err(HyperbytedbError::ClusterUnavailable(
            "no raft leader for shard bootstrap".into(),
        ));
    }

    wait_for_measurement_bootstrapped(ctx, db, rp, measurement, timeout).await
}

async fn measurement_bootstrapped(
    ctx: &ShardRoutingContext,
    db: &str,
    rp: &str,
    measurement: &str,
) -> Result<bool, HyperbytedbError> {
    let map = ctx.shard_map.snapshot().await?;
    Ok(map.space(db, rp, measurement).is_some())
}

async fn wait_for_measurement_bootstrapped(
    ctx: &ShardRoutingContext,
    db: &str,
    rp: &str,
    measurement: &str,
    timeout: Duration,
) -> Result<(), HyperbytedbError> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let map = ctx.shard_map.snapshot().await?;
        if map.space(db, rp, measurement).is_some() {
            ctx.location_cache.refresh_from_map(&map);
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(HyperbytedbError::ShardMap(
                "bootstrap replication timeout".into(),
            ));
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

pub async fn build_bootstrap_op(
    ctx: &ShardRoutingContext,
    db: &str,
    rp: &str,
    measurement: &str,
) -> Result<ShardMapOp, HyperbytedbError> {
    let peers = select_bootstrap_peers(ctx, db, rp, measurement).await?;
    if peers.is_empty() {
        return Err(HyperbytedbError::ClusterUnavailable(
            "no active peers for shard bootstrap".into(),
        ));
    }
    let primary = peers[0];
    let map = ctx.shard_map.snapshot().await?;
    let region_id = map.next_region_id;
    let last_split_at = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(1)
        .max(1);
    let region = ShardRegion {
        region_id,
        start: 0,
        end: u64::MAX,
        epoch: ShardEpoch::default(),
        peers: peers.clone(),
        primary,
        last_split_at,
        transfer_verified: None,
        transfer_first_seen: None,
    };
    Ok(ShardMapOp::BootstrapMeasurement {
        key: MeasurementKey::new(db, rp, measurement),
        region,
    })
}

pub async fn bootstrap_measurement_local(
    ctx: &ShardRoutingContext,
    db: &str,
    rp: &str,
    measurement: &str,
) -> Result<(), HyperbytedbError> {
    if measurement_bootstrapped(ctx, db, rp, measurement).await? {
        return Ok(());
    }
    let op = build_bootstrap_op(ctx, db, rp, measurement).await?;
    ctx.shard_map.apply_op(op).await?;
    Ok(())
}

/// Effective replica count for a region: never more than live membership.
///
/// A 1-member cluster therefore has RF=1 even when `configured` is 3.
/// RF is also never raised above the configured target.
#[must_use]
pub fn effective_replication_factor(configured: usize, active_members: usize) -> usize {
    let target = configured.max(1);
    if active_members == 0 {
        return 0;
    }
    target.min(active_members)
}

/// Pick the peer set for a brand-new measurement's first region.
///
/// Candidates are ordered by
/// [`crate::domain::sharding::placement::target_placement`] — the same function
/// that places every other region — so a freshly bootstrapped region already
/// sits where the scheduler would put it and needs no movement. A measurement's
/// first region starts at 0, which is the `start` passed here.
///
/// The previous ordering used `DefaultHasher`, which std does not guarantee
/// stable across releases; placement is recomputed continuously on every node,
/// so two builds disagreeing would scatter regions during a rolling upgrade.
///
/// Peer count is [`effective_replication_factor`]: n=1 yields `[self]`,
/// not an empty set truncated against a configured RF of 2 or 3.
async fn select_bootstrap_peers(
    ctx: &ShardRoutingContext,
    db: &str,
    rp: &str,
    measurement: &str,
) -> Result<Vec<u64>, HyperbytedbError> {
    let membership = ctx.peer_client.membership().read().await;
    let mut peers: Vec<u64> = membership
        .active_peers(ctx.node_id)
        .iter()
        .map(|n| n.node_id)
        .collect();
    peers.push(ctx.node_id);
    drop(membership);
    peers.sort_unstable();
    peers.dedup();
    let member_count = peers.len();
    let rf = effective_replication_factor(ctx.config.replication_factor, member_count);
    let key = crate::domain::sharding::types::MeasurementKey::new(db, rp, measurement);
    Ok(crate::domain::sharding::placement::target_placement(
        &key, 0, &peers, rf,
    ))
}

/// Partition an owned batch into local vs per-region forward buckets.
///
/// Takes ownership and *moves* points into their buckets — a `Point` move is
/// pointer-copying, so this replaces what used to be a deep clone of every
/// point (strings + two BTreeMaps) on the hot ingest path. Region lookups use
/// the allocation-free [`ShardLocationCache::locate_route`].
pub async fn partition_points(
    ctx: &ShardRoutingContext,
    db: &str,
    rp: &str,
    points: Vec<Point>,
) -> Result<PointBuckets, HyperbytedbError> {
    let map = ctx.shard_map.snapshot().await?;
    {
        let mut seen: HashSet<&str> = HashSet::new();
        for p in &points {
            // contains-then-insert avoids a String allocation per point.
            if seen.insert(p.measurement.as_str()) && map.space(db, rp, &p.measurement).is_none() {
                return Err(HyperbytedbError::ShardMap(
                    format!("measurement {db}.{rp}.{} not bootstrapped", p.measurement).into(),
                ));
            }
        }
    }

    let mut local = Vec::new();
    let mut forward: HashMap<u64, Vec<Point>> = HashMap::new();

    for p in points {
        let sid = series_id_for_point(&p);
        // TiDB-like routing: only the region primary writes locally; replica
        // peers and non-peers forward to the primary via /internal/shard/write.
        let route = ctx
            .location_cache
            .locate_route(&map, db, rp, &p.measurement, sid, ctx.node_id)
            .ok_or_else(|| {
                HyperbytedbError::ShardMap(format!("no region for series_id {sid}").into())
            })?;
        if route.self_is_peer && route.primary == ctx.node_id {
            local.push(p);
        } else {
            forward.entry(route.region_id).or_default().push(p);
        }
    }

    Ok(PointBuckets { local, forward })
}

pub async fn forward_shard_write_to_region(
    ctx: &ShardRoutingContext,
    db: &str,
    rp: &str,
    precision: Option<&str>,
    points: &[Point],
) -> Result<(), HyperbytedbError> {
    match try_forward_shard_write_to_region(ctx, db, rp, precision, points).await {
        Err(HyperbytedbError::StaleShardEpoch { region_id }) => {
            let meas = points
                .first()
                .map(|p| p.measurement.as_str())
                .ok_or_else(|| HyperbytedbError::ShardMap("empty forward batch".into()))?;
            reload_region(ctx, db, rp, meas, region_id).await?;
            try_forward_shard_write_to_region(ctx, db, rp, precision, points)
                .await
                .map_err(|e| match e {
                    HyperbytedbError::StaleShardEpoch { .. } => {
                        HyperbytedbError::StaleShardEpoch { region_id }
                    }
                    other => other,
                })
        }
        other => other,
    }
}

async fn try_forward_shard_write_to_region(
    ctx: &ShardRoutingContext,
    db: &str,
    rp: &str,
    precision: Option<&str>,
    points: &[Point],
) -> Result<(), HyperbytedbError> {
    use crate::application::line_protocol::encode_points_to_line_protocol;
    use crate::domain::database::Precision;

    if points.is_empty() {
        return Ok(());
    }

    let sid = series_id_for_point(&points[0]);
    let map = ctx.shard_map.snapshot().await?;
    let meas = &points[0].measurement;
    let region = ctx
        .location_cache
        .locate(&map, db, rp, meas, sid)
        .ok_or_else(|| HyperbytedbError::ShardMap("region missing for forward".into()))?
        .clone();

    if !ctx
        .location_cache
        .check_epoch(&map, db, rp, meas, sid, region.epoch)
    {
        ctx.location_cache
            .invalidate_measurement(&MeasurementKey::new(db, rp, meas));
        return Err(HyperbytedbError::StaleShardEpoch {
            region_id: region.region_id,
        });
    }

    let precision_val = Precision::from_str_opt(precision);
    let body = encode_points_to_line_protocol(points, precision_val)?;
    let req = ShardWriteRequest {
        db: db.to_string(),
        rp: rp.to_string(),
        precision: precision.map(|s| s.to_string()),
        epoch: region.epoch,
        region_id: region.region_id,
        body,
    };

    let region_id = region.region_id;
    let meas_owned = meas.clone();

    scatter_to_region_peers(
        ctx,
        &region,
        RegionTargetRole::Write,
        ScatterKind::Write,
        |peer_id, addr, timeout| {
            let req = req.clone();
            let addr = addr.to_string();
            let db = db.to_string();
            let rp = rp.to_string();
            let meas = meas_owned.clone();
            async move {
                let url = format!("http://{addr}/internal/shard/write");
                let resp = ctx
                    .peer_client
                    .http_client()
                    .post(&url)
                    .json(&req)
                    .timeout(timeout)
                    .send()
                    .await
                    .map_err(|e| HyperbytedbError::PeerUnreachable(e.to_string()))?;

                if resp.status() == reqwest::StatusCode::CONFLICT {
                    ctx.location_cache
                        .invalidate_measurement(&MeasurementKey::new(&db, &rp, &meas));
                    return Err(HyperbytedbError::StaleShardEpoch { region_id });
                }
                if !resp.status().is_success() {
                    return Err(HyperbytedbError::PeerUnreachable(format!(
                        "forward write to peer {peer_id} failed: {}",
                        resp.status()
                    )));
                }
                Ok(())
            }
        },
    )
    .await
}

pub async fn region_replication_targets(
    ctx: &ShardRoutingContext,
    region: &ShardRegion,
) -> Vec<u64> {
    let membership = ctx.peer_client.membership().read().await;
    active_region_peer_targets(region, ctx.node_id, &membership)
}

pub async fn propose_shard_map_op_via_raft(
    leader_addr: &str,
    op: ShardMapOp,
) -> Result<ClusterResponse, HyperbytedbError> {
    let url = format!("http://{leader_addr}/cluster/raft/client-write");
    let req = ClusterRequest::ShardMapMutation(Box::new(op));
    let client = reqwest::Client::new();
    let resp = client
        .post(&url)
        .json(&req)
        .timeout(Duration::from_secs(5))
        .send()
        .await
        .map_err(|e| HyperbytedbError::PeerUnreachable(e.to_string()))?;
    if !resp.status().is_success() {
        return Err(HyperbytedbError::ShardMap(
            format!("raft client_write failed: {}", resp.status()).into(),
        ));
    }
    resp.json()
        .await
        .map_err(|e| HyperbytedbError::ShardMap(e.to_string().into()))
}

/// Per-region outcome counts for one sharded delete fan-out.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ShardDeleteOutcome {
    /// Regions whose primary applied the delete locally.
    pub applied: usize,
    /// Regions skipped because their primary was inactive or unknown.
    pub skipped: usize,
    /// Regions whose primary rejected or failed the delete RPC.
    pub failed: usize,
}

impl ShardDeleteOutcome {
    pub fn fully_applied(&self) -> bool {
        self.skipped == 0 && self.failed == 0
    }
}

pub async fn scatter_delete_to_regions(
    ctx: &ShardRoutingContext,
    metadata: &dyn MetadataPort,
    db: &str,
    rp: &str,
    measurement: &str,
    predicate: &str,
) -> Result<ShardDeleteOutcome, HyperbytedbError> {
    use crate::domain::sharding::ShardDeleteRequest;

    let map = ctx.shard_map.snapshot().await?;
    let Some(space) = map.space(db, rp, measurement) else {
        return Ok(ShardDeleteOutcome::default());
    };

    let mut outcome = ShardDeleteOutcome::default();

    for region in &space.regions {
        if region.primary == ctx.node_id {
            if !predicate.is_empty() {
                metadata
                    .store_tombstone(db, rp, measurement, predicate)
                    .await?;
            }
            metadata
                .delete_series_matching(db, rp, Some(measurement), predicate)
                .await?;
            counter!("hyperbytedb_shard_delete_applied_total").increment(1);
            outcome.applied += 1;
            continue;
        }

        let membership = ctx.peer_client.membership().read().await;
        if !is_active_peer(&membership, region.primary) {
            tracing::warn!(
                region_id = region.region_id,
                primary = region.primary,
                "shard delete skipped: region primary not active"
            );
            outcome.skipped += 1;
            counter!("hyperbytedb_shard_delete_skipped_total").increment(1);
            continue;
        }
        let Some(addr) = peer_addr(&membership, region.primary) else {
            tracing::warn!(
                region_id = region.region_id,
                "shard delete skipped: unknown primary address"
            );
            outcome.skipped += 1;
            counter!("hyperbytedb_shard_delete_skipped_total").increment(1);
            continue;
        };
        drop(membership);

        let req = ShardDeleteRequest {
            db: db.to_string(),
            rp: rp.to_string(),
            measurement: measurement.to_string(),
            epoch: region.epoch,
            region_id: region.region_id,
            predicate: if predicate.is_empty() {
                None
            } else {
                Some(predicate.to_string())
            },
        };
        let timeout = Duration::from_millis(ctx.config.scatter_peer_timeout_ms.max(1));
        let url = format!("http://{addr}/internal/shard/delete");
        let resp = ctx
            .peer_client
            .http_client()
            .post(&url)
            .json(&req)
            .timeout(timeout)
            .send()
            .await;
        match resp {
            Ok(r) if r.status() == reqwest::StatusCode::CONFLICT => {
                refresh_location_cache(ctx).await?;
                outcome.failed += 1;
                counter!("hyperbytedb_shard_delete_failures_total", "reason" => "stale_epoch")
                    .increment(1);
                tracing::warn!(
                    region_id = region.region_id,
                    "shard delete stale epoch; retry on next request"
                );
            }
            Ok(r) if !r.status().is_success() => {
                outcome.failed += 1;
                counter!("hyperbytedb_shard_delete_failures_total", "reason" => "http_status")
                    .increment(1);
                tracing::warn!(
                    status = %r.status(),
                    region_id = region.region_id,
                    "shard delete scatter failed for region"
                );
            }
            Err(e) => {
                outcome.failed += 1;
                counter!("hyperbytedb_shard_delete_failures_total", "reason" => "transport")
                    .increment(1);
                tracing::warn!(
                    error = %e,
                    region_id = region.region_id,
                    "shard delete scatter failed for region"
                );
            }
            _ => {
                outcome.applied += 1;
            }
        }
    }

    // Partial deletes are otherwise invisible: every caller currently drops
    // this result, so surface degraded fan-outs loudly.
    if !outcome.fully_applied() {
        tracing::warn!(
            db = %db,
            rp = %rp,
            measurement = %measurement,
            applied = outcome.applied,
            skipped = outcome.skipped,
            failed = outcome.failed,
            "sharded delete completed partially across regions"
        );
        return Err(HyperbytedbError::ShardMap(
            format!(
                "sharded delete incomplete: applied={} skipped={} failed={}",
                outcome.applied, outcome.skipped, outcome.failed
            )
            .into(),
        ));
    }
    Ok(outcome)
}

#[cfg(test)]
mod scatter_tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use axum::Router;
    use axum::http::StatusCode;
    use axum::routing::post;
    use tokio::task::JoinHandle;

    use crate::adapters::cluster::peer_client::PeerClient;
    use crate::adapters::cluster::replication_log::ReplicationLog;
    use crate::adapters::sharding::rocksdb_shard_map::RocksDbShardMap;
    use crate::application::shard_peer_resolution::{RegionTargetRole, ScatterKind};
    use crate::config::ShardingConfig;
    use crate::domain::cluster::membership::{
        ClusterMembership, NodeInfo, NodeState, SharedMembership, new_shared,
    };
    use crate::domain::sharding::{ShardEpoch, ShardRegion};
    use crate::error::HyperbytedbError;

    use super::*;

    fn node(id: u64, addr: &str, state: NodeState) -> NodeInfo {
        NodeInfo {
            node_id: id,
            addr: addr.to_string(),
            state,
            joined_at: 0,
            last_heartbeat: 0,
            needs_sync: false,
            consecutive_misses: 0,
        }
    }

    async fn spawn_mock_peer(status: StatusCode, body: &'static str) -> (String, JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let app = Router::new().route(
            "/internal/shard/query",
            post(move || async move { (status, body) }),
        );
        let handle = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        (addr, handle)
    }

    async fn spawn_counting_peer(
        counter: Arc<AtomicUsize>,
        status: StatusCode,
    ) -> (String, JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let app = Router::new().route(
            "/internal/shard/query",
            post(move || {
                let counter = counter.clone();
                async move {
                    counter.fetch_add(1, Ordering::SeqCst);
                    (status, "fail")
                }
            }),
        );
        let handle = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        (addr, handle)
    }

    fn test_ctx(
        membership: SharedMembership,
        config: ShardingConfig,
        node_id: u64,
    ) -> ShardRoutingContext {
        let repl_dir = tempfile::tempdir().unwrap();
        let repl_log = Arc::new(ReplicationLog::open(repl_dir.path()).unwrap());
        let peer_client = Arc::new(PeerClient::new(
            node_id,
            "127.0.0.1:9".into(),
            membership,
            repl_log,
            1,
            1024,
            1,
            1024,
        ));
        let map_dir = tempfile::tempdir().unwrap();
        let shard_map = Arc::new(RocksDbShardMap::open(map_dir.path(), true).unwrap());
        ShardRoutingContext {
            shard_map,
            location_cache: Arc::new(ShardLocationCache::new()),
            config,
            node_id,
            peer_client,
            region_write_stats: Arc::new(RegionWriteStats::new()),
        }
    }

    fn sample_region() -> ShardRegion {
        ShardRegion {
            region_id: 1,
            start: 0,
            end: u64::MAX,
            epoch: ShardEpoch::default(),
            peers: vec![1, 2],
            primary: 1,
            last_split_at: 0,
            transfer_verified: None,
            transfer_first_seen: None,
        }
    }

    fn membership_with(addrs: &[(u64, &str)]) -> SharedMembership {
        let mut m = ClusterMembership::new();
        for (id, addr) in addrs {
            m.add_node(node(*id, addr, NodeState::Active));
        }
        new_shared(m)
    }

    #[tokio::test]
    async fn scatter_invokes_callback_with_peer_addrs() {
        let (addr1, h1) = spawn_mock_peer(StatusCode::INTERNAL_SERVER_ERROR, "fail").await;
        let (addr2, h2) = spawn_mock_peer(StatusCode::OK, "ok").await;
        let membership = membership_with(&[(1, &addr1), (2, &addr2)]);
        let config = ShardingConfig {
            scatter_max_peer_attempts: 3,
            ..Default::default()
        };
        let ctx = test_ctx(membership, config, 99);
        let region = sample_region();

        let mut attempts = Vec::new();
        let result = scatter_to_region_peers(
            &ctx,
            &region,
            RegionTargetRole::Read,
            ScatterKind::Query,
            |peer_id, addr, _timeout| {
                attempts.push((peer_id, addr.to_string()));
                async move {
                    if peer_id == 1 {
                        Err(HyperbytedbError::PeerUnreachable("down".into()))
                    } else {
                        Ok("success")
                    }
                }
            },
        )
        .await;

        assert_eq!(result.unwrap(), "success");
        assert_eq!(attempts.len(), 2);
        assert_eq!(attempts[0].0, 1);
        assert_eq!(attempts[1].0, 2);
        h1.abort();
        h2.abort();
    }

    #[tokio::test]
    async fn scatter_stale_epoch_aborts_without_retry() {
        let (addr1, h1) = spawn_mock_peer(StatusCode::CONFLICT, "stale").await;
        let (addr2, h2) = spawn_mock_peer(StatusCode::OK, "ok").await;
        let membership = membership_with(&[(1, &addr1), (2, &addr2)]);
        let ctx = test_ctx(membership, ShardingConfig::default(), 99);
        let region = sample_region();

        let err = scatter_to_region_peers(
            &ctx,
            &region,
            RegionTargetRole::Read,
            ScatterKind::Query,
            |peer_id, _addr, _timeout| async move {
                if peer_id == 1 {
                    Err(HyperbytedbError::StaleShardEpoch { region_id: 1 })
                } else {
                    Ok("should not reach")
                }
            },
        )
        .await
        .unwrap_err();

        assert!(matches!(
            err,
            HyperbytedbError::StaleShardEpoch { region_id: 1 }
        ));
        h1.abort();
        h2.abort();
    }

    #[tokio::test]
    async fn scatter_all_peers_fail() {
        let (addr1, h1) = spawn_mock_peer(StatusCode::INTERNAL_SERVER_ERROR, "fail").await;
        let (addr2, h2) = spawn_mock_peer(StatusCode::INTERNAL_SERVER_ERROR, "fail").await;
        let membership = membership_with(&[(1, &addr1), (2, &addr2)]);
        let ctx = test_ctx(membership, ShardingConfig::default(), 99);
        let region = sample_region();

        let err = scatter_to_region_peers(
            &ctx,
            &region,
            RegionTargetRole::Read,
            ScatterKind::Query,
            |_peer_id, _addr, _timeout| async {
                Err::<&str, _>(HyperbytedbError::PeerUnreachable("down".into()))
            },
        )
        .await
        .unwrap_err();

        assert!(matches!(err, HyperbytedbError::PeerUnreachable(_)));
        h1.abort();
        h2.abort();
    }

    #[tokio::test]
    async fn scatter_respects_max_attempts() {
        let counter = Arc::new(AtomicUsize::new(0));
        let (addr1, h1) =
            spawn_counting_peer(counter.clone(), StatusCode::INTERNAL_SERVER_ERROR).await;
        let (addr2, h2) =
            spawn_counting_peer(counter.clone(), StatusCode::INTERNAL_SERVER_ERROR).await;
        let (addr3, h3) =
            spawn_counting_peer(counter.clone(), StatusCode::INTERNAL_SERVER_ERROR).await;
        let membership = membership_with(&[(1, &addr1), (2, &addr2), (3, &addr3)]);
        let config = ShardingConfig {
            scatter_max_peer_attempts: 2,
            ..Default::default()
        };
        let mut region = sample_region();
        region.peers = vec![1, 2, 3];
        let ctx = test_ctx(membership, config, 99);
        let attempts = counter.clone();

        let _: Result<(), _> = scatter_to_region_peers(
            &ctx,
            &region,
            RegionTargetRole::Read,
            ScatterKind::Query,
            move |_peer_id, _addr, _timeout| {
                let attempts = attempts.clone();
                async move {
                    attempts.fetch_add(1, Ordering::SeqCst);
                    Err(HyperbytedbError::PeerUnreachable("down".into()))
                }
            },
        )
        .await;

        assert_eq!(counter.load(Ordering::SeqCst), 2);
        h1.abort();
        h2.abort();
        h3.abort();
    }

    #[tokio::test]
    async fn build_bootstrap_op_uses_next_region_id() {
        let membership = membership_with(&[(1, "127.0.0.1:1"), (2, "127.0.0.1:2")]);
        let ctx = test_ctx(membership, ShardingConfig::default(), 1);

        let op_a = build_bootstrap_op(&ctx, "db", "autogen", "cpu")
            .await
            .unwrap();
        let region_a_id = match &op_a {
            ShardMapOp::BootstrapMeasurement { region, .. } => region.region_id,
            _ => panic!("expected bootstrap op"),
        };
        assert_eq!(region_a_id, 1);
        ctx.shard_map.apply_op(op_a).await.unwrap();

        let op_b = build_bootstrap_op(&ctx, "db", "autogen", "mem")
            .await
            .unwrap();
        let region_b_id = match &op_b {
            ShardMapOp::BootstrapMeasurement { region, .. } => region.region_id,
            _ => panic!("expected bootstrap op"),
        };
        assert_eq!(region_b_id, 2);
        assert_ne!(region_a_id, region_b_id);

        match op_b {
            ShardMapOp::BootstrapMeasurement { region, .. } => {
                assert!(
                    region.last_split_at > 0,
                    "bootstrap must stamp last_split_at so cooldown is not vacuously elapsed"
                );
            }
            _ => unreachable!(),
        }
    }

    #[tokio::test]
    async fn bootstrap_placement_spreads_across_measurements() {
        // 4-node membership, RF=2: the old lowest-ID-first ordering put every
        // new measurement's primary on node 1. Hash-ordered placement must
        // distribute primaries across measurements while staying
        // deterministic for a given key.
        let membership = membership_with(&[
            (1, "127.0.0.1:1"),
            (2, "127.0.0.1:2"),
            (3, "127.0.0.1:3"),
            (4, "127.0.0.1:4"),
        ]);
        let ctx = test_ctx(membership, ShardingConfig::default(), 99);

        let mut primaries = std::collections::HashSet::new();
        for i in 0..16u32 {
            let meas = format!("m{i}");
            let op = build_bootstrap_op(&ctx, "db", "autogen", &meas)
                .await
                .unwrap();
            let ShardMapOp::BootstrapMeasurement { region, .. } = &op else {
                panic!("expected bootstrap op");
            };
            // Deterministic: same key always yields the same peer set.
            let op_again = build_bootstrap_op(&ctx, "db", "autogen", &meas)
                .await
                .unwrap();
            let ShardMapOp::BootstrapMeasurement { region: again, .. } = &op_again else {
                panic!("expected bootstrap op");
            };
            assert_eq!(
                region.peers, again.peers,
                "placement must be deterministic for {meas}"
            );
            primaries.insert(region.primary);
        }
        assert!(
            primaries.len() >= 3,
            "primaries must spread across nodes, got {primaries:?}"
        );
    }

    #[test]
    fn effective_rf_never_exceeds_membership() {
        assert_eq!(effective_replication_factor(3, 1), 1);
        assert_eq!(effective_replication_factor(3, 2), 2);
        assert_eq!(effective_replication_factor(2, 4), 2);
        assert_eq!(effective_replication_factor(0, 3), 1);
        assert_eq!(effective_replication_factor(3, 0), 0);
    }

    #[tokio::test]
    async fn bootstrap_n1_peers_are_self() {
        let membership = membership_with(&[(1, "127.0.0.1:1")]);
        let config = ShardingConfig {
            replication_factor: 3,
            ..Default::default()
        };
        let ctx = test_ctx(membership, config, 1);

        let op = build_bootstrap_op(&ctx, "db", "autogen", "cpu")
            .await
            .unwrap();
        let ShardMapOp::BootstrapMeasurement { region, .. } = op else {
            panic!("expected bootstrap op");
        };
        assert_eq!(region.peers, vec![1], "n=1 must own the region");
        assert_eq!(region.primary, 1);
    }

    #[test]
    fn bootstrap_placement_matches_the_shared_placement_function() {
        // A measurement's first region starts at 0, so bootstrap placement is
        // exactly target_placement(key, 0, members, rf). If these diverge, a
        // freshly bootstrapped region is immediately "misplaced" and the
        // scheduler moves it on the first tick.
        use crate::domain::sharding::placement::target_placement;
        use crate::domain::sharding::types::MeasurementKey;

        let key = MeasurementKey::new("db", "autogen", "cpu");
        let members: Vec<u64> = vec![1, 2, 3, 4, 5];
        let placed = target_placement(&key, 0, &members, 3);
        assert_eq!(placed.len(), 3);
        assert!(members.contains(&placed[0]));

        // Order-independent: membership enumeration order must not change it.
        let mut reversed = members.clone();
        reversed.reverse();
        assert_eq!(placed, target_placement(&key, 0, &reversed, 3));

        // Different measurements land on different subsets, which is why
        // bootstrap hashes at all.
        let other = MeasurementKey::new("db", "autogen", "mem");
        assert_ne!(placed, target_placement(&other, 0, &members, 3));
    }
}
