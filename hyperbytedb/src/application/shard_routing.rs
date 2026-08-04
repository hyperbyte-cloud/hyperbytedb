//! Shard routing helpers: bootstrap, partition, forward.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use crate::adapters::cluster::peer_client::PeerClient;
use crate::adapters::cluster::raft::types::{ClusterRequest, ClusterResponse};
use crate::adapters::sharding::rocksdb_shard_map::RocksDbShardMap;
use crate::config::ShardingConfig;
use crate::domain::point::Point;
use crate::domain::series::series_id_for_point;
use crate::domain::sharding::{
    MeasurementKey, ShardBootstrapRequest, ShardEpoch, ShardMapOp, ShardRegion, ShardWriteRequest,
};
use crate::domain::sharding::ShardLocationCache;
use crate::error::HyperbytedbError;
use crate::ports::metadata::MetadataPort;
use crate::ports::sharding::ShardMapPort;

pub struct ShardRoutingContext {
    pub shard_map: Arc<RocksDbShardMap>,
    pub location_cache: Arc<ShardLocationCache>,
    pub config: ShardingConfig,
    pub node_id: u64,
    pub peer_client: Arc<PeerClient>,
}

pub struct PointBuckets {
    pub local: Vec<Point>,
    /// primary_node_id -> points
    pub forward: HashMap<u64, Vec<Point>>,
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
            return Err(HyperbytedbError::ShardMap(format!(
                "bootstrap failed: {}",
                resp.status()
            )));
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
    let peers = select_bootstrap_peers(ctx).await?;
    if peers.is_empty() {
        return Err(HyperbytedbError::ClusterUnavailable(
            "no active peers for shard bootstrap".into(),
        ));
    }
    let primary = peers[0];
    let region = ShardRegion {
        region_id: 1,
        start: 0,
        end: u64::MAX,
        epoch: ShardEpoch::default(),
        peers: peers.clone(),
        primary,
        last_split_at: 0,
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

async fn select_bootstrap_peers(ctx: &ShardRoutingContext) -> Result<Vec<u64>, HyperbytedbError> {
    let membership = ctx.peer_client.membership().read().await;
    let mut peers: Vec<u64> = membership
        .active_peers(ctx.node_id)
        .iter()
        .map(|n| n.node_id)
        .collect();
    peers.push(ctx.node_id);
    peers.sort_unstable();
    peers.dedup();
    peers.truncate(ctx.config.replication_factor.max(1));
    Ok(peers)
}

pub async fn partition_points(
    ctx: &ShardRoutingContext,
    db: &str,
    rp: &str,
    points: &[Point],
) -> Result<PointBuckets, HyperbytedbError> {
    let map = ctx.shard_map.snapshot().await?;
    let mut measurements: HashSet<String> = HashSet::new();
    for p in points {
        measurements.insert(p.measurement.clone());
    }
    for meas in &measurements {
        if map.space(db, rp, meas).is_none() {
            return Err(HyperbytedbError::ShardMap(format!(
                "measurement {db}.{rp}.{meas} not bootstrapped"
            )));
        }
    }

    let mut local = Vec::new();
    let mut forward: HashMap<u64, Vec<Point>> = HashMap::new();

    for p in points {
        let sid = series_id_for_point(p);
        let region = ctx
            .location_cache
            .locate(&map, db, rp, &p.measurement, sid)
            .ok_or_else(|| HyperbytedbError::ShardMap(format!("no region for series_id {sid}")))?;
        if region.peers.contains(&ctx.node_id) {
            local.push(p.clone());
        } else {
            forward.entry(region.primary).or_default().push(p.clone());
        }
    }

    Ok(PointBuckets { local, forward })
}

pub async fn forward_shard_write(
    ctx: &ShardRoutingContext,
    primary_id: u64,
    db: &str,
    rp: &str,
    precision: Option<&str>,
    points: &[Point],
) -> Result<(), HyperbytedbError> {
    use crate::application::line_protocol::encode_points_to_line_protocol;
    use crate::domain::database::Precision;

    let membership = ctx.peer_client.membership().read().await;
    let addr = membership
        .get_node(primary_id)
        .map(|n| n.addr.clone())
        .ok_or_else(|| HyperbytedbError::PeerUnreachable(format!("unknown primary {primary_id}")))?;

    let sid = series_id_for_point(&points[0]);
    let map = ctx.shard_map.snapshot().await?;
    let meas = &points[0].measurement;
    let region = ctx
        .location_cache
        .locate(&map, db, rp, meas, sid)
        .ok_or_else(|| HyperbytedbError::ShardMap("region missing for forward".into()))?;

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

    let url = format!("http://{addr}/internal/shard/write");
    let resp = ctx
        .peer_client
        .http_client()
        .post(&url)
        .json(&req)
        .send()
        .await
        .map_err(|e| HyperbytedbError::PeerUnreachable(e.to_string()))?;

    if resp.status() == reqwest::StatusCode::CONFLICT {
        ctx.location_cache.invalidate_measurement(&MeasurementKey::new(db, rp, meas));
        return Err(HyperbytedbError::StaleShardEpoch {
            region_id: region.region_id,
        });
    }
    if !resp.status().is_success() {
        return Err(HyperbytedbError::PeerUnreachable(format!(
            "forward write failed: {}",
            resp.status()
        )));
    }
    Ok(())
}

pub fn region_peer_targets(region: &ShardRegion, self_id: u64) -> Vec<u64> {
    region
        .peers
        .iter()
        .copied()
        .filter(|id| *id != self_id)
        .collect()
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
        return Err(HyperbytedbError::ShardMap(format!(
            "raft client_write failed: {}",
            resp.status()
        )));
    }
    resp.json()
        .await
        .map_err(|e| HyperbytedbError::ShardMap(e.to_string()))
}

pub async fn scatter_delete_to_regions(
    ctx: &ShardRoutingContext,
    metadata: &dyn MetadataPort,
    db: &str,
    rp: &str,
    measurement: &str,
    predicate: &str,
) -> Result<(), HyperbytedbError> {
    use crate::domain::sharding::ShardDeleteRequest;

    let map = ctx.shard_map.snapshot().await?;
    let Some(space) = map.space(db, rp, measurement) else {
        return Ok(());
    };

    for region in &space.regions {
        if region.primary == ctx.node_id {
            metadata
                .delete_series_matching(db, rp, Some(measurement), predicate)
                .await?;
            continue;
        }
        let membership = ctx.peer_client.membership().read().await;
        let addr = membership
            .get_node(region.primary)
            .map(|n| n.addr.clone())
            .ok_or_else(|| HyperbytedbError::PeerUnreachable("unknown primary".into()))?;
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
        let url = format!("http://{addr}/internal/shard/delete");
        let resp = ctx
            .peer_client
            .http_client()
            .post(&url)
            .json(&req)
            .send()
            .await
            .map_err(|e| HyperbytedbError::PeerUnreachable(e.to_string()))?;
        if !resp.status().is_success() {
            tracing::warn!(
                status = %resp.status(),
                region_id = region.region_id,
                "shard delete scatter failed for region"
            );
        }
    }
    Ok(())
}
