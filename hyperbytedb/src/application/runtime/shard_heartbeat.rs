//! Periodic region heartbeat reports from store nodes to the Raft leader.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use metrics::counter;
use reqwest::StatusCode;
use tokio::sync::watch;

use crate::adapters::cluster::peer_client::PeerClient;
use crate::adapters::cluster::raft::HyperbytedbRaft;
use crate::adapters::sharding::rocksdb_shard_map::RocksDbShardMap;
use crate::application::runtime::RegionWriteStats;
use crate::domain::chdb_naming::unquoted_table_name;
use crate::domain::cluster::membership::SharedMembership;
use crate::domain::sharding::{RegionHeartbeat, ShardMap, ShardRegion};
use crate::ports::metadata::MetadataPort;
use crate::ports::query::QueryPort;
use crate::ports::sharding::ShardMapPort;

/// Byte estimates are recomputed at most every `ESTIMATE_REFRESH_TICKS` ticks —
/// the underlying `SELECT count()` scans the whole fact table per region, so
/// running it on every 10s heartbeat is disproportionate for a heuristic used
/// only by the rebalancer.
const ESTIMATE_REFRESH_TICKS: u32 = 15;

type ByteEstimateKey = (String, String, String, u64);

#[derive(Default)]
struct ByteEstimateCache {
    /// (db, rp, measurement, region_id) -> (approx_bytes, computed_at_tick)
    values: HashMap<ByteEstimateKey, (u64, u32)>,
    tick: u32,
}

#[allow(clippy::too_many_arguments)]
pub async fn run_region_heartbeat_reporter(
    node_id: u64,
    peer_client: Arc<PeerClient>,
    membership: SharedMembership,
    shard_map: Arc<RocksDbShardMap>,
    raft: HyperbytedbRaft,
    metadata: Arc<dyn MetadataPort>,
    query_port: Arc<dyn QueryPort>,
    region_write_stats: Arc<RegionWriteStats>,
    interval: Duration,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut byte_estimates = ByteEstimateCache::default();

    loop {
        tokio::select! {
            _ = ticker.tick() => {
                byte_estimates.tick = byte_estimates.tick.wrapping_add(1);
                if let Err(e) = report_once(
                    node_id,
                    &peer_client,
                    &membership,
                    &shard_map,
                    &raft,
                    metadata.as_ref(),
                    query_port.as_ref(),
                    region_write_stats.as_ref(),
                    interval,
                    &mut byte_estimates,
                ).await {
                    tracing::warn!(error = %e, "region heartbeat report failed");
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

#[allow(clippy::too_many_arguments)]
async fn report_once(
    node_id: u64,
    peer_client: &PeerClient,
    membership: &SharedMembership,
    shard_map: &RocksDbShardMap,
    raft: &HyperbytedbRaft,
    metadata: &dyn MetadataPort,
    query_port: &dyn QueryPort,
    region_write_stats: &RegionWriteStats,
    interval: Duration,
    byte_estimates: &mut ByteEstimateCache,
) -> Result<(), String> {
    let map = shard_map.snapshot().await.map_err(|e| e.to_string())?;
    let leader_id = raft
        .metrics()
        .borrow()
        .current_leader
        .ok_or_else(|| "no raft leader".to_string())?;

    let leader_addr = {
        let m = membership.read().await;
        m.get_node(leader_id)
            .map(|n| n.addr.clone())
            .ok_or_else(|| "leader addr unknown".to_string())?
    };

    for space in map.spaces.values() {
        for region in &space.regions {
            if !region.peers.contains(&node_id) {
                continue;
            }
            let series_count = match metadata
                .count_series_ids_in_range(
                    &space.key.db,
                    &space.key.rp,
                    &space.key.measurement,
                    region.start,
                    region.end,
                )
                .await
            {
                Ok(n) => n,
                Err(e) => {
                    tracing::warn!(
                        db = %space.key.db,
                        rp = %space.key.rp,
                        measurement = %space.key.measurement,
                        region_id = region.region_id,
                        error = %e,
                        "heartbeat skipped: series count failed"
                    );
                    counter!("hyperbytedb_shard_heartbeat_series_count_errors_total").increment(1);
                    continue;
                }
            };
            let write_qps = region_write_stats.take_qps(region.region_id, interval);

            let est_key: ByteEstimateKey = (
                space.key.db.clone(),
                space.key.rp.clone(),
                space.key.measurement.clone(),
                region.region_id,
            );
            let approx_bytes = match byte_estimates.values.get(&est_key) {
                Some((bytes, at_tick))
                    if byte_estimates.tick.wrapping_sub(*at_tick) < ESTIMATE_REFRESH_TICKS =>
                {
                    *bytes
                }
                _ => {
                    let bytes = estimate_region_bytes(
                        query_port,
                        &space.key.db,
                        &space.key.rp,
                        &space.key.measurement,
                        region.start,
                        region.end,
                        series_count,
                    )
                    .await;
                    byte_estimates.values.retain(|_, (_, t)| {
                        byte_estimates.tick.wrapping_sub(*t) < ESTIMATE_REFRESH_TICKS * 4
                    });
                    byte_estimates
                        .values
                        .insert(est_key, (bytes, byte_estimates.tick));
                    bytes
                }
            };

            let mut hb = RegionHeartbeat {
                region_id: region.region_id,
                node_id,
                series_count,
                approx_bytes,
                write_qps,
                epoch: region.epoch,
            };
            let url = format!("http://{leader_addr}/internal/shard/heartbeat");
            if let Err(reason) = post_region_heartbeat(peer_client, &url, &hb).await {
                if reason == "stale_epoch" {
                    let fresh_map = shard_map.snapshot().await.map_err(|e| e.to_string())?;
                    if let Some(fresh) = lookup_region(
                        &fresh_map,
                        &space.key.db,
                        &space.key.rp,
                        &space.key.measurement,
                        region.region_id,
                    ) {
                        hb.epoch = fresh.epoch;
                        if let Err(retry_reason) =
                            post_region_heartbeat(peer_client, &url, &hb).await
                        {
                            record_heartbeat_failure(retry_reason);
                        }
                    } else {
                        record_heartbeat_failure(reason);
                    }
                } else {
                    record_heartbeat_failure(reason);
                }
            }
        }
    }
    Ok(())
}

fn lookup_region<'a>(
    map: &'a ShardMap,
    db: &str,
    rp: &str,
    measurement: &str,
    region_id: u64,
) -> Option<&'a ShardRegion> {
    map.space(db, rp, measurement)?
        .regions
        .iter()
        .find(|r| r.region_id == region_id)
}

async fn post_region_heartbeat(
    peer_client: &PeerClient,
    url: &str,
    hb: &RegionHeartbeat,
) -> Result<(), &'static str> {
    let resp = peer_client
        .http_client()
        .post(url)
        .json(hb)
        .timeout(Duration::from_secs(5))
        .send()
        .await
        .map_err(|_| "transport")?;

    if resp.status() == StatusCode::CONFLICT {
        return Err("stale_epoch");
    }
    if !resp.status().is_success() {
        return Err("http_status");
    }
    Ok(())
}

fn record_heartbeat_failure(reason: &'static str) {
    counter!(
        "hyperbytedb_shard_heartbeat_failures_total",
        "reason" => reason,
    )
    .increment(1);
    tracing::warn!(reason, "region heartbeat rejected");
}

#[allow(clippy::too_many_arguments)]
async fn estimate_region_bytes(
    query_port: &dyn QueryPort,
    db: &str,
    rp: &str,
    measurement: &str,
    start: u64,
    end: u64,
    series_count: u64,
) -> u64 {
    let table = unquoted_table_name(db, rp, measurement);
    let range = if end == u64::MAX {
        format!("series_id >= {start}")
    } else {
        format!("series_id >= {start} AND series_id < {end}")
    };
    let sql = format!(
        "SELECT count() AS c FROM `{}` WHERE {range} FORMAT JSONEachRow",
        table.as_str()
    );
    let row_count = query_port
        .execute_sql(&sql)
        .await
        .ok()
        .and_then(|raw| {
            raw.lines().next().and_then(|l| {
                serde_json::from_str::<serde_json::Value>(l)
                    .ok()
                    .and_then(|v| v.get("c").and_then(|c| c.as_u64()))
            })
        })
        .unwrap_or(0);

    row_count.saturating_mul(128) + series_count.saturating_mul(64)
}
