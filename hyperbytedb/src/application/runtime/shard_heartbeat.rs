//! Periodic region heartbeat reports from store nodes to the Raft leader.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::watch;

use crate::adapters::cluster::peer_client::PeerClient;
use crate::adapters::cluster::raft::HyperbytedbRaft;
use crate::adapters::sharding::rocksdb_shard_map::RocksDbShardMap;
use crate::domain::cluster::membership::SharedMembership;
use crate::domain::sharding::RegionHeartbeat;
use crate::ports::metadata::MetadataPort;
use crate::ports::sharding::ShardMapPort;

pub async fn run_region_heartbeat_reporter(
    node_id: u64,
    peer_client: Arc<PeerClient>,
    membership: SharedMembership,
    shard_map: Arc<RocksDbShardMap>,
    raft: HyperbytedbRaft,
    metadata: Arc<dyn MetadataPort>,
    interval: Duration,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            _ = ticker.tick() => {
                if let Err(e) = report_once(
                    node_id,
                    &peer_client,
                    &membership,
                    &shard_map,
                    &raft,
                    metadata.as_ref(),
                ).await {
                    tracing::debug!(error = %e, "region heartbeat report failed");
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

async fn report_once(
    node_id: u64,
    peer_client: &PeerClient,
    membership: &SharedMembership,
    shard_map: &RocksDbShardMap,
    raft: &HyperbytedbRaft,
    metadata: &dyn MetadataPort,
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
            let series_count = metadata
                .list_series_ids(&space.key.db, &space.key.rp, &space.key.measurement)
                .await
                .map(|ids| ids.len() as u64)
                .unwrap_or(0);
            let hb = RegionHeartbeat {
                region_id: region.region_id,
                series_count,
                approx_bytes: 0,
                write_qps: 0,
                epoch: region.epoch,
            };
            let url = format!("http://{leader_addr}/internal/shard/heartbeat");
            peer_client
                .http_client()
                .post(&url)
                .json(&hb)
                .timeout(Duration::from_secs(5))
                .send()
                .await
                .map_err(|e| e.to_string())?;
        }
    }
    Ok(())
}
