use std::sync::Arc;
use std::time::Duration;

use crate::adapters::http::router::AppState;
use crate::application::cluster::sync_manifest;
use crate::domain::cluster::membership::is_departing;
use crate::domain::cluster::sync::{RegionWatermark, SyncManifest};

/// Periodically checks if this node is the Raft leader; if so, queries each
/// follower's sync manifest and triggers a re-sync on significantly lagging nodes.
pub async fn run_leader_replication_monitor(
    state: Arc<AppState>,
    node_id: u64,
    shutdown_rx: tokio::sync::watch::Receiver<bool>,
) {
    // Wait for the cluster to stabilize before monitoring
    tokio::time::sleep(Duration::from_secs(30)).await;

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .unwrap_or_else(|_| reqwest::Client::new());

    loop {
        if *shutdown_rx.borrow() {
            return;
        }

        tokio::time::sleep(Duration::from_secs(30)).await;

        if *shutdown_rx.borrow() {
            return;
        }

        // Only the Raft leader runs this check
        let is_leader = if let Some(ref raft) = state.raft {
            let metrics = raft.metrics().borrow().clone();
            metrics.current_leader == Some(node_id)
        } else {
            false
        };

        if !is_leader {
            continue;
        }

        let membership = match &state.membership {
            Some(m) => m.clone(),
            None => continue,
        };

        // Get local manifest as the baseline (WAL watermark + catalog).
        let local_manifest = match sync_manifest::build_manifest(
            node_id,
            &state.metadata,
            &state.wal,
            state
                .shard_map
                .as_ref()
                .map(|m| m.as_ref() as &dyn crate::ports::sharding::ShardMapPort),
        )
        .await
        {
            Ok(m) => m,
            Err(e) => {
                tracing::debug!(error = %e, "leader monitor: could not build local manifest");
                continue;
            }
        };

        let peers = {
            let m = membership.read().await;
            m.all_peers(node_id)
                .iter()
                .filter(|p| !is_departing(p))
                .map(|p| (p.node_id, p.addr.clone(), p.needs_sync))
                .collect::<Vec<_>>()
        };

        for (peer_id, peer_addr, needs_sync) in &peers {
            let url = format!("http://{}/internal/sync/manifest", peer_addr);
            let resp = match client.get(&url).send().await {
                Ok(r) => r,
                Err(_) => continue,
            };

            if !resp.status().is_success() {
                continue;
            }

            let peer_manifest: SyncManifest = match resp.json().await {
                Ok(m) => m,
                Err(_) => continue,
            };

            let wal_gap = local_manifest
                .wal_last_seq
                .saturating_sub(peer_manifest.wal_last_seq);

            metrics::gauge!("hyperbytedb_replication_lag_files", "peer_id" => peer_id.to_string())
                .set(0.0);
            metrics::gauge!("hyperbytedb_replication_lag_bytes", "peer_id" => peer_id.to_string())
                .set(0.0);
            metrics::gauge!("hyperbytedb_replication_lag_wal_seq", "peer_id" => peer_id.to_string())
                .set(wal_gap as f64);

            let mut region_lag = false;
            if let Some(local_db) = local_manifest.databases.first() {
                for local_meas in &local_db.measurements {
                    let Some(peer_db) = peer_manifest
                        .databases
                        .iter()
                        .find(|d| d.name == local_db.name)
                    else {
                        continue;
                    };
                    let Some(peer_meas) = peer_db
                        .measurements
                        .iter()
                        .find(|m| m.name == local_meas.name && m.rp == local_meas.rp)
                    else {
                        continue;
                    };
                    if peer_is_lagging_on_regions(
                        &local_meas.region_watermarks,
                        &peer_meas.region_watermarks,
                    ) {
                        region_lag = true;
                    }
                }
            }

            if !*needs_sync && !region_lag && (state.shard_map.is_some() || wal_gap == 0) {
                continue;
            }

            // `wal_last_seq` is a per-node counter incremented on every local
            // append. Once sharding gives nodes disjoint region sets they
            // legitimately diverge -- observed live at 622 on one node against
            // ~220 on the rest -- so a cross-node difference says nothing about
            // lag. Worse, acting on it makes the follower request a sequence
            // range that never existed in the peer's counter space, the peer
            // truthfully returns nothing, catch-up fails closed, and the loop
            // repeats forever. Only an explicit `needs_sync` or a real
            // per-region lag may trigger a re-sync under sharding.
            let sharded = state.shard_map.is_some();
            let gap_is_meaningful = !sharded && wal_gap > 0;
            let should_trigger = *needs_sync || gap_is_meaningful || region_lag;

            if should_trigger {
                tracing::warn!(
                    peer_id = peer_id,
                    wal_gap = wal_gap,
                    needs_sync = needs_sync,
                    "follower behind on WAL or flagged for sync, triggering re-sync"
                );

                let trigger_url = format!("http://{}/internal/sync/trigger", peer_addr);
                match client.post(&trigger_url).send().await {
                    Ok(resp) if resp.status().is_success() => {
                        tracing::info!(peer_id = peer_id, "re-sync triggered on lagging follower");
                        let mut m = membership.write().await;
                        m.set_needs_sync(*peer_id, false);
                    }
                    Ok(resp) => {
                        tracing::warn!(
                            peer_id = peer_id,
                            status = %resp.status(),
                            "failed to trigger re-sync on follower"
                        );
                    }
                    Err(e) => {
                        tracing::warn!(
                            peer_id = peer_id,
                            error = %e,
                            "failed to reach follower for re-sync trigger"
                        );
                    }
                }
            }
        }
    }
}

/// Pure decision mirroring the `region_lag` comparison performed inline in
/// the monitor loop above (lines ~107-142). Extracted here so the bug can be
/// exercised without a live cluster.
///
/// `RegionWatermark` entries only exist in a manifest for a region where the
/// *reporting* node is the `primary` (see
/// `sync_manifest::build_manifest`, which filters `r.primary == node_id`
/// before ever pushing a `RegionWatermark`). So a peer's manifest carries NO
/// entry for any region where that peer is a healthy secondary replica, or
/// has no involvement in the region at all -- that is the expected, correct
/// shape of the data, not evidence the peer is behind.
///
/// The current inline logic instead treats a missing peer entry as "peer is
/// at watermark 0" via `.unwrap_or(0)`, which makes it indistinguishable
/// from "peer never applied anything for this region" -- so a peer that is
/// perfectly caught up (or not a replica of the region at all) is flagged as
/// lagging on literally every leader-primary region, every single monitor
/// cycle, forever, driving the infinite re-sync loop even when `wal_gap == 0`
/// and `needs_sync == false`. See `region_lag_tests` below.
pub(crate) fn peer_is_lagging_on_regions(
    local_regions: &[RegionWatermark],
    peer_regions: &[RegionWatermark],
) -> bool {
    local_regions.iter().any(|rw| {
        // A region the peer does not report is a region it does not own, not a
        // region it is infinitely behind on. `sync_manifest` emits watermarks
        // only for regions where the node is PRIMARY, so a peer never reports
        // one the leader primaries -- reading that silence as watermark 0 made
        // every peer look behind on every region, every cycle, forever.
        peer_regions
            .iter()
            .find(|p| p.region_id == rw.region_id)
            .is_some_and(|p| rw.wal_watermark.saturating_sub(p.wal_watermark) > 0)
    })
}

#[cfg(test)]
mod region_lag_tests {
    use super::{RegionWatermark, peer_is_lagging_on_regions};

    /// Regression for the infinite re-sync loop reproduced live on the
    /// `feat/linear-scaling-p1` kind cluster: the leader (node 2) triggers a
    /// re-sync on every one of its 5 peers, every ~30s, indefinitely, with
    /// `wal_gap == 0` and `needs_sync == false` in 100% of the ~4000 captured
    /// events -- the only remaining condition in
    /// `wal_gap > 0 || needs_sync || region_lag` that can be true every time
    /// is `region_lag`.
    ///
    /// A peer that is the up-to-date SECONDARY replica for a region (or does
    /// not host the region at all) never publishes a watermark for it, by
    /// design. The leader must not treat that absence as "peer is behind".
    /// Today it does -- this test fails against the current logic and should
    /// pass once the fix lands (skip the comparison when the peer has no
    /// entry for the region, instead of defaulting to watermark 0).
    #[test]
    fn healthy_secondary_replica_is_not_flagged_as_lagging() {
        let local = vec![RegionWatermark {
            region_id: 4,
            wal_watermark: 216,
        }];
        // Peer is current for region 4 (or simply isn't a replica of it) --
        // its manifest has no entry for region 4 at all, matching what
        // `sync_manifest::build_manifest` actually produces for a non-primary
        // node.
        let peer: Vec<RegionWatermark> = vec![];

        assert!(
            !peer_is_lagging_on_regions(&local, &peer),
            "a peer reporting no watermark for a region it doesn't \
             primary-own must not be treated as lagging on that region"
        );
    }

    /// Sanity check: a peer that DOES report a region watermark lower than
    /// the leader's is genuinely lagging and must still be flagged.
    #[test]
    fn peer_reporting_lower_watermark_is_flagged_as_lagging() {
        let local = vec![RegionWatermark {
            region_id: 4,
            wal_watermark: 216,
        }];
        let peer = vec![RegionWatermark {
            region_id: 4,
            wal_watermark: 200,
        }];

        assert!(peer_is_lagging_on_regions(&local, &peer));
    }
}
