use std::sync::Arc;
use std::time::Duration;

use crate::adapters::http::router::AppState;
use crate::application::cluster::sync_manifest;
use crate::domain::cluster::sync::SyncManifest;

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
        let local_manifest =
            match sync_manifest::build_manifest(node_id, &state.metadata, &state.wal).await {
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

            if wal_gap == 0 && !*needs_sync {
                continue;
            }

            let should_trigger = *needs_sync || wal_gap > 0;

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
