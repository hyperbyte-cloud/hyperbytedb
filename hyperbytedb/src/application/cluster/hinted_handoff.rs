use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use crate::adapters::cluster::peer_client::PeerClient;
use crate::domain::cluster::membership::{NodeState, SharedMembership};

/// Polls membership state periodically: when a previously-disconnected
/// peer transitions to Active, drain the hinted handoff queue for that peer
/// and trigger a WAL catch-up on the reconnected node. Hints queued for a
/// peer that never went away (the outbound replication queue was full) are
/// drained too, one drain per peer at a time.
pub async fn run_hinted_handoff_watcher(
    self_id: u64,
    peer_client: Arc<PeerClient>,
    membership: SharedMembership,
    mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
) {
    let sync_client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .unwrap_or_else(|_| reqwest::Client::new());

    let mut prev_states: HashMap<u64, NodeState> = HashMap::new();
    // Peers with a drain running, so ticks do not start a second one.
    let draining: Arc<std::sync::Mutex<std::collections::HashSet<u64>>> = Arc::default();
    let mut interval = tokio::time::interval(Duration::from_secs(5));
    interval.tick().await;

    tracing::info!("hinted handoff watcher started");

    loop {
        tokio::select! {
            _ = interval.tick() => {
                let current_nodes: Vec<(u64, String, NodeState)> = {
                    let m = membership.read().await;
                    m.all_peers(self_id)
                        .iter()
                        .map(|n| (n.node_id, n.addr.clone(), n.state))
                        .collect()
                };

                for (node_id, peer_addr, state) in &current_nodes {
                    let was_disconnected = prev_states
                        .get(node_id)
                        .map(|s| *s != NodeState::Active)
                        .unwrap_or(true);

                    if *state == NodeState::Active && was_disconnected {
                        tracing::debug!(
                            peer_id = node_id,
                            "peer became active, draining hinted handoff queue"
                        );
                        let pc = peer_client.clone();
                        let nid = *node_id;
                        tokio::spawn(async move {
                            pc.drain_hints_for_peer(nid).await;
                        });

                        let trigger_url = format!("http://{}/internal/sync/trigger", peer_addr);
                        match sync_client.post(&trigger_url).send().await {
                            Ok(resp) if resp.status().is_success() => {
                                tracing::debug!(
                                    peer_id = node_id,
                                    "triggered WAL catch-up on reconnected peer"
                                );
                            }
                            Ok(resp) => {
                                tracing::warn!(
                                    peer_id = node_id,
                                    status = %resp.status(),
                                    "failed to trigger WAL catch-up on reconnected peer"
                                );
                            }
                            Err(e) => {
                                tracing::warn!(
                                    peer_id = node_id,
                                    error = %e,
                                    "failed to reach reconnected peer for WAL catch-up"
                                );
                            }
                        }
                    }
                }

                // A peer that stayed Active never takes the branch above, so
                // hints queued because its replication queue was full were
                // never replayed: its copy silently stayed incomplete, and on
                // n3 a scale-up that moved primacy onto such copies hid 1.3M
                // of 90M points from queries.
                for (node_id, _addr, state) in &current_nodes {
                    if *state != NodeState::Active || peer_client.pending_hints(*node_id) == 0 {
                        continue;
                    }
                    if !draining.lock().unwrap_or_else(|p| p.into_inner()).insert(*node_id) {
                        continue;
                    }
                    let (pc, nid, draining) = (peer_client.clone(), *node_id, draining.clone());
                    tokio::spawn(async move {
                        pc.drain_hints_for_peer(nid).await;
                        draining.lock().unwrap_or_else(|p| p.into_inner()).remove(&nid);
                    });
                }

                prev_states.clear();
                for (node_id, _addr, state) in current_nodes {
                    prev_states.insert(node_id, state);
                }
            }
            _ = shutdown_rx.changed() => {
                if *shutdown_rx.borrow() {
                    tracing::info!("hinted handoff watcher shutting down");
                    break;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use crate::adapters::cluster::hinted_handoff::HintedHandoff;
    use crate::adapters::cluster::peer_client::PeerClient;
    use crate::adapters::cluster::replication_log::ReplicationLog;
    use crate::domain::cluster::membership::{ClusterMembership, NodeInfo, NodeState, new_shared};
    use crate::domain::cluster::replication_wire::ReplicationHintPayload;

    /// Hints queued for a peer that stayed Active (its replication queue was
    /// full) are replayed, not left until it next reconnects.
    #[tokio::test]
    async fn hints_for_a_peer_that_stayed_active_are_replayed() {
        let replayed = Arc::new(AtomicUsize::new(0));
        let seen = replayed.clone();
        let app = axum::Router::new().route(
            "/internal/replicate",
            axum::routing::post(move || {
                let seen = seen.clone();
                async move {
                    seen.fetch_add(1, Ordering::SeqCst);
                    axum::http::StatusCode::NO_CONTENT
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        let mut m = ClusterMembership::new();
        m.add_node(NodeInfo {
            node_id: 2,
            addr,
            state: NodeState::Active,
            joined_at: 0,
            last_heartbeat: 0,
            needs_sync: false,
            consecutive_misses: 0,
        });
        let membership = new_shared(m);
        let dir = tempfile::tempdir().unwrap();
        let log = Arc::new(ReplicationLog::open(dir.path()).unwrap());
        let hh = Arc::new(HintedHandoff::new(log.db().clone(), 100).unwrap());
        let pc = Arc::new(
            PeerClient::new(
                1,
                "127.0.0.1:1".into(),
                membership.clone(),
                log,
                3,
                64,
                2,
                1 << 20,
            )
            .with_hinted_handoff(hh.clone()),
        );
        // The peer is Active from the first tick on, so the reconnect branch
        // never fires; queue the hints after the watcher has seen it.
        let (tx, rx) = tokio::sync::watch::channel(false);
        let watcher = tokio::spawn(super::run_hinted_handoff_watcher(
            1,
            pc.clone(),
            membership,
            rx,
        ));
        tokio::time::sleep(Duration::from_secs(6)).await;
        for i in 0..3 {
            let hint = ReplicationHintPayload {
                database: "db".into(),
                retention_policy: "autogen".into(),
                precision: None,
                line_body: format!("cpu,host=h{i} v=1 1").into_bytes(),
            };
            hh.enqueue_hint(2, &hint).unwrap();
        }
        for _ in 0..40 {
            if hh.pending_count(2).unwrap() == 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
        let _ = tx.send(true);
        watcher.abort();
        assert_eq!(hh.pending_count(2).unwrap(), 0, "hints left queued");
        assert_eq!(replayed.load(Ordering::SeqCst), 3);
    }
}
