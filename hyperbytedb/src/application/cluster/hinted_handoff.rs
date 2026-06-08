use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use crate::adapters::cluster::peer_client::PeerClient;
use crate::domain::cluster::membership::{NodeState, SharedMembership};

/// Polls membership state periodically: when a previously-disconnected
/// peer transitions to Active, drain the hinted handoff queue for that peer
/// and trigger a WAL catch-up on the reconnected node.
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
                        tracing::info!(
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
                                tracing::info!(
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
