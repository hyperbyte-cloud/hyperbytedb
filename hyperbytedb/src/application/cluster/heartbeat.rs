use std::time::Duration;

use crate::domain::cluster::membership::{NodeState, SharedMembership};

/// Logs cluster membership summary every 60 seconds for observability.
pub async fn run_heartbeat_logger(
    node_addr: String,
    self_id: u64,
    membership: SharedMembership,
    mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
) {
    let mut interval = tokio::time::interval(Duration::from_secs(60));
    interval.tick().await;
    loop {
        tokio::select! {
            _ = interval.tick() => {
                let m = membership.read().await;
                let active_peers = m.active_peers(self_id).len();
                let total_nodes = m.nodes.len();
                tracing::info!(
                    node_addr = %node_addr,
                    active_peers = active_peers,
                    total_nodes = total_nodes,
                    mode = "master-master",
                    "cluster heartbeat"
                );
            }
            _ = async {
                while !*shutdown_rx.borrow() {
                    shutdown_rx.changed().await.ok();
                }
            } => {
                tracing::info!("cluster heartbeat logger shutting down");
                break;
            }
        }
    }
}

/// Periodically probes every peer via `GET /ping` and updates
/// `last_heartbeat` for reachable peers.  Peers that fail the probe
/// are transitioned to `Disconnected` (unless already `Draining`/`Leaving`).
pub async fn run_heartbeat_updater(
    self_id: u64,
    membership: SharedMembership,
    interval: Duration,
    probe_timeout: Duration,
    mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
) {
    let client = match reqwest::Client::builder().timeout(probe_timeout).build() {
        Ok(c) => c,
        Err(e) => {
            tracing::error!(error = %e, "failed to build heartbeat probe client");
            return;
        }
    };

    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    tracing::info!(
        interval_secs = interval.as_secs(),
        timeout_ms = probe_timeout.as_millis() as u64,
        "peer heartbeat updater started"
    );

    loop {
        tokio::select! {
            _ = ticker.tick() => {
                probe_peers(self_id, &membership, &client).await;
            }
            _ = async {
                while !*shutdown_rx.borrow() {
                    shutdown_rx.changed().await.ok();
                }
            } => {
                tracing::info!("peer heartbeat updater shutting down");
                break;
            }
        }
    }
}

async fn probe_peers(self_id: u64, membership: &SharedMembership, client: &reqwest::Client) {
    let peers: Vec<(u64, String, NodeState)> = {
        let m = membership.read().await;
        m.all_peers(self_id)
            .into_iter()
            .map(|n| (n.node_id, n.addr.clone(), n.state))
            .collect()
    };

    if peers.is_empty() {
        return;
    }

    let futures: Vec<_> = peers
        .iter()
        .map(|(peer_id, addr, _state)| {
            let url = format!("http://{}/ping", addr);
            let client = client.clone();
            let pid = *peer_id;
            async move {
                let ok = client.get(&url).send().await.is_ok();
                (pid, ok)
            }
        })
        .collect();

    let results = futures::future::join_all(futures).await;

    let now = chrono::Utc::now().timestamp();
    let mut m = membership.write().await;

    for (pid, reachable) in results {
        if reachable {
            m.update_heartbeat(pid, now);

            if let Some(node) = m.get_node(pid)
                && node.state == NodeState::Disconnected
            {
                tracing::info!(peer_id = pid, "peer reconnected, marking active");
                m.set_state(pid, NodeState::Active);
            }
        } else {
            let should_disconnect = m
                .get_node(pid)
                .map(|n| n.state == NodeState::Active || n.state == NodeState::Syncing)
                .unwrap_or(false);

            if should_disconnect {
                tracing::warn!(peer_id = pid, "peer unreachable, marking disconnected");
                m.set_state(pid, NodeState::Disconnected);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::cluster::membership::{ClusterMembership, NodeInfo, new_shared};

    fn make_membership(peer_addrs: &[(u64, &str, NodeState)]) -> SharedMembership {
        let mut m = ClusterMembership::new();
        for &(id, addr, state) in peer_addrs {
            m.add_node(NodeInfo {
                node_id: id,
                addr: addr.to_string(),
                state,
                joined_at: 1000,
                last_heartbeat: 1000,
                needs_sync: false,
            });
        }
        new_shared(m)
    }

    #[tokio::test]
    async fn unreachable_peer_is_disconnected() {
        let membership = make_membership(&[(1, "127.0.0.1:19999", NodeState::Active)]);

        let client = reqwest::Client::builder()
            .timeout(Duration::from_millis(100))
            .build()
            .unwrap();

        probe_peers(0, &membership, &client).await;

        let m = membership.read().await;
        assert_eq!(m.get_node(1).unwrap().state, NodeState::Disconnected);
    }

    #[tokio::test]
    async fn draining_peer_stays_draining_when_unreachable() {
        let membership = make_membership(&[(1, "127.0.0.1:19999", NodeState::Draining)]);

        let client = reqwest::Client::builder()
            .timeout(Duration::from_millis(100))
            .build()
            .unwrap();

        probe_peers(0, &membership, &client).await;

        let m = membership.read().await;
        assert_eq!(m.get_node(1).unwrap().state, NodeState::Draining);
    }

    #[tokio::test]
    async fn no_peers_is_noop() {
        let membership = make_membership(&[]);
        let client = reqwest::Client::builder()
            .timeout(Duration::from_millis(100))
            .build()
            .unwrap();

        probe_peers(0, &membership, &client).await;
    }
}
