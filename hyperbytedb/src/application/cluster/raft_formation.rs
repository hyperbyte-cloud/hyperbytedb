use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use crate::adapters::http::router::AppState;
use crate::domain::cluster::membership::{NodeInfo, NodeState};

/// Background task that forms the Raft cluster from the static peer list.
///
/// **Bootstrap node (node_id == 1)**:
///   Probes each configured peer address to discover its node_id, adds it as
///   a Raft learner, then promotes all learners to voters once the expected
///   cluster size is reached.
///
/// **Non-bootstrap nodes**:
///   No-op — they wait for the leader to discover and add them via Raft.
///   Membership changes propagate automatically via `sync_raft_membership_to_shared`.
pub async fn run_raft_cluster_formation(
    state: Arc<AppState>,
    node_id: u64,
    peer_addrs: Vec<String>,
    shutdown_rx: tokio::sync::watch::Receiver<bool>,
) {
    let raft = match &state.raft {
        Some(r) => r.clone(),
        None => return,
    };

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap_or_else(|_| reqwest::Client::new());

    // Give the local HTTP server a moment to start
    tokio::time::sleep(Duration::from_secs(1)).await;

    // When no static peers are configured the cluster is operator-driven:
    // Raft is initialized as a single-node group and the operator (or any
    // external controller) is responsible for calling /cluster/raft/add-learner
    // and /cluster/raft/change-membership as new pods become reachable.
    if peer_addrs.is_empty() {
        tracing::info!(
            node_id = node_id,
            "no static peers configured; raft initialized as single-node, awaiting API-driven membership changes"
        );
        return;
    }

    let expected_size = 1 + peer_addrs.len();

    // Discover peers, add as Raft learners, then promote to voters.
    //
    // Phase 1: Discover all peers and propose them as Raft learners.
    //   Uses blocking=false so we don't hang on unreachable learners
    //   (their HTTP servers may still be starting up).
    // Phase 2: Wait for all learners to catch up via Raft metrics.
    // Phase 3: Promote all learners to voters.
    for attempt in 1u32..=120 {
        if *shutdown_rx.borrow() {
            return;
        }

        let metrics = raft.metrics().borrow().clone();

        // Count current voters — if already fully formed, we're done
        let voter_ids: BTreeSet<u64> = metrics
            .membership_config
            .membership()
            .get_joint_config()
            .iter()
            .flat_map(|set| set.iter())
            .copied()
            .collect();

        if voter_ids.len() >= expected_size {
            tracing::info!(
                voters = ?voter_ids,
                "raft cluster fully formed"
            );
            break;
        }

        // ── Phase 1: Discover peers and add as learners ──────────────
        let mut discovered_ids: BTreeSet<u64> = BTreeSet::new();
        discovered_ids.insert(node_id);

        for peer_addr in &peer_addrs {
            let url = format!("http://{}/cluster/metrics", peer_addr);
            let probe = async {
                let resp = client.get(&url).send().await.ok()?;
                let v: serde_json::Value = resp.json().await.ok()?;
                v["node_id"].as_u64()
            };

            if let Some(peer_id) = probe.await {
                discovered_ids.insert(peer_id);

                // Update SharedMembership so the data plane can see this peer
                if let Some(ref m) = state.membership {
                    let mut guard = m.write().await;
                    if guard.get_node(peer_id).is_none() {
                        let now = chrono::Utc::now().timestamp();
                        guard.add_node(NodeInfo {
                            node_id: peer_id,
                            addr: peer_addr.clone(),
                            state: NodeState::Active,
                            joined_at: now,
                            last_heartbeat: now,
                            needs_sync: false,
                        });
                    }
                }

                // Non-blocking: proposes the learner without waiting for
                // it to catch up. Avoids deadlocking when the learner's
                // HTTP server isn't up yet.
                let node = openraft::BasicNode::new(peer_addr.clone());
                match raft.add_learner(peer_id, node, false).await {
                    Ok(_) => {
                        tracing::info!(
                            peer_id = peer_id,
                            peer_addr = %peer_addr,
                            attempt = attempt,
                            "raft learner added"
                        );
                    }
                    Err(e) => {
                        tracing::warn!(
                            peer_id = peer_id,
                            error = %e,
                            attempt = attempt,
                            "add_learner failed (may already exist or change in progress)"
                        );
                    }
                }
            }
        }

        // ── Phase 2 & 3: Promote when all peers discovered ──────────
        if discovered_ids.len() >= expected_size {
            match raft.change_membership(discovered_ids.clone(), false).await {
                Ok(_) => {
                    tracing::info!(
                        voters = ?discovered_ids,
                        "raft membership promotion succeeded"
                    );
                    break;
                }
                Err(e) => {
                    tracing::info!(
                        error = %e,
                        attempt = attempt,
                        discovered = ?discovered_ids,
                        "membership promotion not ready yet (learners catching up)"
                    );
                }
            }
        } else {
            tracing::info!(
                attempt = attempt,
                discovered = discovered_ids.len(),
                expected = expected_size,
                "waiting for peers to become reachable"
            );
        }

        let backoff = Duration::from_secs(if attempt < 10 { 2 } else { 5 });
        tokio::time::sleep(backoff).await;
    }
}
