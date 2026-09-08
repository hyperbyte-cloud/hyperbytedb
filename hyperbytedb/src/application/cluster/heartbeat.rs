use std::time::Duration;

use crate::domain::cluster::membership::{NodeState, SharedMembership};

/// Self-reported readiness fields parsed from a peer's `/health` body.
///
/// Both fields are optional: absent/unparseable bodies (legacy binaries,
/// non-JSON responses) carry no membership information and produce no
/// inference — see [`decide_transition`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct HealthBody {
    pub state: Option<NodeState>,
    pub needs_sync: Option<bool>,
}

impl HealthBody {
    fn parse(text: &str) -> Option<Self> {
        let v: serde_json::Value = serde_json::from_str(text).ok()?;
        let state = v
            .get("state")
            .and_then(serde_json::Value::as_str)
            .and_then(|s| s.parse::<NodeState>().ok());
        let needs_sync = v.get("needs_sync").and_then(serde_json::Value::as_bool);
        // A body that is valid JSON but carries neither field is treated as
        // no-information, same as an unparseable body.
        if state.is_none() && needs_sync.is_none() {
            return None;
        }
        Some(Self { state, needs_sync })
    }

    /// Self-truth readiness: the peer reports itself caught up and accepting
    /// data. A failed startup sync (`needs_sync=true`) deliberately serves
    /// healthy traffic but must not be promoted as caught-up.
    fn ready(&self) -> bool {
        self.state == Some(NodeState::Active) && self.needs_sync != Some(true)
    }
}

/// Outcome of probing one peer.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum ProbeSignal {
    /// No response at all (connect error, timeout): the only crash detector.
    Unreachable,
    /// The peer answered. `health_ok` records the overall HTTP status for
    /// observability only — it never drives membership transitions.
    Response {
        health_ok: bool,
        body: Option<HealthBody>,
    },
}

/// Compute the membership transition implied by one probe of a peer in
/// `current` state.
///
/// Authority model: for reachable peers the decoded body `state` field is the
/// sole transition input; overall HTTP status never transitions membership.
/// `Joining`/`Draining`/`Leaving` are operator-owned and never auto-promoted.
pub(crate) fn decide_transition(
    current: NodeState,
    signal: &ProbeSignal,
    consecutive_misses: u32,
    miss_threshold: u32,
) -> Option<NodeState> {
    use NodeState::{Active, Decommissioning, Disconnected, Draining, Joining, Leaving, Syncing};
    match (current, signal) {
        // Transport failure is the sole demotion path to Disconnected;
        // operator states stay untouched.
        (_, ProbeSignal::Unreachable) => {
            // Hysteresis: a demotion now costs real data movement, because
            // placement is a pure function of membership. One dropped probe
            // must not move regions. Slow to evict, fast to readmit.
            if matches!(current, Active | Syncing) && consecutive_misses >= miss_threshold {
                Some(Disconnected)
            } else {
                None
            }
        }
        // Reachable but no usable body: legacy liveness heal only.
        (current, ProbeSignal::Response { body: None, .. }) => {
            (current == Disconnected).then_some(Active)
        }
        // Reachable with a parseable self-report: mirror self-truth.
        (current, ProbeSignal::Response { body: Some(b), .. }) => match current {
            Disconnected => {
                if b.ready() {
                    Some(Active)
                } else if b.state == Some(Syncing) {
                    Some(Syncing)
                } else {
                    None
                }
            }
            Syncing => b.ready().then_some(Active),
            Active => (b.state == Some(Syncing)).then_some(Syncing),
            // A drained node is restarting, not leaving: it must be able to
            // rejoin. Without this it stays `Draining` in its peers' views
            // forever, and since `Draining` keeps its placement seat, the
            // cluster would keep targeting a node it never readmits.
            Draining => b.ready().then_some(Active),
            // Terminal and operator-owned. No probe result may resurrect a
            // node that is being removed, or evacuation would be undone.
            Decommissioning | Leaving => None,
            Joining => None,
        },
    }
}

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
                tracing::debug!(
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

/// Periodically probes every peer via `GET /health` and converges the local
/// membership view toward each peer's self-reported state. Peers that fail to
/// answer at all are transitioned to `Disconnected` (unless already
/// `Draining`/`Leaving`).
pub async fn run_heartbeat_updater(
    self_id: u64,
    membership: SharedMembership,
    miss_threshold: u32,
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
                probe_peers(self_id, &membership, &client, miss_threshold).await;
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

async fn probe_peers(
    self_id: u64,
    membership: &SharedMembership,
    client: &reqwest::Client,
    miss_threshold: u32,
) {
    let peers: Vec<(u64, String)> = {
        let m = membership.read().await;
        m.all_peers(self_id)
            .into_iter()
            .map(|n| (n.node_id, n.addr.clone()))
            .collect()
    };

    if peers.is_empty() {
        return;
    }

    let futures: Vec<_> = peers
        .iter()
        .map(|(peer_id, addr)| {
            let health_url = format!("http://{addr}/health");
            let ping_url = format!("http://{addr}/ping");
            let client = client.clone();
            let pid = *peer_id;
            async move {
                let health = client.get(&health_url).send().await;
                let signal = match health {
                    Ok(resp) => {
                        let health_ok = resp.status().is_success();
                        let body = resp.text().await.ok().and_then(|t| HealthBody::parse(&t));
                        ProbeSignal::Response { health_ok, body }
                    }
                    Err(_) => {
                        // Health endpoint unreachable entirely: fall back to
                        // liveness ping so reachable-but-unhealthy peers are
                        // not mistaken for crashed ones.
                        match client.get(&ping_url).send().await {
                            Ok(_) => ProbeSignal::Response {
                                health_ok: false,
                                body: None,
                            },
                            Err(_) => ProbeSignal::Unreachable,
                        }
                    }
                };
                (pid, signal)
            }
        })
        .collect();

    let results = futures::future::join_all(futures).await;

    let now = chrono::Utc::now().timestamp();
    let mut m = membership.write().await;

    for (pid, signal) in results {
        match &signal {
            ProbeSignal::Unreachable => {
                // Count first: the counter lives on NodeInfo precisely so it
                // survives across ticks, which a map local to this function
                // would not.
                let misses = m.record_probe_miss(pid);
                let Some(current) = m.get_node(pid).map(|n| n.state) else {
                    continue;
                };
                let Some(next) = decide_transition(current, &signal, misses, miss_threshold) else {
                    continue;
                };
                if next != current {
                    tracing::warn!(
                        peer_id = pid,
                        misses,
                        miss_threshold,
                        "peer unreachable past threshold, marking disconnected"
                    );
                    m.set_state(pid, next);
                }
            }
            ProbeSignal::Response { health_ok, .. } => {
                m.update_heartbeat(pid, now);
                m.record_probe_success(pid);
                let Some(current) = m.get_node(pid).map(|n| n.state) else {
                    continue;
                };
                let Some(next) = decide_transition(current, &signal, 0, miss_threshold) else {
                    continue;
                };
                if next != current {
                    tracing::info!(
                        peer_id = pid,
                        from = %current,
                        to = %next,
                        health_ok,
                        "peer membership converged from self-reported state"
                    );
                    m.set_state(pid, next);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Probe-count arguments for `decide_transition`. A Response signal ignores
    /// them; an Unreachable signal is passed AT the threshold so these tests
    /// keep asserting that the *state* decides, not that a low count blocked
    /// the demotion. Hysteresis itself is covered separately below.
    const THRESHOLD: u32 = 5;
    const MISSES_AT_THRESHOLD: u32 = 5;
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
                consecutive_misses: 0,
            });
        }
        new_shared(m)
    }

    fn body(state: Option<NodeState>, needs_sync: Option<bool>) -> ProbeSignal {
        ProbeSignal::Response {
            health_ok: true,
            body: Some(HealthBody { state, needs_sync }),
        }
    }

    // ── pure transition matrix (H.2/H.3/H.4 semantics) ──────────────────

    #[test]
    fn syncing_promotes_when_body_reports_active_ready() {
        let next = decide_transition(
            NodeState::Syncing,
            &body(Some(NodeState::Active), Some(false)),
            0,
            THRESHOLD,
        );
        assert_eq!(next, Some(NodeState::Active));
    }

    #[test]
    fn syncing_stays_when_body_reports_needs_sync() {
        let next = decide_transition(
            NodeState::Syncing,
            &body(Some(NodeState::Active), Some(true)),
            0,
            THRESHOLD,
        );
        assert_eq!(next, None);
    }

    #[test]
    fn syncing_stays_when_body_reports_syncing() {
        let next = decide_transition(
            NodeState::Syncing,
            &body(Some(NodeState::Syncing), Some(false)),
            0,
            THRESHOLD,
        );
        assert_eq!(next, None);
    }

    #[test]
    fn syncing_stays_when_body_has_no_usable_state() {
        let next = decide_transition(NodeState::Syncing, &body(None, None), 0, THRESHOLD);
        assert_eq!(next, None);
    }

    #[test]
    fn active_demotes_when_body_reports_syncing() {
        let next = decide_transition(
            NodeState::Active,
            &body(Some(NodeState::Syncing), Some(false)),
            0,
            THRESHOLD,
        );
        assert_eq!(next, Some(NodeState::Syncing));
    }

    #[test]
    fn active_unchanged_when_body_absent() {
        let next = decide_transition(
            NodeState::Active,
            &ProbeSignal::Response {
                health_ok: false,
                body: None,
            },
            0,
            THRESHOLD,
        );
        assert_eq!(next, None);
    }

    #[test]
    fn disconnected_heals_to_active_on_ready_body() {
        let next = decide_transition(
            NodeState::Disconnected,
            &body(Some(NodeState::Active), Some(false)),
            0,
            THRESHOLD,
        );
        assert_eq!(next, Some(NodeState::Active));
    }

    #[test]
    fn disconnected_set_syncing_when_body_reports_syncing() {
        let next = decide_transition(
            NodeState::Disconnected,
            &body(Some(NodeState::Syncing), Some(false)),
            0,
            THRESHOLD,
        );
        assert_eq!(next, Some(NodeState::Syncing));
    }

    #[test]
    fn disconnected_legacy_heal_without_body() {
        let next = decide_transition(
            NodeState::Disconnected,
            &ProbeSignal::Response {
                health_ok: true,
                body: None,
            },
            0,
            THRESHOLD,
        );
        assert_eq!(next, Some(NodeState::Active));
    }

    #[test]
    fn unreachable_demotes_only_active_and_syncing() {
        for current in [NodeState::Active, NodeState::Syncing] {
            assert_eq!(
                decide_transition(
                    current,
                    &ProbeSignal::Unreachable,
                    MISSES_AT_THRESHOLD,
                    THRESHOLD
                ),
                Some(NodeState::Disconnected)
            );
        }
        for current in [
            NodeState::Disconnected,
            NodeState::Joining,
            NodeState::Draining,
            NodeState::Leaving,
        ] {
            assert_eq!(
                decide_transition(
                    current,
                    &ProbeSignal::Unreachable,
                    MISSES_AT_THRESHOLD,
                    THRESHOLD
                ),
                None
            );
        }
    }

    #[test]
    fn operator_states_are_never_promoted() {
        // `Draining` is deliberately absent: a drained node is restarting, not
        // leaving, and must be able to rejoin. See
        // `draining_recovers_when_the_node_reports_ready`.
        for current in [
            NodeState::Joining,
            NodeState::Decommissioning,
            NodeState::Leaving,
        ] {
            assert_eq!(
                decide_transition(
                    current,
                    &body(Some(NodeState::Active), Some(false)),
                    0,
                    THRESHOLD
                ),
                None
            );
            assert_eq!(
                decide_transition(
                    current,
                    &ProbeSignal::Response {
                        health_ok: false,
                        body: None
                    },
                    0,
                    THRESHOLD
                ),
                None
            );
        }
    }

    #[test]
    fn draining_recovers_when_the_node_reports_ready() {
        // Drain keeps the node's seat in every region it belongs to, so it
        // must come back. Without this it stays `Draining` in its peers' views
        // forever while the cluster keeps targeting it -- a live defect today,
        // and load-bearing once placement keys on candidacy.
        assert_eq!(
            decide_transition(
                NodeState::Draining,
                &body(Some(NodeState::Active), Some(false)),
                0,
                THRESHOLD
            ),
            Some(NodeState::Active)
        );
        // Not ready yet: stay put rather than readmitting a node mid-restart.
        assert_eq!(
            decide_transition(
                NodeState::Draining,
                &body(Some(NodeState::Active), Some(true)),
                0,
                THRESHOLD
            ),
            None
        );
    }

    #[test]
    fn decommissioning_is_terminal_under_every_signal() {
        // No probe result may resurrect a node being removed, or evacuation
        // would be undone by a heartbeat.
        for signal in [
            body(Some(NodeState::Active), Some(false)),
            ProbeSignal::Unreachable,
            ProbeSignal::Response {
                health_ok: false,
                body: None,
            },
        ] {
            assert_eq!(
                decide_transition(NodeState::Decommissioning, &signal, 0, THRESHOLD),
                None
            );
        }
    }

    #[test]
    fn flap_freedom_status_does_not_drive_transitions() {
        // Alternating overall status with an identical body must produce the
        // same decision every time: only the first application ever changes
        // state.
        let ok = body(Some(NodeState::Active), Some(false));
        let degraded = ProbeSignal::Response {
            health_ok: false,
            body: Some(HealthBody {
                state: Some(NodeState::Active),
                needs_sync: Some(false),
            }),
        };
        let mut state = NodeState::Syncing;
        for signal in [&ok, &degraded, &ok, &degraded] {
            if let Some(next) = decide_transition(state, signal, 0, THRESHOLD) {
                state = next;
            }
        }
        assert_eq!(state, NodeState::Active);
    }

    #[test]
    fn health_body_parse_handles_fields_and_garbage() {
        let b = HealthBody::parse(r#"{"state":"active","needs_sync":false}"#).unwrap();
        assert_eq!(b.state, Some(NodeState::Active));
        assert!(b.ready());
        let lagging = HealthBody::parse(r#"{"state":"active","needs_sync":true}"#).unwrap();
        assert!(!lagging.ready(), "needs_sync must block readiness");
        assert_eq!(
            HealthBody::parse(r#"{"state":"syncing"}"#).unwrap().state,
            Some(NodeState::Syncing)
        );
        assert!(HealthBody::parse("not json").is_none());
        assert!(HealthBody::parse(r#"{"status":"pass"}"#).is_none());
    }

    // ── probe_peers against real HTTP endpoints ─────────────────────────

    /// Serves `/health` (+ optional `/ping`) with responses produced by
    /// `responder(count) -> (status, body)`; returns the bound address.
    async fn spawn_health_server(
        responder: impl Fn(u64) -> (axum::http::StatusCode, &'static str) + Send + Sync + 'static,
    ) -> String {
        use axum::extract::State;
        use axum::routing::get;
        use std::sync::atomic::AtomicU64;

        type Responder =
            std::sync::Arc<dyn Fn(u64) -> (axum::http::StatusCode, &'static str) + Send + Sync>;

        let count = std::sync::Arc::new(AtomicU64::new(0));
        let responder: Responder = std::sync::Arc::new(responder);

        async fn health(
            State((count, responder)): State<(std::sync::Arc<AtomicU64>, Responder)>,
        ) -> (axum::http::StatusCode, &'static str) {
            let n = count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            responder(n)
        }

        let app = axum::Router::new()
            .route("/health", get(health))
            .route(
                "/ping",
                get(|| async { axum::http::StatusCode::NO_CONTENT }),
            )
            .with_state((count, responder));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = format!("127.0.0.1:{}", listener.local_addr().unwrap().port());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        addr
    }

    #[tokio::test]
    async fn prober_promotes_syncing_peer_from_self_reported_body() {
        let addr = spawn_health_server(|_| {
            (
                axum::http::StatusCode::SERVICE_UNAVAILABLE,
                r#"{"status":"warn","message":"x","state":"active","needs_sync":false}"#,
            )
        })
        .await;

        let membership = make_membership(&[(2, addr.as_str(), NodeState::Syncing)]);

        let client = reqwest::Client::builder()
            .timeout(Duration::from_millis(500))
            .build()
            .unwrap();
        probe_peers(1, &membership, &client, THRESHOLD).await;

        let m = membership.read().await;
        assert_eq!(m.get_node(2).unwrap().state, NodeState::Active);
    }

    #[tokio::test]
    async fn prober_demotes_active_peer_reporting_syncing() {
        let addr = spawn_health_server(|_| {
            (
                axum::http::StatusCode::SERVICE_UNAVAILABLE,
                r#"{"status":"warn","message":"x","state":"syncing","needs_sync":false}"#,
            )
        })
        .await;

        let membership = make_membership(&[(2, addr.as_str(), NodeState::Active)]);

        let client = reqwest::Client::builder()
            .timeout(Duration::from_millis(500))
            .build()
            .unwrap();
        probe_peers(1, &membership, &client, THRESHOLD).await;

        let m = membership.read().await;
        assert_eq!(m.get_node(2).unwrap().state, NodeState::Syncing);
    }

    #[tokio::test]
    async fn prober_no_inference_on_bodyless_response() {
        let addr = spawn_health_server(|_| (axum::http::StatusCode::OK, "")).await;

        let membership = make_membership(&[(2, addr.as_str(), NodeState::Syncing)]);

        let client = reqwest::Client::builder()
            .timeout(Duration::from_millis(500))
            .build()
            .unwrap();
        probe_peers(1, &membership, &client, THRESHOLD).await;

        let m = membership.read().await;
        assert_eq!(
            m.get_node(2).unwrap().state,
            NodeState::Syncing,
            "unparseable body must not promote"
        );
    }

    #[tokio::test]
    async fn prober_flap_freedom_over_alternating_status() {
        let addr = spawn_health_server(|n| {
            if n % 2 == 0 {
                (
                    axum::http::StatusCode::OK,
                    r#"{"status":"pass","message":"ok","state":"active","needs_sync":false}"#,
                )
            } else {
                (
                    axum::http::StatusCode::SERVICE_UNAVAILABLE,
                    r#"{"status":"fail","message":"wal","state":"active","needs_sync":false}"#,
                )
            }
        })
        .await;

        let membership = make_membership(&[(2, addr.as_str(), NodeState::Syncing)]);

        let client = reqwest::Client::builder()
            .timeout(Duration::from_millis(500))
            .build()
            .unwrap();
        for _ in 0..6 {
            probe_peers(1, &membership, &client, THRESHOLD).await;
        }

        let m = membership.read().await;
        assert_eq!(
            m.get_node(2).unwrap().state,
            NodeState::Active,
            "alternating status with constant self-truth must not flap"
        );
    }

    // ── pre-existing behavior guards ────────────────────────────────────

    #[tokio::test]
    async fn unreachable_peer_is_disconnected_only_past_the_threshold() {
        // Rewritten, not widened: under hysteresis a single failed probe no
        // longer demotes, because a demotion now moves data. The old
        // single-probe assertion encoded the pre-hysteresis contract.
        let membership = make_membership(&[(1, "127.0.0.1:19999", NodeState::Active)]);
        let client = reqwest::Client::builder()
            .timeout(Duration::from_millis(100))
            .build()
            .unwrap();

        for probe in 1..THRESHOLD {
            probe_peers(0, &membership, &client, THRESHOLD).await;
            assert_eq!(
                membership.read().await.get_node(1).unwrap().state,
                NodeState::Active,
                "demoted after only {probe} miss(es); threshold is {THRESHOLD}"
            );
        }

        probe_peers(0, &membership, &client, THRESHOLD).await;
        assert_eq!(
            membership.read().await.get_node(1).unwrap().state,
            NodeState::Disconnected,
            "still Active at the threshold"
        );
    }

    #[tokio::test]
    async fn a_success_decrements_rather_than_resets_the_miss_count() {
        // Hard-reset-on-success never demotes a node alternating four misses
        // and one success -- unreachable 80% of the time yet permanently
        // Active, which is worse than the old single-miss rule.
        let membership = make_membership(&[(1, "127.0.0.1:19999", NodeState::Active)]);
        {
            let mut m = membership.write().await;
            for _ in 0..4 {
                m.record_probe_miss(1);
            }
            m.record_probe_success(1);
            assert_eq!(
                m.get_node(1).unwrap().consecutive_misses,
                3,
                "a success must decrement, not zero the count"
            );
            for _ in 0..4 {
                m.record_probe_miss(1);
            }
            m.record_probe_success(1);
            assert!(
                m.get_node(1).unwrap().consecutive_misses >= THRESHOLD,
                "sustained flapping must converge on demotion"
            );
        }
    }

    #[tokio::test]
    async fn draining_peer_stays_draining_when_unreachable() {
        let membership = make_membership(&[(1, "127.0.0.1:19999", NodeState::Draining)]);

        let client = reqwest::Client::builder()
            .timeout(Duration::from_millis(100))
            .build()
            .unwrap();

        probe_peers(0, &membership, &client, THRESHOLD).await;

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

        probe_peers(0, &membership, &client, THRESHOLD).await;
    }
}
