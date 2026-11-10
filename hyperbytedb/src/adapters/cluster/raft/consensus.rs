//! [`ConsensusPort`] over OpenRaft: propose locally, or forward to the
//! leader over [`PeerRpc`] when this node is a follower.

use std::time::Duration;

use openraft::BasicNode;
use openraft::error::ForwardToLeader;

use super::HyperbytedbRaft;
use crate::domain::cluster::membership::SharedMembership;
use crate::domain::cluster::raft_request::{ClusterRequest, ClusterResponse};
use crate::error::HyperbytedbError;
use crate::ports::consensus::ConsensusPort;
use crate::ports::leadership::LeadershipPort;
use crate::ports::peer_rpc::{PeerRpc, SharedPeerRpc};

/// Proposals forwarded to a leader that cannot be reached are retried this
/// many times in all, re-resolving the leader each time.
const MAX_ATTEMPTS: u32 = 3;
/// A forwarded proposal's deadline (the old dedicated client's timeout).
const FORWARD_TIMEOUT: Duration = Duration::from_secs(10);

pub struct RaftConsensus {
    raft: HyperbytedbRaft,
    rpc: SharedPeerRpc,
    /// Addresses for a leader openraft names by id only.
    membership: Option<SharedMembership>,
}

impl RaftConsensus {
    #[must_use]
    pub fn new(
        raft: HyperbytedbRaft,
        rpc: SharedPeerRpc,
        membership: Option<SharedMembership>,
    ) -> Self {
        Self {
            raft,
            rpc,
            membership,
        }
    }

    async fn leader_addr(
        &self,
        forward: &ForwardToLeader<u64, BasicNode>,
    ) -> Result<String, HyperbytedbError> {
        let metrics = self.raft.metrics().borrow().clone();
        let membership_addrs = match &self.membership {
            Some(m) => m
                .read()
                .await
                .nodes
                .values()
                .map(|node| (node.node_id, node.addr.clone()))
                .collect::<Vec<_>>(),
            None => Vec::new(),
        };
        resolve_leader_addr_with_lookup(
            forward,
            metrics.current_leader,
            |leader_id| {
                metrics
                    .membership_config
                    .membership()
                    .get_node(&leader_id)
                    .map(|node| node.addr.clone())
            },
            |leader_id| {
                membership_addrs
                    .iter()
                    .find(|(id, _)| *id == leader_id)
                    .map(|(_, addr)| addr.clone())
            },
        )
    }
}

impl LeadershipPort for RaftConsensus {
    fn current_leader(&self) -> Option<u64> {
        self.raft.metrics().borrow().current_leader
    }
}

#[async_trait::async_trait]
impl ConsensusPort for RaftConsensus {
    async fn propose(&self, req: ClusterRequest) -> Result<ClusterResponse, HyperbytedbError> {
        let local = || async {
            match self.raft.client_write(req.clone()).await {
                Ok(resp) => LocalWrite::Done(resp.data),
                Err(e) => match e.forward_to_leader::<BasicNode>() {
                    Some(fwd) => LocalWrite::Forward(fwd.clone()),
                    None => LocalWrite::Failed(e.to_string()),
                },
            }
        };
        propose_via(self.rpc.as_ref(), &req, local, |fwd| async move {
            self.leader_addr(&fwd).await
        })
        .await
    }
}

/// What proposing on this node's own Raft gave.
pub(crate) enum LocalWrite {
    /// Committed here.
    Done(ClusterResponse),
    /// Not the leader: forward to the one this names.
    Forward(ForwardToLeader<u64, BasicNode>),
    /// Failed for another reason.
    Failed(String),
}

/// Propose with `local`; when it says to forward, resolve the leader with
/// `leader_addr` and forward over `rpc`, at most `MAX_ATTEMPTS` times in all.
pub(crate) async fn propose_via<L, LF, A, AF>(
    rpc: &dyn PeerRpc,
    req: &ClusterRequest,
    local: L,
    leader_addr: A,
) -> Result<ClusterResponse, HyperbytedbError>
where
    L: Fn() -> LF,
    LF: std::future::Future<Output = LocalWrite>,
    A: Fn(ForwardToLeader<u64, BasicNode>) -> AF,
    AF: std::future::Future<Output = Result<String, HyperbytedbError>>,
{
    for attempt in 0..MAX_ATTEMPTS {
        let fwd = match local().await {
            LocalWrite::Done(resp) => return Ok(resp),
            LocalWrite::Failed(e) => {
                tracing::error!(error = %e, "raft proposal failed");
                return Err(HyperbytedbError::Internal(
                    format!("raft replication failed: {e}").into(),
                ));
            }
            LocalWrite::Forward(fwd) => fwd,
        };
        tracing::debug!(
            leader_id = ?fwd.leader_id,
            leader_addr = ?fwd.leader_node.as_ref().map(|n| n.addr.as_str()),
            attempt,
            "forwarding proposal to raft leader"
        );
        let result = match leader_addr(fwd).await {
            Ok(addr) => forward(rpc, &addr, req).await,
            Err(e) => Err(e),
        };
        match result {
            Ok(resp) => return Ok(resp),
            Err(e) if attempt + 1 >= MAX_ATTEMPTS => return Err(e),
            Err(e) => {
                tracing::warn!(error = %e, attempt, "forward to raft leader failed; retrying");
            }
        }
    }
    Err(HyperbytedbError::Internal(crate::error::ChainedError::new(
        "raft replication failed after retries",
    )))
}

/// Send `req` to the leader at `leader_addr` and return its answer.
pub(crate) async fn forward(
    rpc: &dyn PeerRpc,
    leader_addr: &str,
    req: &ClusterRequest,
) -> Result<ClusterResponse, HyperbytedbError> {
    rpc.raft_client_write(leader_addr, req, FORWARD_TIMEOUT)
        .await
        .map_err(|e| {
            HyperbytedbError::Internal(
                format!("raft replication failed: forward to leader {leader_addr}: {e}").into(),
            )
        })
}

fn resolve_leader_addr_with_lookup(
    forward: &ForwardToLeader<u64, BasicNode>,
    fallback_leader_id: Option<u64>,
    raft_lookup: impl Fn(u64) -> Option<String>,
    membership_lookup: impl Fn(u64) -> Option<String>,
) -> Result<String, HyperbytedbError> {
    if let Some(node) = forward.leader_node.as_ref() {
        return Ok(node.addr.clone());
    }

    let leader_id = forward.leader_id.or(fallback_leader_id);
    let leader_id = leader_id.ok_or_else(|| {
        HyperbytedbError::Internal(crate::error::ChainedError::new(
            "raft replication failed: forward to leader but leader id is unknown",
        ))
    })?;

    raft_lookup(leader_id)
        .or_else(|| membership_lookup(leader_id))
        .ok_or_else(|| {
            HyperbytedbError::Internal(
                format!("raft replication failed: leader {leader_id} has no known address").into(),
            )
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapters::cluster::http_peer_rpc::HttpPeerRpc;
    use crate::domain::sharding::ops::ShardMapOp;
    use std::sync::{Arc, Mutex};

    async fn stub_leader(
        status: axum::http::StatusCode,
        reply: ClusterResponse,
    ) -> (String, Arc<Mutex<Vec<serde_json::Value>>>) {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let rec = seen.clone();
        let app = axum::Router::new().route(
            "/cluster/raft/client-write",
            axum::routing::post(move |axum::Json(v): axum::Json<serde_json::Value>| {
                let rec = rec.clone();
                let reply = reply.clone();
                async move {
                    rec.lock().unwrap().push(v);
                    (status, axum::Json(reply))
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        (addr, seen)
    }

    /// A leader that answers 503 to its first `fail_first` proposals and
    /// accepts the rest; returns its address and a request counter.
    async fn flaky_leader(fail_first: usize) -> (String, Arc<Mutex<usize>>) {
        let count = Arc::new(Mutex::new(0usize));
        let c = count.clone();
        let app = axum::Router::new().route(
            "/cluster/raft/client-write",
            axum::routing::post(move || {
                let c = c.clone();
                async move {
                    let n = {
                        let mut g = c.lock().unwrap();
                        *g += 1;
                        *g
                    };
                    let status = if n <= fail_first {
                        axum::http::StatusCode::SERVICE_UNAVAILABLE
                    } else {
                        axum::http::StatusCode::OK
                    };
                    (status, axum::Json(ClusterResponse::success()))
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        (addr, count)
    }

    /// A follower keeps forwarding through a leader that fails, at most
    /// `MAX_ATTEMPTS` times, and re-resolves the leader on each attempt.
    #[tokio::test]
    async fn propose_retries_forwards_up_to_the_cap() {
        let rpc = HttpPeerRpc::with_client(reqwest::Client::new());
        let not_leader = || async {
            LocalWrite::Forward(ForwardToLeader {
                leader_id: Some(2),
                leader_node: None,
            })
        };

        let (down, hits) = flaky_leader(usize::MAX).await;
        let resolved = Mutex::new(0usize);
        let err = propose_via(&rpc, &op(), not_leader, |_| {
            *resolved.lock().unwrap() += 1;
            let a = down.clone();
            async move { Ok(a) }
        })
        .await
        .unwrap_err();
        assert!(err.to_string().contains("503"), "{err}");
        assert_eq!(*hits.lock().unwrap(), MAX_ATTEMPTS as usize);
        assert_eq!(*resolved.lock().unwrap(), MAX_ATTEMPTS as usize);

        let (recovers, hits) = flaky_leader(1).await;
        let resp = propose_via(&rpc, &op(), not_leader, |_| {
            let a = recovers.clone();
            async move { Ok(a) }
        })
        .await
        .unwrap();
        assert!(resp.ok);
        assert_eq!(*hits.lock().unwrap(), 2);
    }

    fn op() -> ClusterRequest {
        ClusterRequest::ShardMapMutation(Box::new(ShardMapOp::DropSpace {
            db: "db".into(),
            rp: None,
            measurement: None,
        }))
    }

    /// The follower's request reaches the leader unchanged, and the
    /// leader's answer comes back as-is: a refusal is a response the caller
    /// checks, not an error; an error status is an error.
    #[tokio::test]
    async fn propose_follows_forward_to_leader() {
        let refused = ClusterResponse {
            ok: false,
            message: Some("stale epoch".into()),
        };
        let (addr, seen) = stub_leader(axum::http::StatusCode::OK, refused.clone()).await;
        let rpc = HttpPeerRpc::with_client(reqwest::Client::new());

        let resp = forward(&rpc, &addr, &op()).await.unwrap();
        assert!(!resp.ok);
        assert_eq!(resp.message.as_deref(), Some("stale epoch"));
        assert_eq!(
            seen.lock().unwrap().as_slice(),
            [serde_json::to_value(op()).unwrap()]
        );

        let (down, _) = stub_leader(axum::http::StatusCode::SERVICE_UNAVAILABLE, refused).await;
        let err = forward(&rpc, &down, &op()).await.unwrap_err().to_string();
        assert!(err.contains("503"), "{err}");
    }

    #[test]
    fn resolve_leader_addr_prefers_forward_node() {
        let forward = ForwardToLeader {
            leader_id: Some(1),
            leader_node: Some(BasicNode::new("hyperbytedb-0:8086")),
        };

        let addr = resolve_leader_addr_with_lookup(&forward, None, |_| None, |_| None)
            .expect("leader addr");

        assert_eq!(addr, "hyperbytedb-0:8086");
    }

    #[test]
    fn resolve_leader_addr_falls_back_to_raft_membership() {
        let forward = ForwardToLeader {
            leader_id: Some(1),
            leader_node: None,
        };

        let addr = resolve_leader_addr_with_lookup(
            &forward,
            None,
            |id| (id == 1).then(|| "hyperbytedb-0:8086".into()),
            |_| None,
        )
        .expect("leader addr");

        assert_eq!(addr, "hyperbytedb-0:8086");
    }

    #[test]
    fn resolve_leader_addr_falls_back_to_cluster_membership() {
        let forward = ForwardToLeader {
            leader_id: Some(2),
            leader_node: None,
        };

        let addr = resolve_leader_addr_with_lookup(
            &forward,
            None,
            |_| None,
            |id| (id == 2).then(|| "hyperbytedb-1:8086".into()),
        )
        .expect("leader addr");

        assert_eq!(addr, "hyperbytedb-1:8086");
    }

    #[test]
    fn resolve_leader_addr_uses_metrics_leader_when_forward_id_missing() {
        let forward = ForwardToLeader {
            leader_id: None,
            leader_node: None,
        };

        let addr = resolve_leader_addr_with_lookup(
            &forward,
            Some(1),
            |id| (id == 1).then(|| "hyperbytedb-0:8086".into()),
            |_| None,
        )
        .expect("leader addr");

        assert_eq!(addr, "hyperbytedb-0:8086");
    }

    #[test]
    fn resolve_leader_addr_errors_when_unresolvable() {
        let forward = ForwardToLeader {
            leader_id: Some(99),
            leader_node: None,
        };

        let err = resolve_leader_addr_with_lookup(&forward, None, |_| None, |_| None)
            .expect_err("expected error");

        assert!(err.to_string().contains("leader 99 has no known address"));
    }
}
