//! [`PeerRpc`] over HTTP, on the pooled clients `PeerClient` keeps (they
//! carry the cluster credential).

use std::sync::Arc;
use std::time::Duration;

use crate::adapters::cluster::peer_client::PeerClient;
use crate::domain::cluster::peer_rpc::PeerRpcError;
use crate::ports::peer_rpc::PeerRpc;

pub struct HttpPeerRpc {
    peers: Arc<PeerClient>,
}

impl HttpPeerRpc {
    #[must_use]
    pub fn new(peers: Arc<PeerClient>) -> Self {
        Self { peers }
    }

    /// POST `req` as JSON to `http://{addr}{path}`; a non-success status is
    /// [`PeerRpcError::Status`] with the body, a failed send `Unreachable`.
    pub async fn post_json<Req: serde::Serialize + ?Sized + Sync>(
        &self,
        addr: &str,
        path: &str,
        req: &Req,
        timeout: Duration,
    ) -> Result<reqwest::Response, PeerRpcError> {
        let resp = self
            .peers
            .http_client()
            .post(format!("http://{addr}{path}"))
            .json(req)
            .timeout(timeout)
            .send()
            .await;
        Self::checked(resp).await
    }

    /// GET `http://{addr}{path}`, with the same error mapping.
    pub async fn get(
        &self,
        addr: &str,
        path: &str,
        timeout: Duration,
    ) -> Result<reqwest::Response, PeerRpcError> {
        let resp = self
            .peers
            .http_client()
            .get(format!("http://{addr}{path}"))
            .timeout(timeout)
            .send()
            .await;
        Self::checked(resp).await
    }

    async fn checked(
        resp: Result<reqwest::Response, reqwest::Error>,
    ) -> Result<reqwest::Response, PeerRpcError> {
        let resp = resp.map_err(|e| PeerRpcError::Unreachable(e.to_string()))?;
        if resp.status().is_success() {
            return Ok(resp);
        }
        let code = resp.status().as_u16();
        let body = resp.text().await.unwrap_or_default();
        Err(PeerRpcError::Status { code, body })
    }
}

#[async_trait::async_trait]
impl PeerRpc for HttpPeerRpc {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapters::cluster::replication_log::ReplicationLog;
    use crate::domain::cluster::membership::{ClusterMembership, new_shared};

    fn rpc(dir: &tempfile::TempDir) -> HttpPeerRpc {
        let pc = PeerClient::new(
            1,
            "127.0.0.1:1".into(),
            new_shared(ClusterMembership::default()),
            Arc::new(ReplicationLog::open(dir.path().join("repl")).unwrap()),
            3,
            64,
            2,
            1 << 20,
            Default::default(),
        );
        HttpPeerRpc::new(Arc::new(pc))
    }

    /// "Answered 503" (back-pressure, retried) and "unreachable" must stay
    /// distinct: callers retry one and not the other.
    #[tokio::test]
    async fn status_and_transport_errors_stay_distinct() {
        let app = axum::Router::new().route(
            "/x",
            axum::routing::post(|| async { (axum::http::StatusCode::SERVICE_UNAVAILABLE, "busy") }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        let dir = tempfile::tempdir().unwrap();
        let rpc = rpc(&dir);
        let err = rpc
            .post_json(&addr, "/x", &serde_json::json!({}), Duration::from_secs(5))
            .await
            .unwrap_err();
        assert_eq!(
            err,
            PeerRpcError::Status {
                code: 503,
                body: "busy".into()
            }
        );

        let closed = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let closed_addr = closed.local_addr().unwrap().to_string();
        drop(closed);
        let err = rpc
            .get(&closed_addr, "/x", Duration::from_secs(5))
            .await
            .unwrap_err();
        assert!(matches!(err, PeerRpcError::Unreachable(_)), "{err:?}");
    }
}
