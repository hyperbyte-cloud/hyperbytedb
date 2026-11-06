//! [`PeerRpc`] over HTTP. Requests go out on `PeerClient`'s pooled clients
//! (they carry the cluster credential), one per call, as the call sites did.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;

use crate::adapters::cluster::peer_client::PeerClient;
use crate::domain::cluster::peer_rpc::{PeerRpcError, ShardWriteTarget};
use crate::domain::sharding::types::{
    ShardBootstrapRequest, ShardDeleteRequest, ShardMetadataRequest, ShardQueryRequest,
    ShardWriteBatchResponse,
};
use crate::error::error_chain;
use crate::ports::peer_rpc::{ByteStream, PeerRpc};

enum Clients {
    Pooled(Arc<PeerClient>),
    Single(reqwest::Client),
}

pub struct HttpPeerRpc {
    clients: Clients,
}

impl HttpPeerRpc {
    #[must_use]
    pub fn new(peers: Arc<PeerClient>) -> Self {
        Self {
            clients: Clients::Pooled(peers),
        }
    }

    /// Every request on one client (tests, tools).
    #[must_use]
    pub fn with_client(client: reqwest::Client) -> Self {
        Self {
            clients: Clients::Single(client),
        }
    }

    fn client(&self) -> &reqwest::Client {
        match &self.clients {
            Clients::Pooled(p) => p.http_client(),
            Clients::Single(c) => c,
        }
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
            .client()
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
            .client()
            .get(format!("http://{addr}{path}"))
            .timeout(timeout)
            .send()
            .await;
        Self::checked(resp).await
    }

    async fn checked(
        resp: Result<reqwest::Response, reqwest::Error>,
    ) -> Result<reqwest::Response, PeerRpcError> {
        let resp = resp.map_err(|e| PeerRpcError::Unreachable(error_chain(&e)))?;
        if resp.status().is_success() {
            return Ok(resp);
        }
        let code = resp.status().as_u16();
        let body = resp.text().await.unwrap_or_default();
        Err(PeerRpcError::Status { code, body })
    }

    async fn body(resp: reqwest::Response) -> Result<Bytes, PeerRpcError> {
        resp.bytes()
            .await
            .map_err(|e| PeerRpcError::Unreachable(error_chain(&e)))
    }
}

#[async_trait::async_trait]
impl PeerRpc for HttpPeerRpc {
    async fn shard_bootstrap(
        &self,
        addr: &str,
        req: &ShardBootstrapRequest,
        timeout: Duration,
    ) -> Result<(), PeerRpcError> {
        self.post_json(addr, "/internal/shard/bootstrap", req, timeout)
            .await
            .map(|_| ())
    }

    async fn shard_write(
        &self,
        addr: &str,
        target: &ShardWriteTarget,
        body: Bytes,
        timeout: Duration,
    ) -> Result<(), PeerRpcError> {
        let mut req = self
            .client()
            .post(format!("http://{addr}/internal/shard/write"))
            .header("Content-Type", "application/octet-stream")
            .header("X-HyperbyteDB-Shard-Db", &target.db)
            .header("X-HyperbyteDB-Shard-Rp", &target.rp)
            .header(
                "X-HyperbyteDB-Shard-Region-Id",
                target.region_id.to_string(),
            )
            .header("X-HyperbyteDB-Shard-Epoch", &target.epoch_json)
            .body(body);
        if let Some(p) = target.precision.as_deref() {
            req = req.header("X-HyperbyteDB-Shard-Precision", p);
        }
        Self::checked(req.timeout(timeout).send().await)
            .await
            .map(|_| ())
    }

    async fn shard_write_batch(
        &self,
        addr: &str,
        body: Bytes,
        timeout: Duration,
    ) -> Result<ShardWriteBatchResponse, PeerRpcError> {
        let req = crate::telemetry::inject(
            self.client()
                .post(format!("http://{addr}/internal/shard/write-batch"))
                .header("Content-Type", "application/octet-stream"),
        )
        .body(body)
        .timeout(timeout);
        let resp = Self::checked(req.send().await).await?;
        let status = resp.status();
        resp.json::<ShardWriteBatchResponse>()
            .await
            .map_err(|e| PeerRpcError::Unreachable(format!("{status}: {e}")))
    }

    async fn shard_query(
        &self,
        addr: &str,
        req: &ShardQueryRequest,
        timeout: Duration,
    ) -> Result<ByteStream, PeerRpcError> {
        let resp = self
            .post_json(addr, "/internal/shard/query", req, timeout)
            .await?;
        // Chunk by chunk, so a large Arrow reply is decoded as it arrives
        // rather than buffered whole.
        let stream = futures::stream::unfold(Some(resp), |state| async move {
            let mut resp = state?;
            match resp.chunk().await {
                Ok(Some(chunk)) => Some((Ok(chunk), Some(resp))),
                Ok(None) => None,
                Err(e) => Some((Err(PeerRpcError::Unreachable(error_chain(&e))), None)),
            }
        });
        Ok(Box::pin(stream))
    }

    async fn shard_metadata(
        &self,
        addr: &str,
        req: &ShardMetadataRequest,
        timeout: Duration,
    ) -> Result<Bytes, PeerRpcError> {
        let resp = self
            .post_json(addr, "/internal/shard/metadata", req, timeout)
            .await?;
        Self::body(resp).await
    }

    async fn shard_delete(
        &self,
        addr: &str,
        req: &ShardDeleteRequest,
        timeout: Duration,
    ) -> Result<(), PeerRpcError> {
        self.post_json(addr, "/internal/shard/delete", req, timeout)
            .await
            .map(|_| ())
    }
}

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
