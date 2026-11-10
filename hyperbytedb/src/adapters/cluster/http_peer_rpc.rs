//! [`PeerRpc`] over HTTP. Requests go out on `PeerClient`'s pooled clients
//! (they carry the cluster credential), one per call, as the call sites did.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;

use crate::adapters::cluster::peer_client::PeerClient;
use crate::domain::cluster::peer_rpc::{PeerRpcError, ShardWriteTarget};
use crate::domain::sharding::transfer::{
    ShardRehomeRequest, ShardTransferPayload, TRANSFER_PAYLOAD_HEADER,
};
use crate::domain::sharding::types::{
    FEATURE_CROSS_EPOCH_MERGE, MvHandoffRequest, NODE_FEATURES_HEADER, REHOME_PROGRESS_HEADER,
    RegionHeartbeat, ShardBootstrapRequest, ShardDeleteRequest, ShardMetadataRequest,
    ShardMvBackfillRequest, ShardQueryRequest, ShardWriteBatchResponse,
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

/// The port on `pc`'s pooled clients, with its membership.
#[must_use]
pub fn peers_for(pc: Arc<PeerClient>) -> crate::ports::peer_rpc::Peers {
    crate::ports::peer_rpc::Peers {
        membership: pc.membership().clone(),
        rpc: Arc::new(HttpPeerRpc::new(pc)),
    }
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

    async fn shard_transfer_push(
        &self,
        addr: &str,
        mut payload: ShardTransferPayload,
        timeout: Duration,
    ) -> Result<String, PeerRpcError> {
        use base64::Engine as _;
        // Bytes: the fallback below resends it without a copy.
        let body = Bytes::from(payload.body.take().unwrap_or_default());
        let header = serde_json::to_vec(&payload)
            .map(|json| base64::engine::general_purpose::STANDARD.encode(json))
            .map_err(|e| PeerRpcError::Unreachable(format!("transfer header: {e}")))?;
        let raw = self
            .client()
            .post(format!("http://{addr}/internal/shard/transfer/raw"))
            .header(TRANSFER_PAYLOAD_HEADER, header)
            .header(reqwest::header::CONTENT_TYPE, "text/plain; charset=utf-8")
            .body(body.clone())
            .timeout(timeout)
            .send()
            .await
            .map_err(|e| PeerRpcError::Unreachable(error_chain(&e)))?;
        let resp = if raw.status() == reqwest::StatusCode::NOT_FOUND {
            // An older node, or a region the receiver does not know: the JSON
            // route answers the second the same way.
            payload.body = Some(body.to_vec());
            self.post_json(addr, "/internal/shard/transfer", &payload, timeout)
                .await?
        } else {
            Self::checked(Ok(raw)).await?
        };
        Ok(resp.text().await.unwrap_or_default())
    }

    async fn shard_transfer_ack(
        &self,
        addr: &str,
        payload: &ShardTransferPayload,
        timeout: Duration,
    ) -> Result<(), PeerRpcError> {
        self.post_json(addr, "/internal/shard/transfer", payload, timeout)
            .await
            .map(|_| ())
    }

    async fn shard_mv_backfill(
        &self,
        addr: &str,
        req: &ShardMvBackfillRequest,
        timeout: Duration,
    ) -> Result<(), PeerRpcError> {
        self.post_json(addr, "/internal/shard/mv-backfill", req, timeout)
            .await
            .map(|_| ())
    }

    async fn mv_handoff(
        &self,
        addr: &str,
        req: &MvHandoffRequest,
        timeout: Duration,
    ) -> Result<(), PeerRpcError> {
        self.post_json(addr, "/internal/mv/handoff", req, timeout)
            .await
            .map(|_| ())
    }

    async fn shard_heartbeat(
        &self,
        addr: &str,
        hb: &RegionHeartbeat,
        timeout: Duration,
    ) -> Result<(), PeerRpcError> {
        let resp = self
            .client()
            .post(format!("http://{addr}/internal/shard/heartbeat"))
            .header(NODE_FEATURES_HEADER, FEATURE_CROSS_EPOCH_MERGE)
            .json(hb)
            .timeout(timeout)
            .send()
            .await;
        Self::checked(resp).await.map(|_| ())
    }

    async fn shard_rehome(
        &self,
        addr: &str,
        req: &ShardRehomeRequest,
        idle: Duration,
        max: Duration,
    ) -> Result<serde_json::Value, PeerRpcError> {
        let resp = self
            .client()
            .post(format!("http://{addr}/internal/shard/rehome"))
            .header(REHOME_PROGRESS_HEADER, "1")
            .json(req)
            .timeout(max)
            .send()
            .await;
        let mut resp = Self::checked(resp).await?;
        let mut raw = Vec::new();
        loop {
            match tokio::time::timeout(idle, resp.chunk()).await {
                Err(_) => {
                    return Err(PeerRpcError::Unreachable(format!(
                        "rehome silent for {idle:?}"
                    )));
                }
                Ok(Err(e)) => return Err(PeerRpcError::Unreachable(error_chain(&e))),
                Ok(Ok(None)) => break,
                Ok(Ok(Some(chunk))) => raw.extend_from_slice(&chunk),
            }
        }
        serde_json::from_slice(&raw)
            .map_err(|e| PeerRpcError::Unreachable(format!("rehome result: {e}")))
    }

    async fn sync_manifest(
        &self,
        addr: &str,
        timeout: Duration,
    ) -> Result<crate::domain::cluster::sync::SyncManifest, PeerRpcError> {
        let resp = self.get(addr, "/internal/sync/manifest", timeout).await?;
        let body = Self::body(resp).await?;
        serde_json::from_slice(&body)
            .map_err(|e| PeerRpcError::Unreachable(format!("parse sync manifest: {e}")))
    }

    async fn shard_map(
        &self,
        addr: &str,
        timeout: Duration,
    ) -> Result<crate::domain::sharding::ShardMap, PeerRpcError> {
        let resp = self.get(addr, "/internal/shard/map", timeout).await?;
        let body = Self::body(resp).await?;
        let json: crate::domain::sharding::ShardMapJson = serde_json::from_slice(&body)
            .map_err(|e| PeerRpcError::Unreachable(format!("parse shard map: {e}")))?;
        Ok(crate::domain::sharding::ShardMap::from(json))
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

    /// An older peer without the raw route answers 404; the push is then
    /// sent once as JSON, as before.
    #[tokio::test]
    async fn transfer_push_falls_back_to_json_on_404() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let raw = Arc::new(AtomicUsize::new(0));
        let json = Arc::new(AtomicUsize::new(0));
        let (r, j) = (raw.clone(), json.clone());
        let app = axum::Router::new()
            .route(
                "/internal/shard/transfer/raw",
                axum::routing::post(move || {
                    r.fetch_add(1, Ordering::SeqCst);
                    async { axum::http::StatusCode::NOT_FOUND }
                }),
            )
            .route(
                "/internal/shard/transfer",
                axum::routing::post(move || {
                    j.fetch_add(1, Ordering::SeqCst);
                    async { "applied=3" }
                }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        let rpc = HttpPeerRpc::with_client(reqwest::Client::new());
        let payload = crate::domain::sharding::transfer::ShardTransferPayload::ack(
            "db",
            "autogen",
            "cpu",
            1,
            0,
            u64::MAX,
            Default::default(),
            1,
            7,
        );
        let body = rpc
            .shard_transfer_push(&addr, payload, Duration::from_secs(5))
            .await
            .unwrap();
        assert_eq!(body, "applied=3");
        assert_eq!(raw.load(Ordering::SeqCst), 1);
        assert_eq!(json.load(Ordering::SeqCst), 1);
    }
}
