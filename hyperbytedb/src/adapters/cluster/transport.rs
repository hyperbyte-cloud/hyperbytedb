//! Pluggable HTTP transport for cluster RPCs.
//!
//! `PeerClient`, `SyncClient`, and the OpenRaft `Network` implementation
//! currently hardcode [`reqwest::Client`]. The [`HttpTransport`] trait is the
//! seam for alternative transports (e.g. in-memory routing for tests).
//!
//! ### Current status
//!
//! This module introduces the trait and the [`ReqwestTransport`] implementation
//! without yet rewiring `PeerClient`, `SyncClient`, or the raft `Network` to
//! consume it.

use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;

/// One RPC request destined for a peer node.
#[derive(Debug, Clone)]
pub struct TransportRequest {
    /// Absolute URL, e.g. `http://10.0.0.3:8086/cluster/replicate/v1/lp`.
    /// Transport implementations may ignore the hostname but are required to
    /// preserve the path and query string.
    pub url: String,
    /// HTTP method. We keep it a string so transports don't need to depend
    /// on any specific http crate version.
    pub method: &'static str,
    /// Headers as a `Vec` (order-preserving) so transports that care about
    /// header order can serialize deterministically.
    pub headers: Vec<(String, String)>,
    pub body: Bytes,
    /// Peer node id, if known. Optional hint for transports that route by id.
    pub peer_node_id: Option<u64>,
    /// Overall timeout for the request.
    pub timeout: Duration,
}

/// The peer's response.
#[derive(Debug, Clone)]
pub struct TransportResponse {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Bytes,
}

/// Errors returned from a transport send. Kept intentionally simple — the
/// production code paths already translate into [`crate::error::HyperbytedbError`] variants
/// like `PeerUnreachable` / `ReplicationTimeout`.
#[derive(Debug, thiserror::Error)]
pub enum TransportError {
    #[error("peer unreachable: {0}")]
    Unreachable(String),
    #[error("request timed out after {0:?}")]
    Timeout(Duration),
    #[error("transport error: {0}")]
    Other(String),
}

/// HTTP-style transport for cluster RPCs.
///
/// Implementations must be `Send + Sync` and `Clone` — the production code
/// passes the transport through many async tasks and cloning it is expected
/// to be cheap (e.g. `reqwest::Client` internally refcounts).
#[async_trait]
pub trait HttpTransport: Send + Sync + 'static {
    async fn send(&self, request: TransportRequest) -> Result<TransportResponse, TransportError>;
}

/// Default transport backed by `reqwest::Client`. Behaviour is equivalent
/// to the direct `reqwest` calls the cluster code uses today.
#[derive(Clone)]
pub struct ReqwestTransport {
    client: reqwest::Client,
}

impl ReqwestTransport {
    pub fn new(client: reqwest::Client) -> Self {
        Self { client }
    }
}

#[async_trait]
impl HttpTransport for ReqwestTransport {
    async fn send(&self, request: TransportRequest) -> Result<TransportResponse, TransportError> {
        let method = match request.method {
            "GET" => reqwest::Method::GET,
            "POST" => reqwest::Method::POST,
            "PUT" => reqwest::Method::PUT,
            "DELETE" => reqwest::Method::DELETE,
            "PATCH" => reqwest::Method::PATCH,
            "HEAD" => reqwest::Method::HEAD,
            other => {
                return Err(TransportError::Other(format!(
                    "unsupported http method: {other}"
                )));
            }
        };
        let mut builder = self
            .client
            .request(method, &request.url)
            .timeout(request.timeout)
            .body(request.body.to_vec());
        for (k, v) in &request.headers {
            builder = builder.header(k, v);
        }
        let resp = builder.send().await.map_err(|e| {
            if e.is_timeout() {
                TransportError::Timeout(request.timeout)
            } else if e.is_connect() {
                TransportError::Unreachable(e.to_string())
            } else {
                TransportError::Other(e.to_string())
            }
        })?;
        let status = resp.status().as_u16();
        let mut headers = Vec::with_capacity(resp.headers().len());
        for (k, v) in resp.headers() {
            if let Ok(s) = v.to_str() {
                headers.push((k.as_str().to_string(), s.to_string()));
            }
        }
        let body = resp.bytes().await.map_err(|e| {
            if e.is_timeout() {
                TransportError::Timeout(request.timeout)
            } else {
                TransportError::Other(e.to_string())
            }
        })?;
        Ok(TransportResponse {
            status,
            headers,
            body,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transport_request_is_clone_and_debug() {
        let req = TransportRequest {
            url: "http://peer/lp".into(),
            method: "POST",
            headers: vec![("Content-Type".into(), "text/plain".into())],
            body: Bytes::from_static(b"measurement,tag=a value=1"),
            peer_node_id: Some(7),
            timeout: Duration::from_secs(5),
        };
        let _cloned = req.clone();
        let formatted = format!("{req:?}");
        assert!(formatted.contains("peer_node_id"));
    }

    #[test]
    fn unsupported_method_produces_other_error() {
        let client = reqwest::Client::new();
        let transport = ReqwestTransport::new(client);
        let req = TransportRequest {
            url: "http://example.invalid/".into(),
            method: "TRACE",
            headers: Vec::new(),
            body: Bytes::new(),
            peer_node_id: None,
            timeout: Duration::from_millis(10),
        };
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("current_thread runtime");
        let err = rt
            .block_on(transport.send(req))
            .expect_err("trace should error");
        match err {
            TransportError::Other(msg) => {
                assert!(msg.contains("TRACE"), "error was: {msg}")
            }
            _ => panic!("expected Other, got {err:?}"),
        }
    }
}
