//! The credential one node presents to another's internal routes.
//!
//! With `auth.enabled`, `/internal/*` and `/cluster/*` need an admin user.
//! Nodes and the operator carry no user, so they send the shared
//! `auth.cluster_token` in [`HEADER`] instead; every node of a cluster gets
//! the same token (the operator puts it in a Secret). [`ClusterAuth`] is
//! built once at startup and handed to everything that talks to a peer or
//! checks a peer's request, so none can forget it. Only peers get its
//! clients: the token must never reach another host.

use std::sync::Arc;

use axum::http::header::{HeaderMap, HeaderValue};

use crate::adapters::cluster::reqwest_http_client::ReqwestHttpClient;
use crate::error::HyperbytedbError;
use crate::ports::http_client::{HttpClientError, HttpClientPort, PeerHttpClient};
use crate::ports::peer_http::{PeerClientOptions, PeerHttp};

pub const HEADER: &str = "x-hyperbytedb-cluster-token";

/// This node's cluster token: sent to peers, and accepted from them. The
/// default has none, as without `auth.cluster_token`.
#[derive(Clone, Default)]
pub struct ClusterAuth {
    token: Option<Arc<str>>,
    header: Option<HeaderValue>,
    /// Sends every peer request in place of `reqwest` (the simulated
    /// network); `None` in production.
    http_client: Option<Arc<dyn HttpClientPort>>,
}

impl std::fmt::Debug for ClusterAuth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClusterAuth")
            .field("token", &self.token.as_ref().map(|_| "<redacted>"))
            .field(
                "http_client",
                &self.http_client.as_ref().map(|_| "<injected>"),
            )
            .finish()
    }
}

impl ClusterAuth {
    /// From `auth.cluster_token` (empty means none). Fails if the token
    /// cannot be sent as a header, or no HTTP client can be built at all,
    /// so either stops startup instead of every peer call.
    pub fn new(token: Option<&str>) -> Result<Self, HyperbytedbError> {
        Self::new_with(token, None)
    }

    /// [`Self::new`], with every peer request sent through `http_client`
    /// when one is given (no `reqwest` client is then built).
    pub fn new_with(
        token: Option<&str>,
        http_client: Option<Arc<dyn HttpClientPort>>,
    ) -> Result<Self, HyperbytedbError> {
        let mut auth = match token.filter(|t| !t.is_empty()) {
            None => Self::default(),
            Some(t) => {
                let mut header = HeaderValue::from_str(t).map_err(|_| {
                    HyperbytedbError::Internal(
                        "auth.cluster_token cannot be sent as an HTTP header".into(),
                    )
                })?;
                header.set_sensitive(true);
                Self {
                    token: Some(Arc::from(t)),
                    header: Some(header),
                    http_client: None,
                }
            }
        };
        match http_client {
            Some(port) => auth.http_client = Some(port),
            None => {
                ReqwestHttpClient::build(&PeerClientOptions::default()).map_err(|e| {
                    HyperbytedbError::Internal(format!("cannot build an HTTP client: {e}").into())
                })?;
            }
        }
        Ok(auth)
    }

    /// Whether `presented` is this cluster's token. False when none is set.
    #[must_use]
    pub fn token_matches(&self, presented: &[u8]) -> bool {
        let Some(token) = self.token.as_deref() else {
            return false;
        };
        let token = token.as_bytes();
        // Constant time over the token's length.
        token.len() == presented.len()
            && token
                .iter()
                .zip(presented)
                .fold(0u8, |acc, (a, b)| acc | (a ^ b))
                == 0
    }
}

impl ClusterAuth {
    /// The credential header, for every request to a peer.
    fn default_headers(&self) -> HeaderMap {
        let mut headers = HeaderMap::new();
        if let Some(value) = &self.header {
            headers.insert(HEADER, value.clone());
        }
        headers
    }

    fn with_credential(&self, port: Arc<dyn HttpClientPort>) -> PeerHttpClient {
        PeerHttpClient::new(port).with_default_headers(self.default_headers())
    }
}

impl PeerHttp for ClusterAuth {
    fn try_client(&self, options: PeerClientOptions) -> Result<PeerHttpClient, HttpClientError> {
        let port: Arc<dyn HttpClientPort> = match &self.http_client {
            Some(port) => Arc::clone(port),
            None => Arc::new(ReqwestHttpClient::build(&options)?),
        };
        Ok(self.with_credential(port).with_timeout(options.timeout))
    }

    fn client(&self, options: PeerClientOptions) -> PeerHttpClient {
        self.try_client(options).unwrap_or_else(|e| {
            tracing::error!(error = %e, "peer HTTP client could not be built; using a plain one");
            self.try_client(PeerClientOptions::default())
                .unwrap_or_else(|_| self.with_credential(Arc::new(ReqwestHttpClient::default())))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_the_configured_token_matches() {
        let none = ClusterAuth::new(None).unwrap();
        assert!(!none.token_matches(b""));
        assert!(!none.token_matches(b"anything"));
        let empty = ClusterAuth::new(Some("")).unwrap();
        assert!(!empty.token_matches(b""), "an empty token is no token");
        let auth = ClusterAuth::new(Some("s3cret")).unwrap();
        assert!(auth.token_matches(b"s3cret"));
        assert!(!auth.token_matches(b"s3cre"));
        assert!(!auth.token_matches(b"s3cret!"));
        assert!(!auth.token_matches(b"S3cret"));
    }

    #[test]
    fn a_token_that_is_not_a_header_value_is_refused() {
        assert!(ClusterAuth::new(Some("s3cret\n")).is_err());
    }

    /// An injected port gets every peer request, with the credential and
    /// the client's timeout.
    #[tokio::test]
    async fn an_injected_http_client_sends_with_the_credential() {
        use crate::ports::http_client::{PeerRequest, PeerResponse};
        use std::sync::Mutex;
        use std::time::Duration;

        #[derive(Default)]
        struct Recorder(Mutex<Vec<PeerRequest>>);
        #[async_trait::async_trait]
        impl HttpClientPort for Recorder {
            async fn send(&self, req: PeerRequest) -> Result<PeerResponse, HttpClientError> {
                self.0.lock().unwrap().push(req);
                Ok(axum::http::Response::new(bytes::Bytes::new()).into())
            }
        }

        let port = Arc::new(Recorder::default());
        let auth = ClusterAuth::new_with(Some("s3cret"), Some(port.clone())).unwrap();
        let client = auth.client(PeerClientOptions::with_timeout(Duration::from_secs(7)));
        client.get("http://peer:8086/health").send().await.unwrap();
        let sent = port.0.lock().unwrap();
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].headers[HEADER], "s3cret");
        assert_eq!(sent[0].timeout, Some(Duration::from_secs(7)));
    }

    #[test]
    fn debug_does_not_show_the_token() {
        let auth = ClusterAuth::new(Some("s3cret")).unwrap();
        assert!(!format!("{auth:?}").contains("s3cret"));
    }
}
