//! [`HttpClientPort`] over `reqwest`: production peer HTTP.

use std::time::Duration;

use async_trait::async_trait;

use crate::adapters::cluster::dns_cache::CachingResolver;
use crate::ports::http_client::{HttpClientError, HttpClientPort, PeerRequest, PeerResponse};
use crate::ports::peer_http::{PeerClientOptions, PeerConnections};

/// One `reqwest::Client` (one connection pool) behind the port.
#[derive(Clone, Default)]
pub struct ReqwestHttpClient {
    client: reqwest::Client,
}

impl ReqwestHttpClient {
    pub fn new(client: reqwest::Client) -> Self {
        Self { client }
    }

    /// A client set up as `options` says. Fails only when the HTTP stack
    /// itself cannot start (TLS backend, proxy settings).
    pub fn build(options: &PeerClientOptions) -> Result<Self, HttpClientError> {
        let mut builder = reqwest::Client::builder();
        if let Some(timeout) = options.timeout {
            builder = builder.timeout(timeout);
        }
        builder = match options.connections {
            PeerConnections::Default => builder,
            PeerConnections::CachedDns => builder.dns_resolver(CachingResolver::shared()),
            // connect_timeout bounds the worst-case tail for dead or
            // unreachable peers (black-holed SYNs would otherwise hang until
            // the full request timeout); keepalive keeps long-lived pool
            // sockets from silently expiring behind NAT/firewalls.
            PeerConnections::Http2 => builder
                .connect_timeout(Duration::from_secs(3))
                .tcp_keepalive(Duration::from_secs(60))
                .dns_resolver(CachingResolver::shared())
                .http2_prior_knowledge()
                .http2_adaptive_window(true)
                .http2_keep_alive_interval(Duration::from_secs(30))
                .http2_keep_alive_while_idle(true),
        };
        builder
            .build()
            .map(Self::new)
            .map_err(|e| HttpClientError::Other(error_chain(&e)))
    }
}

#[async_trait]
impl HttpClientPort for ReqwestHttpClient {
    async fn send(&self, req: PeerRequest) -> Result<PeerResponse, HttpClientError> {
        let mut builder = self
            .client
            .request(req.method, req.url)
            .headers(req.headers);
        // A request without a body is sent as before, with none.
        if !req.body.is_empty() {
            builder = builder.body(req.body);
        }
        if let Some(timeout) = req.timeout {
            builder = builder.timeout(timeout);
        }
        let resp = builder.send().await.map_err(classify)?;
        let status = resp.status();
        let headers = resp.headers().clone();
        // `chunk` keeps the request's timeout running over the body, as
        // reading the `reqwest::Response` directly did.
        let body = futures::stream::unfold(Some(resp), |resp| async move {
            let mut resp = resp?;
            match resp.chunk().await {
                Ok(Some(chunk)) => Some((Ok(chunk), Some(resp))),
                Ok(None) => None,
                Err(e) => Some((Err(classify(e)), None)),
            }
        });
        Ok(PeerResponse {
            status,
            headers,
            body: Box::pin(body),
        })
    }
}

/// `reqwest`'s classification, one-to-one: `is_connect` first (a connect
/// timeout is both, and callers asking `is_connect` saw it as one), then
/// `is_timeout`, then body/decode errors.
fn classify(e: reqwest::Error) -> HttpClientError {
    if e.is_connect() {
        HttpClientError::Connect(error_chain(&e))
    } else if e.is_timeout() {
        HttpClientError::Timeout
    } else if e.is_body() || e.is_decode() {
        HttpClientError::Body(error_chain(&e))
    } else {
        HttpClientError::Other(error_chain(&e))
    }
}

/// `e` and its causes, joined: reqwest's own message ("error sending request
/// for url ...") hides whether the peer refused, reset or timed out.
fn error_chain(e: &dyn std::error::Error) -> String {
    let mut out = e.to_string();
    let mut cause = e.source();
    while let Some(c) = cause {
        out.push_str(": ");
        out.push_str(&c.to_string());
        cause = c.source();
    }
    out
}

#[cfg(all(test, not(madsim)))]
mod tests {
    use super::*;
    use crate::ports::http_client::PeerHttpClient;
    use std::sync::Arc;

    fn client() -> PeerHttpClient {
        PeerHttpClient::new(Arc::new(ReqwestHttpClient::default()))
    }

    #[tokio::test]
    async fn reqwest_http_client_maps_errors() {
        // Accepts connections and never answers.
        let blackhole = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let hole = blackhole.local_addr().unwrap();
        let _held = tokio::spawn(async move {
            let mut conns = Vec::new();
            while let Ok((c, _)) = blackhole.accept().await {
                conns.push(c);
            }
        });
        let err = client()
            .get(format!("http://{hole}/health"))
            .timeout(Duration::from_millis(200))
            .send()
            .await
            .unwrap_err();
        assert!(matches!(err, HttpClientError::Timeout), "{err:?}");
        assert!(err.is_timeout() && !err.is_connect());

        // Nothing listens.
        let closed = {
            let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            l.local_addr().unwrap()
        };
        let err = client()
            .get(format!("http://{closed}/health"))
            .timeout(Duration::from_secs(5))
            .send()
            .await
            .unwrap_err();
        assert!(matches!(err, HttpClientError::Connect(_)), "{err:?}");
        assert!(err.is_connect() && !err.is_timeout());

        // An error status is a response.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let app = axum::Router::new().route(
            "/busy",
            axum::routing::post(|body: String| async move {
                (
                    axum::http::StatusCode::SERVICE_UNAVAILABLE,
                    format!("got {body}"),
                )
            }),
        );
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let resp = client()
            .post(format!("http://{addr}/busy"))
            .body("x")
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), axum::http::StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(resp.text().await.unwrap(), "got x");
    }
}
