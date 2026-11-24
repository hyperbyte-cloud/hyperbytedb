//! Outbound HTTP to this cluster's own nodes.
//!
//! Every request one node makes to another (replication, Raft RPCs,
//! forwarding, shard moves, probes, sync) goes through [`HttpClientPort`]:
//! `reqwest` in production
//! ([`crate::adapters::cluster::reqwest_http_client::ReqwestHttpClient`]),
//! the simulated network in the deterministic simulation rig. Callers use
//! [`PeerHttpClient`], a cheap handle with the `reqwest`-shaped builder the
//! cluster code was written against, which adds the cluster credential and
//! the client's default timeout to every request.

use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use axum::http::header::{CONTENT_TYPE, HeaderName};
use axum::http::{HeaderMap, HeaderValue, Method, StatusCode};
use bytes::Bytes;
use futures::{Stream, StreamExt};
use serde::Serialize;
use serde::de::DeserializeOwned;

/// A response body, as it arrives.
pub type ByteStream = Pin<Box<dyn Stream<Item = Result<Bytes, HttpClientError>> + Send>>;

/// One request to a peer.
#[derive(Debug, Clone)]
pub struct PeerRequest {
    pub method: Method,
    /// Absolute URL, e.g. `http://10.0.0.3:8086/internal/replicate`.
    pub url: String,
    pub headers: HeaderMap,
    pub body: Bytes,
    /// Total time from connecting until the whole body is read; `None`
    /// waits forever.
    pub timeout: Option<Duration>,
}

/// A peer's answer. The body streams: read it with [`Self::chunk`], or
/// whole with [`Self::bytes`], [`Self::text`] or [`Self::json`]. A
/// request's timeout still applies while the body is read.
pub struct PeerResponse {
    pub status: StatusCode,
    pub headers: HeaderMap,
    pub body: ByteStream,
}

/// Why a request got no response, or its body could not be read.
///
/// Production maps `reqwest` errors one-to-one: `is_connect()` is
/// [`Self::Connect`] (connect timeouts included), otherwise `is_timeout()`
/// is [`Self::Timeout`], body and decode errors are [`Self::Body`], and
/// anything else is [`Self::Other`].
#[derive(thiserror::Error, Debug)]
pub enum HttpClientError {
    #[error("connect: {0}")]
    Connect(String),
    #[error("timeout")]
    Timeout,
    #[error("body: {0}")]
    Body(String),
    #[error("{0}")]
    Other(String),
}

impl HttpClientError {
    /// The request's timeout expired (`reqwest::Error::is_timeout`).
    #[must_use]
    pub fn is_timeout(&self) -> bool {
        matches!(self, Self::Timeout)
    }

    /// No connection to the peer (`reqwest::Error::is_connect`).
    #[must_use]
    pub fn is_connect(&self) -> bool {
        matches!(self, Self::Connect(_))
    }
}

#[async_trait]
pub trait HttpClientPort: Send + Sync {
    /// Send `req`; `Ok` for any status the peer answers with.
    async fn send(&self, req: PeerRequest) -> Result<PeerResponse, HttpClientError>;
}

impl PeerResponse {
    #[must_use]
    pub fn status(&self) -> StatusCode {
        self.status
    }

    #[must_use]
    pub fn headers(&self) -> &HeaderMap {
        &self.headers
    }

    /// The next piece of the body; `None` once it has all been read.
    pub async fn chunk(&mut self) -> Result<Option<Bytes>, HttpClientError> {
        self.body.next().await.transpose()
    }

    /// The whole body.
    pub async fn bytes(mut self) -> Result<Bytes, HttpClientError> {
        let Some(first) = self.chunk().await? else {
            return Ok(Bytes::new());
        };
        let Some(second) = self.chunk().await? else {
            return Ok(first);
        };
        let mut all = Vec::with_capacity(first.len() + second.len());
        all.extend_from_slice(&first);
        all.extend_from_slice(&second);
        while let Some(more) = self.chunk().await? {
            all.extend_from_slice(&more);
        }
        Ok(all.into())
    }

    /// The whole body as text; invalid UTF-8 is replaced, as `reqwest`
    /// does without its `charset` feature.
    pub async fn text(self) -> Result<String, HttpClientError> {
        let bytes = self.bytes().await?;
        Ok(String::from_utf8_lossy(&bytes).into_owned())
    }

    /// The whole body, as JSON.
    pub async fn json<T: DeserializeOwned>(self) -> Result<T, HttpClientError> {
        let bytes = self.bytes().await?;
        serde_json::from_slice(&bytes).map_err(|e| HttpClientError::Body(e.to_string()))
    }
}

impl std::fmt::Debug for PeerResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PeerResponse")
            .field("status", &self.status)
            .field("headers", &self.headers)
            .finish_non_exhaustive()
    }
}

/// A complete response, already in memory.
impl<B: Into<Bytes>> From<axum::http::Response<B>> for PeerResponse {
    fn from(resp: axum::http::Response<B>) -> Self {
        let (parts, body) = resp.into_parts();
        let body: Bytes = body.into();
        Self {
            status: parts.status,
            headers: parts.headers,
            body: Box::pin(futures::stream::iter(
                (!body.is_empty()).then_some(Ok(body)),
            )),
        }
    }
}

/// A client for requests to peers: the port, plus the headers (the cluster
/// credential) and default timeout its requests get. Cheap to clone.
#[derive(Clone)]
pub struct PeerHttpClient {
    port: Arc<dyn HttpClientPort>,
    default_headers: HeaderMap,
    timeout: Option<Duration>,
}

impl std::fmt::Debug for PeerHttpClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PeerHttpClient")
            .field("timeout", &self.timeout)
            .finish_non_exhaustive()
    }
}

impl PeerHttpClient {
    /// Requests through `port`, with no extra headers and no timeout.
    pub fn new(port: Arc<dyn HttpClientPort>) -> Self {
        Self {
            port,
            default_headers: HeaderMap::new(),
            timeout: None,
        }
    }

    /// Every request carries `headers`, unless it sets the same name itself.
    #[must_use]
    pub fn with_default_headers(mut self, headers: HeaderMap) -> Self {
        self.default_headers = headers;
        self
    }

    /// Requests without their own timeout get `timeout`.
    #[must_use]
    pub fn with_timeout(mut self, timeout: Option<Duration>) -> Self {
        self.timeout = timeout;
        self
    }

    pub fn request(&self, method: Method, url: impl Into<String>) -> PeerRequestBuilder {
        PeerRequestBuilder {
            client: self.clone(),
            request: Ok(PeerRequest {
                method,
                url: url.into(),
                headers: HeaderMap::new(),
                body: Bytes::new(),
                timeout: None,
            }),
        }
    }

    pub fn get(&self, url: impl Into<String>) -> PeerRequestBuilder {
        self.request(Method::GET, url)
    }

    pub fn post(&self, url: impl Into<String>) -> PeerRequestBuilder {
        self.request(Method::POST, url)
    }

    pub fn put(&self, url: impl Into<String>) -> PeerRequestBuilder {
        self.request(Method::PUT, url)
    }

    pub fn delete(&self, url: impl Into<String>) -> PeerRequestBuilder {
        self.request(Method::DELETE, url)
    }

    /// Send `req` as built, adding the default headers and timeout.
    pub async fn execute(&self, mut req: PeerRequest) -> Result<PeerResponse, HttpClientError> {
        for (name, value) in &self.default_headers {
            if !req.headers.contains_key(name) {
                req.headers.insert(name.clone(), value.clone());
            }
        }
        if req.timeout.is_none() {
            req.timeout = self.timeout;
        }
        self.port.send(req).await
    }
}

/// One request being built, as with `reqwest::RequestBuilder`: an invalid
/// header or a body that does not serialize fails at [`Self::send`].
#[must_use]
pub struct PeerRequestBuilder {
    client: PeerHttpClient,
    request: Result<PeerRequest, HttpClientError>,
}

impl PeerRequestBuilder {
    pub fn header<K, V>(mut self, name: K, value: V) -> Self
    where
        K: TryInto<HeaderName>,
        K::Error: std::fmt::Display,
        V: TryInto<HeaderValue>,
        V::Error: std::fmt::Display,
    {
        if let Ok(req) = &mut self.request {
            match (name.try_into(), value.try_into()) {
                (Ok(name), Ok(value)) => {
                    req.headers.append(name, value);
                }
                (Err(e), _) => self.request = Err(HttpClientError::Other(e.to_string())),
                (_, Err(e)) => self.request = Err(HttpClientError::Other(e.to_string())),
            }
        }
        self
    }

    /// Each name in `headers` replaces that name's values so far, as
    /// `reqwest::RequestBuilder::headers` does.
    pub fn headers(mut self, headers: HeaderMap) -> Self {
        if let Ok(req) = &mut self.request {
            let mut prev: Option<HeaderName> = None;
            for (name, value) in headers {
                match name {
                    Some(name) => {
                        req.headers.insert(name.clone(), value);
                        prev = Some(name);
                    }
                    None => {
                        if let Some(name) = &prev {
                            req.headers.append(name.clone(), value);
                        }
                    }
                }
            }
        }
        self
    }

    pub fn body(mut self, body: impl Into<Bytes>) -> Self {
        if let Ok(req) = &mut self.request {
            req.body = body.into();
        }
        self
    }

    /// `value` as a JSON body, with `content-type: application/json` unless
    /// one is set.
    pub fn json<T: Serialize + ?Sized>(mut self, value: &T) -> Self {
        if let Ok(req) = &mut self.request {
            match serde_json::to_vec(value) {
                Ok(body) => {
                    if !req.headers.contains_key(CONTENT_TYPE) {
                        req.headers
                            .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
                    }
                    req.body = body.into();
                }
                Err(e) => self.request = Err(HttpClientError::Other(e.to_string())),
            }
        }
        self
    }

    /// This request's total timeout, in place of the client's.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        if let Ok(req) = &mut self.request {
            req.timeout = Some(timeout);
        }
        self
    }

    pub async fn send(self) -> Result<PeerResponse, HttpClientError> {
        self.client.execute(self.request?).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    /// Records each request and answers 200 with an empty body.
    #[derive(Default)]
    struct Recorder(Mutex<Vec<PeerRequest>>);

    #[async_trait]
    impl HttpClientPort for Recorder {
        async fn send(&self, req: PeerRequest) -> Result<PeerResponse, HttpClientError> {
            self.0.lock().unwrap().push(req);
            Ok(axum::http::Response::new(Bytes::new()).into())
        }
    }

    #[tokio::test]
    async fn requests_get_the_default_headers_and_timeout_unless_they_set_their_own() {
        let port = Arc::new(Recorder::default());
        let mut defaults = HeaderMap::new();
        defaults.insert("x-cred", HeaderValue::from_static("c"));
        defaults.insert("x-other", HeaderValue::from_static("default"));
        let client = PeerHttpClient::new(port.clone())
            .with_default_headers(defaults)
            .with_timeout(Some(Duration::from_secs(10)));

        client.get("http://a/x").send().await.unwrap();
        client
            .post("http://a/y")
            .header("x-other", "mine")
            .json(&serde_json::json!({"k": 1}))
            .timeout(Duration::from_secs(2))
            .send()
            .await
            .unwrap();

        let sent = port.0.lock().unwrap();
        assert_eq!(sent[0].method, Method::GET);
        assert_eq!(sent[0].headers["x-cred"], "c");
        assert_eq!(sent[0].headers["x-other"], "default");
        assert_eq!(sent[0].timeout, Some(Duration::from_secs(10)));
        assert_eq!(sent[1].headers["x-cred"], "c");
        assert_eq!(sent[1].headers.get_all("x-other").iter().count(), 1);
        assert_eq!(sent[1].headers["x-other"], "mine");
        assert_eq!(sent[1].headers[CONTENT_TYPE], "application/json");
        assert_eq!(&sent[1].body[..], br#"{"k":1}"#);
        assert_eq!(sent[1].timeout, Some(Duration::from_secs(2)));
    }

    #[tokio::test]
    async fn an_invalid_header_fails_at_send() {
        let client = PeerHttpClient::new(Arc::new(Recorder::default()));
        let err = client
            .get("http://a/x")
            .header("x-bad", "line\nbreak")
            .send()
            .await
            .unwrap_err();
        assert!(matches!(err, HttpClientError::Other(_)));
    }

    #[tokio::test]
    async fn a_body_reads_whole_or_in_chunks() {
        let mut resp = PeerResponse {
            status: StatusCode::OK,
            headers: HeaderMap::new(),
            body: Box::pin(futures::stream::iter(vec![
                Ok(Bytes::from_static(b"ab")),
                Ok(Bytes::from_static(b"cd")),
            ])),
        };
        assert_eq!(resp.chunk().await.unwrap().unwrap(), "ab");
        assert_eq!(resp.text().await.unwrap(), "cd");

        let resp: PeerResponse = axum::http::Response::new("[1,2]").into();
        assert_eq!(resp.json::<Vec<u8>>().await.unwrap(), vec![1, 2]);
        let mut empty: PeerResponse = axum::http::Response::new("").into();
        assert!(empty.chunk().await.unwrap().is_none());
    }
}
