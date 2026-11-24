/// Builds HTTP clients for requests to this cluster's own nodes, carrying
/// the cluster's credential. Only peers get these clients: the credential
/// must never reach another host.
pub trait PeerHttp: Send + Sync {
    /// A builder whose requests carry the cluster credential.
    fn client_builder(&self) -> reqwest::ClientBuilder;

    /// `builder`, built. Building fails only when the HTTP stack itself
    /// cannot start (TLS backend, proxy settings), which startup checks. A
    /// later failure is logged and gets a client with only the credential.
    fn build(&self, builder: reqwest::ClientBuilder) -> reqwest::Client {
        builder.build().unwrap_or_else(|e| {
            tracing::error!(error = %e, "peer HTTP client could not be built; using a plain one");
            self.client_builder().build().unwrap_or_default()
        })
    }
}

/// How a client to peers is set up. The adapter turns this into its own
/// settings; an injected [`crate::ports::http_client::HttpClientPort`]
/// (the simulated network) ignores everything but `timeout`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PeerClientOptions {
    /// Total timeout for requests that set none.
    pub timeout: Option<std::time::Duration>,
    pub connections: PeerConnections,
}

impl PeerClientOptions {
    /// Default connections; requests time out after `timeout`.
    #[must_use]
    pub fn with_timeout(timeout: std::time::Duration) -> Self {
        Self {
            timeout: Some(timeout),
            ..Self::default()
        }
    }
}

/// The kind of connections a peer client keeps.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum PeerConnections {
    /// The HTTP library's defaults.
    #[default]
    Default,
    /// Peer names resolved through the process-wide caching resolver
    /// ([`crate::adapters::cluster::dns_cache::CachingResolver`]).
    CachedDns,
    /// Long-lived HTTP/2 connections (prior knowledge, adaptive window,
    /// keepalive pings while idle, TCP keepalive 60 s), a 3 s connect
    /// timeout and the caching resolver: replication and forwarding.
    Http2,
}
