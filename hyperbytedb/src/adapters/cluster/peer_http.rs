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
