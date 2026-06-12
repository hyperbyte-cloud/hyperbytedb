mod admin;
mod auth;
mod http_transport;
mod query;
mod write;

pub use admin::PingInfo;
pub use auth::Credentials;
pub use http_transport::{HttpBackend, HttpRequest, RawResponse};
pub use query::{QueryOptions, QueryResponse, SeriesResult, StatementResult};
pub use write::WriteOptions;

use std::sync::Arc;

use crate::config::ConnectionConfig;
use crate::error::Result;

pub struct HyperbytedbClient {
    pub(crate) backend: Arc<HttpBackend>,
    pub(crate) config: ConnectionConfig,
    pub(crate) base: String,
    pub(crate) credentials: Credentials,
    pub(crate) verbose: bool,
}

impl HyperbytedbClient {
    pub fn new(config: &ConnectionConfig, verbose: bool) -> Result<Self> {
        Ok(Self {
            backend: Arc::new(HttpBackend::from_config(config, verbose)?),
            base: config.base_url(),
            config: config.clone(),
            credentials: Credentials::from_config(config),
            verbose,
        })
    }

    pub fn base_url(&self) -> &str {
        &self.base
    }

    pub fn set_base_url(&mut self, url: String) {
        self.config.host = url.trim_end_matches('/').to_string();
        self.config.socket = None;
        self.base = self.config.base_url();
    }

    pub(crate) fn api_path(&self, path: &str) -> String {
        self.config.api_path(path)
    }

    pub(crate) async fn request(
        &self,
        method: &str,
        path: &str,
        query: &str,
        headers: &[(&str, &str)],
        body: Option<Vec<u8>>,
    ) -> Result<RawResponse> {
        self.backend
            .as_ref()
            .request(HttpRequest {
                method,
                base: &self.base,
                path: &self.api_path(path),
                query,
                headers,
                body,
                verbose: self.verbose,
            })
            .await
    }

    pub(crate) fn auth_headers(&self) -> Vec<(String, String)> {
        self.credentials
            .authorization_header()
            .into_iter()
            .collect()
    }
}
