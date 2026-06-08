mod admin;
mod auth;
mod query;
mod write;

pub use admin::PingInfo;
pub use auth::Credentials;
pub use query::{QueryOptions, QueryResponse, SeriesResult, StatementResult};
pub use write::WriteOptions;

use crate::config::ConnectionConfig;
use crate::error::{CliError, Result};

#[derive(Clone)]
pub struct HyperbytedbClient {
    pub(crate) http: reqwest::Client,
    pub(crate) base: String,
    pub(crate) credentials: Credentials,
}

impl HyperbytedbClient {
    pub fn new(config: &ConnectionConfig) -> Result<Self> {
        let mut builder = reqwest::Client::builder();
        if config.unsafe_ssl {
            builder = builder.danger_accept_invalid_certs(true);
        }
        let http = builder
            .build()
            .map_err(|e| CliError::Connection(e.to_string()))?;

        Ok(Self {
            http,
            base: config.base_url(),
            credentials: Credentials::from_config(config),
        })
    }

    pub fn with_credentials(mut self, username: Option<String>, password: Option<String>) -> Self {
        self.credentials = Credentials { username, password };
        self
    }

    pub fn base_url(&self) -> &str {
        &self.base
    }

    pub fn set_base_url(&mut self, url: String) {
        self.base = url.trim_end_matches('/').to_string();
    }
}
