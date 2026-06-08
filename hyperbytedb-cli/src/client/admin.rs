use serde_json::Value;

use crate::error::{CliError, Result};

use super::HyperbytedbClient;

#[derive(Debug, Clone)]
pub struct PingInfo {
    pub version: Option<String>,
    pub build: Option<String>,
}

impl HyperbytedbClient {
    pub async fn ping(&self) -> Result<PingInfo> {
        let url = format!("{}/ping", self.base_url());
        let mut req = self.http.get(&url);
        req = self.credentials.apply_basic_auth(req);

        let resp = req
            .send()
            .await
            .map_err(|e| CliError::Connection(e.to_string()))?;
        let status = resp.status();
        if !status.is_success() && status.as_u16() != 204 {
            let body = resp.text().await.unwrap_or_default();
            return Err(CliError::from_status(status, &body));
        }

        let version = resp
            .headers()
            .get("X-Influxdb-Version")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());
        let build = resp
            .headers()
            .get("X-Influxdb-Build")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());

        Ok(PingInfo { version, build })
    }

    pub async fn health(&self) -> Result<String> {
        self.get_text("/health").await
    }

    pub async fn health_ready(&self) -> Result<String> {
        self.get_text("/health/ready").await
    }

    pub async fn metrics(&self) -> Result<String> {
        self.get_text("/metrics").await
    }

    pub async fn statements(&self) -> Result<String> {
        self.get_text("/api/v1/statements").await
    }

    pub async fn cluster_nodes(&self) -> Result<String> {
        self.get_text("/cluster/nodes").await
    }

    pub async fn cluster_leader(&self) -> Result<String> {
        self.get_text("/cluster/leader").await
    }

    pub async fn cluster_metrics(&self) -> Result<String> {
        self.get_text("/cluster/metrics").await
    }

    pub async fn cluster_drain(&self) -> Result<String> {
        let url = format!("{}/internal/drain", self.base_url());
        let mut req = self.http.post(&url);
        req = self.credentials.apply_basic_auth(req);

        let resp = req
            .send()
            .await
            .map_err(|e| CliError::Connection(e.to_string()))?;
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        if !status.is_success() {
            return Err(CliError::from_status(status, &body));
        }
        Ok(body)
    }

    async fn get_text(&self, path: &str) -> Result<String> {
        let url = format!("{}{}", self.base_url(), path);
        let mut req = self.http.get(&url);
        req = self.credentials.apply_basic_auth(req);

        let resp = req
            .send()
            .await
            .map_err(|e| CliError::Connection(e.to_string()))?;
        let status = resp.status();
        let body = resp
            .text()
            .await
            .map_err(|e| CliError::Connection(e.to_string()))?;
        if !status.is_success() {
            return Err(CliError::from_status(status, &body));
        }
        Ok(body)
    }

    pub async fn get_json(&self, path: &str) -> Result<Value> {
        let text = self.get_text(path).await?;
        serde_json::from_str(&text).map_err(|e| CliError::Other(format!("invalid JSON: {e}")))
    }
}
