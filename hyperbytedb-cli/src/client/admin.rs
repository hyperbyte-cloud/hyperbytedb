use crate::error::{CliError, Result};

use super::HyperbytedbClient;

#[derive(Debug, Clone)]
pub struct PingInfo {
    pub version: Option<String>,
    pub build: Option<String>,
}

impl HyperbytedbClient {
    pub async fn ping(&self) -> Result<PingInfo> {
        let headers = self.auth_headers();
        let header_refs: Vec<(&str, &str)> = headers
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        let resp = self.request("GET", "/ping", "", &header_refs, None).await?;
        if resp.status != 204 && !(200..300).contains(&resp.status) {
            let body = String::from_utf8_lossy(&resp.body);
            return Err(CliError::from_status(
                reqwest::StatusCode::from_u16(resp.status)
                    .unwrap_or(reqwest::StatusCode::BAD_REQUEST),
                &body,
            ));
        }
        Ok(PingInfo {
            version: resp.version,
            build: resp.build,
        })
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
        let headers = self.auth_headers();
        let header_refs: Vec<(&str, &str)> = headers
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        let resp = self
            .request("POST", "/internal/drain", "", &header_refs, None)
            .await?;
        if !(200..300).contains(&resp.status) {
            let body = String::from_utf8_lossy(&resp.body);
            return Err(CliError::from_status(
                reqwest::StatusCode::from_u16(resp.status)
                    .unwrap_or(reqwest::StatusCode::BAD_REQUEST),
                &body,
            ));
        }
        Ok(String::from_utf8_lossy(&resp.body).into_owned())
    }

    async fn get_text(&self, path: &str) -> Result<String> {
        let headers = self.auth_headers();
        let header_refs: Vec<(&str, &str)> = headers
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        let resp = self.request("GET", path, "", &header_refs, None).await?;
        if !(200..300).contains(&resp.status) {
            let body = String::from_utf8_lossy(&resp.body);
            return Err(CliError::from_status(
                reqwest::StatusCode::from_u16(resp.status)
                    .unwrap_or(reqwest::StatusCode::BAD_REQUEST),
                &body,
            ));
        }
        Ok(String::from_utf8_lossy(&resp.body).into_owned())
    }
}
