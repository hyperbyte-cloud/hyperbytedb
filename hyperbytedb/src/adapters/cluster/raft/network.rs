use openraft::BasicNode;
use openraft::error::{NetworkError, RPCError, RaftError};
use openraft::network::RPCOption;
use openraft::network::{RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};

use super::TypeConfig;

/// HTTP-based Raft network transport using reqwest.
pub struct Network {
    client: reqwest::Client,
}

impl Default for Network {
    fn default() -> Self {
        Self::new()
    }
}

impl Network {
    pub fn new() -> Self {
        Self {
            // `Client::builder().build()` only fails if TLS init is broken,
            // in which case `Client::new()` (same default config) would also
            // be unusable. Fall back to defaults so we don't panic on a path
            // that's exercised on the hot bootstrap line.
            client: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(10))
                .build()
                .unwrap_or_else(|_| reqwest::Client::new()),
        }
    }
}

impl RaftNetworkFactory<TypeConfig> for Network {
    type Network = NetworkConnection;

    async fn new_client(&mut self, _target: u64, node: &BasicNode) -> Self::Network {
        NetworkConnection {
            addr: node.addr.clone(),
            client: self.client.clone(),
        }
    }
}

/// A connection to a single remote Raft node.
pub struct NetworkConnection {
    addr: String,
    client: reqwest::Client,
}

impl NetworkConnection {
    fn url(&self, path: &str) -> String {
        format!("http://{}{}", self.addr, path)
    }

    async fn post_json<Req: serde::Serialize, Resp: serde::de::DeserializeOwned>(
        &self,
        path: &str,
        req: &Req,
    ) -> Result<Resp, RPCError<u64, BasicNode, RaftError<u64>>> {
        let resp = self
            .client
            .post(self.url(path))
            .json(req)
            .send()
            .await
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;

        let status = resp.status();
        let body = resp
            .bytes()
            .await
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;

        if !status.is_success() {
            let msg = String::from_utf8_lossy(&body).to_string();
            return Err(RPCError::Network(NetworkError::new(
                &std::io::Error::other(format!("HTTP {}: {}", status, msg)),
            )));
        }

        serde_json::from_slice(&body).map_err(|e| RPCError::Network(NetworkError::new(&e)))
    }

    async fn post_json_snapshot<Req: serde::Serialize, Resp: serde::de::DeserializeOwned>(
        &self,
        path: &str,
        req: &Req,
    ) -> Result<Resp, RPCError<u64, BasicNode, RaftError<u64, openraft::error::InstallSnapshotError>>>
    {
        let resp = self
            .client
            .post(self.url(path))
            .json(req)
            .send()
            .await
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;

        let status = resp.status();
        let body = resp
            .bytes()
            .await
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;

        if !status.is_success() {
            let msg = String::from_utf8_lossy(&body).to_string();
            return Err(RPCError::Network(NetworkError::new(
                &std::io::Error::other(format!("HTTP {}: {}", status, msg)),
            )));
        }

        serde_json::from_slice(&body).map_err(|e| RPCError::Network(NetworkError::new(&e)))
    }
}

impl RaftNetwork<TypeConfig> for NetworkConnection {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        self.post_json("/internal/raft/append", &rpc).await
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<u64>,
        RPCError<u64, BasicNode, RaftError<u64, openraft::error::InstallSnapshotError>>,
    > {
        self.post_json_snapshot("/internal/raft/snapshot", &rpc)
            .await
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<u64>,
        _option: RPCOption,
    ) -> Result<VoteResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        self.post_json("/internal/raft/vote", &rpc).await
    }
}
