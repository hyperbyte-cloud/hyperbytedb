use std::time::Duration;

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
    default_timeout: Duration,
    /// `cluster.control_port`; `0` sends Raft RPCs to the main address only.
    control_port: u16,
}

impl Network {
    pub fn new(
        default_timeout_secs: u64,
        control_port: u16,
        cluster_auth: &crate::adapters::cluster::cluster_auth::ClusterAuth,
    ) -> Self {
        use crate::ports::peer_http::PeerHttp;
        let default_timeout = Duration::from_secs(default_timeout_secs);
        Self {
            // `Client::builder().build()` only fails if TLS init is broken,
            // in which case `Client::new()` (same default config) would also
            // be unusable. Fall back to defaults so we don't panic on a path
            // that's exercised on the hot bootstrap line.
            client: cluster_auth.build(
                cluster_auth
                    .client_builder()
                    .timeout(default_timeout)
                    .dns_resolver(crate::adapters::cluster::dns_cache::CachingResolver::shared()),
            ),
            default_timeout,
            control_port,
        }
    }
}

/// A peer's control-listener address: its host on `control_port`, or `None`
/// when the control listener is disabled.
fn control_addr(addr: &str, control_port: u16) -> Option<String> {
    if control_port == 0 {
        return None;
    }
    let (host, _) = addr.rsplit_once(':')?;
    Some(format!("{host}:{control_port}"))
}

impl RaftNetworkFactory<TypeConfig> for Network {
    type Network = NetworkConnection;

    async fn new_client(&mut self, _target: u64, node: &BasicNode) -> Self::Network {
        NetworkConnection {
            addr: node.addr.clone(),
            control_addr: control_addr(&node.addr, self.control_port),
            client: self.client.clone(),
            default_timeout: self.default_timeout,
        }
    }
}

/// A connection to a single remote Raft node.
pub struct NetworkConnection {
    addr: String,
    /// The peer's control listener, tried first when configured.
    control_addr: Option<String>,
    client: reqwest::Client,
    default_timeout: Duration,
}

impl NetworkConnection {
    /// POST `req` to the peer's control listener, falling back to its main
    /// address when nothing accepts the connection there (a peer still on a
    /// build without the control listener, mid-roll).
    async fn send<Req: serde::Serialize>(
        &self,
        path: &str,
        req: &Req,
        timeout: Duration,
    ) -> Result<reqwest::Response, reqwest::Error> {
        if let Some(ctrl) = &self.control_addr {
            match self
                .client
                .post(format!("http://{ctrl}{path}"))
                .json(req)
                .timeout(timeout)
                .send()
                .await
            {
                Err(e) if e.is_connect() => {
                    metrics::counter!("hyperbytedb_raft_control_fallback_total").increment(1);
                }
                other => return other,
            }
        }
        self.client
            .post(format!("http://{}{path}", self.addr))
            .json(req)
            .timeout(timeout)
            .send()
            .await
    }

    fn effective_timeout(option: &RPCOption, default: Duration) -> Duration {
        let ttl = option.hard_ttl();
        if ttl.is_zero() { default } else { ttl }
    }

    async fn post_json<Req: serde::Serialize, Resp: serde::de::DeserializeOwned>(
        &self,
        path: &str,
        req: &Req,
        option: &RPCOption,
    ) -> Result<Resp, RPCError<u64, BasicNode, RaftError<u64>>> {
        let timeout = Self::effective_timeout(option, self.default_timeout);
        let resp = self
            .send(path, req, timeout)
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
        option: &RPCOption,
    ) -> Result<Resp, RPCError<u64, BasicNode, RaftError<u64, openraft::error::InstallSnapshotError>>>
    {
        let timeout = Self::effective_timeout(option, self.default_timeout);
        let resp = self
            .send(path, req, timeout)
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
        option: RPCOption,
    ) -> Result<AppendEntriesResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        self.post_json("/internal/raft/append", &rpc, &option).await
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<u64>,
        RPCError<u64, BasicNode, RaftError<u64, openraft::error::InstallSnapshotError>>,
    > {
        self.post_json_snapshot("/internal/raft/snapshot", &rpc, &option)
            .await
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<u64>,
        option: RPCOption,
    ) -> Result<VoteResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        self.post_json("/internal/raft/vote", &rpc, &option).await
    }
}

#[cfg(test)]
mod tests {
    use super::{NetworkConnection, control_addr};

    #[test]
    fn control_address_swaps_the_port_or_is_off() {
        assert_eq!(
            control_addr("hb-0.hb-headless.ns.svc:8086", 8087).as_deref(),
            Some("hb-0.hb-headless.ns.svc:8087")
        );
        assert_eq!(
            control_addr("[::1]:8086", 9000).as_deref(),
            Some("[::1]:9000")
        );
        assert_eq!(control_addr("hb-0:8086", 0), None);
    }
    use openraft::network::RPCOption;
    use std::time::Duration;

    #[test]
    fn effective_timeout_uses_rpc_option_when_non_zero() {
        let option = RPCOption::new(Duration::from_secs(5));
        assert_eq!(
            NetworkConnection::effective_timeout(&option, Duration::from_secs(10)),
            Duration::from_secs(5)
        );
    }

    #[test]
    fn effective_timeout_falls_back_to_default_when_zero() {
        let option = RPCOption::new(Duration::ZERO);
        assert_eq!(
            NetworkConnection::effective_timeout(&option, Duration::from_secs(10)),
            Duration::from_secs(10)
        );
    }
}
