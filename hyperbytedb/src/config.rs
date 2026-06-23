use figment::{
    Figment,
    providers::{Env, Format, Serialized, Toml},
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct HyperbytedbConfig {
    pub server: ServerConfig,
    pub storage: StorageConfig,
    pub flush: FlushConfig,
    pub chdb: ChdbConfig,
    pub auth: AuthConfig,
    pub cardinality: CardinalityConfig,
    pub cluster: ClusterConfig,
    pub logging: LoggingConfig,
    pub statement_summary: StatementSummaryConfig,
    pub hinted_handoff: HintedHandoffConfig,
    pub rate_limit: RateLimitConfig,
    #[serde(default)]
    pub retention: RetentionConfig,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RateLimitConfig {
    pub enabled: bool,
    /// Maximum requests per second per endpoint (/write, /query). 0 = unlimited.
    pub max_requests_per_second: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ServerConfig {
    pub bind_address: String,
    pub port: u16,
    pub max_body_size_bytes: usize,
    pub request_timeout_secs: u64,
    pub query_timeout_secs: u64,
    pub max_concurrent_queries: usize,
    pub tls_enabled: bool,
    pub tls_cert_path: String,
    pub tls_key_path: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct StorageConfig {
    pub wal_dir: String,
    pub meta_dir: String,
    /// Durable WAL value encoding: `bincode` (default) or `arrow_ipc`.
    #[serde(default = "default_wal_format")]
    pub wal_format: String,
}

fn default_wal_format() -> String {
    "bincode".to_string()
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct FlushConfig {
    pub interval_secs: u64,
    pub wal_size_threshold_mb: u64,
    pub time_bucket_duration: String,
    /// Max points per chDB insert batch. `0` uses [`default_max_points_per_batch`].
    #[serde(default = "default_max_points_per_batch")]
    pub max_points_per_batch: usize,
    /// WAL group-commit: max entries to coalesce per write batch (0 = disabled).
    #[serde(default = "default_wal_batch_size")]
    pub wal_batch_size: usize,
    /// WAL group-commit: max microseconds to wait for more entries before flushing.
    #[serde(default = "default_wal_batch_delay_us")]
    pub wal_batch_delay_us: u64,
    /// When true, keep chDB-ready Arrow batches in an in-memory WAL cache for
    /// zero-copy flush. Requires ingest to supply prepared slots via
    /// [`WalAppendBundle`].
    #[serde(default = "default_arrow_wal_enabled")]
    pub arrow_wal_enabled: bool,
}

fn default_arrow_wal_enabled() -> bool {
    true
}

fn default_max_points_per_batch() -> usize {
    50_000
}

fn default_wal_batch_size() -> usize {
    64
}

fn default_wal_batch_delay_us() -> u64 {
    200
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ChdbConfig {
    pub session_data_path: String,
    /// Number of chDB connections opened to the same `session_data_path`.
    /// Each connection has its own `ChdbClient` mutex, so flush inserts and
    /// concurrent queries overlap when `pool_size > 1`. A second connection
    /// to a *different* path still fails (process-global singleton per path).
    /// Clamped to 1..=32. For best overlap, set `server.max_concurrent_queries`
    /// ≥ `pool_size`.
    pub pool_size: usize,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct CardinalityConfig {
    pub max_tag_values_per_measurement: usize,
    pub max_measurements_per_database: usize,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AuthConfig {
    pub enabled: bool,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ClusterConfig {
    pub enabled: bool,
    pub node_id: u64,
    pub cluster_addr: String,
    /// **Deprecated.** Comma-separated list of peer addresses (e.g.
    /// `"host1:8086,host2:8086"`) used to seed cluster formation when no
    /// external orchestrator is present.
    ///
    /// When empty (the default), the cluster is operator-driven: the
    /// bootstrap node initializes Raft as a single-member cluster and an
    /// external controller (e.g. the Kubernetes operator) is responsible
    /// for adding new nodes via the `/cluster/membership/add-node` (or
    /// `/cluster/raft/add-learner` + `/cluster/raft/change-membership`) API
    /// as the StatefulSet scales.
    #[serde(default)]
    pub peers: String,
    pub heartbeat_interval_secs: u64,
    pub heartbeat_miss_threshold: u64,
    /// **Deprecated.** Merkle anti-entropy was removed in 0.7. The field is
    /// kept so existing TOML keeps deserializing; the value is logged and
    /// otherwise ignored.
    #[serde(default = "default_anti_entropy_enabled")]
    pub anti_entropy_enabled: bool,
    /// **Deprecated.** See [`Self::anti_entropy_enabled`].
    #[serde(default = "default_anti_entropy_interval_secs")]
    pub anti_entropy_interval_secs: u64,
    pub replication_log_dir: String,
    pub raft_dir: String,
    pub replication_max_retries: u32,
    /// Bounded outbound replication queue (ingest-sized batches).
    #[serde(default = "default_replication_queue_depth")]
    pub replication_queue_depth: usize,
    /// Max concurrent outbound replication fan-out rounds (token bucket).
    #[serde(default = "default_replication_max_inflight_batches")]
    pub replication_max_inflight_batches: usize,
    /// Max bytes for coalescing consecutive WAL batches (same db/rp/precision).
    #[serde(default = "default_replication_max_coalesce_body_bytes")]
    pub replication_max_coalesce_body_bytes: usize,
    /// Bounded apply queue on the replicate receiver.
    #[serde(default = "default_replicate_receiver_queue_depth")]
    pub replicate_receiver_queue_depth: usize,
    /// Deprecated: receiver always uses a single ordered worker. Kept for backward-compatible TOML.
    #[serde(default = "default_replicate_receiver_workers")]
    pub replicate_receiver_workers: usize,
    /// When >0, peers with ack 0 and stale heartbeats are omitted from truncate barrier.
    #[serde(default = "default_replication_truncate_stale_peer_multiplier")]
    pub replication_truncate_stale_peer_multiplier: u64,
    /// Raft heartbeat interval in milliseconds (default: 300).
    pub raft_heartbeat_interval_ms: Option<u64>,
    /// Raft election timeout in milliseconds (default: 1000).
    pub raft_election_timeout_ms: Option<u64>,
    /// Number of log entries since last snapshot before a new snapshot is taken (default: 1000).
    pub raft_snapshot_threshold: Option<u32>,
    /// Per-node replication mode and tuning. When the entire `[cluster.replication]`
    /// block is omitted, the resolved mode is `async`, exactly preserving today's
    /// fire-and-forget behavior.
    #[serde(default)]
    pub replication: ReplicationConfig,
}

/// Per-node, per-write replication mode. Controls only the COORDINATOR side
/// (how this node's accepted client writes are replicated). Receivers always
/// accept both async (fire-and-forget) and sync (header-gated, ack-with-WAL-seq)
/// requests on the SAME `/internal/replicate` endpoint.
#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[derive(Default)]
pub enum ReplicationMode {
    /// Fire-and-forget HTTP fan-out (current behavior). Local WAL append
    /// succeeds immediately, replication is best-effort with hinted handoff.
    #[default]
    Async,
    /// HTTP fan-out + await W-of-N peer acks before returning to the client.
    /// W is `sync_quorum.min_acks` resolved against current `active_peers().len()`.
    SyncQuorum,
}

impl ReplicationMode {
    pub fn as_str(self) -> &'static str {
        match self {
            ReplicationMode::Async => "async",
            ReplicationMode::SyncQuorum => "sync_quorum",
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ReplicationConfig {
    #[serde(default)]
    pub mode: ReplicationMode,
    /// Bound on the worst-case client-perceived latency for `sync_quorum`
    /// writes. On timeout the coordinator returns 504 and unacked peers fall
    /// back to hinted-handoff in the background.
    #[serde(default = "default_replication_ack_timeout_ms")]
    pub ack_timeout_ms: u64,
    #[serde(default)]
    pub sync_quorum: SyncQuorumConfig,
}

impl Default for ReplicationConfig {
    fn default() -> Self {
        Self {
            mode: ReplicationMode::Async,
            ack_timeout_ms: default_replication_ack_timeout_ms(),
            sync_quorum: SyncQuorumConfig::default(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct SyncQuorumConfig {
    /// Number of peer acks required for `sync_quorum`. The local WAL append
    /// always happens before fan-out, so self-durability is implicit and the
    /// local node is never counted toward the quorum.
    #[serde(default)]
    pub min_acks: SyncQuorumMinAcks,
}

/// Either `"majority"` (resolved at request time against current
/// `active_peers().len()`) or an explicit integer count.
#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(untagged)]
pub enum SyncQuorumMinAcks {
    Keyword(SyncQuorumMinAcksKeyword),
    Count(usize),
}

impl Default for SyncQuorumMinAcks {
    fn default() -> Self {
        Self::Keyword(SyncQuorumMinAcksKeyword::Majority)
    }
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SyncQuorumMinAcksKeyword {
    Majority,
}

impl SyncQuorumMinAcks {
    /// Resolve the configured `min_acks` against the current peer count.
    /// Returns the number of peer acks required (clamped to `[0, peer_count]`).
    /// Self is never counted (the local WAL append already happened).
    pub fn resolve(self, peer_count: usize) -> usize {
        let raw = match self {
            // Majority across the FULL cluster (peers + self). For a cluster
            // of N nodes total, majority is `floor(N/2) + 1`. Subtracting 1
            // for self gives `floor(N/2)` peer acks required.
            SyncQuorumMinAcks::Keyword(SyncQuorumMinAcksKeyword::Majority) => {
                peer_count.div_ceil(2)
            }
            SyncQuorumMinAcks::Count(n) => n,
        };
        raw.min(peer_count)
    }
}

fn default_replication_ack_timeout_ms() -> u64 {
    5000
}

fn default_replication_queue_depth() -> usize {
    8192
}

fn default_anti_entropy_enabled() -> bool {
    false
}

fn default_anti_entropy_interval_secs() -> u64 {
    60
}

fn default_replication_max_inflight_batches() -> usize {
    8
}

fn default_replication_max_coalesce_body_bytes() -> usize {
    8 * 1024 * 1024
}

fn default_replicate_receiver_queue_depth() -> usize {
    1024
}

fn default_replicate_receiver_workers() -> usize {
    1
}

fn default_replication_truncate_stale_peer_multiplier() -> u64 {
    2
}

impl ClusterConfig {
    pub fn peer_list(&self) -> Vec<String> {
        if self.peers.is_empty() {
            return Vec::new();
        }
        self.peers
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect()
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct LoggingConfig {
    pub level: String,
    pub format: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct StatementSummaryConfig {
    pub enabled: bool,
    pub max_entries: usize,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct HintedHandoffConfig {
    pub enabled: bool,
    /// Maximum queued hints per unreachable peer before oldest are dropped.
    pub max_hints_per_peer: u64,
    /// Hints older than this are discarded on drain. Seconds.
    pub max_hint_age_secs: u64,
}

/// Controls the retention policy enforcement loop.
///
/// The retention service periodically scans every database / retention
/// policy / measurement triple and deletes rows older than the policy's
/// configured `duration` from the embedded chDB tables. This config governs
/// only how often that scan runs; the per-policy `duration` is metadata
/// stored alongside each retention policy (`CREATE/ALTER RETENTION POLICY`).
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RetentionConfig {
    /// When `false`, the retention enforcement loop is not spawned and
    /// expired data stays in chDB until removed manually or via DDL.
    /// Defaults to `true`.
    #[serde(default = "default_retention_enabled")]
    pub enabled: bool,

    /// How often retention enforcement scans run, expressed as a
    /// `humantime` duration string ("1m", "1h", "24h", "30s", ...).
    /// Defaults to `"12h"`.
    /// Values that fail to parse fall back to the default and emit a
    /// warning at startup.
    #[serde(default = "default_retention_interval")]
    pub interval: String,
}

fn default_retention_enabled() -> bool {
    true
}

fn default_retention_interval() -> String {
    "12h".to_string()
}

impl Default for RetentionConfig {
    fn default() -> Self {
        Self {
            enabled: default_retention_enabled(),
            interval: default_retention_interval(),
        }
    }
}

impl RetentionConfig {
    /// Default interval used when [`Self::interval`] fails to parse.
    pub const FALLBACK_INTERVAL: std::time::Duration = std::time::Duration::from_secs(60);

    /// Parse [`Self::interval`] into a [`std::time::Duration`].
    ///
    /// Accepts any `humantime` form (`"1m"`, `"1h"`, `"24h"`, `"30s"`,
    /// `"1d"`, ...). On parse failure the [`Self::FALLBACK_INTERVAL`]
    /// (60 seconds) is returned and the caller should log a warning so
    /// operators see invalid TOML at startup. A zero duration is
    /// rejected the same way to avoid a busy-loop scan.
    pub fn interval_duration(&self) -> std::time::Duration {
        match humantime::parse_duration(self.interval.trim()) {
            Ok(d) if !d.is_zero() => d,
            _ => Self::FALLBACK_INTERVAL,
        }
    }
}

fn default_chdb_pool_size() -> usize {
    crate::adapters::chdb::connection_pool::DEFAULT_POOL_SIZE
}

impl HyperbytedbConfig {
    pub fn load(config_path: Option<&str>) -> anyhow::Result<Self> {
        let mut figment = Figment::new().merge(Serialized::defaults(Self::defaults()));

        if let Some(path) = config_path
            && std::path::Path::new(path).exists()
        {
            figment = figment.merge(Toml::file(path));
        }

        figment = figment.merge(Env::prefixed("HYPERBYTEDB__").split("__"));
        Ok(figment.extract()?)
    }

    fn defaults() -> Self {
        Self {
            server: ServerConfig {
                bind_address: "0.0.0.0".to_string(),
                port: 8086,
                max_body_size_bytes: 25 * 1024 * 1024,
                request_timeout_secs: 30,
                query_timeout_secs: 30,
                max_concurrent_queries: 0,
                tls_enabled: false,
                tls_cert_path: String::new(),
                tls_key_path: String::new(),
            },
            storage: StorageConfig {
                wal_dir: "./wal".to_string(),
                meta_dir: "./meta".to_string(),
                wal_format: default_wal_format(),
            },
            flush: FlushConfig {
                interval_secs: 10,
                wal_size_threshold_mb: 64,
                time_bucket_duration: "1h".to_string(),
                max_points_per_batch: default_max_points_per_batch(),
                wal_batch_size: default_wal_batch_size(),
                wal_batch_delay_us: default_wal_batch_delay_us(),
                arrow_wal_enabled: default_arrow_wal_enabled(),
            },
            chdb: ChdbConfig {
                session_data_path: "./chdb_data".to_string(),
                pool_size: default_chdb_pool_size(),
            },
            auth: AuthConfig { enabled: false },
            cardinality: CardinalityConfig {
                max_tag_values_per_measurement: 100_000,
                max_measurements_per_database: 10_000,
            },
            cluster: ClusterConfig {
                enabled: false,
                node_id: 1,
                cluster_addr: "127.0.0.1:8086".to_string(),
                peers: String::new(),
                heartbeat_interval_secs: 2,
                heartbeat_miss_threshold: 5,
                anti_entropy_enabled: false,
                anti_entropy_interval_secs: 60,
                replication_log_dir: "./replication_log".to_string(),
                raft_dir: "./raft".to_string(),
                replication_max_retries: 5,
                replication_queue_depth: default_replication_queue_depth(),
                replication_max_inflight_batches: default_replication_max_inflight_batches(),
                replication_max_coalesce_body_bytes: default_replication_max_coalesce_body_bytes(),
                replicate_receiver_queue_depth: default_replicate_receiver_queue_depth(),
                replicate_receiver_workers: default_replicate_receiver_workers(),
                replication_truncate_stale_peer_multiplier:
                    default_replication_truncate_stale_peer_multiplier(),
                raft_heartbeat_interval_ms: None,
                raft_election_timeout_ms: None,
                raft_snapshot_threshold: None,
                replication: ReplicationConfig::default(),
            },
            logging: LoggingConfig {
                level: "info".to_string(),
                format: "text".to_string(),
            },
            statement_summary: StatementSummaryConfig {
                enabled: true,
                max_entries: 1000,
            },
            hinted_handoff: HintedHandoffConfig {
                enabled: true,
                max_hints_per_peer: 100_000,
                max_hint_age_secs: 3600,
            },
            rate_limit: RateLimitConfig {
                enabled: false,
                max_requests_per_second: 0,
            },
            retention: RetentionConfig::default(),
        }
    }
}

#[cfg(test)]
mod replication_config_tests {
    use super::{ReplicationConfig, ReplicationMode, SyncQuorumMinAcks, SyncQuorumMinAcksKeyword};

    #[test]
    fn replication_defaults_to_async_when_block_missing() {
        let r = ReplicationConfig::default();
        assert_eq!(r.mode, ReplicationMode::Async);
        assert_eq!(r.ack_timeout_ms, 5000);
        assert!(matches!(
            r.sync_quorum.min_acks,
            SyncQuorumMinAcks::Keyword(SyncQuorumMinAcksKeyword::Majority)
        ));
    }

    #[test]
    fn replication_partial_json_keeps_defaults() {
        let r: ReplicationConfig = serde_json::from_str(r#"{"mode":"sync_quorum"}"#).unwrap();
        assert_eq!(r.mode, ReplicationMode::SyncQuorum);
        assert_eq!(r.ack_timeout_ms, 5000);
        assert!(matches!(
            r.sync_quorum.min_acks,
            SyncQuorumMinAcks::Keyword(SyncQuorumMinAcksKeyword::Majority)
        ));
    }

    #[test]
    fn replication_explicit_count_min_acks() {
        let r: ReplicationConfig =
            serde_json::from_str(r#"{"mode":"sync_quorum","sync_quorum":{"min_acks":2}}"#).unwrap();
        assert_eq!(r.sync_quorum.min_acks, SyncQuorumMinAcks::Count(2));
    }

    #[test]
    fn min_acks_majority_resolves_against_peer_count() {
        let m = SyncQuorumMinAcks::Keyword(SyncQuorumMinAcksKeyword::Majority);
        // 1-node cluster: 0 peers, 0 peer acks needed
        assert_eq!(m.resolve(0), 0);
        // 2-node cluster: 1 peer, majority of (1+1)=2 -> 1, minus self -> 0 peer acks
        assert_eq!(m.resolve(1), 1);
        // 3-node cluster: 2 peers, majority of (2+1)=3 -> 2, minus self -> 1 peer ack
        assert_eq!(m.resolve(2), 1);
        // 5-node cluster: 4 peers, majority of (4+1)=5 -> 3, minus self -> 2 peer acks
        assert_eq!(m.resolve(4), 2);
    }

    #[test]
    fn min_acks_explicit_clamps_to_peer_count() {
        let m = SyncQuorumMinAcks::Count(5);
        assert_eq!(m.resolve(0), 0);
        assert_eq!(m.resolve(2), 2);
        assert_eq!(m.resolve(10), 5);
    }
}

#[cfg(test)]
mod retention_config_tests {
    use super::RetentionConfig;
    use std::time::Duration;

    #[test]
    fn defaults_to_60s_enabled() {
        let r = RetentionConfig::default();
        assert!(r.enabled);
        assert_eq!(r.interval_duration(), Duration::from_secs(60));
    }

    #[test]
    fn parses_minute_form() {
        let r = RetentionConfig {
            enabled: true,
            interval: "1m".into(),
        };
        assert_eq!(r.interval_duration(), Duration::from_secs(60));
    }

    #[test]
    fn parses_hour_form() {
        let r = RetentionConfig {
            enabled: true,
            interval: "1h".into(),
        };
        assert_eq!(r.interval_duration(), Duration::from_secs(3600));
    }

    #[test]
    fn parses_24h_form() {
        let r = RetentionConfig {
            enabled: true,
            interval: "24h".into(),
        };
        assert_eq!(r.interval_duration(), Duration::from_secs(86_400));
    }

    #[test]
    fn rejects_zero_with_fallback() {
        let r = RetentionConfig {
            enabled: true,
            interval: "0s".into(),
        };
        assert_eq!(r.interval_duration(), RetentionConfig::FALLBACK_INTERVAL);
    }

    #[test]
    fn rejects_garbage_with_fallback() {
        let r = RetentionConfig {
            enabled: true,
            interval: "not-a-duration".into(),
        };
        assert_eq!(r.interval_duration(), RetentionConfig::FALLBACK_INTERVAL);
    }

    #[test]
    fn deserializes_from_partial_json() {
        let r: RetentionConfig = serde_json::from_str(r#"{"interval":"5m"}"#).expect("parse");
        assert!(r.enabled);
        assert_eq!(r.interval_duration(), Duration::from_secs(5 * 60));
    }

    #[test]
    fn deserializes_disabled() {
        let r: RetentionConfig = serde_json::from_str(r#"{"enabled":false}"#).expect("parse");
        assert!(!r.enabled);
        assert_eq!(r.interval, "60s");
    }
}
