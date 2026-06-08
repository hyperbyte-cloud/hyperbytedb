use serde::{Deserialize, Serialize};

use crate::domain::database::RetentionPolicy;
use crate::domain::point::Point;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncManifest {
    pub node_id: u64,
    pub wal_last_seq: u64,
    pub databases: Vec<DatabaseManifest>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseManifest {
    pub name: String,
    pub retention_policies: Vec<RetentionPolicy>,
    pub measurements: Vec<MeasurementManifest>,
    pub users: Vec<String>,
    pub continuous_queries: Vec<String>,
    pub tombstones: Vec<(String, Vec<(String, String)>)>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MeasurementManifest {
    pub name: String,
    pub rp: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetadataSnapshot {
    pub entries: Vec<MetadataEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetadataEntry {
    pub key: String,
    pub value: Vec<u8>,
}

/// Request to join the cluster.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JoinRequest {
    pub node_id: Option<u64>,
    pub addr: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JoinResponse {
    pub assigned_node_id: u64,
    pub membership: super::membership::ClusterMembership,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeaveRequest {
    pub node_id: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeaveResponse {
    pub ok: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalSyncRequest {
    pub from_seq: u64,
    pub max_entries: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalSyncResponse {
    pub entries: Vec<WalSyncEntry>,
    pub last_seq: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalSyncEntry {
    pub seq: u64,
    pub database: String,
    pub retention_policy: String,
    pub points: Vec<Point>,
    #[serde(default)]
    pub origin_node_id: u64,
}
