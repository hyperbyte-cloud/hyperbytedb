use serde::{Deserialize, Serialize};

use crate::domain::continuous_query::ContinuousQueryDef;
use crate::domain::database::RetentionPolicy;
use crate::domain::materialized_view::MaterializedViewDef;
use crate::domain::measurement::MeasurementMeta;
use crate::timeseriesql::ast::RetentionPolicyChange;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MutationRequest {
    CreateDatabase {
        name: String,
        rp: Option<RetentionPolicy>,
    },
    DropDatabase(String),
    CreateRetentionPolicy {
        db: String,
        rp: RetentionPolicy,
    },
    DropRetentionPolicy {
        db: String,
        name: String,
    },
    CreateUser {
        username: String,
        password_hash: String,
        admin: bool,
    },
    DropUser(String),
    SetPassword {
        username: String,
        password_hash: String,
    },
    Delete {
        database: String,
        rp: String,
        measurement: String,
        predicate_sql: String,
    },
    CreateContinuousQuery {
        database: String,
        name: String,
        definition: ContinuousQueryDef,
    },
    DropContinuousQuery {
        database: String,
        name: String,
    },
    CreateMaterializedView {
        database: String,
        name: String,
        definition: MaterializedViewDef,
        /// The source measurement's schema on the proposing node. With
        /// sharding a node has the measurement only once rows for it land
        /// there, and it needs it to create the view before the first row.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        source_meta: Option<Box<MeasurementMeta>>,
    },
    DropMaterializedView {
        database: String,
        name: String,
    },
    AlterRetentionPolicy {
        db: String,
        name: String,
        change: RetentionPolicyChange,
    },
    DropSeries {
        database: String,
        rp: String,
        measurement: Option<String>,
        predicate_sql: String,
    },
    DropMeasurement {
        database: String,
        rp: String,
        name: String,
    },
    Grant {
        username: String,
        database: Option<String>,
        #[serde(default)]
        privilege: crate::domain::user::DatabasePrivilege,
    },
    Revoke {
        username: String,
        database: Option<String>,
        #[serde(default)]
        privilege: crate::domain::user::DatabasePrivilege,
    },
    /// `SET <setting> = <value> | DEFAULT`. A node on a build without it
    /// cannot decode it: do not run `SET` during a rolling upgrade.
    SetSetting {
        name: String,
        value: Option<crate::domain::setting::SettingValue>,
    },
}

/// Wrapper sent over the wire -- carries the sender's seq for ack tracking.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MutationReplicateRequest {
    pub seq: u64,
    #[serde(default)]
    pub origin_node_id: u64,
    pub mutation: MutationRequest,
    /// When set, replicate only to these node ids (sharding region peers).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_node_ids: Option<Vec<u64>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MutationReplicateResponse {
    pub ok: bool,
    pub ack_seq: u64,
}
