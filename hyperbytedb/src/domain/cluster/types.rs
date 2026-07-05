use serde::{Deserialize, Serialize};

use crate::domain::database::RetentionPolicy;
use crate::ports::metadata::{ContinuousQueryDef, MaterializedViewDef};
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
    },
    Revoke {
        username: String,
        database: Option<String>,
    },
}

/// Wrapper sent over the wire -- carries the sender's seq for ack tracking.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MutationReplicateRequest {
    pub seq: u64,
    #[serde(default)]
    pub origin_node_id: u64,
    pub mutation: MutationRequest,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MutationReplicateResponse {
    pub ok: bool,
    pub ack_seq: u64,
}
