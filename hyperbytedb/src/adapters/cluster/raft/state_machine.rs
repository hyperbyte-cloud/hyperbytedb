use std::sync::Arc;

use openraft::{LogId, StoredMembership};

use crate::domain::cluster::membership::ClusterMembership;
use crate::domain::cluster::types::MutationRequest;
use crate::domain::database::RetentionPolicy;
use crate::ports::metadata::MetadataPort;

/// Snapshot-serializable state machine data.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, Default)]
pub struct StateMachineData {
    pub last_applied_log: Option<LogId<u64>>,
    pub last_membership: StoredMembership<u64, openraft::BasicNode>,
    pub cluster_membership: ClusterMembership,
}

/// Apply CREATE DATABASE locally (idempotent, honors WITH retention policy on peers).
pub async fn apply_create_database(
    metadata: &Arc<dyn MetadataPort>,
    name: &str,
    rp: Option<RetentionPolicy>,
) -> Result<(), crate::error::HyperbytedbError> {
    if let Some(rp) = rp {
        let stmt =
            crate::domain::database::create_database_statement_from_rp(name.to_string(), &rp);
        metadata.create_database_with(&stmt).await
    } else {
        metadata.create_database(name).await
    }
}

/// Apply a schema mutation to the local metadata store (metadata-only fallback).
pub async fn apply_schema_mutation(
    metadata: &Arc<dyn MetadataPort>,
    mutation: MutationRequest,
) -> Result<(), crate::error::HyperbytedbError> {
    crate::application::schema_mutation_apply::apply_schema_mutation(metadata, None, mutation).await
}
