use std::sync::Arc;

use openraft::{LogId, StoredMembership};

use crate::domain::cluster::membership::ClusterMembership;
use crate::domain::cluster::types::MutationRequest;
use crate::ports::metadata::MetadataPort;

/// Snapshot-serializable state machine data.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, Default)]
pub struct StateMachineData {
    pub last_applied_log: Option<LogId<u64>>,
    pub last_membership: StoredMembership<u64, openraft::BasicNode>,
    pub cluster_membership: ClusterMembership,
}

/// Apply a schema mutation to the local metadata store (metadata-only fallback).
pub async fn apply_schema_mutation(
    metadata: &Arc<dyn MetadataPort>,
    mutation: MutationRequest,
) -> Result<(), crate::error::HyperbytedbError> {
    crate::application::schema_mutation_apply::apply_schema_mutation(
        crate::application::schema_mutation_apply::SchemaMutationDeps {
            metadata,
            mv_service: None,
            points_sink: None,
            wal: None,
            shard_routing: None,
            runtime_settings: None,
        },
        mutation,
    )
    .await
}
