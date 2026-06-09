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

/// Apply a schema mutation to the local metadata store.
pub async fn apply_schema_mutation(
    metadata: &Arc<dyn MetadataPort>,
    mutation: MutationRequest,
) -> Result<(), crate::error::HyperbytedbError> {
    match mutation {
        MutationRequest::CreateDatabase(name) => metadata.create_database(&name).await,
        MutationRequest::DropDatabase(name) => metadata.drop_database(&name).await,
        MutationRequest::CreateRetentionPolicy { db, rp } => {
            metadata.create_retention_policy(&db, rp).await
        }
        MutationRequest::DropRetentionPolicy { db, name } => {
            metadata.drop_retention_policy(&db, &name).await
        }
        MutationRequest::CreateUser {
            username,
            password_hash,
            admin,
        } => metadata.create_user(&username, &password_hash, admin).await,
        MutationRequest::DropUser(username) => metadata.drop_user(&username).await,
        MutationRequest::SetPassword {
            username,
            password_hash,
        } => metadata.create_user(&username, &password_hash, false).await,
        MutationRequest::Delete {
            database,
            measurement,
            predicate_sql,
        } => {
            metadata
                .store_tombstone(&database, &measurement, &predicate_sql)
                .await?;
            Ok(())
        }
        MutationRequest::CreateContinuousQuery {
            database,
            name,
            definition,
        } => {
            metadata
                .store_continuous_query(&database, &name, &definition)
                .await
        }
        MutationRequest::DropContinuousQuery { database, name } => {
            metadata.drop_continuous_query(&database, &name).await
        }
        MutationRequest::CreateMaterializedView {
            database,
            name,
            definition,
        } => {
            metadata
                .store_materialized_view(&database, &name, &definition)
                .await
        }
        MutationRequest::DropMaterializedView { database, name } => {
            metadata.drop_materialized_view(&database, &name).await
        }
    }
}
