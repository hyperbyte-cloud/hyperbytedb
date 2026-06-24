//! Apply cluster schema mutations with full local side effects (metadata + chDB DDL).
//!
//! Used by Raft state-machine apply, `/internal/replicate-mutation`, and startup
//! metadata sync so every node converges the same way — separate from async
//! point (WAL) replication.

use std::sync::Arc;

use crate::application::materialized_view_service::MaterializedViewService;
use crate::domain::cluster::types::MutationRequest;
use crate::error::HyperbytedbError;
use crate::ports::metadata::MetadataPort;

/// Apply a schema mutation locally, including chDB DDL where required.
pub async fn apply_schema_mutation(
    metadata: &Arc<dyn MetadataPort>,
    mv_service: Option<&MaterializedViewService>,
    mutation: MutationRequest,
) -> Result<(), HyperbytedbError> {
    match mutation {
        MutationRequest::CreateDatabase { name, rp } => {
            crate::adapters::cluster::raft::state_machine::apply_create_database(
                metadata, &name, rp,
            )
            .await
        }
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
            if let Some(mv) = mv_service {
                mv.apply_replicated_definition(&definition).await
            } else {
                metadata
                    .store_materialized_view(&database, &name, &definition)
                    .await
            }
        }
        MutationRequest::DropMaterializedView { database, name } => {
            if let Some(mv) = mv_service {
                mv.apply_replicated_drop(&database, &name).await
            } else {
                metadata.drop_materialized_view(&database, &name).await
            }
        }
        MutationRequest::AlterRetentionPolicy { db, name, change } => {
            metadata.alter_retention_policy(&db, &name, &change).await
        }
        MutationRequest::DropSeries {
            database,
            measurement,
            predicate_sql,
        } => {
            let rp = metadata.get_default_rp(&database).await?;
            metadata
                .delete_series_matching(&database, &rp, measurement.as_deref(), &predicate_sql)
                .await?;
            Ok(())
        }
    }
}
