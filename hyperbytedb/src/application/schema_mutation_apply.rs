//! Apply cluster schema mutations with full local side effects (metadata + chDB DDL).
//!
//! Used by Raft state-machine apply, `/internal/replicate-mutation`, and startup
//! metadata sync so every node converges the same way — separate from async
//! point (WAL) replication.

use std::sync::Arc;

use crate::application::materialized_view_service::MaterializedViewService;
use crate::application::shard_routing::{self, ShardRoutingContext};
use crate::domain::cluster::types::MutationRequest;
use crate::error::HyperbytedbError;
use crate::ports::metadata::MetadataPort;
use crate::ports::points_sink::PointsSinkPort;
use crate::ports::wal::WalPort;

/// Dependencies required to apply schema mutations with full local side effects.
pub struct SchemaMutationDeps<'a> {
    pub metadata: &'a Arc<dyn MetadataPort>,
    pub mv_service: Option<&'a MaterializedViewService>,
    pub points_sink: Option<&'a Arc<dyn PointsSinkPort>>,
    pub wal: Option<&'a Arc<dyn WalPort>>,
    pub shard_routing: Option<&'a Arc<ShardRoutingContext>>,
}

/// Apply a schema mutation locally, including chDB DDL where required.
pub async fn apply_schema_mutation(
    deps: SchemaMutationDeps<'_>,
    mutation: MutationRequest,
) -> Result<(), HyperbytedbError> {
    let op = schema_mutation_op_label(&mutation);
    let result = apply_schema_mutation_inner(deps, mutation).await;
    if result.is_ok() {
        metrics::counter!(
            "hyperbytedb_schema_mutations_applied_total",
            "op" => op,
        )
        .increment(1);
    }
    result
}

fn schema_mutation_op_label(mutation: &MutationRequest) -> &'static str {
    match mutation {
        MutationRequest::CreateDatabase { .. } => "create_database",
        MutationRequest::DropDatabase(_) => "drop_database",
        MutationRequest::CreateRetentionPolicy { .. } => "create_retention_policy",
        MutationRequest::DropRetentionPolicy { .. } => "drop_retention_policy",
        MutationRequest::CreateUser { .. } => "create_user",
        MutationRequest::DropUser(_) => "drop_user",
        MutationRequest::SetPassword { .. } => "set_password",
        MutationRequest::Delete { .. } => "delete",
        MutationRequest::CreateContinuousQuery { .. } => "create_continuous_query",
        MutationRequest::DropContinuousQuery { .. } => "drop_continuous_query",
        MutationRequest::CreateMaterializedView { .. } => "create_materialized_view",
        MutationRequest::DropMaterializedView { .. } => "drop_materialized_view",
        MutationRequest::AlterRetentionPolicy { .. } => "alter_retention_policy",
        MutationRequest::DropSeries { .. } => "drop_series",
        MutationRequest::DropMeasurement { .. } => "drop_measurement",
        MutationRequest::Grant { .. } => "grant",
        MutationRequest::Revoke { .. } => "revoke",
    }
}

async fn apply_schema_mutation_inner(
    deps: SchemaMutationDeps<'_>,
    mutation: MutationRequest,
) -> Result<(), HyperbytedbError> {
    let SchemaMutationDeps {
        metadata,
        mv_service,
        points_sink,
        wal,
        shard_routing,
    } = deps;
    match mutation {
        MutationRequest::CreateDatabase { name, rp } => {
            crate::adapters::cluster::raft::state_machine::apply_create_database(
                metadata, &name, rp,
            )
            .await
        }
        MutationRequest::DropDatabase(name) => {
            let sink = points_sink.or_else(|| mv_service.map(|mv| mv.points_sink()));
            crate::application::database_drop::drop_database(metadata, mv_service, sink, wal, &name)
                .await
        }
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
        } => {
            let admin = match metadata.get_user(&username).await? {
                Some(user) => user.admin,
                None => false,
            };
            metadata.create_user(&username, &password_hash, admin).await
        }
        MutationRequest::Delete {
            database,
            rp,
            measurement,
            predicate_sql,
        } => {
            metadata
                .store_tombstone(&database, &rp, &measurement, &predicate_sql)
                .await?;
            if let Some(ctx) = shard_routing {
                shard_routing::scatter_delete_to_regions(
                    ctx,
                    metadata.as_ref(),
                    &database,
                    &rp,
                    &measurement,
                    &predicate_sql,
                )
                .await?;
            } else {
                metadata
                    .delete_series_matching(&database, &rp, Some(&measurement), &predicate_sql)
                    .await?;
            }
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
            rp,
            measurement,
            predicate_sql,
        } => {
            if !predicate_sql.is_empty() && measurement.is_some() {
                metadata
                    .store_tombstone(
                        &database,
                        &rp,
                        measurement.as_deref().unwrap_or(""),
                        &predicate_sql,
                    )
                    .await?;
            }
            metadata
                .delete_series_matching(&database, &rp, measurement.as_deref(), &predicate_sql)
                .await?;
            Ok(())
        }
        MutationRequest::DropMeasurement { database, rp, name } => {
            metadata.delete_measurement(&database, &rp, &name).await
        }
        MutationRequest::Grant { username, database } => {
            if let Some(db) = &database {
                metadata
                    .grant_privilege(&username, db, crate::domain::user::DatabasePrivilege::All)
                    .await?;
            } else {
                if let Some(user) = metadata.get_user(&username).await? {
                    metadata
                        .create_user(&username, &user.password_hash, true)
                        .await?;
                }
            }
            Ok(())
        }
        MutationRequest::Revoke { username, database } => {
            if let Some(db) = &database {
                metadata.revoke_privilege(&username, db).await?;
            } else {
                if let Some(user) = metadata.get_user(&username).await? {
                    metadata
                        .create_user(&username, &user.password_hash, false)
                        .await?;
                }
            }
            Ok(())
        }
    }
}
