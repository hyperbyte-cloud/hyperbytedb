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

/// The shard-map op that must accompany a schema drop, if any.
///
/// Dropping a database, retention policy or measurement must also drop its
/// shard spaces. Without Raft the query service proposes that separately; with
/// Raft the drop arrives here as a `SchemaMutation` and nothing else did it, so
/// on a real cluster the space outlived its data. The scheduler then kept
/// moving regions whose data was gone -- every transfer failed with
/// "measurement not found", every tick, on every node (seen on n3) -- and the
/// next run of the same database started from the old region count.
#[must_use]
pub fn drop_space_for(
    mutation: &MutationRequest,
) -> Option<crate::domain::sharding::ops::ShardMapOp> {
    use crate::domain::sharding::ops::ShardMapOp;
    match mutation {
        MutationRequest::DropDatabase(db) => Some(ShardMapOp::DropSpace {
            db: db.clone(),
            rp: None,
            measurement: None,
        }),
        MutationRequest::DropRetentionPolicy { db, name } => Some(ShardMapOp::DropSpace {
            db: db.clone(),
            rp: Some(name.clone()),
            measurement: None,
        }),
        MutationRequest::DropMeasurement { database, rp, name } => Some(ShardMapOp::DropSpace {
            db: database.clone(),
            rp: Some(rp.clone()),
            measurement: Some(name.clone()),
        }),
        _ => None,
    }
}

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

pub fn schema_mutation_op_label(mutation: &MutationRequest) -> &'static str {
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
        MutationRequest::SetSetting { .. } => "set_setting",
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
        } => create_user(metadata.as_ref(), &username, &password_hash, admin).await,
        MutationRequest::DropUser(username) => metadata.drop_user(&username).await,
        MutationRequest::SetPassword {
            username,
            password_hash,
        } => set_password(metadata.as_ref(), &username, &password_hash).await,
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
            source_meta,
        } => {
            if let Some(mv) = mv_service {
                mv.apply_replicated_definition(&definition, source_meta.as_deref())
                    .await
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
        MutationRequest::Grant {
            username,
            database,
            privilege,
        } => {
            if let Some(db) = &database {
                metadata.grant_privilege(&username, db, privilege).await?;
            } else {
                set_admin(metadata.as_ref(), &username, true).await?;
            }
            Ok(())
        }
        MutationRequest::SetSetting { name, value } => {
            crate::application::runtime_settings::apply(metadata.as_ref(), &name, value).await
        }
        MutationRequest::Revoke {
            username,
            database,
            privilege,
        } => {
            if let Some(db) = &database {
                revoke_privilege(metadata.as_ref(), &username, db, privilege).await?;
            } else {
                set_admin(metadata.as_ref(), &username, false).await?;
            }
            Ok(())
        }
    }
}

/// `CREATE USER`. On an existing user (a re-run, or a replayed entry) the
/// password and admin flag are set and its grants kept.
pub async fn create_user(
    metadata: &dyn crate::ports::metadata::MetadataPort,
    username: &str,
    password_hash: &str,
    admin: bool,
) -> Result<(), HyperbytedbError> {
    match metadata.get_user(username).await? {
        Some(mut user) => {
            user.password_hash = password_hash.to_string();
            user.admin = admin;
            metadata.put_user(username, &user).await
        }
        None => metadata.create_user(username, password_hash, admin).await,
    }
}

/// `SET PASSWORD`: a new hash, privileges and admin flag kept. A user that
/// doesn't exist is created (a replayed entry may arrive first).
pub async fn set_password(
    metadata: &dyn crate::ports::metadata::MetadataPort,
    username: &str,
    password_hash: &str,
) -> Result<(), HyperbytedbError> {
    match metadata.get_user(username).await? {
        Some(mut user) => {
            user.password_hash = password_hash.to_string();
            metadata.put_user(username, &user).await
        }
        None => metadata.create_user(username, password_hash, false).await,
    }
}

/// `GRANT` / `REVOKE ALL PRIVILEGES` without `ON`: the admin flag only,
/// per-database grants kept. No-op for a user that doesn't exist.
pub async fn set_admin(
    metadata: &dyn crate::ports::metadata::MetadataPort,
    username: &str,
    admin: bool,
) -> Result<(), HyperbytedbError> {
    if let Some(mut user) = metadata.get_user(username).await? {
        user.admin = admin;
        metadata.put_user(username, &user).await?;
    }
    Ok(())
}

/// `REVOKE <privilege> ON <db>`: clear just that privilege, so revoking
/// READ from a user with ALL leaves WRITE.
pub async fn revoke_privilege(
    metadata: &dyn crate::ports::metadata::MetadataPort,
    username: &str,
    db: &str,
    revoked: crate::domain::user::DatabasePrivilege,
) -> Result<(), HyperbytedbError> {
    let held = metadata
        .get_user(username)
        .await?
        .and_then(|u| u.privileges.get(db).copied());
    match held.and_then(|p| p.without(revoked)) {
        Some(left) => metadata.grant_privilege(username, db, left).await,
        None => metadata.revoke_privilege(username, db).await,
    }
}

#[cfg(test)]
mod drop_space_tests {
    use super::drop_space_for;
    use crate::domain::cluster::types::MutationRequest;
    use crate::domain::sharding::ops::ShardMapOp;

    #[test]
    fn each_schema_drop_carries_its_space_drop() {
        assert_eq!(
            drop_space_for(&MutationRequest::DropDatabase("linearity".into())),
            Some(ShardMapOp::DropSpace {
                db: "linearity".into(),
                rp: None,
                measurement: None,
            })
        );
        assert_eq!(
            drop_space_for(&MutationRequest::DropRetentionPolicy {
                db: "d".into(),
                name: "r".into(),
            }),
            Some(ShardMapOp::DropSpace {
                db: "d".into(),
                rp: Some("r".into()),
                measurement: None,
            })
        );
        assert_eq!(
            drop_space_for(&MutationRequest::DropMeasurement {
                database: "d".into(),
                rp: "r".into(),
                name: "cpu".into(),
            }),
            Some(ShardMapOp::DropSpace {
                db: "d".into(),
                rp: Some("r".into()),
                measurement: Some("cpu".into()),
            })
        );
        assert_eq!(drop_space_for(&MutationRequest::DropUser("u".into())), None);
    }
}
