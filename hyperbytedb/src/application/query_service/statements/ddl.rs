//! Create, drop and alter of measurements, retention policies and continuous queries.

use super::super::*;

pub(in crate::application::query_service) async fn drop_measurement(
    svc: &QueryServiceImpl,
    db: &str,
    name: &str,
    stmt_rp: &Option<String>,
    statement_id: u32,
) -> Result<StatementResult, HyperbytedbError> {
    if let Err(e) = svc.mv_service.drop_for_source_measurement(db, name).await {
        tracing::warn!(
            db = db,
            measurement = name,
            error = %e,
            "failed to cascade-drop materialized views for source measurement"
        );
    }
    let rp = if let Some(inner) = stmt_rp {
        inner.clone()
    } else {
        svc.metadata.get_default_rp(db).await?
    };
    svc.metadata.delete_measurement(db, &rp, name).await?;
    if let Some(ctx) = svc.shard_routing.as_ref() {
        scatter_delete_to_regions(ctx, svc.metadata.as_ref(), None, db, &rp, name, "").await?;
    }
    if let Err(e) = svc.points_sink.drop_measurement(db, &rp, name).await {
        tracing::warn!(
            db = db,
            measurement = name,
            error = %e,
            "failed to drop chDB native table for measurement (metadata already cleared)"
        );
    }
    svc.drop_shard_spaces(db, Some(&rp), Some(name)).await;
    Ok(StatementResult {
        statement_id,
        series: Some(vec![]),
        error: None,
    })
}

pub(in crate::application::query_service) async fn create_continuous_query(
    svc: &QueryServiceImpl,
    cq: &crate::timeseriesql::ast::CreateContinuousQueryStatement,
    statement_id: u32,
) -> Result<StatementResult, HyperbytedbError> {
    svc.metadata
        .get_database(&cq.database)
        .await?
        .ok_or_else(|| HyperbytedbError::DatabaseNotFound(cq.database.clone()))?;

    let def = match ContinuousQueryDef::from_create(cq) {
        Ok(def) => def,
        Err(e) => {
            return Ok(StatementResult {
                statement_id,
                series: None,
                error: Some(e.to_string()),
            });
        }
    };

    svc.metadata
        .store_continuous_query(&def.database, &def.name, &def)
        .await?;

    Ok(StatementResult {
        statement_id,
        series: Some(vec![]),
        error: None,
    })
}

// `&String` and one parameter per field: the body is moved verbatim from the
// match arm.
#[allow(clippy::ptr_arg, clippy::too_many_arguments)]
pub(in crate::application::query_service) async fn create_retention_policy(
    svc: &QueryServiceImpl,
    db: &str,
    name: &String,
    rp_db: &String,
    duration: &Option<crate::timeseriesql::ast::Duration>,
    replication: &u32,
    shard_duration: &Option<crate::timeseriesql::ast::Duration>,
    is_default: &bool,
    statement_id: u32,
) -> Result<StatementResult, HyperbytedbError> {
    let target_db = if rp_db.is_empty() { db } else { rp_db.as_str() };
    let std_duration = duration
        .as_ref()
        .map(|d| std::time::Duration::from_nanos(d.to_nanos() as u64));
    let shard_dur = shard_duration
        .as_ref()
        .map(|d| std::time::Duration::from_nanos(d.to_nanos() as u64))
        .unwrap_or_else(|| crate::domain::database::derive_shard_group_duration(std_duration));
    let rp = crate::domain::database::RetentionPolicy {
        name: name.clone(),
        duration: std_duration,
        shard_group_duration: shard_dur,
        replication_factor: *replication,
        is_default: *is_default,
    };
    svc.metadata.create_retention_policy(target_db, rp).await?;
    Ok(StatementResult {
        statement_id,
        series: Some(vec![]),
        error: None,
    })
}

// `&String` and one parameter per field: the body is moved verbatim from the
// match arm.
#[allow(clippy::ptr_arg, clippy::too_many_arguments)]
pub(in crate::application::query_service) async fn alter_retention_policy(
    svc: &QueryServiceImpl,
    db: &str,
    name: &String,
    rp_db: &String,
    duration: &Option<crate::timeseriesql::ast::Duration>,
    replication: &Option<u32>,
    shard_duration: &Option<crate::timeseriesql::ast::Duration>,
    is_default: &Option<bool>,
    statement_id: u32,
) -> Result<StatementResult, HyperbytedbError> {
    let target_db = if rp_db.is_empty() { db } else { rp_db.as_str() };
    let change = RetentionPolicyChange {
        duration: duration.as_ref().map(|d| {
            if d.value == 0 && d.unit == DurationUnit::Second {
                None
            } else {
                Some(d.clone())
            }
        }),
        replication: *replication,
        shard_duration: shard_duration.clone(),
        is_default: *is_default,
    };
    svc.metadata
        .alter_retention_policy(target_db, name, &change)
        .await?;
    Ok(StatementResult {
        statement_id,
        series: Some(vec![]),
        error: None,
    })
}

pub(in crate::application::query_service) async fn drop_database(
    svc: &QueryServiceImpl,
    name: &str,
    statement_id: u32,
) -> Result<StatementResult, HyperbytedbError> {
    crate::application::database_drop::drop_database(
        &svc.metadata,
        Some(&svc.mv_service),
        Some(&svc.points_sink),
        Some(&svc.wal),
        name,
    )
    .await?;
    svc.drop_shard_spaces(name, None, None).await;
    Ok(StatementResult {
        statement_id,
        series: Some(vec![]),
        error: None,
    })
}

pub(in crate::application::query_service) async fn drop_continuous_query(
    svc: &QueryServiceImpl,
    db: &str,
    name: &str,
    cq_db: &str,
    statement_id: u32,
) -> Result<StatementResult, HyperbytedbError> {
    let target_db = if cq_db.is_empty() { db } else { cq_db };
    svc.metadata
        .get_database(target_db)
        .await?
        .ok_or_else(|| HyperbytedbError::DatabaseNotFound(target_db.to_string()))?;
    svc.metadata.drop_continuous_query(target_db, name).await?;
    Ok(StatementResult {
        statement_id,
        series: Some(vec![]),
        error: None,
    })
}

pub(in crate::application::query_service) async fn create_materialized_view(
    svc: &QueryServiceImpl,
    mv: &crate::timeseriesql::ast::CreateMaterializedViewStatement,
    statement_id: u32,
) -> Result<StatementResult, HyperbytedbError> {
    svc.metadata
        .get_database(&mv.database)
        .await?
        .ok_or_else(|| HyperbytedbError::DatabaseNotFound(mv.database.clone()))?;
    svc.mv_service.create(mv).await?;
    Ok(StatementResult {
        statement_id,
        series: Some(vec![]),
        error: None,
    })
}

pub(in crate::application::query_service) async fn set_setting(
    svc: &QueryServiceImpl,
    name: &str,
    value: &Option<crate::domain::setting::SettingValue>,
    statement_id: u32,
) -> Result<StatementResult, HyperbytedbError> {
    // A single node; a cluster replicates the SET through Raft and
    // every node applies it the same way.
    let value = crate::application::runtime_settings::validate(name, value.clone())?;
    svc.settings()?
        .apply(svc.metadata.as_ref(), name, value)
        .await?;
    Ok(StatementResult {
        statement_id,
        series: Some(vec![]),
        error: None,
    })
}

pub(in crate::application::query_service) async fn drop_materialized_view(
    svc: &QueryServiceImpl,
    db: &str,
    name: &str,
    mv_db: &str,
    statement_id: u32,
) -> Result<StatementResult, HyperbytedbError> {
    let target_db = if mv_db.is_empty() { db } else { mv_db };
    svc.metadata
        .get_database(target_db)
        .await?
        .ok_or_else(|| HyperbytedbError::DatabaseNotFound(target_db.to_string()))?;
    svc.mv_service.drop_mv(target_db, name).await?;
    Ok(StatementResult {
        statement_id,
        series: Some(vec![]),
        error: None,
    })
}

#[allow(clippy::ptr_arg)] // `&String`: the body is moved verbatim from the match arm.
pub(in crate::application::query_service) async fn drop_retention_policy(
    svc: &QueryServiceImpl,
    db: &str,
    name: &str,
    rp_db: &String,
    statement_id: u32,
) -> Result<StatementResult, HyperbytedbError> {
    let target_db = if rp_db.is_empty() { db } else { rp_db.as_str() };
    svc.metadata.drop_retention_policy(target_db, name).await?;
    svc.drop_shard_spaces(target_db, Some(name), None).await;
    Ok(StatementResult {
        statement_id,
        series: Some(vec![]),
        error: None,
    })
}
