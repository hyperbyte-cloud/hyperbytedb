//! SHOW statements over the catalog: databases, retention policies, users, grants, continuous queries, materialized views and settings.

use super::super::*;

pub(in crate::application::query_service) async fn show_continuous_queries(
    svc: &QueryServiceImpl,
    statement_id: u32,
) -> Result<StatementResult, HyperbytedbError> {
    let dbs = svc.metadata.list_databases().await?;
    let mut all_series = Vec::new();
    for db_entry in &dbs {
        let cqs = svc.metadata.list_continuous_queries(&db_entry.name).await?;
        if cqs.is_empty() {
            continue;
        }
        let columns = vec!["name".to_string(), "query".to_string()];
        let values: Vec<Vec<serde_json::Value>> = cqs
            .iter()
            .map(|cq| {
                vec![
                    serde_json::Value::String(cq.name.clone()),
                    serde_json::Value::String(reconstruct_cq_text(cq)),
                ]
            })
            .collect();
        all_series.push(SeriesResult {
            name: db_entry.name.clone(),
            tags: None,
            columns,
            values,
            partial: None,
        });
    }

    Ok(StatementResult {
        statement_id,
        series: Some(all_series),
        error: None,
    })
}

pub(in crate::application::query_service) async fn show_materialized_views(
    svc: &QueryServiceImpl,
    statement_id: u32,
) -> Result<StatementResult, HyperbytedbError> {
    let dbs = svc.metadata.list_databases().await?;
    let mut all_mvs = Vec::new();
    for db_entry in &dbs {
        let mvs = svc.metadata.list_materialized_views(&db_entry.name).await?;
        all_mvs.extend(mvs);
    }

    let columns = vec![
        "name".to_string(),
        "database".to_string(),
        "query".to_string(),
        "source_measurement".to_string(),
        "dest_measurement".to_string(),
    ];
    let values: Vec<Vec<serde_json::Value>> = all_mvs
        .iter()
        .map(|mv| {
            vec![
                serde_json::Value::String(mv.name.clone()),
                serde_json::Value::String(mv.database.clone()),
                serde_json::Value::String(mv.query_text.clone()),
                serde_json::Value::String(mv.source_measurement.clone()),
                serde_json::Value::String(mv.dest_measurement.clone()),
            ]
        })
        .collect();

    Ok(StatementResult {
        statement_id,
        series: Some(vec![SeriesResult {
            name: "materialized_views".to_string(),
            tags: None,
            columns,
            values,
            partial: None,
        }]),
        error: None,
    })
}

pub(in crate::application::query_service) async fn show_settings(
    svc: &QueryServiceImpl,
    statement_id: u32,
) -> Result<StatementResult, HyperbytedbError> {
    let rows = svc.settings()?.show(svc.metadata.as_ref()).await?;
    Ok(StatementResult {
        statement_id,
        series: Some(vec![SeriesResult {
            name: "settings".to_string(),
            tags: None,
            columns: vec![
                "name".into(),
                "value".into(),
                "source".into(),
                "config".into(),
                "description".into(),
            ],
            values: rows
                .into_iter()
                .map(|r| {
                    vec![
                        serde_json::Value::String(r.name.to_string()),
                        r.value
                            .as_ref()
                            .map_or(serde_json::Value::Null, |v| v.to_json()),
                        r.source.map_or(serde_json::Value::Null, |s| {
                            serde_json::Value::String(s.as_str().to_string())
                        }),
                        serde_json::Value::String(r.config.to_string()),
                        serde_json::Value::String(r.description.to_string()),
                    ]
                })
                .collect(),
            partial: None,
        }]),
        error: None,
    })
}

pub(in crate::application::query_service) async fn show_retention_policies(
    svc: &QueryServiceImpl,
    db: &str,
    rp_db: &str,
    statement_id: u32,
) -> Result<StatementResult, HyperbytedbError> {
    let target_db = if rp_db.is_empty() { db } else { rp_db };
    if !rp_db.is_empty() {
        svc.metadata
            .get_database(target_db)
            .await?
            .ok_or_else(|| HyperbytedbError::DatabaseNotFound(target_db.to_string()))?;
    }
    let rps = svc.metadata.list_retention_policies(target_db).await?;
    let columns = vec![
        "name".to_string(),
        "duration".to_string(),
        "shardGroupDuration".to_string(),
        "replicaN".to_string(),
        "default".to_string(),
    ];
    let values: Vec<Vec<serde_json::Value>> = rps
        .iter()
        .map(|rp| {
            let dur_str = crate::domain::database::format_influx_duration(rp.duration);
            let sgd_str =
                crate::domain::database::format_influx_duration(Some(rp.shard_group_duration));
            vec![
                serde_json::Value::String(rp.name.clone()),
                serde_json::Value::String(dur_str),
                serde_json::Value::String(sgd_str),
                serde_json::json!(rp.replication_factor),
                serde_json::Value::Bool(rp.is_default),
            ]
        })
        .collect();
    Ok(StatementResult {
        statement_id,
        series: Some(vec![SeriesResult {
            name: "".to_string(),
            tags: None,
            columns,
            values,
            partial: None,
        }]),
        error: None,
    })
}

pub(in crate::application::query_service) async fn show_grants(
    svc: &QueryServiceImpl,
    username: &str,
    statement_id: u32,
) -> Result<StatementResult, HyperbytedbError> {
    let user =
        svc.metadata.get_user(username).await?.ok_or_else(|| {
            HyperbytedbError::QueryParse(format!("user '{username}' does not exist"))
        })?;
    let mut grants: Vec<_> = user.privileges.into_iter().collect();
    grants.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(StatementResult {
        statement_id,
        series: Some(vec![SeriesResult {
            name: String::new(),
            tags: None,
            columns: vec!["database".to_string(), "privilege".to_string()],
            values: grants
                .into_iter()
                .map(|(db, p)| {
                    vec![
                        serde_json::Value::String(db),
                        serde_json::Value::String(p.as_str().to_string()),
                    ]
                })
                .collect(),
            partial: None,
        }]),
        error: None,
    })
}

pub(in crate::application::query_service) async fn show_users(
    svc: &QueryServiceImpl,
    statement_id: u32,
) -> Result<StatementResult, HyperbytedbError> {
    let users = svc.metadata.list_users().await?;
    let columns = vec!["user".to_string(), "admin".to_string()];
    let mut values = Vec::new();
    for u in &users {
        if let Ok(Some(stored)) = svc.metadata.get_user(u).await {
            values.push(vec![
                serde_json::Value::String(u.clone()),
                serde_json::Value::Bool(stored.admin),
            ]);
        }
    }
    Ok(StatementResult {
        statement_id,
        series: Some(vec![SeriesResult {
            name: "users".to_string(),
            tags: None,
            columns,
            values,
            partial: None,
        }]),
        error: None,
    })
}

pub(in crate::application::query_service) async fn show_databases(
    svc: &QueryServiceImpl,
    statement_id: u32,
) -> Result<StatementResult, HyperbytedbError> {
    let dbs = svc.metadata.list_databases().await?;
    let columns = vec!["name".to_string()];
    let values: Vec<Vec<serde_json::Value>> = dbs
        .iter()
        .map(|d| vec![serde_json::Value::String(d.name.clone())])
        .collect();
    Ok(StatementResult {
        statement_id,
        series: Some(vec![SeriesResult {
            name: "databases".to_string(),
            tags: None,
            columns,
            values,
            partial: None,
        }]),
        error: None,
    })
}
