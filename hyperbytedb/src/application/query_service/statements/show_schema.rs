//! SHOW statements over a database's measurement schema.

use super::super::*;

pub(in crate::application::query_service) async fn show_measurements(
    svc: &QueryServiceImpl,
    db: &str,
    s: &crate::timeseriesql::ast::ShowMeasurementsStatement,
    query_rp: Option<&str>,
    statement_id: u32,
) -> Result<StatementResult, HyperbytedbError> {
    let db = s.database.as_deref().unwrap_or(db);
    if db.is_empty() {
        return Ok(StatementResult {
            statement_id,
            series: Some(vec![]),
            error: Some("database is required".to_string()),
        });
    }
    let rp = resolve_retention_policy_for_select(svc.metadata.as_ref(), db, None, query_rp).await?;
    let names = list_measurements_for_rp(svc, db, &rp).await?;
    let filtered = filter_names_by_selector(&names, s.measurement_filter.as_ref());
    let columns = vec!["name".to_string()];
    let values: Vec<Vec<serde_json::Value>> = filtered
        .iter()
        .map(|n| vec![serde_json::Value::String(n.clone())])
        .collect();
    Ok(StatementResult {
        statement_id,
        series: Some(vec![SeriesResult {
            name: "measurements".to_string(),
            tags: None,
            columns,
            values,
            partial: None,
        }]),
        error: None,
    })
}

pub(in crate::application::query_service) async fn show_tag_keys(
    svc: &QueryServiceImpl,
    db: &str,
    s: &crate::timeseriesql::ast::ShowTagKeysStatement,
    query_rp: Option<&str>,
    statement_id: u32,
) -> Result<StatementResult, HyperbytedbError> {
    let db = s.database.as_deref().unwrap_or(db);
    let measurement = s.from.as_ref().and_then(|m| m.name_str());
    let default_rp =
        resolve_retention_policy_for_select(svc.metadata.as_ref(), db, None, query_rp).await?;
    let keys = if let (Some(ctx), Some(m), Some(name)) = (
        svc.shard_routing.as_ref(),
        s.from.as_ref(),
        s.from.as_ref().and_then(|m| m.name_str()),
    ) {
        let query_db = m.database.as_deref().unwrap_or(db);
        let rp = if m.retention_policy.is_some() {
            resolve_retention_policy(
                svc.metadata.as_ref(),
                query_db,
                m.retention_policy.as_deref(),
            )
            .await?
        } else {
            default_rp.clone()
        };
        scatter_tag_keys(ctx, svc.metadata.as_ref(), query_db, &rp, name).await?
    } else if let Some(m) = s.from.as_ref() {
        if let Some(name) = m.name_str() {
            if m.retention_policy.is_some() {
                let query_db = m.database.as_deref().unwrap_or(db);
                let rp = resolve_retention_policy(
                    svc.metadata.as_ref(),
                    query_db,
                    m.retention_policy.as_deref(),
                )
                .await?;
                tag_keys_for_measurement(svc, query_db, &rp, name).await?
            } else {
                svc.metadata
                    .list_tag_keys(db, &default_rp, Some(name))
                    .await?
            }
        } else {
            svc.metadata
                .list_tag_keys(db, &default_rp, measurement)
                .await?
        }
    } else {
        svc.metadata
            .list_tag_keys(db, &default_rp, measurement)
            .await?
    };
    let columns = vec!["tagKey".to_string()];
    let values: Vec<Vec<serde_json::Value>> = keys
        .iter()
        .map(|k| vec![serde_json::Value::String(k.clone())])
        .collect();
    Ok(StatementResult {
        statement_id,
        series: Some(vec![SeriesResult {
            name: measurement.unwrap_or("").to_string(),
            tags: None,
            columns,
            values,
            partial: None,
        }]),
        error: None,
    })
}

pub(in crate::application::query_service) async fn show_tag_values(
    svc: &QueryServiceImpl,
    db: &str,
    s: &crate::timeseriesql::ast::ShowTagValuesStatement,
    query_rp: Option<&str>,
    statement_id: u32,
) -> Result<StatementResult, HyperbytedbError> {
    let db = s.database.as_deref().unwrap_or(db);
    let measurement = s.from.as_ref().and_then(|m| m.name_str());
    let default_rp =
        resolve_retention_policy_for_select(svc.metadata.as_ref(), db, None, query_rp).await?;

    let all_tag_keys = if let (Some(ctx), Some(m), Some(name)) =
        (svc.shard_routing.as_ref(), s.from.as_ref(), measurement)
    {
        let query_db = m.database.as_deref().unwrap_or(db);
        let rp = if m.retention_policy.is_some() {
            resolve_retention_policy(
                svc.metadata.as_ref(),
                query_db,
                m.retention_policy.as_deref(),
            )
            .await?
        } else {
            default_rp.clone()
        };
        scatter_tag_keys(ctx, svc.metadata.as_ref(), query_db, &rp, name).await?
    } else if let (Some(m), Some(name)) = (s.from.as_ref(), measurement) {
        if m.retention_policy.is_some() {
            let query_db = m.database.as_deref().unwrap_or(db);
            let rp = resolve_retention_policy(
                svc.metadata.as_ref(),
                query_db,
                m.retention_policy.as_deref(),
            )
            .await?;
            tag_keys_for_measurement(svc, query_db, &rp, name).await?
        } else {
            svc.metadata
                .list_tag_keys(db, &default_rp, Some(name))
                .await?
        }
    } else {
        svc.metadata
            .list_tag_keys(db, &default_rp, measurement)
            .await?
    };

    let matching_keys: Vec<String> = filter_names_by_selector(&all_tag_keys, Some(&s.tag_key));

    // With a WHERE: the values of the series that match it. It used
    // to be ignored, so a Grafana variable chained on another (the
    // CPUs of the chosen host) listed every value.
    if let Some(cond) = &s.condition {
        let predicate = crate::application::tag_condition::TagPredicate::compile(cond)?;
        let measurements: Vec<String> = match measurement {
            Some(name) => vec![name.to_string()],
            None => svc.metadata.list_measurements(db).await?,
        };
        let (query_db, rp) = match s.from.as_ref() {
            Some(m) if m.retention_policy.is_some() => {
                let query_db = m.database.as_deref().unwrap_or(db);
                let rp = resolve_retention_policy(
                    svc.metadata.as_ref(),
                    query_db,
                    m.retention_policy.as_deref(),
                )
                .await?;
                (query_db, rp)
            }
            Some(m) => (m.database.as_deref().unwrap_or(db), default_rp.clone()),
            None => (db, default_rp.clone()),
        };
        let mut pairs = std::collections::BTreeSet::new();
        for name in &measurements {
            let series = match svc.shard_routing.as_ref() {
                Some(ctx) => {
                    crate::application::shard_metadata_scatter::scatter_series_tags(
                        ctx,
                        svc.metadata.as_ref(),
                        query_db,
                        &rp,
                        name,
                    )
                    .await?
                }
                None => svc
                    .metadata
                    .list_series(query_db, &rp, name)
                    .await?
                    .into_iter()
                    .map(|(_, tags)| tags)
                    .collect(),
            };
            for tags in series.iter().filter(|t| predicate.matches(t)) {
                for key in &matching_keys {
                    if let Some(v) = tags.get(key) {
                        pairs.insert((key.clone(), v.clone()));
                    }
                }
            }
        }
        let values = limit_offset(
            pairs
                .into_iter()
                .map(|(k, v)| vec![serde_json::Value::String(k), serde_json::Value::String(v)])
                .collect(),
            s.limit,
            s.offset,
        );
        return Ok(StatementResult {
            statement_id,
            series: Some(vec![SeriesResult {
                name: measurement.unwrap_or("").to_string(),
                tags: None,
                columns: vec!["key".to_string(), "value".to_string()],
                values,
                partial: None,
            }]),
            error: None,
        });
    }
    let default_rp_for_values = default_rp.clone();

    let key_value_futures: Vec<_> = matching_keys
        .iter()
        .map(|tag_key| {
            let tag_key = tag_key.clone();
            let default_rp = default_rp_for_values.clone();
            async move {
                let values_list = if let (Some(ctx), Some(m), Some(name)) =
                    (svc.shard_routing.as_ref(), s.from.as_ref(), measurement)
                {
                    let query_db = m.database.as_deref().unwrap_or(db);
                    let rp = if m.retention_policy.is_some() {
                        resolve_retention_policy(
                            svc.metadata.as_ref(),
                            query_db,
                            m.retention_policy.as_deref(),
                        )
                        .await?
                    } else {
                        default_rp.clone()
                    };
                    scatter_tag_values(ctx, svc.metadata.as_ref(), query_db, &rp, name, &tag_key)
                        .await?
                } else if let (Some(m), Some(name)) = (s.from.as_ref(), measurement) {
                    if m.retention_policy.is_some() {
                        let query_db = m.database.as_deref().unwrap_or(db);
                        let rp = resolve_retention_policy(
                            svc.metadata.as_ref(),
                            query_db,
                            m.retention_policy.as_deref(),
                        )
                        .await?;
                        tag_values_for_measurement(svc, query_db, &rp, name, &tag_key).await?
                    } else {
                        svc.metadata
                            .list_tag_values(db, &default_rp, &tag_key, Some(name))
                            .await?
                    }
                } else {
                    svc.metadata
                        .list_tag_values(db, &default_rp, &tag_key, measurement)
                        .await?
                };
                Ok::<_, HyperbytedbError>((tag_key, values_list))
            }
        })
        .collect();

    let mut all_values = Vec::new();
    for (tag_key, values_list) in futures::future::try_join_all(key_value_futures).await? {
        for v in values_list {
            all_values.push(vec![
                serde_json::Value::String(tag_key.clone()),
                serde_json::Value::String(v),
            ]);
        }
    }

    let columns = vec!["key".to_string(), "value".to_string()];
    Ok(StatementResult {
        statement_id,
        series: Some(vec![SeriesResult {
            name: measurement.unwrap_or("").to_string(),
            tags: None,
            columns,
            values: limit_offset(all_values, s.limit, s.offset),
            partial: None,
        }]),
        error: None,
    })
}

pub(in crate::application::query_service) async fn show_field_keys(
    svc: &QueryServiceImpl,
    db: &str,
    s: &crate::timeseriesql::ast::ShowFieldKeysStatement,
    query_rp: Option<&str>,
    statement_id: u32,
) -> Result<StatementResult, HyperbytedbError> {
    let db = s.database.as_deref().unwrap_or(db);
    let measurement = s.from.as_ref().and_then(|m| m.name_str());
    if db.is_empty() {
        return Ok(StatementResult {
            statement_id,
            series: Some(vec![]),
            error: Some("database is required".to_string()),
        });
    }
    let rp = resolve_retention_policy_for_select(svc.metadata.as_ref(), db, None, query_rp).await?;
    let mut field_values = Vec::new();
    let measurements: Vec<String> = if let Some(m) = measurement {
        vec![m.to_string()]
    } else {
        svc.metadata.list_measurements(db).await?
    };
    for m in &measurements {
        let fields = match svc.shard_routing.as_ref() {
            Some(ctx) => {
                crate::application::shard_metadata_scatter::scatter_field_keys(
                    ctx,
                    svc.metadata.as_ref(),
                    db,
                    &rp,
                    m,
                )
                .await?
            }
            None => crate::application::shard_metadata_scatter::local_field_keys(
                svc.metadata.get_measurement(db, &rp, m).await?.as_ref(),
            ),
        };
        for (name, typ) in fields {
            field_values.push(vec![
                serde_json::Value::String(name),
                serde_json::Value::String(typ),
            ]);
        }
    }
    let columns = vec!["fieldKey".to_string(), "fieldType".to_string()];
    let values: Vec<Vec<serde_json::Value>> = field_values;
    Ok(StatementResult {
        statement_id,
        series: Some(vec![SeriesResult {
            name: measurement.unwrap_or("").to_string(),
            tags: None,
            columns,
            values,
            partial: None,
        }]),
        error: None,
    })
}

pub(in crate::application::query_service) async fn show_series(
    svc: &QueryServiceImpl,
    db: &str,
    s: &crate::timeseriesql::ast::ShowSeriesStatement,
    query_rp: Option<&str>,
    statement_id: u32,
) -> Result<StatementResult, HyperbytedbError> {
    use crate::domain::series::SeriesKey;

    let query_db = s.database.as_deref().unwrap_or(db);
    if query_db.is_empty() {
        return Ok(StatementResult {
            statement_id,
            series: Some(vec![]),
            error: Some("database is required".to_string()),
        });
    }

    let measurements: Vec<String> = if let Some(ref from) = s.from {
        if let Some(name) = from.name_str() {
            vec![name.to_string()]
        } else {
            svc.metadata.list_measurements(query_db).await?
        }
    } else {
        svc.metadata.list_measurements(query_db).await?
    };

    let rp = if let Some(ref from) = s.from
        && let Some(rp_name) = from.retention_policy.as_ref()
    {
        resolve_retention_policy(svc.metadata.as_ref(), query_db, Some(rp_name)).await?
    } else {
        resolve_retention_policy_for_select(svc.metadata.as_ref(), query_db, None, query_rp).await?
    };

    let mut values = Vec::new();
    for meas in &measurements {
        if let Some(ctx) = svc.shard_routing.as_ref() {
            for key in scatter_series_keys(ctx, svc.metadata.as_ref(), query_db, &rp, meas).await? {
                values.push(vec![serde_json::Value::String(key)]);
            }
        } else {
            let series = svc.metadata.list_series(query_db, &rp, meas).await?;
            for (_, tags) in series {
                let key = SeriesKey::new(meas, &tags);
                values.push(vec![serde_json::Value::String(key.to_canonical())]);
            }
        }
    }

    Ok(StatementResult {
        statement_id,
        series: Some(vec![SeriesResult {
            name: String::new(),
            tags: None,
            columns: vec!["key".to_string()],
            values,
            partial: None,
        }]),
        error: None,
    })
}
