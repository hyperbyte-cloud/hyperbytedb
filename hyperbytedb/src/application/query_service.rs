use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

use parking_lot::RwLock;
use regex::Regex;

use crate::application::line_protocol::encode_points_to_line_protocol;
use crate::application::replication_dispatch::dispatch_outbound_replication;
use crate::application::system_trace::{self, PhaseTimer};
use crate::config::ReplicationConfig;
use crate::domain::chdb_naming::{quoted_series_table_name, quoted_table_name};
use crate::domain::column_mapping::{ColumnMapping, measurement_meta_fingerprint};
use crate::domain::database::Precision;
use crate::domain::query_result::{QueryResponse, SeriesResult, StatementResult};
use crate::error::HyperbytedbError;
use crate::ports::metadata::MetadataPort;
use crate::ports::query::{QueryPort, QueryService};
use crate::ports::replication::{OutboundReplicationBatch, ReplicationPort};
use crate::timeseriesql::ast::*;
use crate::timeseriesql::to_clickhouse;

/// Max `(database, measurement)` entries in the query-side column mapping cache.
const COLUMN_MAPPING_CACHE_MAX: usize = 4096;

#[derive(Clone)]
pub struct QueryServiceImpl {
    query_port: Arc<dyn QueryPort>,
    metadata: Arc<dyn MetadataPort>,
    wal: Arc<dyn crate::ports::wal::WalPort>,
    query_timeout_secs: u64,
    /// Native MergeTree sink: `DROP TABLE` when measurements / databases are dropped.
    points_sink: Arc<dyn crate::ports::points_sink::PointsSinkPort>,
    /// `(db, measurement)` → (schema fingerprint, mapping) for TimeseriesQL translation.
    column_mapping_cache: Arc<RwLock<HashMap<(String, String), (u64, ColumnMapping)>>>,
    /// When set, `SELECT ... INTO` writes replicate to peers after local WAL append.
    replication_port: Option<Arc<dyn ReplicationPort>>,
    node_id: u64,
    replication_config: ReplicationConfig,
}

impl QueryServiceImpl {
    pub fn new(
        query_port: Arc<dyn QueryPort>,
        metadata: Arc<dyn MetadataPort>,
        wal: Arc<dyn crate::ports::wal::WalPort>,
        query_timeout_secs: u64,
        points_sink: Arc<dyn crate::ports::points_sink::PointsSinkPort>,
    ) -> Self {
        Self {
            query_port,
            metadata,
            wal,
            query_timeout_secs,
            points_sink,
            column_mapping_cache: Arc::new(RwLock::new(HashMap::with_capacity(256))),
            replication_port: None,
            node_id: 0,
            replication_config: ReplicationConfig::default(),
        }
    }

    async fn column_mapping_for(
        &self,
        db: &str,
        measurement: &str,
    ) -> Result<Option<ColumnMapping>, HyperbytedbError> {
        let meta = self.metadata.get_measurement(db, measurement).await?;
        let Some(m) = meta else {
            return Ok(None);
        };
        let fp = measurement_meta_fingerprint(&m);
        let key = (db.to_string(), measurement.to_string());
        {
            let cache = self.column_mapping_cache.read();
            if let Some((cached_fp, mapping)) = cache.get(&key)
                && *cached_fp == fp
            {
                return Ok(Some(mapping.clone()));
            }
        }
        let mapping = ColumnMapping::from_measurement_meta(&m);
        {
            let mut cache = self.column_mapping_cache.write();
            if cache.len() >= COLUMN_MAPPING_CACHE_MAX {
                cache.clear();
            }
            cache.insert(key, (fp, mapping.clone()));
        }
        Ok(Some(mapping))
    }

    /// Enable peer replication for mutating queries (`SELECT ... INTO`).
    #[must_use]
    pub fn with_cluster_replication(
        mut self,
        replication_port: Arc<dyn ReplicationPort>,
        node_id: u64,
        replication_config: ReplicationConfig,
    ) -> Self {
        self.replication_port = Some(replication_port);
        self.node_id = node_id;
        self.replication_config = replication_config;
        self
    }
}

fn check_authorization(
    user: &crate::domain::user::StoredUser,
    db: &str,
    stmt: &Statement,
) -> Result<(), HyperbytedbError> {
    if user.admin {
        return Ok(());
    }
    match stmt {
        Statement::CreateDatabase(_)
        | Statement::DropDatabase(_)
        | Statement::CreateUser { .. }
        | Statement::DropUser(_)
        | Statement::SetPassword { .. }
        | Statement::Grant { .. }
        | Statement::Revoke { .. } => Err(HyperbytedbError::Forbidden(
            "admin privileges required".to_string(),
        )),
        Statement::Select(s) if s.into.is_some() => {
            if !db.is_empty() && !user.can_write(db) {
                return Err(HyperbytedbError::Forbidden(format!(
                    "not authorized to write to database '{db}'"
                )));
            }
            Ok(())
        }
        Statement::Select(_)
        | Statement::ShowDatabases
        | Statement::ShowMeasurements(_)
        | Statement::ShowTagKeys(_)
        | Statement::ShowTagValues(_)
        | Statement::ShowFieldKeys(_)
        | Statement::ShowSeries(_)
        | Statement::ShowRetentionPolicies(_)
        | Statement::ShowUsers
        | Statement::ShowContinuousQueries => {
            if !db.is_empty() && !user.can_read(db) {
                return Err(HyperbytedbError::Forbidden(format!(
                    "not authorized to read from database '{db}'"
                )));
            }
            Ok(())
        }
        _ => {
            if !db.is_empty() && !user.can_write(db) {
                return Err(HyperbytedbError::Forbidden(format!(
                    "not authorized to write to database '{db}'"
                )));
            }
            Ok(())
        }
    }
}

/// Returns true for DDL/DML and for `SELECT ... INTO` (writes).
fn is_mutating_statement(stmt: &Statement) -> bool {
    match stmt {
        Statement::Select(s) => s.into.is_some(),
        Statement::ShowDatabases
        | Statement::ShowMeasurements(_)
        | Statement::ShowTagKeys(_)
        | Statement::ShowTagValues(_)
        | Statement::ShowFieldKeys(_)
        | Statement::ShowSeries(_)
        | Statement::ShowRetentionPolicies(_)
        | Statement::ShowUsers
        | Statement::ShowContinuousQueries => false,
        Statement::CreateDatabase(_)
        | Statement::DropDatabase(_)
        | Statement::DropMeasurement(_)
        | Statement::DropUser(_)
        | Statement::CreateRetentionPolicyStmt { .. }
        | Statement::AlterRetentionPolicyStmt { .. }
        | Statement::DropRetentionPolicyStmt { .. }
        | Statement::CreateUser { .. }
        | Statement::SetPassword { .. }
        | Statement::Grant { .. }
        | Statement::Revoke { .. }
        | Statement::Delete(_)
        | Statement::CreateContinuousQuery(_)
        | Statement::DropContinuousQuery { .. } => true,
    }
}

#[async_trait]
impl QueryService for QueryServiceImpl {
    async fn execute_query(
        &self,
        db: &str,
        query: &str,
        epoch: Option<&str>,
        caller: Option<&crate::domain::user::StoredUser>,
    ) -> Result<QueryResponse, HyperbytedbError> {
        let timeout = std::time::Duration::from_secs(self.query_timeout_secs);
        let caller_owned = caller.cloned();
        let fut = async {
            let mut pt = PhaseTimer::start();
            let stmts = crate::timeseriesql::parse(query)?;
            pt.record_phase("parse_us");

            if let Some(ref user) = caller_owned {
                for stmt in &stmts {
                    check_authorization(user, db, stmt)?;
                }
            }
            pt.record_phase("authorize_us");

            let stmt_count = stmts.len();
            let _exec_span = system_trace::query_execute_span(db, stmt_count);
            let _exec_guard = _exec_span.enter();
            let svc = Arc::new(self.clone());
            let db_arc = Arc::<str>::from(db);
            let epoch_arc = epoch.map(Arc::<str>::from);

            let mut results = Vec::with_capacity(stmt_count);
            let mut i = 0usize;
            while i < stmts.len() {
                if is_mutating_statement(&stmts[i]) {
                    let statement_id = i as u32;
                    let r = execute_statement(
                        &svc,
                        db_arc.as_ref(),
                        &stmts[i],
                        epoch_arc.as_deref(),
                        statement_id,
                    )
                    .await?;
                    results.push(r);
                    i += 1;
                } else {
                    let start = i;
                    while i < stmts.len() && !is_mutating_statement(&stmts[i]) {
                        i += 1;
                    }
                    let futures = (start..i).map(|j| {
                        let statement_id = j as u32;
                        let svc = Arc::clone(&svc);
                        let db_arc = Arc::clone(&db_arc);
                        let epoch_arc = epoch_arc.clone();
                        let stmt = &stmts[j];

                        async move {
                            execute_statement(
                                &svc,
                                db_arc.as_ref(),
                                stmt,
                                epoch_arc.as_deref(),
                                statement_id,
                            )
                            .await
                        }
                    });
                    for r in futures::future::join_all(futures).await {
                        results.push(r?);
                    }
                }
            }

            pt.record_phase_final("statement_us");
            Ok(QueryResponse { results })
        };

        match tokio::time::timeout(timeout, fut).await {
            Ok(result) => result,
            Err(_) => Err(HyperbytedbError::QueryTimeout),
        }
    }
}

async fn execute_statement(
    svc: &QueryServiceImpl,
    db: &str,
    stmt: &Statement,
    epoch: Option<&str>,
    statement_id: u32,
) -> Result<StatementResult, HyperbytedbError> {
    match stmt {
        Statement::ShowDatabases => {
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
        Statement::ShowMeasurements(s) => {
            let db = s.database.as_deref().unwrap_or(db);
            if db.is_empty() {
                return Ok(StatementResult {
                    statement_id,
                    series: Some(vec![]),
                    error: Some("database is required".to_string()),
                });
            }
            let names = svc.metadata.list_measurements(db).await?;
            let columns = vec!["name".to_string()];
            let values: Vec<Vec<serde_json::Value>> = names
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
        Statement::ShowTagKeys(s) => {
            let db = s.database.as_deref().unwrap_or(db);
            let measurement = s.from.as_ref().and_then(|m| m.name_str());
            let keys = svc.metadata.list_tag_keys(db, measurement).await?;
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
        Statement::ShowTagValues(s) => {
            let db = s.database.as_deref().unwrap_or(db);
            let measurement = s.from.as_ref().and_then(|m| m.name_str());

            let all_tag_keys = svc.metadata.list_tag_keys(db, measurement).await?;

            let matching_keys: Vec<String> = match &s.tag_key {
                TagKeySelector::All => all_tag_keys,
                TagKeySelector::Eq(k) => vec![k.clone()],
                TagKeySelector::Neq(k) => all_tag_keys.into_iter().filter(|tk| tk != k).collect(),
                TagKeySelector::Regex(pattern) => match regex::Regex::new(pattern) {
                    Ok(re) => all_tag_keys
                        .into_iter()
                        .filter(|tk| re.is_match(tk))
                        .collect(),
                    Err(_) => vec![],
                },
                TagKeySelector::In(keys) => {
                    let key_set: std::collections::HashSet<&String> = keys.iter().collect();
                    all_tag_keys
                        .into_iter()
                        .filter(|tk| key_set.contains(tk))
                        .collect()
                }
            };

            let mut all_values = Vec::new();
            for tag_key in &matching_keys {
                let values_list = svc
                    .metadata
                    .list_tag_values(db, tag_key, measurement)
                    .await?;
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
                    values: all_values,
                    partial: None,
                }]),
                error: None,
            })
        }
        Statement::ShowFieldKeys(s) => {
            let db = s.database.as_deref().unwrap_or(db);
            let measurement = s.from.as_ref().and_then(|m| m.name_str());
            if db.is_empty() {
                return Ok(StatementResult {
                    statement_id,
                    series: Some(vec![]),
                    error: Some("database is required".to_string()),
                });
            }
            let mut field_values = Vec::new();
            let measurements: Vec<String> = if let Some(m) = measurement {
                vec![m.to_string()]
            } else {
                svc.metadata.list_measurements(db).await?
            };
            for m in &measurements {
                if let Some(meta) = svc.metadata.get_measurement(db, m).await? {
                    for (name, disc) in meta.field_types_as_tuples() {
                        let typ =
                            crate::domain::point::FieldValue::type_name_from_discriminant(disc);
                        field_values.push(vec![
                            serde_json::Value::String(name),
                            serde_json::Value::String(typ.to_string()),
                        ]);
                    }
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
        Statement::CreateDatabase(name) => {
            svc.metadata.create_database(name).await?;
            Ok(StatementResult {
                statement_id,
                series: Some(vec![]),
                error: None,
            })
        }
        Statement::DropDatabase(name) => {
            // Snapshot measurements + retention policies before
            // metadata drops them so the native sink can DROP TABLE
            // each backing chDB table.
            let to_drop: Vec<(String, String)> = {
                let rps = svc
                    .metadata
                    .list_retention_policies(name)
                    .await
                    .unwrap_or_default();
                let measurements = svc
                    .metadata
                    .list_measurements(name)
                    .await
                    .unwrap_or_default();
                let mut pairs = Vec::with_capacity(rps.len() * measurements.len());
                for rp in &rps {
                    for m in &measurements {
                        pairs.push((rp.name.clone(), m.clone()));
                    }
                }
                pairs
            };
            svc.metadata.drop_database(name).await?;
            for (rp, m) in &to_drop {
                if let Err(e) = svc.points_sink.drop_measurement(name, rp, m).await {
                    tracing::warn!(
                        db = name,
                        rp = %rp,
                        measurement = %m,
                        error = %e,
                        "failed to drop chDB native table during DROP DATABASE"
                    );
                }
            }
            Ok(StatementResult {
                statement_id,
                series: Some(vec![]),
                error: None,
            })
        }
        Statement::Select(select_stmt) => {
            if db.is_empty() {
                return Ok(StatementResult {
                    statement_id,
                    series: None,
                    error: Some("database is required".to_string()),
                });
            }
            let rp = svc
                .metadata
                .get_default_rp(db)
                .await
                .unwrap_or_else(|_| "autogen".to_string());

            let source = select_stmt.from.first().ok_or_else(|| {
                HyperbytedbError::QueryParse("SELECT requires FROM clause".to_string())
            })?;

            let group_by_tags: Vec<String> = select_stmt
                .group_by
                .as_ref()
                .map(|gb| {
                    gb.tag_dimensions()
                        .into_iter()
                        .map(|s| s.to_string())
                        .collect()
                })
                .unwrap_or_default();

            let (time_min, time_max) =
                to_clickhouse::extract_time_bounds(select_stmt.condition.as_ref());

            let mut all_series = match source {
                MeasurementSource::Concrete(m) => match &m.name {
                    MeasurementName::Regex(pattern) => {
                        let measurements = svc.metadata.list_measurements(db).await?;
                        let matching: Vec<_> = {
                            let re = regex_pattern_matches(pattern);
                            measurements.into_iter().filter(|m| re(m)).collect()
                        };
                        let futs: Vec<_> = matching
                            .iter()
                            .map(|meas_name| {
                                execute_measurement_query(
                                    svc,
                                    db,
                                    &rp,
                                    meas_name,
                                    select_stmt,
                                    time_min,
                                    time_max,
                                    epoch,
                                    &group_by_tags,
                                )
                            })
                            .collect();
                        let results = futures::future::join_all(futs).await;
                        let mut combined = Vec::new();
                        for result in results {
                            combined.append(&mut result?);
                        }
                        combined
                    }
                    MeasurementName::Name(name) => {
                        execute_measurement_query(
                            svc,
                            db,
                            &rp,
                            name,
                            select_stmt,
                            time_min,
                            time_max,
                            epoch,
                            &group_by_tags,
                        )
                        .await?
                    }
                },
                MeasurementSource::Subquery(sub_stmt) => {
                    let sub_source = sub_stmt
                        .from
                        .first()
                        .and_then(|s| s.name_str())
                        .ok_or_else(|| {
                            HyperbytedbError::QueryParse(
                                "subquery requires measurement".to_string(),
                            )
                        })?;
                    let sub_mapping = svc.column_mapping_for(db, sub_source).await?;
                    let table = quoted_table_name(db, &rp, sub_source);
                    let sub_series_table = quoted_series_table_name(db, &rp, sub_source);
                    let sub_series_join = sub_mapping.as_ref().map(|_| to_clickhouse::SeriesJoin {
                        table: &sub_series_table,
                        force: false,
                    });
                    let sub_sql = to_clickhouse::translate_native_table(
                        sub_stmt,
                        &table,
                        sub_mapping.as_ref(),
                        sub_series_join,
                    )?;
                    let outer_sql = to_clickhouse::translate_with_source(
                        select_stmt,
                        &format!("({})", sub_sql),
                    )?;
                    let raw = svc.query_port.execute_sql(&outer_sql).await?;
                    parse_json_each_row_to_series(
                        &raw,
                        sub_source,
                        epoch,
                        &group_by_tags,
                        drop_fill_null_buckets(select_stmt),
                    )?
                }
            };

            // Apply SLIMIT/SOFFSET (series-level pagination)
            if select_stmt.slimit.is_some() || select_stmt.soffset.is_some() {
                let soffset = select_stmt.soffset.unwrap_or(0) as usize;
                let slimit = select_stmt.slimit.unwrap_or(u64::MAX) as usize;
                let len = all_series.len();
                let start = soffset.min(len);
                let end = (start + slimit).min(len);
                all_series = all_series[start..end].to_vec();
            }

            // Handle INTO clause: write results back as a new measurement
            if let Some(ref into_target) = select_stmt.into
                && let MeasurementName::Name(ref target_name) = into_target.name
            {
                let into_db = into_target.database.as_deref().unwrap_or(db);
                let count = write_series_as_points(svc, into_db, target_name, &all_series).await?;
                return Ok(StatementResult {
                    statement_id,
                    series: Some(vec![SeriesResult {
                        name: "result".to_string(),
                        tags: None,
                        columns: vec!["time".to_string(), "written".to_string()],
                        values: vec![vec![
                            serde_json::Value::String(chrono::Utc::now().to_rfc3339()),
                            serde_json::json!(count),
                        ]],
                        partial: None,
                    }]),
                    error: None,
                });
            }

            Ok(StatementResult {
                statement_id,
                series: Some(all_series),
                error: None,
            })
        }
        Statement::ShowSeries(s) => {
            let db = s.database.as_deref().unwrap_or(db);
            let measurements = svc.metadata.list_measurements(db).await?;
            let mut values = Vec::new();
            for m in &measurements {
                if let Some(meta) = svc.metadata.get_measurement(db, m).await? {
                    values.push(vec![serde_json::Value::String(format!(
                        "{},{}",
                        m,
                        meta.tag_keys.join(",")
                    ))]);
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
        Statement::DropMeasurement(name) => {
            let rp = svc
                .metadata
                .get_default_rp(db)
                .await
                .unwrap_or_else(|_| "autogen".to_string());
            svc.metadata.delete_measurement(db, name).await?;
            if let Err(e) = svc.points_sink.drop_measurement(db, &rp, name).await {
                tracing::warn!(
                    db = db,
                    measurement = name,
                    error = %e,
                    "failed to drop chDB native table for measurement (metadata already cleared)"
                );
            }
            Ok(StatementResult {
                statement_id,
                series: Some(vec![]),
                error: None,
            })
        }
        Statement::Delete(del) => {
            // Convert the WHERE condition to SQL for tombstone storage, resolving
            // tag identifiers to physical column names so the predicate matches
            // the tag columns exposed by the series-rejoin inline view at query
            // time (the fact table itself no longer stores tag columns).
            let del_mapping = svc.column_mapping_for(db, &del.from).await?;
            let predicate_sql = if let Some(ref cond) = del.condition {
                let mut sql = String::new();
                to_clickhouse::translate_condition_with_mapping(
                    cond,
                    del_mapping.as_ref(),
                    &mut sql,
                )?;
                sql
            } else {
                String::new()
            };

            svc.metadata
                .store_tombstone(db, &del.from, &predicate_sql)
                .await?;

            tracing::info!(
                db = db,
                measurement = %del.from,
                predicate = %predicate_sql,
                "DELETE tombstone stored"
            );

            Ok(StatementResult {
                statement_id,
                series: Some(vec![]),
                error: None,
            })
        }
        Statement::CreateContinuousQuery(cq) => {
            use crate::ports::metadata::ContinuousQueryDef;

            let resample_every_secs = cq
                .resample_every
                .as_ref()
                .map(|d| (d.to_nanos() / 1_000_000_000) as u64);
            let resample_for_secs = cq
                .resample_for
                .as_ref()
                .map(|d| (d.to_nanos() / 1_000_000_000) as u64);

            let query_text = cq.raw_query.clone();

            let def = ContinuousQueryDef {
                name: cq.name.clone(),
                database: cq.database.clone(),
                query_text,
                resample_every_secs,
                resample_for_secs,
                created_at: chrono::Utc::now().to_rfc3339(),
            };

            svc.metadata
                .store_continuous_query(&cq.database, &cq.name, &def)
                .await?;

            Ok(StatementResult {
                statement_id,
                series: Some(vec![]),
                error: None,
            })
        }
        Statement::ShowContinuousQueries => {
            let dbs = svc.metadata.list_databases().await?;
            let mut all_cqs = Vec::new();
            for db_entry in &dbs {
                let cqs = svc.metadata.list_continuous_queries(&db_entry.name).await?;
                all_cqs.extend(cqs);
            }

            let columns = vec![
                "name".to_string(),
                "database".to_string(),
                "query".to_string(),
            ];
            let values: Vec<Vec<serde_json::Value>> = all_cqs
                .iter()
                .map(|cq| {
                    vec![
                        serde_json::Value::String(cq.name.clone()),
                        serde_json::Value::String(cq.database.clone()),
                        serde_json::Value::String(cq.query_text.clone()),
                    ]
                })
                .collect();

            Ok(StatementResult {
                statement_id,
                series: Some(vec![SeriesResult {
                    name: "continuous_queries".to_string(),
                    tags: None,
                    columns,
                    values,
                    partial: None,
                }]),
                error: None,
            })
        }
        Statement::DropContinuousQuery { name, db: cq_db } => {
            let target_db = if cq_db.is_empty() { db } else { cq_db };
            svc.metadata.drop_continuous_query(target_db, name).await?;
            Ok(StatementResult {
                statement_id,
                series: Some(vec![]),
                error: None,
            })
        }
        Statement::ShowRetentionPolicies(rp_db) => {
            let target_db = if rp_db.is_empty() { db } else { rp_db };
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
                    let dur_str = match rp.duration {
                        Some(d) => format!("{}s", d.as_secs()),
                        None => "0s".to_string(),
                    };
                    let sgd_str = format!("{}s", rp.shard_group_duration.as_secs());
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
        Statement::ShowUsers => {
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
        Statement::CreateRetentionPolicyStmt {
            name,
            db: rp_db,
            duration,
            replication,
            shard_duration,
            is_default,
        } => {
            let target_db = if rp_db.is_empty() { db } else { rp_db.as_str() };
            let std_duration = duration
                .as_ref()
                .map(|d| std::time::Duration::from_nanos(d.to_nanos() as u64));
            let shard_dur = shard_duration
                .as_ref()
                .map(|d| std::time::Duration::from_nanos(d.to_nanos() as u64))
                .unwrap_or(std::time::Duration::from_secs(7 * 24 * 3600));
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
        Statement::DropRetentionPolicyStmt { name, db: rp_db } => {
            let target_db = if rp_db.is_empty() { db } else { rp_db.as_str() };
            svc.metadata.drop_retention_policy(target_db, name).await?;
            Ok(StatementResult {
                statement_id,
                series: Some(vec![]),
                error: None,
            })
        }
        Statement::CreateUser {
            username,
            password,
            admin,
        } => {
            let password_hash = crate::adapters::http::auth_middleware::hash_password(password)
                .map_err(|e| HyperbytedbError::Internal(e.to_string()))?;
            svc.metadata
                .create_user(username, &password_hash, *admin)
                .await?;
            Ok(StatementResult {
                statement_id,
                series: Some(vec![]),
                error: None,
            })
        }
        Statement::DropUser(username) => {
            svc.metadata.drop_user(username).await?;
            Ok(StatementResult {
                statement_id,
                series: Some(vec![]),
                error: None,
            })
        }
        Statement::SetPassword { username, password } => {
            let existing = svc.metadata.get_user(username).await?;
            let is_admin = existing.map(|u| u.admin).unwrap_or(false);
            let password_hash = crate::adapters::http::auth_middleware::hash_password(password)
                .map_err(|e| HyperbytedbError::Internal(e.to_string()))?;
            svc.metadata
                .create_user(username, &password_hash, is_admin)
                .await?;
            Ok(StatementResult {
                statement_id,
                series: Some(vec![]),
                error: None,
            })
        }
        Statement::AlterRetentionPolicyStmt { .. } => Ok(StatementResult {
            statement_id,
            series: Some(vec![]),
            error: None,
        }),
        Statement::Grant { username, database } => {
            match database {
                Some(db) => {
                    svc.metadata
                        .grant_privilege(username, db, crate::domain::user::DatabasePrivilege::All)
                        .await?;
                }
                None => {
                    if let Some(user) = svc.metadata.get_user(username).await? {
                        svc.metadata
                            .create_user(username, &user.password_hash, true)
                            .await?;
                    }
                }
            }
            Ok(StatementResult {
                statement_id,
                series: Some(vec![]),
                error: None,
            })
        }
        Statement::Revoke { username, database } => {
            match database {
                Some(db) => {
                    svc.metadata.revoke_privilege(username, db).await?;
                }
                None => {
                    if let Some(user) = svc.metadata.get_user(username).await? {
                        svc.metadata
                            .create_user(username, &user.password_hash, false)
                            .await?;
                    }
                }
            }
            Ok(StatementResult {
                statement_id,
                series: Some(vec![]),
                error: None,
            })
        }
    }
}

/// ClickHouse/JSONEachRow is strict JSON; chDB/CH may still emit unquoted `nan` / `-inf` in
/// numeric fields, which simd_json rejects (e.g. `InvalidNumber` at a digit in `0`-like junk).
static CHDB_NON_JSON_NUMERIC: OnceLock<Regex> = OnceLock::new();

/// Replace `:<ws>NaN` / `:<ws>±inf` (non-JSON) with `:<ws>null` so serde_json can parse.
fn chdb_sanitize_non_json_number_tokens(line: &str) -> String {
    let re = CHDB_NON_JSON_NUMERIC.get_or_init(|| {
        // Literal pattern; invalid regex is a programming error, not user input.
        #[allow(clippy::expect_used)]
        Regex::new(r#"(?i)(:\s*)(?:-?inf(?:inity)?|nan)\b"#)
            .expect("CHDB non-JSON float regex is valid")
    });
    re.replace_all(line, |caps: &regex::Captures| format!("{}null", &caps[1]))
        .to_string()
}

/// `fill(null)` with `GROUP BY time`: omit buckets where every value column is NULL
/// (Influx shows nulls; we drop those rows from the JSON series so clients do not plot 0).
#[inline]
fn drop_fill_null_buckets(stmt: &SelectStatement) -> bool {
    matches!(stmt.fill, Some(FillOption::Null))
        && stmt
            .group_by
            .as_ref()
            .and_then(|gb| gb.time_dimension())
            .is_some()
}

fn series_omit_all_null_value_rows(series: &mut [SeriesResult]) {
    for sr in series.iter_mut() {
        let Some(time_idx) = sr.columns.iter().position(|c| c == "time") else {
            continue;
        };
        sr.values.retain(|row| {
            row.iter()
                .enumerate()
                .any(|(i, v)| i != time_idx && !v.is_null())
        });
    }
}

/// Parse one JSON object line from a JSONEachRow (or similar) chDB/ClickHouse result.
fn parse_chdb_json_line(line: &str) -> Result<serde_json::Value, HyperbytedbError> {
    let mut parse_buf: Vec<u8> = line.as_bytes().to_vec();
    if let Ok(v) = simd_json::from_slice(&mut parse_buf) {
        return Ok(v);
    }
    if let Ok(v) = serde_json::from_str(line) {
        return Ok(v);
    }
    let fixed = chdb_sanitize_non_json_number_tokens(line);
    if let Ok(v) = serde_json::from_str(&fixed) {
        return Ok(v);
    }
    Err(HyperbytedbError::Internal(format!(
        "chDB JSON line parse (after non-JSON float sanitize): {line:.256}"
    )))
}

fn parse_json_each_row_to_series(
    raw: &str,
    measurement: &str,
    epoch: Option<&str>,
    group_by_tags: &[String],
    omit_all_null_buckets: bool,
) -> Result<Vec<SeriesResult>, HyperbytedbError> {
    if raw.trim().is_empty() {
        return Ok(vec![]);
    }

    let line_count = raw
        .as_bytes()
        .iter()
        .filter(|&&b| b == b'\n')
        .count()
        .max(1);
    let mut rows: Vec<serde_json::Value> = Vec::with_capacity(line_count);
    for line in raw.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let v: serde_json::Value = parse_chdb_json_line(line)?;
        rows.push(v);
    }
    if rows.is_empty() {
        return Ok(vec![]);
    }

    let raw_columns: Vec<String> = rows
        .first()
        .and_then(|r| r.as_object())
        .map(|o| o.keys().cloned().collect())
        .unwrap_or_default();

    let tag_set: std::collections::HashSet<&str> =
        group_by_tags.iter().map(|s| s.as_str()).collect();

    let mut col_pairs: Vec<(String, String)> = raw_columns
        .into_iter()
        .filter(|raw| !tag_set.contains(raw.as_str()))
        .map(|raw| {
            let display = if raw == "__time" {
                "time".to_string()
            } else {
                raw.clone()
            };
            (raw, display)
        })
        .collect();

    if let Some(pos) = col_pairs.iter().position(|(_, d)| d == "time")
        && pos != 0
    {
        let pair = col_pairs.remove(pos);
        col_pairs.insert(0, pair);
    }

    let columns: Vec<String> = col_pairs.iter().map(|(_, d)| d.clone()).collect();
    let time_idx = columns.iter().position(|c| c == "time");

    if group_by_tags.is_empty() {
        let values: Vec<Vec<serde_json::Value>> = rows
            .iter()
            .map(|r| row_to_values(r, &col_pairs, time_idx, epoch))
            .collect();
        let mut out = vec![SeriesResult {
            name: measurement.to_string(),
            tags: None,
            columns,
            values,
            partial: None,
        }];
        if omit_all_null_buckets {
            series_omit_all_null_value_rows(&mut out);
        }
        return Ok(out);
    }

    // Single-pass: parse rows and bucket into series simultaneously
    let mut series_map: indexmap::IndexMap<Vec<(String, String)>, Vec<Vec<serde_json::Value>>> =
        indexmap::IndexMap::new();

    for row in &rows {
        let obj = row.as_object();
        let tag_kv: Vec<(String, String)> = group_by_tags
            .iter()
            .map(|tag| {
                let val = obj
                    .and_then(|o| o.get(tag))
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                (tag.clone(), val)
            })
            .collect();
        let row_values = row_to_values(row, &col_pairs, time_idx, epoch);
        series_map.entry(tag_kv).or_default().push(row_values);
    }

    let mut result: Vec<SeriesResult> = series_map
        .into_iter()
        .map(|(tag_kv, values)| {
            let tags: HashMap<String, String> = tag_kv.into_iter().collect();
            SeriesResult {
                name: measurement.to_string(),
                tags: Some(tags),
                columns: columns.clone(),
                values,
                partial: None,
            }
        })
        .collect();

    if omit_all_null_buckets {
        series_omit_all_null_value_rows(&mut result);
    }

    Ok(result)
}

fn row_to_values(
    row: &serde_json::Value,
    col_pairs: &[(String, String)],
    time_idx: Option<usize>,
    epoch: Option<&str>,
) -> Vec<serde_json::Value> {
    row.as_object()
        .map(|o| {
            col_pairs
                .iter()
                .enumerate()
                .map(|(i, (raw_key, _))| {
                    let v = o.get(raw_key).cloned().unwrap_or(serde_json::Value::Null);
                    if Some(i) == time_idx {
                        normalize_time_value(v, epoch)
                    } else {
                        v
                    }
                })
                .collect()
        })
        .unwrap_or_default()
}

/// Parse a chDB DateTime64 string into a Unix nanosecond timestamp.
/// Handles formats like "2026-03-01 14:30:00.000000000" and "2026-03-01 14:30:00".
fn parse_chdb_datetime_to_nanos(s: &str) -> Option<i64> {
    let s = s.trim();
    if s.len() < 19 || s.as_bytes().get(10) != Some(&b' ') {
        return None;
    }
    let date_part = &s[..10];
    let time_part = &s[11..];

    let (hms, frac_nanos) = if let Some(dot_pos) = time_part.find('.') {
        let hms = &time_part[..dot_pos];
        let frac_str = &time_part[dot_pos + 1..];
        // Pad or truncate to 9 digits (nanoseconds)
        let padded = format!("{:0<9}", &frac_str[..frac_str.len().min(9)]);
        let nanos: i64 = padded.parse().unwrap_or(0);
        (hms, nanos)
    } else {
        (time_part, 0i64)
    };

    // Parse date: YYYY-MM-DD
    let parts: Vec<&str> = date_part.split('-').collect();
    if parts.len() != 3 {
        return None;
    }
    let year: i32 = parts[0].parse().ok()?;
    let month: u32 = parts[1].parse().ok()?;
    let day: u32 = parts[2].parse().ok()?;

    // Parse time: HH:MM:SS
    let tparts: Vec<&str> = hms.split(':').collect();
    if tparts.len() != 3 {
        return None;
    }
    let hour: u32 = tparts[0].parse().ok()?;
    let min: u32 = tparts[1].parse().ok()?;
    let sec: u32 = tparts[2].parse().ok()?;

    let dt = chrono::NaiveDate::from_ymd_opt(year, month, day)?.and_hms_opt(hour, min, sec)?;
    let unix_secs = dt.and_utc().timestamp();
    Some(unix_secs * 1_000_000_000 + frac_nanos)
}

/// Convert chDB's DateTime64 string to the format requested by the `epoch` param.
/// - epoch=None  → RFC3339 string ("2026-03-01T14:30:00Z")
/// - epoch="ns"  → nanosecond integer
/// - epoch="u"   → microsecond integer
/// - epoch="ms"  → millisecond integer
/// - epoch="s"   → second integer
fn normalize_time_value(v: serde_json::Value, epoch: Option<&str>) -> serde_json::Value {
    match &v {
        serde_json::Value::String(s) => {
            let s = s.trim();
            if s.len() < 19 || s.as_bytes().get(10) != Some(&b' ') {
                return v;
            }

            match epoch {
                Some("ns") | Some("n") => {
                    if let Some(nanos) = parse_chdb_datetime_to_nanos(s) {
                        serde_json::Value::Number(serde_json::Number::from(nanos))
                    } else {
                        v
                    }
                }
                Some("u") | Some("us") => {
                    if let Some(nanos) = parse_chdb_datetime_to_nanos(s) {
                        serde_json::Value::Number(serde_json::Number::from(nanos / 1_000))
                    } else {
                        v
                    }
                }
                Some("ms") => {
                    if let Some(nanos) = parse_chdb_datetime_to_nanos(s) {
                        serde_json::Value::Number(serde_json::Number::from(nanos / 1_000_000))
                    } else {
                        v
                    }
                }
                Some("s") => {
                    if let Some(nanos) = parse_chdb_datetime_to_nanos(s) {
                        serde_json::Value::Number(serde_json::Number::from(nanos / 1_000_000_000))
                    } else {
                        v
                    }
                }
                _ => {
                    // Default: RFC3339 string
                    let mut rfc = String::with_capacity(s.len() + 2);
                    rfc.push_str(&s[..10]);
                    rfc.push('T');
                    let time_part = &s[11..];
                    if let Some(dot_pos) = time_part.find('.') {
                        let frac = &time_part[dot_pos + 1..];
                        if frac.chars().all(|c| c == '0') {
                            rfc.push_str(&time_part[..dot_pos]);
                        } else {
                            let trimmed = frac.trim_end_matches('0');
                            rfc.push_str(&time_part[..dot_pos + 1]);
                            rfc.push_str(trimmed);
                        }
                    } else {
                        rfc.push_str(time_part);
                    }
                    rfc.push('Z');
                    serde_json::Value::String(rfc)
                }
            }
        }
        _ => v,
    }
}

fn inject_tombstone_predicates(sql: String, tombstones: &[(String, String)]) -> String {
    if tombstones.is_empty() {
        return sql;
    }
    let mut result = sql;
    for (_id, predicate) in tombstones {
        if predicate.is_empty() {
            continue;
        }
        let negated = format!(" AND NOT ({})", predicate);
        if result.contains("\nWHERE ") {
            if let Some(where_end) = result.find("\nGROUP BY") {
                result.insert_str(where_end, &negated);
            } else if let Some(where_end) = result.find("\nORDER BY") {
                result.insert_str(where_end, &negated);
            } else if let Some(where_end) = result.find("\nLIMIT") {
                result.insert_str(where_end, &negated);
            } else {
                result.push_str(&negated);
            }
        } else {
            let from_end = result
                .find("\nGROUP BY")
                .or_else(|| result.find("\nORDER BY"))
                .or_else(|| result.find("\nLIMIT"))
                .unwrap_or(result.len());
            result.insert_str(from_end, &format!("\nWHERE NOT ({})", predicate));
        }
    }
    result
}

/// Execute TimeseriesQL for a single measurement against the backing MergeTree table.
#[allow(clippy::too_many_arguments)]
async fn execute_measurement_query(
    svc: &QueryServiceImpl,
    db: &str,
    rp: &str,
    measurement: &str,
    stmt: &SelectStatement,
    _time_min: Option<i64>,
    _time_max: Option<i64>,
    epoch: Option<&str>,
    group_by_tags: &[String],
) -> Result<Vec<SeriesResult>, HyperbytedbError> {
    let column_mapping = svc.column_mapping_for(db, measurement).await?;

    // Tombstone predicates (spliced into WHERE below) may reference tag columns,
    // which only exist on the series-rejoin inline view — so force the join when
    // any tombstone is present for this measurement.
    let tombstones = svc.metadata.list_tombstones(db, measurement).await?;

    let table = quoted_table_name(db, rp, measurement);
    let series_table = quoted_series_table_name(db, rp, measurement);
    let series_join = column_mapping.as_ref().map(|_| to_clickhouse::SeriesJoin {
        table: &series_table,
        force: !tombstones.is_empty(),
    });
    let mut sql =
        to_clickhouse::translate_native_table(stmt, &table, column_mapping.as_ref(), series_join)?;
    sql = inject_tombstone_predicates(sql, &tombstones);
    let raw = svc.query_port.execute_sql(&sql).await?;
    parse_json_each_row_to_series(
        &raw,
        measurement,
        epoch,
        group_by_tags,
        drop_fill_null_buckets(stmt),
    )
}

fn regex_pattern_matches(pattern: &str) -> Box<dyn Fn(&str) -> bool + '_> {
    let anchored = if pattern.starts_with('^') {
        pattern.to_string()
    } else {
        format!("^{}$", pattern)
    };
    match Regex::new(&anchored) {
        Ok(re) => Box::new(move |s: &str| re.is_match(s)),
        Err(_) => Box::new(move |s: &str| s == pattern),
    }
}

async fn write_series_as_points(
    svc: &QueryServiceImpl,
    db: &str,
    measurement: &str,
    series: &[SeriesResult],
) -> Result<u64, HyperbytedbError> {
    use crate::domain::point::{FieldValue, Point};
    use crate::ports::metadata::MeasurementMeta;
    use crate::ports::wal::WalEntry;
    use std::collections::BTreeMap;

    let mut total_count = 0u64;
    let mut all_points = Vec::new();
    let mut tag_keys: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();

    for sr in series {
        let tags: BTreeMap<String, String> =
            sr.tags.clone().map(BTreeMap::from_iter).unwrap_or_default();
        for key in tags.keys() {
            tag_keys.insert(key.clone());
        }

        for row in &sr.values {
            let mut fields = BTreeMap::new();
            let mut timestamp = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);

            for (i, col) in sr.columns.iter().enumerate() {
                if col == "time" {
                    if let Some(serde_json::Value::Number(n)) = row.get(i) {
                        if let Some(ts) = n.as_i64() {
                            timestamp = ts;
                        }
                    } else if let Some(serde_json::Value::String(s)) = row.get(i)
                        && let Ok(dt) = chrono::DateTime::parse_from_rfc3339(s)
                    {
                        timestamp = dt.timestamp_nanos_opt().unwrap_or(0);
                    }
                    continue;
                }
                if tags.contains_key(col) {
                    continue;
                }
                if let Some(val) = row.get(i) {
                    match val {
                        serde_json::Value::Number(n) => {
                            if let Some(f) = n.as_f64() {
                                fields.insert(col.clone(), FieldValue::Float(f));
                            } else if let Some(i) = n.as_i64() {
                                fields.insert(col.clone(), FieldValue::Integer(i));
                            }
                        }
                        serde_json::Value::String(s) => {
                            fields.insert(col.clone(), FieldValue::String(s.clone()));
                        }
                        serde_json::Value::Bool(b) => {
                            fields.insert(col.clone(), FieldValue::Boolean(*b));
                        }
                        _ => {}
                    }
                }
            }

            if !fields.is_empty() {
                all_points.push(Point {
                    measurement: measurement.to_string(),
                    tags: tags.clone(),
                    fields,
                    timestamp,
                });
                total_count += 1;
            }
        }
    }

    if !all_points.is_empty() {
        // Register metadata
        let mut field_types = std::collections::HashMap::new();
        for p in &all_points {
            for (k, v) in &p.fields {
                field_types.insert(k.clone(), v.type_discriminant());
            }
        }
        let meta = MeasurementMeta {
            name: measurement.to_string(),
            field_types,
            tag_keys: tag_keys.into_iter().collect(),
        };
        svc.metadata.register_measurement(db, &meta).await?;

        let rp = svc
            .metadata
            .get_default_rp(db)
            .await
            .unwrap_or_else(|_| "autogen".to_string());
        let replication_body = if svc.replication_port.is_some() {
            Some(encode_points_to_line_protocol(
                &all_points,
                Precision::Nanosecond,
            )?)
        } else {
            None
        };
        let entry = WalEntry {
            database: db.to_string(),
            retention_policy: rp.clone(),
            points: all_points,
            origin_node_id: svc.node_id,
        };
        let wal_seq = svc.wal.append(entry).await?;

        if let Some(ref replication_port) = svc.replication_port
            && let Some(body) = replication_body
        {
            dispatch_outbound_replication(
                Arc::clone(replication_port),
                svc.node_id,
                &svc.replication_config,
                OutboundReplicationBatch {
                    database: db.to_string(),
                    retention_policy: rp,
                    precision: Some("ns".to_string()),
                    body,
                    wal_seq,
                },
            )
            .await?;
        }
    }

    Ok(total_count)
}
