use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

use parking_lot::RwLock;
use regex::Regex;

use crate::application::line_protocol::encode_points_to_line_protocol;
use crate::application::materialized_view_service::MaterializedViewService;
use crate::application::replication_dispatch::dispatch_outbound_replication;
use crate::config::ReplicationConfig;
use crate::domain::chdb_naming::{
    quote_backticks, quoted_series_table_name, quoted_table_name, unquoted_series_table_name,
};
use crate::domain::column_mapping::{ColumnMapping, measurement_meta_fingerprint};
use crate::domain::continuous_query::ContinuousQueryDef;
use crate::domain::cq_schedule::{coverage_window, should_run};
use crate::domain::database::Precision;
use crate::domain::query_result::{QueryResponse, SeriesResult, StatementResult};
use crate::error::HyperbytedbError;
use crate::ports::metadata::MetadataPort;
use crate::ports::query::{CqRunResult, QueryPort, QueryService};
use crate::ports::replication::{OutboundReplicationBatch, ReplicationPort};
use crate::timeseriesql::ast::*;
use crate::timeseriesql::to_clickhouse;

/// Max `(database, measurement)` entries in the query-side column mapping cache.
const COLUMN_MAPPING_CACHE_MAX: usize = 4096;

type ColumnMappingCacheEntry = (u64, ColumnMapping);
type ColumnMappingCache = HashMap<(String, String), ColumnMappingCacheEntry>;

#[derive(Clone)]
pub struct QueryServiceImpl {
    query_port: Arc<dyn QueryPort>,
    metadata: Arc<dyn MetadataPort>,
    wal: Arc<dyn crate::ports::wal::WalPort>,
    query_timeout_secs: u64,
    /// Native MergeTree sink: `DROP TABLE` when measurements / databases are dropped.
    points_sink: Arc<dyn crate::ports::points_sink::PointsSinkPort>,
    /// `(db, measurement)` → (schema fingerprint, mapping) for TimeseriesQL translation.
    column_mapping_cache: Arc<RwLock<ColumnMappingCache>>,
    /// When set, `SELECT ... INTO` writes replicate to peers after local WAL append.
    replication_port: Option<Arc<dyn ReplicationPort>>,
    node_id: u64,
    replication_config: ReplicationConfig,
    max_points_per_request: usize,
    mv_service: Arc<MaterializedViewService>,
}

impl QueryServiceImpl {
    pub fn new(
        query_port: Arc<dyn QueryPort>,
        metadata: Arc<dyn MetadataPort>,
        wal: Arc<dyn crate::ports::wal::WalPort>,
        query_timeout_secs: u64,
        points_sink: Arc<dyn crate::ports::points_sink::PointsSinkPort>,
    ) -> Self {
        let mv_service = Arc::new(MaterializedViewService::new(
            metadata.clone(),
            query_port.clone(),
            points_sink.clone(),
        ));
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
            max_points_per_request: 0,
            mv_service,
        }
    }

    #[must_use]
    pub fn with_max_points_per_request(mut self, max_points_per_request: usize) -> Self {
        self.max_points_per_request = max_points_per_request;
        self
    }

    async fn column_mapping_for(
        &self,
        db: &str,
        rp: &str,
        measurement: &str,
    ) -> Result<Option<ColumnMapping>, HyperbytedbError> {
        let meta = self.metadata.get_measurement(db, rp, measurement).await?;
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

    /// Execute one InfluxDB v1-style continuous query run at `now`.
    pub async fn execute_continuous_query(
        &self,
        cq: &mut ContinuousQueryDef,
        now: chrono::DateTime<chrono::Utc>,
    ) -> Result<CqRunResult, HyperbytedbError> {
        use metrics::{counter, histogram};
        use std::time::Instant;

        cq.normalize()?;
        if !should_run(now, cq) {
            return Err(HyperbytedbError::QueryParse(
                "continuous query is not due at this time".to_string(),
            ));
        }

        let started = Instant::now();
        let window = coverage_window(now, cq);
        let start_nanos = window.start.timestamp_nanos_opt().unwrap_or(0);
        let end_nanos = window.end.timestamp_nanos_opt().unwrap_or(0);

        let stmts = crate::timeseriesql::parse(&cq.query_text)?;
        let select_stmt = match stmts.into_iter().next() {
            Some(Statement::Select(s)) => s,
            _ => {
                return Err(HyperbytedbError::QueryParse(
                    "continuous query body must be a SELECT statement".to_string(),
                ));
            }
        };

        let prepared =
            to_clickhouse::prepare_cq_select(&select_stmt, start_nanos, end_nanos, !cq.is_advanced);

        tracing::debug!(
            cq = %cq.name,
            db = %cq.database,
            window_start = %window.start,
            window_end = %window.end,
            "executing continuous query"
        );

        let points_written = execute_select_into(self, &cq.database, &prepared, None).await?;

        cq.last_run_at = Some(now.to_rfc3339());
        self.metadata
            .store_continuous_query(&cq.database, &cq.name, cq)
            .await?;

        let duration_ms = started.elapsed().as_millis() as u64;
        counter!("hyperbytedb_cq_executions_total").increment(1);
        histogram!("hyperbytedb_cq_duration_ms").record(duration_ms as f64);
        histogram!("hyperbytedb_cq_window_secs")
            .record((end_nanos - start_nanos) as f64 / 1_000_000_000.0);

        Ok(CqRunResult {
            window,
            points_written,
            duration_ms,
        })
    }
}

/// Databases a statement actually touches for authorization, mirroring execution.
#[derive(Debug, Default)]
struct StatementAuthTargets {
    read: Vec<String>,
    write: Vec<String>,
    requires_admin: bool,
}

fn push_db_unique(targets: &mut Vec<String>, db: &str) {
    if !db.is_empty() && !targets.iter().any(|d| d == db) {
        targets.push(db.to_string());
    }
}

/// Resolve `ON <db>` (or statement-embedded database) the same way execution does.
fn resolve_on_db(stmt_db: &str, request_db: &str) -> String {
    if stmt_db.is_empty() {
        request_db.to_string()
    } else {
        stmt_db.to_string()
    }
}

fn statement_auth_targets(stmt: &Statement, request_db: &str) -> StatementAuthTargets {
    let mut targets = StatementAuthTargets::default();

    match stmt {
        Statement::CreateDatabase(_)
        | Statement::DropDatabase(_)
        | Statement::CreateUser { .. }
        | Statement::DropUser(_)
        | Statement::SetPassword { .. }
        | Statement::Grant { .. }
        | Statement::Revoke { .. } => {
            targets.requires_admin = true;
        }
        Statement::Select(s) => {
            push_db_unique(&mut targets.read, request_db);
            if let Some(into) = &s.into {
                let into_db = into.database.as_deref().unwrap_or(request_db);
                push_db_unique(&mut targets.write, into_db);
            }
            for source in &s.from {
                if let MeasurementSource::Concrete(m) = source
                    && let Some(src_db) = m.database.as_deref()
                {
                    push_db_unique(&mut targets.read, src_db);
                }
            }
        }
        Statement::ShowRetentionPolicies(db) => {
            push_db_unique(&mut targets.read, &resolve_on_db(db, request_db));
        }
        Statement::ShowMeasurements(s) => {
            push_db_unique(
                &mut targets.read,
                s.database.as_deref().unwrap_or(request_db),
            );
        }
        Statement::ShowTagKeys(s) => {
            push_db_unique(
                &mut targets.read,
                s.database.as_deref().unwrap_or(request_db),
            );
        }
        Statement::ShowTagValues(s) => {
            push_db_unique(
                &mut targets.read,
                s.database.as_deref().unwrap_or(request_db),
            );
        }
        Statement::ShowFieldKeys(s) => {
            push_db_unique(
                &mut targets.read,
                s.database.as_deref().unwrap_or(request_db),
            );
        }
        Statement::ShowSeries(s) => {
            push_db_unique(
                &mut targets.read,
                s.database.as_deref().unwrap_or(request_db),
            );
        }
        Statement::ShowDatabases
        | Statement::ShowUsers
        | Statement::ShowContinuousQueries
        | Statement::ShowMaterializedViews => {
            push_db_unique(&mut targets.read, request_db);
        }
        Statement::CreateRetentionPolicyStmt { db, .. }
        | Statement::AlterRetentionPolicyStmt { db, .. }
        | Statement::DropRetentionPolicyStmt { db, .. } => {
            push_db_unique(&mut targets.write, &resolve_on_db(db, request_db));
        }
        Statement::CreateContinuousQuery(cq) => {
            push_db_unique(&mut targets.write, &cq.database);
        }
        Statement::DropContinuousQuery { db, .. } => {
            push_db_unique(&mut targets.write, &resolve_on_db(db, request_db));
        }
        Statement::CreateMaterializedView(mv) => {
            push_db_unique(&mut targets.write, &mv.database);
        }
        Statement::DropMaterializedView { db, .. } => {
            push_db_unique(&mut targets.write, &resolve_on_db(db, request_db));
        }
        Statement::DropSeries(s) => {
            push_db_unique(
                &mut targets.write,
                s.database.as_deref().unwrap_or(request_db),
            );
        }
        Statement::Delete(_) | Statement::DropMeasurement { .. } => {
            push_db_unique(&mut targets.write, request_db);
        }
    }

    targets
}

pub(crate) fn check_authorization(
    user: &crate::domain::user::StoredUser,
    db: &str,
    stmt: &Statement,
) -> Result<(), HyperbytedbError> {
    if user.admin {
        return Ok(());
    }

    let targets = statement_auth_targets(stmt, db);
    if targets.requires_admin {
        return Err(HyperbytedbError::Forbidden(
            "admin privileges required".to_string(),
        ));
    }

    for read_db in &targets.read {
        if !user.can_read(read_db) {
            return Err(HyperbytedbError::Forbidden(format!(
                "not authorized to read from database '{read_db}'"
            )));
        }
    }
    for write_db in &targets.write {
        if !user.can_write(write_db) {
            return Err(HyperbytedbError::Forbidden(format!(
                "not authorized to write to database '{write_db}'"
            )));
        }
    }

    Ok(())
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
        | Statement::ShowContinuousQueries
        | Statement::ShowMaterializedViews => false,
        Statement::CreateDatabase(_)
        | Statement::DropDatabase(_)
        | Statement::DropMeasurement { .. }
        | Statement::DropSeries(_)
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
        | Statement::DropContinuousQuery { .. }
        | Statement::CreateMaterializedView(_)
        | Statement::DropMaterializedView { .. } => true,
    }
}

#[async_trait]
impl QueryService for QueryServiceImpl {
    async fn execute_query(
        &self,
        db: &str,
        query: &str,
        epoch: Option<&str>,
        retention_policy: Option<&str>,
        caller: Option<&crate::domain::user::StoredUser>,
    ) -> Result<QueryResponse, HyperbytedbError> {
        let timeout = std::time::Duration::from_secs(self.query_timeout_secs);
        let caller_owned = caller.cloned();
        let fut = async {
            let stmts = crate::timeseriesql::parse(query)?;
            let stmt_count = stmts.len();

            if let Some(ref user) = caller_owned {
                for stmt in &stmts {
                    check_authorization(user, db, stmt)?;
                }
            }

            let svc = Arc::new(self.clone());
            let db_arc = Arc::<str>::from(db);
            let epoch_arc = epoch.map(Arc::<str>::from);
            let rp_arc = retention_policy.map(Arc::<str>::from);

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
                        rp_arc.as_deref(),
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
                        let rp_arc = rp_arc.clone();
                        let stmt = &stmts[j];

                        async move {
                            execute_statement(
                                &svc,
                                db_arc.as_ref(),
                                stmt,
                                epoch_arc.as_deref(),
                                rp_arc.as_deref(),
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

            Ok(QueryResponse { results })
        };

        match tokio::time::timeout(timeout, fut).await {
            Ok(result) => result,
            Err(_) => Err(HyperbytedbError::QueryTimeout),
        }
    }

    async fn execute_continuous_query(
        &self,
        cq: &mut ContinuousQueryDef,
        now: chrono::DateTime<chrono::Utc>,
    ) -> Result<CqRunResult, HyperbytedbError> {
        QueryServiceImpl::execute_continuous_query(self, cq, now).await
    }
}

async fn execute_statement(
    svc: &QueryServiceImpl,
    db: &str,
    stmt: &Statement,
    epoch: Option<&str>,
    query_rp: Option<&str>,
    statement_id: u32,
) -> Result<StatementResult, HyperbytedbError> {
    // Verify database exists for DB-scoped statements.
    // Skip when db is empty (statements like CREATE/DROP RETENTION POLICY
    // or DROP DATABASE carry their own DB reference). DropDatabase has its
    // own explicit check below.
    if !db.is_empty() {
        match stmt {
            Statement::ShowDatabases
            | Statement::CreateDatabase(_)
            | Statement::DropDatabase(_)
            | Statement::CreateUser { .. }
            | Statement::DropUser(_)
            | Statement::SetPassword { .. }
            | Statement::ShowUsers
            | Statement::ShowContinuousQueries
            | Statement::ShowMaterializedViews => {}
            _ => {
                svc.metadata
                    .get_database(db)
                    .await?
                    .ok_or_else(|| HyperbytedbError::DatabaseNotFound(db.to_string()))?;
            }
        }
    }

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
            let rp = resolve_retention_policy_for_select(svc.metadata.as_ref(), db, None, query_rp)
                .await?;
            let names = list_measurements_for_rp(svc, db, &rp).await?;
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
            let default_rp =
                resolve_retention_policy_for_select(svc.metadata.as_ref(), db, None, query_rp)
                    .await?;
            let keys = if let Some(m) = s.from.as_ref() {
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
        Statement::ShowTagValues(s) => {
            let db = s.database.as_deref().unwrap_or(db);
            let measurement = s.from.as_ref().and_then(|m| m.name_str());
            let default_rp =
                resolve_retention_policy_for_select(svc.metadata.as_ref(), db, None, query_rp)
                    .await?;

            let all_tag_keys = if let (Some(m), Some(name)) = (s.from.as_ref(), measurement) {
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
                let values_list = if let (Some(m), Some(name)) = (s.from.as_ref(), measurement) {
                    if m.retention_policy.is_some() {
                        let query_db = m.database.as_deref().unwrap_or(db);
                        let rp = resolve_retention_policy(
                            svc.metadata.as_ref(),
                            query_db,
                            m.retention_policy.as_deref(),
                        )
                        .await?;
                        tag_values_for_measurement(svc, query_db, &rp, name, tag_key).await?
                    } else {
                        svc.metadata
                            .list_tag_values(db, &default_rp, tag_key, Some(name))
                            .await?
                    }
                } else {
                    svc.metadata
                        .list_tag_values(db, &default_rp, tag_key, measurement)
                        .await?
                };
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
            let rp = resolve_retention_policy_for_select(svc.metadata.as_ref(), db, None, query_rp)
                .await?;
            let mut field_values = Vec::new();
            let measurements: Vec<String> = if let Some(m) = measurement {
                vec![m.to_string()]
            } else {
                svc.metadata.list_measurements(db).await?
            };
            for m in &measurements {
                if let Some(meta) = svc.metadata.get_measurement(db, &rp, m).await? {
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
        Statement::CreateDatabase(stmt) => {
            svc.metadata.create_database_with(stmt).await?;
            Ok(StatementResult {
                statement_id,
                series: Some(vec![]),
                error: None,
            })
        }
        Statement::DropDatabase(name) => {
            svc.metadata
                .get_database(name)
                .await?
                .ok_or_else(|| HyperbytedbError::DatabaseNotFound(name.clone()))?;
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
            if let Err(e) = svc.mv_service.drop_all_in_database(name).await {
                tracing::warn!(
                    db = name,
                    error = %e,
                    "failed to cascade-drop materialized views for database"
                );
            }
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

            if select_stmt.into.is_some() {
                let count = execute_select_into(svc, db, select_stmt, epoch).await?;
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

            if select_stmt.from.is_empty() {
                return Err(HyperbytedbError::QueryParse(
                    "SELECT requires FROM clause".to_string(),
                ));
            }

            let mut all_series = Vec::new();
            for source in &select_stmt.from {
                let mut series = execute_select_from_source(
                    svc,
                    db,
                    select_stmt,
                    source,
                    time_min,
                    time_max,
                    epoch,
                    &group_by_tags,
                    query_rp,
                )
                .await?;
                all_series.append(&mut series);
            }

            // Apply SLIMIT/SOFFSET (series-level pagination)
            if select_stmt.slimit.is_some() || select_stmt.soffset.is_some() {
                let soffset = select_stmt.soffset.unwrap_or(0) as usize;
                let slimit = select_stmt.slimit.unwrap_or(u64::MAX) as usize;
                let len = all_series.len();
                let start = soffset.min(len);
                let end = (start + slimit).min(len);
                all_series = all_series[start..end].to_vec();
            }

            Ok(StatementResult {
                statement_id,
                series: Some(all_series),
                error: None,
            })
        }
        Statement::ShowSeries(s) => {
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
                resolve_retention_policy_for_select(svc.metadata.as_ref(), query_db, None, query_rp)
                    .await?
            };

            let mut values = Vec::new();
            for meas in &measurements {
                let series = svc.metadata.list_series(query_db, &rp, meas).await?;
                for (_, tags) in series {
                    let key = SeriesKey::new(meas, &tags);
                    values.push(vec![serde_json::Value::String(key.to_canonical())]);
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
        Statement::DropMeasurement { name, rp: stmt_rp } => {
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
            let del_rp = if let Some(rp) = query_rp {
                rp.to_string()
            } else {
                svc.metadata
                    .get_default_rp(db)
                    .await
                    .unwrap_or_else(|_| "autogen".to_string())
            };
            let predicate_sql = if let Some(ref cond) = del.condition {
                crate::application::predicate_sql::build_predicate_sql(
                    &svc.metadata,
                    db,
                    &del_rp,
                    &del.from,
                    cond,
                )
                .await?
            } else {
                String::new()
            };

            svc.metadata
                .store_tombstone(db, &del_rp, &del.from, &predicate_sql)
                .await?;

            tracing::debug!(
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
        Statement::ShowContinuousQueries => {
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
        Statement::DropContinuousQuery { name, db: cq_db } => {
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
        Statement::CreateMaterializedView(mv) => {
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
        Statement::ShowMaterializedViews => {
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
        Statement::DropMaterializedView { name, db: mv_db } => {
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
        Statement::ShowRetentionPolicies(rp_db) => {
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
                    let sgd_str = crate::domain::database::format_influx_duration(Some(
                        rp.shard_group_duration,
                    ));
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
                .unwrap_or_else(|| {
                    crate::domain::database::derive_shard_group_duration(std_duration)
                });
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
        Statement::AlterRetentionPolicyStmt {
            name,
            db: rp_db,
            duration,
            replication,
            shard_duration,
            is_default,
        } => {
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
        Statement::DropSeries(s) => {
            let target_db = s.database.as_deref().unwrap_or(db);
            let measurement = s.from.as_ref().and_then(|n| match n {
                MeasurementName::Name(n) => Some(n.clone()),
                MeasurementName::Regex(_) => None,
            });
            let rp = if let Some(rp) = query_rp {
                rp.to_string()
            } else {
                svc.metadata.get_default_rp(target_db).await?
            };
            let predicate_sql = if let Some(ref cond) = s.condition {
                let meas = measurement.as_deref().unwrap_or("");
                crate::application::predicate_sql::build_predicate_sql(
                    &svc.metadata,
                    target_db,
                    &rp,
                    meas,
                    cond,
                )
                .await?
            } else {
                String::new()
            };
            if measurement.is_some() || !predicate_sql.is_empty() {
                if let Some(ref meas) = measurement
                    && !predicate_sql.is_empty()
                {
                    svc.metadata
                        .store_tombstone(target_db, &rp, meas, &predicate_sql)
                        .await?;
                }
                svc.metadata
                    .delete_series_matching(target_db, &rp, measurement.as_deref(), &predicate_sql)
                    .await?;
            }
            Ok(StatementResult {
                statement_id,
                series: Some(vec![]),
                error: None,
            })
        }
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
        let out = vec![SeriesResult {
            name: measurement.to_string(),
            tags: None,
            columns,
            values,
            partial: None,
        }];
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

    let result: Vec<SeriesResult> = series_map
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
    // Wrapped translations (`ORDER BY time DESC` + fill, per-point transform
    // NULL filtering) hold the real query in an inner SELECT whose closing
    // `\n) ` line carries the outer clauses; splice predicates into the inner.
    if let Some(inner_and_tail) = sql.strip_prefix("SELECT * FROM (\n")
        && let Some(end) = inner_and_tail.rfind("\n) ")
    {
        let inner = inner_and_tail[..end].to_string();
        let tail = &inner_and_tail[end..];
        let injected = inject_tombstone_predicates(inner, tombstones);
        return format!("SELECT * FROM (\n{injected}{tail}");
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
async fn execute_select_from_source(
    svc: &QueryServiceImpl,
    db: &str,
    select_stmt: &SelectStatement,
    source: &MeasurementSource,
    time_min: Option<i64>,
    time_max: Option<i64>,
    epoch: Option<&str>,
    group_by_tags: &[String],
    query_rp: Option<&str>,
) -> Result<Vec<SeriesResult>, HyperbytedbError> {
    match source {
        MeasurementSource::Concrete(m) => match &m.name {
            MeasurementName::Regex(pattern) => {
                let query_db = m.database.as_deref().unwrap_or(db);
                let rp = resolve_retention_policy_for_select(
                    svc.metadata.as_ref(),
                    query_db,
                    m.retention_policy.as_deref(),
                    query_rp,
                )
                .await?;
                let measurements = svc.metadata.list_measurements(query_db).await?;
                let matching: Vec<_> = {
                    let re = regex_pattern_matches(pattern);
                    measurements.into_iter().filter(|m| re(m)).collect()
                };
                let futs: Vec<_> = matching
                    .iter()
                    .map(|meas_name| {
                        execute_measurement_query(
                            svc,
                            query_db,
                            &rp,
                            meas_name,
                            select_stmt,
                            time_min,
                            time_max,
                            epoch,
                            group_by_tags,
                        )
                    })
                    .collect();
                let results = futures::future::join_all(futs).await;
                let mut combined = Vec::new();
                for result in results {
                    combined.append(&mut result?);
                }
                Ok(combined)
            }
            MeasurementName::Name(name) => {
                let query_db = m.database.as_deref().unwrap_or(db);
                let rp = resolve_retention_policy_for_select(
                    svc.metadata.as_ref(),
                    query_db,
                    m.retention_policy.as_deref(),
                    query_rp,
                )
                .await?;
                execute_measurement_query(
                    svc,
                    query_db,
                    &rp,
                    name,
                    select_stmt,
                    time_min,
                    time_max,
                    epoch,
                    group_by_tags,
                )
                .await
            }
        },
        MeasurementSource::Subquery(sub_stmt) => {
            let sub_meas = sub_stmt
                .from
                .first()
                .and_then(|s| match s {
                    MeasurementSource::Concrete(m) => Some(m),
                    _ => None,
                })
                .ok_or_else(|| {
                    HyperbytedbError::QueryParse("subquery requires measurement".to_string())
                })?;
            let sub_source = sub_meas.name_str().ok_or_else(|| {
                HyperbytedbError::QueryParse("subquery requires measurement".to_string())
            })?;
            let query_db = sub_meas.database.as_deref().unwrap_or(db);
            let rp = resolve_retention_policy_for_select(
                svc.metadata.as_ref(),
                query_db,
                sub_meas.retention_policy.as_deref(),
                query_rp,
            )
            .await?;
            let sub_mapping = svc.column_mapping_for(query_db, &rp, sub_source).await?;
            let table = quoted_table_name(query_db, &rp, sub_source);
            let sub_series_table = quoted_series_table_name(query_db, &rp, sub_source);

            let sub_effective_mapping = if let Some(ref mapping) = sub_mapping {
                let fact_cols = fact_table_columns(svc, query_db, &rp, sub_source).await?;
                if mapping.field_names.iter().any(|f| !fact_cols.contains(f)) {
                    let mut reconciled = mapping.clone();
                    reconciled.field_names = fact_cols.iter().cloned().collect();
                    for col in &fact_cols {
                        reconciled
                            .field_rollups
                            .entry(col.clone())
                            .or_insert(crate::domain::rollup::RollupCombine::Last);
                    }
                    Some(reconciled)
                } else {
                    sub_mapping.clone()
                }
            } else {
                None
            };

            let sub_series_tag_columns: Vec<String> =
                series_table_columns(svc, query_db, &rp, sub_source)
                    .await?
                    .into_iter()
                    .collect();
            let sub_series_join =
                sub_effective_mapping
                    .as_ref()
                    .map(|_| to_clickhouse::SeriesJoin {
                        table: &sub_series_table,
                        force: false,
                        tag_columns: &sub_series_tag_columns,
                    });
            let sub_tag_keys = tag_keys_from_mapping(sub_effective_mapping.as_ref());
            let (sub_stmt_expanded, _) = select_with_expanded_group_by(sub_stmt, &sub_tag_keys);
            let (select_stmt_expanded, resolved_group_by_tags) =
                select_with_expanded_group_by(select_stmt, &sub_tag_keys);
            let sub_sql = to_clickhouse::translate_native_table(
                &sub_stmt_expanded,
                &table,
                sub_effective_mapping.as_ref(),
                sub_series_join,
                Some((time_min, time_max)),
            )?;
            // A GROUP BY time() inner query exposes its bucket column under the
            // internal `__time` alias; as a FROM source it must expose `time`
            // so the outer query's bucketing/ordering can reference it.
            let sub_sql = to_clickhouse::rename_time_bucket_alias(&sub_sql);
            let outer_sql = to_clickhouse::translate_with_source(
                &select_stmt_expanded,
                &format!("({sub_sql})"),
            )?;
            let raw = svc.query_port.execute_sql(&outer_sql).await?;
            parse_json_each_row_to_series(&raw, sub_source, epoch, &resolved_group_by_tags)
        }
    }
}

/// Execute TimeseriesQL for a single measurement against the backing MergeTree table.
#[allow(clippy::too_many_arguments)]
async fn execute_measurement_query(
    svc: &QueryServiceImpl,
    db: &str,
    rp: &str,
    measurement: &str,
    stmt: &SelectStatement,
    time_min: Option<i64>,
    time_max: Option<i64>,
    epoch: Option<&str>,
    _group_by_tags: &[String],
) -> Result<Vec<SeriesResult>, HyperbytedbError> {
    let column_mapping = svc.column_mapping_for(db, rp, measurement).await?;
    let tag_keys = tag_keys_from_mapping(column_mapping.as_ref());
    let (effective_stmt, resolved_group_by_tags) = select_with_expanded_group_by(stmt, &tag_keys);

    // Tombstone predicates (spliced into WHERE below) may reference tag columns,
    // which only exist on the series-rejoin inline view — so force the join when
    // any tombstone is present for this measurement.
    let tombstones = svc.metadata.list_tombstones(db, rp, measurement).await?;

    let table = quoted_table_name(db, rp, measurement);
    let series_table = quoted_series_table_name(db, rp, measurement);

    // Reconcile the column mapping's field names with the fact table's actual columns.
    // MV destinations (e.g. from CREATE MATERIALIZED VIEW) may rename fields via aliases,
    // so the source column mapping's field names may not match the table's column names.
    let effective_mapping = if let Some(ref mapping) = column_mapping {
        let fact_cols = fact_table_columns(svc, db, rp, measurement).await?;
        if mapping.field_names.iter().any(|f| !fact_cols.contains(f)) {
            let mut reconciled = mapping.clone();
            reconciled.field_names = fact_cols.iter().cloned().collect();
            for col in &fact_cols {
                reconciled
                    .field_rollups
                    .entry(col.clone())
                    .or_insert(crate::domain::rollup::RollupCombine::Last);
            }
            Some(reconciled)
        } else {
            column_mapping.clone()
        }
    } else {
        None
    };

    let series_tag_columns: Vec<String> = series_table_columns(svc, db, rp, measurement)
        .await?
        .into_iter()
        .collect();
    let series_join = effective_mapping
        .as_ref()
        .map(|_| to_clickhouse::SeriesJoin {
            table: &series_table,
            force: !tombstones.is_empty(),
            tag_columns: &series_tag_columns,
        });
    let mut sql = to_clickhouse::translate_native_table(
        &effective_stmt,
        &table,
        effective_mapping.as_ref(),
        series_join,
        Some((time_min, time_max)),
    )?;
    sql = inject_tombstone_predicates(sql, &tombstones);
    let raw = svc.query_port.execute_sql(&sql).await?;
    parse_json_each_row_to_series(&raw, measurement, epoch, &resolved_group_by_tags)
}

fn tag_keys_from_mapping(
    mapping: Option<&crate::domain::column_mapping::ColumnMapping>,
) -> Vec<String> {
    let mut keys: Vec<String> = mapping
        .map(|m| m.tag_keys.iter().cloned().collect())
        .unwrap_or_default();
    keys.sort();
    keys
}

fn select_with_expanded_group_by(
    stmt: &crate::timeseriesql::ast::SelectStatement,
    tag_keys: &[String],
) -> (crate::timeseriesql::ast::SelectStatement, Vec<String>) {
    let Some(ref gb) = stmt.group_by else {
        return (stmt.clone(), Vec::new());
    };
    let (expanded_gb, resolved_tags) = gb.expand_all_tags(tag_keys);
    let mut effective = stmt.clone();
    effective.group_by = Some(expanded_gb);
    (effective, resolved_tags)
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

async fn resolve_retention_policy(
    metadata: &dyn MetadataPort,
    db: &str,
    retention_policy: Option<&str>,
) -> Result<String, HyperbytedbError> {
    resolve_retention_policy_for_select(metadata, db, retention_policy, None).await
}

/// Resolve the retention policy for a SELECT. Measurement qualification in the
/// FROM clause wins over the HTTP/CLI `rp` parameter (InfluxDB semantics).
async fn resolve_retention_policy_for_select(
    metadata: &dyn MetadataPort,
    db: &str,
    measurement_rp: Option<&str>,
    query_rp: Option<&str>,
) -> Result<String, HyperbytedbError> {
    if let Some(rp) = measurement_rp.filter(|s| !s.is_empty()) {
        return Ok(normalize_rp_name(metadata, db, rp).await);
    }
    if let Some(rp) = query_rp.filter(|s| !s.is_empty()) {
        return Ok(normalize_rp_name(metadata, db, rp).await);
    }
    if db.is_empty() {
        return Ok("autogen".to_string());
    }
    metadata.get_default_rp(db).await
}

/// Map InfluxDB's conventional default RP name to this database's default.
async fn normalize_rp_name(metadata: &dyn MetadataPort, db: &str, rp: &str) -> String {
    if rp == "default"
        && let Ok(default) = metadata.get_default_rp(db).await
    {
        return default;
    }
    rp.to_string()
}

async fn list_measurements_for_rp(
    svc: &QueryServiceImpl,
    db: &str,
    rp: &str,
) -> Result<Vec<String>, HyperbytedbError> {
    use crate::domain::chdb_naming::unquoted_table_name;

    let all = svc.metadata.list_measurements(db).await?;
    let mut names = Vec::new();
    for measurement in all {
        let table = unquoted_table_name(db, rp, &measurement);
        let sql = format!(
            "SELECT count() FROM system.tables WHERE database = 'default' AND name = '{table}' FORMAT TabSeparated"
        );
        if svc.query_port.execute_sql(&sql).await?.trim() == "1" {
            names.push(measurement);
        }
    }
    Ok(names)
}

async fn tag_keys_for_measurement(
    svc: &QueryServiceImpl,
    db: &str,
    rp: &str,
    measurement: &str,
) -> Result<Vec<String>, HyperbytedbError> {
    let series = svc.metadata.list_series(db, rp, measurement).await?;
    if !series.is_empty() {
        let mut keys: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
        for (_, tags) in series {
            keys.extend(tags.keys().cloned());
        }
        let mut result: Vec<_> = keys.into_iter().collect();
        result.sort();
        return Ok(result);
    }

    tag_keys_from_series_table(svc, db, rp, measurement).await
}

async fn tag_keys_from_series_table(
    svc: &QueryServiceImpl,
    db: &str,
    rp: &str,
    measurement: &str,
) -> Result<Vec<String>, HyperbytedbError> {
    let mapping = svc.column_mapping_for(db, rp, measurement).await?;
    let Some(mapping) = mapping else {
        return Ok(Vec::new());
    };

    let phys_cols = series_table_columns(svc, db, rp, measurement).await?;
    let mut keys: Vec<String> = mapping
        .tag_keys
        .iter()
        .filter(|logical| phys_cols.contains(&mapping.tag_column_name(logical)))
        .cloned()
        .collect();
    keys.sort();
    Ok(keys)
}

async fn tag_values_for_measurement(
    svc: &QueryServiceImpl,
    db: &str,
    rp: &str,
    measurement: &str,
    tag_key: &str,
) -> Result<Vec<String>, HyperbytedbError> {
    let series = svc.metadata.list_series(db, rp, measurement).await?;
    if !series.is_empty() {
        let mut values: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
        for (_, tags) in series {
            if let Some(v) = tags.get(tag_key) {
                values.insert(v.clone());
            }
        }
        let mut result: Vec<_> = values.into_iter().collect();
        result.sort();
        return Ok(result);
    }

    let mapping = svc.column_mapping_for(db, rp, measurement).await?;
    let Some(mapping) = mapping else {
        return Ok(Vec::new());
    };
    let phys = mapping.tag_column_name(tag_key);
    if !series_table_columns(svc, db, rp, measurement)
        .await?
        .contains(&phys)
    {
        return Ok(Vec::new());
    }

    let series_table = quoted_series_table_name(db, rp, measurement);
    let phys_col = quote_backticks(&phys);
    let sql = format!(
        "SELECT DISTINCT {phys_col} FROM {series_table} WHERE {phys_col} != '' ORDER BY {phys_col} FORMAT TabSeparated"
    );
    let raw = svc.query_port.execute_sql(&sql).await?;
    Ok(raw
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(str::to_string)
        .collect())
}

/// Query the non-system column names that exist in the fact table for a given
/// (db, rp, measurement). Excludes columns managed by the engine.
async fn fact_table_columns(
    svc: &QueryServiceImpl,
    db: &str,
    rp: &str,
    measurement: &str,
) -> Result<Vec<String>, HyperbytedbError> {
    use crate::domain::chdb_naming::unquoted_table_name;
    let table = unquoted_table_name(db, rp, measurement);
    let sql = format!(
        "SELECT name FROM system.columns WHERE table = '{}' AND name NOT IN ('series_id', 'time', 'origin_node_id', 'ingest_seq') FORMAT TabSeparated",
        table.replace('\'', "''")
    );
    let raw = svc.query_port.execute_sql(&sql).await?;
    Ok(raw
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(str::to_string)
        .collect())
}

async fn series_table_columns(
    svc: &QueryServiceImpl,
    db: &str,
    rp: &str,
    measurement: &str,
) -> Result<std::collections::HashSet<String>, HyperbytedbError> {
    let table = unquoted_series_table_name(db, rp, measurement);
    let sql = format!(
        "SELECT name FROM system.columns WHERE table = '{}' AND name != 'series_id' FORMAT TabSeparated",
        table.replace('\'', "''")
    );
    let raw = svc.query_port.execute_sql(&sql).await?;
    Ok(raw
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(str::to_string)
        .collect())
}

async fn write_series_as_points(
    svc: &QueryServiceImpl,
    db: &str,
    rp: &str,
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
        crate::application::ingest_metadata::validate_point_count(
            all_points.len(),
            svc.max_points_per_request,
        )?;
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
            ..Default::default()
        };
        svc.metadata.register_measurement(db, rp, &meta).await?;

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
            retention_policy: rp.to_string(),
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
                    retention_policy: rp.to_string(),
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

/// Run `SELECT ... INTO ...` and return the number of points written.
async fn execute_select_into(
    svc: &QueryServiceImpl,
    db: &str,
    select_stmt: &SelectStatement,
    epoch: Option<&str>,
) -> Result<u64, HyperbytedbError> {
    let into_target = select_stmt.into.as_ref().ok_or_else(|| {
        HyperbytedbError::QueryParse("SELECT INTO requires INTO clause".to_string())
    })?;
    let MeasurementName::Name(target_name) = &into_target.name else {
        return Err(HyperbytedbError::QueryParse(
            "SELECT INTO does not support regex destination measurements".to_string(),
        ));
    };

    let source = select_stmt
        .from
        .first()
        .ok_or_else(|| HyperbytedbError::QueryParse("SELECT requires FROM clause".to_string()))?;

    let (source_db, source_rp, source_name) = match source {
        MeasurementSource::Concrete(m) => {
            let MeasurementName::Name(name) = &m.name else {
                return Err(HyperbytedbError::QueryParse(
                    "SELECT INTO does not support regex source measurements".to_string(),
                ));
            };
            let query_db = m.database.as_deref().unwrap_or(db);
            let rp = resolve_retention_policy(
                svc.metadata.as_ref(),
                query_db,
                m.retention_policy.as_deref(),
            )
            .await?;
            (query_db.to_string(), rp, name.clone())
        }
        MeasurementSource::Subquery(_) => {
            return Err(HyperbytedbError::QueryParse(
                "SELECT INTO does not support subquery sources".to_string(),
            ));
        }
    };

    let into_db = into_target.database.as_deref().unwrap_or(db);
    let dest_rp = resolve_retention_policy(
        svc.metadata.as_ref(),
        into_db,
        into_target.retention_policy.as_deref(),
    )
    .await?;

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

    let (time_min, time_max) = to_clickhouse::extract_time_bounds(select_stmt.condition.as_ref());

    let all_series = execute_measurement_query(
        svc,
        &source_db,
        &source_rp,
        &source_name,
        select_stmt,
        time_min,
        time_max,
        epoch,
        &group_by_tags,
    )
    .await?;

    write_series_as_points(svc, into_db, &dest_rp, target_name, &all_series).await
}

fn reconstruct_cq_text(cq: &ContinuousQueryDef) -> String {
    let mut out = format!(
        r#"CREATE CONTINUOUS QUERY "{}" ON "{}""#,
        cq.name, cq.database
    );
    if cq.resample_every_secs.is_some() || cq.resample_for_secs.is_some() {
        out.push_str(" RESAMPLE");
        if let Some(every) = cq.resample_every_secs {
            out.push_str(&format!(" EVERY {}s", every));
        }
        if let Some(for_secs) = cq.resample_for_secs {
            out.push_str(&format!(" FOR {}s", for_secs));
        }
    }
    out.push_str(" BEGIN ");
    out.push_str(&cq.query_text);
    if !cq.query_text.trim().ends_with(';') {
        out.push(' ');
    }
    out.push_str("END");
    out
}

#[cfg(test)]
mod auth_tests {
    use std::collections::HashMap;

    use super::check_authorization;
    use crate::domain::user::{DatabasePrivilege, StoredUser};
    use crate::error::HyperbytedbError;
    use crate::timeseriesql::ddl_parser::parse_ddl_statement;
    use crate::timeseriesql::parser::parse_query;

    fn writer_on_mine() -> StoredUser {
        StoredUser {
            password_hash: String::new(),
            admin: false,
            created_at: String::new(),
            privileges: HashMap::from([("mine".to_string(), DatabasePrivilege::Write)]),
        }
    }

    fn assert_forbidden(user: &StoredUser, request_db: &str, query: &str) {
        let stmt = parse_ddl_statement(query)
            .or_else(|_| parse_query(query).map(|mut v| v.remove(0)))
            .expect("parse query");
        let err = check_authorization(user, request_db, &stmt).unwrap_err();
        assert!(
            matches!(err, HyperbytedbError::Forbidden(_)),
            "expected Forbidden, got {err:?}"
        );
    }

    fn assert_allowed(user: &StoredUser, request_db: &str, query: &str) {
        let stmt = parse_ddl_statement(query)
            .or_else(|_| parse_query(query).map(|mut v| v.remove(0)))
            .expect("parse query");
        check_authorization(user, request_db, &stmt).expect("should be authorized");
    }

    #[test]
    fn on_clause_write_targets_statement_database() {
        let user = writer_on_mine();
        assert_forbidden(
            &user,
            "mine",
            "ALTER RETENTION POLICY autogen ON victim DURATION 1h",
        );
        assert_forbidden(
            &user,
            "mine",
            "CREATE RETENTION POLICY extra ON victim DURATION 1h REPLICATION 1",
        );
        assert_forbidden(&user, "mine", "DROP RETENTION POLICY autogen ON victim");
        assert_forbidden(&user, "mine", "DROP SERIES FROM cpu ON victim");
    }

    #[test]
    fn on_clause_read_targets_statement_database() {
        let user = StoredUser {
            privileges: HashMap::from([("mine".to_string(), DatabasePrivilege::Read)]),
            ..writer_on_mine()
        };
        assert_forbidden(&user, "mine", "SHOW MEASUREMENTS ON victim");
        assert_forbidden(&user, "mine", "SHOW RETENTION POLICIES ON victim");
        assert_forbidden(&user, "mine", "SHOW TAG KEYS ON victim FROM cpu");
    }

    #[test]
    fn same_database_on_clause_still_allowed() {
        let user = writer_on_mine();
        assert_allowed(
            &user,
            "mine",
            "ALTER RETENTION POLICY autogen ON mine DURATION 1h",
        );
    }
}
