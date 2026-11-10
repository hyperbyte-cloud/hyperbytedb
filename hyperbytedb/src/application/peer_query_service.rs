use async_trait::async_trait;
use std::sync::Arc;

use crate::application::materialized_view_service::def_from_statement;
use crate::application::predicate_sql::build_predicate_sql;
use crate::application::query_service::check_authorization;
use crate::application::shard_metadata_scatter::scatter_measurement_meta;
use crate::application::shard_routing::ShardRoutingContext;
use crate::domain::cluster::raft_request::ClusterRequest;
use crate::domain::cluster::types::MutationRequest;
use crate::domain::database::{RetentionPolicy, retention_policy_from_create};
use crate::domain::materialized_view::MaterializedViewDef;
use crate::domain::measurement::MeasurementMeta;
use crate::domain::query_result::{QueryResponse, StatementResult};
use crate::error::HyperbytedbError;
use crate::ports::consensus::ConsensusPort;
use crate::ports::metadata::ContinuousQueryDef;
use crate::ports::metadata::MetadataPort;
use crate::ports::query::QueryService;
use crate::ports::replication::ReplicationPort;
use crate::timeseriesql::ast::{
    CreateMaterializedViewStatement, DurationUnit, MeasurementName, MeasurementSource,
    RetentionPolicyChange, Statement,
};

/// Wraps an inner QueryService, intercepting mutations (CREATE/DROP DATABASE,
/// DELETE, CREATE/DROP CONTINUOUS QUERY) and replicating them to peers after
/// local application. When a Raft instance is available, schema mutations
/// are routed through Raft consensus instead of direct peer replication.
/// Followers automatically forward schema mutations to the current leader.
pub struct PeerQueryService {
    inner: Arc<dyn QueryService>,
    metadata: Arc<dyn MetadataPort>,
    replication_port: Arc<dyn ReplicationPort>,
    consensus: std::sync::OnceLock<Arc<dyn ConsensusPort>>,
    shard_routing: std::sync::OnceLock<Arc<ShardRoutingContext>>,
}

impl PeerQueryService {
    pub fn new(
        inner: Arc<dyn QueryService>,
        metadata: Arc<dyn MetadataPort>,
        replication_port: Arc<dyn ReplicationPort>,
    ) -> Self {
        Self {
            inner,
            metadata,
            replication_port,
            consensus: std::sync::OnceLock::new(),
            shard_routing: std::sync::OnceLock::new(),
        }
    }

    /// Sharding context, for statements that need metadata held on other
    /// nodes (CREATE MATERIALIZED VIEW's source schema).
    pub fn set_shard_routing(&self, ctx: Arc<ShardRoutingContext>) {
        let _ = self.shard_routing.set(ctx);
    }

    /// Route schema mutations through consensus. Called after Raft
    /// initialization completes.
    pub fn set_consensus(&self, consensus: Arc<dyn ConsensusPort>) {
        if self.consensus.set(consensus).is_err() {
            tracing::error!(
                "PeerQueryService::set_consensus called more than once; ignoring duplicate"
            );
        }
    }

    async fn replicate_mutation(&self, req: MutationRequest) -> Result<(), HyperbytedbError> {
        if let Some(consensus) = self.consensus.get() {
            let resp = consensus
                .propose(ClusterRequest::SchemaMutation(Box::new(req)))
                .await?;
            if resp.ok {
                return Ok(());
            }
            Err(HyperbytedbError::Internal(crate::error::ChainedError::new(
                resp.message
                    .unwrap_or_else(|| "raft schema mutation apply failed".to_string()),
            )))
        } else {
            self.replication_port
                .clone()
                .replicate_mutation_sync(req)
                .await
        }
    }

    async fn execute_inner_statement(
        &self,
        db: &str,
        stmt_query: &str,
        epoch: Option<&str>,
        retention_policy: Option<&str>,
        caller: Option<&crate::domain::user::StoredUser>,
        statement_id: u32,
    ) -> StatementResult {
        execute_or_error(
            self.inner
                .execute_query(db, stmt_query, epoch, retention_policy, caller)
                .await,
            statement_id,
        )
    }

    async fn execute_raft_mutation(
        &self,
        db: &str,
        retention_policy: Option<&str>,
        caller: Option<&crate::domain::user::StoredUser>,
        statement_id: u32,
        stmt: &Statement,
    ) -> Result<StatementResult, HyperbytedbError> {
        if let Some(user) = caller
            && let Err(e) = check_authorization(user, db, stmt)
        {
            return Ok(StatementResult {
                statement_id,
                series: None,
                error: Some(e.to_string()),
            });
        }
        match mutation_request_from_statement(
            stmt,
            db,
            retention_policy,
            &self.metadata,
            self.shard_routing.get(),
        )
        .await
        {
            Ok(req) => {
                self.replicate_mutation(req).await?;
                Ok(StatementResult {
                    statement_id,
                    series: Some(vec![]),
                    error: None,
                })
            }
            Err(e) => Ok(StatementResult {
                statement_id,
                series: None,
                error: Some(e.to_string()),
            }),
        }
    }
}

fn extract_mv_source_rp(mv: &CreateMaterializedViewStatement) -> Option<String> {
    mv.query.from.first().and_then(|s| match s {
        MeasurementSource::Concrete(m) => m.retention_policy.clone(),
        _ => None,
    })
}

fn extract_mv_dest_rp(mv: &CreateMaterializedViewStatement) -> Option<String> {
    mv.query
        .into
        .as_ref()
        .and_then(|m| m.retention_policy.clone())
}

/// The source measurement's schema, sent with the CREATE so a node that has
/// no rows for it yet can still create the view. With sharding this node may
/// have none either, so it asks the source's region owners.
async fn mv_source_meta(
    metadata: &Arc<dyn MetadataPort>,
    shard_routing: Option<&Arc<ShardRoutingContext>>,
    def: &MaterializedViewDef,
) -> Result<Option<Box<MeasurementMeta>>, HyperbytedbError> {
    let (db, rp, m) = (&def.source_db, &def.source_rp, &def.source_measurement);
    let meta = match shard_routing {
        Some(ctx) => scatter_measurement_meta(ctx, metadata.as_ref(), db, rp, m).await?,
        None => metadata.get_measurement(db, rp, m).await?,
    };
    Ok(meta.map(Box::new))
}

async fn resolve_mv_source_rp(
    metadata: &Arc<dyn MetadataPort>,
    mv: &CreateMaterializedViewStatement,
) -> Result<String, HyperbytedbError> {
    if let Some(rp) = extract_mv_source_rp(mv) {
        return Ok(rp);
    }
    let source_db = mv
        .query
        .from
        .first()
        .and_then(|s| match s {
            MeasurementSource::Concrete(m) => {
                Some(m.database.as_deref().unwrap_or(&mv.database).to_string())
            }
            _ => None,
        })
        .unwrap_or_else(|| mv.database.clone());
    Ok(metadata
        .get_default_rp(&source_db)
        .await
        .unwrap_or_else(|_| "autogen".to_string()))
}

async fn resolve_mv_dest_rp(
    metadata: &Arc<dyn MetadataPort>,
    mv: &CreateMaterializedViewStatement,
) -> Result<String, HyperbytedbError> {
    if let Some(rp) = extract_mv_dest_rp(mv) {
        return Ok(rp);
    }
    let dest_db = mv
        .query
        .into
        .as_ref()
        .map(|m| m.database.as_deref().unwrap_or(&mv.database).to_string())
        .unwrap_or_else(|| mv.database.clone());
    Ok(metadata
        .get_default_rp(&dest_db)
        .await
        .unwrap_or_else(|_| "autogen".to_string()))
}

async fn mutation_request_from_statement(
    stmt: &Statement,
    db: &str,
    retention_policy: Option<&str>,
    metadata: &Arc<dyn MetadataPort>,
    shard_routing: Option<&Arc<ShardRoutingContext>>,
) -> Result<MutationRequest, HyperbytedbError> {
    match stmt {
        Statement::CreateDatabase(stmt) => Ok(MutationRequest::CreateDatabase {
            name: stmt.name.clone(),
            rp: retention_policy_from_create(stmt),
        }),
        Statement::DropDatabase(name) => Ok(MutationRequest::DropDatabase(name.clone())),
        Statement::Delete(del) => {
            let del_rp = match retention_policy {
                Some(rp) => rp.to_string(),
                None => metadata.get_default_rp(db).await?,
            };
            let predicate_sql = crate::application::predicate_sql::build_delete_predicate(
                metadata,
                db,
                &del_rp,
                &del.from,
                del.condition.as_ref(),
            )
            .await?;
            Ok(MutationRequest::Delete {
                database: db.to_string(),
                rp: del_rp,
                measurement: del.from.clone(),
                predicate_sql,
            })
        }
        Statement::CreateContinuousQuery(cq) => {
            let definition = ContinuousQueryDef::from_create(cq)?;
            Ok(MutationRequest::CreateContinuousQuery {
                database: cq.database.clone(),
                name: cq.name.clone(),
                definition,
            })
        }
        Statement::DropContinuousQuery { name, db: cq_db } => {
            let target_db = if cq_db.is_empty() { db } else { cq_db };
            Ok(MutationRequest::DropContinuousQuery {
                database: target_db.to_string(),
                name: name.clone(),
            })
        }
        Statement::CreateMaterializedView(mv) => {
            let source_rp = resolve_mv_source_rp(metadata, mv).await?;
            let dest_rp = resolve_mv_dest_rp(metadata, mv).await?;
            let definition = def_from_statement(mv, &source_rp, &dest_rp)?;
            let source_meta = mv_source_meta(metadata, shard_routing, &definition).await?;
            Ok(MutationRequest::CreateMaterializedView {
                database: mv.database.clone(),
                name: mv.name.clone(),
                definition,
                source_meta,
            })
        }
        Statement::DropMaterializedView { name, db: mv_db } => {
            let target_db = if mv_db.is_empty() { db } else { mv_db };
            Ok(MutationRequest::DropMaterializedView {
                database: target_db.to_string(),
                name: name.clone(),
            })
        }
        Statement::CreateRetentionPolicyStmt {
            name,
            db,
            duration,
            replication,
            shard_duration,
            is_default,
        } => {
            let dur = duration
                .as_ref()
                .map(|d| std::time::Duration::from_nanos(d.to_nanos() as u64));
            let shard_dur = shard_duration
                .as_ref()
                .map(|d| std::time::Duration::from_nanos(d.to_nanos() as u64))
                .unwrap_or_else(|| crate::domain::database::derive_shard_group_duration(dur));
            Ok(MutationRequest::CreateRetentionPolicy {
                db: db.clone(),
                rp: RetentionPolicy {
                    name: name.clone(),
                    duration: dur,
                    shard_group_duration: shard_dur,
                    replication_factor: *replication,
                    is_default: *is_default,
                },
            })
        }
        Statement::DropRetentionPolicyStmt { name, db } => {
            Ok(MutationRequest::DropRetentionPolicy {
                db: db.clone(),
                name: name.clone(),
            })
        }
        Statement::CreateUser {
            username,
            password,
            admin,
        } => {
            let password_hash = hash_password_for_replication(password)?;
            Ok(MutationRequest::CreateUser {
                username: username.clone(),
                password_hash,
                admin: *admin,
            })
        }
        Statement::DropUser(username) => Ok(MutationRequest::DropUser(username.clone())),
        Statement::SetPassword { username, password } => {
            let password_hash = hash_password_for_replication(password)?;
            Ok(MutationRequest::SetPassword {
                username: username.clone(),
                password_hash,
            })
        }
        Statement::AlterRetentionPolicyStmt {
            name,
            db,
            duration,
            replication,
            shard_duration,
            is_default,
        } => Ok(MutationRequest::AlterRetentionPolicy {
            db: db.clone(),
            name: name.clone(),
            change: RetentionPolicyChange {
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
            },
        }),
        Statement::DropSeries(s) => {
            let target_db = s.database.as_deref().unwrap_or(db);
            let measurement = s.from.as_ref().and_then(|n| match n {
                MeasurementName::Name(n) => Some(n.clone()),
                MeasurementName::Regex(_) => None,
            });
            let drop_rp = match retention_policy {
                Some(rp) => rp.to_string(),
                None => metadata.get_default_rp(target_db).await?,
            };
            let predicate_sql = match (measurement.as_deref(), s.condition.as_ref()) {
                // Replicas delete the rows this selects.
                (Some(meas), cond) => {
                    crate::application::predicate_sql::build_delete_predicate(
                        metadata, target_db, &drop_rp, meas, cond,
                    )
                    .await?
                }
                (None, Some(cond)) => {
                    build_predicate_sql(metadata, target_db, &drop_rp, "", cond).await?
                }
                (None, None) => String::new(),
            };
            Ok(MutationRequest::DropSeries {
                database: target_db.to_string(),
                rp: drop_rp,
                measurement,
                predicate_sql,
            })
        }
        Statement::DropMeasurement { name, rp: stmt_rp } => {
            let drop_rp = if let Some(rp) = stmt_rp {
                rp.clone()
            } else {
                metadata.get_default_rp(db).await?
            };
            Ok(MutationRequest::DropMeasurement {
                database: db.to_string(),
                rp: drop_rp,
                name: name.clone(),
            })
        }
        Statement::Grant {
            username,
            database,
            privilege,
        } => Ok(MutationRequest::Grant {
            username: username.clone(),
            database: database.clone(),
            privilege: *privilege,
        }),
        Statement::Revoke {
            username,
            database,
            privilege,
        } => Ok(MutationRequest::Revoke {
            username: username.clone(),
            database: database.clone(),
            privilege: *privilege,
        }),
        Statement::SetSetting { name, value } => Ok(MutationRequest::SetSetting {
            name: name.clone(),
            value: crate::application::runtime_settings::validate(name, value.clone())?,
        }),
        _ => Err(HyperbytedbError::Internal(crate::error::ChainedError::new(
            "not a cluster mutation statement",
        ))),
    }
}

/// The database a cluster statement runs against locally: a retention
/// policy statement's own `ON <db>`, otherwise the request's.
fn local_db<'a>(stmt: &'a Statement, db: &'a str) -> &'a str {
    match stmt {
        Statement::CreateRetentionPolicyStmt { db, .. }
        | Statement::DropRetentionPolicyStmt { db, .. }
        | Statement::AlterRetentionPolicyStmt { db, .. } => db,
        _ => db,
    }
}

/// Whether failing to build `stmt`'s mutation (after it applied locally) is
/// reported on the statement rather than failing the whole request: a bad
/// continuous query or view definition, or a password that cannot be hashed.
fn build_error_is_statement_error(stmt: &Statement) -> bool {
    matches!(
        stmt,
        Statement::CreateContinuousQuery(_)
            | Statement::CreateMaterializedView(_)
            | Statement::CreateUser { .. }
            | Statement::SetPassword { .. }
    )
}

fn is_cluster_mutation(stmt: &Statement) -> bool {
    matches!(
        stmt,
        Statement::CreateDatabase(_)
            | Statement::DropDatabase(_)
            | Statement::DropSeries(_)
            | Statement::AlterRetentionPolicyStmt { .. }
            | Statement::Delete(_)
            | Statement::CreateContinuousQuery(_)
            | Statement::DropContinuousQuery { .. }
            | Statement::CreateMaterializedView(_)
            | Statement::DropMaterializedView { .. }
            | Statement::CreateRetentionPolicyStmt { .. }
            | Statement::DropRetentionPolicyStmt { .. }
            | Statement::CreateUser { .. }
            | Statement::DropUser(_)
            | Statement::SetPassword { .. }
            | Statement::DropMeasurement { .. }
            | Statement::Grant { .. }
            | Statement::Revoke { .. }
            | Statement::SetSetting { .. }
    )
}

#[async_trait]
impl QueryService for PeerQueryService {
    async fn execute_query(
        &self,
        db: &str,
        query: &str,
        epoch: Option<&str>,
        retention_policy: Option<&str>,
        caller: Option<&crate::domain::user::StoredUser>,
    ) -> Result<QueryResponse, HyperbytedbError> {
        let stmt_queries: Vec<String> = crate::timeseriesql::lexer::split_statements(query)?
            .into_iter()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
        let stmts = crate::timeseriesql::parse(query)?;

        if !stmts.iter().any(is_cluster_mutation) {
            return self
                .inner
                .execute_query(db, query, epoch, retention_policy, caller)
                .await;
        }

        let mut results = Vec::new();
        let use_raft = self.consensus.get().is_some();
        for (i, stmt) in stmts.into_iter().enumerate() {
            let statement_id = i as u32;
            let stmt_query = stmt_queries.get(i).map(String::as_str).unwrap_or(query);
            if use_raft && is_cluster_mutation(&stmt) {
                results.push(
                    self.execute_raft_mutation(db, retention_policy, caller, statement_id, &stmt)
                        .await?,
                );
                continue;
            }
            let result = match stmt {
                ref stmt if is_cluster_mutation(stmt) => {
                    let mut result = self
                        .execute_inner_statement(
                            local_db(stmt, db),
                            stmt_query,
                            epoch,
                            retention_policy,
                            caller,
                            statement_id,
                        )
                        .await;
                    if result.error.is_none() {
                        match mutation_request_from_statement(
                            stmt,
                            db,
                            retention_policy,
                            &self.metadata,
                            self.shard_routing.get(),
                        )
                        .await
                        {
                            Ok(req) => self.replicate_mutation(req).await?,
                            // Applied here but not replicable: the statement
                            // reports it, the other statements still run.
                            Err(e) if build_error_is_statement_error(stmt) => {
                                result.error = Some(e.to_string());
                            }
                            Err(e) => return Err(e),
                        }
                    }
                    result
                }
                _ => {
                    self.execute_inner_statement(
                        db,
                        stmt_query,
                        epoch,
                        retention_policy,
                        caller,
                        statement_id,
                    )
                    .await
                }
            };
            results.push(result);
        }

        Ok(QueryResponse { results })
    }

    async fn execute_continuous_query(
        &self,
        cq: &mut ContinuousQueryDef,
        now: chrono::DateTime<chrono::Utc>,
    ) -> Result<crate::ports::query::CqRunResult, HyperbytedbError> {
        self.inner.execute_continuous_query(cq, now).await
    }
}

fn hash_password_for_replication(password: &str) -> Result<String, HyperbytedbError> {
    crate::domain::user::hash_password(password)
        .map_err(|e| HyperbytedbError::Internal(crate::error::ChainedError::from_error(e)))
}

fn execute_or_error(
    resp: Result<QueryResponse, HyperbytedbError>,
    statement_id: u32,
) -> StatementResult {
    match resp {
        Ok(r) => r.results.into_iter().next().unwrap_or(StatementResult {
            statement_id,
            series: Some(vec![]),
            error: None,
        }),
        Err(e) => StatementResult {
            statement_id,
            series: None,
            error: Some(e.to_string()),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// In Raft mode a DELETE / DROP SERIES built its mutation with the
    /// database's default retention policy, ignoring `?rp=`: a delete on
    /// `week` removed rows from `autogen` instead.
    #[tokio::test]
    async fn raft_delete_uses_the_requested_retention_policy() {
        use crate::adapters::metadata::rocksdb_meta::RocksDbMetadata;
        let dir = tempfile::tempdir().unwrap();
        let meta: Arc<dyn MetadataPort> = Arc::new(RocksDbMetadata::open(dir.path()).unwrap());
        meta.create_database("db").await.unwrap();
        let mut week = RetentionPolicy::autogen();
        week.name = "week".to_string();
        week.is_default = false;
        meta.create_retention_policy("db", week).await.unwrap();
        meta.register_measurement(
            "db",
            "week",
            &crate::domain::measurement::MeasurementMeta {
                name: "cpu".into(),
                tag_keys: vec!["host".into()],
                ..Default::default()
            },
        )
        .await
        .unwrap();

        let rp_of = |req: MutationRequest| match req {
            MutationRequest::Delete { rp, .. } | MutationRequest::DropSeries { rp, .. } => rp,
            other => panic!("unexpected {other:?}"),
        };
        for q in [
            "DELETE FROM cpu WHERE host='a'",
            "DROP SERIES FROM cpu WHERE host='a'",
        ] {
            let stmt = &crate::timeseriesql::parse(q).unwrap()[0];
            let req = mutation_request_from_statement(stmt, "db", Some("week"), &meta, None)
                .await
                .unwrap();
            assert_eq!(rp_of(req), "week", "{q}");
            let req = mutation_request_from_statement(stmt, "db", None, &meta, None)
                .await
                .unwrap();
            assert_eq!(rp_of(req), "autogen", "{q}");
        }
    }

    /// A replication port that does nothing: the Raft path below never
    /// uses it.
    struct NoReplication;
    #[async_trait::async_trait]
    impl ReplicationPort for NoReplication {
        fn replicate_write(
            self: Arc<Self>,
            _batch: crate::ports::replication::OutboundReplicationBatch,
        ) -> Result<(), HyperbytedbError> {
            Ok(())
        }
        async fn replicate_write_sync(
            self: Arc<Self>,
            _batch: crate::ports::replication::OutboundReplicationBatch,
            _required_acks: usize,
            _timeout: std::time::Duration,
        ) -> Result<(), HyperbytedbError> {
            Ok(())
        }
        fn replicate_mutation(self: Arc<Self>, _req: MutationRequest, _targets: Option<Vec<u64>>) {}
        async fn replicate_mutation_sync(
            self: Arc<Self>,
            _req: MutationRequest,
        ) -> Result<(), HyperbytedbError> {
            Ok(())
        }
        async fn replication_peer_count(&self, _self_node_id: u64) -> usize {
            0
        }
    }

    /// The Raft-mode DELETE wiring end to end: `?rp=week` must reach the
    /// mutation through `execute_query` and `execute_raft_mutation`, and the
    /// state machine must delete the rows in `week`, not in `autogen`.
    #[tokio::test]
    #[serial_test::serial(chdb)]
    async fn raft_delete_with_rp_deletes_from_that_rp() {
        use crate::adapters::chdb::native_adapter::ChdbNativeAdapter;
        use crate::adapters::chdb::query_adapter::ChdbQueryAdapter;
        use crate::adapters::chdb::session::SharedSession;
        use crate::adapters::metadata::rocksdb_meta::RocksDbMetadata;
        use crate::adapters::wal::rocksdb_wal::RocksDbWal;
        use crate::application::flush_service::FlushServiceImpl;
        use crate::application::ingestion_service::IngestionServiceImpl;
        use crate::application::materialized_view_service::MaterializedViewService;
        use crate::application::query_service::QueryServiceImpl;
        use crate::composition::bootstrap::ClusterBootstrap;
        use crate::ports::ingestion::{IngestionPort, WritePayloadFormat};
        use crate::ports::points_sink::PointsSinkPort;
        use crate::ports::query::QueryPort;
        use crate::ports::wal::WalPort;

        let dir = tempfile::tempdir().unwrap();
        let (meta_dir, wal_dir, chdb_dir) = (
            dir.path().join("meta"),
            dir.path().join("wal"),
            dir.path().join("chdb"),
        );
        for p in [&meta_dir, &wal_dir, &chdb_dir] {
            std::fs::create_dir_all(p).unwrap();
        }
        let chdb = SharedSession::new_eager(chdb_dir.to_str().unwrap(), 1).unwrap();
        let query_port: Arc<dyn QueryPort> =
            Arc::new(ChdbQueryAdapter::from_shared(chdb.clone(), 0));
        let sink: Arc<dyn PointsSinkPort> = Arc::new(ChdbNativeAdapter::new(chdb));
        let wal: Arc<dyn WalPort> = Arc::new(RocksDbWal::open(&wal_dir).unwrap());
        let metadata: Arc<dyn MetadataPort> = Arc::new(RocksDbMetadata::open(&meta_dir).unwrap());
        let mv_service = Arc::new(MaterializedViewService::new(
            metadata.clone(),
            query_port.clone(),
            sink.clone(),
        ));

        metadata.create_database("db").await.unwrap();
        let mut week = RetentionPolicy::autogen();
        week.name = "week".to_string();
        week.is_default = false;
        metadata.create_retention_policy("db", week).await.unwrap();
        let ingestion = IngestionServiceImpl::with_sink(
            wal.clone(),
            None,
            metadata.clone(),
            Default::default(),
            0,
        );
        for rp in ["autogen", "week"] {
            ingestion
                .ingest(
                    "db",
                    Some(rp),
                    None,
                    b"cpu,host=a v=1 1000000000",
                    WritePayloadFormat::LineProtocol,
                )
                .await
                .unwrap();
        }
        FlushServiceImpl::new(wal.clone(), 0, sink.clone())
            .flush()
            .await
            .unwrap();

        let mut cluster_cfg = crate::config::HyperbytedbConfig::load(None)
            .unwrap()
            .cluster;
        cluster_cfg.enabled = true;
        cluster_cfg.node_id = 1;
        cluster_cfg.cluster_addr = "127.0.0.1:18087".into();
        cluster_cfg.replication_log_dir = dir.path().join("repl").to_string_lossy().into();
        cluster_cfg.raft_dir = dir.path().join("raft").to_string_lossy().into();
        cluster_cfg.raft_heartbeat_interval_ms = Some(200);
        cluster_cfg.raft_election_timeout_ms = Some(500);
        let bootstrap =
            ClusterBootstrap::init(&cluster_cfg, 1000, Default::default(), Default::default())
                .unwrap();
        let raft = bootstrap
            .start_raft(
                &cluster_cfg,
                metadata.clone(),
                mv_service,
                sink.clone(),
                wal.clone(),
                None,
                None,
            )
            .await
            .unwrap();
        for _ in 0..50 {
            if raft.metrics().borrow().current_leader == Some(1) {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }

        let base: Arc<dyn QueryService> = Arc::new(QueryServiceImpl::new(
            query_port,
            metadata.clone(),
            wal,
            30,
            sink,
        ));
        let peer = PeerQueryService::new(base.clone(), metadata.clone(), Arc::new(NoReplication));
        peer.set_consensus(Arc::new(
            crate::adapters::cluster::raft::consensus::RaftConsensus::new(
                raft,
                Arc::new(
                    crate::adapters::cluster::http_peer_rpc::HttpPeerRpc::with_client(
                        reqwest::Client::new(),
                    ),
                ),
                None,
            ),
        ));

        let resp = peer
            .execute_query(
                "db",
                "DELETE FROM cpu WHERE host='a'",
                None,
                Some("week"),
                None,
            )
            .await
            .unwrap();
        assert!(
            resp.results[0].error.is_none(),
            "{:?}",
            resp.results[0].error
        );

        let rows = |rp: &'static str| {
            let base = base.clone();
            async move {
                let q = format!(r#"SELECT v FROM "{rp}"."cpu""#);
                let resp = base
                    .execute_query("db", &q, None, None, None)
                    .await
                    .unwrap();
                resp.results[0]
                    .series
                    .as_ref()
                    .map_or(0, |s| s.iter().map(|s| s.values.len()).sum::<usize>())
            }
        };
        assert_eq!(rows("week").await, 0, "the DELETE named ?rp=week");
        assert_eq!(rows("autogen").await, 1, "autogen must be untouched");
    }
}
