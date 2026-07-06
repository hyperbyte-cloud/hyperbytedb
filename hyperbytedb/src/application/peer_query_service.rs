use async_trait::async_trait;
use std::sync::Arc;
use std::sync::OnceLock;

use openraft::BasicNode;
use openraft::error::ForwardToLeader;

use crate::adapters::cluster::raft::HyperbytedbRaft;
use crate::adapters::cluster::raft::types::{ClusterRequest, ClusterResponse};
use crate::application::materialized_view_service::def_from_statement;
use crate::application::predicate_sql::build_predicate_sql;
use crate::application::query_service::check_authorization;
use crate::domain::cluster::membership::ClusterMembership;
use crate::domain::cluster::types::MutationRequest;
use crate::domain::database::{RetentionPolicy, retention_policy_from_create};
use crate::domain::query_result::{QueryResponse, StatementResult};
use crate::error::HyperbytedbError;
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
    raft: std::sync::OnceLock<HyperbytedbRaft>,
    membership: std::sync::OnceLock<Arc<tokio::sync::RwLock<ClusterMembership>>>,
}

fn raft_http_client() -> &'static reqwest::Client {
    static CLIENT: OnceLock<reqwest::Client> = OnceLock::new();
    CLIENT.get_or_init(|| {
        reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(10))
            .build()
            .unwrap_or_else(|_| reqwest::Client::new())
    })
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
            raft: std::sync::OnceLock::new(),
            membership: std::sync::OnceLock::new(),
        }
    }

    /// Set the Raft instance for consensus-based schema mutation replication.
    /// Called after Raft initialization completes.
    pub fn set_raft(&self, raft: HyperbytedbRaft) {
        if self.raft.set(raft).is_err() {
            tracing::error!("PeerQueryService::set_raft called more than once; ignoring duplicate");
        }
    }

    /// Shared cluster membership for resolving leader addresses during Raft
    /// forward when openraft omits `leader_node` from `ForwardToLeader`.
    pub fn set_membership(&self, membership: Arc<tokio::sync::RwLock<ClusterMembership>>) {
        if self.membership.set(membership).is_err() {
            tracing::error!(
                "PeerQueryService::set_membership called more than once; ignoring duplicate"
            );
        }
    }

    async fn replicate_mutation(&self, req: MutationRequest) -> Result<(), HyperbytedbError> {
        if let Some(raft) = self.raft.get() {
            let cluster_req = ClusterRequest::SchemaMutation(Box::new(req));
            self.client_write_with_forward(raft, cluster_req).await
        } else {
            self.replication_port.clone().replicate_mutation(req);
            Ok(())
        }
    }

    async fn client_write_with_forward(
        &self,
        raft: &HyperbytedbRaft,
        req: ClusterRequest,
    ) -> Result<(), HyperbytedbError> {
        const MAX_ATTEMPTS: u32 = 3;
        for attempt in 0..MAX_ATTEMPTS {
            match raft.client_write(req.clone()).await {
                Ok(resp) => {
                    if resp.data.ok {
                        return Ok(());
                    }
                    return Err(HyperbytedbError::Internal(
                        resp.data
                            .message
                            .unwrap_or_else(|| "raft schema mutation apply failed".into()),
                    ));
                }
                Err(e) => {
                    if let Some(forward) = e.forward_to_leader::<BasicNode>() {
                        tracing::debug!(
                            leader_id = ?forward.leader_id,
                            leader_addr = ?forward
                                .leader_node
                                .as_ref()
                                .map(|node| node.addr.as_str()),
                            attempt,
                            "forwarding schema mutation to raft leader"
                        );
                        match forward_client_write(forward, raft, self.membership.get(), &req).await
                        {
                            Ok(()) => return Ok(()),
                            Err(forward_err) => {
                                tracing::warn!(
                                    error = %forward_err,
                                    attempt,
                                    "forward to raft leader failed; retrying"
                                );
                                if attempt + 1 >= MAX_ATTEMPTS {
                                    return Err(forward_err);
                                }
                                continue;
                            }
                        }
                    }
                    tracing::error!(error = %e, "failed to replicate mutation via raft");
                    return Err(HyperbytedbError::Internal(format!(
                        "raft replication failed: {e}"
                    )));
                }
            }
        }
        Err(HyperbytedbError::Internal(
            "raft replication failed after retries".into(),
        ))
    }

    async fn execute_raft_mutation(
        &self,
        db: &str,
        caller: Option<&crate::domain::user::StoredUser>,
        statement_id: u32,
        stmt: &Statement,
    ) -> StatementResult {
        if let Some(user) = caller
            && let Err(e) = check_authorization(user, db, stmt)
        {
            return StatementResult {
                statement_id,
                series: None,
                error: Some(e.to_string()),
            };
        }
        match mutation_request_from_statement(stmt, db, &self.metadata).await {
            Ok(req) => match self.replicate_mutation(req).await {
                Ok(()) => StatementResult {
                    statement_id,
                    series: Some(vec![]),
                    error: None,
                },
                Err(e) => StatementResult {
                    statement_id,
                    series: None,
                    error: Some(e.to_string()),
                },
            },
            Err(e) => StatementResult {
                statement_id,
                series: None,
                error: Some(e.to_string()),
            },
        }
    }
}

async fn resolve_forward_leader_addr(
    forward: &ForwardToLeader<u64, BasicNode>,
    raft: &HyperbytedbRaft,
    membership: Option<&Arc<tokio::sync::RwLock<ClusterMembership>>>,
) -> Result<String, HyperbytedbError> {
    let metrics = raft.metrics().borrow().clone();
    let membership_addrs = if let Some(membership) = membership {
        let m = membership.read().await;
        m.nodes
            .values()
            .map(|node| (node.node_id, node.addr.clone()))
            .collect::<Vec<_>>()
    } else {
        Vec::new()
    };

    resolve_leader_addr_with_lookup(
        forward,
        metrics.current_leader,
        |leader_id| {
            metrics
                .membership_config
                .membership()
                .get_node(&leader_id)
                .map(|node| node.addr.clone())
        },
        |leader_id| {
            membership_addrs
                .iter()
                .find(|(id, _)| *id == leader_id)
                .map(|(_, addr)| addr.clone())
        },
    )
}

fn resolve_leader_addr_with_lookup(
    forward: &ForwardToLeader<u64, BasicNode>,
    fallback_leader_id: Option<u64>,
    raft_lookup: impl Fn(u64) -> Option<String>,
    membership_lookup: impl Fn(u64) -> Option<String>,
) -> Result<String, HyperbytedbError> {
    if let Some(node) = forward.leader_node.as_ref() {
        return Ok(node.addr.clone());
    }

    let leader_id = forward.leader_id.or(fallback_leader_id);
    let leader_id = leader_id.ok_or_else(|| {
        HyperbytedbError::Internal(
            "raft replication failed: forward to leader but leader id is unknown".into(),
        )
    })?;

    raft_lookup(leader_id)
        .or_else(|| membership_lookup(leader_id))
        .ok_or_else(|| {
            HyperbytedbError::Internal(format!(
                "raft replication failed: leader {leader_id} has no known address"
            ))
        })
}

async fn forward_client_write(
    forward: &ForwardToLeader<u64, BasicNode>,
    raft: &HyperbytedbRaft,
    membership: Option<&Arc<tokio::sync::RwLock<ClusterMembership>>>,
    req: &ClusterRequest,
) -> Result<(), HyperbytedbError> {
    let leader_addr = resolve_forward_leader_addr(forward, raft, membership).await?;

    let url = format!("http://{leader_addr}/cluster/raft/client-write");
    let resp = raft_http_client()
        .post(&url)
        .json(req)
        .send()
        .await
        .map_err(|e| {
            HyperbytedbError::Internal(format!(
                "raft replication failed: forward to leader {url}: {e}"
            ))
        })?;

    let status = resp.status();
    let body_text = resp.text().await.map_err(|e| {
        HyperbytedbError::Internal(format!(
            "raft replication failed: read leader response: {e}"
        ))
    })?;

    if !status.is_success() {
        return Err(HyperbytedbError::Internal(format!(
            "raft replication failed: leader returned HTTP {status}: {body_text}"
        )));
    }

    let body: ClusterResponse = serde_json::from_str(&body_text).map_err(|e| {
        HyperbytedbError::Internal(format!(
            "raft replication failed: invalid leader response: {e}: {body_text}"
        ))
    })?;

    if body.ok {
        Ok(())
    } else {
        Err(HyperbytedbError::Internal(body.message.unwrap_or_else(
            || "raft leader rejected schema mutation".into(),
        )))
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
    metadata: &Arc<dyn MetadataPort>,
) -> Result<MutationRequest, HyperbytedbError> {
    match stmt {
        Statement::CreateDatabase(stmt) => Ok(MutationRequest::CreateDatabase {
            name: stmt.name.clone(),
            rp: retention_policy_from_create(stmt),
        }),
        Statement::DropDatabase(name) => Ok(MutationRequest::DropDatabase(name.clone())),
        Statement::Delete(del) => {
            let del_rp = metadata.get_default_rp(db).await?;
            let predicate_sql = if let Some(ref cond) = del.condition {
                build_predicate_sql(metadata, db, &del_rp, &del.from, cond).await?
            } else {
                String::new()
            };
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
            Ok(MutationRequest::CreateMaterializedView {
                database: mv.database.clone(),
                name: mv.name.clone(),
                definition,
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
            let password_hash =
                crate::adapters::http::auth_middleware::hash_password(password).unwrap_or_default();
            Ok(MutationRequest::CreateUser {
                username: username.clone(),
                password_hash,
                admin: *admin,
            })
        }
        Statement::DropUser(username) => Ok(MutationRequest::DropUser(username.clone())),
        Statement::SetPassword { username, password } => {
            let password_hash =
                crate::adapters::http::auth_middleware::hash_password(password).unwrap_or_default();
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
            let drop_rp = metadata.get_default_rp(target_db).await?;
            let predicate_sql = if let Some(ref cond) = s.condition {
                let meas = measurement.as_deref().unwrap_or("");
                build_predicate_sql(metadata, target_db, &drop_rp, meas, cond).await?
            } else {
                String::new()
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
        Statement::Grant { username, database } => Ok(MutationRequest::Grant {
            username: username.clone(),
            database: database.clone(),
        }),
        Statement::Revoke { username, database } => Ok(MutationRequest::Revoke {
            username: username.clone(),
            database: database.clone(),
        }),
        _ => Err(HyperbytedbError::Internal(
            "not a cluster mutation statement".into(),
        )),
    }
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
        let stmts = crate::timeseriesql::parse(query)?;

        if !stmts.iter().any(is_cluster_mutation) {
            return self
                .inner
                .execute_query(db, query, epoch, retention_policy, caller)
                .await;
        }

        let mut results = Vec::new();
        let use_raft = self.raft.get().is_some();
        for (i, stmt) in stmts.into_iter().enumerate() {
            let statement_id = i as u32;
            if use_raft && is_cluster_mutation(&stmt) {
                results.push(
                    self.execute_raft_mutation(db, caller, statement_id, &stmt)
                        .await,
                );
                continue;
            }
            let result = match stmt {
                Statement::CreateDatabase(ref stmt) => {
                    let resp = self
                        .inner
                        .execute_query(db, query, epoch, retention_policy, caller)
                        .await;
                    let mut result = match resp {
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
                    };
                    if result.error.is_none()
                        && let Err(e) = self
                            .replicate_mutation(MutationRequest::CreateDatabase {
                                name: stmt.name.clone(),
                                rp: retention_policy_from_create(stmt),
                            })
                            .await
                    {
                        result.error = Some(e.to_string());
                    }
                    result
                }
                Statement::DropDatabase(ref name) => {
                    let resp = self
                        .inner
                        .execute_query(db, query, epoch, retention_policy, caller)
                        .await;
                    let mut result = match resp {
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
                    };
                    if result.error.is_none()
                        && let Err(e) = self
                            .replicate_mutation(MutationRequest::DropDatabase(name.clone()))
                            .await
                    {
                        result.error = Some(e.to_string());
                    }
                    result
                }
                Statement::Delete(ref del) => {
                    let resp = self
                        .inner
                        .execute_query(db, query, epoch, retention_policy, caller)
                        .await;
                    let mut result = match resp {
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
                    };
                    if result.error.is_none() {
                        let del_rp = if let Some(rp) = retention_policy {
                            rp.to_string()
                        } else {
                            self.metadata.get_default_rp(db).await?
                        };
                        let predicate_sql = if let Some(ref cond) = del.condition {
                            build_predicate_sql(&self.metadata, db, &del_rp, &del.from, cond)
                                .await?
                        } else {
                            String::new()
                        };
                        if let Err(e) = self
                            .replicate_mutation(MutationRequest::Delete {
                                database: db.to_string(),
                                rp: del_rp,
                                measurement: del.from.clone(),
                                predicate_sql,
                            })
                            .await
                        {
                            result.error = Some(e.to_string());
                        }
                    }
                    result
                }
                Statement::CreateContinuousQuery(ref cq) => {
                    let resp = self
                        .inner
                        .execute_query(db, query, epoch, retention_policy, caller)
                        .await;
                    let mut result = match resp {
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
                    };
                    if result.error.is_none() {
                        let def = match ContinuousQueryDef::from_create(cq) {
                            Ok(def) => def,
                            Err(e) => {
                                result.error = Some(e.to_string());
                                results.push(result);
                                continue;
                            }
                        };
                        if let Err(e) = self
                            .replicate_mutation(MutationRequest::CreateContinuousQuery {
                                database: cq.database.clone(),
                                name: cq.name.clone(),
                                definition: def,
                            })
                            .await
                        {
                            result.error = Some(e.to_string());
                        }
                    }
                    result
                }
                Statement::DropContinuousQuery {
                    ref name,
                    db: ref cq_db,
                } => {
                    let resp = self
                        .inner
                        .execute_query(db, query, epoch, retention_policy, caller)
                        .await;
                    let mut result = match resp {
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
                    };
                    if result.error.is_none() {
                        let target_db = if cq_db.is_empty() { db } else { cq_db };
                        if let Err(e) = self
                            .replicate_mutation(MutationRequest::DropContinuousQuery {
                                database: target_db.to_string(),
                                name: name.clone(),
                            })
                            .await
                        {
                            result.error = Some(e.to_string());
                        }
                    }
                    result
                }
                Statement::CreateMaterializedView(ref mv) => {
                    let resp = self
                        .inner
                        .execute_query(db, query, epoch, retention_policy, caller)
                        .await;
                    let mut result = match resp {
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
                    };
                    if result.error.is_none() {
                        let source_rp = match resolve_mv_source_rp(&self.metadata, mv).await {
                            Ok(rp) => rp,
                            Err(e) => {
                                result.error = Some(e.to_string());
                                results.push(result);
                                continue;
                            }
                        };
                        let dest_rp = match resolve_mv_dest_rp(&self.metadata, mv).await {
                            Ok(rp) => rp,
                            Err(e) => {
                                result.error = Some(e.to_string());
                                results.push(result);
                                continue;
                            }
                        };
                        if let Ok(def) = def_from_statement(mv, &source_rp, &dest_rp)
                            && let Err(e) = self
                                .replicate_mutation(MutationRequest::CreateMaterializedView {
                                    database: mv.database.clone(),
                                    name: mv.name.clone(),
                                    definition: def,
                                })
                                .await
                        {
                            result.error = Some(e.to_string());
                        }
                    }
                    result
                }
                Statement::DropMaterializedView {
                    ref name,
                    db: ref mv_db,
                } => {
                    let resp = self
                        .inner
                        .execute_query(db, query, epoch, retention_policy, caller)
                        .await;
                    let mut result = match resp {
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
                    };
                    if result.error.is_none() {
                        let target_db = if mv_db.is_empty() { db } else { mv_db };
                        if let Err(e) = self
                            .replicate_mutation(MutationRequest::DropMaterializedView {
                                database: target_db.to_string(),
                                name: name.clone(),
                            })
                            .await
                        {
                            result.error = Some(e.to_string());
                        }
                    }
                    result
                }
                Statement::CreateRetentionPolicyStmt {
                    ref name,
                    ref db,
                    ref duration,
                    replication,
                    ref shard_duration,
                    is_default,
                } => {
                    let resp = self
                        .inner
                        .execute_query(db, query, epoch, retention_policy, caller)
                        .await;
                    let mut result = execute_or_error(resp, statement_id);
                    if result.error.is_none() {
                        let dur = duration
                            .as_ref()
                            .map(|d| std::time::Duration::from_nanos(d.to_nanos() as u64));
                        let shard_dur = shard_duration
                            .as_ref()
                            .map(|d| std::time::Duration::from_nanos(d.to_nanos() as u64))
                            .unwrap_or_else(|| {
                                crate::domain::database::derive_shard_group_duration(dur)
                            });
                        let rp = RetentionPolicy {
                            name: name.clone(),
                            duration: dur,
                            shard_group_duration: shard_dur,
                            replication_factor: replication,
                            is_default,
                        };
                        if let Err(e) = self
                            .replicate_mutation(MutationRequest::CreateRetentionPolicy {
                                db: db.clone(),
                                rp,
                            })
                            .await
                        {
                            result.error = Some(e.to_string());
                        }
                    }
                    result
                }
                Statement::DropRetentionPolicyStmt { ref name, ref db } => {
                    let resp = self
                        .inner
                        .execute_query(db, query, epoch, retention_policy, caller)
                        .await;
                    let mut result = execute_or_error(resp, statement_id);
                    if result.error.is_none()
                        && let Err(e) = self
                            .replicate_mutation(MutationRequest::DropRetentionPolicy {
                                db: db.clone(),
                                name: name.clone(),
                            })
                            .await
                    {
                        result.error = Some(e.to_string());
                    }
                    result
                }
                Statement::CreateUser {
                    ref username,
                    ref password,
                    admin,
                } => {
                    let resp = self
                        .inner
                        .execute_query(db, query, epoch, retention_policy, caller)
                        .await;
                    let mut result = execute_or_error(resp, statement_id);
                    if result.error.is_none() {
                        let password_hash =
                            crate::adapters::http::auth_middleware::hash_password(password)
                                .unwrap_or_default();
                        if let Err(e) = self
                            .replicate_mutation(MutationRequest::CreateUser {
                                username: username.clone(),
                                password_hash,
                                admin,
                            })
                            .await
                        {
                            result.error = Some(e.to_string());
                        }
                    }
                    result
                }
                Statement::DropUser(ref username) => {
                    let resp = self
                        .inner
                        .execute_query(db, query, epoch, retention_policy, caller)
                        .await;
                    let mut result = execute_or_error(resp, statement_id);
                    if result.error.is_none()
                        && let Err(e) = self
                            .replicate_mutation(MutationRequest::DropUser(username.clone()))
                            .await
                    {
                        result.error = Some(e.to_string());
                    }
                    result
                }
                Statement::SetPassword {
                    ref username,
                    ref password,
                } => {
                    let resp = self
                        .inner
                        .execute_query(db, query, epoch, retention_policy, caller)
                        .await;
                    let mut result = execute_or_error(resp, statement_id);
                    if result.error.is_none() {
                        let password_hash =
                            crate::adapters::http::auth_middleware::hash_password(password)
                                .unwrap_or_default();
                        if let Err(e) = self
                            .replicate_mutation(MutationRequest::SetPassword {
                                username: username.clone(),
                                password_hash,
                            })
                            .await
                        {
                            result.error = Some(e.to_string());
                        }
                    }
                    result
                }
                Statement::AlterRetentionPolicyStmt {
                    ref name,
                    ref db,
                    duration,
                    replication,
                    ref shard_duration,
                    is_default,
                } => {
                    let resp = self
                        .inner
                        .execute_query(db, query, epoch, retention_policy, caller)
                        .await;
                    let mut result = execute_or_error(resp, statement_id);
                    if result.error.is_none() {
                        let change = RetentionPolicyChange {
                            duration: duration.as_ref().map(|d| {
                                if d.value == 0 && d.unit == DurationUnit::Second {
                                    None
                                } else {
                                    Some(d.clone())
                                }
                            }),
                            replication,
                            shard_duration: shard_duration.clone(),
                            is_default,
                        };
                        if let Err(e) = self
                            .replicate_mutation(MutationRequest::AlterRetentionPolicy {
                                db: db.clone(),
                                name: name.clone(),
                                change,
                            })
                            .await
                        {
                            result.error = Some(e.to_string());
                        }
                    }
                    result
                }
                Statement::DropSeries(ref s) => {
                    let resp = self
                        .inner
                        .execute_query(db, query, epoch, retention_policy, caller)
                        .await;
                    let mut result = execute_or_error(resp, statement_id);
                    if result.error.is_none() {
                        let target_db = s.database.as_deref().unwrap_or(db);
                        let measurement = s.from.as_ref().and_then(|n| match n {
                            MeasurementName::Name(n) => Some(n.clone()),
                            MeasurementName::Regex(_) => None,
                        });
                        let ds_rp = if let Some(rp) = retention_policy {
                            rp.to_string()
                        } else {
                            self.metadata.get_default_rp(target_db).await?
                        };
                        let predicate_sql = if let Some(ref cond) = s.condition {
                            let meas = measurement.as_deref().unwrap_or("");
                            build_predicate_sql(&self.metadata, target_db, &ds_rp, meas, cond)
                                .await?
                        } else {
                            String::new()
                        };
                        if let Err(e) = self
                            .replicate_mutation(MutationRequest::DropSeries {
                                database: target_db.to_string(),
                                rp: ds_rp,
                                measurement,
                                predicate_sql,
                            })
                            .await
                        {
                            result.error = Some(e.to_string());
                        }
                    }
                    result
                }
                Statement::DropMeasurement {
                    ref name,
                    rp: ref stmt_rp,
                } => {
                    let resp = self
                        .inner
                        .execute_query(db, query, epoch, retention_policy, caller)
                        .await;
                    let mut result = match resp {
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
                    };
                    if result.error.is_none() {
                        let dm_rp = if let Some(rp) = stmt_rp {
                            rp.clone()
                        } else {
                            self.metadata.get_default_rp(db).await?
                        };
                        if let Err(e) = self
                            .replicate_mutation(MutationRequest::DropMeasurement {
                                database: db.to_string(),
                                rp: dm_rp,
                                name: name.clone(),
                            })
                            .await
                        {
                            result.error = Some(e.to_string());
                        }
                    }
                    result
                }
                Statement::Grant {
                    ref username,
                    ref database,
                } => {
                    let resp = self
                        .inner
                        .execute_query(db, query, epoch, retention_policy, caller)
                        .await;
                    let mut result = match resp {
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
                    };
                    if result.error.is_none()
                        && let Err(e) = self
                            .replicate_mutation(MutationRequest::Grant {
                                username: username.clone(),
                                database: database.clone(),
                            })
                            .await
                    {
                        result.error = Some(e.to_string());
                    }
                    result
                }
                Statement::Revoke {
                    ref username,
                    ref database,
                } => {
                    let resp = self
                        .inner
                        .execute_query(db, query, epoch, retention_policy, caller)
                        .await;
                    let mut result = match resp {
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
                    };
                    if result.error.is_none()
                        && let Err(e) = self
                            .replicate_mutation(MutationRequest::Revoke {
                                username: username.clone(),
                                database: database.clone(),
                            })
                            .await
                    {
                        result.error = Some(e.to_string());
                    }
                    result
                }
                _ => {
                    let resp = self
                        .inner
                        .execute_query(db, query, epoch, retention_policy, caller)
                        .await?;
                    resp.results.into_iter().next().unwrap_or(StatementResult {
                        statement_id,
                        series: Some(vec![]),
                        error: None,
                    })
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

    #[test]
    fn resolve_leader_addr_prefers_forward_node() {
        let forward = ForwardToLeader {
            leader_id: Some(1),
            leader_node: Some(BasicNode::new("hyperbytedb-0:8086")),
        };

        let addr = resolve_leader_addr_with_lookup(&forward, None, |_| None, |_| None)
            .expect("leader addr");

        assert_eq!(addr, "hyperbytedb-0:8086");
    }

    #[test]
    fn resolve_leader_addr_falls_back_to_raft_membership() {
        let forward = ForwardToLeader {
            leader_id: Some(1),
            leader_node: None,
        };

        let addr = resolve_leader_addr_with_lookup(
            &forward,
            None,
            |id| (id == 1).then(|| "hyperbytedb-0:8086".into()),
            |_| None,
        )
        .expect("leader addr");

        assert_eq!(addr, "hyperbytedb-0:8086");
    }

    #[test]
    fn resolve_leader_addr_falls_back_to_cluster_membership() {
        let forward = ForwardToLeader {
            leader_id: Some(2),
            leader_node: None,
        };

        let addr = resolve_leader_addr_with_lookup(
            &forward,
            None,
            |_| None,
            |id| (id == 2).then(|| "hyperbytedb-1:8086".into()),
        )
        .expect("leader addr");

        assert_eq!(addr, "hyperbytedb-1:8086");
    }

    #[test]
    fn resolve_leader_addr_uses_metrics_leader_when_forward_id_missing() {
        let forward = ForwardToLeader {
            leader_id: None,
            leader_node: None,
        };

        let addr = resolve_leader_addr_with_lookup(
            &forward,
            Some(1),
            |id| (id == 1).then(|| "hyperbytedb-0:8086".into()),
            |_| None,
        )
        .expect("leader addr");

        assert_eq!(addr, "hyperbytedb-0:8086");
    }

    #[test]
    fn resolve_leader_addr_errors_when_unresolvable() {
        let forward = ForwardToLeader {
            leader_id: Some(99),
            leader_node: None,
        };

        let err = resolve_leader_addr_with_lookup(&forward, None, |_| None, |_| None)
            .expect_err("expected error");

        assert!(err.to_string().contains("leader 99 has no known address"));
    }
}
