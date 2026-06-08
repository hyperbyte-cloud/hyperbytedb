use async_trait::async_trait;
use std::sync::Arc;
use std::sync::OnceLock;

use openraft::BasicNode;
use openraft::error::ForwardToLeader;

use crate::adapters::cluster::raft::HyperbytedbRaft;
use crate::adapters::cluster::raft::types::{ClusterRequest, ClusterResponse};
use crate::domain::cluster::types::MutationRequest;
use crate::domain::database::RetentionPolicy;
use crate::domain::query_result::{QueryResponse, StatementResult};
use crate::error::HyperbytedbError;
use crate::ports::metadata::ContinuousQueryDef;
use crate::ports::query::QueryService;
use crate::ports::replication::ReplicationPort;
use crate::timeseriesql::ast::Statement;
use crate::timeseriesql::to_clickhouse;

/// Wraps an inner QueryService, intercepting mutations (CREATE/DROP DATABASE,
/// DELETE, CREATE/DROP CONTINUOUS QUERY) and replicating them to peers after
/// local application. When a Raft instance is available, schema mutations
/// are routed through Raft consensus instead of direct peer replication.
/// Followers automatically forward schema mutations to the current leader.
pub struct PeerQueryService {
    inner: Arc<dyn QueryService>,
    replication_port: Arc<dyn ReplicationPort>,
    raft: std::sync::OnceLock<HyperbytedbRaft>,
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
    pub fn new(inner: Arc<dyn QueryService>, replication_port: Arc<dyn ReplicationPort>) -> Self {
        Self {
            inner,
            replication_port,
            raft: std::sync::OnceLock::new(),
        }
    }

    /// Set the Raft instance for consensus-based schema mutation replication.
    /// Called after Raft initialization completes.
    pub fn set_raft(&self, raft: HyperbytedbRaft) {
        if self.raft.set(raft).is_err() {
            tracing::error!("PeerQueryService::set_raft called more than once; ignoring duplicate");
        }
    }

    async fn replicate_mutation(&self, req: MutationRequest) -> Result<(), HyperbytedbError> {
        if let Some(raft) = self.raft.get() {
            let cluster_req = ClusterRequest::SchemaMutation(req);
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
                        match forward_client_write(forward, &req).await {
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
}

async fn forward_client_write(
    forward: &ForwardToLeader<u64, BasicNode>,
    req: &ClusterRequest,
) -> Result<(), HyperbytedbError> {
    let leader = forward.leader_node.as_ref().ok_or_else(|| {
        HyperbytedbError::Internal(
            "raft replication failed: forward to leader but leader node is unknown".into(),
        )
    })?;

    let url = format!("http://{}/cluster/raft/client-write", leader.addr);
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

fn is_cluster_mutation(stmt: &Statement) -> bool {
    matches!(
        stmt,
        Statement::CreateDatabase(_)
            | Statement::DropDatabase(_)
            | Statement::Delete(_)
            | Statement::CreateContinuousQuery(_)
            | Statement::DropContinuousQuery { .. }
            | Statement::CreateRetentionPolicyStmt { .. }
            | Statement::DropRetentionPolicyStmt { .. }
            | Statement::CreateUser { .. }
            | Statement::DropUser(_)
            | Statement::SetPassword { .. }
    )
}

#[async_trait]
impl QueryService for PeerQueryService {
    async fn execute_query(
        &self,
        db: &str,
        query: &str,
        epoch: Option<&str>,
        caller: Option<&crate::domain::user::StoredUser>,
    ) -> Result<QueryResponse, HyperbytedbError> {
        let stmts = crate::timeseriesql::parse(query)?;

        if !stmts.iter().any(is_cluster_mutation) {
            return self.inner.execute_query(db, query, epoch, caller).await;
        }

        let mut results = Vec::new();
        for (i, stmt) in stmts.into_iter().enumerate() {
            let statement_id = i as u32;
            let result = match stmt {
                Statement::CreateDatabase(ref name) => {
                    let resp = self.inner.execute_query(db, query, epoch, caller).await;
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
                            .replicate_mutation(MutationRequest::CreateDatabase(name.clone()))
                            .await
                    {
                        result.error = Some(e.to_string());
                    }
                    result
                }
                Statement::DropDatabase(ref name) => {
                    let resp = self.inner.execute_query(db, query, epoch, caller).await;
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
                    let resp = self.inner.execute_query(db, query, epoch, caller).await;
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
                        let predicate_sql = if let Some(ref cond) = del.condition {
                            let mut sql = String::new();
                            if let Ok(()) = to_clickhouse::translate_condition(cond, &mut sql) {
                                sql
                            } else {
                                String::new()
                            }
                        } else {
                            String::new()
                        };
                        if let Err(e) = self
                            .replicate_mutation(MutationRequest::Delete {
                                database: db.to_string(),
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
                    let resp = self.inner.execute_query(db, query, epoch, caller).await;
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
                        let resample_every_secs = cq
                            .resample_every
                            .as_ref()
                            .map(|d| (d.to_nanos() / 1_000_000_000) as u64);
                        let resample_for_secs = cq
                            .resample_for
                            .as_ref()
                            .map(|d| (d.to_nanos() / 1_000_000_000) as u64);
                        let def = ContinuousQueryDef {
                            name: cq.name.clone(),
                            database: cq.database.clone(),
                            query_text: cq.raw_query.clone(),
                            resample_every_secs,
                            resample_for_secs,
                            created_at: chrono::Utc::now().to_rfc3339(),
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
                    let resp = self.inner.execute_query(db, query, epoch, caller).await;
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
                Statement::CreateRetentionPolicyStmt {
                    ref name,
                    ref db,
                    ref duration,
                    replication,
                    ref shard_duration,
                    is_default,
                } => {
                    let resp = self.inner.execute_query(db, query, epoch, caller).await;
                    let mut result = execute_or_error(resp, statement_id);
                    if result.error.is_none() {
                        let dur = duration
                            .as_ref()
                            .map(|d| std::time::Duration::from_nanos(d.to_nanos() as u64));
                        let shard_dur = shard_duration
                            .as_ref()
                            .map(|d| std::time::Duration::from_nanos(d.to_nanos() as u64))
                            .unwrap_or(std::time::Duration::from_secs(7 * 24 * 3600));
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
                    let resp = self.inner.execute_query(db, query, epoch, caller).await;
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
                    let resp = self.inner.execute_query(db, query, epoch, caller).await;
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
                    let resp = self.inner.execute_query(db, query, epoch, caller).await;
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
                    let resp = self.inner.execute_query(db, query, epoch, caller).await;
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
                _ => {
                    let resp = self.inner.execute_query(db, query, epoch, caller).await?;
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
