use metrics::counter;
use std::sync::Arc;
use tokio::sync::watch;

use crate::adapters::cluster::raft::HyperbytedbRaft;
use crate::domain::cq_schedule::should_run;
use crate::error::HyperbytedbError;
use crate::ports::metadata::MetadataPort;
use crate::ports::query::QueryService;

pub struct ContinuousQueryService {
    metadata: Arc<dyn MetadataPort>,
    query_service: Arc<dyn QueryService>,
    raft: Option<HyperbytedbRaft>,
    node_id: u64,
}

impl ContinuousQueryService {
    pub fn new(
        metadata: Arc<dyn MetadataPort>,
        query_service: Arc<dyn QueryService>,
        raft: Option<HyperbytedbRaft>,
        node_id: u64,
    ) -> Self {
        Self {
            metadata,
            query_service,
            raft,
            node_id,
        }
    }

    fn is_raft_leader(&self) -> bool {
        match &self.raft {
            Some(raft) => {
                let metrics = raft.metrics().borrow().clone();
                metrics.current_leader == Some(self.node_id)
            }
            None => true,
        }
    }

    pub async fn run(
        &self,
        check_interval: std::time::Duration,
        mut shutdown_rx: watch::Receiver<bool>,
    ) {
        let mut ticker = tokio::time::interval(check_interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        tracing::info!(
            check_interval = ?check_interval,
            raft_gated = self.raft.is_some(),
            node_id = self.node_id,
            "continuous query service started"
        );
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    if let Err(e) = self.run_pending_queries().await {
                        tracing::error!("continuous query execution error: {}", e);
                    }
                }
                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        tracing::info!("continuous query service received shutdown");
                        break;
                    }
                }
            }
        }
    }

    async fn run_pending_queries(&self) -> Result<(), HyperbytedbError> {
        if !self.is_raft_leader() {
            tracing::debug!(
                node_id = self.node_id,
                "skipping continuous query tick: not raft leader"
            );
            return Ok(());
        }

        let cqs = self.metadata.list_all_continuous_queries().await?;
        if cqs.is_empty() {
            return Ok(());
        }

        let now = chrono::Utc::now();

        for mut cq in cqs {
            if let Err(e) = cq.normalize() {
                tracing::warn!(cq = %cq.name, error = %e, "failed to normalize continuous query");
                counter!("hyperbytedb_cq_errors_total").increment(1);
                continue;
            }

            if !should_run(now, &cq) {
                continue;
            }

            match self
                .query_service
                .execute_continuous_query(&mut cq, now)
                .await
            {
                Ok(result) => {
                    tracing::debug!(
                        cq = %cq.name,
                        db = %cq.database,
                        points_written = result.points_written,
                        duration_ms = result.duration_ms,
                        window_start = %result.window.start,
                        window_end = %result.window.end,
                        "continuous query completed"
                    );
                }
                Err(e) => {
                    tracing::warn!(cq = %cq.name, error = %e, "CQ execution failed");
                    counter!("hyperbytedb_cq_errors_total").increment(1);
                }
            }
        }

        Ok(())
    }
}
