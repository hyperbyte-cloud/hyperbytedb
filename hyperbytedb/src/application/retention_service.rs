use metrics::counter;
use std::sync::Arc;
use tokio::sync::watch;

use crate::adapters::cluster::raft::HyperbytedbRaft;
use crate::domain::chdb_naming::{quoted_series_table_name, quoted_table_name};
use crate::error::HyperbytedbError;
use crate::ports::metadata::MetadataPort;
use crate::ports::query::QueryPort;

pub struct RetentionService {
    metadata: Arc<dyn MetadataPort>,
    query: Arc<dyn QueryPort>,
    raft: Option<HyperbytedbRaft>,
    node_id: u64,
}

impl RetentionService {
    pub fn new(
        metadata: Arc<dyn MetadataPort>,
        query: Arc<dyn QueryPort>,
        raft: Option<HyperbytedbRaft>,
        node_id: u64,
    ) -> Self {
        Self {
            metadata,
            query,
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

    pub async fn run(&self, interval: std::time::Duration, mut shutdown_rx: watch::Receiver<bool>) {
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        tracing::info!(
            interval = ?interval,
            raft_gated = self.raft.is_some(),
            node_id = self.node_id,
            "retention service started"
        );
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    tracing::debug!("retention enforcement tick");
                    match self.enforce().await {
                        Ok(()) => {
                            counter!("hyperbytedb_retention_runs_total").increment(1);
                        }
                        Err(e) => {
                            tracing::error!(error = %e, "retention enforcement error");
                        }
                    }
                }
                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        tracing::info!("retention service received shutdown");
                        break;
                    }
                }
            }
        }
    }

    async fn enforce(&self) -> Result<(), HyperbytedbError> {
        if !self.is_raft_leader() {
            tracing::debug!(
                node_id = self.node_id,
                "skipping retention tick: not raft leader"
            );
            return Ok(());
        }

        let now_nanos = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);
        let databases = self.metadata.list_databases().await?;

        for db in &databases {
            let rps = match self.metadata.list_retention_policies(&db.name).await {
                Ok(r) => r,
                Err(e) => {
                    tracing::error!(db = %db.name, error = %e, "retention: failed to list retention policies, skipping database");
                    continue;
                }
            };

            for rp in &rps {
                let duration = match rp.duration {
                    Some(d) if !d.is_zero() => d,
                    Some(_) => {
                        tracing::debug!(
                            db = %db.name,
                            rp = %rp.name,
                            "retention: duration is zero (infinite), skipping"
                        );
                        continue;
                    }
                    None => continue,
                };

                tracing::debug!(
                    db = %db.name,
                    rp = %rp.name,
                    duration_secs = duration.as_secs(),
                    "retention: enforcing finite retention policy"
                );

                let cutoff_nanos = now_nanos - (duration.as_nanos() as i64);

                let measurements = match self
                    .metadata
                    .list_measurements_for_rp(&db.name, &rp.name)
                    .await
                {
                    Ok(m) => m,
                    Err(e) => {
                        tracing::error!(
                            db = %db.name,
                            rp = %rp.name,
                            error = %e,
                            "retention: failed to list measurements for retention policy, skipping"
                        );
                        continue;
                    }
                };

                for meas in &measurements {
                    let fact_table = quoted_table_name(&db.name, &rp.name, meas);
                    let series_table = quoted_series_table_name(&db.name, &rp.name, meas);
                    for table in [fact_table, series_table] {
                        let sql = format!("ALTER TABLE {table} DELETE WHERE time < {cutoff_nanos}");
                        match self.query.execute_sql(&sql).await {
                            Ok(_) => {
                                tracing::debug!(
                                    db = %db.name,
                                    rp = %rp.name,
                                    measurement = %meas,
                                    table = %table,
                                    "retention ALTER DELETE issued"
                                );
                                counter!("hyperbytedb_retention_delete_mutations_total")
                                    .increment(1);
                            }
                            Err(e) => {
                                tracing::warn!(
                                    db = %db.name,
                                    rp = %rp.name,
                                    measurement = %meas,
                                    table = %table,
                                    error = %e,
                                    "retention: ALTER DELETE failed (table may not exist yet)"
                                );
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }
}
