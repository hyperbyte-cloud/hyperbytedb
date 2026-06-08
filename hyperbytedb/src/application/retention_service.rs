use metrics::counter;
use std::sync::Arc;
use tokio::sync::watch;

use crate::domain::chdb_naming::quoted_table_name;
use crate::error::HyperbytedbError;
use crate::ports::metadata::MetadataPort;
use crate::ports::query::QueryPort;

pub struct RetentionService {
    metadata: Arc<dyn MetadataPort>,
    query: Arc<dyn QueryPort>,
}

impl RetentionService {
    pub fn new(metadata: Arc<dyn MetadataPort>, query: Arc<dyn QueryPort>) -> Self {
        Self { metadata, query }
    }

    pub async fn run(&self, interval: std::time::Duration, mut shutdown_rx: watch::Receiver<bool>) {
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        tracing::info!("retention service started, interval = {:?}", interval);
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    tracing::debug!("retention enforcement tick");
                    match self.enforce().await {
                        Ok(()) => {
                            counter!("hyperbytedb_retention_runs_total").increment(1);
                        }
                        Err(e) => {
                            tracing::error!("retention enforcement error: {}", e);
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

                let measurements = match self.metadata.list_measurements(&db.name).await {
                    Ok(m) => m,
                    Err(e) => {
                        tracing::error!(db = %db.name, error = %e, "retention: failed to list measurements, skipping database");
                        continue;
                    }
                };

                for meas in &measurements {
                    let table = quoted_table_name(&db.name, &rp.name, meas);
                    let sql = format!("ALTER TABLE {table} DELETE WHERE time < {cutoff_nanos}");
                    match self.query.execute_sql(&sql).await {
                        Ok(_) => {
                            tracing::debug!(
                                db = %db.name,
                                rp = %rp.name,
                                measurement = %meas,
                                "retention ALTER DELETE issued"
                            );
                            counter!("hyperbytedb_retention_delete_mutations_total").increment(1);
                        }
                        Err(e) => {
                            tracing::warn!(
                                db = %db.name,
                                rp = %rp.name,
                                measurement = %meas,
                                error = %e,
                                "retention: ALTER DELETE failed (table may not exist yet)"
                            );
                        }
                    }
                }
            }
        }

        Ok(())
    }
}
