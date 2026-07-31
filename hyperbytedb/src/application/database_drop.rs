//! Full local teardown for `DROP DATABASE` (metadata + MV + WAL + chDB).

use std::sync::Arc;

use crate::application::materialized_view_service::MaterializedViewService;
use crate::error::HyperbytedbError;
use crate::ports::metadata::MetadataPort;
use crate::ports::points_sink::PointsSinkPort;
use crate::ports::wal::WalPort;

/// Drop a database and await full local cleanup.
///
/// MV and chDB table drops run concurrently but the caller waits for every task
/// to finish before returning. WAL purge and metadata removal are awaited inline.
pub async fn drop_database(
    metadata: &Arc<dyn MetadataPort>,
    mv_service: Option<&MaterializedViewService>,
    points_sink: Option<&Arc<dyn PointsSinkPort>>,
    wal: Option<&Arc<dyn WalPort>>,
    name: &str,
) -> Result<(), HyperbytedbError> {
    metadata
        .get_database(name)
        .await?
        .ok_or_else(|| HyperbytedbError::DatabaseNotFound(name.to_string()))?;

    let to_drop: Vec<(String, String)> = {
        let rps = metadata.list_retention_policies(name).await?;
        let mut pairs = Vec::new();
        for rp in &rps {
            let measurements = metadata.list_measurements_for_rp(name, &rp.name).await?;
            for m in measurements {
                pairs.push((rp.name.clone(), m));
            }
        }
        pairs
    };

    if let Some(mv) = mv_service
        && let Err(e) = mv.drop_all_in_database(name).await
    {
        tracing::warn!(
            db = name,
            error = %e,
            "failed to cascade-drop materialized views for database"
        );
    }

    if let Some(wal) = wal {
        wal.purge_database(name).await?;
    }

    metadata.drop_database(name).await?;

    if let Some(sink) = points_sink {
        let db = name.to_string();
        let mut handles = Vec::with_capacity(to_drop.len());
        for (rp, measurement) in to_drop {
            let sink = sink.clone();
            let db = db.clone();
            handles.push(tokio::spawn(async move {
                sink.drop_measurement(&db, &rp, &measurement).await
            }));
        }
        for handle in handles {
            match handle.await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    tracing::warn!(
                        db = name,
                        error = %e,
                        "failed to drop chDB native table during DROP DATABASE"
                    );
                }
                Err(e) => {
                    tracing::warn!(
                        db = name,
                        error = %e,
                        "chDB drop task panicked during DROP DATABASE"
                    );
                }
            }
        }
    }

    Ok(())
}
