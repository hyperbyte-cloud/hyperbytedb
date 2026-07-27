use std::sync::Arc;

use crate::domain::cluster::sync::{
    DatabaseManifest, MeasurementManifest, RegionWatermark, SyncManifest,
};
use crate::error::HyperbytedbError;
use crate::ports::metadata::MetadataPort;
use crate::ports::sharding::ShardMapPort;
use crate::ports::wal::WalPort;

/// Build a sync manifest from the current node's state (metadata + WAL only).
pub async fn build_manifest(
    node_id: u64,
    metadata: &Arc<dyn MetadataPort>,
    wal: &Arc<dyn WalPort>,
    shard_map: Option<&dyn ShardMapPort>,
) -> Result<SyncManifest, HyperbytedbError> {
    let wal_last_seq = wal.last_sequence().await?;
    let databases = metadata.list_databases().await?;
    let users = metadata.list_users().await?;

    let shard_snapshot = if let Some(map) = shard_map {
        Some(map.snapshot().await?)
    } else {
        None
    };
    let shard_map_version = shard_snapshot
        .as_ref()
        .map(|m| m.map_version)
        .unwrap_or(0);

    let mut db_manifests = Vec::new();
    for db in &databases {
        let rps = metadata.list_retention_policies(&db.name).await?;
        let measurements = metadata.list_measurements(&db.name).await?;
        let cqs = metadata.list_continuous_queries(&db.name).await?;
        let cq_names: Vec<String> = cqs.into_iter().map(|c| c.name).collect();

        let mut tombstone_list = Vec::new();
        for rp in &rps {
            for meas in &measurements {
                let ts = metadata.list_tombstones(&db.name, &rp.name, meas).await?;
                if !ts.is_empty() {
                    tombstone_list.push((meas.clone(), ts));
                }
            }
        }

        let mut seen = std::collections::HashSet::new();
        let mut meas_manifests = Vec::new();
        for rp in &rps {
            for meas in &measurements {
                let key = (rp.name.clone(), meas.clone());
                if seen.insert(key) {
                    let region_watermarks = if let Some(ref snap) = shard_snapshot {
                        snap.space(&db.name, &rp.name, meas)
                            .map(|space| {
                                space
                                    .regions
                                    .iter()
                                    .filter(|r| r.primary == node_id)
                                    .map(|r| RegionWatermark {
                                        region_id: r.region_id,
                                        wal_watermark: wal_last_seq,
                                    })
                                    .collect()
                            })
                            .unwrap_or_default()
                    } else {
                        Vec::new()
                    };
                    meas_manifests.push(MeasurementManifest {
                        name: meas.clone(),
                        rp: rp.name.clone(),
                        region_watermarks,
                    });
                }
            }
        }

        db_manifests.push(DatabaseManifest {
            name: db.name.clone(),
            retention_policies: rps,
            measurements: meas_manifests,
            users: users.clone(),
            continuous_queries: cq_names,
            tombstones: tombstone_list,
        });
    }

    Ok(SyncManifest {
        node_id,
        wal_last_seq,
        databases: db_manifests,
        shard_map_version,
    })
}
