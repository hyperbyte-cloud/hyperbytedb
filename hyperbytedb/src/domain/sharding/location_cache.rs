use std::collections::HashMap;
use std::sync::RwLock;

use super::types::{MeasurementKey, ShardEpoch, ShardMap, ShardRegion};

#[derive(Debug, Clone)]
struct CachedRegion {
    region: ShardRegion,
    #[allow(dead_code)] // reserved for future stale-entry eviction
    map_version: u64,
}

/// In-memory cache for shard routing (TiDB RegionCache analogue).
#[derive(Debug, Default)]
pub struct ShardLocationCache {
    inner: RwLock<CacheInner>,
}

#[derive(Debug, Default)]
struct CacheInner {
    map_version: u64,
    /// `(db, rp, measurement, series_id)` → region snapshot
    by_series: HashMap<(String, String, String, u64), CachedRegion>,
}

impl ShardLocationCache {
    pub fn new() -> Self {
        Self::default()
    }

    fn read_inner(&self) -> std::sync::RwLockReadGuard<'_, CacheInner> {
        self.inner
            .read()
            .unwrap_or_else(|e| e.into_inner())
    }

    fn write_inner(&self) -> std::sync::RwLockWriteGuard<'_, CacheInner> {
        self.inner
            .write()
            .unwrap_or_else(|e| e.into_inner())
    }

    pub fn refresh_from_map(&self, map: &ShardMap) {
        let mut inner = self.write_inner();
        if map.map_version >= inner.map_version {
            inner.map_version = map.map_version;
            inner.by_series.clear();
        }
    }

    pub fn invalidate_all(&self) {
        let mut inner = self.write_inner();
        inner.by_series.clear();
    }

    pub fn invalidate_measurement(&self, key: &MeasurementKey) {
        let mut inner = self.write_inner();
        inner.by_series.retain(|(db, rp, meas, _), _| {
            !(db == &key.db && rp == &key.rp && meas == &key.measurement)
        });
    }

    pub fn locate(
        &self,
        map: &ShardMap,
        db: &str,
        rp: &str,
        measurement: &str,
        series_id: u64,
    ) -> Option<ShardRegion> {
        {
            let inner = self.read_inner();
            if inner.map_version == map.map_version
                && let Some(cached) = inner.by_series.get(&(
                    db.to_string(),
                    rp.to_string(),
                    measurement.to_string(),
                    series_id,
                ))
            {
                return Some(cached.region.clone());
            }
        }

        let region = map.locate(db, rp, measurement, series_id)?.clone();
        let mut inner = self.write_inner();
        inner.map_version = map.map_version;
        inner.by_series.insert(
            (
                db.to_string(),
                rp.to_string(),
                measurement.to_string(),
                series_id,
            ),
            CachedRegion {
                region: region.clone(),
                map_version: map.map_version,
            },
        );
        Some(region)
    }

    pub fn check_epoch(&self, db: &str, rp: &str, measurement: &str, series_id: u64, epoch: ShardEpoch) -> bool {
        let inner = self.read_inner();
        inner
            .by_series
            .get(&(
                db.to_string(),
                rp.to_string(),
                measurement.to_string(),
                series_id,
            ))
            .is_some_and(|c| c.region.epoch == epoch)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::sharding::ops::{apply_shard_map_op, ShardMapOp};
    use crate::domain::sharding::types::ShardEpoch;

    fn sample_region(id: u64, start: u64, end: u64) -> ShardRegion {
        ShardRegion {
            region_id: id,
            start,
            end,
            epoch: ShardEpoch::default(),
            peers: vec![1, 2, 3],
            primary: 1,
            last_split_at: 0,
        health: Default::default(),
        }
    }

    #[test]
    fn bootstrap_and_locate() {
        let mut map = ShardMap::default();
        let key = MeasurementKey::new("db", "rp", "cpu");
        apply_shard_map_op(
            &mut map,
            ShardMapOp::BootstrapMeasurement {
                key: key.clone(),
                region: sample_region(1, 0, u64::MAX),
            },
        )
        .unwrap();

        let cache = ShardLocationCache::new();
        let r = cache
            .locate(&map, "db", "rp", "cpu", 42)
            .expect("located");
        assert_eq!(r.region_id, 1);
        assert!(r.contains(42));
    }

    #[test]
    fn split_preserves_full_coverage() {
        let mut map = ShardMap::default();
        let key = MeasurementKey::new("db", "rp", "cpu");
        apply_shard_map_op(
            &mut map,
            ShardMapOp::BootstrapMeasurement {
                key: key.clone(),
                region: sample_region(1, 0, u64::MAX),
            },
        )
        .unwrap();

        let split_key = 1u64 << 63;
        apply_shard_map_op(
            &mut map,
            ShardMapOp::Split {
                key: key.clone(),
                region_id: 1,
                split_key,
                left: sample_region(1, 0, split_key),
                right: sample_region(2, split_key, u64::MAX),
            },
        )
        .unwrap();

        let space = map.space("db", "rp", "cpu").unwrap();
        space.validate().unwrap();
        assert_eq!(space.regions.len(), 2);

        let cache = ShardLocationCache::new();
        cache.refresh_from_map(&map);
        assert_eq!(
            cache.locate(&map, "db", "rp", "cpu", 0).unwrap().region_id,
            1
        );
        assert_eq!(
            cache
                .locate(&map, "db", "rp", "cpu", u64::MAX - 1)
                .unwrap()
                .region_id,
            2
        );
    }
}
