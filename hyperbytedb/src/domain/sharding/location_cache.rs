use std::collections::HashMap;
use std::sync::RwLock;

use super::types::{MeasurementKey, ShardEpoch, ShardMap, ShardRegion};

#[derive(Debug, Clone)]
struct CachedMeasurement {
    map_version: u64,
    regions: Vec<ShardRegion>,
}

fn locate_in_regions(regions: &[ShardRegion], series_id: u64) -> Option<&ShardRegion> {
    if regions.is_empty() {
        return None;
    }
    let idx = regions.partition_point(|r| r.start <= series_id);
    let region = regions.get(idx.checked_sub(1)?)?;
    region.contains(series_id).then_some(region)
}

/// In-memory cache for shard routing (TiDB RegionCache analogue).
#[derive(Debug, Default)]
pub struct ShardLocationCache {
    inner: RwLock<CacheInner>,
}

#[derive(Debug, Default)]
struct CacheInner {
    map_version: u64,
    /// `(db, rp, measurement)` → sorted region ranges
    by_measurement: HashMap<(String, String, String), CachedMeasurement>,
}

/// Routing decision without cloning the region (peers vector included).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RegionRoute {
    pub region_id: u64,
    pub primary: u64,
    /// Whether `self_id` is a member of the region's peer set.
    pub self_is_peer: bool,
}

impl ShardLocationCache {
    pub fn new() -> Self {
        Self::default()
    }

    fn read_inner(&self) -> std::sync::RwLockReadGuard<'_, CacheInner> {
        self.inner.read().unwrap_or_else(|e| e.into_inner())
    }

    fn write_inner(&self) -> std::sync::RwLockWriteGuard<'_, CacheInner> {
        self.inner.write().unwrap_or_else(|e| e.into_inner())
    }

    fn measurement_key(db: &str, rp: &str, measurement: &str) -> (String, String, String) {
        (db.to_string(), rp.to_string(), measurement.to_string())
    }

    pub fn refresh_from_map(&self, map: &ShardMap) {
        let mut inner = self.write_inner();
        inner.map_version = inner.map_version.max(map.map_version);
        for space in map.spaces.values() {
            let key = Self::measurement_key(&space.key.db, &space.key.rp, &space.key.measurement);
            inner.by_measurement.insert(
                key,
                CachedMeasurement {
                    map_version: map.map_version,
                    regions: space.regions.clone(),
                },
            );
        }
    }

    pub fn refresh_measurement(&self, map: &ShardMap, key: &MeasurementKey) {
        let mut inner = self.write_inner();
        inner.map_version = inner.map_version.max(map.map_version);
        let cache_key = Self::measurement_key(&key.db, &key.rp, &key.measurement);
        if let Some(space) = map.space(&key.db, &key.rp, &key.measurement) {
            inner.by_measurement.insert(
                cache_key,
                CachedMeasurement {
                    map_version: map.map_version,
                    regions: space.regions.clone(),
                },
            );
        } else {
            inner.by_measurement.remove(&cache_key);
        }
    }

    pub fn invalidate_all(&self) {
        let mut inner = self.write_inner();
        inner.by_measurement.clear();
    }

    pub fn invalidate_measurement(&self, key: &MeasurementKey) {
        let mut inner = self.write_inner();
        inner
            .by_measurement
            .remove(&Self::measurement_key(&key.db, &key.rp, &key.measurement));
    }

    pub fn locate(
        &self,
        map: &ShardMap,
        db: &str,
        rp: &str,
        measurement: &str,
        series_id: u64,
    ) -> Option<ShardRegion> {
        let cache_key = Self::measurement_key(db, rp, measurement);
        {
            let inner = self.read_inner();
            if let Some(cached) = inner.by_measurement.get(&cache_key)
                && cached.map_version == map.map_version
                && let Some(region) = locate_in_regions(&cached.regions, series_id)
            {
                return Some(region.clone());
            }
        }

        let region = map.locate(db, rp, measurement, series_id)?.clone();
        let mut inner = self.write_inner();
        if let Some(space) = map.space(db, rp, measurement) {
            inner.map_version = inner.map_version.max(map.map_version);
            inner.by_measurement.insert(
                cache_key,
                CachedMeasurement {
                    map_version: map.map_version,
                    regions: space.regions.clone(),
                },
            );
        }
        Some(region)
    }

    /// Like [`Self::locate`] but allocation-free: returns just the routing
    /// fields the write path needs instead of cloning the full `ShardRegion`
    /// (including its peers vector) once per point.
    pub fn locate_route(
        &self,
        map: &ShardMap,
        db: &str,
        rp: &str,
        measurement: &str,
        series_id: u64,
        self_id: u64,
    ) -> Option<RegionRoute> {
        let cache_key = Self::measurement_key(db, rp, measurement);
        {
            let inner = self.read_inner();
            if let Some(cached) = inner.by_measurement.get(&cache_key)
                && cached.map_version == map.map_version
                && let Some(region) = locate_in_regions(&cached.regions, series_id)
            {
                return Some(RegionRoute {
                    region_id: region.region_id,
                    primary: region.primary,
                    self_is_peer: region.peers.contains(&self_id),
                });
            }
        }

        let region = map.locate(db, rp, measurement, series_id)?;
        let route = RegionRoute {
            region_id: region.region_id,
            primary: region.primary,
            self_is_peer: region.peers.contains(&self_id),
        };
        let mut inner = self.write_inner();
        if let Some(space) = map.space(db, rp, measurement) {
            inner.map_version = inner.map_version.max(map.map_version);
            inner.by_measurement.insert(
                cache_key,
                CachedMeasurement {
                    map_version: map.map_version,
                    regions: space.regions.clone(),
                },
            );
        }
        Some(route)
    }

    pub fn check_epoch(
        &self,
        map: &ShardMap,
        db: &str,
        rp: &str,
        measurement: &str,
        series_id: u64,
        epoch: ShardEpoch,
    ) -> bool {
        let cache_key = Self::measurement_key(db, rp, measurement);
        let inner = self.read_inner();
        if let Some(cached) = inner.by_measurement.get(&cache_key)
            && cached.map_version == map.map_version
        {
            return locate_in_regions(&cached.regions, series_id).is_some_and(|r| r.epoch == epoch);
        }
        map.locate(db, rp, measurement, series_id)
            .is_some_and(|r| r.epoch == epoch)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::sharding::ops::{ShardMapOp, apply_shard_map_op};
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
            transfer_verified: None,
            transfer_first_seen: None,
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
        let r = cache.locate(&map, "db", "rp", "cpu", 42).expect("located");
        assert_eq!(r.region_id, 1);
        assert!(r.contains(42));
    }

    #[test]
    fn locate_route_matches_locate_without_clone() {
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
        let warm = cache.locate(&map, "db", "rp", "cpu", 42).expect("located");
        let route = cache
            .locate_route(&map, "db", "rp", "cpu", 42, 1)
            .expect("route");
        assert_eq!(route.region_id, warm.region_id);
        assert_eq!(route.primary, warm.primary);
        // Peer 1 is in the region's peer set; node 99 is not.
        assert!(route.self_is_peer);
        let outsider = cache.locate_route(&map, "db", "rp", "cpu", 42, 99).unwrap();
        assert!(!outsider.self_is_peer);

        // Cold-cache path (fresh cache, no prior locate) agrees too.
        let cold = ShardLocationCache::new();
        let route2 = cold.locate_route(&map, "db", "rp", "cpu", 7, 1).unwrap();
        assert_eq!(route2.region_id, 1);
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
                epoch: ShardEpoch::default(),
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

    #[test]
    fn refresh_measurement_invalidates_only_one_measurement() {
        let mut map_a = ShardMap::default();
        let key_a = MeasurementKey::new("db", "rp", "cpu");
        apply_shard_map_op(
            &mut map_a,
            ShardMapOp::BootstrapMeasurement {
                key: key_a.clone(),
                region: sample_region(1, 0, u64::MAX),
            },
        )
        .unwrap();

        let mut map_b = map_a.clone();
        let key_b = MeasurementKey::new("db", "rp", "mem");
        apply_shard_map_op(
            &mut map_b,
            ShardMapOp::BootstrapMeasurement {
                key: key_b.clone(),
                region: sample_region(2, 0, u64::MAX),
            },
        )
        .unwrap();

        let cache = ShardLocationCache::new();
        cache.refresh_from_map(&map_b);

        let split_key = 1u64 << 62;
        apply_shard_map_op(
            &mut map_b,
            ShardMapOp::Split {
                key: key_a.clone(),
                region_id: 1,
                split_key,
                epoch: ShardEpoch::default(),
                left: sample_region(1, 0, split_key),
                right: sample_region(3, split_key, u64::MAX),
            },
        )
        .unwrap();
        cache.refresh_measurement(&map_b, &key_a);

        assert_eq!(
            cache
                .locate(&map_b, "db", "rp", "cpu", split_key)
                .unwrap()
                .region_id,
            3
        );
        assert_eq!(
            cache
                .locate(&map_b, "db", "rp", "mem", 99)
                .unwrap()
                .region_id,
            2
        );
    }
}
