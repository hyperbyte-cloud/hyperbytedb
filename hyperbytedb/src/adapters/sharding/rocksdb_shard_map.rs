use async_trait::async_trait;
use parking_lot::RwLock;
use rocksdb::{DB, IteratorMode, Options, WriteBatch};
use serde_json;
use std::path::Path;
use std::sync::Arc;

use crate::domain::sharding::{
    MeasurementKey, ShardMap, ShardMapOp, ShardRegion, apply_shard_map_op,
};
use crate::error::HyperbytedbError;
use crate::ports::sharding::ShardMapPort;

/// Legacy whole-map key (read during migration, never written anymore).
const LEGACY_SHARD_MAP_KEY: &[u8] = b"shardmap:global";
const META_KEY: &[u8] = b"shardmap:meta";
const SPACE_PREFIX: &str = "shardmap:space:";

fn space_key(key: &MeasurementKey) -> Vec<u8> {
    // The components are joined with the ASCII unit separator, which cannot
    // occur inside database / retention / measurement identifiers. The value
    // carries the authoritative `MeasurementKey`; the encoded key only has to
    // be unique and prefix-scannable.
    format!(
        "{SPACE_PREFIX}{}\u{1f}{}\u{1f}{}",
        key.db, key.rp, key.measurement
    )
    .into_bytes()
}

#[derive(serde::Serialize, serde::Deserialize)]
struct PersistedShardMapMeta {
    map_version: u64,
    #[serde(default = "default_next_region_id")]
    next_region_id: u64,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct PersistedShardSpace {
    #[serde(flatten)]
    space: crate::domain::sharding::MeasurementShardSpace,
}

fn default_next_region_id() -> u64 {
    1
}

pub struct RocksDbShardMap {
    db: Arc<DB>,
    cache: RwLock<Arc<ShardMap>>,
    apply_lock: tokio::sync::Mutex<()>,
    enabled: bool,
}

impl RocksDbShardMap {
    pub fn open(meta_dir: &Path, enabled: bool) -> Result<Self, HyperbytedbError> {
        let path = meta_dir.join("shard_map");
        std::fs::create_dir_all(&path)
            .map_err(|e| HyperbytedbError::Storage(e.to_string().into()))?;
        let mut opts = Options::default();
        opts.create_if_missing(true);
        let db =
            DB::open(&opts, &path).map_err(|e| HyperbytedbError::Storage(e.to_string().into()))?;
        let db = Arc::new(db);
        let cache = Arc::new(load_map(&db)?);
        Ok(Self {
            db,
            cache: RwLock::new(cache),
            apply_lock: tokio::sync::Mutex::new(()),
            enabled,
        })
    }
}

fn load_meta(db: &DB) -> Result<PersistedShardMapMeta, HyperbytedbError> {
    match db
        .get(META_KEY)
        .map_err(|e| HyperbytedbError::Storage(e.to_string().into()))?
    {
        Some(bytes) => serde_json::from_slice(&bytes).map_err(|e| {
            HyperbytedbError::ShardMap(crate::error::ChainedError::with_context(
                "shard map meta corrupt",
                e,
            ))
        }),
        None => Ok(PersistedShardMapMeta {
            map_version: 0,
            next_region_id: default_next_region_id(),
        }),
    }
}

fn load_spaces(
    db: &DB,
) -> Result<Vec<crate::domain::sharding::MeasurementShardSpace>, HyperbytedbError> {
    let prefix = SPACE_PREFIX.as_bytes();
    let mut out = Vec::new();
    for item in db.iterator(IteratorMode::From(prefix, rocksdb::Direction::Forward)) {
        let (key, value) = item.map_err(|e| HyperbytedbError::Storage(e.to_string().into()))?;
        if !key.starts_with(prefix) {
            break;
        }
        let space: PersistedShardSpace = serde_json::from_slice(&value).map_err(|e| {
            HyperbytedbError::ShardMap(crate::error::ChainedError::with_context(
                "shard space corrupt",
                e,
            ))
        })?;
        out.push(space.space);
    }
    Ok(out)
}

/// Migrate the pre-per-space single-key format if present. Returns true when a
/// migration was performed (the caller must then re-load spaces).
fn migrate_legacy_if_present(db: &DB) -> Result<bool, HyperbytedbError> {
    let Some(bytes) = db
        .get(LEGACY_SHARD_MAP_KEY)
        .map_err(|e| HyperbytedbError::Storage(e.to_string().into()))?
    else {
        return Ok(false);
    };
    let legacy: PersistedShardMap = serde_json::from_slice(&bytes).map_err(|e| {
        HyperbytedbError::ShardMap(crate::error::ChainedError::with_context(
            "legacy shard map corrupt",
            e,
        ))
    })?;
    let map = legacy.into_shard_map();

    let mut batch = WriteBatch::default();
    batch.put(
        META_KEY,
        serde_json::to_vec(&PersistedShardMapMeta {
            map_version: map.map_version,
            next_region_id: map.next_region_id,
        })
        .map_err(|e| {
            HyperbytedbError::ShardMap(crate::error::ChainedError::with_context(
                "shard map meta serialize",
                e,
            ))
        })?,
    );
    for space in map.spaces.values() {
        batch.put(
            space_key(&space.key),
            serde_json::to_vec(&PersistedShardSpace {
                space: space.clone(),
            })
            .map_err(|e| {
                HyperbytedbError::ShardMap(crate::error::ChainedError::with_context(
                    "shard map meta serialize",
                    e,
                ))
            })?,
        );
    }
    batch.delete(LEGACY_SHARD_MAP_KEY);
    db.write(batch)
        .map_err(|e| HyperbytedbError::Storage(e.to_string().into()))?;
    tracing::info!(
        spaces = map.spaces.len(),
        "migrated shard map to per-space storage"
    );
    Ok(true)
}

fn load_map(db: &DB) -> Result<ShardMap, HyperbytedbError> {
    if migrate_legacy_if_present(db)? {
        return assemble_map(db);
    }
    assemble_map(db)
}

fn assemble_map(db: &DB) -> Result<ShardMap, HyperbytedbError> {
    let meta = load_meta(db)?;
    let spaces = load_spaces(db)?;

    let mut space_map = std::collections::HashMap::new();
    for space in spaces {
        // Fail startup loudly on invariant violations rather than serving
        // misrouted traffic from a corrupt map (locate assumes sorted,
        // contiguous coverage).
        space
            .validate()
            .map_err(|e| HyperbytedbError::ShardMap(e.into()))?;
        space_map.insert(space.key.clone(), space);
    }

    let mut map = ShardMap {
        map_version: meta.map_version,
        next_region_id: meta.next_region_id,
        spaces: space_map,
    };
    if map.next_region_id == 0 {
        map.next_region_id = map
            .spaces
            .values()
            .flat_map(|s| s.regions.iter().map(|r| r.region_id))
            .max()
            .map(|m| m.saturating_add(1))
            .unwrap_or(1);
    }
    map.validate_global_region_ids()
        .map_err(|e| HyperbytedbError::ShardMap(e.into()))?;
    Ok(map)
}

/// Atomically persist the full map (join catch-up / snapshot install).
fn persist_full_map(db: &DB, map: &ShardMap) -> Result<(), HyperbytedbError> {
    let existing = load_spaces(db)?;
    let incoming: std::collections::HashSet<&MeasurementKey> = map.spaces.keys().collect();
    let mut batch = WriteBatch::default();
    batch.put(
        META_KEY,
        serde_json::to_vec(&PersistedShardMapMeta {
            map_version: map.map_version,
            next_region_id: map.next_region_id,
        })
        .map_err(|e| {
            HyperbytedbError::ShardMap(crate::error::ChainedError::with_context(
                "shard map meta serialize",
                e,
            ))
        })?,
    );
    for space in &existing {
        if !incoming.contains(&space.key) {
            batch.delete(space_key(&space.key));
        }
    }
    for space in map.spaces.values() {
        batch.put(
            space_key(&space.key),
            serde_json::to_vec(&PersistedShardSpace {
                space: space.clone(),
            })
            .map_err(|e| {
                HyperbytedbError::ShardMap(crate::error::ChainedError::with_context(
                    "shard space serialize",
                    e,
                ))
            })?,
        );
    }
    db.write(batch)
        .map_err(|e| HyperbytedbError::Storage(e.to_string().into()))
}

/// Atomically persist the global counters plus the one space an op touched.
fn persist_space(
    db: &DB,
    map: &ShardMap,
    changed: &MeasurementKey,
) -> Result<(), HyperbytedbError> {
    let mut batch = WriteBatch::default();
    batch.put(
        META_KEY,
        serde_json::to_vec(&PersistedShardMapMeta {
            map_version: map.map_version,
            next_region_id: map.next_region_id,
        })
        .map_err(|e| {
            HyperbytedbError::ShardMap(crate::error::ChainedError::with_context(
                "shard map meta serialize",
                e,
            ))
        })?,
    );
    match map.space(&changed.db, &changed.rp, &changed.measurement) {
        Some(space) => batch.put(
            space_key(changed),
            serde_json::to_vec(&PersistedShardSpace {
                space: space.clone(),
            })
            .map_err(|e| {
                HyperbytedbError::ShardMap(crate::error::ChainedError::with_context(
                    "shard space serialize",
                    e,
                ))
            })?,
        ),
        None => batch.delete(space_key(changed)),
    }
    db.write(batch)
        .map_err(|e| HyperbytedbError::Storage(e.to_string().into()))
}

/// Legacy whole-map envelope (kept solely to migrate old deployments).
#[derive(serde::Serialize, serde::Deserialize)]
struct PersistedShardMap {
    map_version: u64,
    #[serde(default = "default_next_region_id")]
    next_region_id: u64,
    spaces: Vec<crate::domain::sharding::MeasurementShardSpace>,
}

impl PersistedShardMap {
    fn into_shard_map(self) -> ShardMap {
        let mut spaces = std::collections::HashMap::new();
        for space in self.spaces {
            spaces.insert(space.key.clone(), space);
        }
        let mut next_region_id = self.next_region_id;
        if next_region_id == 0 {
            next_region_id = spaces
                .values()
                .flat_map(|s| s.regions.iter().map(|r| r.region_id))
                .max()
                .map(|m| m.saturating_add(1))
                .unwrap_or(1);
        }
        ShardMap {
            map_version: self.map_version,
            next_region_id,
            spaces,
        }
    }
}

#[async_trait]
impl ShardMapPort for RocksDbShardMap {
    fn enabled(&self) -> bool {
        self.enabled
    }

    async fn snapshot(&self) -> Result<Arc<ShardMap>, HyperbytedbError> {
        Ok(Arc::clone(&*self.cache.read()))
    }

    async fn locate(
        &self,
        db: &str,
        rp: &str,
        measurement: &str,
        series_id: u64,
    ) -> Result<Option<ShardRegion>, HyperbytedbError> {
        Ok(self
            .cache
            .read()
            .locate(db, rp, measurement, series_id)
            .cloned())
    }

    async fn regions_for_measurement(
        &self,
        db: &str,
        rp: &str,
        measurement: &str,
    ) -> Result<Vec<ShardRegion>, HyperbytedbError> {
        Ok(self
            .cache
            .read()
            .space(db, rp, measurement)
            .map(|s| s.regions.clone())
            .unwrap_or_default())
    }

    async fn apply_op(&self, op: ShardMapOp) -> Result<ShardMap, HyperbytedbError> {
        let _guard = self.apply_lock.lock().await;
        let changed = op.measurement_key().clone();
        let mut map = (*self.cache.read()).as_ref().clone();
        apply_shard_map_op(&mut map, op).map_err(HyperbytedbError::from)?;
        persist_space(&self.db, &map, &changed)?;
        *self.cache.write() = Arc::new(map.clone());
        Ok(map)
    }

    async fn replace_map(&self, map: ShardMap) -> Result<(), HyperbytedbError> {
        let _guard = self.apply_lock.lock().await;
        for space in map.spaces.values() {
            space
                .validate()
                .map_err(|e| HyperbytedbError::ShardMap(e.into()))?;
        }
        map.validate_global_region_ids()
            .map_err(|e| HyperbytedbError::ShardMap(e.into()))?;
        persist_full_map(&self.db, &map)?;
        *self.cache.write() = Arc::new(map);
        Ok(())
    }

    async fn node_owns_measurement(
        &self,
        node_id: u64,
        db: &str,
        rp: &str,
        measurement: &str,
    ) -> Result<bool, HyperbytedbError> {
        if !self.enabled {
            return Ok(true);
        }
        Ok(self
            .cache
            .read()
            .space(db, rp, measurement)
            .is_some_and(|space| space.regions.iter().any(|r| r.peers.contains(&node_id))))
    }
}

pub fn shared(meta_dir: &Path, enabled: bool) -> Result<Arc<RocksDbShardMap>, HyperbytedbError> {
    Ok(Arc::new(RocksDbShardMap::open(meta_dir, enabled)?))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::sharding::ShardEpoch;

    fn region(id: u64, start: u64, end: u64) -> ShardRegion {
        ShardRegion {
            region_id: id,
            start,
            end,
            epoch: ShardEpoch::default(),
            peers: vec![1],
            primary: 1,
            last_split_at: 0,
            transfer_verified: None,
            transfer_first_seen: None,
        }
    }

    fn open_raw(path: &Path) -> DB {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        DB::open(&opts, path).unwrap()
    }

    #[tokio::test]
    async fn per_space_persistence_survives_reopen() {
        let dir = tempfile::tempdir().unwrap();
        {
            let sm = RocksDbShardMap::open(dir.path(), true).unwrap();
            sm.apply_op(ShardMapOp::BootstrapMeasurement {
                key: MeasurementKey::new("db", "rp", "cpu"),
                region: region(1, 0, u64::MAX),
            })
            .await
            .unwrap();
        }

        let reopened = RocksDbShardMap::open(dir.path(), true).unwrap();
        let map = reopened.snapshot().await.unwrap();
        assert!(map.space("db", "rp", "cpu").is_some());
        assert_eq!(map.next_region_id, 2);
    }

    #[tokio::test]
    async fn replace_map_installs_peer_snapshot() {
        let dir = tempfile::tempdir().unwrap();
        let local = RocksDbShardMap::open(dir.path(), true).unwrap();
        local
            .apply_op(ShardMapOp::BootstrapMeasurement {
                key: MeasurementKey::new("db", "rp", "old"),
                region: region(1, 0, u64::MAX),
            })
            .await
            .unwrap();

        let incoming = crate::domain::sharding::ShardMap {
            map_version: 4,
            next_region_id: 3,
            spaces: [(
                MeasurementKey::new("db", "rp", "cpu"),
                crate::domain::sharding::MeasurementShardSpace {
                    key: MeasurementKey::new("db", "rp", "cpu"),
                    regions: vec![region(2, 0, u64::MAX)],
                },
            )]
            .into_iter()
            .collect(),
        };
        local.replace_map(incoming).await.unwrap();
        let snap = local.snapshot().await.unwrap();
        assert_eq!(snap.map_version, 4);
        assert!(snap.space("db", "rp", "old").is_none());
        assert!(snap.space("db", "rp", "cpu").is_some());
    }

    #[tokio::test]
    async fn legacy_single_key_format_migrates_on_open() {
        let dir = tempfile::tempdir().unwrap();
        let map_dir = dir.path().join("shard_map");
        std::fs::create_dir_all(&map_dir).unwrap();
        {
            let db = open_raw(&map_dir);
            let legacy = PersistedShardMap {
                map_version: 7,
                next_region_id: 5,
                spaces: vec![crate::domain::sharding::MeasurementShardSpace {
                    key: MeasurementKey::new("db", "rp", "mem"),
                    regions: vec![region(4, 0, u64::MAX)],
                }],
            };
            db.put(LEGACY_SHARD_MAP_KEY, serde_json::to_vec(&legacy).unwrap())
                .unwrap();
        }
        let sm = RocksDbShardMap::open(dir.path(), true).unwrap();
        let map = sm.snapshot().await.unwrap();
        assert!(map.space("db", "rp", "mem").is_some());
        assert_eq!(map.next_region_id, 5);
        // Legacy key consumed so migration is not re-run.
        drop(sm);
        let raw = open_raw(&map_dir);
        assert!(raw.get(LEGACY_SHARD_MAP_KEY).unwrap().is_none());
    }

    #[test]
    fn invalid_persisted_space_fails_load() {
        let dir = tempfile::tempdir().unwrap();
        let map_dir = dir.path().join("shard_map");
        std::fs::create_dir_all(&map_dir).unwrap();
        {
            let db = open_raw(&map_dir);
            // Gap between regions violates contiguity.
            let broken = PersistedShardSpace {
                space: crate::domain::sharding::MeasurementShardSpace {
                    key: MeasurementKey::new("db", "rp", "cpu"),
                    regions: vec![region(1, 0, 100), region(2, 200, u64::MAX)],
                },
            };
            db.put(
                META_KEY,
                serde_json::to_vec(&PersistedShardMapMeta {
                    map_version: 1,
                    next_region_id: 3,
                })
                .unwrap(),
            )
            .unwrap();
            db.put(
                space_key(&MeasurementKey::new("db", "rp", "cpu")),
                serde_json::to_vec(&broken).unwrap(),
            )
            .unwrap();
        }
        assert!(RocksDbShardMap::open(dir.path(), true).is_err());
    }
}
