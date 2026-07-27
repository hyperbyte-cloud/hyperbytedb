use async_trait::async_trait;
use parking_lot::RwLock;
use rocksdb::{IteratorMode, Options, DB};
use serde_json;
use std::path::Path;
use std::sync::Arc;

use crate::domain::sharding::{apply_shard_map_op, ShardMap, ShardMapOp, ShardRegion};
use crate::error::HyperbytedbError;
use crate::ports::sharding::ShardMapPort;

const SHARD_MAP_KEY: &[u8] = b"shardmap:global";
const HEARTBEAT_PREFIX: &str = "shardhb:";

fn heartbeat_key(region_id: u64, node_id: u64) -> Vec<u8> {
    format!("{HEARTBEAT_PREFIX}{region_id}:{node_id}").into_bytes()
}

pub struct RocksDbShardMap {
    db: Arc<DB>,
    cache: RwLock<ShardMap>,
    enabled: bool,
    node_id: u64,
}

impl RocksDbShardMap {
    pub fn open(meta_dir: &Path, enabled: bool, node_id: u64) -> Result<Self, HyperbytedbError> {
        let path = meta_dir.join("shard_map");
        std::fs::create_dir_all(&path).map_err(|e| HyperbytedbError::Storage(e.to_string()))?;
        let mut opts = Options::default();
        opts.create_if_missing(true);
        let db = DB::open(&opts, &path).map_err(|e| HyperbytedbError::Storage(e.to_string()))?;
        let db = Arc::new(db);
        let cache = load_map(&db)?;
        Ok(Self {
            db,
            cache: RwLock::new(cache),
            enabled,
            node_id,
        })
    }

    pub fn store_heartbeat(
        &self,
        region_id: u64,
        node_id: u64,
        payload: &[u8],
    ) -> Result<(), HyperbytedbError> {
        self.db
            .put(heartbeat_key(region_id, node_id), payload)
            .map_err(|e| HyperbytedbError::Storage(e.to_string()))
    }

    pub fn list_heartbeats(&self) -> Result<Vec<(u64, u64, Vec<u8>)>, HyperbytedbError> {
        let prefix = HEARTBEAT_PREFIX.as_bytes();
        let mut out = Vec::new();
        for item in self.db.iterator(IteratorMode::From(prefix, rocksdb::Direction::Forward)) {
            let (key, value) = item.map_err(|e| HyperbytedbError::Storage(e.to_string()))?;
            if !key.starts_with(prefix) {
                break;
            }
            let s = String::from_utf8_lossy(&key);
            let rest = s.strip_prefix(HEARTBEAT_PREFIX).unwrap_or("");
            let mut parts = rest.split(':');
            let region_id: u64 = parts
                .next()
                .and_then(|p| p.parse().ok())
                .unwrap_or(0);
            let node_id: u64 = parts
                .next()
                .and_then(|p| p.parse().ok())
                .unwrap_or(0);
            out.push((region_id, node_id, value.to_vec()));
        }
        Ok(out)
    }
}

fn load_map(db: &DB) -> Result<ShardMap, HyperbytedbError> {
    match db.get(SHARD_MAP_KEY).map_err(|e| HyperbytedbError::Storage(e.to_string()))? {
        Some(bytes) => {
            let persisted: PersistedShardMap = serde_json::from_slice(&bytes)
                .map_err(|e| HyperbytedbError::ShardMap(e.to_string()))?;
            Ok(persisted.into_shard_map())
        }
        None => Ok(ShardMap::default()),
    }
}

fn persist_map(db: &DB, map: &ShardMap) -> Result<(), HyperbytedbError> {
    let bytes = serde_json::to_vec(&PersistedShardMap::from(map))
        .map_err(|e| HyperbytedbError::ShardMap(e.to_string()))?;
    db.put(SHARD_MAP_KEY, bytes)
        .map_err(|e| HyperbytedbError::Storage(e.to_string()))
}

#[derive(serde::Serialize, serde::Deserialize)]
struct PersistedShardMap {
    map_version: u64,
    spaces: Vec<crate::domain::sharding::MeasurementShardSpace>,
}

impl PersistedShardMap {
    fn from(map: &ShardMap) -> Self {
        Self {
            map_version: map.map_version,
            spaces: map.spaces.values().cloned().collect(),
        }
    }

    fn into_shard_map(self) -> ShardMap {
        let mut spaces = std::collections::HashMap::new();
        for space in self.spaces {
            spaces.insert(space.key.clone(), space);
        }
        ShardMap {
            map_version: self.map_version,
            spaces,
        }
    }
}

#[async_trait]
impl ShardMapPort for RocksDbShardMap {
    fn enabled(&self) -> bool {
        self.enabled
    }

    async fn snapshot(&self) -> Result<ShardMap, HyperbytedbError> {
        Ok(self.cache.read().clone())
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
        let mut map = self.cache.write().clone();
        apply_shard_map_op(&mut map, op).map_err(HyperbytedbError::ShardMap)?;
        persist_map(&self.db, &map)?;
        *self.cache.write() = map.clone();
        Ok(map)
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

pub fn shared(
    meta_dir: &Path,
    enabled: bool,
    node_id: u64,
) -> Result<Arc<RocksDbShardMap>, HyperbytedbError> {
    Ok(Arc::new(RocksDbShardMap::open(meta_dir, enabled, node_id)?))
}
