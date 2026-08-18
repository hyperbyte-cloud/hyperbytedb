use std::collections::HashMap;

use serde::{Deserialize, Serialize};

/// TiDB-style Region epoch: `version` bumps on range change; `conf_ver` on peer set change.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ShardEpoch {
    pub conf_ver: u64,
    pub version: u64,
}

impl ShardEpoch {
    pub fn bump_version(self) -> Self {
        Self {
            version: self.version.saturating_add(1),
            ..self
        }
    }

    pub fn bump_conf_ver(self) -> Self {
        Self {
            conf_ver: self.conf_ver.saturating_add(1),
            ..self
        }
    }
}

/// Contiguous `[start, end)` range over `series_id` for one measurement.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardRegion {
    pub region_id: u64,
    pub start: u64,
    /// Exclusive upper bound.
    pub end: u64,
    #[serde(default)]
    pub epoch: ShardEpoch,
    pub peers: Vec<u64>,
    pub primary: u64,
    /// Unix seconds when this region was last split (for merge cooldown).
    #[serde(default)]
    pub last_split_at: u64,
}

impl ShardRegion {
    pub fn contains(&self, series_id: u64) -> bool {
        series_id >= self.start && series_id < self.end
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct MeasurementKey {
    pub db: String,
    pub rp: String,
    pub measurement: String,
}

impl MeasurementKey {
    pub fn new(
        db: impl Into<String>,
        rp: impl Into<String>,
        measurement: impl Into<String>,
    ) -> Self {
        Self {
            db: db.into(),
            rp: rp.into(),
            measurement: measurement.into(),
        }
    }
}

/// Ordered non-overlapping regions covering `[0, u64::MAX)` for one measurement.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MeasurementShardSpace {
    pub key: MeasurementKey,
    pub regions: Vec<ShardRegion>,
}

impl MeasurementShardSpace {
    pub fn locate(&self, series_id: u64) -> Option<&ShardRegion> {
        if self.regions.is_empty() {
            return None;
        }
        let idx = self.regions.partition_point(|r| r.start <= series_id);
        let region = self.regions.get(idx.checked_sub(1)?)?;
        region.contains(series_id).then_some(region)
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.regions.is_empty() {
            return Err("empty region list".into());
        }
        let mut sorted = self.regions.clone();
        sorted.sort_by_key(|r| r.start);
        if sorted[0].start != 0 {
            return Err(format!(
                "first region must start at 0, got {}",
                sorted[0].start
            ));
        }
        for w in sorted.windows(2) {
            if w[0].end != w[1].start {
                return Err(format!(
                    "gap or overlap between regions {} and {}",
                    w[0].region_id, w[1].region_id
                ));
            }
        }
        if sorted.last().map(|r| r.end) != Some(u64::MAX) {
            return Err("last region must end at u64::MAX".into());
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardMap {
    pub map_version: u64,
    /// Monotonic allocator for new region IDs (initialized from max on load).
    #[serde(default = "default_next_region_id")]
    pub next_region_id: u64,
    pub spaces: HashMap<MeasurementKey, MeasurementShardSpace>,
}

fn default_next_region_id() -> u64 {
    1
}

impl ShardMap {
    pub fn locate(
        &self,
        db: &str,
        rp: &str,
        measurement: &str,
        series_id: u64,
    ) -> Option<&ShardRegion> {
        let key = MeasurementKey::new(db, rp, measurement);
        self.spaces.get(&key)?.locate(series_id)
    }

    pub fn space(&self, db: &str, rp: &str, measurement: &str) -> Option<&MeasurementShardSpace> {
        self.spaces.get(&MeasurementKey::new(db, rp, measurement))
    }
}

/// JSON-safe shard map for HTTP responses (`HashMap` keys are not valid JSON object keys).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardMapJson {
    pub map_version: u64,
    #[serde(default = "default_next_region_id")]
    pub next_region_id: u64,
    pub spaces: Vec<MeasurementShardSpace>,
}

impl From<&ShardMap> for ShardMapJson {
    fn from(map: &ShardMap) -> Self {
        Self {
            map_version: map.map_version,
            next_region_id: map.next_region_id,
            spaces: map.spaces.values().cloned().collect(),
        }
    }
}

#[cfg(test)]
mod locate_tests {
    use super::*;

    fn region(id: u64, start: u64, end: u64) -> ShardRegion {
        ShardRegion {
            region_id: id,
            start,
            end,
            epoch: ShardEpoch::default(),
            peers: vec![1],
            primary: 1,
            last_split_at: 0,
        }
    }

    #[test]
    fn shard_region_deserializes_without_epoch() {
        let json = r#"{"region_id":1,"start":0,"end":100,"peers":[1],"primary":1}"#;
        let region: ShardRegion = serde_json::from_str(json).unwrap();
        assert_eq!(region.epoch, ShardEpoch::default());
    }

    #[test]
    fn locate_uses_binary_search_on_sorted_starts() {
        let split = 1u64 << 32;
        let space = MeasurementShardSpace {
            key: MeasurementKey::new("db", "rp", "cpu"),
            regions: vec![region(1, 0, split), region(2, split, u64::MAX)],
        };
        assert_eq!(space.locate(0).unwrap().region_id, 1);
        assert_eq!(space.locate(split - 1).unwrap().region_id, 1);
        assert_eq!(space.locate(split).unwrap().region_id, 2);
        assert_eq!(space.locate(u64::MAX - 1).unwrap().region_id, 2);
        assert!(space.locate(u64::MAX).is_none());
    }
}

/// Per-region stats reported by store nodes to the Raft leader (PD-lite).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RegionHeartbeat {
    pub region_id: u64,
    /// Store node that produced this heartbeat (region peer, not necessarily Raft leader).
    #[serde(default)]
    pub node_id: u64,
    pub series_count: u64,
    pub approx_bytes: u64,
    pub write_qps: u64,
    #[serde(default)]
    pub epoch: ShardEpoch,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardWriteRequest {
    pub db: String,
    pub rp: String,
    pub precision: Option<String>,
    pub epoch: ShardEpoch,
    pub region_id: u64,
    pub body: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardBootstrapRequest {
    pub db: String,
    pub rp: String,
    pub measurement: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardQueryRequest {
    pub db: String,
    pub rp: String,
    pub measurement: String,
    pub epoch: ShardEpoch,
    pub region_id: u64,
    pub time_min: i64,
    pub time_max: i64,
    pub series_id_start: u64,
    pub series_id_end: u64,
    pub select_sql: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardMetadataRequest {
    pub db: String,
    pub rp: String,
    pub measurement: String,
    pub epoch: ShardEpoch,
    pub region_id: u64,
    pub kind: ShardMetadataKind,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ShardMetadataKind {
    TagKeys,
    TagValues { tag_key: String },
    Series,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardDeleteRequest {
    pub db: String,
    pub rp: String,
    pub measurement: String,
    pub epoch: ShardEpoch,
    pub region_id: u64,
    /// Optional tombstone predicate fragment applied on the primary.
    #[serde(default)]
    pub predicate: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardTransferRequest {
    pub db: String,
    pub rp: String,
    pub measurement: String,
    pub region_id: u64,
    pub start: u64,
    pub end: u64,
    pub epoch: ShardEpoch,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MvBackfillPhase {
    Fact,
    Series,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardMvBackfillRequest {
    pub db: String,
    pub rp: String,
    pub epoch: ShardEpoch,
    pub region_id: u64,
    pub phase: MvBackfillPhase,
    pub sql: String,
    pub dest_db: String,
    pub dest_rp: String,
    pub dest_measurement: String,
}
