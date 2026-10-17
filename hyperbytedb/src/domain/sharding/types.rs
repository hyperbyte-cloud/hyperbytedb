use std::collections::{HashMap, HashSet};

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

    /// True when `self` is an earlier state of the region that `other`
    /// describes: no counter ahead and at least one behind. A node whose map
    /// has not applied the latest op yet holds such an epoch.
    #[must_use]
    pub fn is_behind(self, other: Self) -> bool {
        self != other && self.conf_ver <= other.conf_ver && self.version <= other.version
    }
}

#[cfg(test)]
mod epoch_tests {
    use super::ShardEpoch;

    #[test]
    fn an_epoch_is_behind_only_a_strictly_later_one() {
        let e = |conf_ver, version| ShardEpoch { conf_ver, version };
        assert!(e(1, 0).is_behind(e(2, 0)));
        assert!(e(1, 0).is_behind(e(1, 1)));
        assert!(!e(1, 0).is_behind(e(1, 0)), "equal is not behind");
        assert!(!e(2, 0).is_behind(e(1, 0)), "ahead is not behind");
        assert!(!e(2, 0).is_behind(e(1, 1)), "diverged is not behind");
    }
}

/// Whether a region may serve reads and writes.
///
/// A split creates its child `Staging`: the region exists in the map with a
/// primary and peers and is being filled, but the donor still physically holds
/// its rows, so readers must not be sent to it. Publishing a child before its
/// data has moved is not a slow answer -- a scatter read returns zero rows
/// *successfully*, and nothing in the read path can tell that from a correct
/// empty result.
///
/// Cutover flips the child to `Serving` in one committed op, which every
/// node's location cache applies atomically.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RegionState {
    /// Routable. The default, so every region in an already-serialized map
    /// deserializes as serving -- which is correct: anything published today
    /// is serving.
    #[default]
    Serving,
    /// Present in the map but not routable; reads and writes for its range go
    /// to [`ShardRegion::staged_from`] until cutover.
    Staging,
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
    /// Durable reconciliation intent: `Some(false)` marks a split child whose
    /// primary changed and whose historical rows have not yet been verified
    /// as re-pushed. Cleared via [`crate::domain::sharding::ops::ShardMapOp::ClearVerified`]
    /// once movement verifies.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transfer_verified: Option<bool>,
    /// Unix seconds when the outstanding transfer debt was first recorded;
    /// survives restarts and leadership changes so age-based parking cannot
    /// be reset by failover.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transfer_first_seen: Option<i64>,
    /// Whether this region may serve reads and writes. See [`RegionState`].
    #[serde(default)]
    pub state: RegionState,
    /// While [`Self::state`] is `Staging`, the region whose primary still holds
    /// this range and must serve it. Cleared at cutover.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub staged_from: Option<u64>,
    /// Members of [`Self::peers`] that must NOT serve reads yet.
    ///
    /// The learner set. A peer is committed into `peers` BEFORE its data is
    /// copied, so replication starts flowing to it immediately and no write can
    /// slip through the gap -- this is TiKV's `AddLearner` ordering. Until it is
    /// promoted it must not answer reads, because it holds only a prefix of the
    /// range and scatter cannot tell a partial answer from a correct one.
    ///
    /// Copying first and committing afterwards (the obvious alternative) leaves
    /// exactly that gap: writes landing between the snapshot and the commit
    /// reach the primary alone, and a later primary transfer strands them.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub staging_peers: Vec<u64>,
}

impl ShardRegion {
    /// True when this region may be routed to.
    #[must_use]
    pub fn is_serving(&self) -> bool {
        matches!(self.state, RegionState::Serving)
    }

    /// True when `peer` is a member that may serve reads.
    ///
    /// A learner in [`Self::staging_peers`] is a member -- it receives
    /// replicated writes -- but holds only a prefix of the range until its
    /// copy finishes, so it must not answer.
    #[must_use]
    pub fn is_serving_peer(&self, peer: u64) -> bool {
        self.peers.contains(&peer) && !self.staging_peers.contains(&peer)
    }

    pub fn contains(&self, series_id: u64) -> bool {
        series_id >= self.start && series_id < self.end
    }

    /// True when this region carries unverified split-transfer debt.
    pub fn transfer_outstanding(&self) -> bool {
        self.transfer_verified == Some(false)
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

    /// Find the region exactly matching a committed `[start, end)` range.
    ///
    /// Used after a Split commits: region ids are reallocated at apply time
    /// (`apply_shard_map_op`), so the proposing node's child may carry a stale
    /// id. Matching by range is authoritative.
    #[must_use]
    pub fn region_with_range(&self, start: u64, end: u64) -> Option<&ShardRegion> {
        self.regions
            .iter()
            .find(|r| r.start == start && r.end == end)
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
        let mut seen = HashSet::new();
        for r in &self.regions {
            if !seen.insert(r.region_id) {
                return Err(format!("duplicate region_id {} within space", r.region_id));
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardMap {
    pub map_version: u64,
    /// Monotonic allocator for new region IDs (initialized from max on load).
    #[serde(default = "default_next_region_id")]
    pub next_region_id: u64,
    pub spaces: HashMap<MeasurementKey, MeasurementShardSpace>,
}

impl Default for ShardMap {
    fn default() -> Self {
        Self {
            map_version: 0,
            next_region_id: default_next_region_id(),
            spaces: HashMap::new(),
        }
    }
}

fn default_next_region_id() -> u64 {
    1
}

impl ShardMap {
    pub fn validate_global_region_ids(&self) -> Result<(), String> {
        let mut seen = HashSet::new();
        for space in self.spaces.values() {
            for r in &space.regions {
                if !seen.insert(r.region_id) {
                    return Err(format!("duplicate region_id {}", r.region_id));
                }
            }
        }
        Ok(())
    }

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

impl From<ShardMapJson> for ShardMap {
    fn from(json: ShardMapJson) -> Self {
        let mut spaces = HashMap::new();
        for space in json.spaces {
            spaces.insert(space.key.clone(), space);
        }
        Self {
            map_version: json.map_version,
            next_region_id: json.next_region_id,
            spaces,
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
            transfer_verified: None,
            transfer_first_seen: None,
            state: Default::default(),
            staged_from: None,
            staging_peers: Vec::new(),
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

    #[test]
    fn region_with_range_matches_committed_child_after_id_reallocation() {
        let split = 1u64 << 32;
        // Apply-time reallocation renamed child 2 -> 9; the proposer still
        // believes the right child is id 2. Range lookup must find id 9.
        let space = MeasurementShardSpace {
            key: MeasurementKey::new("db", "rp", "cpu"),
            regions: vec![region(1, 0, split), region(9, split, u64::MAX)],
        };
        assert_eq!(
            space.region_with_range(split, u64::MAX).unwrap().region_id,
            9
        );
        assert_eq!(space.region_with_range(0, split).unwrap().region_id, 1);
        assert!(space.region_with_range(0, u64::MAX).is_none());
    }
}

/// Header on region heartbeats listing what the reporting build supports.
/// A header, not a heartbeat field: `RegionHeartbeat` refuses unknown fields,
/// so a leader on an older build would have rejected every report.
pub const NODE_FEATURES_HEADER: &str = "x-hyperbytedb-features";
/// The node applies a `Merge` whose two regions are at different epochs
/// (`right_epoch`). Proposed only once every node reports it: an older node
/// would refuse such a merge that the others apply.
pub const FEATURE_CROSS_EPOCH_MERGE: &str = "cross-epoch-merge";

/// Request header asking `/internal/shard/rehome` to stream progress: a
/// newline every few seconds while copying, then the result JSON with its
/// status in `"status"`. A header, not a body field, because the request is
/// `deny_unknown_fields` and an older node would refuse the whole request;
/// an older node ignores the header and answers as before.
pub const REHOME_PROGRESS_HEADER: &str = "x-hyperbytedb-rehome-progress";

/// Per-region stats reported by store nodes to the Raft leader (PD-lite).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
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
    /// The reporter's median series id in the region, sent only once the
    /// region is big enough to split. The leader used its OWN index for this,
    /// which is complete only where the leader is a region peer; elsewhere
    /// it held a skewed handful of series, the "median" landed just above the
    /// range start, and a region split into dozens of near-empty slivers
    /// while the heavy half kept every series (seen at N=6 on n3).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub split_key_hint: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ShardWriteRequest {
    pub db: String,
    pub rp: String,
    pub precision: Option<String>,
    pub epoch: ShardEpoch,
    pub region_id: u64,
    pub body: Vec<u8>,
}

/// Several regions' forwarded writes for one primary node, in one request.
///
/// Forwarding one request per region made the cost of a client batch grow
/// with the region count: at 32 regions a 1,000-point batch became ~30
/// forwards of ~30 points, and six saturated nodes managed 21k points/s. One
/// request per destination node bounds the fan-out at N-1 whatever the
/// region count. Nothing waits to fill a batch: it is one client write,
/// grouped. Sent bincode-encoded to `/internal/shard/write-batch`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ShardWriteBatch {
    pub parts: Vec<ShardWriteRequest>,
}

/// The outcome of one part of a [`ShardWriteBatch`]: the HTTP status the
/// single-region endpoint would have returned for it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ShardWritePartResult {
    pub region_id: u64,
    pub status: u16,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ShardWriteBatchResponse {
    pub results: Vec<ShardWritePartResult>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ShardBootstrapRequest {
    pub db: String,
    pub rp: String,
    pub measurement: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
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
    /// Run on this node's local tables without the region checks: a
    /// materialized view destination's partials are per node, not per
    /// region. Omitted when false, so older nodes still accept the rest.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub node_local: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
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
    TagValues {
        tag_key: String,
    },
    Series,
    FieldKeys,
    /// The whole `MeasurementMeta`, or null when the node has none.
    Measurement,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
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
#[serde(deny_unknown_fields)]
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
#[serde(deny_unknown_fields)]
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
