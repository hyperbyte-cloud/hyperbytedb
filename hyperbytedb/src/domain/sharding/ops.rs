use serde::{Deserialize, Serialize};

use super::types::{MeasurementKey, ShardEpoch, ShardRegion};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ShardMapOp {
    BootstrapMeasurement {
        key: MeasurementKey,
        region: ShardRegion,
    },
    Split {
        key: MeasurementKey,
        region_id: u64,
        split_key: u64,
        #[serde(default)]
        epoch: ShardEpoch,
        left: ShardRegion,
        right: ShardRegion,
    },
    Merge {
        key: MeasurementKey,
        left_region_id: u64,
        right_region_id: u64,
        #[serde(default)]
        epoch: ShardEpoch,
        merged: ShardRegion,
    },
    MovePeer {
        key: MeasurementKey,
        region_id: u64,
        from_peer: u64,
        to_peer: u64,
        #[serde(default)]
        epoch: ShardEpoch,
    },
    TransferPrimary {
        key: MeasurementKey,
        region_id: u64,
        new_primary: u64,
        #[serde(default)]
        epoch: ShardEpoch,
    },
}

impl ShardMapOp {
    pub fn measurement_key(&self) -> &MeasurementKey {
        match self {
            ShardMapOp::BootstrapMeasurement { key, .. }
            | ShardMapOp::Split { key, .. }
            | ShardMapOp::Merge { key, .. }
            | ShardMapOp::MovePeer { key, .. }
            | ShardMapOp::TransferPrimary { key, .. } => key,
        }
    }
}

fn sort_space_regions(space: &mut super::types::MeasurementShardSpace) {
    space.regions.sort_by_key(|r| r.start);
}

/// Apply `op` to an in-memory map snapshot (used by Raft apply and unit tests).
pub fn apply_shard_map_op(map: &mut super::types::ShardMap, op: ShardMapOp) -> Result<(), String> {
    map.map_version = map.map_version.saturating_add(1);
    match op {
        ShardMapOp::BootstrapMeasurement { key, region } => {
            if map.spaces.contains_key(&key) {
                return Err(format!(
                    "measurement shard space already exists: {}.{}.{}",
                    key.db, key.rp, key.measurement
                ));
            }
            if map.spaces.values().any(|space| {
                space
                    .regions
                    .iter()
                    .any(|r| r.region_id == region.region_id)
            }) {
                return Err(format!("duplicate region_id {}", region.region_id));
            }
            map.next_region_id = map.next_region_id.max(region.region_id.saturating_add(1));
            let mut space = super::types::MeasurementShardSpace {
                key: key.clone(),
                regions: vec![region],
            };
            sort_space_regions(&mut space);
            space.validate()?;
            map.spaces.insert(key, space);
        }
        ShardMapOp::Split {
            key,
            region_id,
            split_key,
            epoch,
            left,
            right,
        } => {
            let space = map
                .spaces
                .get_mut(&key)
                .ok_or_else(|| format!("unknown measurement space: {:?}", key))?;
            let idx = space
                .regions
                .iter()
                .position(|r| r.region_id == region_id)
                .ok_or_else(|| format!("unknown region_id {region_id}"))?;
            let old = &space.regions[idx];
            if old.epoch != epoch {
                return Err("stale epoch on Split".into());
            }
            if split_key <= old.start || split_key >= old.end {
                return Err(format!(
                    "split_key {split_key} not inside region [{}, {})",
                    old.start, old.end
                ));
            }
            if left.end != split_key || right.start != split_key {
                return Err("split regions must meet at split_key".into());
            }
            map.next_region_id = map.next_region_id.max(right.region_id.saturating_add(1));
            space.regions[idx] = left;
            space.regions.insert(idx + 1, right);
            sort_space_regions(space);
            space.validate()?;
        }
        ShardMapOp::Merge {
            key,
            left_region_id,
            right_region_id,
            epoch,
            merged,
        } => {
            let space = map
                .spaces
                .get_mut(&key)
                .ok_or_else(|| format!("unknown measurement space: {:?}", key))?;
            let li = space
                .regions
                .iter()
                .position(|r| r.region_id == left_region_id)
                .ok_or_else(|| format!("unknown left region {left_region_id}"))?;
            let left_epoch = space.regions[li].epoch;
            if left_epoch != epoch {
                return Err("stale epoch on Merge".into());
            }
            let ri = space
                .regions
                .iter()
                .position(|r| r.region_id == right_region_id)
                .ok_or_else(|| format!("unknown right region {right_region_id}"))?;
            if ri != li + 1 {
                return Err("merge regions must be adjacent".into());
            }
            space.regions.remove(ri);
            space.regions[li] = merged;
            sort_space_regions(space);
            space.validate()?;
        }
        ShardMapOp::MovePeer {
            key,
            region_id,
            from_peer,
            to_peer,
            epoch,
        } => {
            let space = map
                .spaces
                .get_mut(&key)
                .ok_or_else(|| format!("unknown measurement space: {:?}", key))?;
            let region = space
                .regions
                .iter_mut()
                .find(|r| r.region_id == region_id)
                .ok_or_else(|| format!("unknown region {region_id}"))?;
            if region.epoch != epoch {
                return Err("stale epoch on MovePeer".into());
            }
            if !region.peers.contains(&from_peer) {
                return Err(format!("peer {from_peer} not in region"));
            }
            region.peers.retain(|p| *p != from_peer);
            if !region.peers.contains(&to_peer) {
                region.peers.push(to_peer);
            }
            region.epoch = epoch.bump_conf_ver();
            if region.primary == from_peer {
                region.primary = to_peer;
            }
            sort_space_regions(space);
        }
        ShardMapOp::TransferPrimary {
            key,
            region_id,
            new_primary,
            epoch,
        } => {
            let space = map
                .spaces
                .get_mut(&key)
                .ok_or_else(|| format!("unknown measurement space: {:?}", key))?;
            let region = space
                .regions
                .iter_mut()
                .find(|r| r.region_id == region_id)
                .ok_or_else(|| format!("unknown region {region_id}"))?;
            if region.epoch != epoch {
                return Err("stale epoch on TransferPrimary".into());
            }
            if !region.peers.contains(&new_primary) {
                return Err(format!("new primary {new_primary} not a peer"));
            }
            region.primary = new_primary;
            region.epoch = epoch.bump_conf_ver();
            sort_space_regions(space);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::sharding::types::{MeasurementKey, ShardEpoch, ShardMap, ShardRegion};

    fn sample_region(id: u64, start: u64, end: u64, primary: u64) -> ShardRegion {
        ShardRegion {
            region_id: id,
            start,
            end,
            epoch: ShardEpoch::default(),
            peers: vec![primary, primary + 1],
            primary,
            last_split_at: 0,
        }
    }

    #[test]
    fn merge_adjacent_regions() {
        let key = MeasurementKey::new("db", "autogen", "cpu");
        let split_key = u64::MAX / 2;
        let left = sample_region(1, 0, split_key, 1);
        let right = sample_region(2, split_key, u64::MAX, 1);
        let mut map = ShardMap::default();
        apply_shard_map_op(
            &mut map,
            ShardMapOp::BootstrapMeasurement {
                key: key.clone(),
                region: sample_region(1, 0, u64::MAX, 1),
            },
        )
        .unwrap();
        apply_shard_map_op(
            &mut map,
            ShardMapOp::Split {
                key: key.clone(),
                region_id: 1,
                split_key,
                epoch: ShardEpoch::default(),
                left: left.clone(),
                right: right.clone(),
            },
        )
        .unwrap();
        let merged = sample_region(1, 0, u64::MAX, 1);
        apply_shard_map_op(
            &mut map,
            ShardMapOp::Merge {
                key,
                left_region_id: 1,
                right_region_id: 2,
                epoch: left.epoch,
                merged,
            },
        )
        .unwrap();
        assert_eq!(map.spaces.values().next().unwrap().regions.len(), 1);
    }

    #[test]
    fn move_peer_updates_primary_when_primary_leaves() {
        let key = MeasurementKey::new("db", "autogen", "cpu");
        let region = sample_region(1, 0, u64::MAX, 1);
        let mut map = ShardMap::default();
        apply_shard_map_op(
            &mut map,
            ShardMapOp::BootstrapMeasurement {
                key: key.clone(),
                region: region.clone(),
            },
        )
        .unwrap();
        apply_shard_map_op(
            &mut map,
            ShardMapOp::MovePeer {
                key,
                region_id: 1,
                from_peer: 1,
                to_peer: 2,
                epoch: region.epoch,
            },
        )
        .unwrap();
        let r = &map.spaces.values().next().unwrap().regions[0];
        assert_eq!(r.primary, 2);
        assert!(!r.peers.contains(&1));
        assert!(r.peers.contains(&2));
    }

    #[test]
    fn apply_keeps_regions_sorted_by_start() {
        let key = MeasurementKey::new("db", "autogen", "cpu");
        let mut map = ShardMap::default();
        apply_shard_map_op(
            &mut map,
            ShardMapOp::BootstrapMeasurement {
                key: key.clone(),
                region: sample_region(1, 0, u64::MAX, 1),
            },
        )
        .unwrap();
        let split_key = 1u64 << 40;
        apply_shard_map_op(
            &mut map,
            ShardMapOp::Split {
                key,
                region_id: 1,
                split_key,
                epoch: ShardEpoch::default(),
                left: sample_region(1, 0, split_key, 1),
                right: sample_region(2, split_key, u64::MAX, 1),
            },
        )
        .unwrap();
        let starts: Vec<u64> = map
            .spaces
            .values()
            .next()
            .unwrap()
            .regions
            .iter()
            .map(|r| r.start)
            .collect();
        assert_eq!(starts, vec![0, split_key]);
    }

    #[test]
    fn transfer_primary_changes_owner() {
        let key = MeasurementKey::new("db", "autogen", "cpu");
        let region = sample_region(1, 0, u64::MAX, 1);
        let mut map = ShardMap::default();
        apply_shard_map_op(
            &mut map,
            ShardMapOp::BootstrapMeasurement {
                key: key.clone(),
                region: region.clone(),
            },
        )
        .unwrap();
        apply_shard_map_op(
            &mut map,
            ShardMapOp::TransferPrimary {
                key,
                region_id: 1,
                new_primary: 2,
                epoch: region.epoch,
            },
        )
        .unwrap();
        let r = &map.spaces.values().next().unwrap().regions[0];
        assert_eq!(r.primary, 2);
    }

    #[test]
    fn bootstrap_assigns_unique_region_ids_across_measurements() {
        let mut map = ShardMap::default();
        apply_shard_map_op(
            &mut map,
            ShardMapOp::BootstrapMeasurement {
                key: MeasurementKey::new("db", "autogen", "cpu"),
                region: sample_region(1, 0, u64::MAX, 1),
            },
        )
        .unwrap();
        apply_shard_map_op(
            &mut map,
            ShardMapOp::BootstrapMeasurement {
                key: MeasurementKey::new("db", "autogen", "mem"),
                region: sample_region(2, 0, u64::MAX, 1),
            },
        )
        .unwrap();
        assert_eq!(map.next_region_id, 3);
        let ids: Vec<u64> = map
            .spaces
            .values()
            .flat_map(|s| s.regions.iter().map(|r| r.region_id))
            .collect();
        assert_eq!(ids.len(), 2);
        assert_ne!(ids[0], ids[1]);
    }

    #[test]
    fn bootstrap_rejects_duplicate_region_id() {
        let mut map = ShardMap::default();
        apply_shard_map_op(
            &mut map,
            ShardMapOp::BootstrapMeasurement {
                key: MeasurementKey::new("db", "autogen", "cpu"),
                region: sample_region(1, 0, u64::MAX, 1),
            },
        )
        .unwrap();
        let err = apply_shard_map_op(
            &mut map,
            ShardMapOp::BootstrapMeasurement {
                key: MeasurementKey::new("db", "autogen", "mem"),
                region: sample_region(1, 0, u64::MAX, 1),
            },
        )
        .unwrap_err();
        assert!(err.contains("duplicate region_id"));
    }

    #[test]
    fn split_rejects_duplicate_region_id_within_space() {
        let key = MeasurementKey::new("db", "autogen", "cpu");
        let mut map = ShardMap::default();
        apply_shard_map_op(
            &mut map,
            ShardMapOp::BootstrapMeasurement {
                key: key.clone(),
                region: sample_region(1, 0, u64::MAX, 1),
            },
        )
        .unwrap();
        let split_key = 1u64 << 40;
        let err = apply_shard_map_op(
            &mut map,
            ShardMapOp::Split {
                key,
                region_id: 1,
                split_key,
                epoch: ShardEpoch::default(),
                left: sample_region(1, 0, split_key, 1),
                right: sample_region(1, split_key, u64::MAX, 1),
            },
        )
        .unwrap_err();
        assert!(err.contains("duplicate region_id"));
    }
}
