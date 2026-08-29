use serde::{Deserialize, Serialize};
use thiserror::Error;

use super::types::{MeasurementKey, ShardEpoch, ShardRegion};

#[derive(Debug, Error)]
pub enum ShardMapApplyError {
    #[error("measurement shard space already exists: {db}.{rp}.{measurement}")]
    SpaceExists {
        db: String,
        rp: String,
        measurement: String,
    },
    #[error("duplicate region_id {0}")]
    DuplicateRegionId(u64),
    #[error("unknown measurement space")]
    UnknownSpace,
    #[error("unknown region_id {0}")]
    UnknownRegion(u64),
    #[error("unknown left region {0}")]
    UnknownLeftRegion(u64),
    #[error("unknown right region {0}")]
    UnknownRightRegion(u64),
    #[error("stale epoch on {0}")]
    StaleEpoch(&'static str),
    #[error("{0}")]
    Invalid(String),
}

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
    /// Marks a split child's historical-row re-push as verified. Proposed by
    /// the Raft leader once post-commit movement confirms every exported
    /// point applied at the destination. Gated behind the rolling-upgrade
    /// window: old binaries cannot decode this variant.
    ClearVerified {
        key: MeasurementKey,
        region_id: u64,
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
            | ShardMapOp::TransferPrimary { key, .. }
            | ShardMapOp::ClearVerified { key, .. } => key,
        }
    }
}

fn sort_space_regions(space: &mut super::types::MeasurementShardSpace) {
    space.regions.sort_by_key(|r| r.start);
}

/// Apply `op` to an in-memory map snapshot (used by Raft apply and unit tests).
pub fn apply_shard_map_op(
    map: &mut super::types::ShardMap,
    op: ShardMapOp,
) -> Result<(), ShardMapApplyError> {
    map.map_version = map.map_version.saturating_add(1);
    match op {
        ShardMapOp::BootstrapMeasurement { key, region } => {
            if map.spaces.contains_key(&key) {
                return Err(ShardMapApplyError::SpaceExists {
                    db: key.db.clone(),
                    rp: key.rp.clone(),
                    measurement: key.measurement.clone(),
                });
            }
            if map.spaces.values().any(|space| {
                space
                    .regions
                    .iter()
                    .any(|r| r.region_id == region.region_id)
            }) {
                return Err(ShardMapApplyError::DuplicateRegionId(region.region_id));
            }
            map.next_region_id = map.next_region_id.max(region.region_id.saturating_add(1));
            let mut space = super::types::MeasurementShardSpace {
                key: key.clone(),
                regions: vec![region],
            };
            sort_space_regions(&mut space);
            space.validate().map_err(ShardMapApplyError::Invalid)?;
            map.spaces.insert(key, space);
        }
        ShardMapOp::Split {
            key,
            region_id,
            split_key,
            epoch,
            mut left,
            mut right,
        } => {
            // Allocate region_id at apply time so concurrent split proposals cannot
            // collide on a stale proposer-chosen id.
            let allocated = map.next_region_id;
            if map
                .spaces
                .values()
                .any(|s| s.regions.iter().any(|r| r.region_id == allocated))
            {
                return Err(ShardMapApplyError::DuplicateRegionId(allocated));
            }
            right.region_id = allocated;
            map.next_region_id = allocated.saturating_add(1);

            let space = map
                .spaces
                .get_mut(&key)
                .ok_or(ShardMapApplyError::UnknownSpace)?;
            let idx = space
                .regions
                .iter()
                .position(|r| r.region_id == region_id)
                .ok_or(ShardMapApplyError::UnknownRegion(region_id))?;
            let old = &space.regions[idx];
            if old.epoch != epoch {
                return Err(ShardMapApplyError::StaleEpoch("Split"));
            }
            if split_key <= old.start || split_key >= old.end {
                return Err(ShardMapApplyError::Invalid(format!(
                    "split_key {split_key} not inside region [{}, {})",
                    old.start, old.end
                )));
            }
            if left.end != split_key || right.start != split_key {
                return Err(ShardMapApplyError::Invalid(
                    "split regions must meet at split_key".into(),
                ));
            }
            // Apply-time normalization (durable reconciliation intent): flag
            // every child whose primary differs from the parent's — that
            // child's historical rows must be re-pushed and verified before
            // the debt clears. `transfer_first_seen` propagates from the
            // parent so re-splitting a flagged region cannot reset the age
            // gate. Predicates are deterministic from (op, prior map), so all
            // replicas normalize identically.
            let inherited_first_seen = old.transfer_first_seen;
            for child in [&mut left, &mut right] {
                if child.primary != old.primary {
                    child.transfer_verified = Some(false);
                    child.transfer_first_seen = inherited_first_seen.or(child.transfer_first_seen);
                }
            }
            space.regions[idx] = left;
            space.regions.insert(idx + 1, right);
            sort_space_regions(space);
            space.validate().map_err(ShardMapApplyError::Invalid)?;
            map.validate_global_region_ids()
                .map_err(ShardMapApplyError::Invalid)?;
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
                .ok_or(ShardMapApplyError::UnknownSpace)?;
            let li = space
                .regions
                .iter()
                .position(|r| r.region_id == left_region_id)
                .ok_or(ShardMapApplyError::UnknownLeftRegion(left_region_id))?;
            let left_epoch = space.regions[li].epoch;
            if left_epoch != epoch {
                return Err(ShardMapApplyError::StaleEpoch("Merge"));
            }
            let ri = space
                .regions
                .iter()
                .position(|r| r.region_id == right_region_id)
                .ok_or(ShardMapApplyError::UnknownRightRegion(right_region_id))?;
            if ri != li + 1 {
                return Err(ShardMapApplyError::Invalid(
                    "merge regions must be adjacent".into(),
                ));
            }
            // The right half's data was staged onto the merged owner based on
            // its state at proposal time; a concurrently changed right region
            // (MovePeer/TransferPrimary) invalidates that staging.
            if space.regions[ri].epoch != epoch {
                return Err(ShardMapApplyError::StaleEpoch("Merge right region"));
            }
            space.regions.remove(ri);
            space.regions[li] = merged;
            sort_space_regions(space);
            space.validate().map_err(ShardMapApplyError::Invalid)?;
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
                .ok_or(ShardMapApplyError::UnknownSpace)?;
            let region = space
                .regions
                .iter_mut()
                .find(|r| r.region_id == region_id)
                .ok_or(ShardMapApplyError::UnknownRegion(region_id))?;
            if region.epoch != epoch {
                return Err(ShardMapApplyError::StaleEpoch("MovePeer"));
            }
            if !region.peers.contains(&from_peer) {
                return Err(ShardMapApplyError::Invalid(format!(
                    "peer {from_peer} not in region"
                )));
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
                .ok_or(ShardMapApplyError::UnknownSpace)?;
            let region = space
                .regions
                .iter_mut()
                .find(|r| r.region_id == region_id)
                .ok_or(ShardMapApplyError::UnknownRegion(region_id))?;
            if region.epoch != epoch {
                return Err(ShardMapApplyError::StaleEpoch("TransferPrimary"));
            }
            if !region.peers.contains(&new_primary) {
                return Err(ShardMapApplyError::Invalid(format!(
                    "new primary {new_primary} not a peer"
                )));
            }
            region.primary = new_primary;
            region.epoch = epoch.bump_conf_ver();
            sort_space_regions(space);
        }
        ShardMapOp::ClearVerified {
            key,
            region_id,
            epoch,
        } => {
            let space = map
                .spaces
                .get_mut(&key)
                .ok_or(ShardMapApplyError::UnknownSpace)?;
            let region = space
                .regions
                .iter_mut()
                .find(|r| r.region_id == region_id)
                .ok_or(ShardMapApplyError::UnknownRegion(region_id))?;
            if region.epoch != epoch {
                return Err(ShardMapApplyError::StaleEpoch("ClearVerified"));
            }
            region.transfer_verified = Some(true);
            region.transfer_first_seen = None;
            region.epoch = epoch.bump_conf_ver();
            sort_space_regions(space);
        }
    }
    Ok(())
}

impl From<ShardMapApplyError> for crate::error::HyperbytedbError {
    fn from(e: ShardMapApplyError) -> Self {
        crate::error::HyperbytedbError::ShardMap(crate::error::ChainedError::from_error(e))
    }
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
            transfer_verified: None,
            transfer_first_seen: None,
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
    fn merge_rejects_stale_right_epoch() {
        let key = MeasurementKey::new("db", "autogen", "cpu");
        let split_key = u64::MAX / 2;
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
                left: sample_region(1, 0, split_key, 1),
                right: sample_region(2, split_key, u64::MAX, 1),
            },
        )
        .unwrap();

        // Concurrently change the right region's peer set (bumps conf_ver).
        apply_shard_map_op(
            &mut map,
            ShardMapOp::TransferPrimary {
                key: key.clone(),
                region_id: 2,
                new_primary: sample_region(2, split_key, u64::MAX, 1).peers[1],
                epoch: ShardEpoch::default(),
            },
        )
        .unwrap();

        let err = apply_shard_map_op(
            &mut map,
            ShardMapOp::Merge {
                key,
                left_region_id: 1,
                right_region_id: 2,
                epoch: ShardEpoch::default(),
                merged: sample_region(1, 0, u64::MAX, 1),
            },
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("stale epoch on Merge right"),
            "{err}"
        );
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
        assert!(err.to_string().contains("duplicate region_id"));
    }

    #[test]
    fn split_assigns_next_region_id_at_apply() {
        let cpu_key = MeasurementKey::new("db", "autogen", "cpu");
        let mem_key = MeasurementKey::new("db", "autogen", "mem");
        let mut map = ShardMap::default();
        apply_shard_map_op(
            &mut map,
            ShardMapOp::BootstrapMeasurement {
                key: cpu_key.clone(),
                region: sample_region(1, 0, u64::MAX, 1),
            },
        )
        .unwrap();
        apply_shard_map_op(
            &mut map,
            ShardMapOp::BootstrapMeasurement {
                key: mem_key,
                region: sample_region(2, 0, u64::MAX, 1),
            },
        )
        .unwrap();
        let split_key = 1u64 << 40;
        apply_shard_map_op(
            &mut map,
            ShardMapOp::Split {
                key: cpu_key.clone(),
                region_id: 1,
                split_key,
                epoch: ShardEpoch::default(),
                left: sample_region(1, 0, split_key, 1),
                right: sample_region(99, split_key, u64::MAX, 1),
            },
        )
        .unwrap();
        let cpu = map.space("db", "autogen", "cpu").unwrap();
        let ids: Vec<u64> = cpu.regions.iter().map(|r| r.region_id).collect();
        assert_eq!(
            ids,
            vec![1, 3],
            "stale proposed right id must be replaced at apply"
        );
        assert_eq!(map.next_region_id, 4);
    }

    fn flagged_region(id: u64, start: u64, end: u64, primary: u64, first_seen: i64) -> ShardRegion {
        ShardRegion {
            transfer_verified: Some(false),
            transfer_first_seen: Some(first_seen),
            ..sample_region(id, start, end, primary)
        }
    }

    #[test]
    fn split_flags_only_changed_primary_children() {
        let key = MeasurementKey::new("db", "autogen", "cpu");
        let split_key = u64::MAX / 2;
        let mut map = ShardMap::default();
        apply_shard_map_op(
            &mut map,
            ShardMapOp::BootstrapMeasurement {
                key: key.clone(),
                region: sample_region(1, 0, u64::MAX, 1),
            },
        )
        .unwrap();
        let left = sample_region(1, 0, split_key, 1);
        let right = {
            let mut r = sample_region(2, split_key, u64::MAX, 2);
            r.peers = vec![2, 1];
            r.transfer_first_seen = Some(1_700_000_000);
            r
        };
        apply_shard_map_op(
            &mut map,
            ShardMapOp::Split {
                key,
                region_id: 1,
                split_key,
                epoch: ShardEpoch::default(),
                left,
                right,
            },
        )
        .unwrap();

        let regions = &map.space("db", "autogen", "cpu").unwrap().regions;
        assert!(
            !regions[0].transfer_outstanding(),
            "unchanged primary carries no debt"
        );
        assert_eq!(regions[0].transfer_verified, None);
        assert!(
            regions[1].transfer_outstanding(),
            "changed primary is flagged"
        );
        assert_eq!(regions[1].transfer_first_seen, Some(1_700_000_000));
    }

    #[test]
    fn split_apply_is_deterministic() {
        let key = MeasurementKey::new("db", "autogen", "cpu");
        let split_key = u64::MAX / 2;
        let mut base = ShardMap::default();
        apply_shard_map_op(
            &mut base,
            ShardMapOp::BootstrapMeasurement {
                key: key.clone(),
                region: sample_region(1, 0, u64::MAX, 1),
            },
        )
        .unwrap();
        let left = sample_region(1, 0, split_key, 1);
        let right = {
            let mut r = sample_region(2, split_key, u64::MAX, 2);
            r.peers = vec![2, 1];
            r.transfer_first_seen = Some(1_700_000_000);
            r
        };
        let op = ShardMapOp::Split {
            key,
            region_id: 1,
            split_key,
            epoch: ShardEpoch::default(),
            left,
            right,
        };
        let mut a = base.clone();
        let mut b = base.clone();
        apply_shard_map_op(&mut a, op.clone()).unwrap();
        apply_shard_map_op(&mut b, op).unwrap();
        assert_eq!(a, b);
    }

    #[test]
    fn split_propagates_parent_first_seen_to_flagged_child() {
        let key = MeasurementKey::new("db", "autogen", "cpu");
        let split_key = u64::MAX / 2;
        let mut map = ShardMap::default();
        apply_shard_map_op(
            &mut map,
            ShardMapOp::BootstrapMeasurement {
                key: key.clone(),
                region: flagged_region(1, 0, u64::MAX, 1, 1000),
            },
        )
        .unwrap();
        let right = {
            let mut r = sample_region(2, split_key, u64::MAX, 2);
            r.peers = vec![2, 1];
            r
        };
        apply_shard_map_op(
            &mut map,
            ShardMapOp::Split {
                key,
                region_id: 1,
                split_key,
                epoch: ShardEpoch::default(),
                left: sample_region(1, 0, split_key, 1),
                right,
            },
        )
        .unwrap();

        let regions = &map.space("db", "autogen", "cpu").unwrap().regions;
        assert_eq!(
            regions[1].transfer_first_seen,
            Some(1000),
            "re-split must not reset the debt age gate"
        );
    }

    #[test]
    fn clear_verified_clears_debt_and_bumps_conf_ver() {
        let key = MeasurementKey::new("db", "autogen", "cpu");
        let region = flagged_region(1, 0, u64::MAX, 1, 1000);
        let epoch = region.epoch;
        let mut map = ShardMap::default();
        apply_shard_map_op(
            &mut map,
            ShardMapOp::BootstrapMeasurement {
                key: key.clone(),
                region,
            },
        )
        .unwrap();
        apply_shard_map_op(
            &mut map,
            ShardMapOp::ClearVerified {
                key: key.clone(),
                region_id: 1,
                epoch,
            },
        )
        .unwrap();
        let r = &map.space("db", "autogen", "cpu").unwrap().regions[0];
        assert_eq!(r.transfer_verified, Some(true));
        assert_eq!(r.transfer_first_seen, None);
        assert_eq!(r.epoch.conf_ver, epoch.conf_ver + 1);

        let err = apply_shard_map_op(
            &mut map,
            ShardMapOp::ClearVerified {
                key,
                region_id: 1,
                epoch,
            },
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("stale epoch on ClearVerified"),
            "{err}"
        );
    }
}
