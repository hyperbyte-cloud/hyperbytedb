use serde::{Deserialize, Serialize};
use thiserror::Error;

use super::types::{MeasurementKey, RegionState, ShardEpoch, ShardRegion};

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
    /// Grow a region's replica set. Does not change primary.
    ///
    /// Used when effective RF has room (n grew). Transfer data onto `to_peer`
    /// *before* proposing this op — commit-then-stage is the heal path only.
    AddPeer {
        key: MeasurementKey,
        region_id: u64,
        to_peer: u64,
        #[serde(default)]
        epoch: ShardEpoch,
        /// Commit the peer as a LEARNER: a member that receives replicated
        /// writes but may not serve reads until [`ShardMapOp::PromotePeer`].
        ///
        /// This is the ordering that closes the copy gap. Committing
        /// membership BEFORE the data is copied means replication starts
        /// flowing immediately, so no write can land in the window between the
        /// snapshot and the commit. TiKV does the same with AddLearner.
        #[serde(default)]
        staging: bool,
    },
    /// Promote a learner to a full replica once its copy has landed.
    ///
    /// The mirror of `AddPeer { staging: true }`, and TiKV's PromoteLearner.
    PromotePeer {
        key: MeasurementKey,
        region_id: u64,
        peer: u64,
        #[serde(default)]
        epoch: ShardEpoch,
    },
    /// Shrink a region's replica set. Does not change primary.
    ///
    /// The mirror of [`ShardMapOp::AddPeer`], and what makes drain and
    /// rebalancing possible: without it a peer set can only grow. Refuses to
    /// remove the primary or the last peer, so a region is never left
    /// ownerless. RF policy belongs to the caller.
    RemovePeer {
        key: MeasurementKey,
        region_id: u64,
        from_peer: u64,
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
    /// Cutover: make a staging region routable.
    ///
    /// The single atomic step of a split. Until this commits the donor serves
    /// the child's range; after it, the child does. Every node's location cache
    /// is refreshed by the same Raft apply, so no node sees a half-applied
    /// handover.
    ActivateRegion {
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
            | ShardMapOp::AddPeer { key, .. }
            | ShardMapOp::PromotePeer { key, .. }
            | ShardMapOp::RemovePeer { key, .. }
            | ShardMapOp::TransferPrimary { key, .. }
            | ShardMapOp::ClearVerified { key, .. }
            | ShardMapOp::ActivateRegion { key, .. } => key,
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
            region.staging_peers.retain(|p| *p != from_peer);
            if !region.peers.contains(&to_peer) {
                region.peers.push(to_peer);
            }
            region.epoch = epoch.bump_conf_ver();
            if region.primary == from_peer {
                region.primary = to_peer;
            }
            sort_space_regions(space);
        }
        ShardMapOp::AddPeer {
            key,
            region_id,
            to_peer,
            epoch,
            staging,
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
                return Err(ShardMapApplyError::StaleEpoch("AddPeer"));
            }
            if region.transfer_outstanding() {
                return Err(ShardMapApplyError::Invalid(
                    "cannot add peer while transfer debt is outstanding".into(),
                ));
            }
            if region.peers.contains(&to_peer) {
                return Err(ShardMapApplyError::Invalid(format!(
                    "peer {to_peer} already in region"
                )));
            }
            region.peers.push(to_peer);
            if staging && !region.staging_peers.contains(&to_peer) {
                region.staging_peers.push(to_peer);
            }
            region.epoch = epoch.bump_conf_ver();
            sort_space_regions(space);
        }
        ShardMapOp::PromotePeer {
            key,
            region_id,
            peer,
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
                return Err(ShardMapApplyError::StaleEpoch("PromotePeer"));
            }
            if !region.peers.contains(&peer) {
                return Err(ShardMapApplyError::Invalid(format!(
                    "cannot promote {peer}: not a member of region {region_id}"
                )));
            }
            // Idempotent: a retry after a leadership change must not wedge a
            // learner that is already promoted.
            region.staging_peers.retain(|p| *p != peer);
            region.epoch = epoch.bump_conf_ver();
            sort_space_regions(space);
        }
        ShardMapOp::RemovePeer {
            key,
            region_id,
            from_peer,
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
                return Err(ShardMapApplyError::StaleEpoch("RemovePeer"));
            }
            if region.transfer_outstanding() {
                return Err(ShardMapApplyError::Invalid(
                    "cannot remove peer while transfer debt is outstanding".into(),
                ));
            }
            if !region.peers.contains(&from_peer) {
                return Err(ShardMapApplyError::Invalid(format!(
                    "peer {from_peer} not in region"
                )));
            }
            if region.primary == from_peer {
                return Err(ShardMapApplyError::Invalid(format!(
                    "cannot remove peer {from_peer}: it is the region primary"
                )));
            }
            if region.peers.len() <= 1 {
                return Err(ShardMapApplyError::Invalid(
                    "cannot remove the last peer of a region".into(),
                ));
            }
            region.peers.retain(|p| *p != from_peer);
            // Drop the learner marker too, or a peer re-added later inherits a
            // stale "not ready" flag that nothing will ever clear.
            region.staging_peers.retain(|p| *p != from_peer);
            region.epoch = epoch.bump_conf_ver();
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
        ShardMapOp::ActivateRegion {
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
                return Err(ShardMapApplyError::StaleEpoch("ActivateRegion"));
            }
            // Idempotent rather than an error: a retried activation after a
            // leadership change must not wedge the region in Staging.
            region.state = RegionState::Serving;
            region.staged_from = None;
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
            state: Default::default(),
            staged_from: None,
            staging_peers: Vec::new(),
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
    fn add_peer_appends_replica_without_moving_primary() {
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
            ShardMapOp::AddPeer {
                key,
                region_id: 1,
                to_peer: 3,
                epoch: region.epoch,
                staging: false,
            },
        )
        .unwrap();
        let r = &map.spaces.values().next().unwrap().regions[0];
        assert_eq!(r.primary, 1);
        assert!(r.peers.contains(&3));
        assert_eq!(r.epoch.conf_ver, region.epoch.conf_ver + 1);
    }

    #[test]
    fn add_peer_rejects_duplicate_and_stale_epoch() {
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
        let dup = apply_shard_map_op(
            &mut map,
            ShardMapOp::AddPeer {
                key: key.clone(),
                region_id: 1,
                to_peer: 1,
                epoch: region.epoch,
                staging: false,
            },
        )
        .unwrap_err();
        assert!(dup.to_string().contains("already in region"), "{dup}");

        apply_shard_map_op(
            &mut map,
            ShardMapOp::AddPeer {
                key: key.clone(),
                region_id: 1,
                to_peer: 3,
                epoch: region.epoch,
                staging: false,
            },
        )
        .unwrap();
        let stale = apply_shard_map_op(
            &mut map,
            ShardMapOp::AddPeer {
                key,
                region_id: 1,
                to_peer: 4,
                epoch: region.epoch,
                staging: false,
            },
        )
        .unwrap_err();
        assert!(
            stale.to_string().contains("stale epoch on AddPeer"),
            "{stale}"
        );
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
            state: Default::default(),
            staged_from: None,
            staging_peers: Vec::new(),
            ..sample_region(id, start, end, primary)
        }
    }

    #[test]
    fn add_peer_rejects_outstanding_transfer_debt() {
        let key = MeasurementKey::new("db", "autogen", "cpu");
        let region = flagged_region(1, 0, u64::MAX, 1, 1000);
        let mut map = ShardMap::default();
        apply_shard_map_op(
            &mut map,
            ShardMapOp::BootstrapMeasurement {
                key: key.clone(),
                region: region.clone(),
            },
        )
        .unwrap();
        let err = apply_shard_map_op(
            &mut map,
            ShardMapOp::AddPeer {
                key,
                region_id: 1,
                to_peer: 3,
                epoch: region.epoch,
                staging: false,
            },
        )
        .unwrap_err();
        assert!(err.to_string().contains("transfer debt"), "{err}");
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

    fn bootstrapped(peers: Vec<u64>, primary: u64) -> (MeasurementKey, ShardMap, ShardEpoch) {
        let key = MeasurementKey::new("db", "autogen", "cpu");
        let mut region = sample_region(1, 0, u64::MAX, primary);
        region.peers = peers;
        region.primary = primary;
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
        (key, map, epoch)
    }

    fn remove_op(key: &MeasurementKey, from_peer: u64, epoch: ShardEpoch) -> ShardMapOp {
        ShardMapOp::RemovePeer {
            key: key.clone(),
            region_id: 1,
            from_peer,
            epoch,
        }
    }

    #[test]
    fn remove_peer_drops_the_peer_and_bumps_conf_ver() {
        let (key, mut map, epoch) = bootstrapped(vec![1, 2, 3], 1);
        apply_shard_map_op(&mut map, remove_op(&key, 3, epoch)).unwrap();
        let r = &map.spaces.values().next().unwrap().regions[0];
        assert_eq!(r.peers, vec![1, 2]);
        assert_eq!(r.primary, 1);
        assert_eq!(r.epoch.conf_ver, epoch.conf_ver + 1);
    }

    #[test]
    fn remove_peer_refuses_the_primary() {
        let (key, mut map, epoch) = bootstrapped(vec![1, 2, 3], 1);
        assert!(matches!(
            apply_shard_map_op(&mut map, remove_op(&key, 1, epoch)).unwrap_err(),
            ShardMapApplyError::Invalid(_)
        ));
    }

    #[test]
    fn remove_peer_refuses_a_non_peer() {
        let (key, mut map, epoch) = bootstrapped(vec![1, 2, 3], 1);
        assert!(matches!(
            apply_shard_map_op(&mut map, remove_op(&key, 9, epoch)).unwrap_err(),
            ShardMapApplyError::Invalid(_)
        ));
    }

    #[test]
    fn remove_peer_refuses_the_last_peer() {
        let (key, mut map, epoch) = bootstrapped(vec![1], 1);
        assert!(matches!(
            apply_shard_map_op(&mut map, remove_op(&key, 1, epoch)).unwrap_err(),
            ShardMapApplyError::Invalid(_)
        ));
    }

    #[test]
    fn remove_peer_refuses_stale_epoch() {
        let (key, mut map, epoch) = bootstrapped(vec![1, 2, 3], 1);
        let stale = ShardEpoch {
            conf_ver: epoch.conf_ver + 5,
            version: epoch.version,
        };
        assert!(matches!(
            apply_shard_map_op(&mut map, remove_op(&key, 3, stale)).unwrap_err(),
            ShardMapApplyError::StaleEpoch("RemovePeer")
        ));
    }

    #[test]
    fn remove_peer_refuses_while_transfer_debt_outstanding() {
        let key = MeasurementKey::new("db", "autogen", "cpu");
        let mut region = sample_region(1, 0, u64::MAX, 1);
        region.peers = vec![1, 2, 3];
        region.transfer_verified = Some(false);
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
        assert!(matches!(
            apply_shard_map_op(&mut map, remove_op(&key, 3, epoch)).unwrap_err(),
            ShardMapApplyError::Invalid(_)
        ));
    }

    /// Cutover is one committed op: the region becomes routable and forgets the
    /// donor it was staged from, and the epoch bumps so a concurrent proposal
    /// built on the old view is rejected.
    #[test]
    fn activate_region_makes_a_staging_region_serving() {
        let mut map = ShardMap::default();
        let key = MeasurementKey::new("db", "rp", "cpu");
        let mut region = sample_region(1, 0, u64::MAX, 1);
        region.state = RegionState::Staging;
        region.staged_from = Some(7);
        let epoch = region.epoch;
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
            ShardMapOp::ActivateRegion {
                key: key.clone(),
                region_id: 1,
                epoch,
            },
        )
        .unwrap();

        let r = &map.spaces[&key].regions[0];
        assert!(r.is_serving(), "cutover must make the region routable");
        assert_eq!(r.staged_from, None, "the donor link must be cleared");
        assert_ne!(r.epoch, epoch, "the epoch must bump");
    }

    /// A cutover proposed against a stale view must be refused, or two leaders
    /// could activate a child whose movement was restarted underneath them.
    #[test]
    fn activate_region_refuses_a_stale_epoch() {
        let mut map = ShardMap::default();
        let key = MeasurementKey::new("db", "rp", "cpu");
        let mut region = sample_region(1, 0, u64::MAX, 1);
        region.state = RegionState::Staging;
        let stale = region.epoch;
        region.epoch = region.epoch.bump_conf_ver();
        apply_shard_map_op(
            &mut map,
            ShardMapOp::BootstrapMeasurement {
                key: key.clone(),
                region,
            },
        )
        .unwrap();

        let err = apply_shard_map_op(
            &mut map,
            ShardMapOp::ActivateRegion {
                key: key.clone(),
                region_id: 1,
                epoch: stale,
            },
        )
        .unwrap_err();
        assert!(matches!(err, ShardMapApplyError::StaleEpoch(_)), "{err:?}");
        assert!(!map.spaces[&key].regions[0].is_serving());
    }

    /// A learner is a member -- it receives replicated writes -- but must not
    /// serve reads until promoted.
    ///
    /// The ordering is the point: committing membership BEFORE copying is what
    /// closes the gap. Copy-then-commit leaves every write between the snapshot
    /// and the commit reaching the primary alone, and a later TransferPrimary
    /// strands them.
    #[test]
    fn a_learner_is_a_member_but_not_a_read_target() {
        let mut map = ShardMap::default();
        let key = MeasurementKey::new("db", "rp", "cpu");
        let region = sample_region(1, 0, u64::MAX, 1);
        let epoch = region.epoch;
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
            ShardMapOp::AddPeer {
                key: key.clone(),
                region_id: 1,
                to_peer: 7,
                epoch,
                staging: true,
            },
        )
        .unwrap();

        let r = &map.spaces[&key].regions[0];
        assert!(
            r.peers.contains(&7),
            "a learner must be a MEMBER so writes replicate to it"
        );
        assert!(!r.is_serving_peer(7), "a learner must not be a read target");
        let after_add = r.epoch;

        apply_shard_map_op(
            &mut map,
            ShardMapOp::PromotePeer {
                key: key.clone(),
                region_id: 1,
                peer: 7,
                epoch: after_add,
            },
        )
        .unwrap();

        let r = &map.spaces[&key].regions[0];
        assert!(r.is_serving_peer(7), "promotion must make it a read target");
        assert!(r.staging_peers.is_empty());
    }

    /// Without `staging` the peer is immediately a read target -- the existing
    /// behaviour, which the drain and rebalance paths still rely on.
    #[test]
    fn add_peer_without_staging_serves_immediately() {
        let mut map = ShardMap::default();
        let key = MeasurementKey::new("db", "rp", "cpu");
        let region = sample_region(1, 0, u64::MAX, 1);
        let epoch = region.epoch;
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
            ShardMapOp::AddPeer {
                key: key.clone(),
                region_id: 1,
                to_peer: 7,
                epoch,
                staging: false,
            },
        )
        .unwrap();
        assert!(map.spaces[&key].regions[0].is_serving_peer(7));
    }

    /// A retry after a leadership change must not wedge an already-promoted
    /// learner, and a non-member must never be promoted into service.
    #[test]
    fn promote_peer_is_idempotent_and_refuses_a_non_member() {
        let mut map = ShardMap::default();
        let key = MeasurementKey::new("db", "rp", "cpu");
        let region = sample_region(1, 0, u64::MAX, 1);
        apply_shard_map_op(
            &mut map,
            ShardMapOp::BootstrapMeasurement {
                key: key.clone(),
                region,
            },
        )
        .unwrap();

        let epoch = map.spaces[&key].regions[0].epoch;
        // Already serving: promoting again is a no-op, not an error.
        let primary = map.spaces[&key].regions[0].primary;
        apply_shard_map_op(
            &mut map,
            ShardMapOp::PromotePeer {
                key: key.clone(),
                region_id: 1,
                peer: primary,
                epoch,
            },
        )
        .expect("promoting an already-serving member must be idempotent");

        let epoch = map.spaces[&key].regions[0].epoch;
        let err = apply_shard_map_op(
            &mut map,
            ShardMapOp::PromotePeer {
                key: key.clone(),
                region_id: 1,
                peer: 999,
                epoch,
            },
        )
        .unwrap_err();
        assert!(matches!(err, ShardMapApplyError::Invalid(_)), "{err:?}");
    }

    /// Removing a peer must drop its learner marker too, or a re-added peer
    /// would inherit a stale "not ready" flag it can never clear.
    #[test]
    fn remove_peer_clears_the_learner_marker() {
        let mut map = ShardMap::default();
        let key = MeasurementKey::new("db", "rp", "cpu");
        let region = sample_region(1, 0, u64::MAX, 1);
        let epoch = region.epoch;
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
            ShardMapOp::AddPeer {
                key: key.clone(),
                region_id: 1,
                to_peer: 7,
                epoch,
                staging: true,
            },
        )
        .unwrap();
        let epoch = map.spaces[&key].regions[0].epoch;
        apply_shard_map_op(
            &mut map,
            ShardMapOp::RemovePeer {
                key: key.clone(),
                region_id: 1,
                from_peer: 7,
                epoch,
            },
        )
        .unwrap();
        let r = &map.spaces[&key].regions[0];
        assert!(!r.peers.contains(&7));
        assert!(
            r.staging_peers.is_empty(),
            "stale learner marker left behind"
        );
    }
}
