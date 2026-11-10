//! One function per [`ShardMapOp`]: what `apply_shard_map_op` does for it,
//! after bumping `map_version`.

use super::*;
use crate::domain::sharding::types::ShardMap;

pub(super) fn apply_bootstrap_measurement_presplit(
    map: &mut ShardMap,
    key: MeasurementKey,
    regions: Vec<ShardRegion>,
) -> Result<(), ShardMapApplyError> {
    if map.spaces.contains_key(&key) {
        return Err(ShardMapApplyError::SpaceExists {
            db: key.db.clone(),
            rp: key.rp.clone(),
            measurement: key.measurement,
        });
    }
    let mut ids = std::collections::HashSet::new();
    for region in &regions {
        if !ids.insert(region.region_id)
            || map.spaces.values().any(|space| {
                space
                    .regions
                    .iter()
                    .any(|r| r.region_id == region.region_id)
            })
        {
            return Err(ShardMapApplyError::DuplicateRegionId(region.region_id));
        }
    }
    if let Some(max) = regions.iter().map(|r| r.region_id).max() {
        map.next_region_id = map.next_region_id.max(max.saturating_add(1));
    }
    let mut space = super::types::MeasurementShardSpace {
        key: key.clone(),
        regions,
    };
    sort_space_regions(&mut space);
    space.validate().map_err(ShardMapApplyError::Invalid)?;
    map.spaces.insert(key, space);
    Ok(())
}

pub(super) fn apply_bootstrap_measurement(
    map: &mut ShardMap,
    key: MeasurementKey,
    region: ShardRegion,
) -> Result<(), ShardMapApplyError> {
    if map.spaces.contains_key(&key) {
        return Err(ShardMapApplyError::SpaceExists {
            db: key.db.clone(),
            rp: key.rp.clone(),
            measurement: key.measurement,
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
    Ok(())
}

pub(super) fn apply_split(
    map: &mut ShardMap,
    key: MeasurementKey,
    region_id: u64,
    split_key: u64,
    epoch: ShardEpoch,
    mut left: ShardRegion,
    mut right: ShardRegion,
) -> Result<(), ShardMapApplyError> {
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
    // Apply-time normalization: flag every child whose primary differs
    // from the parent's (a metadata-only split never does). Nothing
    // clears the flag any more: the reconciliation that re-pushed
    // and verified flagged children was removed. `transfer_first_seen` propagates from the
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
    Ok(())
}

pub(super) fn apply_drop_space(
    map: &mut ShardMap,
    db: String,
    rp: Option<String>,
    measurement: Option<String>,
) -> Result<(), ShardMapApplyError> {
    map.spaces
        .retain(|key, _| !ShardMapOp::drop_matches(&db, &rp, &measurement, key));
    Ok(())
}

pub(super) fn apply_merge(
    map: &mut ShardMap,
    key: MeasurementKey,
    left_region_id: u64,
    right_region_id: u64,
    epoch: ShardEpoch,
    right_epoch: Option<ShardEpoch>,
    merged: ShardRegion,
) -> Result<(), ShardMapApplyError> {
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
    if space.regions[ri].epoch != right_epoch.unwrap_or(epoch) {
        return Err(ShardMapApplyError::StaleEpoch("Merge right region"));
    }
    space.regions.remove(ri);
    space.regions[li] = merged;
    sort_space_regions(space);
    space.validate().map_err(ShardMapApplyError::Invalid)?;
    Ok(())
}

pub(super) fn apply_move_peer(
    map: &mut ShardMap,
    key: MeasurementKey,
    region_id: u64,
    from_peer: u64,
    to_peer: u64,
    epoch: ShardEpoch,
) -> Result<(), ShardMapApplyError> {
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
    if region.staging_peers.contains(&to_peer) {
        return Err(ShardMapApplyError::Invalid(format!(
            "cannot move peer to learner {to_peer}"
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
    Ok(())
}

pub(super) fn apply_add_peer(
    map: &mut ShardMap,
    key: MeasurementKey,
    region_id: u64,
    to_peer: u64,
    epoch: ShardEpoch,
    staging: bool,
) -> Result<(), ShardMapApplyError> {
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
    Ok(())
}

pub(super) fn apply_promote_peer(
    map: &mut ShardMap,
    key: MeasurementKey,
    region_id: u64,
    peer: u64,
    epoch: ShardEpoch,
) -> Result<(), ShardMapApplyError> {
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
    Ok(())
}

pub(super) fn apply_remove_peer(
    map: &mut ShardMap,
    key: MeasurementKey,
    region_id: u64,
    from_peer: u64,
    epoch: ShardEpoch,
) -> Result<(), ShardMapApplyError> {
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
    let remaining_serving = region
        .peers
        .iter()
        .filter(|p| **p != from_peer && region.is_serving_peer(**p))
        .count();
    if remaining_serving == 0 {
        return Err(ShardMapApplyError::Invalid(
            "cannot remove peer: no serving replica would remain".into(),
        ));
    }
    region.peers.retain(|p| *p != from_peer);
    // Drop the learner marker too, or a peer re-added later inherits a
    // stale "not ready" flag that nothing will ever clear.
    region.staging_peers.retain(|p| *p != from_peer);
    region.epoch = epoch.bump_conf_ver();
    sort_space_regions(space);
    Ok(())
}

pub(super) fn apply_transfer_primary(
    map: &mut ShardMap,
    key: MeasurementKey,
    region_id: u64,
    new_primary: u64,
    epoch: ShardEpoch,
) -> Result<(), ShardMapApplyError> {
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
    // A learner holds only a prefix of the range until it is promoted.
    // Making it primary is the data-less-primary bug: PrimaryRead then
    // resolves to NOTHING, because resolve_region_peers refuses a
    // learner, and every aggregate for the region fails with
    // "no active peers" even though the region is serving and the
    // node is a member. Observed exactly that way on region 64.
    if region.staging_peers.contains(&new_primary) {
        return Err(ShardMapApplyError::Invalid(format!(
            "cannot make learner {new_primary} the primary of region {region_id}"
        )));
    }
    if !region.peers.contains(&new_primary) {
        return Err(ShardMapApplyError::Invalid(format!(
            "new primary {new_primary} not a peer"
        )));
    }
    region.primary = new_primary;
    region.epoch = epoch.bump_conf_ver();
    sort_space_regions(space);
    Ok(())
}

pub(super) fn apply_clear_verified(
    map: &mut ShardMap,
    key: MeasurementKey,
    region_id: u64,
    epoch: ShardEpoch,
) -> Result<(), ShardMapApplyError> {
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
    Ok(())
}

pub(super) fn apply_activate_region(
    map: &mut ShardMap,
    key: MeasurementKey,
    region_id: u64,
    epoch: ShardEpoch,
) -> Result<(), ShardMapApplyError> {
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
    Ok(())
}
