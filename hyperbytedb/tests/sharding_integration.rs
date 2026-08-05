//! Integration tests for automatic series sharding (cluster-only).

use std::sync::Arc;

use hyperbytedb::adapters::sharding::rocksdb_shard_map::RocksDbShardMap;
use hyperbytedb::application::shard_query::inject_region_series_id_predicate;
use hyperbytedb::application::shard_query_routing::{
    select_regions_for_query, RegionSelection,
};
use hyperbytedb::application::shard_peer_resolution::{
    active_region_peer_targets, is_active_peer, resolve_region_peers, RegionTargetRole,
};
use hyperbytedb::domain::cluster::membership::{ClusterMembership, NodeInfo, NodeState};
use hyperbytedb::config::HyperbytedbConfig;
use hyperbytedb::domain::point::Point;
use hyperbytedb::domain::sharding::{
    ops::apply_shard_map_op, MeasurementKey, ShardEpoch, ShardLocationCache, ShardMap, ShardMapOp,
    ShardRegion,
};
use hyperbytedb::domain::series::{series_id, series_id_for_point};
use hyperbytedb::timeseriesql::parser::parse_query;
use hyperbytedb::ports::sharding::ShardMapPort;
use serial_test::serial;

#[test]
fn sharding_requires_cluster_enabled() {
    let mut cfg = HyperbytedbConfig::load(None).expect("config");
    cfg.sharding.enabled = true;
    cfg.cluster.enabled = false;
    assert!(cfg.validate().is_err());
}

#[test]
fn sharding_disabled_validation_passes() {
    let cfg = HyperbytedbConfig::load(None).expect("config");
    assert!(cfg.validate().is_ok());
}

#[tokio::test]
async fn location_cache_locates_after_bootstrap() {
    let dir = tempfile::tempdir().unwrap();
    let map_store = Arc::new(RocksDbShardMap::open(dir.path(), true, 1).expect("open shard map"));
    let region = ShardRegion {
        region_id: 1,
        start: 0,
        end: u64::MAX,
        epoch: ShardEpoch::default(),
        peers: vec![1, 2, 3],
        primary: 1,
        last_split_at: 0,
    health: Default::default(),
    };
    map_store
        .apply_op(ShardMapOp::BootstrapMeasurement {
            key: MeasurementKey::new("db", "autogen", "cpu"),
            region: region.clone(),
        })
        .await
        .unwrap();

    let cache = ShardLocationCache::new();
    let snapshot = map_store.snapshot().await.unwrap();
    cache.refresh_from_map(&snapshot);

    let p = Point {
        measurement: "cpu".into(),
        tags: [("host".into(), "a".into())].into(),
        fields: [(
            "value".into(),
            hyperbytedb::domain::point::FieldValue::Float(1.0),
        )]
        .into(),
        timestamp: 1,
    };
    let sid = series_id_for_point(&p);
    let located = cache
        .locate(&snapshot, "db", "autogen", "cpu", sid)
        .expect("region");
    assert_eq!(located.region_id, 1);
}

#[test]
#[serial]
fn flag_off_shard_map_is_empty() {
    let dir = tempfile::tempdir().unwrap();
    let map = RocksDbShardMap::open(dir.path(), false, 1).unwrap();
    let rt = tokio::runtime::Runtime::new().unwrap();
    let snapshot: ShardMap = rt.block_on(map.snapshot()).unwrap();
    assert!(snapshot.spaces.is_empty());
}

#[test]
fn region_series_id_predicate_injection() {
    let sql = "SELECT * FROM t\nWHERE time > 0".to_string();
    let out = inject_region_series_id_predicate(sql, 10, 20);
    assert!(out.contains("`series_id` >= 10"));
    assert!(out.contains("`series_id` < 20"));
}

#[test]
fn split_op_divides_region_range() {
    let key = MeasurementKey::new("db", "autogen", "cpu");
    let region = ShardRegion {
        region_id: 1,
        start: 0,
        end: u64::MAX,
        epoch: ShardEpoch::default(),
        peers: vec![1, 2],
        primary: 1,
        last_split_at: 0,
    health: Default::default(),
    };
    let mut map = ShardMap::default();
    apply_shard_map_op(
        &mut map,
        ShardMapOp::BootstrapMeasurement {
            key: key.clone(),
            region: region.clone(),
        },
    )
    .unwrap();
    let split_key = u64::MAX / 2;
    let mut left = region.clone();
    left.end = split_key;
    let mut right = region;
    right.region_id = 2;
    right.start = split_key;
    apply_shard_map_op(
        &mut map,
        ShardMapOp::Split {
            key,
            region_id: 1,
            split_key,
            left,
            right,
        },
    )
    .unwrap();
    let space = map.spaces.values().next().unwrap();
    assert_eq!(space.regions.len(), 2);
    assert_eq!(space.regions[0].end, split_key);
    assert_eq!(space.regions[1].start, split_key);
    assert_eq!(space.regions[1].end, u64::MAX);
}

#[test]
fn merge_op_consolidates_adjacent_regions() {
    let key = MeasurementKey::new("db", "autogen", "cpu");
    let split_key = u64::MAX / 2;
    let left = ShardRegion {
        region_id: 1,
        start: 0,
        end: split_key,
        epoch: ShardEpoch::default(),
        peers: vec![1, 2],
        primary: 1,
        last_split_at: 0,
    health: Default::default(),
    };
    let right = ShardRegion {
        region_id: 2,
        start: split_key,
        end: u64::MAX,
        epoch: ShardEpoch::default(),
        peers: vec![1, 2],
        primary: 1,
        last_split_at: 0,
    health: Default::default(),
    };
    let mut map = ShardMap::default();
    apply_shard_map_op(
        &mut map,
        ShardMapOp::BootstrapMeasurement {
            key: key.clone(),
            region: ShardRegion {
                region_id: 1,
                start: 0,
                end: u64::MAX,
                epoch: ShardEpoch::default(),
                peers: vec![1, 2],
                primary: 1,
                last_split_at: 0,
            health: Default::default(),
            },
        },
    )
    .unwrap();
    apply_shard_map_op(
        &mut map,
        ShardMapOp::Split {
            key: key.clone(),
            region_id: 1,
            split_key,
            left: left.clone(),
            right: right.clone(),
        },
    )
    .unwrap();
    let merged = ShardRegion {
        region_id: 1,
        start: 0,
        end: u64::MAX,
        epoch: ShardEpoch::default(),
        peers: vec![1, 2],
        primary: 1,
        last_split_at: 0,
    health: Default::default(),
    };
    apply_shard_map_op(
        &mut map,
        ShardMapOp::Merge {
            key,
            left_region_id: 1,
            right_region_id: 2,
            merged,
        },
    )
    .unwrap();
    assert_eq!(map.spaces.values().next().unwrap().regions.len(), 1);
}

fn four_region_space() -> hyperbytedb::domain::sharding::MeasurementShardSpace {
    let q = u64::MAX / 4;
    hyperbytedb::domain::sharding::MeasurementShardSpace {
        key: MeasurementKey::new("db", "autogen", "metrics"),
        regions: vec![
            ShardRegion {
                region_id: 1,
                start: 0,
                end: q,
                epoch: ShardEpoch::default(),
                peers: vec![1],
                primary: 1,
                last_split_at: 0,
            health: Default::default(),
            },
            ShardRegion {
                region_id: 2,
                start: q,
                end: q * 2,
                epoch: ShardEpoch::default(),
                peers: vec![1],
                primary: 1,
                last_split_at: 0,
            health: Default::default(),
            },
            ShardRegion {
                region_id: 3,
                start: q * 2,
                end: q * 3,
                epoch: ShardEpoch::default(),
                peers: vec![1],
                primary: 1,
                last_split_at: 0,
            health: Default::default(),
            },
            ShardRegion {
                region_id: 4,
                start: q * 3,
                end: u64::MAX,
                epoch: ShardEpoch::default(),
                peers: vec![1],
                primary: 1,
                last_split_at: 0,
            health: Default::default(),
            },
        ],
    }
}

#[test]
fn region_selection_equality_where_hits_single_region() {
    let space = four_region_space();
    let stmt = match parse_query(r#"SELECT value FROM metrics WHERE host = 's50000'"#)
        .unwrap()
        .remove(0)
    {
        hyperbytedb::timeseriesql::ast::Statement::Select(s) => s,
        _ => panic!("expected select"),
    };
    let sel = select_regions_for_query(&space, "metrics", &stmt);
    assert!(matches!(sel, RegionSelection::Single(_)));
    assert_eq!(sel.region_count(space.regions.len()), 1);
}

#[test]
fn region_selection_count_without_where_uses_all_regions() {
    let space = four_region_space();
    let stmt = match parse_query("SELECT count(value) FROM metrics").unwrap().remove(0) {
        hyperbytedb::timeseriesql::ast::Statement::Select(s) => s,
        _ => panic!("expected select"),
    };
    let sel = select_regions_for_query(&space, "metrics", &stmt);
    assert!(matches!(sel, RegionSelection::All));
    assert_eq!(sel.region_count(space.regions.len()), 4);
}

#[test]
fn region_selection_point_lookup_series_id_in_one_region() {
    let space = four_region_space();
    let mut tags = std::collections::BTreeMap::new();
    tags.insert("host".into(), "s50000".into());
    let sid = series_id("metrics", &tags);
    let expected_region = space.locate(sid).expect("series maps to a region");

    let stmt = match parse_query(r#"SELECT value FROM metrics WHERE host = 's50000'"#)
        .unwrap()
        .remove(0)
    {
        hyperbytedb::timeseriesql::ast::Statement::Select(s) => s,
        _ => panic!("expected select"),
    };
    let sel = select_regions_for_query(&space, "metrics", &stmt);
    match sel {
        RegionSelection::Single(region) => assert_eq!(region.region_id, expected_region.region_id),
        other => panic!("expected single region, got {other:?}"),
    }
}

fn sample_membership(states: &[(u64, NodeState)]) -> ClusterMembership {
    let mut m = ClusterMembership::new();
    for &(id, state) in states {
        m.add_node(NodeInfo {
            node_id: id,
            addr: format!("127.0.0.1:{id}"),
            state,
            joined_at: 0,
            last_heartbeat: 0,
            needs_sync: false,
        });
    }
    m
}

#[test]
fn resolve_region_peers_skips_disconnected_primary() {
    let region = ShardRegion {
        region_id: 1,
        start: 0,
        end: u64::MAX,
        epoch: ShardEpoch::default(),
        peers: vec![1, 2, 3],
        primary: 1,
        last_split_at: 0,
    health: Default::default(),
    };
    let membership = sample_membership(&[
        (1, NodeState::Disconnected),
        (2, NodeState::Active),
        (3, NodeState::Active),
    ]);
    let peers = resolve_region_peers(&region, 99, &membership, RegionTargetRole::Read);
    assert_eq!(peers, vec![2, 3]);
    assert!(!is_active_peer(&membership, 1));
}

#[test]
fn active_replication_targets_skip_disconnected_peers() {
    let region = ShardRegion {
        region_id: 1,
        start: 0,
        end: u64::MAX,
        epoch: ShardEpoch::default(),
        peers: vec![1, 2, 3],
        primary: 1,
        last_split_at: 0,
    health: Default::default(),
    };
    let membership = sample_membership(&[
        (1, NodeState::Active),
        (2, NodeState::Active),
        (3, NodeState::Disconnected),
    ]);
    let targets = active_region_peer_targets(&region, 1, &membership);
    assert_eq!(targets, vec![2]);
}

#[test]
fn sharding_config_includes_down_node_avoidance_defaults() {
    let cfg = HyperbytedbConfig::load(None).expect("config");
    assert_eq!(cfg.sharding.primary_failover_after_secs, 60);
    assert_eq!(cfg.sharding.scatter_peer_timeout_ms, 5000);
    assert_eq!(cfg.sharding.scatter_max_peer_attempts, 3);
}
