//! Integration tests for automatic series sharding (cluster-only).

use std::sync::Arc;

use hyperbytedb::adapters::sharding::rocksdb_shard_map::RocksDbShardMap;
use hyperbytedb::application::shard_query::inject_region_series_id_predicate;
use hyperbytedb::config::HyperbytedbConfig;
use hyperbytedb::domain::point::Point;
use hyperbytedb::domain::sharding::{
    ops::apply_shard_map_op, MeasurementKey, ShardEpoch, ShardLocationCache, ShardMap, ShardMapOp,
    ShardRegion,
};
use hyperbytedb::domain::series::series_id_for_point;
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
    assert!(out.contains("t.`series_id` >= 10"));
    assert!(out.contains("t.`series_id` < 20"));
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
    };
    let right = ShardRegion {
        region_id: 2,
        start: split_key,
        end: u64::MAX,
        epoch: ShardEpoch::default(),
        peers: vec![1, 2],
        primary: 1,
        last_split_at: 0,
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
