mod common;

use std::sync::Arc;
use std::time::Duration;

use common::sharding_cluster::*;
use hyperbytedb::adapters::chdb::native_adapter::ChdbNativeAdapter;
use hyperbytedb::adapters::chdb::query_adapter::ChdbQueryAdapter;
use hyperbytedb::adapters::chdb::session::SharedSession;
use hyperbytedb::adapters::metadata::rocksdb_meta::RocksDbMetadata;
use hyperbytedb::adapters::sharding::rocksdb_shard_map::RocksDbShardMap;
use hyperbytedb::adapters::wal::rocksdb_wal::RocksDbWal;
use hyperbytedb::application::cluster::bootstrap::ClusterBootstrap;
use hyperbytedb::application::materialized_view_service::MaterializedViewService;
use hyperbytedb::application::shard_scheduler::ShardScheduler;
use hyperbytedb::config::ClusterConfig;
use hyperbytedb::domain::cluster::membership::NodeState;
use hyperbytedb::domain::sharding::{
    MeasurementKey, ShardEpoch, ShardLocationCache, ShardMapOp, ShardRegion,
};
use hyperbytedb::ports::points_sink::PointsSinkPort;
use hyperbytedb::ports::sharding::ShardMapPort;
use serial_test::serial;
use tokio::sync::watch;

fn test_cluster_config(dir: &std::path::Path, addr: &str) -> ClusterConfig {
    let mut cfg = hyperbytedb::config::HyperbytedbConfig::load(None)
        .unwrap()
        .cluster;
    cfg.enabled = true;
    cfg.node_id = 1;
    cfg.cluster_addr = addr.to_string();
    cfg.replication_log_dir = dir.join("repl").to_string_lossy().into();
    cfg.raft_dir = dir.join("raft").to_string_lossy().into();
    cfg.raft_heartbeat_interval_ms = Some(200);
    cfg.raft_election_timeout_ms = Some(500);
    cfg.raft_rpc_timeout_secs = 1;
    cfg
}

#[tokio::test]
#[serial(chdb)]
async fn primary_failover_updates_shard_map_after_unhealthy_timeout() {
    let dir = tempfile::tempdir().unwrap();
    let listener = bind_ephemeral().await;
    let addr = listener.local_addr().unwrap().to_string();

    let chdb_dir = dir.path().join("chdb");
    std::fs::create_dir_all(&chdb_dir).unwrap();
    let chdb = SharedSession::new_eager(chdb_dir.to_str().unwrap(), 1).unwrap();
    let chdb_adapter = Arc::new(ChdbQueryAdapter::from_shared(chdb.clone(), 0));
    let sink: Arc<dyn PointsSinkPort> = Arc::new(ChdbNativeAdapter::new(chdb));

    let wal_dir = dir.path().join("wal");
    let meta_dir = dir.path().join("meta");
    std::fs::create_dir_all(&wal_dir).unwrap();
    std::fs::create_dir_all(&meta_dir).unwrap();
    let wal = Arc::new(RocksDbWal::open(&wal_dir).unwrap());
    let metadata = Arc::new(RocksDbMetadata::open(&meta_dir).unwrap());
    let mv_service = Arc::new(MaterializedViewService::new(
        metadata.clone(),
        chdb_adapter.clone(),
        sink.clone(),
    ));

    let cluster_cfg = test_cluster_config(dir.path(), &addr);
    let bootstrap = ClusterBootstrap::init(&cluster_cfg, 1000).unwrap();

    let shard_map = Arc::new(RocksDbShardMap::open(&meta_dir, true, 1).unwrap());
    let location_cache = Arc::new(ShardLocationCache::new());
    let raft = bootstrap
        .start_raft(
            &cluster_cfg,
            metadata.clone(),
            mv_service,
            sink.clone(),
            wal.clone(),
            Some((shard_map.clone(), location_cache.clone())),
        )
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(300)).await;

    let region = ShardRegion {
        region_id: 1,
        start: 0,
        end: u64::MAX,
        epoch: ShardEpoch::default(),
        peers: vec![1, 2],
        primary: 1,
        last_split_at: 0,
    };
    let op = ShardMapOp::BootstrapMeasurement {
        key: MeasurementKey::new("sharddb", "autogen", "cpu"),
        region: region.clone(),
    };
    use hyperbytedb::adapters::cluster::raft::types::ClusterRequest;
    raft.client_write(ClusterRequest::ShardMapMutation(Box::new(op)))
        .await
        .unwrap();

    let sharding = hyperbytedb::config::ShardingConfig {
        enabled: true,
        primary_failover_after_secs: 1,
        ..Default::default()
    };

    let scheduler = Arc::new(ShardScheduler::new(
        shard_map.clone(),
        bootstrap.membership.clone(),
        raft.clone(),
        None,
        metadata,
        wal,
        None,
        Some(sink),
        1,
        sharding,
        0,
    ));

    {
        let mut m = bootstrap.membership.write().await;
        m.add_node(hyperbytedb::domain::cluster::membership::NodeInfo {
            node_id: 2,
            addr: "127.0.0.1:2".into(),
            state: NodeState::Active,
            joined_at: 0,
            last_heartbeat: 0,
            needs_sync: false,
        });
    }

    set_node_state(&bootstrap.membership, 1, NodeState::Disconnected).await;

    let (_shutdown_tx, shutdown_rx) = watch::channel(false);
    let sched = scheduler.clone();
    let handle = tokio::spawn(async move {
        sched.run(Duration::from_millis(200), shutdown_rx).await;
    });

    tokio::time::sleep(Duration::from_secs(3)).await;
    handle.abort();

    let map = shard_map.snapshot().await.unwrap();
    let primary = map
        .space("sharddb", "autogen", "cpu")
        .expect("space")
        .regions[0]
        .primary;
    assert_eq!(primary, 2, "expected primary failover to node 2");
}
