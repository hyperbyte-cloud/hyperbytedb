//! Shared harness for sharded multi-node HTTP integration tests.
#![allow(dead_code)]

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use hyperbytedb::adapters::chdb::native_adapter::ChdbNativeAdapter;
use hyperbytedb::adapters::chdb::query_adapter::ChdbQueryAdapter;
use hyperbytedb::adapters::chdb::session::SharedSession;
use hyperbytedb::adapters::cluster::peer_client::PeerClient;
use hyperbytedb::adapters::cluster::raft::HyperbytedbRaft;
use hyperbytedb::adapters::cluster::replication_log::ReplicationLog;
use hyperbytedb::adapters::http::router::{AppState, QueryService, build_router};
use hyperbytedb::adapters::metadata::rocksdb_meta::RocksDbMetadata;
use hyperbytedb::adapters::sharding::rocksdb_shard_map::RocksDbShardMap;
use hyperbytedb::adapters::wal::rocksdb_wal::RocksDbWal;
use hyperbytedb::application::flush_service::FlushServiceImpl;
use hyperbytedb::application::ingest_metadata::IngestCardinalityLimits;
use hyperbytedb::application::materialized_view_service::MaterializedViewService;
use hyperbytedb::application::peer_ingestion_service::PeerIngestionService;
use hyperbytedb::application::peer_query_service::PeerQueryService;
use hyperbytedb::application::query_service::QueryServiceImpl;
use hyperbytedb::application::raft_leader_callbacks::RaftLeaderCallbacks;
use hyperbytedb::application::replication_apply::ReplicationApplyQueue;
use hyperbytedb::application::shard_routing::ShardRoutingContext;
use hyperbytedb::config::{ReplicationConfig, ShardingConfig};
use hyperbytedb::domain::cluster::membership::{
    ClusterMembership, NodeInfo, NodeState, SharedMembership, new_shared,
};
use hyperbytedb::domain::sharding::{
    MeasurementKey, ShardEpoch, ShardLocationCache, ShardMapOp, ShardRegion, ShardTransferPayload,
};
use hyperbytedb::ports::points_sink::PointsSinkPort;
use hyperbytedb::ports::query::QueryPort;
use hyperbytedb::ports::sharding::ShardMapPort;
use tokio::sync::watch;

pub struct ShardedTestNode {
    pub url: String,
    pub addr: String,
    pub node_id: u64,
    pub wal: Arc<RocksDbWal>,
    pub metadata: Arc<RocksDbMetadata>,
    pub points_sink: Arc<dyn PointsSinkPort>,
    pub shard_map: Arc<RocksDbShardMap>,
    pub location_cache: Arc<ShardLocationCache>,
    pub membership: SharedMembership,
    pub query_port: Arc<ChdbQueryAdapter>,
    flush: Arc<FlushServiceImpl>,
    handle: tokio::task::JoinHandle<()>,
    shutdown: Option<watch::Sender<bool>>,
}

#[derive(Clone)]
pub struct PeerSpec {
    pub node_id: u64,
    pub addr: String,
}

pub struct ShardedClusterOptions {
    pub sharding: ShardingConfig,
    pub replication: ReplicationConfig,
    pub raft: Option<HyperbytedbRaft>,
    pub leader_callbacks: Option<Arc<RaftLeaderCallbacks>>,
}

impl Default for ShardedClusterOptions {
    fn default() -> Self {
        Self {
            sharding: ShardingConfig {
                enabled: true,
                scatter_peer_timeout_ms: 500,
                scatter_max_peer_attempts: 3,
                ..Default::default()
            },
            replication: ReplicationConfig::default(),
            raft: None,
            leader_callbacks: None,
        }
    }
}

pub async fn bind_ephemeral() -> tokio::net::TcpListener {
    tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap()
}

pub fn build_shared_membership(specs: &[(u64, String)]) -> SharedMembership {
    let mut membership = ClusterMembership::new();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    for (id, addr) in specs {
        membership.add_node(NodeInfo {
            node_id: *id,
            addr: addr.clone(),
            state: NodeState::Active,
            joined_at: now,
            last_heartbeat: now,
            needs_sync: false,
        });
    }
    new_shared(membership)
}

pub async fn start_sharded_node(
    dir: &Path,
    node_id: u64,
    listener: tokio::net::TcpListener,
    shared_membership: SharedMembership,
    opts: &ShardedClusterOptions,
    chdb: SharedSession,
) -> ShardedTestNode {
    let wal_dir = dir.join(format!("wal-{node_id}"));
    let meta_dir = dir.join(format!("meta-{node_id}"));
    let repl_dir = dir.join(format!("repl-{node_id}"));
    for p in [&wal_dir, &meta_dir, &repl_dir] {
        std::fs::create_dir_all(p).unwrap();
    }

    let chdb_path_str = chdb.data_path().to_owned();
    let wal = Arc::new(RocksDbWal::open(&wal_dir).unwrap());
    let metadata = Arc::new(RocksDbMetadata::open(&meta_dir).unwrap());
    let chdb_adapter = Arc::new(ChdbQueryAdapter::from_shared(chdb.clone(), 0));
    let sink: Arc<dyn PointsSinkPort> = Arc::new(ChdbNativeAdapter::new(chdb));
    let flush: Arc<FlushServiceImpl> =
        Arc::new(FlushServiceImpl::new(wal.clone(), 0, sink.clone()));

    let addr = listener.local_addr().unwrap().to_string();
    let url = format!("http://{addr}");

    let replication_log = Arc::new(ReplicationLog::open(&repl_dir).unwrap());
    let peer_client = Arc::new(PeerClient::new(
        node_id,
        addr.clone(),
        shared_membership.clone(),
        replication_log,
        2,
        8192,
        8,
        8 * 1024 * 1024,
    ));

    let shard_map = Arc::new(RocksDbShardMap::open(&meta_dir, true, node_id).unwrap());
    let location_cache = Arc::new(ShardLocationCache::new());
    let shard_routing = Arc::new(ShardRoutingContext {
        shard_map: shard_map.clone(),
        location_cache: location_cache.clone(),
        config: opts.sharding.clone(),
        node_id,
        peer_client: peer_client.clone(),
        region_write_stats: Arc::new(hyperbytedb::application::runtime::RegionWriteStats::new()),
    });

    let replication_apply = Some(ReplicationApplyQueue::with_sink_and_sharding(
        1024,
        metadata.clone(),
        wal.clone(),
        Some(sink.clone()),
        IngestCardinalityLimits::default(),
        0,
        true,
        node_id,
        Some(shared_membership.clone()),
    ));

    let is_leader: Arc<dyn Fn() -> bool + Send + Sync> = if let Some(ref cb) = opts.leader_callbacks
    {
        let cb = cb.clone();
        Arc::new(move || cb.is_leader())
    } else {
        Arc::new(|| true)
    };
    let leader_addr: Arc<dyn Fn() -> Option<String> + Send + Sync> =
        if let Some(ref cb) = opts.leader_callbacks {
            let cb = cb.clone();
            Arc::new(move || cb.leader_addr())
        } else {
            Arc::new(|| None)
        };

    let mut base_query = QueryServiceImpl::new(
        chdb_adapter.clone(),
        metadata.clone(),
        wal.clone(),
        30,
        sink.clone(),
    )
    .with_sharding(shard_routing.clone());
    base_query =
        base_query.with_cluster_replication(peer_client.clone(), node_id, opts.replication.clone());
    base_query = base_query.with_materialized_view_sharding(
        shard_routing.clone(),
        is_leader.clone(),
        leader_addr.clone(),
    );
    let base_query: Arc<dyn QueryService> = Arc::new(base_query);

    let ingestion: Arc<dyn hyperbytedb::ports::ingestion::IngestionPort> = Arc::new(
        PeerIngestionService::with_replication_and_sink(
            wal.clone(),
            Some(sink.clone()),
            metadata.clone(),
            peer_client.clone(),
            node_id,
            IngestCardinalityLimits::default(),
            0,
            opts.replication.clone(),
        )
        .with_sharding(
            shard_routing.clone(),
            is_leader.clone(),
            leader_addr.clone(),
        ),
    );

    let query: Arc<dyn QueryService> = Arc::new(PeerQueryService::new(
        base_query,
        metadata.clone(),
        peer_client.clone(),
    ));

    let (shutdown_tx, _) = watch::channel(false);

    let app_state = Arc::new(AppState {
        ingestion,
        query,
        query_port: chdb_adapter.clone(),
        metadata: metadata.clone(),
        wal: wal.clone(),
        points_sink: sink.clone(),
        mv_service: Arc::new(
            MaterializedViewService::new(metadata.clone(), chdb_adapter.clone(), sink.clone())
                .with_sharding(shard_routing.clone(), is_leader, leader_addr),
        ),
        auth: Arc::new(hyperbytedb::adapters::auth::MetadataAuthAdapter::new(
            metadata.clone(),
        )),
        peer_client: Some(peer_client),
        membership: Some(shared_membership.clone()),
        replication_log: None,
        drain_service: None,
        raft: opts.raft.clone(),
        auth_enabled: false,
        auth_allow_query_param_credentials: false,
        prometheus_handle: None,
        statement_summary: None,
        statement_summary_require_auth: true,
        replication_apply,
        chdb_session_data_path: chdb_path_str,
        node_id,
        max_body_size_bytes: 25 * 1024 * 1024,
        replicate_body_limit_bytes: 32 * 1024 * 1024,
        max_points_per_request: 0,
        request_timeout_secs: 30,
        rate_limiter: None,
        wal_batcher_alive: None,
        disk_read_only: None,
        sharding_enabled: true,
        shard_map: Some(shard_map.clone() as Arc<dyn ShardMapPort>),
        shard_location_cache: location_cache.clone(),
        shard_routing: Some(shard_routing),
        shard_scheduler: None,
        ingest_cardinality: IngestCardinalityLimits::default(),
        cluster_replication: opts.replication.clone(),
    });

    let app = build_router(app_state);
    let handle = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    tokio::time::sleep(Duration::from_millis(100)).await;

    ShardedTestNode {
        url,
        addr,
        node_id,
        wal,
        metadata,
        points_sink: sink,
        shard_map,
        location_cache,
        membership: shared_membership,
        query_port: chdb_adapter,
        flush,
        handle,
        shutdown: Some(shutdown_tx),
    }
}

pub async fn start_sharded_pair_cluster(
    dir: &Path,
    opts: ShardedClusterOptions,
) -> [ShardedTestNode; 2] {
    let chdb_dir = dir.join("chdb-shared");
    std::fs::create_dir_all(&chdb_dir).unwrap();
    let chdb = SharedSession::new_eager(chdb_dir.to_str().unwrap(), 1).unwrap();

    let l1 = bind_ephemeral().await;
    let l2 = bind_ephemeral().await;
    let a1 = l1.local_addr().unwrap().to_string();
    let a2 = l2.local_addr().unwrap().to_string();

    let membership = build_shared_membership(&[(1, a1), (2, a2)]);

    let n1 = start_sharded_node(dir, 1, l1, membership.clone(), &opts, chdb.clone()).await;
    let n2 = start_sharded_node(dir, 2, l2, membership.clone(), &opts, chdb).await;
    [n1, n2]
}

pub async fn start_sharded_three_node_cluster(
    dir: &Path,
    opts: ShardedClusterOptions,
) -> [ShardedTestNode; 3] {
    let chdb_dir = dir.join("chdb-shared");
    std::fs::create_dir_all(&chdb_dir).unwrap();
    let chdb = SharedSession::new_eager(chdb_dir.to_str().unwrap(), 1).unwrap();

    let l1 = bind_ephemeral().await;
    let l2 = bind_ephemeral().await;
    let l3 = bind_ephemeral().await;
    let a1 = l1.local_addr().unwrap().to_string();
    let a2 = l2.local_addr().unwrap().to_string();
    let a3 = l3.local_addr().unwrap().to_string();

    let membership = build_shared_membership(&[(1, a1), (2, a2), (3, a3)]);

    let n1 = start_sharded_node(dir, 1, l1, membership.clone(), &opts, chdb.clone()).await;
    let n2 = start_sharded_node(dir, 2, l2, membership.clone(), &opts, chdb.clone()).await;
    let n3 = start_sharded_node(dir, 3, l3, membership.clone(), &opts, chdb).await;
    [n1, n2, n3]
}

pub async fn bootstrap_region_on_all_nodes(
    nodes: &[ShardedTestNode],
    db: &str,
    rp: &str,
    measurement: &str,
    peers: Vec<u64>,
    primary: u64,
) {
    let region = ShardRegion {
        region_id: 1,
        start: 0,
        end: u64::MAX,
        epoch: ShardEpoch::default(),
        peers,
        primary,
        last_split_at: 0,
    };
    let op = ShardMapOp::BootstrapMeasurement {
        key: MeasurementKey::new(db, rp, measurement),
        region,
    };
    for node in nodes {
        node.shard_map.apply_op(op.clone()).await.unwrap();
        let snap = node.shard_map.snapshot().await.unwrap();
        node.location_cache.refresh_from_map(&snap);
    }
}

pub async fn set_node_state(membership: &SharedMembership, node_id: u64, state: NodeState) {
    membership.write().await.set_state(node_id, state);
}

pub async fn flush_node(node: &ShardedTestNode) {
    node.flush.flush().await.expect("flush");
}

pub fn stop_node(node: &mut ShardedTestNode) {
    if let Some(tx) = node.shutdown.take() {
        let _ = tx.send(true);
    }
    node.handle.abort();
}

pub async fn create_db(client: &reqwest::Client, url: &str, db: &str) {
    let resp = client
        .get(format!("{url}/query"))
        .query(&[("q", format!("CREATE DATABASE {db}"))])
        .send()
        .await
        .unwrap();
    assert!(
        resp.status().is_success(),
        "create db failed: {}",
        resp.status()
    );
}

pub async fn write_line(
    client: &reqwest::Client,
    url: &str,
    db: &str,
    line: &str,
) -> reqwest::Response {
    client
        .post(format!("{url}/write"))
        .query(&[("db", db)])
        .body(line.to_string())
        .send()
        .await
        .unwrap()
}

pub async fn query_sql(
    client: &reqwest::Client,
    url: &str,
    db: &str,
    q: &str,
) -> reqwest::Response {
    client
        .get(format!("{url}/query"))
        .query(&[("db", db), ("q", q)])
        .send()
        .await
        .unwrap()
}

pub async fn chdb_row_count(query_port: &dyn QueryPort, table: &str) -> u64 {
    query_port
        .execute_sql(&format!(
            "SELECT count() AS c FROM `{table}` FORMAT JSONEachRow"
        ))
        .await
        .unwrap()
        .lines()
        .next()
        .and_then(|l| serde_json::from_str::<serde_json::Value>(l).ok())
        .and_then(|v| v.get("c").and_then(|c| c.as_u64()))
        .unwrap_or(0)
}

pub async fn wait_for_show_materialized_view(
    client: &reqwest::Client,
    url: &str,
    db: &str,
    mv_name: &str,
) -> bool {
    for _ in 0..50 {
        let resp = query_sql(client, url, db, "SHOW MATERIALIZED VIEWS").await;
        if resp.status().is_success() {
            let body = resp.text().await.unwrap_or_default();
            if body.contains(mv_name) {
                return true;
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    false
}

pub async fn wait_for_chdb_mv_object(
    client: &reqwest::Client,
    url: &str,
    mv_name_substr: &str,
) -> bool {
    let chdb_sql = format!(
        "SELECT name FROM system.tables WHERE database = 'default' AND name LIKE '%{mv_name_substr}%' FORMAT TabSeparated"
    );
    for _ in 0..50 {
        let resp = client
            .post(format!("{url}/api/v1/chdb"))
            .json(&serde_json::json!({ "q": chdb_sql }))
            .send()
            .await;
        if let Ok(resp) = resp
            && resp.status().is_success()
        {
            let body = resp.text().await.unwrap_or_default();
            if body.contains(mv_name_substr) {
                return true;
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    false
}

pub async fn optimize_chdb_table(query_port: &dyn QueryPort, table: &str) {
    let _ = query_port
        .execute_sql(&format!("OPTIMIZE TABLE `{table}` FINAL"))
        .await;
}

pub async fn wait_for_chdb_row_count_at_most(
    query_port: &dyn QueryPort,
    table: &str,
    max_rows: u64,
) -> bool {
    for _ in 0..50 {
        optimize_chdb_table(query_port, table).await;
        let count = chdb_row_count(query_port, table).await;
        if count <= max_rows {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    false
}

pub async fn post_shard_transfer(
    client: &reqwest::Client,
    url: &str,
    payload: &ShardTransferPayload,
) -> reqwest::Response {
    client
        .post(format!("{url}/internal/shard/transfer"))
        .json(payload)
        .send()
        .await
        .unwrap()
}
