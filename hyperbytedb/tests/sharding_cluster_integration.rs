mod common;

use common::sharding_cluster::*;
use hyperbytedb::domain::sharding::ShardEpoch;
use serial_test::serial;

#[tokio::test]
#[serial(chdb)]
async fn query_scatter_falls_back_when_primary_unreachable() {
    let dir = tempfile::tempdir().unwrap();
    let mut nodes =
        start_sharded_three_node_cluster(dir.path(), ShardedClusterOptions::default()).await;
    bootstrap_region_on_all_nodes(&nodes, "sharddb", "autogen", "cpu", vec![1, 2], 1).await;

    let client = reqwest::Client::new();
    for node in &nodes {
        create_db(&client, &node.url, "sharddb").await;
    }

    write_line(
        &client,
        &nodes[0].url,
        "sharddb",
        "cpu,host=a value=1 1000000000",
    )
    .await;
    flush_node(&nodes[0]).await;

    stop_node(&mut nodes[0]);

    let resp = query_sql(&client, &nodes[2].url, "sharddb", "SELECT value FROM cpu").await;
    assert!(
        resp.status().is_success(),
        "scatter query failed: {}",
        resp.status()
    );
}

#[tokio::test]
#[serial(chdb)]
async fn write_forwards_to_active_replica_when_primary_down() {
    let dir = tempfile::tempdir().unwrap();
    let mut nodes =
        start_sharded_three_node_cluster(dir.path(), ShardedClusterOptions::default()).await;
    bootstrap_region_on_all_nodes(&nodes, "sharddb", "autogen", "cpu", vec![1, 2], 1).await;

    let client = reqwest::Client::new();
    for node in &nodes {
        create_db(&client, &node.url, "sharddb").await;
    }

    stop_node(&mut nodes[0]);

    let resp = write_line(
        &client,
        &nodes[2].url,
        "sharddb",
        "cpu,host=b value=2 2000000000",
    )
    .await;
    assert!(
        resp.status().is_success(),
        "write failed: {}",
        resp.status()
    );

    flush_node(&nodes[1]).await;

    let q = query_sql(&client, &nodes[1].url, "sharddb", "SHOW MEASUREMENTS").await;
    assert!(q.status().is_success(), "query failed: {}", q.status());
    let body = q.text().await.unwrap();
    assert!(
        body.contains("cpu"),
        "expected measurement on replica: {body}"
    );
}

#[tokio::test]
#[serial(chdb)]
async fn scatter_returns_error_when_all_peers_unreachable() {
    let dir = tempfile::tempdir().unwrap();
    let mut nodes =
        start_sharded_three_node_cluster(dir.path(), ShardedClusterOptions::default()).await;
    bootstrap_region_on_all_nodes(&nodes, "sharddb", "autogen", "cpu", vec![1], 1).await;

    let client = reqwest::Client::new();
    create_db(&client, &nodes[1].url, "sharddb").await;

    stop_node(&mut nodes[0]);

    let resp = query_sql(&client, &nodes[1].url, "sharddb", "SELECT value FROM cpu").await;
    assert!(!resp.status().is_success());
}

#[tokio::test]
#[serial(chdb)]
async fn internal_shard_query_handler_serves_region_sql() {
    let dir = tempfile::tempdir().unwrap();
    let nodes =
        start_sharded_three_node_cluster(dir.path(), ShardedClusterOptions::default()).await;
    bootstrap_region_on_all_nodes(&nodes, "sharddb", "autogen", "cpu", vec![1, 2], 1).await;

    let client = reqwest::Client::new();
    create_db(&client, &nodes[0].url, "sharddb").await;
    write_line(
        &client,
        &nodes[0].url,
        "sharddb",
        "cpu,host=a value=1 1000000000",
    )
    .await;
    flush_node(&nodes[0]).await;

    let req = hyperbytedb::domain::sharding::ShardQueryRequest {
        db: "sharddb".into(),
        rp: "autogen".into(),
        measurement: "cpu".into(),
        epoch: ShardEpoch::default(),
        region_id: 1,
        time_min: 0,
        time_max: i64::MAX,
        series_id_start: 0,
        series_id_end: u64::MAX,
        select_sql: "SELECT \"value\" FROM `sharddb_autogen_cpu`".into(),
    };

    let resp = client
        .post(format!("{}/internal/shard/query", nodes[0].url))
        .json(&req)
        .send()
        .await
        .unwrap();
    let status = resp.status();
    let body = resp.text().await.unwrap();
    assert!(
        status.is_success(),
        "internal query failed: {status} {body}"
    );
}
