mod common;

use common::sharding_cluster::*;
use hyperbytedb::domain::series::series_id;
use hyperbytedb::domain::sharding::{
    MeasurementKey, ShardEpoch, ShardMapOp, ShardRegion, ShardTransferPayload, TransferPhase,
};
use hyperbytedb::ports::sharding::ShardMapPort;
use serial_test::serial;
use std::collections::BTreeMap;

/// Epoch nanoseconds on a 1-minute boundary (matches MV `toStartOfInterval` bucket keys).
const MV_MINUTE_ALIGNED_NS: i64 = 1_700_000_040_000_000_000;

#[allow(dead_code)]
async fn bootstrap_two_regions(nodes: &[ShardedTestNode], db: &str, rp: &str, measurement: &str) {
    let mid = u64::MAX / 2;
    bootstrap_region_on_all_nodes(nodes, db, rp, measurement, vec![1, 2, 3], 1).await;

    let region_a = ShardRegion {
        region_id: 1,
        start: 0,
        end: mid,
        epoch: ShardEpoch {
            conf_ver: 0,
            version: 1,
        },
        peers: vec![1, 2],
        primary: 1,
        last_split_at: 0,
        transfer_verified: None,
        transfer_first_seen: None,
    };
    let region_b = ShardRegion {
        region_id: 2,
        start: mid,
        end: u64::MAX,
        epoch: ShardEpoch {
            conf_ver: 0,
            version: 1,
        },
        peers: vec![2, 3],
        primary: 2,
        last_split_at: 0,
        transfer_verified: None,
        transfer_first_seen: None,
    };
    let split = ShardMapOp::Split {
        key: MeasurementKey::new(db, rp, measurement),
        region_id: 1,
        split_key: mid,
        epoch: ShardEpoch::default(),
        left: region_a,
        right: region_b,
    };
    for node in nodes {
        node.shard_map.apply_op(split.clone()).await.unwrap();
        let snap = node.shard_map.snapshot().await.unwrap();
        node.location_cache.refresh_from_map(&snap);
    }
}

fn host_tag(host: &str) -> BTreeMap<String, String> {
    BTreeMap::from([("host".into(), host.into())])
}

#[tokio::test]
#[serial(chdb)]
async fn sharded_mv_incremental_rollup() {
    let dir = tempfile::tempdir().unwrap();
    let nodes =
        start_sharded_three_node_cluster(dir.path(), ShardedClusterOptions::default()).await;
    bootstrap_region_on_all_nodes(&nodes, "sharddb", "autogen", "cpu", vec![1, 2, 3], 1).await;

    let client = reqwest::Client::new();
    for node in &nodes {
        create_db(&client, &node.url, "sharddb").await;
    }

    let t = MV_MINUTE_ALIGNED_NS;
    write_line(
        &client,
        &nodes[0].url,
        "sharddb",
        &format!("cpu,host=a value=10 {t}"),
    )
    .await;
    write_line(
        &client,
        &nodes[0].url,
        "sharddb",
        &format!("cpu,host=b value=20 {t}"),
    )
    .await;
    flush_node(&nodes[0]).await;
    flush_node(&nodes[1]).await;
    flush_node(&nodes[2]).await;

    let src = query_sql(
        &client,
        &nodes[0].url,
        "sharddb",
        &format!(r#"SELECT value FROM "cpu" WHERE time = {t}"#),
    )
    .await;
    assert!(
        src.status().is_success(),
        "source query failed: {}",
        src.status()
    );
    let src_body = src.text().await.unwrap();
    assert!(
        src_body.contains("10") && src_body.contains("20"),
        "source missing written points: {src_body}"
    );

    let create_mv = r#"CREATE MATERIALIZED VIEW "mv_cpu_1m" ON "sharddb" AS SELECT mean("value") INTO "cpu_1m" FROM "cpu" GROUP BY time(1m), *"#;
    let resp = query_sql(&client, &nodes[0].url, "sharddb", create_mv).await;
    assert!(
        resp.status().is_success(),
        "CREATE MV failed: {}",
        resp.text().await.unwrap_or_default()
    );

    let dest_table = "sharddb_autogen_cpu_1m";
    assert_eq!(
        chdb_row_count(nodes[0].query_port.as_ref(), dest_table).await,
        0,
        "dest should be empty before post-create writes"
    );
    let dest_series_table = "sharddb_autogen_cpu_1m_series";
    assert!(
        chdb_row_count(nodes[0].query_port.as_ref(), dest_series_table).await > 0,
        "dest series table should be seeded at MV create"
    );

    let t2 = t + 1_000_000_000;
    write_line(
        &client,
        &nodes[0].url,
        "sharddb",
        &format!("cpu,host=a value=30 {t2}"),
    )
    .await;
    write_line(
        &client,
        &nodes[0].url,
        "sharddb",
        &format!("cpu,host=b value=40 {t2}"),
    )
    .await;
    for node in &nodes {
        flush_node(node).await;
    }

    let q = query_sql(
        &client,
        &nodes[0].url,
        "sharddb",
        &format!(r#"SELECT mean("value") FROM "cpu_1m" WHERE time = {t} GROUP BY *"#),
    )
    .await;
    assert!(q.status().is_success(), "dest query failed: {}", q.status());
    let body = q.text().await.unwrap();
    assert!(
        body.contains("30") && body.contains("40"),
        "expected post-create means for both hosts, got: {body}"
    );
}

#[tokio::test]
#[serial(chdb)]
async fn sharded_mv_drop() {
    let dir = tempfile::tempdir().unwrap();
    let nodes =
        start_sharded_three_node_cluster(dir.path(), ShardedClusterOptions::default()).await;
    bootstrap_region_on_all_nodes(&nodes, "sharddb", "autogen", "cpu", vec![1, 2, 3], 1).await;

    let client = reqwest::Client::new();
    for node in &nodes {
        create_db(&client, &node.url, "sharddb").await;
    }

    write_line(
        &client,
        &nodes[0].url,
        "sharddb",
        &format!("cpu,host=a value=1 {MV_MINUTE_ALIGNED_NS}"),
    )
    .await;
    flush_node(&nodes[0]).await;

    let create_mv = r#"CREATE MATERIALIZED VIEW "mv_cpu_1m" ON "sharddb" AS SELECT mean("value") INTO "cpu_1m" FROM "cpu" GROUP BY time(1m), *"#;
    let resp = query_sql(&client, &nodes[0].url, "sharddb", create_mv).await;
    assert!(resp.status().is_success());

    let drop_mv = r#"DROP MATERIALIZED VIEW "mv_cpu_1m" ON "sharddb""#;
    let resp = query_sql(&client, &nodes[0].url, "sharddb", drop_mv).await;
    assert!(
        resp.status().is_success(),
        "DROP MV failed: {}",
        resp.status()
    );
}

#[tokio::test]
#[serial(chdb)]
async fn sharded_mv_with_backfill() {
    let dir = tempfile::tempdir().unwrap();
    let nodes =
        start_sharded_three_node_cluster(dir.path(), ShardedClusterOptions::default()).await;
    bootstrap_region_on_all_nodes(&nodes, "sharddb", "autogen", "metrics", vec![1, 2, 3], 1).await;

    let client = reqwest::Client::new();
    for node in &nodes {
        create_db(&client, &node.url, "sharddb").await;
    }

    let t = MV_MINUTE_ALIGNED_NS;
    write_line(
        &client,
        &nodes[0].url,
        "sharddb",
        &format!("metrics,host=a value=5 {t}"),
    )
    .await;
    write_line(
        &client,
        &nodes[0].url,
        "sharddb",
        &format!("metrics,host=b value=7 {t}"),
    )
    .await;
    for node in &nodes {
        flush_node(node).await;
    }

    let create_mv = r#"CREATE MATERIALIZED VIEW "mv_metrics" ON "sharddb" WITH BACKFILL AS SELECT sum("value") AS "value" INTO "metrics_1m" FROM "metrics" GROUP BY time(1m), "host""#;
    let resp = query_sql(&client, &nodes[0].url, "sharddb", create_mv).await;
    assert!(
        resp.status().is_success(),
        "CREATE MV WITH BACKFILL failed: {}",
        resp.text().await.unwrap_or_default()
    );

    let dest_table = "sharddb_autogen_metrics_1m";
    let dest_rows = chdb_row_count(nodes[0].query_port.as_ref(), dest_table).await;
    assert!(
        dest_rows >= 2,
        "backfill should populate dest rows, got {dest_rows}"
    );

    let q = query_sql(
        &client,
        &nodes[0].url,
        "sharddb",
        &format!(r#"SELECT sum("value") FROM "metrics_1m" WHERE time = {t} GROUP BY host"#),
    )
    .await;
    assert!(q.status().is_success(), "dest query failed: {}", q.status());
    let body = q.text().await.unwrap();
    assert!(
        body.contains("5") && body.contains("7"),
        "expected backfill sums: {body}"
    );
}

#[tokio::test]
#[serial(chdb)]
async fn sharded_mv_tag_subset_group_by() {
    let dir = tempfile::tempdir().unwrap();
    let nodes =
        start_sharded_three_node_cluster(dir.path(), ShardedClusterOptions::default()).await;
    bootstrap_region_on_all_nodes(&nodes, "sharddb", "autogen", "metrics", vec![1, 2, 3], 1).await;

    let client = reqwest::Client::new();
    for node in &nodes {
        create_db(&client, &node.url, "sharddb").await;
    }

    let t = MV_MINUTE_ALIGNED_NS;
    // Same host tag, different rack — collapsed by GROUP BY host only.
    write_line(
        &client,
        &nodes[0].url,
        "sharddb",
        &format!("metrics,host=a,rack=r1 value=2 {t}"),
    )
    .await;
    write_line(
        &client,
        &nodes[0].url,
        "sharddb",
        &format!("metrics,host=a,rack=r2 value=3 {t}"),
    )
    .await;
    for node in &nodes {
        flush_node(node).await;
    }

    let create_mv = r#"CREATE MATERIALIZED VIEW "mv_host" ON "sharddb" WITH BACKFILL AS SELECT sum("value") AS "value" INTO "metrics_host" FROM "metrics" GROUP BY time(1m), "host""#;
    let resp = query_sql(&client, &nodes[0].url, "sharddb", create_mv).await;
    assert!(
        resp.status().is_success(),
        "CREATE MV failed: {}",
        resp.text().await.unwrap_or_default()
    );

    let q = query_sql(
        &client,
        &nodes[0].url,
        "sharddb",
        &format!(r#"SELECT sum("value") FROM "metrics_host" WHERE host = 'a' AND time = {t}"#),
    )
    .await;
    assert!(q.status().is_success(), "query failed: {}", q.status());
    let body = q.text().await.unwrap();
    assert!(body.contains("5"), "expected merged sum 5, got: {body}");

    // Sanity: hosts map to known series ids for routing setup.
    let _ = series_id("metrics", &host_tag("a"));
}

#[tokio::test]
#[serial(chdb)]
async fn sharded_mv_raft_replicate() {
    let dir = tempfile::tempdir().unwrap();
    let nodes = start_sharded_pair_cluster(dir.path(), ShardedClusterOptions::default()).await;
    bootstrap_region_on_all_nodes(&nodes, "sharddb", "autogen", "cpu", vec![1, 2], 1).await;

    let client = reqwest::Client::new();
    for node in &nodes {
        create_db(&client, &node.url, "sharddb").await;
    }

    let t = MV_MINUTE_ALIGNED_NS;
    write_line(
        &client,
        &nodes[0].url,
        "sharddb",
        &format!("cpu,host=a value=10 {t}"),
    )
    .await;
    flush_node(&nodes[0]).await;
    flush_node(&nodes[1]).await;

    let create_mv = r#"CREATE MATERIALIZED VIEW "mv_cpu_1m" ON "sharddb" AS SELECT mean("value") INTO "cpu_1m" FROM "cpu" GROUP BY time(1m), *"#;
    let resp = query_sql(&client, &nodes[0].url, "sharddb", create_mv).await;
    assert!(
        resp.status().is_success(),
        "CREATE MV on leader failed: {}",
        resp.text().await.unwrap_or_default()
    );

    assert!(
        wait_for_show_materialized_view(&client, &nodes[1].url, "sharddb", "mv_cpu_1m").await,
        "follower missing MV metadata after mutation replication"
    );
    assert!(
        wait_for_chdb_mv_object(&client, &nodes[1].url, "mv_cpu_1m").await,
        "follower missing ClickHouse MV objects after replication"
    );
}

#[tokio::test]
#[serial(chdb)]
async fn sharded_mv_transfer_purge_dest_partials() {
    let dir = tempfile::tempdir().unwrap();
    let nodes =
        start_sharded_three_node_cluster(dir.path(), ShardedClusterOptions::default()).await;
    bootstrap_region_on_all_nodes(&nodes, "sharddb", "autogen", "metrics", vec![1, 2, 3], 1).await;

    let client = reqwest::Client::new();
    for node in &nodes {
        create_db(&client, &node.url, "sharddb").await;
    }

    let t = MV_MINUTE_ALIGNED_NS;
    write_line(
        &client,
        &nodes[0].url,
        "sharddb",
        &format!("metrics,host=a value=4 {t}"),
    )
    .await;
    write_line(
        &client,
        &nodes[0].url,
        "sharddb",
        &format!("metrics,host=b value=9 {t}"),
    )
    .await;
    for node in &nodes {
        flush_node(node).await;
    }

    let create_mv = r#"CREATE MATERIALIZED VIEW "mv_xfer" ON "sharddb" WITH BACKFILL AS SELECT sum("value") AS "value" INTO "metrics_xfer" FROM "metrics" GROUP BY time(1m), "host""#;
    let resp = query_sql(&client, &nodes[0].url, "sharddb", create_mv).await;
    assert!(
        resp.status().is_success(),
        "CREATE MV WITH BACKFILL failed: {}",
        resp.text().await.unwrap_or_default()
    );

    let dest_table = "sharddb_autogen_metrics_xfer";
    let before = chdb_row_count(nodes[0].query_port.as_ref(), dest_table).await;
    assert!(
        before >= 2,
        "expected dest partials for both hosts before transfer, got {before}"
    );

    let sid_a = series_id("metrics", &host_tag("a"));
    let ack = ShardTransferPayload {
        db: "sharddb".into(),
        rp: "autogen".into(),
        measurement: "metrics".into(),
        region_id: 1,
        start: sid_a,
        end: sid_a.saturating_add(1),
        epoch: ShardEpoch::default(),
        phase: TransferPhase::Ack,
        body: None,
        source_node_id: nodes[0].node_id,
        transfer_id: 1,
        seq: 0,
        done: true,
        stage: false,
    };
    let resp = post_shard_transfer(&client, &nodes[0].url, &ack).await;
    assert!(
        resp.status().is_success(),
        "transfer ACK failed: {}",
        resp.status()
    );

    assert!(
        wait_for_chdb_row_count_at_most(nodes[0].query_port.as_ref(), dest_table, before - 1).await,
        "expected dest partial purge after transfer ACK (before={before})"
    );
    let after = chdb_row_count(nodes[0].query_port.as_ref(), dest_table).await;
    assert!(
        after < before,
        "expected fewer dest rows after transfer ACK (before={before}, after={after})"
    );

    let q_a = query_sql(
        &client,
        &nodes[0].url,
        "sharddb",
        &format!(r#"SELECT sum("value") FROM "metrics_xfer" WHERE host = 'a' AND time = {t}"#),
    )
    .await;
    assert!(q_a.status().is_success());
    let body_a = q_a.text().await.unwrap();
    assert!(
        !body_a.contains("4"),
        "host=a dest partials should be purged, got: {body_a}"
    );

    let q_b = query_sql(
        &client,
        &nodes[0].url,
        "sharddb",
        &format!(r#"SELECT sum("value") FROM "metrics_xfer" WHERE host = 'b' AND time = {t}"#),
    )
    .await;
    assert!(q_b.status().is_success());
    let body_b = q_b.text().await.unwrap();
    assert!(
        body_b.contains("9"),
        "host=b dest partials should remain after ranged purge, got: {body_b}"
    );
}
