mod common;

use common::sharding_cluster::*;
use hyperbytedb::domain::query_result::QueryResponse;
use hyperbytedb::domain::series::series_id;
use hyperbytedb::domain::sharding::{MeasurementKey, ShardEpoch, ShardMapOp, ShardRegion};
use hyperbytedb::ports::forward_queue::ForwardQueuePort;
use hyperbytedb::ports::sharding::ShardMapPort;
use serial_test::serial;
use std::collections::BTreeMap;

#[tokio::test]
#[serial(chdb)]
async fn cross_coordinator_read_after_forwarded_write() {
    let dir = tempfile::tempdir().unwrap();
    let nodes =
        start_sharded_three_node_cluster(dir.path(), ShardedClusterOptions::default()).await;
    bootstrap_region_on_all_nodes(&nodes, "sharddb", "autogen", "cpu", vec![1, 2], 1).await;

    let client = reqwest::Client::new();
    for node in &nodes {
        create_db(&client, &node.url, "sharddb").await;
    }

    // Node 3 is not a region peer; write forwards to primary (node 1).
    write_line(
        &client,
        &nodes[2].url,
        "sharddb",
        "cpu,host=crossnode value=99 5000000000",
    )
    .await;
    flush_node(&nodes[0]).await;

    // Node 2 is a replica peer — must read via primary scatter, not stale local chDB.
    let resp = query_parsed(
        &client,
        &nodes[1].url,
        "sharddb",
        "SELECT value FROM cpu WHERE host='crossnode'",
    )
    .await;
    let value = resp.results[0]
        .series
        .as_ref()
        .and_then(|s| s.first())
        .and_then(|s| s.values.first())
        .and_then(|row| row.get(1))
        .and_then(|v| v.as_f64());
    assert_eq!(value, Some(99.0), "cross-coordinator RYW read");
}

#[tokio::test]
#[serial(chdb)]
async fn group_by_host_mean_on_replica_coordinator() {
    let dir = tempfile::tempdir().unwrap();
    let nodes =
        start_sharded_three_node_cluster(dir.path(), ShardedClusterOptions::default()).await;
    bootstrap_region_on_all_nodes(&nodes, "sharddb", "autogen", "cpu", vec![1, 2], 1).await;

    let client = reqwest::Client::new();
    for node in &nodes {
        create_db(&client, &node.url, "sharddb").await;
    }

    for i in 1..=20 {
        write_line(
            &client,
            &nodes[2].url,
            "sharddb",
            &format!("cpu,host=node{i} value={i} {}", 1_000_000_000 + i),
        )
        .await;
    }
    flush_node(&nodes[0]).await;

    let resp = query_parsed(
        &client,
        &nodes[1].url,
        "sharddb",
        "SELECT mean(value) FROM cpu GROUP BY host",
    )
    .await;
    let series = resp.results[0].series.as_ref().expect("series");
    assert!(
        series.len() >= 20,
        "expected >=20 host groups, got {}",
        series.len()
    );
}

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
async fn write_fails_fast_when_primary_down_then_succeeds_after_failover() {
    let dir = tempfile::tempdir().unwrap();
    let mut nodes =
        start_sharded_three_node_cluster(dir.path(), ShardedClusterOptions::default()).await;
    bootstrap_region_on_all_nodes(&nodes, "sharddb", "autogen", "cpu", vec![1, 2], 1).await;

    let client = reqwest::Client::new();
    for node in &nodes {
        create_db(&client, &node.url, "sharddb").await;
    }

    stop_node(&mut nodes[0]);

    // Single-writer invariant: with the region primary down there is no valid
    // write target — the coordinator must reject the write rather than accept
    // it on a replica (which would create two concurrent WAL writers).
    let resp = write_line(
        &client,
        &nodes[2].url,
        "sharddb",
        "cpu,host=b value=2 2000000000",
    )
    .await;
    assert!(
        !resp.status().is_success(),
        "write must fail while region primary is down, got {}",
        resp.status()
    );

    // Failover: move the primary role to the surviving replica (node 2).
    promote_region_primary_on_all_nodes(&nodes, "sharddb", "autogen", "cpu", 2).await;

    let resp = write_line(
        &client,
        &nodes[1].url,
        "sharddb",
        "cpu,host=b value=3 3000000000",
    )
    .await;
    assert!(
        resp.status().is_success(),
        "write after failover failed: {}",
        resp.status()
    );

    flush_node(&nodes[1]).await;

    let q = query_sql(&client, &nodes[1].url, "sharddb", "SHOW MEASUREMENTS").await;
    assert!(q.status().is_success(), "query failed: {}", q.status());
    let body = q.text().await.unwrap();
    assert!(
        body.contains("cpu"),
        "expected measurement on promoted primary: {body}"
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
        node_local: false,
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

#[tokio::test]
#[serial(chdb)]
async fn transfer_moves_flushed_data_and_drops_source_range() {
    use hyperbytedb::application::shard_transfer::run_region_transfer;
    use hyperbytedb::domain::series::series_id;
    use hyperbytedb::domain::sharding::{MeasurementKey, ShardRegion};
    use hyperbytedb::ports::metadata::MetadataPort;
    use hyperbytedb::ports::query::QueryPort;
    use hyperbytedb::ports::wal::WalPort;
    use std::collections::BTreeMap;
    use std::sync::Arc;

    let dir = tempfile::tempdir().unwrap();
    let nodes = start_sharded_pair_cluster(dir.path(), ShardedClusterOptions::default()).await;
    bootstrap_region_on_all_nodes(&nodes, "sharddb", "autogen", "cpu", vec![1, 2], 1).await;

    let client = reqwest::Client::new();
    for node in &nodes {
        create_db(&client, &node.url, "sharddb").await;
    }

    write_line(
        &client,
        &nodes[0].url,
        "sharddb",
        "cpu,host=keep value=1 1000000000",
    )
    .await;
    write_line(
        &client,
        &nodes[0].url,
        "sharddb",
        "cpu,host=move value=2 2000000000",
    )
    .await;
    flush_node(&nodes[0]).await;

    let keep_id = series_id("cpu", &BTreeMap::from([("host".into(), "keep".into())]));
    let move_id = series_id("cpu", &BTreeMap::from([("host".into(), "move".into())]));
    let split = keep_id.min(move_id).saturating_add(1);

    let transfer_region = ShardRegion {
        region_id: 1,
        start: split,
        end: u64::MAX,
        epoch: ShardEpoch::default(),
        peers: vec![1, 2],
        primary: 1,
        last_split_at: 0,
        transfer_verified: None,
        transfer_first_seen: None,
        state: Default::default(),
        staged_from: None,
        staging_peers: Vec::new(),
    };
    let key = MeasurementKey::new("sharddb", "autogen", "cpu");

    let peer_client = Arc::new(
        hyperbytedb::adapters::cluster::peer_client::PeerClient::new(
            1,
            nodes[0].addr.clone(),
            nodes[0].membership.clone(),
            Arc::new(
                hyperbytedb::adapters::cluster::replication_log::ReplicationLog::open(
                    dir.path().join("repl-transfer"),
                )
                .unwrap(),
            ),
            2,
            8192,
            8,
            8 * 1024 * 1024,
            Default::default(),
        ),
    );

    let metadata: Arc<dyn MetadataPort> = nodes[0].metadata.clone();
    let wal: Arc<dyn WalPort> = nodes[0].wal.clone();
    let query_port: Arc<dyn QueryPort> = nodes[0].query_port.clone();

    run_region_transfer(
        &peer_client,
        &metadata,
        &wal,
        Some(&query_port),
        Some(&nodes[0].points_sink),
        1,
        &key,
        &transfer_region,
        2,
        10_000,
        true,
    )
    .await
    .expect("region transfer");

    let remaining = nodes[0]
        .metadata
        .list_series_ids("sharddb", "autogen", "cpu")
        .await
        .unwrap();
    assert!(remaining.contains(&keep_id.min(move_id)));
    assert!(!remaining.contains(&keep_id.max(move_id)));

    let table = "sharddb_autogen_cpu";
    let dest_rows = chdb_row_count(nodes[1].query_port.as_ref(), table).await;
    assert!(
        dest_rows >= 1,
        "dest should retain flushed rows, got {dest_rows}"
    );
}

// Automatic split trigger (series-count threshold) is covered by
// `tick_proposes_split_when_series_count_exceeds_threshold` in shard_scheduler unit tests.
// Cluster integration tests below exercise post-split multi-region query merge behavior.

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
        state: Default::default(),
        staged_from: None,
        staging_peers: Vec::new(),
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
        state: Default::default(),
        staged_from: None,
        staging_peers: Vec::new(),
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

fn hosts_for_region_split(measurement: &str, mid: u64) -> (String, String) {
    let mut low = None;
    let mut high = None;
    for i in 0..100_000 {
        let host = format!("host{i}");
        let sid = series_id(
            measurement,
            &BTreeMap::from([("host".into(), host.clone())]),
        );
        if sid < mid && low.is_none() {
            low = Some(host.clone());
        }
        if sid >= mid && high.is_none() {
            high = Some(host);
        }
        if low.is_some() && high.is_some() {
            break;
        }
    }
    (
        low.expect("expected a host in the low region"),
        high.expect("expected a host in the high region"),
    )
}

async fn query_parsed(client: &reqwest::Client, url: &str, db: &str, q: &str) -> QueryResponse {
    let resp = query_sql(client, url, db, q).await;
    let status = resp.status();
    let body = resp.text().await.unwrap_or_default();
    assert!(status.is_success(), "query failed: {status} body={body}");
    serde_json::from_str(&body).unwrap()
}

#[tokio::test]
#[serial(chdb)]
async fn multi_region_limit_offset_applies_globally() {
    let dir = tempfile::tempdir().unwrap();
    let nodes =
        start_sharded_three_node_cluster(dir.path(), ShardedClusterOptions::default()).await;
    bootstrap_two_regions(&nodes, "sharddb", "autogen", "cpu").await;

    let client = reqwest::Client::new();
    for node in &nodes {
        create_db(&client, &node.url, "sharddb").await;
    }

    let mid = u64::MAX / 2;
    let (host_low, host_high) = hosts_for_region_split("cpu", mid);

    let points = [
        (1_000_000_000_i64, 10.0, &host_low),
        (2_000_000_000, 20.0, &host_low),
        (3_000_000_000, 30.0, &host_high),
        (4_000_000_000, 40.0, &host_high),
    ];
    for (ts, value, host) in points {
        write_line(
            &client,
            &nodes[0].url,
            "sharddb",
            &format!("cpu,host={host} value={value} {ts}"),
        )
        .await;
    }
    flush_node(&nodes[0]).await;
    flush_node(&nodes[1]).await;

    let resp = query_parsed(
        &client,
        &nodes[2].url,
        "sharddb",
        "SELECT value FROM cpu ORDER BY time ASC LIMIT 2 OFFSET 1",
    )
    .await;
    let series = resp.results[0].series.as_ref().expect("series");
    let mut values: Vec<f64> = series
        .iter()
        .flat_map(|s| s.values.iter().map(|row| row[1].as_f64().unwrap()))
        .collect();
    values.sort_by(|a, b| a.partial_cmp(b).unwrap());
    assert_eq!(values.len(), 2);
    assert_eq!(values[0], 20.0);
    assert_eq!(values[1], 30.0);

    let desc = query_parsed(
        &client,
        &nodes[2].url,
        "sharddb",
        "SELECT time, value FROM cpu ORDER BY time DESC LIMIT 2",
    )
    .await;
    let desc_series = desc.results[0].series.as_ref().expect("series");
    let mut desc_values: Vec<f64> = desc_series
        .iter()
        .flat_map(|s| {
            let vi = s.columns.iter().position(|c| c == "value").unwrap_or(1);
            s.values.iter().map(move |row| row[vi].as_f64().unwrap())
        })
        .collect();
    desc_values.sort_by(|a, b| b.partial_cmp(a).unwrap());
    assert_eq!(desc_values.len(), 2);
    assert_eq!(desc_values[0], 40.0);
    assert_eq!(desc_values[1], 30.0);
}

#[tokio::test]
#[serial(chdb)]
async fn multi_region_mean_merges_sum_and_count() {
    let dir = tempfile::tempdir().unwrap();
    let nodes =
        start_sharded_three_node_cluster(dir.path(), ShardedClusterOptions::default()).await;
    bootstrap_two_regions(&nodes, "sharddb", "autogen", "cpu").await;

    let client = reqwest::Client::new();
    for node in &nodes {
        create_db(&client, &node.url, "sharddb").await;
    }

    let mid = u64::MAX / 2;
    let (host_low, host_high) = hosts_for_region_split("cpu", mid);

    write_line(
        &client,
        &nodes[0].url,
        "sharddb",
        &format!("cpu,host={host_low} value=10 1000000000"),
    )
    .await;
    write_line(
        &client,
        &nodes[0].url,
        "sharddb",
        &format!("cpu,host={host_low} value=20 1000000001"),
    )
    .await;
    write_line(
        &client,
        &nodes[0].url,
        "sharddb",
        &format!("cpu,host={host_high} value=30 1000000002"),
    )
    .await;
    flush_node(&nodes[0]).await;
    flush_node(&nodes[1]).await;

    let resp = query_parsed(
        &client,
        &nodes[2].url,
        "sharddb",
        "SELECT mean(value) FROM cpu",
    )
    .await;
    let series = resp.results[0].series.as_ref().expect("series");
    // InfluxQL: an aggregate over an unbounded range is stamped at epoch 0.
    assert_eq!(series[0].columns, ["time", "mean"]);
    assert_eq!(series[0].values[0][0], "1970-01-01T00:00:00Z");
    let mean = series[0].values[0][1].as_f64().expect("mean");
    assert!(
        (mean - 20.0).abs() < 0.01,
        "expected global mean 20.0, got {mean}"
    );
}

/// `GROUP BY time(...)` across regions merges per bucket. The region query
/// used to gain a bare `time` column next to the bucket, which chDB rejects
/// in an aggregate ("Column 'time' is not under aggregate function").
#[tokio::test]
#[serial(chdb)]
async fn multi_region_group_by_time_merges_buckets() {
    let dir = tempfile::tempdir().unwrap();
    let nodes =
        start_sharded_three_node_cluster(dir.path(), ShardedClusterOptions::default()).await;
    bootstrap_two_regions(&nodes, "sharddb", "autogen", "cpu").await;

    let client = reqwest::Client::new();
    for node in &nodes {
        create_db(&client, &node.url, "sharddb").await;
    }

    let mid = u64::MAX / 2;
    let (host_low, host_high) = hosts_for_region_split("cpu", mid);
    let s = 1_000_000_000u64;
    let body = format!(
        "cpu,host={host_low} value=10 {}\n\
         cpu,host={host_low} value=20 {}\n\
         cpu,host={host_high} value=30 {}\n\
         cpu,host={host_high} value=40 {}",
        0,
        30 * s,
        10 * s,
        70 * s
    );
    let resp = write_line(&client, &nodes[0].url, "sharddb", &body).await;
    assert!(resp.status().is_success(), "write: {}", resp.status());
    flush_node(&nodes[0]).await;
    flush_node(&nodes[1]).await;

    for node in &nodes {
        let resp = query_parsed(
            &client,
            &node.url,
            "sharddb",
            &format!(
                "SELECT count(value), mean(value) FROM cpu \
                 WHERE time >= 0 AND time < {} GROUP BY time(1m)",
                120 * s
            ),
        )
        .await;
        let result = &resp.results[0];
        assert!(result.error.is_none(), "{}: {:?}", node.url, result.error);
        let series = result.series.as_ref().expect("series");
        let rows: Vec<(f64, f64)> = series[0]
            .values
            .iter()
            .map(|r| (r[1].as_f64().unwrap(), r[2].as_f64().unwrap()))
            .collect();
        assert_eq!(
            rows,
            vec![(3.0, 20.0), (1.0, 40.0)],
            "{}: per-minute count and mean across both regions",
            node.url
        );
    }
}

/// TOP, BOTTOM and SAMPLE across two regions: each region returns its own
/// N per group and the coordinator ranks the union again, so the result is
/// the global N, not N per region.
#[tokio::test]
#[serial(chdb)]
async fn multi_region_top_bottom_and_sample() {
    let dir = tempfile::tempdir().unwrap();
    let nodes =
        start_sharded_three_node_cluster(dir.path(), ShardedClusterOptions::default()).await;
    bootstrap_two_regions(&nodes, "sharddb", "autogen", "cpu").await;
    let client = reqwest::Client::new();
    for node in &nodes {
        create_db(&client, &node.url, "sharddb").await;
    }
    let (low, high) = hosts_for_region_split("cpu", u64::MAX / 2);
    let s = 1_000_000_000u64;
    // Low region: 10 @0s, 20 @30s, 25 @50s. High region: 30 @10s, 40 @70s,
    // 5 @80s.
    let body = format!(
        "cpu,host={low} value=10 0\n\
         cpu,host={low} value=20 {}\n\
         cpu,host={low} value=25 {}\n\
         cpu,host={high} value=30 {}\n\
         cpu,host={high} value=40 {}\n\
         cpu,host={high} value=5 {}",
        30 * s,
        50 * s,
        10 * s,
        70 * s,
        80 * s
    );
    let resp = write_line(&client, &nodes[0].url, "sharddb", &body).await;
    assert!(resp.status().is_success(), "write: {}", resp.status());
    for node in &nodes {
        flush_node(node).await;
    }
    let range = format!("time >= 0 AND time < {}", 120 * s);
    let values = |resp: &QueryResponse| -> Vec<Vec<f64>> {
        let result = &resp.results[0];
        assert!(result.error.is_none(), "{:?}", result.error);
        result
            .series
            .as_ref()
            .expect("series")
            .iter()
            .map(|series| {
                assert!(
                    !series.columns.iter().any(|c| c.starts_with("__")),
                    "{:?}",
                    series.columns
                );
                series
                    .values
                    .iter()
                    .map(|r| r[1].as_f64().unwrap())
                    .collect()
            })
            .collect()
    };
    for node in &nodes {
        let resp = query_parsed(
            &client,
            &node.url,
            "sharddb",
            &format!("SELECT top(value, 2) FROM cpu WHERE {range}"),
        )
        .await;
        assert_eq!(
            values(&resp),
            [vec![30.0, 40.0]],
            "{}: global top 2, time order",
            node.url
        );

        let resp = query_parsed(
            &client,
            &node.url,
            "sharddb",
            &format!("SELECT top(value, 1) FROM cpu WHERE {range} GROUP BY time(1m)"),
        )
        .await;
        assert_eq!(
            values(&resp),
            [vec![30.0, 40.0]],
            "{}: per minute",
            node.url
        );

        let resp = query_parsed(
            &client,
            &node.url,
            "sharddb",
            &format!("SELECT bottom(value, 1) FROM cpu WHERE {range} GROUP BY host"),
        )
        .await;
        let mut per_host = values(&resp);
        per_host.sort_by(|a, b| a[0].total_cmp(&b[0]));
        assert_eq!(per_host, [vec![5.0], vec![10.0]], "{}: per host", node.url);

        let resp = query_parsed(
            &client,
            &node.url,
            "sharddb",
            &format!("SELECT top(value, 3) FROM cpu WHERE {range} LIMIT 1 OFFSET 1"),
        )
        .await;
        assert_eq!(
            values(&resp),
            [vec![25.0]],
            "{}: LIMIT/OFFSET after ranking",
            node.url
        );

        let resp = query_parsed(
            &client,
            &node.url,
            "sharddb",
            &format!("SELECT sample(value, 2) FROM cpu WHERE {range}"),
        )
        .await;
        assert_eq!(
            values(&resp)[0].len(),
            2,
            "{}: two points overall",
            node.url
        );
    }
}

/// A subquery over time buckets on a sharded measurement: the inner result
/// is gathered from the regions and the outer query runs over it.
#[tokio::test]
#[serial(chdb)]
async fn multi_region_subquery_over_time_buckets() {
    let dir = tempfile::tempdir().unwrap();
    let nodes =
        start_sharded_three_node_cluster(dir.path(), ShardedClusterOptions::default()).await;
    bootstrap_two_regions(&nodes, "sharddb", "autogen", "cpu").await;
    let client = reqwest::Client::new();
    for node in &nodes {
        create_db(&client, &node.url, "sharddb").await;
    }
    let (low, high) = hosts_for_region_split("cpu", u64::MAX / 2);
    let s = 1_000_000_000u64;
    let body = format!(
        "cpu,host={low} value=10 0\n\
         cpu,host={low} value=20 {}\n\
         cpu,host={high} value=30 {}\n\
         cpu,host={high} value=40 {}",
        30 * s,
        70 * s,
        100 * s
    );
    let resp = write_line(&client, &nodes[0].url, "sharddb", &body).await;
    assert!(resp.status().is_success(), "write: {}", resp.status());
    for node in &nodes {
        flush_node(node).await;
    }
    let range = format!("time >= 0 AND time < {}", 120 * s);
    for node in &nodes {
        let resp = query_parsed(
            &client,
            &node.url,
            "sharddb",
            &format!(
                "SELECT max(m) FROM (SELECT mean(value) AS m FROM cpu WHERE {range} GROUP BY time(1m), host) WHERE {range} GROUP BY time(2m)"
            ),
        )
        .await;
        let result = &resp.results[0];
        assert!(result.error.is_none(), "{}: {:?}", node.url, result.error);
        let series = result.series.as_ref().expect("series");
        let maxes: Vec<f64> = series[0]
            .values
            .iter()
            .map(|r| r[1].as_f64().unwrap())
            .collect();
        // Per minute and host: 15 (low, :00), 35 (high, :01): one 2 m bucket.
        assert_eq!(maxes, [35.0], "{}", node.url);
    }
}

/// Across regions, a lone selector reports the winning point's time and a
/// plain aggregate epoch 0, as on one node. The winners sit in
/// different regions: max and last in one, min and first in the other.
#[tokio::test]
#[serial(chdb)]
async fn multi_region_selectors_report_the_selected_points_time() {
    let dir = tempfile::tempdir().unwrap();
    let nodes =
        start_sharded_three_node_cluster(dir.path(), ShardedClusterOptions::default()).await;
    bootstrap_two_regions(&nodes, "sharddb", "autogen", "cpu").await;
    let client = reqwest::Client::new();
    for node in &nodes {
        create_db(&client, &node.url, "sharddb").await;
    }
    let (low, high) = hosts_for_region_split("cpu", u64::MAX / 2);
    let s = 1_000_000_000u64;
    let body = format!(
        "cpu,host={low} value=10 0\n\
         cpu,host={low} value=20 {}\n\
         cpu,host={high} value=30 {}\n\
         cpu,host={high} value=40 {}",
        30 * s,
        10 * s,
        70 * s
    );
    let resp = write_line(&client, &nodes[0].url, "sharddb", &body).await;
    assert!(resp.status().is_success(), "write: {}", resp.status());
    flush_node(&nodes[0]).await;
    flush_node(&nodes[1]).await;

    let cases = [
        (
            "SELECT max(value) FROM cpu",
            "max",
            "1970-01-01T00:01:10Z",
            40.0,
        ),
        (
            "SELECT last(value) FROM cpu",
            "last",
            "1970-01-01T00:01:10Z",
            40.0,
        ),
        (
            "SELECT min(value) FROM cpu",
            "min",
            "1970-01-01T00:00:00Z",
            10.0,
        ),
        (
            "SELECT first(value) FROM cpu",
            "first",
            "1970-01-01T00:00:00Z",
            10.0,
        ),
        // Stamped with the WHERE's lower bound, as InfluxDB 1.11 does.
        (
            "SELECT count(value) FROM cpu WHERE time >= 5s",
            "count",
            "1970-01-01T00:00:05Z",
            3.0,
        ),
    ];
    for node in &nodes {
        for (q, col, time, value) in cases {
            let resp = query_parsed(&client, &node.url, "sharddb", q).await;
            let series = resp.results[0].series.as_ref().expect("series");
            assert_eq!(series.len(), 1, "{q} via {}", node.url);
            assert_eq!(series[0].columns, ["time", col], "{q} via {}", node.url);
            assert_eq!(series[0].values.len(), 1, "{q} via {}", node.url);
            assert_eq!(series[0].values[0][0], time, "{q} via {}", node.url);
            assert_eq!(
                series[0].values[0][1].as_f64(),
                Some(value),
                "{q} via {}",
                node.url
            );
        }
    }
}

/// With `forward_coalesce` on, concurrent writes forward through the
/// per-node lanes and every point lands once.
#[tokio::test]
#[serial(chdb)]
async fn coalesced_forwards_land_every_point() {
    let dir = tempfile::tempdir().unwrap();
    let mut opts = ShardedClusterOptions::default();
    opts.sharding.forward_coalesce = true;
    // The server's default. These writes are the measurement's first, and
    // creating its tables made them take about 500 ms with libchdb 26.9:
    // the suite's 500 ms failed them now and then on main.
    opts.sharding.scatter_peer_timeout_ms = 5_000;
    let nodes = start_sharded_three_node_cluster(dir.path(), opts).await;
    bootstrap_two_regions(&nodes, "sharddb", "autogen", "cpu").await;
    let client = reqwest::Client::new();
    create_db(&client, &nodes[0].url, "sharddb").await;
    for node in &nodes[1..] {
        wait_for_database(&client, &node.url, "sharddb").await;
    }
    let (low, high) = hosts_for_region_split("cpu", u64::MAX / 2);

    // Node 3 primaries neither region, so every write forwards both halves.
    let writes = (0..20u64).map(|i| {
        let client = client.clone();
        let url = nodes[2].url.clone();
        let body = format!(
            "cpu,host={low} value={i} {t}\ncpu,host={high} value={i} {t}",
            t = 1_000_000_000 + i
        );
        async move {
            let resp = write_line(&client, &url, "sharddb", &body).await;
            (resp.status(), resp.text().await.unwrap_or_default())
        }
    });
    for (status, body) in futures::future::join_all(writes).await {
        assert!(status.is_success(), "write: {status} {body}");
    }
    assert!(
        nodes[2].shard_routing.forward_coalescer.lane_count() > 0,
        "forwards went through the coalescer"
    );
    flush_node(&nodes[0]).await;
    flush_node(&nodes[1]).await;

    let resp = query_parsed(
        &client,
        &nodes[2].url,
        "sharddb",
        "SELECT count(value) FROM cpu",
    )
    .await;
    let count = resp.results[0].series.as_ref().unwrap()[0].values[0][1].as_f64();
    assert_eq!(count, Some(40.0), "every forwarded point landed once");
}

/// With peers on HTTP/2, forwards and replication still land every point.
#[tokio::test]
#[serial(chdb)]
async fn forwards_and_replication_work_over_http2() {
    use hyperbytedb::ports::wal::WalPort;

    let dir = tempfile::tempdir().unwrap();
    let opts = ShardedClusterOptions::default();
    let nodes = start_sharded_three_node_cluster(dir.path(), opts).await;
    bootstrap_two_regions(&nodes, "sharddb", "autogen", "cpu").await;
    let client = reqwest::Client::new();
    create_db(&client, &nodes[0].url, "sharddb").await;
    for node in &nodes[1..] {
        wait_for_database(&client, &node.url, "sharddb").await;
    }
    let (low, high) = hosts_for_region_split("cpu", u64::MAX / 2);
    // One write first: it creates the tables. Twenty concurrent first writes
    // all run that DDL on the shared chDB session and outlast the 500 ms
    // forward timeout, over HTTP/1.1 as much as HTTP/2.
    let warm = format!("cpu,host={low} value=0 999000000\ncpu,host={high} value=0 999000000");
    let resp = write_line(&client, &nodes[2].url, "sharddb", &warm).await;
    assert!(
        resp.status().is_success(),
        "warm-up write: {}",
        resp.status()
    );
    let replica_before = nodes[2].wal.last_sequence().await.unwrap();

    // Node 3 primaries neither region: both halves forward, then replicate.
    let writes = (0..20u64).map(|i| {
        let client = client.clone();
        let url = nodes[2].url.clone();
        let body = format!(
            "cpu,host={low} value={i} {t}\ncpu,host={high} value={i} {t}",
            t = 1_000_000_000 + i
        );
        async move { write_line(&client, &url, "sharddb", &body).await.status() }
    });
    for status in futures::future::join_all(writes).await {
        assert!(status.is_success(), "write: {status}");
    }
    flush_node(&nodes[0]).await;
    flush_node(&nodes[1]).await;
    let resp = query_parsed(
        &client,
        &nodes[2].url,
        "sharddb",
        "SELECT count(value) FROM cpu",
    )
    .await;
    let count = resp.results[0].series.as_ref().unwrap()[0].values[0][1].as_f64();
    assert_eq!(count, Some(42.0), "every forwarded point landed once");

    // Region A's peers are nodes 1 and 2; region B's include node 3, so
    // replication reaches node 3's WAL.
    let mut replicated = 0usize;
    for _ in 0..200 {
        replicated = nodes[2]
            .wal
            .read_from(replica_before + 1)
            .await
            .unwrap()
            .iter()
            .map(|(_, e)| e.points.len())
            .sum();
        if replicated > 0 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    assert!(replicated > 0, "replication reached a peer over HTTP/2");
}

#[tokio::test]
#[serial(chdb)]
async fn multi_region_percentile_returns_error() {
    let dir = tempfile::tempdir().unwrap();
    let nodes =
        start_sharded_three_node_cluster(dir.path(), ShardedClusterOptions::default()).await;
    bootstrap_two_regions(&nodes, "sharddb", "autogen", "cpu").await;

    let client = reqwest::Client::new();
    for node in &nodes {
        create_db(&client, &node.url, "sharddb").await;
    }

    let mid = u64::MAX / 2;
    let (host_low, host_high) = hosts_for_region_split("cpu", mid);
    write_line(
        &client,
        &nodes[0].url,
        "sharddb",
        &format!("cpu,host={host_low} value=1 1000000000"),
    )
    .await;
    write_line(
        &client,
        &nodes[0].url,
        "sharddb",
        &format!("cpu,host={host_high} value=2 1000000001"),
    )
    .await;
    for node in &nodes {
        flush_node(node).await;
    }

    let resp = query_sql(
        &client,
        &nodes[2].url,
        "sharddb",
        "SELECT percentile(value, 95) FROM cpu",
    )
    .await;
    let status = resp.status();
    let body = resp.text().await.unwrap();
    // Both regions have points for the one group (the whole measurement):
    // refused, not merged.
    assert!(
        !status.is_success() && body.contains("spans"),
        "percentile over a group spanning regions must fail: {status} {body}"
    );
}

#[tokio::test]
#[serial(chdb)]
async fn shard_write_sync_quorum_fails_when_peer_unreachable() {
    use hyperbytedb::config::{
        ReplicationConfig, ReplicationMode, SyncQuorumConfig, SyncQuorumMinAcks,
    };
    use hyperbytedb::domain::sharding::ShardWriteRequest;

    let dir = tempfile::tempdir().unwrap();
    let opts = ShardedClusterOptions {
        replication: ReplicationConfig {
            mode: ReplicationMode::SyncQuorum,
            sync_quorum: SyncQuorumConfig {
                min_acks: SyncQuorumMinAcks::Count(2),
            },
            ack_timeout_ms: 500,
        },
        ..Default::default()
    };
    let mut nodes = start_sharded_pair_cluster(dir.path(), opts).await;
    bootstrap_region_on_all_nodes(&nodes, "sharddb", "autogen", "cpu", vec![1, 2], 1).await;

    let client = reqwest::Client::new();
    create_db(&client, &nodes[0].url, "sharddb").await;

    stop_node(&mut nodes[1]);

    let req = ShardWriteRequest {
        db: "sharddb".into(),
        rp: "autogen".into(),
        precision: None,
        epoch: ShardEpoch::default(),
        region_id: 1,
        body: b"cpu,host=a value=1 1000000000".to_vec(),
    };
    let resp = client
        .post(format!("{}/internal/shard/write", nodes[0].url))
        .json(&req)
        .send()
        .await
        .unwrap();
    assert!(
        !resp.status().is_success(),
        "expected sync quorum failure, got {}",
        resp.status()
    );
}

#[tokio::test]
#[serial(chdb)]
async fn multi_region_count_merges_across_regions() {
    let dir = tempfile::tempdir().unwrap();
    let nodes =
        start_sharded_three_node_cluster(dir.path(), ShardedClusterOptions::default()).await;
    bootstrap_two_regions(&nodes, "sharddb", "autogen", "cpu").await;

    let client = reqwest::Client::new();
    create_db(&client, &nodes[0].url, "sharddb").await;
    for node in &nodes[1..] {
        wait_for_database(&client, &node.url, "sharddb").await;
    }

    let mid = u64::MAX / 2;
    let (host_low, host_high) = hosts_for_region_split("cpu", mid);

    for (i, host) in [host_low.as_str(), host_high.as_str()].iter().enumerate() {
        for j in 0..3 {
            write_line(
                &client,
                &nodes[i].url,
                "sharddb",
                &format!(
                    "cpu,host={host} value={} {}",
                    j + 1,
                    1_000_000_000 + j as i64
                ),
            )
            .await;
        }
        flush_node(&nodes[i]).await;
    }

    let resp = query_parsed(
        &client,
        &nodes[2].url,
        "sharddb",
        "SELECT count(value) FROM cpu",
    )
    .await;
    let count = resp.results[0]
        .series
        .as_ref()
        .and_then(|s| s.first())
        .and_then(|s| s.values.first())
        .and_then(|row| row.last())
        .and_then(|v| v.as_f64())
        .unwrap_or(0.0);
    assert_eq!(count, 6.0, "expected global count across two regions");
}

#[tokio::test]
#[serial(chdb)]
async fn aggregate_read_from_primary_when_replica_lags() {
    let dir = tempfile::tempdir().unwrap();
    let nodes =
        start_sharded_three_node_cluster(dir.path(), ShardedClusterOptions::default()).await;
    bootstrap_region_on_all_nodes(&nodes, "sharddb", "autogen", "cpu", vec![1, 2], 1).await;

    let client = reqwest::Client::new();
    create_db(&client, &nodes[0].url, "sharddb").await;
    for node in &nodes[1..] {
        wait_for_database(&client, &node.url, "sharddb").await;
    }

    for i in 0..5 {
        write_line(
            &client,
            &nodes[0].url,
            "sharddb",
            &format!("cpu,host=a value=1 {}", 1_000_000_000 + i),
        )
        .await;
    }
    flush_node(&nodes[0]).await;

    let resp = query_parsed(
        &client,
        &nodes[1].url,
        "sharddb",
        "SELECT count(value) FROM cpu",
    )
    .await;
    let count = resp.results[0]
        .series
        .as_ref()
        .and_then(|s| s.first())
        .and_then(|s| s.values.first())
        .and_then(|row| row.last())
        .and_then(|v| v.as_f64())
        .unwrap_or(0.0);
    assert_eq!(
        count, 5.0,
        "aggregate on lagging replica should read region primary"
    );
}

/// P1.1: a 1-member cluster with sharding on accepts /write and owns the
/// first region (peers=[self], primary=self) even when configured RF is 3.
#[tokio::test]
#[serial(chdb)]
async fn one_member_cluster_owns_region_after_first_write() {
    let dir = tempfile::tempdir().unwrap();
    let opts = ShardedClusterOptions {
        sharding: hyperbytedb::config::ShardingConfig {
            bootstrap_regions_per_node: 0,
            enabled: true,
            replication_factor: 3,
            scatter_peer_timeout_ms: 500,
            scatter_max_peer_attempts: 3,
            ..Default::default()
        },
        ..Default::default()
    };
    let node = start_sharded_single_node(dir.path(), opts).await;

    let client = reqwest::Client::new();
    create_db(&client, &node.url, "p1db").await;
    let resp = write_line(
        &client,
        &node.url,
        "p1db",
        "cpu,host=solo value=1 1000000000",
    )
    .await;
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::NO_CONTENT,
        "n=1 sharded write must succeed: {}",
        resp.status()
    );

    let map = node.shard_map.snapshot().await.unwrap();
    let space = map
        .space("p1db", "autogen", "cpu")
        .expect("first write must bootstrap a region");
    assert!(
        !space.regions.is_empty(),
        "shard map must have at least one region"
    );
    let region = &space.regions[0];
    assert_eq!(region.peers, vec![1], "n=1 peers must be [self]");
    assert_eq!(region.primary, 1, "n=1 primary must be self");
}

/// P1.2: after a joiner is Active, its map_version equals the cluster's
/// before any region data movement (peer-set change) starts.
#[tokio::test]
#[serial(chdb)]
async fn joiner_map_version_matches_before_region_movement() {
    let dir = tempfile::tempdir().unwrap();
    let opts = ShardedClusterOptions {
        sharding: hyperbytedb::config::ShardingConfig {
            bootstrap_regions_per_node: 0,
            enabled: true,
            replication_factor: 3,
            scatter_peer_timeout_ms: 500,
            scatter_max_peer_attempts: 3,
            ..Default::default()
        },
        ..Default::default()
    };
    let node1 = start_sharded_single_node(dir.path(), opts).await;
    let client = reqwest::Client::new();
    create_db(&client, &node1.url, "p1db").await;
    let resp = write_line(
        &client,
        &node1.url,
        "p1db",
        "cpu,host=solo value=1 1000000000",
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::NO_CONTENT);

    let cluster_before = node1.shard_map.snapshot().await.unwrap();
    assert!(
        cluster_before.map_version >= 1,
        "first write must bump map_version"
    );
    let region = cluster_before
        .space("p1db", "autogen", "cpu")
        .and_then(|s| s.regions.first())
        .expect("region after write");
    assert_eq!(region.peers, vec![1]);

    let l2 = bind_ephemeral().await;
    let a2 = l2.local_addr().unwrap().to_string();
    {
        let mut m = node1.membership.write().await;
        m.add_node(hyperbytedb::domain::cluster::membership::NodeInfo {
            node_id: 2,
            addr: a2,
            state: hyperbytedb::domain::cluster::membership::NodeState::Active,
            joined_at: 0,
            last_heartbeat: 0,
            needs_sync: false,
            consecutive_misses: 0,
        });
    }
    // libchdb is process-global; share node 1's session (pair-cluster harness).
    let node2 = start_sharded_node(
        dir.path(),
        2,
        l2,
        node1.membership.clone(),
        &ShardedClusterOptions {
            sharding: hyperbytedb::config::ShardingConfig {
                bootstrap_regions_per_node: 0,
                enabled: true,
                replication_factor: 3,
                scatter_peer_timeout_ms: 500,
                scatter_max_peer_attempts: 3,
                ..Default::default()
            },
            ..Default::default()
        },
        node1.chdb.clone(),
    )
    .await;

    let empty = node2.shard_map.snapshot().await.unwrap();
    assert_eq!(empty.map_version, 0, "joiner starts with an empty map");

    install_shard_map_from_peer(&node1, &node2).await;

    let after = node2.shard_map.snapshot().await.unwrap();
    assert_eq!(
        after.map_version, cluster_before.map_version,
        "joiner map_version must equal the cluster's before movement"
    );
    let joined_region = after
        .space("p1db", "autogen", "cpu")
        .and_then(|s| s.regions.first())
        .expect("installed region");
    assert_eq!(
        joined_region.peers,
        vec![1],
        "catch-up must not add the joiner as a peer"
    );
    assert!(
        hyperbytedb::application::shard_scheduler::joiner_map_caught_up(
            cluster_before.map_version,
            after.map_version
        )
    );
}

/// P1.3: after join, each pre-existing region has the joiner as a committed
/// peer, series rows for that range exist on the joiner, RF = min(config, 2).
#[tokio::test]
#[serial(chdb)]
async fn joiner_receives_existing_region_as_replica() {
    use hyperbytedb::application::shard_transfer::stage_region_transfer_data;
    use hyperbytedb::domain::sharding::placement::target_placement;
    use hyperbytedb::ports::metadata::MetadataPort;
    use hyperbytedb::ports::query::QueryPort;
    use hyperbytedb::ports::wal::WalPort;
    use std::sync::Arc;

    let dir = tempfile::tempdir().unwrap();
    let opts = ShardedClusterOptions {
        sharding: hyperbytedb::config::ShardingConfig {
            bootstrap_regions_per_node: 0,
            enabled: true,
            replication_factor: 3,
            scatter_peer_timeout_ms: 500,
            scatter_max_peer_attempts: 3,
            ..Default::default()
        },
        ..Default::default()
    };
    let node1 = start_sharded_single_node(dir.path(), opts).await;
    let client = reqwest::Client::new();
    create_db(&client, &node1.url, "p1db").await;
    let resp = write_line(
        &client,
        &node1.url,
        "p1db",
        "cpu,host=solo value=1 1000000000",
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::NO_CONTENT);
    flush_node(&node1).await;

    let before = node1.shard_map.snapshot().await.unwrap();
    let region = before
        .space("p1db", "autogen", "cpu")
        .and_then(|s| s.regions.first())
        .cloned()
        .expect("region after write");
    assert_eq!(region.peers, vec![1]);
    let expected_sid = series_id("cpu", &BTreeMap::from([("host".into(), "solo".into())]));
    assert!(
        region.contains(expected_sid),
        "written series must fall in the bootstrapped region"
    );

    let node2 = start_sharded_joiner(
        dir.path(),
        &node1,
        2,
        &ShardedClusterOptions {
            sharding: hyperbytedb::config::ShardingConfig {
                bootstrap_regions_per_node: 0,
                enabled: true,
                replication_factor: 3,
                scatter_peer_timeout_ms: 500,
                scatter_max_peer_attempts: 3,
                ..Default::default()
            },
            ..Default::default()
        },
    )
    .await;
    create_db(&client, &node2.url, "p1db").await;
    install_shard_map_from_peer(&node1, &node2).await;

    // The joiner must be somewhere the placement function actually wants this
    // region. With two live members and RF capped at 2, both are targeted.
    let active = [1u64, 2];
    let key = hyperbytedb::domain::sharding::types::MeasurementKey::new("p1db", "autogen", "cpu");
    let target = target_placement(&key, region.start, &active, 2);
    assert!(
        target.contains(&2),
        "live joiner must be in the region's target placement, got {target:?}"
    );

    let peer_client = Arc::new(
        hyperbytedb::adapters::cluster::peer_client::PeerClient::new(
            1,
            node1.addr.clone(),
            node1.membership.clone(),
            Arc::new(
                hyperbytedb::adapters::cluster::replication_log::ReplicationLog::open(
                    dir.path().join("repl-place"),
                )
                .unwrap(),
            ),
            2,
            8192,
            8,
            8 * 1024 * 1024,
            Default::default(),
        ),
    );
    let metadata: Arc<dyn MetadataPort> = node1.metadata.clone();
    let wal: Arc<dyn WalPort> = node1.wal.clone();
    let query_port: Arc<dyn QueryPort> = node1.query_port.clone();
    let key = MeasurementKey::new("p1db", "autogen", "cpu");
    let outcome = stage_region_transfer_data(
        &peer_client,
        &metadata,
        &wal,
        Some(&query_port),
        1,
        &key,
        &region,
        2,
        10_000,
        0,
    )
    .await
    .expect("stage onto joiner");
    assert!(
        outcome.verified(),
        "stage must confirm every exported point: exported={} applied={}",
        outcome.exported,
        outcome.applied
    );
    assert!(
        outcome.exported >= 1,
        "pre-existing measurement must copy at least one point"
    );

    apply_add_peer_on_nodes(&[&node1, &node2], "p1db", "autogen", "cpu", 2).await;

    for node in [&node1, &node2] {
        let map = node.shard_map.snapshot().await.unwrap();
        let placed = map
            .space("p1db", "autogen", "cpu")
            .and_then(|s| s.regions.first())
            .expect("region after AddPeer");
        assert!(
            placed.peers.contains(&2),
            "node {} map missing joiner peer: {:?}",
            node.node_id,
            placed.peers
        );
        assert_eq!(placed.primary, 1, "AddPeer must not move primary");
        assert_eq!(
            placed.peers.len(),
            2,
            "effective RF = min(configured=3, members=2)"
        );
    }

    let dest_series = node2
        .metadata
        .list_series_ids("p1db", "autogen", "cpu")
        .await
        .unwrap();
    assert!(
        dest_series.contains(&expected_sid),
        "joiner metadata must hold the series for the copied range: {dest_series:?}"
    );
}

/// Points written to `node`'s own WAL at or after `from_seq`.
///
/// A forwarded write leaves nothing here — the coordinator relays it to the
/// primary — so this is what separates "node 2 accepted the write" from
/// "node 2 proxied it".
async fn local_wal_points(
    node: &ShardedTestNode,
    from_seq: u64,
) -> Vec<hyperbytedb::domain::point::Point> {
    use hyperbytedb::ports::wal::WalPort;
    let wal: std::sync::Arc<dyn WalPort> = node.wal.clone();
    // Only writes this node applied as its own. A replica also receives the
    // primary's replicated copy (origin = the primary), which is correct and
    // is not a local apply; counting it made the replica-only check depend on
    // replication being slower than the assertion.
    let me = node.node_id;
    wal.read_from(from_seq)
        .await
        .expect("read wal")
        .into_iter()
        .filter(|(_, entry)| entry.origin_node_id == 0 || entry.origin_node_id == me)
        .flat_map(|(_, entry)| entry.points)
        .collect()
}

/// P1.4/P1.5: a joiner that is only a replica must not be credited as a pass —
/// the primary has to move onto it and writes have to apply locally there.
///
/// Replica-only state is asserted first (write to node 2 forwards, node 2's WAL
/// stays empty), then the primary is placed and the same write lands locally.
#[tokio::test]
#[serial(chdb)]
async fn joiner_takes_region_primary_and_applies_writes_locally() {
    use hyperbytedb::application::shard_transfer::run_region_transfer;
    use hyperbytedb::domain::sharding::placement::target_placement;
    use hyperbytedb::ports::metadata::MetadataPort;
    use hyperbytedb::ports::points_sink::PointsSinkPort;
    use hyperbytedb::ports::query::QueryPort;
    use hyperbytedb::ports::wal::WalPort;
    use std::sync::Arc;

    let dir = tempfile::tempdir().unwrap();
    let opts = || ShardedClusterOptions {
        sharding: hyperbytedb::config::ShardingConfig {
            bootstrap_regions_per_node: 0,
            enabled: true,
            replication_factor: 3,
            scatter_peer_timeout_ms: 500,
            scatter_max_peer_attempts: 3,
            ..Default::default()
        },
        ..Default::default()
    };
    let node1 = start_sharded_single_node(dir.path(), opts()).await;
    let client = reqwest::Client::new();
    create_db(&client, &node1.url, "p14db").await;
    let resp = write_line(
        &client,
        &node1.url,
        "p14db",
        "disk,host=solo value=1 1000000000",
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::NO_CONTENT);
    flush_node(&node1).await;

    let region = node1
        .shard_map
        .snapshot()
        .await
        .unwrap()
        .space("p14db", "autogen", "disk")
        .and_then(|s| s.regions.first())
        .cloned()
        .expect("region after write");
    assert_eq!(region.primary, 1);

    let node2 = start_sharded_joiner(dir.path(), &node1, 2, &opts()).await;
    create_db(&client, &node2.url, "p14db").await;
    install_shard_map_from_peer(&node1, &node2).await;

    // --- P1.3 midpoint: node 2 is a committed replica, primary is still 1. ---
    let peer_client = Arc::new(
        hyperbytedb::adapters::cluster::peer_client::PeerClient::new(
            1,
            node1.addr.clone(),
            node1.membership.clone(),
            Arc::new(
                hyperbytedb::adapters::cluster::replication_log::ReplicationLog::open(
                    dir.path().join("repl-p14"),
                )
                .unwrap(),
            ),
            2,
            8192,
            8,
            8 * 1024 * 1024,
            Default::default(),
        ),
    );
    let metadata: Arc<dyn MetadataPort> = node1.metadata.clone();
    let wal: Arc<dyn WalPort> = node1.wal.clone();
    let query_port: Arc<dyn QueryPort> = node1.query_port.clone();
    let sink: Arc<dyn PointsSinkPort> = node1.points_sink.clone();
    let key = MeasurementKey::new("p14db", "autogen", "disk");

    hyperbytedb::application::shard_transfer::stage_region_transfer_data(
        &peer_client,
        &metadata,
        &wal,
        Some(&query_port),
        1,
        &key,
        &region,
        2,
        10_000,
        0,
    )
    .await
    .expect("stage onto joiner");
    apply_add_peer_on_nodes(&[&node1, &node2], "p14db", "autogen", "disk", 2).await;

    // P1.5: replica-only is not a pass. A write aimed at node 2 forwards to the
    // primary and leaves node 2's own WAL untouched.
    let replica_seq = node2.wal.last_sequence().await.unwrap();
    let resp = write_line(
        &client,
        &node2.url,
        "p14db",
        "disk,host=solo value=2 1000000001",
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::NO_CONTENT);
    assert!(
        local_wal_points(&node2, replica_seq + 1).await.is_empty(),
        "replica-only joiner must forward the write, not apply it locally"
    );

    // --- P1.4: place the primary on the joiner. ---
    let staged = node1
        .shard_map
        .snapshot()
        .await
        .unwrap()
        .space("p14db", "autogen", "disk")
        .and_then(|s| s.regions.first())
        .cloned()
        .expect("region after AddPeer");
    // Under the placement function the primary goes wherever the hash says,
    // not to the joiner by virtue of being newest. This measurement's region
    // targets node 2 first, which is what makes it a P1.4 case at all.
    let key = hyperbytedb::domain::sharding::types::MeasurementKey::new("p14db", "autogen", "disk");
    let target = target_placement(&key, staged.start, &[1, 2], 2);
    assert_eq!(
        target.first().copied(),
        Some(2),
        "expected this region to target the joiner as primary, got {target:?}"
    );

    // Transfer-then-commit: rows move and verify before ownership changes.
    run_region_transfer(
        &peer_client,
        &metadata,
        &wal,
        Some(&query_port),
        Some(&sink),
        1,
        &key,
        &staged,
        2,
        10_000,
        false,
    )
    .await
    .expect("verified handoff to the joiner");
    let nodes = [node1, node2];
    promote_region_primary_on_all_nodes(&nodes, "p14db", "autogen", "disk", 2).await;

    for node in &nodes {
        let placed = node
            .shard_map
            .snapshot()
            .await
            .unwrap()
            .space("p14db", "autogen", "disk")
            .and_then(|s| s.regions.first())
            .cloned()
            .expect("region after TransferPrimary");
        assert_eq!(
            placed.primary, 2,
            "node {} map must show the joiner as primary",
            node.node_id
        );
        assert!(
            placed.peers.contains(&1),
            "the previous owner stays a replica: {:?}",
            placed.peers
        );
    }

    // P1.4: the write now applies on node 2 itself.
    let primary_seq = nodes[1].wal.last_sequence().await.unwrap();
    let resp = write_line(
        &client,
        &nodes[1].url,
        "p14db",
        "disk,host=solo value=3 1000000002",
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::NO_CONTENT);
    let landed = local_wal_points(&nodes[1], primary_seq + 1).await;
    assert!(
        landed.iter().any(|p| p.measurement == "disk"),
        "joiner is primary but the write did not reach its own WAL: {landed:?}"
    );
}

/// P1.6: the 1->2 case must not be a special case. A 4th process joining a
/// 3-node RF-complete cluster gets a replica slot (no region is under RF, so an
/// existing peer steps aside) and then a primary, exactly like the joiner in
/// `joiner_takes_region_primary_and_applies_writes_locally`.
#[tokio::test]
#[serial(chdb)]
async fn fourth_node_joining_rf_complete_cluster_takes_a_region() {
    use hyperbytedb::application::shard_transfer::{
        run_region_transfer, stage_region_transfer_data,
    };
    use hyperbytedb::domain::sharding::placement::{
        PlacementStep, next_placement_step, target_placement,
    };
    use hyperbytedb::ports::metadata::MetadataPort;
    use hyperbytedb::ports::points_sink::PointsSinkPort;
    use hyperbytedb::ports::query::QueryPort;
    use hyperbytedb::ports::wal::WalPort;
    use std::sync::Arc;

    let dir = tempfile::tempdir().unwrap();
    let opts = || ShardedClusterOptions {
        sharding: hyperbytedb::config::ShardingConfig {
            bootstrap_regions_per_node: 0,
            enabled: true,
            replication_factor: 3,
            scatter_peer_timeout_ms: 500,
            scatter_max_peer_attempts: 3,
            ..Default::default()
        },
        ..Default::default()
    };
    let nodes = start_sharded_three_node_cluster(dir.path(), opts()).await;
    bootstrap_region_on_all_nodes(&nodes, "p16db", "autogen", "disk", vec![1, 2, 3], 1).await;

    let client = reqwest::Client::new();
    for node in &nodes {
        create_db(&client, &node.url, "p16db").await;
    }
    let resp = write_line(
        &client,
        &nodes[0].url,
        "p16db",
        "disk,host=rfc value=7 1000000000",
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::NO_CONTENT);
    flush_node(&nodes[0]).await;

    let region = nodes[0]
        .shard_map
        .snapshot()
        .await
        .unwrap()
        .space("p16db", "autogen", "disk")
        .and_then(|s| s.regions.first())
        .cloned()
        .expect("region after write");
    assert_eq!(region.peers.len(), 3, "cluster starts RF-complete");

    let node4 = start_sharded_joiner(dir.path(), &nodes[0], 4, &opts()).await;
    create_db(&client, &node4.url, "p16db").await;
    install_shard_map_from_peer(&nodes[0], &node4).await;

    // No region is under RF, so replica *addition* correctly declines; the
    // newcomer has to displace a replica instead.
    // Under the placement function a fourth node needs no special rule: it
    // simply enters the targets its hash wins. This region targets node 4, so
    // convergence adds it and drops whichever incumbent is no longer wanted.
    // The old policy needed an idle-member displacement rule here precisely
    // because RF was already satisfied and AddPeer declined.
    let placement_key =
        hyperbytedb::domain::sharding::types::MeasurementKey::new("p16db", "autogen", "disk");
    let target = target_placement(&placement_key, region.start, &[1, 2, 3, 4], 3);
    assert!(
        target.contains(&4),
        "the newcomer must be wanted by this region, got {target:?}"
    );
    assert_eq!(
        next_placement_step(&region, &target),
        Some(PlacementStep::Add(4)),
        "convergence must add the newcomer before removing anyone"
    );
    let displaced = *region
        .peers
        .iter()
        .find(|p| !target.contains(p))
        .expect("some incumbent is no longer targeted");

    // The old rule refused to displace a primary at all. The new one may --
    // but never while it still holds the role: `next_placement_step` orders
    // Promote before Remove, so the region is never left without an owner.
    // Walk the steps and assert no Remove ever names the sitting primary.
    let mut walk = region.clone();
    for _ in 0..8 {
        match next_placement_step(&walk, &target) {
            Some(PlacementStep::Add(n)) => walk.peers.push(n),
            Some(PlacementStep::Promote(n)) => walk.primary = n,
            Some(PlacementStep::Remove(n)) => {
                assert_ne!(n, walk.primary, "a sitting primary was removed");
                walk.peers.retain(|p| *p != n);
            }
            None => break,
        }
    }
    assert_eq!(walk.primary, 4, "the newcomer ends up owning the region");

    let peer_client = Arc::new(
        hyperbytedb::adapters::cluster::peer_client::PeerClient::new(
            1,
            nodes[0].addr.clone(),
            nodes[0].membership.clone(),
            Arc::new(
                hyperbytedb::adapters::cluster::replication_log::ReplicationLog::open(
                    dir.path().join("repl-p16"),
                )
                .unwrap(),
            ),
            2,
            8192,
            8,
            8 * 1024 * 1024,
            Default::default(),
        ),
    );
    let metadata: Arc<dyn MetadataPort> = nodes[0].metadata.clone();
    let wal: Arc<dyn WalPort> = nodes[0].wal.clone();
    let query_port: Arc<dyn QueryPort> = nodes[0].query_port.clone();
    let sink: Arc<dyn PointsSinkPort> = nodes[0].points_sink.clone();
    let key = MeasurementKey::new("p16db", "autogen", "disk");

    // P1.3 shape: stage before the map commits the newcomer as a peer.
    let staged = stage_region_transfer_data(
        &peer_client,
        &metadata,
        &wal,
        Some(&query_port),
        1,
        &key,
        &region,
        4,
        10_000,
        0,
    )
    .await
    .expect("stage onto the newcomer");
    assert!(
        staged.verified() && staged.exported >= 1,
        "pre-existing rows must reach the newcomer: exported={} applied={}",
        staged.exported,
        staged.applied
    );

    let all = [&nodes[0], &nodes[1], &nodes[2], &node4];
    apply_move_peer_on_nodes(&all, "p16db", "autogen", "disk", displaced, 4).await;

    let swapped = nodes[0]
        .shard_map
        .snapshot()
        .await
        .unwrap()
        .space("p16db", "autogen", "disk")
        .and_then(|s| s.regions.first())
        .cloned()
        .expect("region after MovePeer");
    assert!(swapped.peers.contains(&4), "newcomer is a committed peer");
    assert!(
        !swapped.peers.contains(&displaced),
        "displaced peer released its slot"
    );
    assert_eq!(swapped.peers.len(), 3, "RF is unchanged by the swap");

    // P1.4 shape: MovePeer appended the newcomer, so it is now the newest peer
    // and the primary-placement rule hands it the region.
    assert_eq!(
        target.first().copied(),
        Some(4),
        "this region targets the newcomer as primary, got {target:?}"
    );

    run_region_transfer(
        &peer_client,
        &metadata,
        &wal,
        Some(&query_port),
        Some(&sink),
        1,
        &key,
        &swapped,
        4,
        10_000,
        false,
    )
    .await
    .expect("verified handoff to the newcomer");

    promote_region_primary_on_nodes(&all, "p16db", "autogen", "disk", 4).await;

    for node in all {
        let placed = node
            .shard_map
            .snapshot()
            .await
            .unwrap()
            .space("p16db", "autogen", "disk")
            .and_then(|s| s.regions.first())
            .cloned()
            .expect("region after TransferPrimary");
        assert_eq!(
            placed.primary, 4,
            "node {} must see the newcomer as primary",
            node.node_id
        );
    }

    let seq = node4.wal.last_sequence().await.unwrap();
    let resp = write_line(
        &client,
        &node4.url,
        "p16db",
        "disk,host=rfc value=8 1000000001",
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::NO_CONTENT);
    let landed = local_wal_points(&node4, seq + 1).await;
    assert!(
        landed.iter().any(|p| p.measurement == "disk"),
        "newcomer is primary but the write did not reach its own WAL: {landed:?}"
    );
}

async fn wait_for_database(client: &reqwest::Client, url: &str, db: &str) {
    for _ in 0..100 {
        let resp = query_sql(client, url, db, "SHOW DATABASES").await;
        if resp.status().is_success() {
            let body = resp.text().await.unwrap_or_default();
            if body.contains(db) {
                return;
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    panic!("database {db} did not appear on {url}");
}

/// B.11: a write that cannot reach one region's primary must not discard the
/// buckets it *could* apply, and must report that it was partial.
///
/// The forward loop used to `try_collect`, so the first unreachable region
/// aborted the whole ingest — after other regions' forwards had already been
/// applied remotely, and before the local bucket was appended at all. The
/// client was told the write failed while rows were durably stored, which is
/// what the split-write suite counted as "24,538 duplicated rows": points
/// stored whose batches returned non-204.
///
/// Region A is primaried locally, region B by node 2, which is stopped. One
/// batch spans both.
#[tokio::test]
#[serial(chdb)]
async fn a_partial_multi_region_write_keeps_the_local_bucket_and_reports_partial() {
    let dir = tempfile::tempdir().unwrap();
    let mut nodes =
        start_sharded_three_node_cluster(dir.path(), ShardedClusterOptions::default()).await;
    bootstrap_two_regions(&nodes, "sharddb", "autogen", "cpu").await;

    let client = reqwest::Client::new();
    for node in &nodes {
        create_db(&client, &node.url, "sharddb").await;
    }

    let mid = u64::MAX / 2;
    let (host_low, host_high) = hosts_for_region_split("cpu", mid);

    // Region B's primary is gone, so host_high can never be forwarded.
    stop_node(&mut nodes[1]);
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    let body = format!(
        "cpu,host={host_low} value=11 1000000000\ncpu,host={host_high} value=22 2000000000"
    );
    let resp = write_line(&client, &nodes[0].url, "sharddb", &body).await;
    assert!(
        !resp.status().is_success(),
        "a write that lost a region must not report success: {}",
        resp.status()
    );

    // The local half must still be there. Under the old abort-on-first-error
    // the local append never ran, so this row was silently thrown away because
    // a different region was unreachable.
    flush_node(&nodes[0]).await;
    let rows = chdb_row_count(nodes[0].query_port.as_ref(), "sharddb_autogen_cpu").await;
    assert!(
        rows >= 1,
        "the locally-owned region's point must survive another region's \
         failure; got {rows} rows"
    );
}

/// A forwarded batch: parts that pass their checks share one WAL entry on
/// the primary; each failing part still gets its own status; each good
/// part still replicates to its region's peers.
#[tokio::test]
#[serial(chdb)]
async fn forwarded_parts_share_one_wal_entry_and_fail_one_by_one() {
    use hyperbytedb::domain::sharding::{
        ShardWriteBatch, ShardWriteBatchResponse, ShardWriteRequest,
    };
    use hyperbytedb::ports::wal::WalPort;

    let dir = tempfile::tempdir().unwrap();
    let nodes = start_sharded_pair_cluster(dir.path(), ShardedClusterOptions::default()).await;
    let client = reqwest::Client::new();
    for node in &nodes {
        create_db(&client, &node.url, "sharddb").await;
    }
    let region = |id: u64, primary: u64| ShardRegion {
        region_id: id,
        start: 0,
        end: u64::MAX,
        epoch: ShardEpoch::default(),
        peers: vec![1, 2],
        primary,
        last_split_at: 0,
        transfer_verified: None,
        transfer_first_seen: None,
        state: Default::default(),
        staged_from: None,
        staging_peers: Vec::new(),
    };
    for (measurement, id, primary) in [("cpu", 1, 1), ("mem", 2, 1), ("disk", 3, 2)] {
        let op = ShardMapOp::BootstrapMeasurement {
            key: MeasurementKey::new("sharddb", "autogen", measurement),
            region: region(id, primary),
        };
        for node in &nodes {
            node.shard_map.apply_op(op.clone()).await.unwrap();
        }
    }

    let part = |region_id: u64, epoch: ShardEpoch, line: &str| ShardWriteRequest {
        db: "sharddb".into(),
        rp: "autogen".into(),
        precision: None,
        epoch,
        region_id,
        body: line.as_bytes().to_vec(),
    };
    let stale = ShardEpoch {
        conf_ver: 9,
        ..Default::default()
    };
    let batch = ShardWriteBatch {
        parts: vec![
            part(1, ShardEpoch::default(), "cpu,host=a value=1 1000000000"),
            part(2, ShardEpoch::default(), "mem,host=a used=2 1000000000"),
            part(1, stale, "cpu,host=b value=3 1000000000"),
            part(7, ShardEpoch::default(), "cpu,host=c value=4 1000000000"),
            part(3, ShardEpoch::default(), "disk,host=a free=5 1000000000"),
        ],
    };

    let primary_before = nodes[0].wal.last_sequence().await.unwrap();
    let replica_before = nodes[1].wal.last_sequence().await.unwrap();
    let resp = client
        .post(format!("{}/internal/shard/write-batch", nodes[0].url))
        .body(bincode::serialize(&batch).unwrap())
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());
    let out: ShardWriteBatchResponse = resp.json().await.unwrap();
    let statuses: Vec<(u64, u16)> = out
        .results
        .iter()
        .map(|r| (r.region_id, r.status))
        .collect();
    assert_eq!(
        statuses,
        vec![(1, 204), (2, 204), (1, 409), (7, 404), (3, 409)],
        "good parts applied; stale epoch, unknown region and not-primary refused"
    );

    let appended = nodes[0].wal.read_from(primary_before + 1).await.unwrap();
    let shape: Vec<(u64, String, Vec<String>)> = appended
        .iter()
        .map(|(s, e)| {
            (
                *s,
                e.database.clone(),
                e.points.iter().map(|p| p.measurement.clone()).collect(),
            )
        })
        .collect();
    assert_eq!(
        appended.len(),
        1,
        "the two good parts share one WAL entry: {shape:?}"
    );
    assert_eq!(appended[0].1.points.len(), 2);

    let mut replicated = 0usize;
    for _ in 0..200 {
        replicated = nodes[1]
            .wal
            .read_from(replica_before + 1)
            .await
            .unwrap()
            .iter()
            .map(|(_, e)| e.points.len())
            .sum();
        if replicated >= 2 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    assert_eq!(replicated, 2, "both good parts reached the region peer");
}

/// The coordinator's own regions land even when a forward fails, now that
/// the local append runs alongside the forwards instead of after them. The
/// write still reports the forward failure.
#[tokio::test]
#[serial(chdb)]
async fn the_local_share_lands_when_a_forward_fails() {
    use hyperbytedb::ports::wal::WalPort;

    let dir = tempfile::tempdir().unwrap();
    let [n1, mut n2] =
        start_sharded_pair_cluster(dir.path(), ShardedClusterOptions::default()).await;
    let client = reqwest::Client::new();
    for node in [&n1, &n2] {
        create_db(&client, &node.url, "sharddb").await;
    }
    let region = |id: u64, peers: Vec<u64>, primary: u64| ShardRegion {
        region_id: id,
        start: 0,
        end: u64::MAX,
        epoch: ShardEpoch::default(),
        peers,
        primary,
        last_split_at: 0,
        transfer_verified: None,
        transfer_first_seen: None,
        state: Default::default(),
        staged_from: None,
        staging_peers: Vec::new(),
    };
    // cpu is node 1's own region; mem lives only on node 2.
    for (measurement, r) in [
        ("cpu", region(1, vec![1], 1)),
        ("mem", region(2, vec![2], 2)),
    ] {
        let op = ShardMapOp::BootstrapMeasurement {
            key: MeasurementKey::new("sharddb", "autogen", measurement),
            region: r,
        };
        for node in [&n1, &n2] {
            node.shard_map.apply_op(op.clone()).await.unwrap();
            let snap = node.shard_map.snapshot().await.unwrap();
            node.location_cache.refresh_from_map(&snap);
        }
    }
    stop_node(&mut n2);

    let before = n1.wal.last_sequence().await.unwrap();
    let resp = write_line(
        &client,
        &n1.url,
        "sharddb",
        "cpu,host=a value=1 1000000000\nmem,host=a used=2 1000000000",
    )
    .await;
    assert!(
        !resp.status().is_success(),
        "a write whose forward failed must not report success"
    );
    let body = resp.text().await.unwrap();
    assert!(body.contains("partially applied"), "got: {body}");

    let landed: Vec<String> = n1
        .wal
        .read_from(before + 1)
        .await
        .unwrap()
        .iter()
        .flat_map(|(_, e)| e.points.iter().map(|p| p.measurement.clone()))
        .collect();
    assert_eq!(
        landed,
        vec!["cpu".to_string()],
        "the local region landed, alone"
    );
}

/// A region its primary refuses at capacity is resent on its own, and the
/// client write succeeds once it lands. Before, the refusal failed the whole
/// write and the client resent every point, including those that had landed.
///
/// Node 2 is replaced by a mock that refuses the first forward batch with a
/// 503 and accepts every part after that.
#[tokio::test]
#[serial(chdb)]
async fn a_region_refused_at_capacity_is_retried_alone() {
    use hyperbytedb::domain::sharding::{
        ShardWriteBatch, ShardWriteBatchResponse, ShardWritePartResult,
    };
    use hyperbytedb::ports::wal::WalPort;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    let dir = tempfile::tempdir().unwrap();
    let [n1, mut n2] =
        start_sharded_pair_cluster(dir.path(), ShardedClusterOptions::default()).await;
    let client = reqwest::Client::new();
    for node in [&n1, &n2] {
        create_db(&client, &node.url, "sharddb").await;
    }
    let region = |id: u64, peers: Vec<u64>, primary: u64| ShardRegion {
        region_id: id,
        start: 0,
        end: u64::MAX,
        epoch: ShardEpoch::default(),
        peers,
        primary,
        last_split_at: 0,
        transfer_verified: None,
        transfer_first_seen: None,
        state: Default::default(),
        staged_from: None,
        staging_peers: Vec::new(),
    };
    // cpu is node 1's own region; mem lives only on node 2.
    for (measurement, r) in [
        ("cpu", region(1, vec![1], 1)),
        ("mem", region(2, vec![2], 2)),
    ] {
        let op = ShardMapOp::BootstrapMeasurement {
            key: MeasurementKey::new("sharddb", "autogen", measurement),
            region: r,
        };
        for node in [&n1, &n2] {
            node.shard_map.apply_op(op.clone()).await.unwrap();
            let snap = node.shard_map.snapshot().await.unwrap();
            node.location_cache.refresh_from_map(&snap);
        }
    }
    stop_node(&mut n2);

    let requests = Arc::new(AtomicUsize::new(0));
    let seen = requests.clone();
    let app = axum::Router::new().route(
        "/internal/shard/write-batch",
        axum::routing::post(move |body: axum::body::Bytes| {
            let seen = seen.clone();
            async move {
                use axum::response::IntoResponse;
                if seen.fetch_add(1, Ordering::SeqCst) == 0 {
                    return axum::http::StatusCode::SERVICE_UNAVAILABLE.into_response();
                }
                let batch: ShardWriteBatch = bincode::deserialize(&body).unwrap();
                axum::Json(ShardWriteBatchResponse {
                    results: batch
                        .parts
                        .iter()
                        .map(|p| ShardWritePartResult {
                            region_id: p.region_id,
                            status: 204,
                            error: None,
                        })
                        .collect(),
                })
                .into_response()
            }
        }),
    );
    let mut listener = None;
    for _ in 0..100 {
        match tokio::net::TcpListener::bind(&n2.addr).await {
            Ok(l) => {
                listener = Some(l);
                break;
            }
            Err(_) => tokio::time::sleep(std::time::Duration::from_millis(20)).await,
        }
    }
    let listener = listener.expect("node 2's address is free again");
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    let before = n1.wal.last_sequence().await.unwrap();
    let resp = write_line(
        &client,
        &n1.url,
        "sharddb",
        "cpu,host=a value=1 1000000000\nmem,host=a used=2 1000000000",
    )
    .await;
    let status = resp.status();
    assert!(
        status.is_success(),
        "a region refused once at capacity is retried and the write succeeds: {status} {}",
        resp.text().await.unwrap_or_default()
    );
    assert_eq!(
        requests.load(Ordering::SeqCst),
        2,
        "node 2 saw the refused batch and one retry"
    );

    let landed: Vec<String> = n1
        .wal
        .read_from(before + 1)
        .await
        .unwrap()
        .iter()
        .flat_map(|(_, e)| e.points.iter().map(|p| p.measurement.clone()))
        .collect();
    assert_eq!(
        landed,
        vec!["cpu".to_string()],
        "the local region landed once; the retry resent only the refused region"
    );
}

/// SHOW FIELD KEYS on a node that holds no copy of the measurement still
/// lists its fields. Fields are recorded only where rows are stored, and the
/// statement used to read local metadata alone, so it came back empty.
#[tokio::test]
#[serial(chdb)]
async fn a_node_without_a_copy_answers_metadata_and_time_queries() {
    let dir = tempfile::tempdir().unwrap();
    let [n1, n2] = start_sharded_pair_cluster(dir.path(), ShardedClusterOptions::default()).await;
    let client = reqwest::Client::new();
    for node in [&n1, &n2] {
        create_db(&client, &node.url, "sharddb").await;
    }
    let op = ShardMapOp::BootstrapMeasurement {
        key: MeasurementKey::new("sharddb", "autogen", "mem"),
        region: ShardRegion {
            region_id: 2,
            start: 0,
            end: u64::MAX,
            epoch: ShardEpoch::default(),
            peers: vec![2],
            primary: 2,
            last_split_at: 0,
            transfer_verified: None,
            transfer_first_seen: None,
            state: Default::default(),
            staged_from: None,
            staging_peers: Vec::new(),
        },
    };
    for node in [&n1, &n2] {
        node.shard_map.apply_op(op.clone()).await.unwrap();
        let snap = node.shard_map.snapshot().await.unwrap();
        node.location_cache.refresh_from_map(&snap);
    }
    let resp = write_line(
        &client,
        &n2.url,
        "sharddb",
        "mem,host=a used=2,free=3i 1000000000",
    )
    .await;
    assert!(resp.status().is_success(), "write: {}", resp.status());

    flush_node(&n2).await;

    // A time-bounded query through the node without a copy. With no local
    // metadata it infers columns from the statement, and used to take
    // `time` for a tag.
    for node in [&n1, &n2] {
        let resp = query_parsed(
            &client,
            &node.url,
            "sharddb",
            "SELECT count(used) FROM mem WHERE time >= 0 AND time < 120s GROUP BY time(1m)",
        )
        .await;
        let result = &resp.results[0];
        assert!(result.error.is_none(), "{}: {:?}", node.url, result.error);
        let counts: Vec<serde_json::Value> = result.series.as_ref().expect("series")[0]
            .values
            .iter()
            .map(|r| r[1].clone())
            .collect();
        assert_eq!(
            counts,
            [serde_json::json!(1), serde_json::json!(0)],
            "{}",
            node.url
        );
    }

    for node in [&n1, &n2] {
        let resp = query_parsed(&client, &node.url, "sharddb", "SHOW FIELD KEYS FROM mem").await;
        let values = &resp.results[0].series.as_ref().expect("series")[0].values;
        assert_eq!(
            values,
            &vec![
                vec![serde_json::json!("free"), serde_json::json!("integer")],
                vec![serde_json::json!("used"), serde_json::json!("float")],
            ],
            "{}",
            node.url
        );
    }
}

/// A write for a database that no longer exists must not leave series
/// metadata behind. After DROP DATABASE, forwards, replication and catch-up
/// still in flight kept registering the dropped database's series: on n3,
/// nodes with no databases warmed 35-38 million series at startup.
#[tokio::test]
#[serial(chdb)]
async fn a_late_write_for_a_dropped_database_leaves_no_series() {
    use hyperbytedb::domain::sharding::{ShardWriteBatch, ShardWriteRequest};
    use hyperbytedb::ports::metadata::MetadataPort;

    let dir = tempfile::tempdir().unwrap();
    let [n1, n2] = start_sharded_pair_cluster(dir.path(), ShardedClusterOptions::default()).await;
    let client = reqwest::Client::new();
    for node in [&n1, &n2] {
        create_db(&client, &node.url, "latedb").await;
    }
    let op = ShardMapOp::BootstrapMeasurement {
        key: MeasurementKey::new("latedb", "autogen", "cpu"),
        region: ShardRegion {
            region_id: 1,
            start: 0,
            end: u64::MAX,
            epoch: ShardEpoch::default(),
            peers: vec![1],
            primary: 1,
            last_split_at: 0,
            transfer_verified: None,
            transfer_first_seen: None,
            state: Default::default(),
            staged_from: None,
            staging_peers: Vec::new(),
        },
    };
    for node in [&n1, &n2] {
        node.shard_map.apply_op(op.clone()).await.unwrap();
    }
    for node in [&n1, &n2] {
        let resp = query_sql(&client, &node.url, "latedb", "DROP DATABASE latedb").await;
        assert!(resp.status().is_success());
    }
    assert_eq!(n1.metadata.warm_series().await.unwrap(), 0);

    // A forward that was in flight when the database went away.
    let batch = ShardWriteBatch {
        parts: vec![ShardWriteRequest {
            db: "latedb".into(),
            rp: "autogen".into(),
            precision: None,
            epoch: ShardEpoch::default(),
            region_id: 1,
            body: b"cpu,host=a value=1 1000000000\ncpu,host=b value=2 1000000000".to_vec(),
        }],
    };
    let _ = client
        .post(format!("{}/internal/shard/write-batch", n1.url))
        .body(bincode::serialize(&batch).unwrap())
        .send()
        .await
        .unwrap();
    assert_eq!(
        n1.metadata.warm_series().await.unwrap(),
        0,
        "a late write re-registered the dropped database's series"
    );
}

/// Pair cluster with the forward queue on: `cpu` lives on node 1, `mem` on
/// node 2 (`mem_peers` decides who else holds it).
async fn queued_pair(
    dir: &std::path::Path,
    mem_peers: Vec<u64>,
    max_lag_ms: u64,
) -> [ShardedTestNode; 2] {
    let mut opts = ShardedClusterOptions {
        forward_queue: true,
        ..Default::default()
    };
    opts.sharding.async_forward = true;
    opts.sharding.async_forward_max_lag_ms = max_lag_ms;
    let nodes = start_sharded_pair_cluster(dir, opts).await;
    let client = reqwest::Client::new();
    for node in &nodes {
        create_db(&client, &node.url, "sharddb").await;
    }
    let region = |id: u64, peers: Vec<u64>, primary: u64| ShardRegion {
        region_id: id,
        start: 0,
        end: u64::MAX,
        epoch: ShardEpoch::default(),
        peers,
        primary,
        last_split_at: 0,
        transfer_verified: None,
        transfer_first_seen: None,
        state: Default::default(),
        staged_from: None,
        staging_peers: Vec::new(),
    };
    for (measurement, r) in [
        ("cpu", region(1, vec![1], 1)),
        ("mem", region(2, mem_peers, 2)),
    ] {
        let op = ShardMapOp::BootstrapMeasurement {
            key: MeasurementKey::new("sharddb", "autogen", measurement),
            region: r,
        };
        for node in &nodes {
            node.shard_map.apply_op(op.clone()).await.unwrap();
            let snap = node.shard_map.snapshot().await.unwrap();
            node.location_cache.refresh_from_map(&snap);
        }
    }
    nodes
}

async fn measurements_in_wal_after(node: &ShardedTestNode, after: u64) -> Vec<String> {
    use hyperbytedb::ports::wal::WalPort;
    node.wal
        .read_from(after + 1)
        .await
        .unwrap()
        .iter()
        .flat_map(|(_, e)| e.points.iter().map(|p| p.measurement.clone()))
        .collect()
}

/// A write is acknowledged once its forwarded share is queued, and the
/// queue delivers it to the primary well inside the lag budget.
#[tokio::test]
#[serial(chdb)]
async fn a_queued_forward_is_acknowledged_and_reaches_its_primary() {
    use hyperbytedb::ports::wal::WalPort;

    let dir = tempfile::tempdir().unwrap();
    let [n1, n2] = queued_pair(dir.path(), vec![2], 1_000).await;
    let client = reqwest::Client::new();
    let before = n2.wal.last_sequence().await.unwrap();

    let resp = write_line(
        &client,
        &n1.url,
        "sharddb",
        "cpu,host=a value=1 1000000000\nmem,host=a used=2 1000000000",
    )
    .await;
    assert_eq!(resp.status(), 204);

    let queue = n1.forward_queue.as_ref().unwrap();
    let mut landed = Vec::new();
    for _ in 0..100 {
        landed = measurements_in_wal_after(&n2, before).await;
        if !landed.is_empty() && queue.is_empty() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert_eq!(
        landed,
        vec!["mem".to_string()],
        "the primary got the forwarded point"
    );
    assert!(queue.is_empty(), "delivered entries leave the queue");
}

/// With the primary down the write is still acknowledged and stays queued;
/// once the queue is older than the lag limit, new writes forward
/// synchronously again -- and, the primary being down, are held in the
/// queue too rather than failed (they used to report the failure, 500).
#[tokio::test]
#[serial(chdb)]
async fn a_lagging_queue_sends_new_writes_back_to_synchronous_forwarding() {
    let dir = tempfile::tempdir().unwrap();
    let [n1, mut n2] = queued_pair(dir.path(), vec![2], 200).await;
    stop_node(&mut n2);
    let client = reqwest::Client::new();

    let first = write_line(&client, &n1.url, "sharddb", "mem,host=a used=1 1000000000").await;
    assert_eq!(first.status(), 204, "queued while the primary is down");
    let queue = n1.forward_queue.as_ref().unwrap();
    assert_eq!(queue.len(), 1);

    tokio::time::sleep(std::time::Duration::from_millis(400)).await;
    let second = write_line(&client, &n1.url, "sharddb", "mem,host=b used=2 1000000000").await;
    assert_eq!(
        second.status(),
        204,
        "past the lag limit a write forwards synchronously, fails, and is held"
    );
    assert_eq!(queue.len(), 2, "both writes are owed, none dropped");
}

/// A queued forward is routed against the shard map at delivery: when its
/// region's primary has moved to the coordinator, it lands locally.
#[tokio::test]
#[serial(chdb)]
async fn a_queued_forward_follows_its_region_to_a_new_primary() {
    use hyperbytedb::ports::wal::WalPort;

    let dir = tempfile::tempdir().unwrap();
    let [n1, mut n2] = queued_pair(dir.path(), vec![1, 2], 5_000).await;
    stop_node(&mut n2);
    let client = reqwest::Client::new();
    let before = n1.wal.last_sequence().await.unwrap();

    let resp = write_line(&client, &n1.url, "sharddb", "mem,host=a used=1 1000000000").await;
    assert_eq!(resp.status(), 204);
    promote_region_primary_on_nodes(&[&n1], "sharddb", "autogen", "mem", 1).await;

    let queue = n1.forward_queue.as_ref().unwrap();
    let mut landed = Vec::new();
    for _ in 0..200 {
        landed = measurements_in_wal_after(&n1, before).await;
        if !landed.is_empty() && queue.is_empty() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    assert_eq!(landed, vec!["mem".to_string()]);
    assert!(queue.is_empty());
}

/// A write whose region's primary is down (restarting) is held in the
/// coordinator's forward queue and acknowledged, not failed with 500 "write
/// partially applied" -- which during a rolling restart was about 60% of
/// writes for 6 minutes. Synchronous forwarding (the default) is on here.
#[tokio::test]
#[serial(chdb)]
async fn a_write_for_a_down_primary_is_held_not_failed() {
    let dir = tempfile::tempdir().unwrap();
    let opts = ShardedClusterOptions {
        forward_queue: true,
        ..Default::default()
    };
    assert!(
        !opts.sharding.async_forward,
        "the default: forwards are synchronous"
    );
    let [n1, mut n2] = start_sharded_pair_cluster(dir.path(), opts).await;
    let client = reqwest::Client::new();
    for node in [&n1, &n2] {
        create_db(&client, &node.url, "hdb").await;
    }
    // Only node 2 holds the measurement, so node 1 forwards every point.
    let op = ShardMapOp::BootstrapMeasurement {
        key: MeasurementKey::new("hdb", "autogen", "mem"),
        region: ShardRegion {
            region_id: 2,
            start: 0,
            end: u64::MAX,
            epoch: ShardEpoch::default(),
            peers: vec![2],
            primary: 2,
            last_split_at: 0,
            transfer_verified: None,
            transfer_first_seen: None,
            state: Default::default(),
            staged_from: None,
            staging_peers: Vec::new(),
        },
    };
    for node in [&n1, &n2] {
        node.shard_map.apply_op(op.clone()).await.unwrap();
        let snap = node.shard_map.snapshot().await.unwrap();
        node.location_cache.refresh_from_map(&snap);
    }
    stop_node(&mut n2);
    let resp = write_line(&client, &n1.url, "hdb", "mem,host=a used=1 1000000000").await;
    let status = resp.status();
    let body = resp.text().await.unwrap_or_default();
    assert_eq!(status, 204, "{body}");
    let queue = n1.forward_queue.as_ref().unwrap();
    assert_eq!(queue.len(), 1, "held for the primary until it is back");
}

/// Two nodes; only node 2 holds `qdb.mem`, so node 1 knows no field types
/// and only the primary can find a conflict.
async fn coordinator_without_types(dir: &std::path::Path, queue: bool) -> [ShardedTestNode; 2] {
    let mut opts = ShardedClusterOptions {
        forward_queue: queue,
        ..Default::default()
    };
    opts.sharding.async_forward = queue;
    let nodes = start_sharded_pair_cluster(dir, opts).await;
    let client = reqwest::Client::new();
    for node in &nodes {
        create_db(&client, &node.url, "qdb").await;
    }
    let op = ShardMapOp::BootstrapMeasurement {
        key: MeasurementKey::new("qdb", "autogen", "mem"),
        region: ShardRegion {
            region_id: 2,
            start: 0,
            end: u64::MAX,
            epoch: ShardEpoch::default(),
            peers: vec![2],
            primary: 2,
            last_split_at: 0,
            transfer_verified: None,
            transfer_first_seen: None,
            state: Default::default(),
            staged_from: None,
            staging_peers: Vec::new(),
        },
    };
    for node in &nodes {
        node.shard_map.apply_op(op.clone()).await.unwrap();
        let snap = node.shard_map.snapshot().await.unwrap();
        node.location_cache.refresh_from_map(&snap);
    }
    nodes
}

/// A conflict only the primary can see reaches the client as the primary's
/// partial write (400), not as a 500.
#[tokio::test]
#[serial(chdb)]
async fn a_conflict_the_primary_finds_is_a_partial_write() {
    let dir = tempfile::tempdir().unwrap();
    let [n1, n2] = coordinator_without_types(dir.path(), false).await;
    let client = reqwest::Client::new();
    let resp = write_line(&client, &n1.url, "qdb", "mem,host=a used=1 1000000000").await;
    assert_eq!(resp.status(), 204);
    let resp = write_line(&client, &n1.url, "qdb", "mem,host=a used=\"x\" 2000000000").await;
    let status = resp.status();
    let body = resp.text().await.unwrap_or_default();
    assert_eq!(status, reqwest::StatusCode::BAD_REQUEST, "{body}");
    assert!(
        body.contains("partial write") && body.contains("dropped=1"),
        "{body}"
    );

    // Mixed, and consistent within itself, so node 1 (no stored types) can
    // drop nothing: the primary drops only the conflicting point and stores
    // the rest; the client gets the primary's message, not its JSON.
    let mixed = "mem,host=a free=2 3000000000\n\
                 mem,host=b used=\"x\" 4000000000\n\
                 mem,host=c free=3 5000000000";
    // Flushed apart from the first write: one flush of both used to stack
    // `free` under `used` (the prepared-batch coalesce bug, fixed apart).
    flush_node(&n2).await;
    let resp = write_line(&client, &n1.url, "qdb", mixed).await;
    let status = resp.status();
    let body = resp.text().await.unwrap_or_default();
    assert_eq!(status, reqwest::StatusCode::BAD_REQUEST, "{body}");
    let doc: serde_json::Value = serde_json::from_str(&body).unwrap();
    let error = doc["error"].as_str().unwrap();
    assert!(
        error.starts_with("partial write: field type conflict: input field \"used\"")
            && error.ends_with(" dropped=1"),
        "{error}"
    );
    flush_node(&n2).await;
    let resp = query_sql(&client, &n2.url, "qdb", "SELECT count(free) FROM mem").await;
    let doc: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(doc["results"][0]["series"][0]["values"][0][1], 2, "{doc}");
}

/// A queued forward its primary refuses (a field type conflict the
/// coordinator could not see) is dropped, not retried: 100 attempts kept it
/// in the queue for over a minute, and its age pushed every other write back
/// to synchronous forwarding.
#[tokio::test]
#[serial(chdb)]
async fn a_queued_forward_the_primary_refuses_is_not_retried() {
    let dir = tempfile::tempdir().unwrap();
    let [n1, _n2] = coordinator_without_types(dir.path(), true).await;
    let client = reqwest::Client::new();
    let queue = n1.forward_queue.as_ref().unwrap();
    let drained = || async {
        for _ in 0..60 {
            if queue.len() == 0 {
                return true;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        false
    };
    let resp = write_line(&client, &n1.url, "qdb", "mem,host=a used=1 1000000000").await;
    assert_eq!(resp.status(), 204);
    assert!(drained().await, "the first write is delivered");

    // Acknowledged when queued; node 2 refuses it on delivery.
    let resp = write_line(&client, &n1.url, "qdb", "mem,host=a used=\"x\" 2000000000").await;
    assert_eq!(resp.status(), 204);
    assert!(
        drained().await,
        "a refused forward leaves the queue within 3 s"
    );
}

/// A decommissioning node that has evacuated its regions must not retire
/// (report Leaving, which lets the operator delete it) while it still owes
/// queued forwards.
#[tokio::test]
#[serial(chdb)]
async fn a_departing_node_waits_for_its_forward_queue_before_retiring() {
    use hyperbytedb::domain::cluster::membership::NodeState;

    let dir = tempfile::tempdir().unwrap();
    let mut opts = ShardedClusterOptions {
        forward_queue: true,
        ..Default::default()
    };
    opts.sharding.async_forward = true;
    let [n1, mut n2] = start_sharded_pair_cluster(dir.path(), opts).await;
    let client = reqwest::Client::new();
    for node in [&n1, &n2] {
        create_db(&client, &node.url, "sharddb").await;
    }
    // Only node 2 holds data; node 1 is a pure coordinator.
    let op = ShardMapOp::BootstrapMeasurement {
        key: MeasurementKey::new("sharddb", "autogen", "mem"),
        region: ShardRegion {
            region_id: 2,
            start: 0,
            end: u64::MAX,
            epoch: ShardEpoch::default(),
            peers: vec![2],
            primary: 2,
            last_split_at: 0,
            transfer_verified: None,
            transfer_first_seen: None,
            state: Default::default(),
            staged_from: None,
            staging_peers: Vec::new(),
        },
    };
    for node in [&n1, &n2] {
        node.shard_map.apply_op(op.clone()).await.unwrap();
        let snap = node.shard_map.snapshot().await.unwrap();
        node.location_cache.refresh_from_map(&snap);
    }
    stop_node(&mut n2);
    let resp = write_line(&client, &n1.url, "sharddb", "mem,host=a used=1 1000000000").await;
    assert_eq!(resp.status(), 204);
    let queue = n1.forward_queue.as_ref().unwrap();
    assert_eq!(queue.len(), 1);

    set_node_state(&n1.membership, 1, NodeState::Decommissioning).await;
    let health = |url: String| {
        let client = client.clone();
        async move {
            let body: serde_json::Value = client
                .get(format!("{url}/health"))
                .send()
                .await
                .unwrap()
                .json()
                .await
                .unwrap();
            body["state"].as_str().unwrap_or_default().to_lowercase()
        }
    };
    assert_eq!(
        health(n1.url.clone()).await,
        "decommissioning",
        "still owes a forward"
    );

    // Once the queue is empty the node retires.
    // The drainer may hold the entry for a moment; keep taking until empty.
    for _ in 0..200 {
        let owed: Vec<u64> = queue
            .claim(u64::MAX, 100, u64::MAX)
            .await
            .unwrap()
            .into_iter()
            .map(|(s, _)| s)
            .collect();
        queue.complete(owed, None).await.unwrap();
        if queue.is_empty() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert!(queue.is_empty());
    assert_eq!(health(n1.url.clone()).await, "leaving");
}

/// With numeric `epoch` the coordinator sorts and fills `fill(null)`
/// buckets and regions skip ORDER BY/WITH FILL; RFC 3339 times keep the old
/// region fill. Both must give the same answer, buckets at the range edges
/// and gaps included.
#[tokio::test]
#[serial(chdb)]
async fn coordinator_fill_matches_region_fill() {
    let dir = tempfile::tempdir().unwrap();
    let nodes =
        start_sharded_three_node_cluster(dir.path(), ShardedClusterOptions::default()).await;
    let client = reqwest::Client::new();
    create_db(&client, &nodes[0].url, "fdb").await;
    bootstrap_two_regions(&nodes, "fdb", "autogen", "cpu").await;
    let sides: std::collections::BTreeSet<bool> = (0..3)
        .flat_map(|h| (0..2).map(move |c| (h, c)))
        .map(|(h, c)| {
            series_id(
                "cpu",
                &BTreeMap::from([
                    ("host".into(), format!("h{h}")),
                    ("cpu".into(), format!("cpu{c}")),
                ]),
            ) < u64::MAX / 2
        })
        .collect();
    assert_eq!(sides.len(), 2, "series must span both regions");

    // 3 hosts x 2 cpus, 10 s points with gaps, across 2 regions.
    let base_s: i64 = 1_791_751_725;
    let mut lines = Vec::new();
    for h in 0..3 {
        for c in 0..2 {
            for i in 0..40i64 {
                if (i + h + c) % 7 == 0 || (h == 1 && (10..25).contains(&i)) {
                    continue;
                }
                let v = (h * 100 + c * 10) as f64 + i as f64 / 4.0;
                lines.push(format!(
                    "cpu,host=h{h},cpu=cpu{c} usage_idle={v} {}",
                    (base_s + i * 10) * 1_000_000_000
                ));
            }
        }
    }
    let resp = write_line(&client, &nodes[0].url, "fdb", &lines.join("\n")).await;
    let status = resp.status();
    assert!(
        status.is_success(),
        "{status}: {}",
        resp.text().await.unwrap()
    );
    for n in &nodes {
        flush_node(n).await;
    }

    let (lo_ms, hi_ms) = ((base_s - 95) * 1000, (base_s + 470) * 1000);
    let run = |q: String, epoch: bool| {
        let client = client.clone();
        let url = nodes[0].url.clone();
        async move {
            let mut params = vec![("db", "fdb".to_string()), ("q", q.clone())];
            if epoch {
                params.push(("epoch", "ms".to_string()));
            }
            let resp = client
                .get(format!("{url}/query"))
                .query(&params)
                .send()
                .await
                .unwrap();
            assert!(resp.status().is_success(), "{q}");
            let body: serde_json::Value = resp.json().await.unwrap();
            assert!(body["results"][0]["error"].is_null(), "{q}: {body}");
            // series name + tags -> rows with times in ms
            let mut out = BTreeMap::new();
            for s in body["results"][0]["series"]
                .as_array()
                .cloned()
                .unwrap_or_default()
            {
                let rows: Vec<(i64, serde_json::Value)> = s["values"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|r| {
                        let t = match &r[0] {
                            serde_json::Value::String(t) => chrono::DateTime::parse_from_rfc3339(t)
                                .unwrap()
                                .timestamp_millis(),
                            n => n.as_i64().unwrap(),
                        };
                        (t, r[1].clone())
                    })
                    .collect();
                out.insert(s["tags"].to_string(), rows);
            }
            out
        }
    };
    for (group, fill) in [
        ("\"host\", \"cpu\"", "fill(null)"),
        ("\"host\", \"cpu\"", ""),
        ("\"host\"", "fill(null)"),
        ("\"host\"", "fill(none)"),
    ] {
        let q = format!(
            "SELECT mean(\"usage_idle\") FROM \"cpu\" WHERE time >= {lo_ms}ms AND time <= {hi_ms}ms \
             GROUP BY time(30s), {group} {fill}"
        );
        let with_epoch = run(q.clone(), true).await;
        let rfc3339 = run(q.clone(), false).await;
        assert!(!with_epoch.is_empty(), "{q}");
        assert_eq!(with_epoch, rfc3339, "{q}");
        for rows in with_epoch.values() {
            let times: Vec<i64> = rows.iter().map(|r| r.0).collect();
            assert!(times.windows(2).all(|w| w[0] < w[1]), "{q}: order");
            if fill != "fill(none)" {
                // Every 30 s bucket from the one holding the lower bound to
                // the one holding the upper bound.
                let first = lo_ms.div_euclid(30_000) * 30_000;
                let last = hi_ms.div_euclid(30_000) * 30_000;
                assert_eq!(times.first(), Some(&first), "{q}");
                assert_eq!(times.last(), Some(&last), "{q}");
                assert_eq!(times.len() as i64, (last - first) / 30_000 + 1, "{q}");
            }
        }
    }
}

/// Aggregates keep answering while a region's primary restarts. A rolling
/// update drains each pod for ~2 minutes; aggregates went only to the
/// primary and failed with "no active peers" the whole time (Grafana on the
/// n3 UAT cluster). A draining primary still answers; a stopped one is
/// covered by a replica, here the coordinator's own copy. Writes still need
/// the primary.
#[tokio::test]
#[serial(chdb)]
async fn aggregates_survive_a_restarting_primary() {
    use hyperbytedb::domain::cluster::membership::NodeState;
    let dir = tempfile::tempdir().unwrap();
    let mut nodes =
        start_sharded_three_node_cluster(dir.path(), ShardedClusterOptions::default()).await;
    // The coordinator (node 3) holds the only other copy.
    bootstrap_region_on_all_nodes(&nodes, "rdb", "autogen", "cpu", vec![1, 3], 1).await;
    let client = reqwest::Client::new();
    for node in &nodes {
        create_db(&client, &node.url, "rdb").await;
    }
    let lines: Vec<String> = (0..50)
        .map(|i: i64| format!("cpu,host=h{} value={i} {}", i % 5, (i + 1) * 1_000_000_000))
        .collect();
    let resp = write_line(&client, &nodes[0].url, "rdb", &lines.join("\n")).await;
    assert!(resp.status().is_success(), "{}", resp.status());
    for node in &nodes {
        flush_node(node).await;
    }
    // Both copies hold the rows before the primary goes away.
    for node in [&nodes[0], &nodes[2]] {
        let mut rows = 0;
        for _ in 0..50 {
            rows = chdb_row_count(node.query_port.as_ref(), "rdb_autogen_cpu").await;
            if rows >= 50 {
                break;
            }
            flush_node(node).await;
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        assert_eq!(rows, 50, "node {} copy", node.node_id);
    }

    let count = |url: String| {
        let client = client.clone();
        async move {
            let resp = query_sql(&client, &url, "rdb", "SELECT count(value) FROM cpu").await;
            let status = resp.status();
            let body: serde_json::Value = resp.json().await.unwrap();
            assert!(status.is_success(), "{status}: {body}");
            assert!(body["results"][0]["error"].is_null(), "{body}");
            body["results"][0]["series"][0]["values"][0][1].clone()
        }
    };
    let coordinator = nodes[2].url.clone();
    assert_eq!(count(coordinator.clone()).await, serde_json::json!(50));

    // The primary drains: still up, still answers.
    set_node_state(&nodes[2].membership, 1, NodeState::Draining).await;
    assert_eq!(
        count(coordinator.clone()).await,
        serde_json::json!(50),
        "draining primary"
    );

    // The primary is gone: a replica answers.
    stop_node(&mut nodes[0]);
    assert_eq!(
        count(coordinator.clone()).await,
        serde_json::json!(50),
        "stopped primary"
    );
    set_node_state(&nodes[2].membership, 1, NodeState::Disconnected).await;
    assert_eq!(
        count(coordinator.clone()).await,
        serde_json::json!(50),
        "disconnected primary"
    );

    let resp = write_line(
        &client,
        &coordinator,
        "rdb",
        "cpu,host=h0 value=1 99000000000",
    )
    .await;
    assert!(
        !resp.status().is_success(),
        "a write must not land on a replica: {}",
        resp.status()
    );
}

/// SHOW TAG VALUES ... WHERE across regions on different nodes: the series
/// of the other region come from its owner.
#[tokio::test]
#[serial(chdb)]
async fn show_tag_values_where_spans_regions() {
    let dir = tempfile::tempdir().unwrap();
    let nodes =
        start_sharded_three_node_cluster(dir.path(), ShardedClusterOptions::default()).await;
    let client = reqwest::Client::new();
    create_db(&client, &nodes[0].url, "tdb").await;
    bootstrap_two_regions(&nodes, "tdb", "autogen", "cpu").await;
    let mut lines = Vec::new();
    let mut want_h0 = std::collections::BTreeSet::new();
    let mut sides = std::collections::BTreeSet::new();
    for h in 0..3 {
        for c in 0..4 {
            lines.push(format!("cpu,host=h{h},cpu=cpu{h}{c} v=1 1000000000"));
            let tags = BTreeMap::from([
                ("host".to_string(), format!("h{h}")),
                ("cpu".to_string(), format!("cpu{h}{c}")),
            ]);
            if h == 0 {
                want_h0.insert(format!("cpu{h}{c}"));
                sides.insert(series_id("cpu", &tags) < u64::MAX / 2);
            }
        }
    }
    assert_eq!(sides.len(), 2, "host h0's series must span both regions");
    let resp = write_line(&client, &nodes[0].url, "tdb", &lines.join("\n")).await;
    assert!(resp.status().is_success(), "{}", resp.status());
    for n in &nodes {
        flush_node(n).await;
    }
    // Ask a node that holds only one of the regions.
    let resp = query_sql(
        &client,
        &nodes[2].url,
        "tdb",
        r#"SHOW TAG VALUES FROM "cpu" WITH KEY = "cpu" WHERE "host"::tag = 'h0'"#,
    )
    .await;
    let body: serde_json::Value = resp.json().await.unwrap();
    let got: std::collections::BTreeSet<String> = body["results"][0]["series"][0]["values"]
        .as_array()
        .cloned()
        .unwrap_or_default()
        .iter()
        .map(|r| r[1].as_str().unwrap().to_string())
        .collect();
    assert_eq!(got, want_h0, "{body}");
}

/// SHOW SERIES [EXACT] CARDINALITY and SHOW SERIES across regions on
/// different nodes: every node reports every series once (a region's series
/// counted by a peer that holds it, from its series index), and SHOW SERIES
/// lists InfluxDB series keys, not ids.
#[tokio::test]
#[serial(chdb)]
async fn series_cardinality_spans_regions() {
    let dir = tempfile::tempdir().unwrap();
    let nodes =
        start_sharded_three_node_cluster(dir.path(), ShardedClusterOptions::default()).await;
    let client = reqwest::Client::new();
    create_db(&client, &nodes[0].url, "cdb").await;
    bootstrap_two_regions(&nodes, "cdb", "autogen", "cpu").await;
    let mut lines = Vec::new();
    let mut want_keys = std::collections::BTreeSet::new();
    let mut sides = std::collections::BTreeSet::new();
    for h in 0..3 {
        for c in 0..4 {
            lines.push(format!("cpu,host=h{h},cpu=cpu{c} v=1 1000000000"));
            want_keys.insert(format!("cpu,cpu=cpu{c},host=h{h}"));
            let tags = BTreeMap::from([
                ("host".to_string(), format!("h{h}")),
                ("cpu".to_string(), format!("cpu{c}")),
            ]);
            sides.insert(series_id("cpu", &tags) < u64::MAX / 2);
        }
    }
    assert_eq!(sides.len(), 2, "the series must span both regions");
    let resp = write_line(&client, &nodes[0].url, "cdb", &lines.join("\n")).await;
    assert!(resp.status().is_success(), "{}", resp.status());
    for n in &nodes {
        flush_node(n).await;
    }
    for (i, node) in nodes.iter().enumerate() {
        let ask = |q: &'static str| {
            let client = client.clone();
            let url = node.url.clone();
            async move {
                let resp = query_sql(&client, &url, "cdb", q).await;
                resp.json::<serde_json::Value>().await.unwrap()
            }
        };
        let body = ask("SHOW SERIES CARDINALITY").await;
        let s = &body["results"][0]["series"][0];
        assert_eq!(
            s["columns"],
            serde_json::json!(["cardinality estimation"]),
            "node {i}: {body}"
        );
        assert_eq!(s["values"], serde_json::json!([[12]]), "node {i}: {body}");

        let body = ask("SHOW SERIES EXACT CARDINALITY FROM cpu").await;
        let s = &body["results"][0]["series"][0];
        assert_eq!(s["name"], "cpu", "node {i}: {body}");
        assert_eq!(s["values"], serde_json::json!([[12]]), "node {i}: {body}");

        let body = ask(r#"SHOW SERIES CARDINALITY WHERE "host" = 'h1'"#).await;
        assert_eq!(
            body["results"][0]["series"][0]["values"],
            serde_json::json!([[4]]),
            "node {i}: {body}"
        );

        let body = ask("SHOW SERIES").await;
        let got: std::collections::BTreeSet<String> = body["results"][0]["series"][0]["values"]
            .as_array()
            .cloned()
            .unwrap_or_default()
            .iter()
            .map(|r| r[0].as_str().unwrap().to_string())
            .collect();
        assert_eq!(got, want_keys, "node {i}");
    }
}

/// A field type conflict is a partial write (400) whichever node takes the
/// write: the conflicting points are dropped, the rest stored. A node that
/// holds no region forwards everything, and the primaries' refusals reach the
/// client as that 400, not as a 500 "write partially applied".
///
/// Field types are known per node, from the data it holds (as InfluxDB knows
/// them per shard): a region whose primary has never seen the field would
/// still take another type for it.
#[tokio::test]
#[serial(chdb)]
async fn field_type_conflicts_are_a_partial_write_on_every_node() {
    let dir = tempfile::tempdir().unwrap();
    let nodes =
        start_sharded_three_node_cluster(dir.path(), ShardedClusterOptions::default()).await;
    let client = reqwest::Client::new();
    create_db(&client, &nodes[0].url, "fdb").await;
    bootstrap_two_regions(&nodes, "fdb", "autogen", "cpu").await;
    let lines: Vec<String> = (0..8)
        .map(|h| format!("cpu,host=h{h} v=1 1000000000"))
        .collect();
    let resp = write_line(&client, &nodes[0].url, "fdb", &lines.join("\n")).await;
    let status = resp.status();
    assert!(
        status.is_success(),
        "{status}: {}",
        resp.text().await.unwrap_or_default()
    );
    for n in &nodes {
        flush_node(n).await;
    }
    for (i, node) in nodes.iter().enumerate() {
        let lines: Vec<String> = (0..8)
            .map(|h| format!("cpu,host=h{h} v=\"text\" 2000000000"))
            .collect();
        let resp = write_line(&client, &node.url, "fdb", &lines.join("\n")).await;
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        assert_eq!(status, reqwest::StatusCode::BAD_REQUEST, "node {i}: {body}");
        assert!(
            body.contains("partial write") && body.contains("field type conflict"),
            "node {i}: {body}"
        );
    }
    // Mixed, through the node that holds no region: the good points land.
    let lines: Vec<String> = (0..8)
        .flat_map(|h| {
            [
                format!("cpu,host=h{h} v=2 3000000000"),
                format!("cpu,host=h{h} v=\"bad\" 4000000000"),
            ]
        })
        .collect();
    let resp = write_line(&client, &nodes[2].url, "fdb", &lines.join("\n")).await;
    let status = resp.status();
    let body = resp.text().await.unwrap_or_default();
    assert_eq!(status, reqwest::StatusCode::BAD_REQUEST, "{body}");
    assert!(body.contains("dropped=8"), "{body}");
    for n in &nodes {
        flush_node(n).await;
    }
    let resp = query_sql(&client, &nodes[0].url, "fdb", "SELECT count(v) FROM cpu").await;
    let doc: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(doc["results"][0]["series"][0]["values"][0][1], 16, "{doc}");
}

/// PERCENTILE/MEDIAN on a sharded measurement: exact when every group is
/// one series (in one region), refused when a group spans regions. They
/// were refused outright, though Grafana's "p95 by host" pins one series per
/// host.
#[tokio::test]
#[serial(chdb)]
async fn percentile_answers_when_groups_are_whole_series() {
    let dir = tempfile::tempdir().unwrap();
    let nodes =
        start_sharded_three_node_cluster(dir.path(), ShardedClusterOptions::default()).await;
    let client = reqwest::Client::new();
    create_db(&client, &nodes[0].url, "pdb").await;
    bootstrap_two_regions(&nodes, "pdb", "autogen", "cpu").await;
    let mut lines = Vec::new();
    let mut sides = std::collections::BTreeSet::new();
    for c in 0..4 {
        let tags = BTreeMap::from([
            ("host".to_string(), "h0".to_string()),
            ("cpu".to_string(), format!("cpu{c}")),
        ]);
        sides.insert(series_id("cpu", &tags) < u64::MAX / 2);
        for i in 1..=20i64 {
            lines.push(format!(
                "cpu,host=h0,cpu=cpu{c} v={} {}",
                i * (c + 1),
                i * 1_000_000_000
            ));
        }
    }
    assert_eq!(sides.len(), 2, "h0's series must span both regions");
    let resp = write_line(&client, &nodes[0].url, "pdb", &lines.join("\n")).await;
    assert!(resp.status().is_success(), "{}", resp.status());
    for n in &nodes {
        flush_node(n).await;
    }
    let ask = |q: &str| {
        let q = q.to_string();
        let client = client.clone();
        let url = nodes[2].url.clone();
        async move {
            let resp = query_sql(&client, &url, "pdb", &q).await;
            resp.json::<serde_json::Value>().await.unwrap()
        }
    };
    // One series per group: each cpu's median is that of i * (c + 1), i = 1..20.
    let body = ask(r#"SELECT median("v") FROM "cpu" GROUP BY "host", "cpu""#).await;
    assert!(body["results"][0]["error"].is_null(), "{body}");
    let mut got: Vec<(String, f64)> = body["results"][0]["series"]
        .as_array()
        .unwrap()
        .iter()
        .map(|s| {
            (
                s["tags"]["cpu"].as_str().unwrap().to_string(),
                s["values"][0][1].as_f64().unwrap(),
            )
        })
        .collect();
    got.sort_by(|a, b| a.0.cmp(&b.0));
    let want: Vec<(String, f64)> = (0..4)
        .map(|c| (format!("cpu{c}"), 10.5 * (c + 1) as f64))
        .collect();
    assert_eq!(got, want, "{body}");
    // A group (host h0) that spans both regions is refused, not guessed.
    let body = ask(r#"SELECT median("v") FROM "cpu" GROUP BY "host""#).await;
    let err = body["error"]
        .as_str()
        .or_else(|| body["results"][0]["error"].as_str())
        .unwrap_or_default()
        .to_string();
    assert!(err.contains("spans"), "{body}");
}
