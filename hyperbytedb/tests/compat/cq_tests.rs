//! Continuous query scheduling and execution (InfluxDB v1 parity).

use chrono::{TimeZone, Utc};
use serial_test::serial;

use hyperbytedb::domain::continuous_query::ContinuousQueryDef;
use hyperbytedb::domain::cq_schedule::{coverage_window, should_run};
use hyperbytedb::ports::ingestion::{IngestionPort, WritePayloadFormat};

use super::TestContext;

fn ts(h: u32, m: u32) -> i64 {
    Utc.with_ymd_and_hms(2016, 8, 28, h, m, 0)
        .unwrap()
        .timestamp_nanos_opt()
        .unwrap()
}

#[tokio::test]
async fn create_cq_stores_schedule_metadata() {
    let ctx = TestContext::new_no_chdb().unwrap();
    ctx.metadata
        .create_database("transportation")
        .await
        .unwrap();

    let resp = ctx
        .query(
            "transportation",
            r#"CREATE CONTINUOUS QUERY "cq_basic" ON "transportation" BEGIN SELECT mean("passengers") INTO "average_passengers" FROM "bus_data" GROUP BY time(1h) END"#,
        )
        .await
        .unwrap();
    assert!(
        resp.results[0].error.is_none(),
        "{:?}",
        resp.results[0].error
    );

    let cqs = ctx
        .metadata
        .list_continuous_queries("transportation")
        .await
        .unwrap();
    assert_eq!(cqs.len(), 1);
    assert_eq!(cqs[0].group_by_interval_secs, 3600);
    assert_eq!(cqs[0].execution_interval_secs, 3600);
    assert_eq!(cqs[0].coverage_interval_secs, 3600);
    assert!(!cqs[0].is_advanced);
}

#[tokio::test]
async fn create_cq_rejects_for_shorter_than_group_by() {
    let ctx = TestContext::new_no_chdb().unwrap();
    ctx.metadata
        .create_database("transportation")
        .await
        .unwrap();

    let resp = ctx
        .query(
            "transportation",
            r#"CREATE CONTINUOUS QUERY "cq_bad" ON "transportation" RESAMPLE FOR 5m BEGIN SELECT mean("passengers") INTO "average_passengers" FROM "bus_data" GROUP BY time(30m) END"#,
        )
        .await
        .unwrap();
    assert!(resp.results[0].error.is_some());
    let err = resp.results[0].error.as_ref().unwrap();
    assert!(err.contains("FOR duration must be >="), "{err}");
}

#[tokio::test]
#[serial(chdb)]
async fn basic_cq_downsamples_bus_data_at_8am() {
    let ctx = match TestContext::new() {
        Ok(ctx) => ctx,
        Err(_) => {
            eprintln!("skipping basic_cq_downsamples_bus_data_at_8am: chDB unavailable");
            return;
        }
    };

    ctx.metadata
        .create_database("transportation")
        .await
        .unwrap();

    let lines = format!(
        "bus_data passengers=5i {}\n\
         bus_data passengers=8i {}\n\
         bus_data passengers=8i {}\n\
         bus_data passengers=7i {}\n\
         bus_data passengers=8i {}\n\
         bus_data passengers=15i {}\n\
         bus_data passengers=15i {}\n\
         bus_data passengers=17i {}",
        ts(7, 0),
        ts(7, 15),
        ts(7, 30),
        ts(7, 45),
        ts(8, 0),
        ts(8, 15),
        ts(8, 30),
        ts(8, 45),
    );
    ctx.write_and_flush("transportation", &lines).await.unwrap();

    let resp = ctx
        .query(
            "transportation",
            r#"CREATE CONTINUOUS QUERY "cq_basic" ON "transportation" BEGIN SELECT mean("passengers") INTO "average_passengers" FROM "bus_data" GROUP BY time(1h) END"#,
        )
        .await
        .unwrap();
    assert!(
        resp.results[0].error.is_none(),
        "{:?}",
        resp.results[0].error
    );

    let mut cq = ctx
        .metadata
        .get_continuous_query("transportation", "cq_basic")
        .await
        .unwrap()
        .unwrap();
    cq.last_run_at = Some(
        Utc.with_ymd_and_hms(2016, 8, 28, 7, 0, 0)
            .unwrap()
            .to_rfc3339(),
    );

    let run_at = Utc.with_ymd_and_hms(2016, 8, 28, 8, 0, 0).unwrap();
    assert!(should_run(run_at, &cq));

    let result = ctx
        .query_service
        .execute_continuous_query(&mut cq, run_at)
        .await
        .unwrap();
    assert_eq!(
        result.window.start,
        Utc.with_ymd_and_hms(2016, 8, 28, 7, 0, 0).unwrap()
    );
    assert_eq!(
        result.window.end,
        Utc.with_ymd_and_hms(2016, 8, 28, 8, 0, 0).unwrap()
    );
    assert_eq!(result.points_written, 1);

    ctx.flush_service.flush().await.unwrap();

    let resp = ctx
        .query(
            "transportation",
            &format!(
                r#"SELECT * FROM "average_passengers" WHERE time >= {} AND time < {}"#,
                ts(7, 0),
                ts(8, 0),
            ),
        )
        .await
        .unwrap();
    assert!(
        resp.results[0].error.is_none(),
        "{:?}",
        resp.results[0].error
    );
    let series = &resp.results[0].series.as_ref().unwrap()[0];
    let mean_idx = series
        .columns
        .iter()
        .position(|c| c == "mean" || c == "mean_passengers")
        .unwrap();
    let value = &series.values[0][mean_idx];
    let mean = value.as_f64().unwrap();
    assert!(
        (mean - 7.0).abs() < 0.01,
        "expected mean ~7 for 7:00-8:00 window, got {mean}"
    );
}

#[tokio::test]
#[serial(chdb)]
async fn advanced_every_cq_recomputes_current_hour_bucket() {
    let ctx = match TestContext::new() {
        Ok(ctx) => ctx,
        Err(_) => {
            eprintln!(
                "skipping advanced_every_cq_recomputes_current_hour_bucket: chDB unavailable"
            );
            return;
        }
    };

    ctx.metadata
        .create_database("transportation")
        .await
        .unwrap();

    let lines = format!(
        "bus_data passengers=8i {}\n\
         bus_data passengers=15i {}\n\
         bus_data passengers=15i {}",
        ts(8, 0),
        ts(8, 15),
        ts(8, 30),
    );
    ctx.write_and_flush("transportation", &lines).await.unwrap();

    ctx.query(
        "transportation",
        r#"CREATE CONTINUOUS QUERY "cq_advanced_every" ON "transportation" RESAMPLE EVERY 30m BEGIN SELECT mean("passengers") INTO "average_passengers" FROM "bus_data" GROUP BY time(1h) END"#,
    )
    .await
    .unwrap();

    let mut cq = ctx
        .metadata
        .get_continuous_query("transportation", "cq_advanced_every")
        .await
        .unwrap()
        .unwrap();
    cq.last_run_at = Some(
        Utc.with_ymd_and_hms(2016, 8, 28, 8, 0, 0)
            .unwrap()
            .to_rfc3339(),
    );

    let run_at = Utc.with_ymd_and_hms(2016, 8, 28, 8, 30, 0).unwrap();
    let window = coverage_window(run_at, &cq);
    assert_eq!(
        window.start,
        Utc.with_ymd_and_hms(2016, 8, 28, 8, 0, 0).unwrap()
    );
    assert_eq!(
        window.end,
        Utc.with_ymd_and_hms(2016, 8, 28, 9, 0, 0).unwrap()
    );

    let result = ctx
        .query_service
        .execute_continuous_query(&mut cq, run_at)
        .await
        .unwrap();
    assert!(result.points_written >= 1);

    ctx.flush_service.flush().await.unwrap();

    let resp = ctx
        .query(
            "transportation",
            &format!(
                r#"SELECT * FROM "average_passengers" WHERE time >= {} AND time < {}"#,
                ts(8, 0),
                ts(9, 0),
            ),
        )
        .await
        .unwrap();
    let series = &resp.results[0].series.as_ref().unwrap()[0];
    let mean_idx = series
        .columns
        .iter()
        .position(|c| c == "mean" || c == "mean_passengers")
        .unwrap();
    let mean = series.values[0][mean_idx].as_f64().unwrap();
    assert!(
        (mean - 12.6667).abs() < 0.1,
        "expected partial-hour mean ~12.67, got {mean}"
    );
}

#[tokio::test]
async fn legacy_cq_definition_is_normalized_on_run() {
    let ctx = TestContext::new_no_chdb().unwrap();
    ctx.metadata
        .create_database("transportation")
        .await
        .unwrap();

    let mut legacy = ContinuousQueryDef {
        name: "cq_legacy".to_string(),
        database: "transportation".to_string(),
        query_text: r#"SELECT mean("passengers") INTO "average_passengers" FROM "bus_data" GROUP BY time(1h)"#
            .to_string(),
        resample_every_secs: None,
        resample_for_secs: None,
        created_at: Utc.with_ymd_and_hms(2016, 8, 28, 6, 0, 0)
            .unwrap()
            .to_rfc3339(),
        group_by_interval_secs: 0,
        group_by_offset_secs: 0,
        execution_interval_secs: 0,
        coverage_interval_secs: 0,
        is_advanced: false,
        last_run_at: None,
    };
    legacy.normalize().unwrap();
    assert_eq!(legacy.group_by_interval_secs, 3600);
    assert_eq!(legacy.execution_interval_secs, 3600);
}

#[test]
fn prepare_cq_select_strips_user_time_and_injects_window() {
    use hyperbytedb::timeseriesql::parser::parse_query;
    use hyperbytedb::timeseriesql::to_clickhouse::{
        extract_time_bounds, prepare_cq_select, strip_time_predicates,
    };

    let q = r#"SELECT mean("passengers") FROM "bus_data" WHERE time >= '2016-08-28T00:00:00Z' AND "region" = 'east' GROUP BY time(1h)"#;
    let stmt = match parse_query(q).unwrap().remove(0) {
        hyperbytedb::timeseriesql::ast::Statement::Select(s) => s,
        _ => panic!("expected select"),
    };

    assert!(strip_time_predicates(stmt.condition.clone()).is_some());
    let start = ts(7, 0);
    let end = ts(8, 0);
    let prepared = prepare_cq_select(&stmt, start, end, true);
    assert!(prepared.fill.is_none());
    let (min, max) = extract_time_bounds(prepared.condition.as_ref());
    assert_eq!(min, Some(start));
    assert_eq!(max, Some(end));
}

#[tokio::test]
#[serial(chdb)]
async fn cq_into_retention_policy_writes_and_queries_isolated_table() {
    let ctx = match TestContext::new() {
        Ok(ctx) => ctx,
        Err(_) => {
            eprintln!(
                "skipping cq_into_retention_policy_writes_and_queries_isolated_table: chDB unavailable"
            );
            return;
        }
    };

    ctx.metadata.create_database("gameservers").await.unwrap();
    ctx.metadata
        .create_retention_policy(
            "gameservers",
            hyperbytedb::domain::database::RetentionPolicy {
                name: "default_high".to_string(),
                duration: Some(std::time::Duration::from_secs(7 * 24 * 3600)),
                shard_group_duration: std::time::Duration::from_secs(3600),
                replication_factor: 1,
                is_default: false,
            },
        )
        .await
        .unwrap();

    let minute = ts(8, 0);
    let lines = format!(
        "server_stats,region_id=us,server_id=s1 cpu=1i {minute}\n\
         server_stats,region_id=eu,server_id=s2 cpu=1i {minute}\n\
         server_stats,region_id=ap,server_id=s3 cpu=1i {minute}\n\
         server_stats,region_id=us,server_id=s4 cpu=1i {minute}\n\
         server_stats,region_id=eu,server_id=s5 cpu=1i {minute}",
    );
    ctx.write_and_flush("gameservers", &lines).await.unwrap();

    ctx.query(
        "gameservers",
        r#"CREATE CONTINUOUS QUERY "server_stats_1m" ON "gameservers" RESAMPLE EVERY 1m BEGIN SELECT count("cpu") AS "num_servers" INTO "default_high"."server_stats" FROM "server_stats" GROUP BY time(1m), "region_id" END"#,
    )
    .await
    .unwrap();

    let mut cq = ctx
        .metadata
        .get_continuous_query("gameservers", "server_stats_1m")
        .await
        .unwrap()
        .unwrap();
    cq.last_run_at = Some(
        Utc.with_ymd_and_hms(2016, 8, 28, 7, 59, 0)
            .unwrap()
            .to_rfc3339(),
    );

    let run_at = Utc.with_ymd_and_hms(2016, 8, 28, 8, 0, 0).unwrap();
    let result = ctx
        .query_service
        .execute_continuous_query(&mut cq, run_at)
        .await
        .unwrap();
    assert_eq!(result.points_written, 3, "one point per region_id");

    ctx.flush_service.flush().await.unwrap();

    let raw_count: u64 = ctx
        .query_port
        .execute_sql(
            "SELECT count() AS c FROM `gameservers_default_high_server_stats` FORMAT JSONEachRow",
        )
        .await
        .unwrap()
        .lines()
        .next()
        .and_then(|l| serde_json::from_str::<serde_json::Value>(l).ok())
        .and_then(|v| v.get("c").and_then(|c| c.as_u64()))
        .unwrap_or(0);
    assert_eq!(
        raw_count, 3,
        "downsampled data should land in default_high table"
    );

    let autogen_count: u64 = ctx
        .query_port
        .execute_sql(
            "SELECT count() AS c FROM `gameservers_autogen_server_stats` FORMAT JSONEachRow",
        )
        .await
        .unwrap()
        .lines()
        .next()
        .and_then(|l| serde_json::from_str::<serde_json::Value>(l).ok())
        .and_then(|v| v.get("c").and_then(|c| c.as_u64()))
        .unwrap_or(0);
    assert_eq!(
        autogen_count, 5,
        "autogen should still have per-server rows"
    );

    let downsampled = ctx
        .query(
            "gameservers",
            &format!(r#"SELECT * FROM "default_high"."server_stats" WHERE time = {minute}"#),
        )
        .await
        .unwrap();
    assert!(
        downsampled.results[0].error.is_none(),
        "{:?}",
        downsampled.results[0].error
    );
    let ds_series = &downsampled.results[0].series.as_ref().unwrap()[0];
    assert_eq!(
        ds_series.values.len(),
        3,
        "default_high should have one row per region"
    );
    assert!(
        !ds_series.columns.iter().any(|c| c == "server_id"),
        "downsampled table should not expose server_id tag"
    );

    let tag_keys = ctx
        .query(
            "gameservers",
            r#"SHOW TAG KEYS FROM "default_high"."server_stats""#,
        )
        .await
        .unwrap();
    assert!(
        tag_keys.results[0].error.is_none(),
        "{:?}",
        tag_keys.results[0].error
    );
    let keys: Vec<&str> = tag_keys.results[0].series.as_ref().unwrap()[0]
        .values
        .iter()
        .filter_map(|row| row.first().and_then(|v| v.as_str()))
        .collect();
    assert!(keys.contains(&"region_id"));
    assert!(!keys.contains(&"server_id"));

    let tag_values = ctx
        .query(
            "gameservers",
            r#"SHOW TAG VALUES FROM "default_high"."server_stats" WITH KEY = "region_id""#,
        )
        .await
        .unwrap();
    assert!(
        tag_values.results[0].error.is_none(),
        "{:?}",
        tag_values.results[0].error
    );
    let regions: Vec<&str> = tag_values.results[0].series.as_ref().unwrap()[0]
        .values
        .iter()
        .filter_map(|row| row.get(1).and_then(|v| v.as_str()))
        .collect();
    assert_eq!(regions.len(), 3);
    assert!(regions.contains(&"us"));
    assert!(regions.contains(&"eu"));
    assert!(regions.contains(&"ap"));
}

#[tokio::test]
#[serial(chdb)]
async fn select_qualified_rp_overrides_http_default_rp() {
    let ctx = match TestContext::new() {
        Ok(c) => c,
        Err(_) => {
            eprintln!("skipping qualified RP test: chDB not available");
            return;
        }
    };

    ctx.metadata.create_database("gameservers").await.unwrap();
    ctx.metadata
        .create_retention_policy(
            "gameservers",
            hyperbytedb::domain::database::RetentionPolicy {
                name: "default_high".to_string(),
                duration: Some(std::time::Duration::from_secs(7 * 24 * 3600)),
                shard_group_duration: std::time::Duration::from_secs(3600),
                replication_factor: 1,
                is_default: false,
            },
        )
        .await
        .unwrap();

    let minute = ts(8, 0);
    ctx.write_and_flush(
        "gameservers",
        &format!(
            "server_stats,region_id=us,server_id=s1 cpu=1i {minute}\n\
             server_stats,region_id=eu,server_id=s2 cpu=1i {minute}"
        ),
    )
    .await
    .unwrap();

    ctx.ingestion
        .ingest(
            "gameservers",
            Some("default_high"),
            None,
            format!(
                "server_stats,region_id=us num_servers=1i {minute}\n\
                 server_stats,region_id=eu num_servers=1i {minute}"
            )
            .as_bytes(),
            WritePayloadFormat::LineProtocol,
        )
        .await
        .unwrap();
    ctx.flush_service.flush().await.unwrap();

    let resp = ctx
        .query_with_rp(
            "gameservers",
            &format!(
                r#"SELECT sum("num_servers") FROM "default_high"."server_stats" WHERE time = {minute}"#
            ),
            Some("default"),
        )
        .await
        .unwrap();
    assert!(
        resp.results[0].error.is_none(),
        "{:?}",
        resp.results[0].error
    );
    let series = resp.results[0].series.as_ref().unwrap();
    assert_eq!(series.len(), 1);
    assert_eq!(
        series[0].values[0][0].as_i64(),
        Some(2),
        "qualified default_high measurement should sum both default_high points"
    );

    // Unqualified measurement with rp=default resolves to autogen, not default_high.
    let autogen = ctx
        .query_with_rp(
            "gameservers",
            &format!(r#"SELECT sum("num_servers") FROM "server_stats" WHERE time = {minute}"#),
            Some("default"),
        )
        .await;
    assert!(
        autogen.is_err(),
        "autogen server_stats has no num_servers column; should not read default_high"
    );
}
