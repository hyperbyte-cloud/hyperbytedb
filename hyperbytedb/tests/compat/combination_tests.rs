//! TimeseriesQL end-to-end combination tests.
//!
//! Exercises the full path (parse → translate → chDB execute → result parse) for
//! InfluxQL features *in combination* — the interactions that unit tests over the
//! generated SQL can't catch. Every test writes deterministic data and asserts on
//! concrete values, column sets, series splitting, and gap-fill behaviour.
//!
//! All tests require chDB, which exposes one process-global server, so they are
//! marked `#[serial(chdb)]` to keep concurrent chDB sessions from colliding.

use hyperbytedb::adapters::http::router::QueryService;
use hyperbytedb::domain::query_result::{QueryResponse, SeriesResult};
use serial_test::serial;

use super::TestContext;

const S: i64 = 1_000_000_000; // one second in nanoseconds

fn series_of(resp: &QueryResponse) -> &[SeriesResult] {
    assert!(
        resp.results[0].error.is_none(),
        "query returned an error: {:?}",
        resp.results[0].error
    );
    resp.results[0].series.as_deref().unwrap_or(&[])
}

fn idx(s: &SeriesResult, name: &str) -> usize {
    s.columns
        .iter()
        .position(|c| c == name)
        .unwrap_or_else(|| panic!("column `{name}` not in {:?}", s.columns))
}

fn fval(s: &SeriesResult, row: usize, name: &str) -> f64 {
    let i = idx(s, name);
    let v = &s.values[row][i];
    v.as_f64()
        .unwrap_or_else(|| panic!("row {row} col `{name}` is not a number: {v:?}"))
}

fn is_null(s: &SeriesResult, row: usize, name: &str) -> bool {
    let i = idx(s, name);
    s.values[row][i].is_null()
}

fn tag(s: &SeriesResult, key: &str) -> String {
    s.tags
        .as_ref()
        .and_then(|t| t.get(key))
        .cloned()
        .unwrap_or_default()
}

// ---------------------------------------------------------------------------
// Raw (non-aggregate) selects — must always carry `time`, one row per point
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial(chdb)]
async fn raw_multi_field_select_includes_time_and_is_sorted() {
    let ctx = TestContext::new().unwrap();
    ctx.metadata.create_database("db").await.unwrap();
    ctx.write_and_flush(
        "db",
        &format!(
            "cpu load1=2.0,load5=20.0 {}\ncpu load1=1.0,load5=10.0 {}",
            2 * S,
            S
        ),
    )
    .await
    .unwrap();

    let resp = ctx
        .query("db", "SELECT load1, load5 FROM cpu")
        .await
        .unwrap();
    let s = &series_of(&resp)[0];
    assert_eq!(s.columns[0], "time", "time must be the first column");
    assert!(s.columns.contains(&"load1".into()) && s.columns.contains(&"load5".into()));
    assert!(
        !s.columns.contains(&"load15".into()),
        "unselected fields must not appear"
    );
    assert_eq!(s.values.len(), 2);
    // Rows sorted ascending by time despite reversed write order.
    assert_eq!(fval(s, 0, "load1"), 1.0);
    assert_eq!(fval(s, 0, "load5"), 10.0);
    assert_eq!(fval(s, 1, "load1"), 2.0);
}

#[tokio::test]
#[serial(chdb)]
async fn raw_select_with_tag_filter_and_time_range() {
    let ctx = TestContext::new().unwrap();
    ctx.metadata.create_database("db").await.unwrap();
    ctx.write_and_flush(
        "db",
        &format!(
            "cpu,host=a v=1.0 {}\ncpu,host=b v=2.0 {}\ncpu,host=a v=3.0 {}",
            S,
            2 * S,
            3 * S
        ),
    )
    .await
    .unwrap();

    let resp = ctx
        .query(
            "db",
            &format!(
                "SELECT v FROM cpu WHERE host='a' AND time >= {} AND time <= {}",
                S,
                3 * S
            ),
        )
        .await
        .unwrap();
    let s = &series_of(&resp)[0];
    assert_eq!(s.columns[0], "time");
    assert_eq!(s.values.len(), 2, "only host=a rows");
    assert_eq!(fval(s, 0, "v"), 1.0);
    assert_eq!(fval(s, 1, "v"), 3.0);
}

// ---------------------------------------------------------------------------
// Aggregates × GROUP BY time × aliases
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial(chdb)]
async fn multiple_aggregates_group_by_time_with_aliases() {
    let ctx = TestContext::new().unwrap();
    ctx.metadata.create_database("db").await.unwrap();
    // bucket 1s: {10,20}; bucket 2s: {30}
    ctx.write_and_flush(
        "db",
        &format!("m v=10.0 {}\nm v=20.0 {}\nm v=30.0 {}", S, S + S / 2, 2 * S),
    )
    .await
    .unwrap();

    let resp = ctx
        .query(
            "db",
            "SELECT mean(v) AS avg, max(v) AS mx, min(v) AS mn, count(v) AS c, sum(v) AS sm \
             FROM m GROUP BY time(1s)",
        )
        .await
        .unwrap();
    let s = &series_of(&resp)[0];
    assert_eq!(s.columns[0], "time");
    for c in ["avg", "mx", "mn", "c", "sm"] {
        assert!(s.columns.contains(&c.to_string()), "missing column {c}");
    }
    assert_eq!(s.values.len(), 2);
    assert_eq!(fval(s, 0, "avg"), 15.0);
    assert_eq!(fval(s, 0, "mx"), 20.0);
    assert_eq!(fval(s, 0, "mn"), 10.0);
    assert_eq!(fval(s, 0, "c"), 2.0);
    assert_eq!(fval(s, 0, "sm"), 30.0);
    assert_eq!(fval(s, 1, "avg"), 30.0);
    assert_eq!(fval(s, 1, "c"), 1.0);
}

#[tokio::test]
#[serial(chdb)]
async fn arithmetic_on_aggregate_with_alias() {
    let ctx = TestContext::new().unwrap();
    ctx.metadata.create_database("db").await.unwrap();
    ctx.write_and_flush("db", &format!("m v=5.0 {}\nm v=15.0 {}", S, S + S / 2))
        .await
        .unwrap();

    let resp = ctx
        .query(
            "db",
            "SELECT mean(v) * 10 + 1 AS scaled FROM m GROUP BY time(1s)",
        )
        .await
        .unwrap();
    let s = &series_of(&resp)[0];
    // mean(5,15)=10 -> 10*10+1 = 101
    assert_eq!(fval(s, 0, "scaled"), 101.0);
}

#[tokio::test]
#[serial(chdb)]
async fn selectors_first_last_group_by_time() {
    let ctx = TestContext::new().unwrap();
    ctx.metadata.create_database("db").await.unwrap();
    ctx.write_and_flush("db", &format!("m v=10.0 {}\nm v=99.0 {}", S, S + S / 2))
        .await
        .unwrap();

    let resp = ctx
        .query(
            "db",
            "SELECT first(v) AS f, last(v) AS l FROM m GROUP BY time(1s)",
        )
        .await
        .unwrap();
    let s = &series_of(&resp)[0];
    assert_eq!(fval(s, 0, "f"), 10.0, "first by time");
    assert_eq!(fval(s, 0, "l"), 99.0, "last by time");
}

#[tokio::test]
#[serial(chdb)]
async fn percentile_and_spread_group_by_time() {
    let ctx = TestContext::new().unwrap();
    ctx.metadata.create_database("db").await.unwrap();
    let mut lp = String::new();
    for (i, v) in [10.0, 20.0, 30.0, 40.0].iter().enumerate() {
        lp.push_str(&format!("m v={v} {}\n", S + (i as i64) * (S / 10)));
    }
    ctx.write_and_flush("db", &lp).await.unwrap();

    let resp = ctx
        .query(
            "db",
            "SELECT percentile(v, 50) AS p50, spread(v) AS sp FROM m GROUP BY time(1s)",
        )
        .await
        .unwrap();
    let s = &series_of(&resp)[0];
    assert_eq!(fval(s, 0, "sp"), 30.0, "spread = max-min = 40-10");
    let p50 = fval(s, 0, "p50");
    assert!((20.0..=30.0).contains(&p50), "p50 in range, got {p50}");
}

// ---------------------------------------------------------------------------
// GROUP BY tag (with and without time)
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial(chdb)]
async fn group_by_tag_only_splits_series() {
    let ctx = TestContext::new().unwrap();
    ctx.metadata.create_database("db").await.unwrap();
    ctx.write_and_flush(
        "db",
        &format!(
            "m,host=a v=10.0 {}\nm,host=a v=30.0 {}\nm,host=b v=100.0 {}",
            S,
            2 * S,
            S
        ),
    )
    .await
    .unwrap();

    let resp = ctx
        .query("db", "SELECT mean(v) AS avg FROM m GROUP BY host")
        .await
        .unwrap();
    let series = series_of(&resp);
    assert_eq!(series.len(), 2, "one series per host");
    let a = series
        .iter()
        .find(|s| tag(s, "host") == "a")
        .expect("host a");
    let b = series
        .iter()
        .find(|s| tag(s, "host") == "b")
        .expect("host b");
    assert_eq!(fval(a, 0, "avg"), 20.0);
    assert_eq!(fval(b, 0, "avg"), 100.0);
}

// ---------------------------------------------------------------------------
// The full Grafana combination: regex + time range + GROUP BY time,tag + fill
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial(chdb)]
async fn grafana_multi_host_regex_groupby_time_tag_fill_null() {
    let ctx = TestContext::new().unwrap();
    ctx.metadata.create_database("db").await.unwrap();
    // Two hosts, sparse points (gaps) inside a 0..50s window, 10s buckets.
    ctx.write_and_flush(
        "db",
        &format!(
            "sys,host=a load=1.0 {}\nsys,host=a load=4.0 {}\n\
             sys,host=b load=10.0 {}\nsys,host=b load=40.0 {}",
            10 * S,
            40 * S,
            10 * S,
            40 * S
        ),
    )
    .await
    .unwrap();

    let q = format!(
        "SELECT mean(\"load\") AS \"L\" FROM \"sys\" \
         WHERE \"host\" =~ /^(a|b)$/ AND time >= {} AND time <= {} \
         GROUP BY time(10s), \"host\" fill(null) ORDER BY time ASC",
        10 * S,
        50 * S
    );
    let resp = ctx.query("db", &q).await.unwrap();
    let series = series_of(&resp);

    // Exactly two real series — no phantom empty-host series.
    assert_eq!(
        series.len(),
        2,
        "expected one series per host, got tags {:?}",
        series.iter().map(|s| &s.tags).collect::<Vec<_>>()
    );
    for s in series {
        let h = tag(s, "host");
        assert!(h == "a" || h == "b", "unexpected host tag {h:?}");
        assert!(!h.is_empty(), "no empty-host phantom series");
        // Filled grid: every bucket present, first bucket has data, a gap is null.
        assert!(s.values.len() >= 3, "fill should produce a bucket grid");
        assert!(!is_null(s, 0, "L"), "first bucket has data");
        assert!(is_null(s, 1, "L"), "gap bucket filled with null");
        let first = fval(s, 0, "L");
        let expected_first = if h == "a" { 1.0 } else { 10.0 };
        assert_eq!(first, expected_first, "host {h} first bucket value");
    }
}

#[tokio::test]
#[serial(chdb)]
async fn regex_not_match_excludes_host() {
    let ctx = TestContext::new().unwrap();
    ctx.metadata.create_database("db").await.unwrap();
    ctx.write_and_flush(
        "db",
        &format!("m,host=keep v=1.0 {}\nm,host=drop v=2.0 {}", S, S),
    )
    .await
    .unwrap();

    let resp = ctx
        .query(
            "db",
            "SELECT mean(v) AS avg FROM m WHERE host !~ /drop/ GROUP BY host",
        )
        .await
        .unwrap();
    let series = series_of(&resp);
    assert_eq!(series.len(), 1);
    assert_eq!(tag(&series[0], "host"), "keep");
}

// ---------------------------------------------------------------------------
// fill() variants — exact gap-fill semantics
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial(chdb)]
async fn fill_zero_fills_gaps_with_zero() {
    let ctx = TestContext::new().unwrap();
    ctx.metadata.create_database("db").await.unwrap();
    ctx.write_and_flush("db", &format!("m v=7.0 {}\nm v=9.0 {}", S, 3 * S))
        .await
        .unwrap();

    let resp = ctx
        .query(
            "db",
            &format!(
                "SELECT mean(v) AS a FROM m WHERE time >= {} AND time <= {} GROUP BY time(1s) fill(0)",
                S,
                3 * S
            ),
        )
        .await
        .unwrap();
    let s = &series_of(&resp)[0];
    assert_eq!(fval(s, 0, "a"), 7.0);
    assert_eq!(fval(s, 1, "a"), 0.0, "gap filled with 0");
    assert_eq!(fval(s, 2, "a"), 9.0);
}

#[tokio::test]
#[serial(chdb)]
async fn fill_null_leaves_gaps_null_not_zero() {
    let ctx = TestContext::new().unwrap();
    ctx.metadata.create_database("db").await.unwrap();
    ctx.write_and_flush("db", &format!("m v=7.0 {}\nm v=9.0 {}", S, 3 * S))
        .await
        .unwrap();

    let resp = ctx
        .query(
            "db",
            &format!(
                "SELECT mean(v) AS a FROM m WHERE time >= {} AND time <= {} GROUP BY time(1s) fill(null)",
                S,
                3 * S
            ),
        )
        .await
        .unwrap();
    let s = &series_of(&resp)[0];
    assert_eq!(fval(s, 0, "a"), 7.0);
    assert!(is_null(s, 1, "a"), "gap stays null, not 0");
    assert_eq!(fval(s, 2, "a"), 9.0);
}

#[tokio::test]
#[serial(chdb)]
async fn fill_previous_carries_last_value_forward() {
    let ctx = TestContext::new().unwrap();
    ctx.metadata.create_database("db").await.unwrap();
    ctx.write_and_flush("db", &format!("m v=5.0 {}\nm v=9.0 {}", S, 4 * S))
        .await
        .unwrap();

    let resp = ctx
        .query(
            "db",
            &format!(
                "SELECT mean(v) AS a FROM m WHERE time >= {} AND time <= {} GROUP BY time(1s) fill(previous)",
                S,
                4 * S
            ),
        )
        .await
        .unwrap();
    let s = &series_of(&resp)[0];
    assert_eq!(fval(s, 0, "a"), 5.0);
    assert_eq!(fval(s, 1, "a"), 5.0, "carried forward");
    assert_eq!(fval(s, 2, "a"), 5.0, "carried forward");
    assert_eq!(fval(s, 3, "a"), 9.0);
}

#[tokio::test]
#[serial(chdb)]
async fn fill_none_omits_gap_buckets() {
    let ctx = TestContext::new().unwrap();
    ctx.metadata.create_database("db").await.unwrap();
    ctx.write_and_flush("db", &format!("m v=7.0 {}\nm v=9.0 {}", S, 3 * S))
        .await
        .unwrap();

    let resp = ctx
        .query(
            "db",
            &format!(
                "SELECT mean(v) AS a FROM m WHERE time >= {} AND time <= {} GROUP BY time(1s) fill(none)",
                S,
                3 * S
            ),
        )
        .await
        .unwrap();
    let s = &series_of(&resp)[0];
    assert_eq!(s.values.len(), 2, "no synthetic gap rows with fill(none)");
}

// ---------------------------------------------------------------------------
// Transforms × GROUP BY time
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial(chdb)]
async fn difference_of_mean_group_by_time() {
    let ctx = TestContext::new().unwrap();
    ctx.metadata.create_database("db").await.unwrap();
    ctx.write_and_flush(
        "db",
        &format!("m v=10.0 {}\nm v=25.0 {}\nm v=45.0 {}", S, 2 * S, 3 * S),
    )
    .await
    .unwrap();

    let resp = ctx
        .query(
            "db",
            "SELECT difference(mean(v)) AS d FROM m GROUP BY time(1s)",
        )
        .await
        .unwrap();
    let s = &series_of(&resp)[0];
    // diffs: 25-10=15, 45-25=20 (first bucket has no previous).
    let diffs: Vec<f64> = (0..s.values.len())
        .filter(|&r| !is_null(s, r, "d"))
        .map(|r| fval(s, r, "d"))
        .collect();
    assert_eq!(diffs, vec![15.0, 20.0]);
}

#[tokio::test]
#[serial(chdb)]
async fn cumulative_sum_group_by_time() {
    let ctx = TestContext::new().unwrap();
    ctx.metadata.create_database("db").await.unwrap();
    ctx.write_and_flush(
        "db",
        &format!("m v=1.0 {}\nm v=2.0 {}\nm v=3.0 {}", S, 2 * S, 3 * S),
    )
    .await
    .unwrap();

    let resp = ctx
        .query(
            "db",
            "SELECT cumulative_sum(sum(v)) AS cs FROM m GROUP BY time(1s)",
        )
        .await
        .unwrap();
    let s = &series_of(&resp)[0];
    let cs: Vec<f64> = (0..s.values.len()).map(|r| fval(s, r, "cs")).collect();
    assert_eq!(cs, vec![1.0, 3.0, 6.0], "running total");
}

// ---------------------------------------------------------------------------
// ORDER BY / LIMIT / OFFSET
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial(chdb)]
async fn order_by_time_desc_with_limit() {
    let ctx = TestContext::new().unwrap();
    ctx.metadata.create_database("db").await.unwrap();
    ctx.write_and_flush(
        "db",
        &format!("m v=1.0 {}\nm v=2.0 {}\nm v=3.0 {}", S, 2 * S, 3 * S),
    )
    .await
    .unwrap();

    let resp = ctx
        .query(
            "db",
            "SELECT mean(v) AS a FROM m GROUP BY time(1s) ORDER BY time DESC LIMIT 2",
        )
        .await
        .unwrap();
    let s = &series_of(&resp)[0];
    assert_eq!(s.values.len(), 2, "LIMIT 2");
    assert_eq!(fval(s, 0, "a"), 3.0, "DESC: newest first");
    assert_eq!(fval(s, 1, "a"), 2.0);
}

// ---------------------------------------------------------------------------
// WHERE boolean combinations
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial(chdb)]
async fn where_or_across_tags_then_aggregate() {
    let ctx = TestContext::new().unwrap();
    ctx.metadata.create_database("db").await.unwrap();
    ctx.write_and_flush(
        "db",
        &format!(
            "m,host=a v=1.0 {}\nm,host=b v=1.0 {}\nm,host=c v=1.0 {}",
            S, S, S
        ),
    )
    .await
    .unwrap();

    let resp = ctx
        .query(
            "db",
            "SELECT count(v) AS c FROM m WHERE host='a' OR host='b'",
        )
        .await
        .unwrap();
    let s = &series_of(&resp)[0];
    assert_eq!(fval(s, 0, "c"), 2.0, "only host a and b counted");
}

// ---------------------------------------------------------------------------
// epoch formatting × GROUP BY time
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial(chdb)]
async fn epoch_ms_with_group_by_time_returns_bucket_millis() {
    let ctx = TestContext::new().unwrap();
    ctx.metadata.create_database("db").await.unwrap();
    ctx.write_and_flush("db", &format!("m v=1.0 {}", 5 * S))
        .await
        .unwrap();

    let resp = ctx
        .query_service
        .execute_query(
            "db",
            "SELECT mean(v) FROM m GROUP BY time(1s)",
            Some("ms"),
            None,
            None,
        )
        .await
        .unwrap();
    let s = &series_of(&resp)[0];
    let ti = idx(s, "time");
    let ts = s.values[0][ti].as_i64().unwrap();
    assert_eq!(ts, 5000, "bucket start 5s = 5000ms");
}

// ---------------------------------------------------------------------------
// Multiple statements in one request
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial(chdb)]
async fn multiple_statements_return_multiple_results() {
    let ctx = TestContext::new().unwrap();
    ctx.metadata.create_database("db").await.unwrap();
    ctx.write_and_flush("db", &format!("m,host=a v=2.0 {}\nm,host=b v=4.0 {}", S, S))
        .await
        .unwrap();

    let resp = ctx
        .query(
            "db",
            "SELECT count(v) AS c FROM m; SELECT mean(v) AS a FROM m GROUP BY host",
        )
        .await
        .unwrap();
    assert_eq!(resp.results.len(), 2, "two statements -> two results");
    assert!(resp.results[0].error.is_none());
    assert!(resp.results[1].error.is_none());
    let first = resp.results[0].series.as_ref().unwrap();
    assert_eq!(fval(&first[0], 0, "c"), 2.0);
    let second = resp.results[1].series.as_ref().unwrap();
    assert_eq!(second.len(), 2, "grouped by host");
}

// ---------------------------------------------------------------------------
// SELECT * still projects time and tags
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial(chdb)]
async fn select_star_projects_time_field_and_keeps_one_series() {
    let ctx = TestContext::new().unwrap();
    ctx.metadata.create_database("db").await.unwrap();
    ctx.write_and_flush("db", &format!("m,host=a v=1.0 {}", S))
        .await
        .unwrap();

    let resp = ctx.query("db", "SELECT * FROM m").await.unwrap();
    let s = &series_of(&resp)[0];
    assert_eq!(s.columns[0], "time", "time first");
    assert!(s.columns.contains(&"v".to_string()), "field present");
    assert!(!s.values.is_empty());
}

/// KNOWN LIMITATION: `fill(linear)` currently carries the previous value forward
/// rather than performing true linear interpolation. ClickHouse `WITH FILL ...
/// INTERPOLATE (x AS x)` can only reference the previous filled value, not look
/// ahead to the next real point, so genuine linear interpolation is not
/// expressible this way. This test pins the *actual* behaviour so a future fix
/// (e.g. a window-function pass over the filled grid) updates it deliberately.
///
/// For data 10@1s and 30@3s grouped by time(1s): InfluxDB yields 20 at the 2s
/// gap; we currently yield 10 (carry-forward).
#[tokio::test]
#[serial(chdb)]
async fn fill_linear_is_currently_carry_forward_known_limitation() {
    let ctx = TestContext::new().unwrap();
    ctx.metadata.create_database("db").await.unwrap();
    ctx.write_and_flush("db", &format!("m v=10.0 {}\nm v=30.0 {}", S, 3 * S))
        .await
        .unwrap();
    let resp = ctx
        .query(
            "db",
            &format!(
                "SELECT mean(v) AS a FROM m WHERE time >= {} AND time <= {} GROUP BY time(1s) fill(linear)",
                S, 3 * S
            ),
        )
        .await
        .unwrap();
    let s = &series_of(&resp)[0];
    assert_eq!(fval(s, 0, "a"), 10.0);
    assert_eq!(
        fval(s, 1, "a"),
        10.0,
        "carry-forward (known limitation); true linear interpolation would be 20.0"
    );
    assert_eq!(fval(s, 2, "a"), 30.0);
}

// ---------------------------------------------------------------------------
// Schema evolution & mixed field types (the Telegraf `system` measurement bug)
// ---------------------------------------------------------------------------

/// The full Telegraf `system` line mixes floats, ints and a string field. A
/// freshly-created table must store every field, not just the string one.
#[tokio::test]
#[serial(chdb)]
async fn mixed_type_system_measurement_stores_all_fields() {
    let ctx = TestContext::new().unwrap();
    ctx.metadata.create_database("db").await.unwrap();
    ctx.write_and_flush(
        "db",
        &format!(
            "system,host=h load1=0.5,load5=0.4,load15=0.3,n_cpus=4i,n_users=2i,uptime=99i,uptime_format=\"3:25\" {}",
            S
        ),
    )
    .await
    .unwrap();

    let resp = ctx.query("db", "SELECT * FROM system").await.unwrap();
    let s = &series_of(&resp)[0];
    assert_eq!(fval(s, 0, "load1"), 0.5);
    assert_eq!(fval(s, 0, "load15"), 0.3);
    assert_eq!(fval(s, 0, "load5"), 0.4);
    assert_eq!(fval(s, 0, "n_cpus"), 4.0);
    assert_eq!(fval(s, 0, "uptime"), 99.0);
    let uf = idx(s, "uptime_format");
    assert_eq!(s.values[0][uf].as_str(), Some("3:25"));
}

/// Regression: a measurement whose schema evolves (a field registered first,
/// others added later via ALTER) must still store each field's value in its own
/// column. ALTER appends columns, so the table's physical order diverges from the
/// alphabetical Arrow batch order; the Arrow insert must therefore map columns by
/// NAME. Previously it mapped by position, scattering values into wrong-typed
/// columns (numerics silently became NULL while the string field "worked").
#[tokio::test]
#[serial(chdb)]
async fn schema_evolution_keeps_columns_aligned_by_name() {
    let ctx = TestContext::new().unwrap();
    ctx.metadata.create_database("db").await.unwrap();
    // First write creates the table with only the string field (sorts LAST).
    ctx.write_and_flush("db", &format!("system,host=h uptime_format=\"3:25\" {}", S))
        .await
        .unwrap();
    // Later write adds numeric columns that sort BEFORE uptime_format.
    ctx.write_and_flush(
        "db",
        &format!(
            "system,host=h load1=0.5,n_cpus=4i,uptime_format=\"3:26\" {}",
            2 * S
        ),
    )
    .await
    .unwrap();

    let resp = ctx.query("db", "SELECT * FROM system").await.unwrap();
    let s = &series_of(&resp)[0];
    // Find the row for the second write (load1 present).
    let row = (0..s.values.len())
        .find(|&r| !is_null(s, r, "load1"))
        .expect("second write row present");
    assert_eq!(fval(s, row, "load1"), 0.5, "load1 must hold its own value");
    assert_eq!(
        fval(s, row, "n_cpus"),
        4.0,
        "n_cpus must hold its own value"
    );
    let uf = idx(s, "uptime_format");
    assert_eq!(
        s.values[row][uf].as_str(),
        Some("3:26"),
        "string field must not receive a numeric value"
    );
}

/// Telegraf sends `system` as three partial lines per interval; they must coalesce
/// so load and uptime fields are queryable together.
#[tokio::test]
#[serial(chdb)]
async fn telegraf_system_partial_lines_coalesce() {
    let ctx = TestContext::new().unwrap();
    ctx.metadata.create_database("db").await.unwrap();
    let ts = S;
    ctx.write_and_flush(
        "db",
        &format!(
            "system,host=h load1=0.5,load5=0.4,load15=0.3,n_cpus=4i {ts}\n\
             system,host=h uptime=16697u {ts}\n\
             system,host=h uptime_format=\"3:25\" {ts}"
        ),
    )
    .await
    .unwrap();

    let resp = ctx
        .query("db", "SELECT load1, uptime, uptime_format FROM system")
        .await
        .unwrap();
    let s = &series_of(&resp)[0];
    assert_eq!(fval(s, 0, "load1"), 0.5);
    assert_eq!(fval(s, 0, "uptime"), 16697.0);
    let uf = idx(s, "uptime_format");
    assert_eq!(s.values[0][uf].as_str(), Some("3:25"));
}

/// Integer then unsigned for the same field widens to UInt64 without rejecting.
#[tokio::test]
#[serial(chdb)]
async fn uptime_integer_then_unsigned_widens() {
    let ctx = TestContext::new().unwrap();
    ctx.metadata.create_database("db").await.unwrap();
    ctx.write_and_flush("db", &format!("system,host=h uptime=99i {}", S))
        .await
        .unwrap();
    ctx.write_and_flush("db", &format!("system,host=h uptime=16697u {}", 2 * S))
        .await
        .unwrap();

    let meta = ctx
        .metadata
        .get_measurement("db", "autogen", "system")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(meta.field_types.get("uptime"), Some(&2));

    let resp = ctx
        .query("db", "SELECT uptime FROM system ORDER BY time")
        .await
        .unwrap();
    let s = &series_of(&resp)[0];
    assert_eq!(fval(s, 0, "uptime"), 99.0);
    assert_eq!(fval(s, 1, "uptime"), 16697.0);
}

/// A sparse first write (string only) followed by a full write must not hit
/// column-count mismatch on flush.
#[tokio::test]
#[serial(chdb)]
async fn partial_then_full_system_write_no_column_mismatch() {
    let ctx = TestContext::new().unwrap();
    ctx.metadata.create_database("db").await.unwrap();
    ctx.write_and_flush("db", &format!("system,host=h uptime_format=\"3:25\" {}", S))
        .await
        .unwrap();
    ctx.write_and_flush(
        "db",
        &format!(
            "system,host=h load1=0.5,load5=0.4,load15=0.3,n_cpus=4i,uptime=16697u,uptime_format=\"3:26\" {}",
            2 * S
        ),
    )
    .await
    .unwrap();

    let resp = ctx
        .query(
            "db",
            "SELECT load1, uptime_format FROM system ORDER BY time",
        )
        .await
        .unwrap();
    let s = &series_of(&resp)[0];
    assert_eq!(s.values.len(), 2);
    let row_with_load = (0..s.values.len())
        .find(|&r| !is_null(s, r, "load1"))
        .expect("full write row present");
    assert_eq!(fval(s, row_with_load, "load1"), 0.5);
}

// ---------------------------------------------------------------------------
// Storage/visibility behaviours (not TimeseriesQL translation)
// ---------------------------------------------------------------------------

/// Data written across many separate flushes is fully visible — no gaps.
#[tokio::test]
#[serial(chdb)]
async fn data_across_multiple_flushes_has_no_gaps() {
    let ctx = TestContext::new().unwrap();
    ctx.metadata.create_database("db").await.unwrap();
    let base: i64 = 1_781_687_070_000;
    for batch in 0..6i64 {
        let mut lp = String::new();
        for j in 0..5i64 {
            let i = batch * 5 + j;
            lp.push_str(&format!(
                "system,host=h load1={}.0 {}\n",
                i,
                (base + i * 10_000) * 1_000_000
            ));
        }
        ctx.write_and_flush("db", &lp).await.unwrap();
    }
    let q = format!(
        "SELECT mean(\"load1\") AS l FROM \"system\" WHERE \"host\" =~ /^(h)$/ \
         AND time >= {}ms and time <= {}ms GROUP BY time(10s), \"host\" fill(null) ORDER BY time ASC",
        base,
        base + 29 * 10_000
    );
    let resp = ctx
        .query_service
        .execute_query("db", &q, Some("ms"), None, None)
        .await
        .unwrap();
    let s = &series_of(&resp)[0];
    let non_null = s.values.iter().filter(|r| !r[1].is_null()).count();
    assert_eq!(
        non_null, 30,
        "all 30 buckets across 6 flushes must be present"
    );
}

/// Documents current behaviour: queries read chDB only, so data still in the WAL
/// (not yet flushed) is invisible until a flush. Pins the boundary so a future
/// WAL-merging read path updates this deliberately.
#[tokio::test]
#[serial(chdb)]
async fn unflushed_data_is_not_visible_until_flush() {
    use hyperbytedb::ports::ingestion::{IngestionPort, WritePayloadFormat};
    let ctx = TestContext::new().unwrap();
    ctx.metadata.create_database("db").await.unwrap();
    let lp = format!("system,host=h load1=1.0 {}", S);
    ctx.ingestion
        .ingest(
            "db",
            None,
            None,
            lp.as_bytes(),
            WritePayloadFormat::LineProtocol,
        )
        .await
        .unwrap();

    let q = "SELECT mean(load1) AS l FROM system GROUP BY time(10s)";
    let before = ctx.query("db", q).await.unwrap();
    assert!(
        before.results[0]
            .series
            .as_deref()
            .unwrap_or(&[])
            .is_empty(),
        "unflushed data is not visible to queries"
    );

    ctx.flush_service.flush().await.unwrap();
    let after = ctx.query("db", q).await.unwrap();
    assert_eq!(
        series_of(&after)[0]
            .values
            .iter()
            .filter(|r| !r[1].is_null())
            .count(),
        1
    );
}

/// The user's `swap` case: mixed int/float fields, schema evolved so the float
/// `used_percent` is misaligned under positional insert. The Grafana-shaped query
/// must return the real values, not garbage from a neighbouring column.
#[tokio::test]
#[serial(chdb)]
async fn swap_used_percent_survives_schema_evolution() {
    let ctx = TestContext::new().unwrap();
    ctx.metadata.create_database("db").await.unwrap();
    let base: i64 = 1_781_690_060_000;
    // Create the table with only the float field (sorts AFTER the int fields).
    ctx.write_and_flush(
        "db",
        &format!("swap,host=h used_percent=10.0 {}", base * 1_000_000),
    )
    .await
    .unwrap();
    // Later: add int fields that sort BEFORE used_percent + a new used_percent value.
    ctx.write_and_flush(
        "db",
        &format!(
            "swap,host=h free=100i,total=200i,used=50i,used_percent=25.0 {}",
            (base + 10_000) * 1_000_000
        ),
    )
    .await
    .unwrap();

    let q = format!(
        "SELECT mean(\"used_percent\") AS \"used %\" FROM \"swap\" \
         WHERE \"host\" =~ /^(h)$/ AND time >= {}ms and time <= {}ms \
         GROUP BY time(10s), \"host\" fill(null) ORDER BY time ASC",
        base,
        base + 10_000
    );
    let resp = ctx
        .query_service
        .execute_query("db", &q, Some("ms"), None, None)
        .await
        .unwrap();
    let s = &series_of(&resp)[0];
    let vals: Vec<f64> = (0..s.values.len())
        .filter(|&r| !is_null(s, r, "used %"))
        .map(|r| fval(s, r, "used %"))
        .collect();
    assert_eq!(
        vals,
        vec![10.0, 25.0],
        "used_percent must hold its own values"
    );
}
