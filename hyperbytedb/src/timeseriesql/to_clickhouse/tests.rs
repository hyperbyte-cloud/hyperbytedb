use super::conditions::quote_identifier;
use super::*;
use crate::domain::chdb_naming::QuotedTableName;
use crate::domain::column_mapping::ColumnMapping;
use crate::timeseriesql::parser;

fn test_table() -> QuotedTableName {
    QuotedTableName::new_quoted("`mydb_autogen_cpu`".to_string())
}

fn test_series_table() -> QuotedTableName {
    QuotedTableName::new_quoted("`mydb_autogen_cpu_series`".to_string())
}

fn qname(s: &str) -> QuotedTableName {
    QuotedTableName::new_quoted(s.to_string())
}

fn translate_test(stmt: &SelectStatement) -> String {
    translate_native_table(stmt, test_table().as_str(), None, None, None).unwrap()
}

/// Mapping with `host` as a tag and `usage_idle` as a field (no collision).
fn cpu_mapping() -> ColumnMapping {
    ColumnMapping {
        tag_keys: ["host", "region"].into_iter().map(String::from).collect(),
        field_names: ["usage_idle"].into_iter().map(String::from).collect(),
        ..Default::default()
    }
}

fn translate_series(stmt: &SelectStatement, m: &ColumnMapping) -> String {
    let table = test_table();
    let series = test_series_table();
    translate_native_table(
        stmt,
        table.as_str(),
        Some(m),
        Some(SeriesJoin {
            table: &series,
            force: false,
            tag_columns: &[],
        }),
        None,
    )
    .unwrap()
}

fn parse_select(q: &str) -> SelectStatement {
    let stmts = parser::parse_query(q).unwrap();
    match stmts.into_iter().next().unwrap() {
        Statement::Select(s) => s,
        _ => panic!("expected SELECT statement"),
    }
}

#[test]
fn group_by_tag_uses_physical_column_name() {
    let mut map = ColumnMapping::default();
    map.tag_keys.insert("host-name".into());
    map.field_names.insert("v".into());
    let stmt = parse_select(r#"SELECT mean("v") FROM m GROUP BY time(1m), "host-name""#);
    let sql = translate_series(&stmt, &map);
    assert!(
        sql.contains("\"host_name\""),
        "tag with punctuation must map to sanitized physical column, got: {sql}"
    );
}

#[test]
fn quote_identifier_rejects_control_characters() {
    assert!(quote_identifier("host\ninject").is_err());
    assert!(quote_identifier("ok_name").is_ok());
}

#[test]
fn test_select_star() {
    let stmt = parse_select("SELECT * FROM cpu");
    let sql = translate_test(&stmt);
    assert!(sql.contains("SELECT *"));
    assert!(sql.contains("FROM `mydb_autogen_cpu`"));
}

#[test]
fn test_mean() {
    let stmt = parse_select(r#"SELECT mean("value") FROM cpu"#);
    let sql = translate_test(&stmt);
    assert!(sql.contains("avg(\"value\")"));
}

#[test]
fn test_median_count_sum_min_max() {
    let stmt =
        parse_select(r#"SELECT median("x"), count("x"), sum("x"), min("x"), max("x") FROM m"#);
    let sql = translate_test(&stmt);
    // InfluxQL median averages the two middle samples on even counts.
    assert!(sql.contains("quantileExactInclusive(0.5)(\"x\")"));
    assert!(sql.contains("count(\"x\")"));
    assert!(sql.contains("sum(\"x\")"));
    assert!(sql.contains("min(\"x\")"));
    assert!(sql.contains("max(\"x\")"));
}

#[test]
fn test_first_last() {
    let stmt = parse_select(r#"SELECT first("v"), last("v") FROM m"#);
    let sql = translate_test(&stmt);
    assert!(sql.contains("argMin(\"v\", time)"));
    assert!(sql.contains("argMax(\"v\", time)"));
}

#[test]
fn test_percentile() {
    let stmt = parse_select(r#"SELECT percentile("value", 95) FROM m"#);
    let sql = translate_test(&stmt);
    // Nearest-rank sample percentile, matching InfluxQL.
    assert!(sql.contains("quantileExactLow(0.95)(\"value\")"));
}

#[test]
fn test_spread_stddev_mode_distinct() {
    let stmt = parse_select(r#"SELECT spread("v"), stddev("v"), mode("v"), distinct("v") FROM m"#);
    let sql = translate_test(&stmt);
    assert!(sql.contains("(max(\"v\") - min(\"v\"))"));
    // InfluxQL stddev is sample stddev.
    assert!(sql.contains("stddevSamp(\"v\")"));
    // mode() must be a scalar, not a one-element Array.
    assert!(sql.contains("arrayElement(topKWeighted(1)(\"v\", 1), 1)"));
    // distinct() must stay valid inside GROUP BY time(); SELECT DISTINCT is not.
    assert!(sql.contains("arrayJoin(groupUniqArray(\"v\"))"));
    assert!(!sql.contains("DISTINCT \"v\""));
}

#[test]
fn test_count_distinct() {
    let stmt = parse_select(r#"SELECT count(distinct("v")) FROM m GROUP BY time(1m)"#);
    let sql = translate_test(&stmt);
    assert!(
        sql.contains("uniqExact(\"v\")"),
        "count(distinct(v)) should translate to uniqExact, got: {sql}"
    );
}

#[test]
fn test_distinct_with_group_by_time_is_valid_expression() {
    let stmt = parse_select(r#"SELECT distinct("v") FROM m GROUP BY time(1m)"#);
    let sql = translate_test(&stmt);
    assert!(
        sql.contains("arrayJoin(groupUniqArray(\"v\"))"),
        "distinct(v) must be an expression usable with GROUP BY time, got: {sql}"
    );
    assert!(!sql.contains("DISTINCT "), "got: {sql}");
}

#[test]
fn test_where_time_and_tag() {
    let stmt = parse_select(r#"SELECT * FROM cpu WHERE "host" = 'server01' AND time > now() - 1h"#);
    let sql = translate_test(&stmt);
    assert!(sql.contains("WHERE"));
    assert!(sql.contains("host"));
    assert!(sql.contains("server01"));
    assert!(sql.contains("time"));
    assert!(sql.contains("now64()"));
    assert!(sql.contains("INTERVAL 1 HOUR"));
}

#[test]
fn test_where_regex() {
    let stmt = parse_select(r#"SELECT * FROM m WHERE "region" =~ /us-.*/"#);
    let sql = translate_test(&stmt);
    assert!(sql.contains("match"));
    assert!(sql.contains("us-.*"));
}

#[test]
fn test_group_by_time() {
    let stmt = parse_select(r#"SELECT mean("value") FROM cpu GROUP BY time(5m)"#);
    let sql = translate_test(&stmt);
    assert!(sql.contains("GROUP BY"));
    assert!(sql.contains("toStartOfInterval(time, INTERVAL 5 MINUTE)"));
}

#[test]
fn test_group_by_time_with_offset() {
    let stmt = parse_select(r#"SELECT mean("value") FROM cpu GROUP BY time(1h, 15m)"#);
    let sql = translate_test(&stmt);
    assert!(sql.contains(
        "toStartOfInterval(time - INTERVAL 15 MINUTE, INTERVAL 1 HOUR) + INTERVAL 15 MINUTE"
    ));
}

#[test]
fn test_group_by_time_and_tags() {
    let stmt = parse_select(r#"SELECT mean("value") FROM cpu GROUP BY time(5m), "host", "region""#);
    let sql = translate_test(&stmt);
    assert!(sql.contains("toStartOfInterval(time, INTERVAL 5 MINUTE)"));
    assert!(sql.contains("\"host\""));
    assert!(sql.contains("\"region\""));
    // Tag columns must appear in SELECT for result splitting
    let select_line = sql.lines().next().unwrap();
    assert!(
        select_line.contains("\"host\""),
        "SELECT must include tag columns, got: {}",
        select_line
    );
    assert!(
        select_line.contains("\"region\""),
        "SELECT must include tag columns, got: {}",
        select_line
    );
}

#[test]
fn test_fill_null() {
    let stmt = parse_select(r#"SELECT mean("value") FROM cpu GROUP BY time(5m) fill(null)"#);
    let sql = translate_test(&stmt);
    assert!(
        !sql.contains("ifNull"),
        "fill(null) must not coerce NULL to 0, got: {sql}"
    );
    assert!(sql.contains("avg(\"value\")"));
    assert!(sql.contains("WITH FILL STEP INTERVAL 5 MINUTE"));
}

#[test]
fn test_fill_null_with_time_bounds_uses_from_to() {
    let stmt = parse_select(
        r#"SELECT mean("load1") FROM "system" WHERE time >= 1781541739132ms AND time <= 1781552539132ms GROUP BY time(10s) fill(null)"#,
    );
    let min = 1_781_541_739_132_000_000i64;
    let max = 1_781_552_539_132_000_000i64;
    let sql = translate_native_table(
        &stmt,
        test_table().as_str(),
        None,
        None,
        Some((Some(min), Some(max))),
    )
    .unwrap();
    assert!(
        sql.contains("WITH FILL FROM toStartOfInterval(fromUnixTimestamp64Nano(1781541739132000000), INTERVAL 10 SECOND)"),
        "expected FROM bound aligned to bucket, got: {sql}"
    );
    // WITH FILL ... TO is exclusive: the anchor extends one step past the
    // bucket containing the upper bound so the final bucket is generated.
    assert!(
        sql.contains("TO toStartOfInterval(fromUnixTimestamp64Nano(1781552539132000000), INTERVAL 10 SECOND) + INTERVAL 10 SECOND"),
        "expected TO bound one step past the last bucket, got: {sql}"
    );
    assert!(
        sql.contains("STEP INTERVAL 10 SECOND"),
        "expected STEP after FROM/TO, got: {sql}"
    );
}

#[test]
fn test_fill_grid_anchors_use_group_by_time_offset() {
    // `time(1m, 30s)` bucket expression is `toStartOfInterval(t - 30s, 1m) + 30s`;
    // the WITH FILL FROM/TO anchors must use the same shape or the grid
    // interleaves phantom buckets between real ones.
    let stmt = parse_select(
        r#"SELECT mean("v") FROM m WHERE time >= 1781541730000ms AND time <= 1781541790000ms GROUP BY time(1m, 30s) fill(null)"#,
    );
    let min = 1_781_541_730_000_000_000i64;
    let max = 1_781_541_790_000_000_000i64;
    let sql = translate_native_table(
        &stmt,
        test_table().as_str(),
        None,
        None,
        Some((Some(min), Some(max))),
    )
    .unwrap();
    assert!(
        sql.contains(
            "WITH FILL FROM toStartOfInterval(fromUnixTimestamp64Nano(1781541730000000000) - INTERVAL 30 SECOND, INTERVAL 1 MINUTE) + INTERVAL 30 SECOND"
        ),
        "FROM anchor must apply the GROUP BY time offset, got: {sql}"
    );
    assert!(
        sql.contains(
            "TO toStartOfInterval(fromUnixTimestamp64Nano(1781541790000000000) - INTERVAL 30 SECOND, INTERVAL 1 MINUTE) + INTERVAL 30 SECOND + INTERVAL 1 MINUTE"
        ),
        "TO anchor must apply the GROUP BY time offset and extend one step, got: {sql}"
    );
}

#[test]
fn test_fill_with_group_by_tag_orders_tag_before_time() {
    // fill() + GROUP BY tag must order the tag column *before* the
    // time-fill column so ClickHouse fills each tag group independently.
    // Otherwise WITH FILL emits gap rows with an empty tag value (a phantom
    // all-NULL series) and never fills the real per-tag series.
    let stmt =
        parse_select(r#"SELECT mean("usage_idle") FROM cpu GROUP BY time(10s), "host" fill(null)"#);
    let sql = translate_series(&stmt, &cpu_mapping());
    assert!(
        sql.contains(
            "ORDER BY \"host\" ASC, toStartOfInterval(time, INTERVAL 10 SECOND) ASC WITH FILL"
        ),
        "tag must precede the time-fill column in ORDER BY, got: {sql}"
    );
}

#[test]
fn test_raw_select_projects_time_and_orders_ascending() {
    // Raw (non-aggregate) selects must carry `time` and default to time ASC,
    // matching InfluxDB. Without this, points come back in storage order.
    let stmt = parse_select(r#"SELECT "load1", "load5" FROM system"#);
    let sql = translate_test(&stmt);
    assert!(
        sql.starts_with("SELECT \"time\","),
        "raw select must project time first, got: {sql}"
    );
    assert!(
        sql.contains("ORDER BY time ASC"),
        "raw select defaults to time ASC, got: {sql}"
    );
}

#[test]
fn test_group_by_time_defaults_to_order_by_time_ascending() {
    let stmt = parse_select(r#"SELECT mean("value") FROM cpu GROUP BY time(5m)"#);
    let sql = translate_test(&stmt);
    assert!(
        sql.contains("ORDER BY toStartOfInterval(time, INTERVAL 5 MINUTE) ASC"),
        "GROUP BY time defaults to time ASC, got: {sql}"
    );
}

#[test]
fn test_aggregate_without_group_by_time_has_no_order_by() {
    // Collapses to a single row — no ORDER BY (and no raw `time` column).
    let stmt = parse_select(r#"SELECT mean("value") FROM cpu"#);
    let sql = translate_test(&stmt);
    assert!(!sql.contains("ORDER BY"), "got: {sql}");
    assert!(!sql.contains("\"time\""), "no raw time column, got: {sql}");
}

#[test]
fn test_select_star_orders_by_time_without_duplicate_time() {
    let stmt = parse_select("SELECT * FROM cpu");
    let sql = translate_test(&stmt);
    assert!(sql.starts_with("SELECT *"), "got: {sql}");
    assert!(sql.contains("ORDER BY time ASC"), "got: {sql}");
}

#[test]
fn test_fill_value() {
    let stmt = parse_select(r#"SELECT mean("value") FROM cpu GROUP BY time(5m) fill(0)"#);
    let sql = translate_test(&stmt);
    assert!(sql.contains("ifNull(avg(\"value\"), 0)"));
    assert!(sql.contains("WITH FILL"));
    // ifNull only reaches existing rows; WITH FILL-generated rows need a
    // constant INTERPOLATE or they surface as column defaults, not the value.
    assert!(
        sql.contains("INTERPOLATE (\"mean_value\" AS 0)"),
        "fill(N) must INTERPOLATE generated rows with N, got: {sql}"
    );
}

#[test]
fn test_fill_value_interpolates_every_field_alias() {
    let stmt =
        parse_select(r#"SELECT mean("a") AS x, max("b") AS y FROM m GROUP BY time(1m) fill(100)"#);
    let sql = translate_test(&stmt);
    assert!(
        sql.contains("INTERPOLATE (\"x\" AS 100, \"y\" AS 100)"),
        "fill(100) must INTERPOLATE all field aliases, got: {sql}"
    );
}

#[test]
fn test_missing_fill_defaults_to_fill_null() {
    // InfluxQL: a GROUP BY time() query without fill() behaves as fill(null).
    let stmt = parse_select(r#"SELECT mean("value") FROM cpu GROUP BY time(5m)"#);
    let sql = translate_test(&stmt);
    assert!(
        sql.contains("WITH FILL STEP INTERVAL 5 MINUTE"),
        "absent fill() must default to fill(null), got: {sql}"
    );
    assert!(
        !sql.contains("ifNull"),
        "default fill must leave NULL aggregates as NULL, got: {sql}"
    );
    assert!(
        !sql.contains("INTERPOLATE"),
        "default fill must not interpolate, got: {sql}"
    );
}

#[test]
fn test_select_into_does_not_default_fill() {
    // Writes must not insert synthetic NULL grid rows.
    let stmt = parse_select(r#"SELECT mean("value") INTO "dest" FROM "cpu" GROUP BY time(5m)"#);
    let sql = translate_select_into(&stmt, &qname("`dest`"), test_table().as_str(), None).unwrap();
    assert!(
        !sql.contains("WITH FILL"),
        "SELECT INTO without fill() must not emit WITH FILL, got: {sql}"
    );
}

#[test]
fn test_order_by_time_desc_with_fill_wraps_ascending_fill() {
    // WITH FILL on a DESC column generates nothing against ascending
    // FROM/TO anchors; the fill happens ascending in an inner SELECT and an
    // outer SELECT re-orders descending.
    let stmt = parse_select(
        r#"SELECT mean("value") FROM cpu GROUP BY time(5m) fill(null) ORDER BY time DESC"#,
    );
    let sql = translate_test(&stmt);
    assert!(
        sql.starts_with("SELECT * FROM (\n"),
        "DESC + fill must wrap, got: {sql}"
    );
    assert!(
        sql.contains(" ASC WITH FILL"),
        "inner fill must be ascending, got: {sql}"
    );
    assert!(
        sql.contains(") ORDER BY __time DESC"),
        "outer must re-order descending, got: {sql}"
    );
}

#[test]
fn test_order_by_time_desc_with_fill_and_tags_orders_tags_first() {
    let stmt = parse_select(
        r#"SELECT mean("usage_idle") FROM cpu GROUP BY time(10s), "host" fill(null) ORDER BY time DESC"#,
    );
    let sql = translate_series(&stmt, &cpu_mapping());
    assert!(
        sql.contains(") ORDER BY \"host\" ASC, __time DESC"),
        "outer ordering must keep tags first, got: {sql}"
    );
}

#[test]
fn test_fill_none() {
    let stmt = parse_select(r#"SELECT mean("value") FROM cpu GROUP BY time(5m) fill(none)"#);
    let sql = translate_test(&stmt);
    assert!(!sql.contains("ifNull"));
    assert!(!sql.contains("WITH FILL"));
}

#[test]
fn test_limit_offset() {
    let stmt = parse_select("SELECT * FROM cpu LIMIT 10 OFFSET 5");
    let sql = translate_test(&stmt);
    assert!(sql.contains("LIMIT 10"));
    assert!(sql.contains("OFFSET 5"));
}

#[test]
fn test_order_by_desc() {
    let stmt =
        parse_select(r#"SELECT mean("value") FROM cpu GROUP BY time(5m) ORDER BY time DESC"#);
    let sql = translate_test(&stmt);
    assert!(sql.contains("ORDER BY"));
    assert!(sql.contains("DESC"));
}

#[test]
fn test_derivative() {
    let stmt = parse_select(r#"SELECT derivative("value", 1s) FROM cpu"#);
    let sql = translate_test(&stmt);
    assert!(
        sql.contains("lagInFrame"),
        "expected lagInFrame, got: {sql}"
    );
    assert!(
        sql.contains("toFloat64"),
        "expected toFloat64 time conversion, got: {sql}"
    );
    assert!(
        !sql.contains("PARTITION BY"),
        "no tags = no PARTITION BY, got: {sql}"
    );
}

#[test]
fn test_non_negative_derivative() {
    let stmt = parse_select(r#"SELECT non_negative_derivative("value", 1s) FROM cpu"#);
    let sql = translate_test(&stmt);
    assert!(
        sql.contains("if("),
        "expected if() for non-negative check, got: {sql}"
    );
    assert!(sql.contains(">= 0"), "expected >= 0 check, got: {sql}");
    assert!(
        sql.contains("lagInFrame"),
        "expected lagInFrame, got: {sql}"
    );
    assert!(
        sql.contains("toFloat64"),
        "expected toFloat64 time conversion, got: {sql}"
    );
}

#[test]
fn test_difference() {
    let stmt = parse_select(r#"SELECT difference("value") FROM cpu"#);
    let sql = translate_test(&stmt);
    assert!(sql.contains("lagInFrame"));
    assert!(!sql.contains("if("));
}

#[test]
fn test_nested_aggregate_in_derivative() {
    let stmt = parse_select(
        r#"SELECT non_negative_derivative(mean("reads"), 1s) FROM "diskio" WHERE time >= 1000ms GROUP BY time(10s), "host" fill(null)"#,
    );
    let sql = translate_test(&stmt);
    assert!(
        sql.contains("avg(\"reads\")"),
        "expected avg(reads), got: {sql}"
    );
    assert!(
        sql.contains("ORDER BY __time"),
        "expected ORDER BY __time, got: {sql}"
    );
    assert!(
        sql.contains("lagInFrame"),
        "expected lagInFrame, got: {sql}"
    );
    assert!(
        sql.contains(">= 0"),
        "expected non-negative check, got: {sql}"
    );
    assert!(
        sql.contains("PARTITION BY \"host\""),
        "GROUP BY tag must produce PARTITION BY in window clause, got: {sql}"
    );
    assert!(
        sql.contains("toFloat64"),
        "expected toFloat64 time conversion, got: {sql}"
    );
    let select_line = sql.lines().next().unwrap();
    assert!(
        select_line.contains("\"host\""),
        "expected host in SELECT, got: {select_line}"
    );
}

#[test]
fn test_derivative_with_nested_first() {
    let stmt = parse_select(
        r#"SELECT derivative(first("bytes_recv"), 1s) * 8 FROM net GROUP BY time(10s) fill(null)"#,
    );
    let sql = translate_test(&stmt);
    // first() → argMin(field, time)
    assert!(
        sql.contains("argMin(\"bytes_recv\", time)"),
        "expected argMin, got: {sql}"
    );
    assert!(
        sql.contains("ORDER BY __time"),
        "expected ORDER BY __time, got: {sql}"
    );
}

#[test]
fn test_moving_average() {
    let stmt = parse_select(r#"SELECT moving_average("value", 5) FROM cpu"#);
    let sql = translate_test(&stmt);
    assert!(sql.contains("avg(\"value\") OVER"));
    assert!(sql.contains("ROWS BETWEEN 4 PRECEDING AND CURRENT ROW"));
    // InfluxQL emits values only once the window holds N points.
    assert!(
        sql.contains("if(count(\"value\") OVER"),
        "moving_average must gate on a full window, got: {sql}"
    );
    assert!(sql.contains(">= 5"), "window-full check, got: {sql}");
}

#[test]
fn test_cumulative_sum() {
    let stmt = parse_select(r#"SELECT cumulative_sum("value") FROM cpu"#);
    let sql = translate_test(&stmt);
    assert!(sql.contains("sum(\"value\") OVER"));
    assert!(sql.contains("ROWS UNBOUNDED PRECEDING"));
}

#[test]
fn test_elapsed() {
    let stmt = parse_select(r#"SELECT elapsed("value", 1s) FROM cpu"#);
    let sql = translate_test(&stmt);
    assert!(
        sql.contains("lagInFrame(toNullable(time), 1)"),
        "expected NULL-defaulting lagInFrame so the first row is omitted, got: {sql}"
    );
    assert!(
        sql.contains("toFloat64"),
        "expected toFloat64 time conversion, got: {sql}"
    );
}

#[test]
fn test_fill_previous() {
    let stmt = parse_select(
        r#"SELECT mean("value") AS avg_val FROM cpu GROUP BY time(5m) fill(previous)"#,
    );
    let sql = translate_test(&stmt);
    assert!(sql.contains("WITH FILL STEP INTERVAL 5 MINUTE"));
    assert!(sql.contains("INTERPOLATE"));
    assert!(sql.contains("\"avg_val\""));
    assert!(!sql.contains("ifNull"));
}

#[test]
fn test_fill_linear() {
    let stmt =
        parse_select(r#"SELECT mean("value") AS avg_val FROM cpu GROUP BY time(5m) fill(linear)"#);
    let sql = translate_test(&stmt);
    assert!(sql.contains("WITH FILL STEP INTERVAL 5 MINUTE"));
    assert!(sql.contains("INTERPOLATE"));
    assert!(sql.contains("\"avg_val\" AS \"avg_val\""));
    assert!(!sql.contains("ifNull"));
}

#[test]
fn test_grafana_tag_annotation() {
    let stmt = parse_select(
        r#"SELECT mean("usage_idle") FROM cpu WHERE time >= 1000ms AND time <= 2000ms GROUP BY time(1s), "host"::tag"#,
    );
    let sql = translate_test(&stmt);
    assert!(sql.contains("GROUP BY"));
    assert!(
        sql.contains("\"host\""),
        "should strip ::tag suffix, got: {sql}"
    );
    assert!(
        !sql.contains("::tag"),
        "should not contain ::tag, got: {sql}"
    );
}

#[test]
fn test_epoch_ms_time_comparison() {
    let stmt = parse_select(
        r#"SELECT * FROM cpu WHERE time >= 1772462462777ms AND time <= 1772466062777ms"#,
    );
    let sql = translate_test(&stmt);
    assert!(
        sql.contains("fromUnixTimestamp64Milli(1772462462777)"),
        "should convert ms epoch to timestamp, got: {sql}"
    );
    assert!(
        sql.contains("fromUnixTimestamp64Milli(1772466062777)"),
        "should convert ms epoch to timestamp, got: {sql}"
    );
    assert!(
        !sql.contains("INTERVAL"),
        "should not use INTERVAL for epoch timestamps, got: {sql}"
    );
}

#[test]
fn test_epoch_ns_time_comparison() {
    let stmt = parse_select(r#"SELECT * FROM cpu WHERE time >= 1772462462777000000"#);
    let sql = translate_test(&stmt);
    assert!(
        sql.contains("fromUnixTimestamp64Nano(1772462462777000000)"),
        "bare integer should become nanosecond timestamp, got: {sql}"
    );
}

#[test]
fn test_non_negative_derivative_with_multiple_tags() {
    let stmt = parse_select(
        r#"SELECT non_negative_derivative(mean("read_bytes"), 1s) AS "Reads", non_negative_derivative(mean("write_bytes"), 1s) AS "Writes" FROM "diskio" WHERE "host" =~ /^(8a8b7bfef1c0)$/ AND time >= 1772542183541ms AND time <= 1772542483541ms GROUP BY time(1s), "host", "name" fill(null)"#,
    );
    let sql = translate_test(&stmt);
    assert!(
        sql.contains(r#"PARTITION BY "host", "name""#),
        "window must PARTITION BY all GROUP BY tags to avoid cross-series derivative, got: {sql}"
    );
    assert!(
        sql.contains("toFloat64"),
        "time diff must use toFloat64 for correct arithmetic, got: {sql}"
    );
    assert!(
        sql.contains("avg(\"read_bytes\")"),
        "expected avg(read_bytes), got: {sql}"
    );
    assert!(
        sql.contains("avg(\"write_bytes\")"),
        "expected avg(write_bytes), got: {sql}"
    );
    assert!(
        sql.contains(">= 0"),
        "expected non-negative check, got: {sql}"
    );
    assert!(
        sql.contains("AS \"Reads\""),
        "expected Reads alias, got: {sql}"
    );
    assert!(
        sql.contains("AS \"Writes\""),
        "expected Writes alias, got: {sql}"
    );
}

#[test]
fn test_difference_with_tags_has_partition_by() {
    let stmt = parse_select(
        r#"SELECT difference(mean("value")) FROM cpu GROUP BY time(10s), "host", "region""#,
    );
    let sql = translate_test(&stmt);
    assert!(
        sql.contains(r#"PARTITION BY "host", "region""#),
        "difference window must PARTITION BY tags, got: {sql}"
    );
}

#[test]
fn test_moving_average_with_tags_has_partition_by() {
    let stmt = parse_select(
        r#"SELECT moving_average(mean("value"), 5) FROM cpu GROUP BY time(10s), "host""#,
    );
    let sql = translate_test(&stmt);
    assert!(
        sql.contains(r#"PARTITION BY "host""#),
        "moving_average window must PARTITION BY tags, got: {sql}"
    );
}

#[test]
fn test_cumulative_sum_with_tags_has_partition_by() {
    let stmt =
        parse_select(r#"SELECT cumulative_sum(mean("value")) FROM cpu GROUP BY time(10s), "host""#);
    let sql = translate_test(&stmt);
    assert!(
        sql.contains(r#"PARTITION BY "host""#),
        "cumulative_sum window must PARTITION BY tags, got: {sql}"
    );
}

#[test]
fn test_non_negative_difference_divided_by_constant() {
    let stmt = parse_select(
        r#"SELECT NON_NEGATIVE_DIFFERENCE(mean("packets_recv"))/10 AS "in", NON_NEGATIVE_DIFFERENCE(mean("packets_sent"))/10 AS "out" FROM "net" WHERE "host" =~ /^(telegraf-664c6bf94-pgt7t)$/ AND "interface" =~ /(vlan|eth|bond).*/ AND time >= 1772706604176ms AND time <= 1772706904176ms GROUP BY time(1s), "host", "interface" fill(null)"#,
    );
    let sql = translate_test(&stmt);
    assert!(
        sql.contains("lagInFrame"),
        "expected lagInFrame for difference, got: {sql}"
    );
    assert!(sql.contains("/ 10"), "expected division by 10, got: {sql}");
    assert!(sql.contains("AS \"in\""), "expected alias 'in', got: {sql}");
    assert!(
        sql.contains("AS \"out\""),
        "expected alias 'out', got: {sql}"
    );
    assert!(
        !sql.contains("NON_NEGATIVE_DIFFERENCE"),
        "should not contain raw TimeseriesQL function name in output SQL, got: {sql}"
    );
}

#[test]
fn test_derivative_unit_conversion() {
    let stmt = parse_select(r#"SELECT derivative("value", 1ms) FROM cpu GROUP BY time(10s)"#);
    let sql = translate_test(&stmt);
    assert!(
        sql.contains("/ 0.001"),
        "1ms unit should divide time diff by 0.001 seconds, got: {sql}"
    );
}

#[test]
fn test_relative_time_still_uses_interval() {
    let stmt = parse_select(r#"SELECT * FROM cpu WHERE time > now() - 1h"#);
    let sql = translate_test(&stmt);
    assert!(sql.contains("now64()"), "should keep now64(), got: {sql}");
    assert!(
        sql.contains("INTERVAL 1 HOUR"),
        "relative duration should stay as interval, got: {sql}"
    );
}

#[test]
fn test_translate_select_into() {
    let q = r#"SELECT mean("value") INTO "cpu_1h" FROM "cpu" WHERE "host" = 'server01' GROUP BY time(1h), "host""#;
    let stmt = parse_select(q);
    let sql = translate_select_into(
        &stmt,
        &qname("`mydb_autogen_cpu_1h`"),
        test_table().as_str(),
        None,
    )
    .unwrap();
    assert!(sql.starts_with("INSERT INTO `mydb_autogen_cpu_1h`"));
    assert!(sql.contains("SELECT "));
    assert!(sql.contains("time"));
    assert!(!sql.contains("__time"));
    assert!(sql.contains("avg(\"value\")"));
    assert!(sql.contains("GROUP BY"));
    assert!(sql.contains("toStartOfInterval(time, INTERVAL 1 HOUR)"));
}

#[test]
fn test_select_into_requires_group_by_time() {
    let q = r#"SELECT mean("value") INTO "cpu_1h" FROM "cpu""#;
    let stmt = parse_select(q);
    assert!(translate_select_into(&stmt, &qname("`dest`"), test_table().as_str(), None).is_err());
}

#[test]
fn test_translate_materialized_view_select() {
    let q = r#"SELECT mean("value") INTO "cpu_5m" FROM "cpu" GROUP BY time(5m), "host""#;
    let stmt = parse_select(q);
    let map = cpu_mapping();
    let sql = translate_materialized_view_select(
        &stmt,
        &test_table(),
        &test_series_table(),
        "cpu_5m",
        &map,
    )
    .unwrap();
    assert!(sql.starts_with("SELECT "));
    assert!(sql.contains("toStartOfInterval(t.time, INTERVAL 5 MINUTE) AS time"));
    assert!(sql.contains("any(t.`_mv_src_origin_node_id`) AS origin_node_id"));
    assert!(sql.contains("max(t.`_mv_src_ingest_seq`) AS ingest_seq"));
    assert!(sql.contains("sipHash64("));
    assert!(sql.contains("AS \"count_value\""));
    assert!(sql.contains("AS \"sum_value\""));
    assert!(!sql.contains("avg(\"value\")"));
    assert!(
        sql.contains("argMax(\"value\", `ingest_seq`)"),
        "MV source should coalesce duplicate raw rows before aggregating"
    );
    assert!(
        sql.contains("FROM (SELECT `series_id`, `time`, max(`ingest_seq`) AS `_mv_src_ingest_seq`"),
        "MV should read from coalesced source subquery, got: {sql}"
    );
    assert!(
        sql.contains("AS t ANY LEFT JOIN `mydb_autogen_cpu_series` AS s"),
        "MV must not drop fact rows whose series row hasn't landed, got: {sql}"
    );
    assert!(sql.contains("GROUP BY toStartOfInterval(t.time, INTERVAL 5 MINUTE)"));
    assert!(sql.contains("s.\"host\""));
    assert!(!sql.contains("INSERT INTO"));
    // Field columns must appear in sorted-by-name order (count < sum).
    let count_pos = sql.find("AS \"count_value\"").unwrap();
    let sum_pos = sql.find("AS \"sum_value\"").unwrap();
    assert!(
        count_pos < sum_pos,
        "fields should be sorted: count_value before sum_value, got: {}..{}",
        count_pos,
        sum_pos
    );
}

#[test]
fn materialized_view_backfill_orders_insert_columns_by_physical_name() {
    let q = r#"SELECT sum("players") AS "players", sum("max_players") AS "maxplayers", sum("cpu") AS "cpu" INTO "server_stats_1m" FROM "server_stats" GROUP BY time(1m), "host""#;
    let stmt = parse_select(q);
    let map = cpu_mapping();
    let sql = translate_materialized_view_backfill(
        &stmt,
        &qname("`dest`"),
        &qname("`source`"),
        &qname("`source_series`"),
        "server_stats_1m",
        &map,
    )
    .unwrap();
    assert!(
        sql.starts_with(
            "INSERT INTO `dest` (\"time\", \"origin_node_id\", \"ingest_seq\", \"series_id\", \"cpu\", \"maxplayers\", \"players\")"
        ),
        "backfill must name columns in DDL order, got: {sql}"
    );
    assert!(
        sql.contains("SELECT \"time\", \"origin_node_id\", \"ingest_seq\", \"series_id\", \"cpu\", \"maxplayers\", \"players\"\nFROM ("),
        "backfill outer SELECT must match INSERT column order, got: {sql}"
    );
}

#[test]
fn rollup_fact_view_uses_sum_for_additive_fields() {
    use crate::domain::rollup::RollupCombine;

    let mut map = cpu_mapping();
    map.field_rollups
        .insert("usage_idle".to_string(), RollupCombine::Sum);
    let sql = build_coalesced_fact_view(&test_table(), &map);
    assert!(
        sql.contains("sum(\"usage_idle\") AS \"usage_idle\""),
        "rollup fields should merge with sum(), got: {sql}"
    );
    assert!(
        !sql.contains("argMax(\"usage_idle\""),
        "rollup sum fields must not use argMax, got: {sql}"
    );
}

#[test]
fn raw_fact_view_still_uses_argmax_without_rollups() {
    let map = cpu_mapping();
    let sql = build_coalesced_fact_view(&test_table(), &map);
    assert!(
        sql.contains("argMax(\"usage_idle\", `ingest_seq`)"),
        "raw measurements should keep argMax coalesce, got: {sql}"
    );
}

#[test]
fn mean_on_rollup_measurement_rewrites_to_sum_over_count() {
    use crate::domain::rollup::{MeanRollupField, RollupCombine};

    let mut map = cpu_mapping();
    map.mean_fields.insert(
        "value".to_string(),
        MeanRollupField {
            sum_col: "sum_value".to_string(),
            count_col: "count_value".to_string(),
        },
    );
    map.field_rollups
        .insert("sum_value".to_string(), RollupCombine::Sum);
    map.field_rollups
        .insert("count_value".to_string(), RollupCombine::Sum);

    let stmt = parse_select(r#"SELECT mean("value") FROM cpu GROUP BY time(5m), "host""#);
    let table = test_table();
    let series = test_series_table();
    let sql = translate_native_table(
        &stmt,
        table.as_str(),
        Some(&map),
        Some(SeriesJoin {
            table: &series,
            force: false,
            tag_columns: &[],
        }),
        None,
    )
    .unwrap();
    assert!(
        sql.contains("sum(\"sum_value\") / nullIf(sum(\"count_value\"), 0)"),
        "expected weighted mean rewrite, got: {sql}"
    );
}

#[test]
fn test_tag_field_collision_uses_column_mapping() {
    let stmt = parse_select(r#"SELECT mean("cpu") FROM m GROUP BY cpu"#);
    let mut map = ColumnMapping::default();
    map.tag_keys.insert("cpu".into());
    map.field_names.insert("cpu".into());
    let table = test_table();
    let series = test_series_table();
    let sql = translate_native_table(
        &stmt,
        table.as_str(),
        Some(&map),
        Some(SeriesJoin {
            table: &series,
            force: false,
            tag_columns: &[],
        }),
        None,
    )
    .unwrap();
    assert!(
        sql.contains("__tag__cpu"),
        "tag column should be prefixed when it collides with a field, got: {sql}"
    );
    assert!(
        sql.contains("avg(\"cpu\")"),
        "aggregate should use field column name, got: {sql}"
    );
    assert!(
        sql.contains("GROUP BY \"__tag__cpu\""),
        "GROUP BY must use the physical tag column to match SELECT, got: {sql}"
    );
}

// --- series_id layout: tag resolution via the dimension-table inline view ---

#[test]
fn series_field_only_query_has_no_join() {
    // No tag referenced → coalesced fact view, no series dimension join.
    let stmt = parse_select(r#"SELECT mean("usage_idle") FROM cpu WHERE time > 0"#);
    let sql = translate_series(&stmt, &cpu_mapping());
    assert!(
        !sql.contains("JOIN") && !sql.contains("_series"),
        "field-only query should not join the series table, got: {sql}"
    );
    assert!(
        sql.contains("argMax(\"usage_idle\", `ingest_seq`)"),
        "field-only query should collapse duplicate rows by ingest_seq, got: {sql}"
    );
    assert!(sql.contains("FROM `mydb_autogen_cpu`"), "got: {sql}");
}

#[test]
fn telegraf_cpu_multi_field_query_coalesces_partial_rows() {
    let stmt = parse_select(
        r#"SELECT mean("usage_guest") AS "Usage Guest", mean("usage_idle") AS "Usage Idle", mean("usage_user") AS "Usage User" FROM "cpu" WHERE "host" =~ /^(d2ddee27a9f4)$/ AND "cpu" = 'cpu-total' AND time >= 1780922276152ms and time <= 1780925876152ms GROUP BY time(2s), "host" fill(null)"#,
    );
    let mut map = ColumnMapping::default();
    map.tag_keys.insert("host".into());
    map.tag_keys.insert("cpu".into());
    for f in [
        "usage_guest",
        "usage_idle",
        "usage_user",
        "usage_system",
        "usage_iowait",
    ] {
        map.field_names.insert(f.into());
    }
    let table = test_table();
    let series = test_series_table();
    let sql = translate_native_table(
        &stmt,
        table.as_str(),
        Some(&map),
        Some(SeriesJoin {
            table: &series,
            force: false,
            tag_columns: &[],
        }),
        None,
    )
    .unwrap();
    assert!(
        sql.contains("argMax(\"usage_idle\", `ingest_seq`)"),
        "expected coalesced fact view, got: {sql}"
    );
    assert!(
        sql.contains("ANY LEFT JOIN `mydb_autogen_cpu_series` AS s"),
        "tag filter should join series table, got: {sql}"
    );
    assert!(sql.contains("avg(\"usage_idle\")"), "got: {sql}");
    assert!(
        sql.contains("toStartOfInterval(time, INTERVAL 2 SECOND)"),
        "got: {sql}"
    );
}

#[test]
fn series_where_tag_filter_joins_dimension() {
    let stmt = parse_select(r#"SELECT mean("usage_idle") FROM cpu WHERE "host" = 'h1'"#);
    let sql = translate_series(&stmt, &cpu_mapping());
    assert!(
        sql.contains("ANY LEFT JOIN `mydb_autogen_cpu_series` AS s"),
        "tag filter should join the series table, got: {sql}"
    );
    assert!(
        sql.contains("t.`series_id` = s.`series_id`"),
        "join key should be series_id, got: {sql}"
    );
    // The tag predicate resolves against the joined view's tag column.
    assert!(sql.contains("\"host\" = 'h1'"), "got: {sql}");
}

#[test]
fn series_group_by_all_tags_expands_to_measurement_tags() {
    let mut stmt = parse_select(r#"SELECT mean("usage_idle") FROM cpu GROUP BY time(5m), *"#);
    let gb = stmt.group_by.as_ref().unwrap().clone();
    let (expanded_gb, tags) = gb.expand_all_tags(&["host".to_string(), "region".to_string()]);
    stmt.group_by = Some(expanded_gb);
    assert_eq!(tags, vec!["host", "region"]);
    let sql = translate_series(&stmt, &cpu_mapping());
    assert!(sql.contains("ANY LEFT JOIN"), "got: {sql}");
    assert!(sql.contains("\"host\""), "got: {sql}");
    assert!(sql.contains("\"region\""), "got: {sql}");
    assert!(!sql.contains("`*`"), "got: {sql}");
}

#[test]
fn series_group_by_tag_projects_and_groups_physical() {
    let stmt = parse_select(r#"SELECT mean("usage_idle") FROM cpu GROUP BY time(5m), "host""#);
    let sql = translate_series(&stmt, &cpu_mapping());
    assert!(sql.contains("ANY LEFT JOIN"), "got: {sql}");
    // host is non-colliding, so physical == logical.
    assert!(
        sql.contains("\"host\""),
        "tag projected/grouped, got: {sql}"
    );
    assert!(sql.contains("GROUP BY"), "got: {sql}");
    assert!(sql.contains("avg(\"usage_idle\")"), "got: {sql}");
}

#[test]
fn series_view_exposes_only_tag_columns_from_dimension() {
    let stmt = parse_select(r#"SELECT mean("usage_idle") FROM cpu GROUP BY "host""#);
    let sql = translate_series(&stmt, &cpu_mapping());
    // Inline view selects t.* plus the dimension's tag columns (sorted).
    assert!(
        sql.contains("SELECT t.*, s.\"host\", s.\"region\""),
        "view should re-attach tag columns, got: {sql}"
    );
}

#[test]
fn series_force_join_without_tag_reference() {
    // force=true (e.g. a tombstone references a tag) joins even a field-only body.
    let stmt = parse_select(r#"SELECT mean("usage_idle") FROM cpu WHERE time > 0"#);
    let m = cpu_mapping();
    let table = test_table();
    let series = test_series_table();
    let sql = translate_native_table(
        &stmt,
        table.as_str(),
        Some(&m),
        Some(SeriesJoin {
            table: &series,
            force: true,
            tag_columns: &[],
        }),
        None,
    )
    .unwrap();
    assert!(
        sql.contains("ANY LEFT JOIN"),
        "force should join, got: {sql}"
    );
}

#[test]
fn mv_series_select_uses_dest_field_names_for_tag_prefix() {
    // Tag "host" collides with a field only in the destination, not the source.
    // Source mapping treats "host" as non-colliding (source field_names is
    // {"usage_idle"}), so tag_column_name("host") returns "host".
    // Destination has field "host", so dest_field_names = {"host", "usage_idle"},
    // and tag_column_name("host") should return "__tag__host".
    let stmt = parse_select(
        r#"SELECT mean("usage_idle") INTO "dest" FROM "cpu" GROUP BY time(5m), "host""#,
    );
    let mut src_mapping = cpu_mapping();
    src_mapping.tag_keys.insert("host".to_string());

    let dest_field_names: std::collections::HashSet<String> =
        ["host".to_string(), "usage_idle".to_string()].into();

    let sql = translate_materialized_view_series_select(
        &stmt,
        &qname("`source_series`"),
        "dest",
        &src_mapping,
        Some(&dest_field_names),
    )
    .unwrap();

    assert!(
        sql.contains("__tag__host"),
        "tag 'host' should be prefixed when dest has colliding field, got: {sql}"
    );
}

#[test]
fn mv_series_select_uses_source_names_when_no_dest_field_names() {
    let stmt = parse_select(
        r#"SELECT mean("usage_idle") INTO "dest" FROM "cpu" GROUP BY time(5m), "host""#,
    );
    let mut src_mapping = cpu_mapping();
    src_mapping.tag_keys.insert("host".to_string());

    let sql = translate_materialized_view_series_select(
        &stmt,
        &qname("`source_series`"),
        "dest",
        &src_mapping,
        None,
    )
    .unwrap();

    // Without dest field names, source mapping says "host" doesn't collide
    // (cpu_mapping has only "usage_idle" as field).
    assert!(
        sql.contains("\"host\""),
        "tag 'host' should NOT be prefixed when dest_field_names is None, got: {sql}"
    );
    assert!(
        !sql.contains("__tag__host"),
        "tag 'host' should NOT be prefixed without dest_field_names, got: {sql}"
    );
}

// --- per-series LIMIT/OFFSET (InfluxQL points-per-series semantics) ---

#[test]
fn test_limit_with_group_by_tag_uses_limit_by() {
    let stmt =
        parse_select(r#"SELECT mean("usage_idle") FROM cpu GROUP BY time(1m), "host" LIMIT 3"#);
    let sql = translate_series(&stmt, &cpu_mapping());
    assert!(
        sql.contains("LIMIT 3 BY (\"host\")"),
        "LIMIT with tag grouping must be per series, got: {sql}"
    );
    assert!(
        !sql.contains("\nLIMIT 3\n") && !sql.ends_with("\nLIMIT 3"),
        "no global LIMIT alongside LIMIT BY, got: {sql}"
    );
}

#[test]
fn test_limit_offset_with_group_by_tags_uses_limit_by() {
    let stmt = parse_select(
        r#"SELECT mean("usage_idle") FROM cpu GROUP BY time(1m), "host", "region" LIMIT 3 OFFSET 2"#,
    );
    let sql = translate_series(&stmt, &cpu_mapping());
    assert!(
        sql.contains("LIMIT 2, 3 BY (\"host\", \"region\")"),
        "OFFSET with tag grouping must be per series, got: {sql}"
    );
    assert!(!sql.contains("\nOFFSET"), "got: {sql}");
}

#[test]
fn test_limit_without_tags_stays_global() {
    let stmt = parse_select(r#"SELECT mean("v") FROM m GROUP BY time(1m) LIMIT 4 OFFSET 1"#);
    let sql = translate_test(&stmt);
    assert!(sql.contains("\nLIMIT 4"), "got: {sql}");
    assert!(sql.contains("\nOFFSET 1"), "got: {sql}");
    assert!(!sql.contains(" BY ("), "got: {sql}");
}

// --- raw (non-aggregate) SELECT with GROUP BY tag ---

#[test]
fn test_raw_select_with_group_by_tag_has_no_sql_group_by() {
    let stmt = parse_select(r#"SELECT "usage_idle" FROM cpu GROUP BY "host""#);
    let sql = translate_series(&stmt, &cpu_mapping());
    assert!(
        !sql.contains("\nGROUP BY"),
        "raw select must not GROUP BY tags in SQL (NOT_AN_AGGREGATE), got: {sql}"
    );
    // Tag stays projected so the result parser can split per-series.
    let select_line = sql.lines().next().unwrap();
    assert!(
        select_line.contains("\"host\""),
        "tag must be projected for series splitting, got: {select_line}"
    );
    assert!(
        select_line.starts_with("SELECT \"time\""),
        "raw select keeps time first, got: {select_line}"
    );
    assert!(sql.contains("ORDER BY time ASC"), "got: {sql}");
}

// --- per-point window transforms without GROUP BY time ---

#[test]
fn test_difference_without_group_by_time_projects_time_and_orders() {
    let stmt = parse_select(r#"SELECT difference("value") FROM cpu"#);
    let sql = translate_test(&stmt);
    assert!(
        sql.contains("SELECT \"time\","),
        "transform must project the point time, got: {sql}"
    );
    assert!(
        sql.contains("ORDER BY \"time\" ASC"),
        "transform output must be time-ordered, got: {sql}"
    );
    // InfluxQL omits the first point (no previous value): NULL outputs are
    // filtered by an outer SELECT.
    assert!(
        sql.starts_with("SELECT * FROM (\n"),
        "transform must wrap to filter NULL rows, got: {sql}"
    );
    assert!(
        sql.contains(") WHERE \"difference_value\" IS NOT NULL"),
        "leading NULL transform rows must be filtered, got: {sql}"
    );
}

#[test]
fn test_transform_with_group_by_tag_partitions_without_sql_group_by() {
    let stmt = parse_select(r#"SELECT difference("usage_idle") FROM cpu GROUP BY "host""#);
    let sql = translate_series(&stmt, &cpu_mapping());
    assert!(
        !sql.contains("\nGROUP BY"),
        "bare transform must not GROUP BY tags in SQL, got: {sql}"
    );
    assert!(
        sql.contains("PARTITION BY \"host\""),
        "transform must still partition per series, got: {sql}"
    );
}

#[test]
fn test_transform_with_group_by_time_keeps_grid_nulls() {
    // GROUP BY time + fill keeps the filled grid (Grafana relies on the
    // NULL rows); no NULL-filtering wrapper.
    let stmt = parse_select(r#"SELECT difference(mean("v")) FROM m GROUP BY time(1m) fill(null)"#);
    let sql = translate_test(&stmt);
    assert!(!sql.starts_with("SELECT * FROM (\n"), "got: {sql}");
}

// --- tag compared to numeric literal ---

#[test]
fn test_tag_numeric_comparison_is_constant_false() {
    let stmt = parse_select(r#"SELECT mean("usage_idle") FROM cpu WHERE "host" = 3"#);
    let sql = translate_series(&stmt, &cpu_mapping());
    assert!(
        sql.contains("WHERE (1 = 0)"),
        "tag vs numeric literal must be constant-false, got: {sql}"
    );
    assert!(
        !sql.contains("\"host\" = 3"),
        "must not emit a string/number comparison, got: {sql}"
    );
}

#[test]
fn test_tag_string_comparison_is_unaffected() {
    let stmt = parse_select(r#"SELECT mean("usage_idle") FROM cpu WHERE "host" = '3'"#);
    let sql = translate_series(&stmt, &cpu_mapping());
    assert!(sql.contains("\"host\" = '3'"), "got: {sql}");
    assert!(!sql.contains("1 = 0"), "got: {sql}");
}

#[test]
fn test_field_numeric_comparison_is_unaffected() {
    let stmt = parse_select(r#"SELECT mean("usage_idle") FROM cpu WHERE "usage_idle" > 3"#);
    let sql = translate_series(&stmt, &cpu_mapping());
    assert!(sql.contains("\"usage_idle\" > 3"), "got: {sql}");
    assert!(!sql.contains("1 = 0"), "got: {sql}");
}

// --- subquery source: inner GROUP BY time must expose `time` ---

#[test]
fn test_subquery_source_bucket_column_composes() {
    // Built directly (the parser can't produce subqueries yet): the inner
    // statement is translated, its `__time` alias renamed to `time`, and
    // used as the outer FROM source.
    let minute = Duration {
        value: 1,
        unit: DurationUnit::Minute,
    };
    let five_minutes = Duration {
        value: 5,
        unit: DurationUnit::Minute,
    };
    let inner = SelectStatement {
        fields: vec![Field {
            expr: Expr::Call(FunctionCall {
                name: "mean".to_string(),
                args: vec![Expr::Identifier("v".to_string())],
            }),
            alias: Some("x".to_string()),
        }],
        into: None,
        from: vec![],
        condition: None,
        group_by: Some(GroupBy {
            dimensions: vec![Dimension::Time {
                interval: minute,
                offset: None,
            }],
        }),
        order_by: None,
        limit: None,
        offset: None,
        slimit: None,
        soffset: None,
        fill: None,
        timezone: None,
    };
    let inner_sql =
        translate_native_table(&inner, test_table().as_str(), None, None, None).unwrap();
    let inner_sql = rename_time_bucket_alias(&inner_sql);
    assert!(
        inner_sql.contains("AS time"),
        "inner bucket must be exposed as `time`, got: {inner_sql}"
    );
    assert!(!inner_sql.contains("__time"), "got: {inner_sql}");

    let outer = SelectStatement {
        fields: vec![Field {
            expr: Expr::Call(FunctionCall {
                name: "max".to_string(),
                args: vec![Expr::Identifier("x".to_string())],
            }),
            alias: None,
        }],
        into: None,
        from: vec![],
        condition: None,
        group_by: Some(GroupBy {
            dimensions: vec![Dimension::Time {
                interval: five_minutes,
                offset: None,
            }],
        }),
        order_by: None,
        limit: None,
        offset: None,
        slimit: None,
        soffset: None,
        fill: None,
        timezone: None,
    };
    let outer_sql = translate_with_source(&outer, &format!("({inner_sql})")).unwrap();
    assert!(
        outer_sql.contains("toStartOfInterval(time, INTERVAL 5 MINUTE) AS __time"),
        "outer buckets the inner `time` column, got: {outer_sql}"
    );
    assert!(outer_sql.contains("max(\"x\")"), "got: {outer_sql}");
}

// --- tz() flows into bucketing and fill anchors ---

#[test]
fn test_timezone_in_bucket_expr_and_fill_anchors() {
    let mut stmt = parse_select(
        r#"SELECT mean("v") FROM m WHERE time >= 1000000000 AND time <= 3000000000 GROUP BY time(1d) fill(null)"#,
    );
    stmt.timezone = Some("America/New_York".to_string());
    let sql = translate_native_table(
        &stmt,
        test_table().as_str(),
        None,
        None,
        Some((Some(1_000_000_000), Some(3_000_000_000))),
    )
    .unwrap();
    assert!(
        sql.contains("toStartOfInterval(time, INTERVAL 1 DAY, 'America/New_York') AS __time"),
        "bucket expression must carry the timezone, got: {sql}"
    );
    assert!(
        sql.contains(
            "WITH FILL FROM toStartOfInterval(fromUnixTimestamp64Nano(1000000000), INTERVAL 1 DAY, 'America/New_York')"
        ),
        "fill anchors must bucket in the same timezone, got: {sql}"
    );
    assert!(
        sql.contains("GROUP BY toStartOfInterval(time, INTERVAL 1 DAY, 'America/New_York')"),
        "GROUP BY must match the SELECT bucket expression, got: {sql}"
    );
}

#[test]
fn test_timezone_string_is_escaped() {
    let mut stmt = parse_select(r#"SELECT mean("v") FROM m GROUP BY time(1h)"#);
    stmt.timezone = Some("bad'zone".to_string());
    let sql = translate_test_tz(&stmt);
    assert!(
        sql.contains("'bad\\'zone'"),
        "timezone must go through quote_string escaping, got: {sql}"
    );
}

fn translate_test_tz(stmt: &SelectStatement) -> String {
    translate_native_table(stmt, test_table().as_str(), None, None, None).unwrap()
}
