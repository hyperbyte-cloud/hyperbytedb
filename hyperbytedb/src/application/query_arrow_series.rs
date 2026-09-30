//! InfluxQL series built straight from chDB's Arrow record batches.
//!
//! The JSON path asked chDB for JSONEachRow, copied the result into a
//! `String`, parsed every line into a `serde_json::Value` map and held all of
//! them before bucketing rows into series: the data lived in chDB's result
//! buffer, the `String` and the parsed rows at once, and ClickHouse formatted
//! text that Rust parsed back. Here each batch is consumed as it arrives and
//! only the output series are kept.
//!
//! The output is what the JSONEachRow path produced before it was removed:
//! - columns in the order a `serde_json` map gives them (sorted by name;
//!   a repeated name keeps its last column), group-by tags removed, `__time`
//!   shown as `time` and moved first;
//! - numbers as ClickHouse prints them and `serde_json` reads them back:
//!   an integral float below 1e21 becomes an integer, non-finite floats are
//!   null;
//! - the time column as an integer in the requested epoch unit, or RFC3339
//!   with trailing fractional zeros removed; other timestamps as ClickHouse's
//!   datetime text.
//!
//! One type cannot be told apart: ClickHouse sends a 32-bit `DateTime` as a
//! plain `UInt32` (no field metadata). `GROUP BY time(...)` buckets of a
//! second or more are `DateTime`, so in the time column a `UInt32` is read as
//! epoch seconds. Anywhere else it stays a number where JSON gave datetime
//! text; InfluxQL results carry datetimes only in the time column.

use std::collections::HashMap;

use arrow::array::{Array, ArrayRef, AsArray};
use arrow::datatypes::{
    DataType, Date32Type, Date64Type, Float16Type, Float32Type, Float64Type, Int8Type, Int16Type,
    Int32Type, Int64Type, TimeUnit, TimestampMicrosecondType, TimestampMillisecondType,
    TimestampNanosecondType, TimestampSecondType, UInt8Type, UInt16Type, UInt32Type, UInt64Type,
};
use arrow::record_batch::RecordBatch;
use serde_json::Value;

use crate::domain::query_result::SeriesResult;
use crate::error::HyperbytedbError;

/// Where each output column comes from in a batch.
struct Layout {
    /// Batch column index per output column.
    sources: Vec<usize>,
    columns: Vec<String>,
    time_idx: Option<usize>,
    /// Batch column index per group-by tag (`None`: not in the result).
    tag_sources: Vec<Option<usize>>,
}

impl Layout {
    fn new(batch: &RecordBatch, group_by_tags: &[String]) -> Self {
        // A JSON object keeps the last value of a repeated key, and a
        // `serde_json` map (no `preserve_order`) iterates keys sorted.
        let mut by_name: std::collections::BTreeMap<String, usize> = Default::default();
        for (i, f) in batch.schema().fields().iter().enumerate() {
            by_name.insert(f.name().clone(), i);
        }
        let tag_set: std::collections::HashSet<&str> =
            group_by_tags.iter().map(String::as_str).collect();
        let mut pairs: Vec<(usize, String)> = by_name
            .iter()
            .filter(|(name, _)| !tag_set.contains(name.as_str()))
            .map(|(name, idx)| {
                let display = if name == "__time" {
                    "time".to_string()
                } else {
                    name.clone()
                };
                (*idx, display)
            })
            .collect();
        if let Some(pos) = pairs.iter().position(|(_, d)| d == "time")
            && pos != 0
        {
            let pair = pairs.remove(pos);
            pairs.insert(0, pair);
        }
        let columns: Vec<String> = pairs.iter().map(|(_, d)| d.clone()).collect();
        let time_idx = columns.iter().position(|c| c == "time");
        Self {
            sources: pairs.into_iter().map(|(i, _)| i).collect(),
            columns,
            time_idx,
            tag_sources: group_by_tags
                .iter()
                .map(|t| by_name.get(t).copied())
                .collect(),
        }
    }
}

/// Accumulates series from record batches, one batch at a time.
pub struct SeriesBuilder<'a> {
    measurement: &'a str,
    epoch: Option<&'a str>,
    group_by_tags: &'a [String],
    layout: Option<Layout>,
    ungrouped: Vec<Vec<Value>>,
    grouped: indexmap::IndexMap<Vec<(String, String)>, Vec<Vec<Value>>>,
}

impl<'a> SeriesBuilder<'a> {
    pub fn new(measurement: &'a str, epoch: Option<&'a str>, group_by_tags: &'a [String]) -> Self {
        Self {
            measurement,
            epoch,
            group_by_tags,
            layout: None,
            ungrouped: Vec::new(),
            grouped: indexmap::IndexMap::new(),
        }
    }

    pub fn push_batch(&mut self, batch: &RecordBatch) -> Result<(), HyperbytedbError> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let layout = self
            .layout
            .get_or_insert_with(|| Layout::new(batch, self.group_by_tags));
        let columns: Vec<ArrayRef> = batch
            .columns()
            .iter()
            .map(|c| decode_dictionary(c))
            .collect::<Result<_, _>>()?;
        let epoch = self.epoch;
        for row in 0..batch.num_rows() {
            let values: Vec<Value> = layout
                .sources
                .iter()
                .enumerate()
                .map(|(i, &src)| cell(&columns[src], row, Some(i) == layout.time_idx, epoch))
                .collect();
            if self.group_by_tags.is_empty() {
                self.ungrouped.push(values);
                continue;
            }
            let key: Vec<(String, String)> = self
                .group_by_tags
                .iter()
                .zip(&layout.tag_sources)
                .map(|(tag, src)| {
                    let v = src
                        .and_then(|s| string_at(&columns[s], row))
                        .unwrap_or_default();
                    (tag.clone(), v)
                })
                .collect();
            self.grouped.entry(key).or_default().push(values);
        }
        Ok(())
    }

    pub fn finish(self) -> Vec<SeriesResult> {
        let Some(layout) = self.layout else {
            return Vec::new();
        };
        if self.group_by_tags.is_empty() {
            if self.ungrouped.is_empty() {
                return Vec::new();
            }
            return vec![SeriesResult {
                name: self.measurement.to_string(),
                tags: None,
                columns: layout.columns,
                values: self.ungrouped,
                partial: None,
            }];
        }
        self.grouped
            .into_iter()
            .map(|(tag_kv, values)| SeriesResult {
                name: self.measurement.to_string(),
                tags: Some(tag_kv.into_iter().collect::<HashMap<_, _>>()),
                columns: layout.columns.clone(),
                values,
                partial: None,
            })
            .collect()
    }
}

fn decode_dictionary(col: &ArrayRef) -> Result<ArrayRef, HyperbytedbError> {
    match col.data_type() {
        DataType::Dictionary(_, value) => arrow::compute::cast(col, value).map_err(|e| {
            HyperbytedbError::Internal(format!("arrow dictionary decode: {e}").into())
        }),
        _ => Ok(col.clone()),
    }
}

/// A string cell, as `serde_json::Value::as_str` would see it: `None` for
/// null and for non-string columns.
fn string_at(col: &ArrayRef, row: usize) -> Option<String> {
    if col.is_null(row) {
        return None;
    }
    match col.data_type() {
        DataType::Utf8 => Some(col.as_string::<i32>().value(row).to_string()),
        DataType::LargeUtf8 => Some(col.as_string::<i64>().value(row).to_string()),
        DataType::Utf8View => Some(col.as_string_view().value(row).to_string()),
        _ => None,
    }
}

/// A float as ClickHouse prints it in JSON and `serde_json` reads it back.
///
/// ClickHouse writes the shortest round-trip digits, in plain notation below
/// 1e21, and `serde_json` reads integer text as an integer (when it fits a
/// u64/i64) and anything else as an f64. Rust's `Display` produces the same
/// digits, so parsing it reproduces the value exactly.
fn float_value(v: f64) -> Value {
    if !v.is_finite() {
        return Value::Null;
    }
    // `-0` reads back as the float -0.0, not the integer 0.
    if v == 0.0 && v.is_sign_negative() {
        return Value::from(v);
    }
    if v.fract() == 0.0 && v.abs() < 1e21 {
        let s = format!("{v}");
        if let Ok(u) = s.parse::<u64>() {
            return Value::from(u);
        }
        if let Ok(i) = s.parse::<i64>() {
            return Value::from(i);
        }
    }
    Value::from(v)
}

fn cell(col: &ArrayRef, row: usize, is_time: bool, epoch: Option<&str>) -> Value {
    if col.is_null(row) {
        return Value::Null;
    }
    match col.data_type() {
        DataType::Float64 => float_value(col.as_primitive::<Float64Type>().value(row)),
        // Printed as the f32's shortest digits, then read as an f64.
        DataType::Float32 => float_value(
            format!("{}", col.as_primitive::<Float32Type>().value(row))
                .parse()
                .unwrap_or(f64::NAN),
        ),
        DataType::Float16 => float_value(col.as_primitive::<Float16Type>().value(row).to_f64()),
        DataType::Int8 => Value::from(col.as_primitive::<Int8Type>().value(row)),
        DataType::Int16 => Value::from(col.as_primitive::<Int16Type>().value(row)),
        DataType::Int32 => Value::from(col.as_primitive::<Int32Type>().value(row)),
        DataType::Int64 => Value::from(col.as_primitive::<Int64Type>().value(row)),
        DataType::UInt8 => Value::from(col.as_primitive::<UInt8Type>().value(row)),
        DataType::UInt16 => Value::from(col.as_primitive::<UInt16Type>().value(row)),
        // A ClickHouse `DateTime` (time buckets); see the module docs.
        DataType::UInt32 if is_time => time_value_from_nanos(
            i64::from(col.as_primitive::<UInt32Type>().value(row)) * 1_000_000_000,
            epoch,
        ),
        DataType::UInt32 => Value::from(col.as_primitive::<UInt32Type>().value(row)),
        DataType::UInt64 => Value::from(col.as_primitive::<UInt64Type>().value(row)),
        DataType::Boolean => Value::Bool(col.as_boolean().value(row)),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
            Value::String(string_at(col, row).unwrap_or_default())
        }
        DataType::Timestamp(unit, _) => {
            let (nanos, digits) = match unit {
                TimeUnit::Second => (
                    col.as_primitive::<TimestampSecondType>()
                        .value(row)
                        .saturating_mul(1_000_000_000),
                    0,
                ),
                TimeUnit::Millisecond => (
                    col.as_primitive::<TimestampMillisecondType>()
                        .value(row)
                        .saturating_mul(1_000_000),
                    3,
                ),
                TimeUnit::Microsecond => (
                    col.as_primitive::<TimestampMicrosecondType>()
                        .value(row)
                        .saturating_mul(1_000),
                    6,
                ),
                TimeUnit::Nanosecond => {
                    (col.as_primitive::<TimestampNanosecondType>().value(row), 9)
                }
            };
            if is_time {
                time_value_from_nanos(nanos, epoch)
            } else {
                Value::String(clickhouse_datetime_text(nanos, digits))
            }
        }
        DataType::Date32 => {
            let days = col.as_primitive::<Date32Type>().value(row);
            Value::String(
                chrono::DateTime::from_timestamp(i64::from(days) * 86_400, 0)
                    .map(|d| d.format("%Y-%m-%d").to_string())
                    .unwrap_or_default(),
            )
        }
        DataType::Date64 => {
            let ms = col.as_primitive::<Date64Type>().value(row);
            Value::String(
                chrono::DateTime::from_timestamp_millis(ms)
                    .map(|d| d.format("%Y-%m-%d").to_string())
                    .unwrap_or_default(),
            )
        }
        // Anything else (decimals, arrays, maps): its display text, read as
        // JSON when it is a number, as ClickHouse would have written it.
        _ => {
            let text = arrow::util::display::array_value_to_string(col, row).unwrap_or_default();
            match serde_json::from_str::<Value>(&text) {
                Ok(v @ Value::Number(_)) => v,
                _ => Value::String(text),
            }
        }
    }
}

/// Decode a peer's `/internal/shard/query` response, an Arrow IPC stream,
/// into series as its chunks arrive. An empty body is an empty result.
pub async fn series_from_ipc_response(
    mut resp: reqwest::Response,
    measurement: &str,
    epoch: Option<&str>,
    group_by_tags: &[String],
) -> Result<Vec<SeriesResult>, HyperbytedbError> {
    let mut decoder = arrow::ipc::reader::StreamDecoder::new();
    let mut builder = SeriesBuilder::new(measurement, epoch, group_by_tags);
    let mut received = false;
    while let Some(chunk) = resp
        .chunk()
        .await
        .map_err(|e| HyperbytedbError::PeerUnreachable(format!("shard query stream: {e}")))?
    {
        received = true;
        let mut buf = arrow::buffer::Buffer::from_vec(chunk.to_vec());
        while !buf.is_empty() {
            if let Some(batch) = decoder.decode(&mut buf).map_err(ipc_error)? {
                builder.push_batch(&batch)?;
            }
        }
    }
    if received {
        decoder.finish().map_err(ipc_error)?;
    }
    Ok(builder.finish())
}

fn ipc_error(e: arrow::error::ArrowError) -> HyperbytedbError {
    HyperbytedbError::PeerUnreachable(format!("shard query stream: {e}"))
}

/// ClickHouse's datetime text for a UTC instant: `YYYY-MM-DD HH:MM:SS`, plus
/// the column's fractional digits.
fn clickhouse_datetime_text(nanos: i64, digits: usize) -> String {
    let dt = chrono::DateTime::from_timestamp_nanos(nanos);
    let base = dt.format("%Y-%m-%d %H:%M:%S").to_string();
    if digits == 0 {
        return base;
    }
    let frac = format!("{:09}", dt.timestamp_subsec_nanos());
    format!("{base}.{}", &frac[..digits])
}

/// The time column's value: an integer in the requested epoch unit, or
/// RFC3339 with trailing fractional zeros removed.
pub(crate) fn time_value_from_nanos(nanos: i64, epoch: Option<&str>) -> Value {
    match epoch {
        Some("ns") | Some("n") => Value::from(nanos),
        Some("u") | Some("us") => Value::from(nanos / 1_000),
        Some("ms") => Value::from(nanos / 1_000_000),
        Some("s") => Value::from(nanos / 1_000_000_000),
        _ => {
            let dt = chrono::DateTime::from_timestamp_nanos(nanos);
            let mut out = dt.format("%Y-%m-%dT%H:%M:%S").to_string();
            let frac = format!("{:09}", dt.timestamp_subsec_nanos());
            let trimmed = frac.trim_end_matches('0');
            if !trimmed.is_empty() {
                out.push('.');
                out.push_str(trimmed);
            }
            out.push('Z');
            Value::String(out)
        }
    }
}

#[cfg(test)]
mod tests {
    //! Expectations are the output the JSONEachRow path produced for the same
    //! queries (checked equal before that path was removed).
    use super::*;
    use crate::adapters::chdb::query_adapter::ChdbQueryAdapter;
    use crate::adapters::chdb::session::SharedSession;
    use crate::ports::query::QueryPort;
    use serde_json::json;

    async fn run(
        port: &ChdbQueryAdapter,
        sql: &str,
        epoch: Option<&str>,
        tags: &[String],
    ) -> (Vec<SeriesResult>, usize) {
        let mut rx = port.execute_sql_arrow(sql).await.unwrap();
        let mut b = SeriesBuilder::new("m", epoch, tags);
        let mut batches = 0;
        while let Some(batch) = rx.recv().await {
            b.push_batch(&batch.unwrap()).unwrap();
            batches += 1;
        }
        (b.finish(), batches)
    }

    const VALUES: &str = "SELECT toDateTime64('2026-01-02 03:04:05.123456789', 9, 'UTC') + toIntervalSecond(number) AS __time, \
        toFloat64(number) AS f_int, number + 0.5 AS f_frac, toFloat64('inf') AS f_inf, \
        -toFloat64('inf') AS f_ninf, toFloat64('nan') AS f_nan, toFloat64(-0.) AS f_negzero, \
        toFloat64(1152921504606846976) AS f_2p60, 1e20 AS f_1e20, 1e21 AS f_1e21, 1e-7 AS f_tiny, \
        toFloat32(0.1) AS f32, toInt64(-7) AS i64, toUInt64(18446744073709551615) AS u64max, \
        toUInt8(3) AS u8, number % 2 = 0 AS b, CAST('x' AS LowCardinality(String)) AS lc, \
        concat('q\"uo', char(10), 'te é') AS s, \
        if(number % 2 = 0, NULL, toFloat64(number)) AS nf, CAST(NULL AS Nullable(String)) AS ns, \
        toDateTime64('2026-05-06 07:08:09.5', 3, 'UTC') AS t3, toDate('2026-05-06') AS d, \
        toDecimal64(1.5, 2) AS dec, \
        concat('host', toString(number % 3)) AS host, if(number % 4 = 0, 'a', 'b') AS region \
        FROM numbers(12)";

    #[tokio::test]
    #[serial_test::serial(chdb)]
    async fn arrow_results_become_the_series_clients_expect() {
        let dir = tempfile::tempdir().unwrap();
        let shared = SharedSession::new_eager(dir.path().to_str().unwrap(), 1).unwrap();
        let port = ChdbQueryAdapter::from_shared(shared, 0);

        // Every value type, one series.
        let (series, _) = run(&port, VALUES, None, &[]).await;
        assert_eq!(series.len(), 1);
        let s0 = &series[0];
        assert_eq!(
            s0.columns,
            [
                "time",
                "b",
                "d",
                "dec",
                "f32",
                "f_1e20",
                "f_1e21",
                "f_2p60",
                "f_frac",
                "f_inf",
                "f_int",
                "f_nan",
                "f_negzero",
                "f_ninf",
                "f_tiny",
                "host",
                "i64",
                "lc",
                "nf",
                "ns",
                "region",
                "s",
                "t3",
                "u64max",
                "u8"
            ],
            "sorted by name, time first"
        );
        assert_eq!(s0.values.len(), 12);
        assert_eq!(
            Value::Array(s0.values[0].clone()),
            json!([
                "2026-01-02T03:04:05.123456789Z",
                1,
                "2026-05-06",
                1.5,
                0.1,
                1e20,
                1e21,
                1152921504606847000u64,
                0.5,
                null,
                0,
                null,
                -0.0,
                null,
                1e-7,
                "host0",
                -7,
                "x",
                null,
                null,
                "a",
                "q\"uo\nte é",
                "2026-05-06 07:08:09.500",
                18446744073709551615u64,
                3
            ])
        );
        // Integral floats are integers and -0.0 stays a float, as JSON gave.
        assert!(s0.values[0][10].is_u64(), "f_int 0 is an integer");
        assert!(s0.values[0][12].is_f64(), "f_negzero is -0.0");

        // The time column in every epoch.
        let ns = 1_767_323_045_123_456_789i64;
        for (epoch, want) in [
            (Some("ns"), json!(ns)),
            (Some("u"), json!(ns / 1_000)),
            (Some("ms"), json!(ns / 1_000_000)),
            (Some("s"), json!(ns / 1_000_000_000)),
        ] {
            let (series, _) = run(&port, VALUES, epoch, &[]).await;
            assert_eq!(series[0].values[0][0], want, "{epoch:?}");
        }

        // GROUP BY tags: series in first-seen order, tag columns removed.
        let tags = vec!["host".to_string(), "region".to_string()];
        let (series, _) = run(&port, VALUES, None, &tags).await;
        let keys: Vec<(String, String)> = series
            .iter()
            .map(|s| {
                let t = s.tags.as_ref().unwrap();
                (t["host"].clone(), t["region"].clone())
            })
            .collect();
        assert_eq!(
            keys,
            [
                ("host0", "a"),
                ("host1", "b"),
                ("host2", "b"),
                ("host0", "b"),
                ("host1", "a"),
                ("host2", "a"),
            ]
            .map(|(h, r)| (h.to_string(), r.to_string()))
        );
        assert!(!series[0].columns.contains(&"host".to_string()));
        assert_eq!(series.iter().map(|s| s.values.len()).sum::<usize>(), 12);

        // GROUP BY time() buckets: DateTime (UInt32 in Arrow) at a minute,
        // DateTime64 below a second; the time column reads the same.
        for (bucket, first) in [
            ("INTERVAL 1 MINUTE", "2026-01-02T03:04:00Z"),
            ("INTERVAL 100 MILLISECOND", "2026-01-02T03:04:05.5Z"),
        ] {
            let sql = format!(
                "SELECT toStartOfInterval(toDateTime64('2026-01-02 03:04:05.5', 9, 'UTC') + \
                 toIntervalSecond(number * 37), {bucket}) AS __time, avg(number) AS mean \
                 FROM numbers(40) GROUP BY __time ORDER BY __time"
            );
            let (series, _) = run(&port, &sql, None, &[]).await;
            assert_eq!(series[0].columns, ["time", "mean"]);
            assert_eq!(series[0].values[0][0], json!(first), "{bucket}");
            let (series, _) = run(&port, &sql, Some("s"), &[]).await;
            assert_eq!(
                series[0].values[0][0],
                json!(1_767_323_040i64 + if bucket.contains("MILLI") { 5 } else { 0 }),
                "{bucket}"
            );
        }

        // A result large enough to arrive in several batches.
        let big = "SELECT toDateTime64('2026-01-01 00:00:00', 9, 'UTC') + toIntervalMillisecond(number) AS __time, \
                   number * 1.25 AS v, concat('h', toString(number % 7)) AS host FROM numbers(300000)";
        let tags = vec!["host".to_string()];
        let (series, batches) = run(&port, big, None, &tags).await;
        assert!(batches > 1, "expected a multi-batch stream, got {batches}");
        assert_eq!(series.len(), 7);
        assert_eq!(
            series.iter().map(|s| s.values.len()).sum::<usize>(),
            300_000
        );

        // Empty result and a table that is not there.
        for sql in [
            "SELECT number AS v FROM numbers(10) WHERE number > 100",
            "SELECT * FROM default.no_such_table",
        ] {
            assert!(run(&port, sql, None, &[]).await.0.is_empty(), "{sql}");
        }
    }
}
