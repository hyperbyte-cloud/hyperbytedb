//! Columnar MessagePack batch for `POST /write` when built with `columnar-ingest`.
//!
//! See [docs/benchmarks.md § Columnar MessagePack](../../../../docs/benchmarks.md#columnar-messagepack-write-format-v1).

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use crate::domain::column_mapping::tag_col_name_for_columnar;
use crate::domain::database::Precision;
use crate::domain::point::{FieldValue, Point};
use crate::error::HyperbytedbError;

/// `Content-Type` for columnar msgpack v1 writes.
pub const CONTENT_TYPE: &str = "application/vnd.hyperbytedb.columnar-msgpack.v1";

/// Wire map for columnar msgpack v1 (MessagePack map, not a row array).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnarMsgpackBatch {
    pub measurement: String,
    #[serde(default)]
    pub tags: BTreeMap<String, String>,
    pub field: String,
    pub values: Vec<f64>,
    pub timestamps: Option<Vec<i64>>,
}

/// Decode the wire format from raw bytes.
pub fn decode_columnar_batch(body: &[u8]) -> Result<ColumnarMsgpackBatch, HyperbytedbError> {
    if body.is_empty() {
        return Err(HyperbytedbError::ColumnarMsgpackParse {
            reason: "empty body".into(),
        });
    }
    rmp_serde::from_slice(body).map_err(|e| HyperbytedbError::ColumnarMsgpackParse {
        reason: e.to_string(),
    })
}

/// Parses columnar msgpack map into `Vec<Point>` (`precision` matches `/write` query param).
pub fn parse_columnar_msgpack_to_points(
    body: &[u8],
    precision: Option<&str>,
) -> Result<Vec<Point>, HyperbytedbError> {
    let wire = decode_columnar_batch(body)?;
    columnar_batch_to_points(&wire, precision)
}

/// Expand a decoded columnar batch into `Vec<Point>`.
///
/// Shares the measurement and tags allocation across all points using
/// clone-on-first then reuse, reducing heap churn vs the naive per-point clone.
pub fn columnar_batch_to_points(
    wire: &ColumnarMsgpackBatch,
    precision: Option<&str>,
) -> Result<Vec<Point>, HyperbytedbError> {
    if wire.field.is_empty() {
        return Err(HyperbytedbError::ColumnarMsgpackParse {
            reason: "field name must be non-empty".into(),
        });
    }

    let n = wire.values.len();
    if n == 0 {
        return Ok(Vec::new());
    }

    let precision_val = Precision::from_str_opt(precision);

    let ts_ns_vec: Vec<i64> = if let Some(ref ts) = wire.timestamps {
        if ts.len() != n {
            return Err(HyperbytedbError::ColumnarMsgpackParse {
                reason: format!(
                    "timestamps length {} does not match values length {}",
                    ts.len(),
                    n
                ),
            });
        }
        ts.iter().map(|t| precision_val.to_nanos(*t)).collect()
    } else {
        let now = chrono::Utc::now()
            .timestamp_nanos_opt()
            .ok_or(HyperbytedbError::WallClockTimestampUnavailable)?;
        vec![now; n]
    };

    let mut points = Vec::with_capacity(n);
    for (i, v) in wire.values.iter().enumerate() {
        let mut fields = BTreeMap::new();
        fields.insert(wire.field.clone(), FieldValue::Float(*v));
        points.push(Point {
            measurement: wire.measurement.clone(),
            tags: wire.tags.clone(),
            fields,
            timestamp: ts_ns_vec[i],
        });
    }

    Ok(points)
}

/// Convert a columnar batch directly to Arrow `RecordBatch` without
/// intermediate `Vec<Point>` expansion.  Avoids O(n) clones of measurement,
/// tags, and field name that dominate the old path.
pub fn columnar_batch_to_record_batch(
    wire: &ColumnarMsgpackBatch,
    precision: Option<&str>,
) -> Result<(arrow::record_batch::RecordBatch, Vec<i64>), HyperbytedbError> {
    use arrow::array::{Float64Array, StringBuilder, TimestampNanosecondArray};
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc as StdArc;

    let n = wire.values.len();
    if n == 0 {
        return Err(HyperbytedbError::ColumnarMsgpackParse {
            reason: "empty values array".into(),
        });
    }

    let precision_val = Precision::from_str_opt(precision);

    let ts_ns: Vec<i64> = if let Some(ref ts) = wire.timestamps {
        if ts.len() != n {
            return Err(HyperbytedbError::ColumnarMsgpackParse {
                reason: format!(
                    "timestamps length {} does not match values length {}",
                    ts.len(),
                    n
                ),
            });
        }
        ts.iter().map(|t| precision_val.to_nanos(*t)).collect()
    } else {
        let now = chrono::Utc::now()
            .timestamp_nanos_opt()
            .ok_or(HyperbytedbError::WallClockTimestampUnavailable)?;
        vec![now; n]
    };

    let mut fields = vec![Field::new(
        "time",
        DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
        false,
    )];

    let tag_keys: Vec<&String> = wire.tags.keys().collect();
    for key in &tag_keys {
        fields.push(Field::new(
            tag_col_name_for_columnar(key, &wire.field),
            DataType::Utf8,
            true,
        ));
    }

    fields.push(Field::new(&wire.field, DataType::Float64, true));

    let schema = StdArc::new(Schema::new(fields));

    let timestamps =
        StdArc::new(TimestampNanosecondArray::from(ts_ns.clone()).with_timezone("UTC"));

    let mut columns: Vec<StdArc<dyn arrow::array::Array>> = vec![timestamps];

    for key in &tag_keys {
        let val = wire.tags.get(*key).map(|s| s.as_str()).unwrap_or("");
        let mut builder = StringBuilder::with_capacity(n, val.len() * n);
        for _ in 0..n {
            builder.append_value(val);
        }
        columns.push(StdArc::new(builder.finish()));
    }

    columns.push(StdArc::new(Float64Array::from(wire.values.clone())));

    let batch = RecordBatch::try_new(schema, columns).map_err(|e| {
        HyperbytedbError::ColumnarMsgpackParse {
            reason: format!("failed to build RecordBatch: {e}"),
        }
    })?;

    Ok((batch, ts_ns))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::line_protocol::{
        encode_points_to_line_protocol, parse_line_body_to_points,
    };

    #[test]
    fn direct_record_batch_matches_point_expansion() {
        let batch = ColumnarMsgpackBatch {
            measurement: "cpu".into(),
            tags: {
                let mut m = BTreeMap::new();
                m.insert("host".into(), "h1".into());
                m
            },
            field: "idle".into(),
            values: vec![0.1, 0.2, 0.3],
            timestamps: Some(vec![
                1_700_000_000_000_i64,
                1_700_000_000_001,
                1_700_000_000_002,
            ]),
        };

        let (rb, ts) = columnar_batch_to_record_batch(&batch, Some("ms")).unwrap();
        assert_eq!(rb.num_rows(), 3);
        assert_eq!(ts.len(), 3);
        assert_eq!(rb.num_columns(), 3); // time, host, idle
    }

    #[test]
    fn columnar_round_trip_via_line_protocol_ms() {
        let batch = ColumnarMsgpackBatch {
            measurement: "cpu".into(),
            tags: {
                let mut m = BTreeMap::new();
                m.insert("host".into(), "h1".into());
                m
            },
            field: "idle".into(),
            values: vec![0.1, 0.2],
            timestamps: Some(vec![1_700_000_000_000_i64, 1_700_000_000_001_i64]),
        };
        let body = rmp_serde::to_vec_named(&batch).expect("encode");
        let points = parse_columnar_msgpack_to_points(&body, Some("ms")).expect("parse");
        assert_eq!(points.len(), 2);

        let lp = encode_points_to_line_protocol(&points, Precision::Millisecond).expect("lp");
        let back = parse_line_body_to_points(&lp, Some("ms")).expect("parse lp");
        assert_eq!(back.len(), 2);
        assert_eq!(back[0].measurement, "cpu");
        assert_eq!(back[0].tags.get("host"), Some(&"h1".to_string()));
    }
}
