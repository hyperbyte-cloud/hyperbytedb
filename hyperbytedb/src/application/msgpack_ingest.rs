//! MessagePack batch format for `POST /write` with `Content-Type: application/msgpack`.
//!
//! Body: one msgpack array of point maps. Each map has `measurement` (str), optional `tags`
//! (map str→str), `fields` (map str→[`FieldValue`](crate::domain::point::FieldValue) in serde
//! default externally-tagged form), and optional `timestamp` (signed int). Timestamp units match
//! the `precision` query parameter, same as line protocol.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use crate::domain::database::Precision;
use crate::domain::point::{FieldValue, Point};
use crate::error::HyperbytedbError;

#[derive(Debug, Serialize, Deserialize)]
struct MsgpackPointWire {
    measurement: String,
    #[serde(default)]
    tags: BTreeMap<String, String>,
    fields: BTreeMap<String, FieldValue>,
    timestamp: Option<i64>,
}

/// Parses a msgpack array of points (`precision` matches `/write` query param).
pub fn parse_msgpack_body_to_points(
    body: &[u8],
    precision: Option<&str>,
) -> Result<Vec<Point>, HyperbytedbError> {
    if body.is_empty() {
        return Ok(Vec::new());
    }

    let wire: Vec<MsgpackPointWire> =
        rmp_serde::from_slice(body).map_err(|e| HyperbytedbError::MsgpackParse {
            reason: e.to_string(),
        })?;

    let precision_val = Precision::from_str_opt(precision);
    let mut points = Vec::with_capacity(wire.len());
    for w in wire {
        let timestamp_ns = if let Some(ts) = w.timestamp {
            precision_val.to_nanos(ts)
        } else {
            match chrono::Utc::now().timestamp_nanos_opt() {
                Some(ns) => ns,
                None => return Err(HyperbytedbError::WallClockTimestampUnavailable),
            }
        };

        points.push(Point {
            measurement: w.measurement,
            tags: w.tags,
            fields: w.fields,
            timestamp: timestamp_ns,
        });
    }
    Ok(points)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::line_protocol::encode_points_to_line_protocol;
    use crate::application::line_protocol::parse_line_body_to_points;
    use crate::domain::database::Precision;

    #[test]
    fn msgpack_round_trip_via_line_protocol_ms_precision() {
        let mut fields = BTreeMap::new();
        fields.insert("idle".to_string(), FieldValue::Float(0.5));
        let mut tags = BTreeMap::new();
        tags.insert("host".to_string(), "h1".to_string());

        let wire = vec![MsgpackPointWire {
            measurement: "cpu".to_string(),
            tags,
            fields,
            timestamp: Some(1_700_000_000_000_i64),
        }];
        let body = rmp_serde::to_vec_named(&wire).expect("encode");
        let points = parse_msgpack_body_to_points(&body, Some("ms")).expect("parse msgpack");
        assert_eq!(points.len(), 1);
        assert_eq!(points[0].timestamp, 1_700_000_000_000_000_000);

        let lp =
            encode_points_to_line_protocol(&points, Precision::Millisecond).expect("encode lp");
        let round = parse_line_body_to_points(&lp, Some("ms")).expect("parse lp");
        assert_eq!(round.len(), points.len());
        assert_eq!(round[0].measurement, points[0].measurement);
        assert_eq!(round[0].timestamp, points[0].timestamp);
        assert_eq!(round[0].fields, points[0].fields);
        assert_eq!(round[0].tags, points[0].tags);
    }
}
