//! Line protocol parsing shared by HTTP ingest, peer ingest, and replication apply.

use influxdb_line_protocol::{FieldValue as LpFieldValue, parse_lines};

use crate::domain::database::Precision;
use crate::domain::point::{FieldValue, Point};
use crate::error::HyperbytedbError;

const PARSE_ERROR_SNIPPET_LEN: usize = 200;

fn line_protocol_parse_context(input: &str, line_index: usize) -> String {
    if input.len() <= PARSE_ERROR_SNIPPET_LEN {
        return format!("line {line_index}, payload (truncated if long): {input:?}");
    }
    let prefix: String = input.chars().take(PARSE_ERROR_SNIPPET_LEN).collect();
    format!("line {line_index}, payload starts with: {prefix:?}")
}

/// Parses line protocol bytes into points (`precision` matches `/write` query param).
pub fn parse_line_body_to_points(
    body: &[u8],
    precision: Option<&str>,
) -> Result<Vec<Point>, HyperbytedbError> {
    parse_line_body_to_points_limited(body, precision, 0)
}

/// Like [`parse_line_body_to_points`] but rejects batches above `max_points`
/// during parsing (`0` = [`crate::config::default_max_points_per_request`]).
pub fn parse_line_body_to_points_limited(
    body: &[u8],
    precision: Option<&str>,
    max_points: usize,
) -> Result<Vec<Point>, HyperbytedbError> {
    let limit = if max_points == 0 {
        crate::config::default_max_points_per_request()
    } else {
        max_points
    };
    if is_binary_points(body) {
        return decode_points_frames(body, limit);
    }
    let cap = estimate_points_capacity(body).min(limit);
    parse_line_body_inner(body, precision, cap, Some(limit))
}

/// Start of a binary points frame. Line protocol never starts with NUL, so a
/// body is binary exactly when it starts with this.
pub const POINTS_FRAME_MAGIC: &[u8; 4] = b"\0HBP";

/// Whether `body` is binary points frames rather than line protocol.
#[must_use]
pub fn is_binary_points(body: &[u8]) -> bool {
    body.starts_with(POINTS_FRAME_MAGIC)
}

/// Encode points for node-to-node forwarding and replication: one frame,
/// `magic | u32 LE length | bincode(Vec<Point>)`.
///
/// Every point used to cross the cluster as line protocol, re-encoded by the
/// sender and re-parsed by the primary and again by each replica: about a
/// quarter of a saturated node's CPU went to parsing. Frames concatenate, and
/// [`parse_line_body_to_points_limited`] decodes them, so every consumer
/// (forwards, replication apply, hint replay) accepts either form.
/// Timestamps are carried as stored (nanoseconds); precision does not apply.
pub fn encode_points_binary(points: &[Point]) -> Result<Vec<u8>, HyperbytedbError> {
    let payload = bincode::serialize(points)
        .map_err(|e| HyperbytedbError::Internal(format!("encode points: {e}").into()))?;
    let len = u32::try_from(payload.len())
        .map_err(|_| HyperbytedbError::PayloadTooLarge("points frame over 4 GiB".into()))?;
    let mut out = Vec::with_capacity(8 + payload.len());
    out.extend_from_slice(POINTS_FRAME_MAGIC);
    out.extend_from_slice(&len.to_le_bytes());
    out.extend_from_slice(&payload);
    Ok(out)
}

/// Decode one or more frames. Whitespace between frames is skipped, so the
/// newline replication coalescing inserts between bodies is harmless.
fn decode_points_frames(body: &[u8], limit: usize) -> Result<Vec<Point>, HyperbytedbError> {
    let bad = |m: &str| HyperbytedbError::LineProtocolParse {
        line: String::new(),
        reason: format!("binary points: {m}"),
    };
    let mut out: Vec<Point> = Vec::new();
    let mut rest = body;
    loop {
        while let Some((&b, tail)) = rest.split_first() {
            if b.is_ascii_whitespace() {
                rest = tail;
            } else {
                break;
            }
        }
        if rest.is_empty() {
            return Ok(out);
        }
        if !rest.starts_with(POINTS_FRAME_MAGIC) || rest.len() < 8 {
            return Err(bad("truncated or mixed body"));
        }
        let len = u32::from_le_bytes([rest[4], rest[5], rest[6], rest[7]]) as usize;
        let end = 8usize
            .checked_add(len)
            .ok_or_else(|| bad("length overflow"))?;
        let payload = rest
            .get(8..end)
            .ok_or_else(|| bad("frame shorter than its length"))?;
        let mut points: Vec<Point> =
            bincode::deserialize(payload).map_err(|e| bad(&e.to_string()))?;
        if out.len() + points.len() > limit {
            return Err(HyperbytedbError::PayloadTooLarge(format!(
                "more than {limit} points in one request"
            )));
        }
        out.append(&mut points);
        rest = &rest[end..];
    }
}

/// Count lines that contain at least one non-whitespace byte. Blank lines are
/// ignored so adversarial `\n`-only bodies do not pre-allocate a huge `Vec`.
fn estimate_points_capacity(body: &[u8]) -> usize {
    let mut count = 0usize;
    let mut line_has_content = false;
    for &b in body {
        if b == b'\n' {
            if line_has_content {
                count += 1;
            }
            line_has_content = false;
        } else if !b.is_ascii_whitespace() {
            line_has_content = true;
        }
    }
    if line_has_content {
        count += 1;
    }
    count.max(1)
}

/// Like [`parse_line_body_to_points`] with an explicit capacity hint for the points vector.
pub fn parse_line_body_to_points_with_capacity(
    body: &[u8],
    precision: Option<&str>,
    points_capacity: usize,
) -> Result<Vec<Point>, HyperbytedbError> {
    parse_line_body_inner(body, precision, points_capacity, None)
}

fn parse_line_body_inner(
    body: &[u8],
    precision: Option<&str>,
    points_capacity: usize,
    max_points: Option<usize>,
) -> Result<Vec<Point>, HyperbytedbError> {
    let input = std::str::from_utf8(body).map_err(|e| HyperbytedbError::LineProtocolParse {
        line: String::new(),
        reason: e.to_string(),
    })?;

    let precision_val = Precision::from_str_opt(precision);
    let mut points = Vec::with_capacity(points_capacity);
    for (i, result) in parse_lines(input).enumerate() {
        let parsed = result.map_err(|e| HyperbytedbError::LineProtocolParse {
            line: line_protocol_parse_context(input, i),
            reason: e.to_string(),
        })?;
        let point = parsed_line_to_point(&parsed, &precision_val)?;
        points.push(point);
        if let Some(limit) = max_points
            && points.len() > limit
        {
            return Err(HyperbytedbError::RequestPointLimitExceeded {
                count: points.len(),
                limit,
            });
        }
    }
    Ok(points)
}

fn parsed_line_to_point(
    parsed: &influxdb_line_protocol::ParsedLine<'_>,
    precision: &Precision,
) -> Result<Point, HyperbytedbError> {
    let measurement = parsed.series.measurement.to_string();

    let tags: std::collections::BTreeMap<String, String> = parsed
        .series
        .tag_set
        .as_ref()
        .map(|ts| {
            ts.iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect()
        })
        .unwrap_or_default();

    let fields: std::collections::BTreeMap<String, FieldValue> = parsed
        .field_set
        .iter()
        .map(|(k, v)| {
            let name = k.to_string();
            let val = lp_field_value_to_domain(v);
            (name, val)
        })
        .collect();

    let timestamp_ns = if let Some(ts) = parsed.timestamp {
        precision.to_nanos(ts)
    } else {
        match chrono::Utc::now().timestamp_nanos_opt() {
            Some(ns) => ns,
            None => {
                metrics::counter!("hyperbytedb_line_protocol_timestamp_unavailable_total")
                    .increment(1);
                return Err(HyperbytedbError::WallClockTimestampUnavailable);
            }
        }
    };

    Ok(Point {
        measurement,
        tags,
        fields,
        timestamp: timestamp_ns,
        series_id_cache: Default::default(),
    })
}

fn lp_field_value_to_domain(lp: &LpFieldValue<'_>) -> FieldValue {
    match lp {
        LpFieldValue::I64(i) => FieldValue::Integer(*i),
        LpFieldValue::U64(u) => FieldValue::UInteger(*u),
        LpFieldValue::F64(f) => FieldValue::Float(*f),
        LpFieldValue::String(s) => FieldValue::String(s.to_string()),
        LpFieldValue::Boolean(b) => FieldValue::Boolean(*b),
    }
}

/// Encodes points as Influx line protocol bytes (UTF-8, newline between lines).
/// `precision` controls how [`Point::timestamp`] (nanoseconds) is written on each line so that
/// parsing with the same `precision` query/header round-trips.
pub fn encode_points_to_line_protocol(
    points: &[Point],
    precision: Precision,
) -> Result<Vec<u8>, HyperbytedbError> {
    if points.is_empty() {
        return Ok(Vec::new());
    }

    let mut lines: Vec<String> = Vec::with_capacity(points.len());
    for p in points {
        if p.fields.is_empty() {
            return Err(HyperbytedbError::LineProtocolParse {
                line: p.measurement.clone(),
                reason: "cannot encode point with no fields".into(),
            });
        }

        let mut line = escape_measurement(&p.measurement);
        for (k, v) in &p.tags {
            // Line protocol has no representation for an empty tag value:
            // `cpu,host= value=1` is a parse error, and a peer handed one
            // rejects the entire payload. A tag with no value is a tag the
            // series does not carry, so omit it rather than emit something no
            // parser -- including ours -- will accept.
            if v.is_empty() {
                continue;
            }
            line.push(',');
            line.push_str(&escape_tag_key(k));
            line.push('=');
            line.push_str(&escape_tag_value(v));
        }
        line.push(' ');

        let mut first_field = true;
        for (fk, fv) in &p.fields {
            if !first_field {
                line.push(',');
            }
            first_field = false;
            line.push_str(&escape_field_key(fk));
            line.push('=');
            line.push_str(&field_value_to_lp(fv));
        }

        line.push(' ');
        let ts = precision.from_nanos(p.timestamp);
        line.push_str(&ts.to_string());

        lines.push(line);
    }

    Ok(lines.join("\n").into_bytes())
}

fn field_value_to_lp(fv: &FieldValue) -> String {
    match fv {
        FieldValue::Float(f) => number_to_lp_float(*f),
        FieldValue::Integer(i) => format!("{i}i"),
        FieldValue::UInteger(u) => format!("{u}u"),
        FieldValue::String(s) => format!("\"{}\"", escape_string_field(s)),
        FieldValue::Boolean(b) => {
            if *b {
                "true".into()
            } else {
                "false".into()
            }
        }
    }
}

fn number_to_lp_float(f: f64) -> String {
    if f.fract() == 0.0 && f.abs() < 1e15 {
        format!("{f:.1}")
    } else {
        let mut s = format!("{f:.15}");
        while s.contains('.') && s.ends_with('0') {
            s.pop();
        }
        if s.ends_with('.') {
            s.pop();
        }
        s
    }
}

fn escape_measurement(s: &str) -> String {
    escape_influx_ident(s, &[',', ' '])
}

fn escape_tag_key(s: &str) -> String {
    escape_influx_ident(s, &[',', '=', ' '])
}

fn escape_tag_value(s: &str) -> String {
    escape_influx_ident(s, &[',', '=', ' '])
}

fn escape_field_key(s: &str) -> String {
    escape_influx_ident(s, &[',', '=', ' '])
}

fn escape_influx_ident(s: &str, chars: &[char]) -> String {
    let mut out = String::with_capacity(s.len());
    for ch in s.chars() {
        if chars.contains(&ch) || ch == '\\' {
            out.push('\\');
        }
        out.push(ch);
    }
    out
}

fn escape_string_field(s: &str) -> String {
    s.replace('\\', "\\\\").replace('"', "\\\"")
}

#[cfg(test)]
mod encode_tests {

    /// `cpu,host= value=1` is invalid line protocol: our own parser rejects it,
    /// and a peer handed it rejects the ENTIRE payload with 500. A region
    /// transfer hit exactly this, wedging that region's movement in a retry
    /// loop with no line above debug to say why.
    #[test]
    fn empty_tag_values_are_omitted_not_emitted() {
        use std::collections::BTreeMap;
        let mut tags = BTreeMap::new();
        tags.insert("host".to_string(), String::new());
        tags.insert("region".to_string(), "eu".to_string());
        let mut fields = BTreeMap::new();
        fields.insert("value".to_string(), FieldValue::Float(1.0));
        let p = Point {
            measurement: "cpu".to_string(),
            tags,
            fields,
            timestamp: 1_700_000_000_000_000_000,
            series_id_cache: Default::default(),
        };

        let encoded = encode_points_to_line_protocol(&[p], Precision::Nanosecond).unwrap();
        let text = String::from_utf8(encoded).unwrap();
        assert!(
            !text.contains("host="),
            "empty tag must be omitted entirely, got: {text}"
        );
        assert!(text.contains("region=eu"), "got: {text}");

        // The real requirement: what we emit, we can read back.
        let reparsed = parse_line_body_to_points_limited(text.as_bytes(), None, 10)
            .expect("encoder output must be parseable");
        assert_eq!(reparsed.len(), 1);
        assert_eq!(
            reparsed[0].tags.get("region").map(String::as_str),
            Some("eu")
        );
        assert!(!reparsed[0].tags.contains_key("host"));
    }

    use super::*;

    #[test]
    fn blank_lines_do_not_inflate_capacity_hint() {
        let body = vec![b'\n'; 10_000];
        assert_eq!(estimate_points_capacity(&body), 1);
        let points = parse_line_body_to_points(&body, None).unwrap();
        assert!(points.is_empty());
    }

    #[test]
    fn encode_decode_round_trip_ns() {
        let mut tags = std::collections::BTreeMap::new();
        tags.insert("h".into(), "x".into());
        let mut fields = std::collections::BTreeMap::new();
        fields.insert("v".into(), FieldValue::Float(1.25));
        let p = Point {
            measurement: "m".into(),
            tags,
            fields,
            timestamp: 1_234_567_890_123_456_789,
            series_id_cache: Default::default(),
        };
        let bytes = encode_points_to_line_protocol(std::slice::from_ref(&p), Precision::Nanosecond)
            .unwrap();
        let back = parse_line_body_to_points(&bytes, Some("ns")).unwrap();
        assert_eq!(back.len(), 1);
        assert_eq!(back[0].measurement, "m");
        assert_eq!(back[0].timestamp, p.timestamp);
    }
}

#[cfg(test)]
mod binary_points_tests {
    use super::{encode_points_binary, is_binary_points, parse_line_body_to_points_limited};
    use crate::domain::point::{FieldValue, Point};

    fn pt(host: &str, v: f64, ts: i64) -> Point {
        Point {
            measurement: "cpu".into(),
            tags: [("host".to_string(), host.to_string())].into(),
            fields: [("value".to_string(), FieldValue::Float(v))].into(),
            timestamp: ts,
            series_id_cache: Default::default(),
        }
    }

    #[test]
    fn frames_round_trip_exactly_including_nanoseconds() {
        let points = vec![pt("a", 1.5, 1_700_000_000_123_456_789), pt("b", -2.0, 1)];
        let body = encode_points_binary(&points).unwrap();
        assert!(is_binary_points(&body));
        // Precision is ignored for frames: timestamps are already ns.
        let back = parse_line_body_to_points_limited(&body, Some("s"), 0).unwrap();
        assert_eq!(back.len(), 2);
        assert_eq!(back[0].timestamp, 1_700_000_000_123_456_789);
        assert_eq!(back[1].tags["host"], "b");
    }

    #[test]
    fn coalesced_frames_decode_as_one_body() {
        // Replication coalescing joins bodies with a newline.
        let mut body = encode_points_binary(&[pt("a", 1.0, 1)]).unwrap();
        body.push(b'\n');
        body.extend(encode_points_binary(&[pt("b", 2.0, 2), pt("c", 3.0, 3)]).unwrap());
        let back = parse_line_body_to_points_limited(&body, None, 0).unwrap();
        assert_eq!(back.len(), 3);
    }

    #[test]
    fn the_point_limit_still_applies() {
        let body = encode_points_binary(&[pt("a", 1.0, 1), pt("b", 2.0, 2)]).unwrap();
        assert!(parse_line_body_to_points_limited(&body, None, 1).is_err());
    }

    #[test]
    fn a_body_mixing_frames_and_text_is_rejected() {
        let mut body = encode_points_binary(&[pt("a", 1.0, 1)]).unwrap();
        body.extend_from_slice(b"\ncpu,host=x value=1 1");
        assert!(parse_line_body_to_points_limited(&body, None, 0).is_err());
        // Plain line protocol is untouched.
        assert!(!is_binary_points(b"cpu value=1 1"));
    }
}
