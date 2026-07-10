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
    let cap = estimate_points_capacity(body).min(limit);
    parse_line_body_inner(body, precision, cap, Some(limit))
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
        };
        let bytes = encode_points_to_line_protocol(std::slice::from_ref(&p), Precision::Nanosecond)
            .unwrap();
        let back = parse_line_body_to_points(&bytes, Some("ns")).unwrap();
        assert_eq!(back.len(), 1);
        assert_eq!(back[0].measurement, "m");
        assert_eq!(back[0].timestamp, p.timestamp);
    }
}
