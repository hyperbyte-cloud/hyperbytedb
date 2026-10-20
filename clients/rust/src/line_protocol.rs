//! InfluxDB line protocol, as HyperbyteDB parses it.
//!
//! A field keeps the type it was first written with: write `1.0`, not `1`,
//! for a float field. NaN and infinite floats are left out, and so are tags
//! with an empty value: line protocol cannot express them.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::error::{Error, Result};

/// A field value.
#[derive(Debug, Clone, PartialEq)]
pub enum FieldValue {
    Float(f64),
    Integer(i64),
    UInteger(u64),
    Boolean(bool),
    String(String),
}

macro_rules! from_field {
    ($($t:ty => $v:ident as $as:ty),* $(,)?) => {
        $(impl From<$t> for FieldValue {
            fn from(x: $t) -> Self {
                FieldValue::$v(x as $as)
            }
        })*
    };
}
from_field!(f64 => Float as f64, f32 => Float as f64, i64 => Integer as i64, i32 => Integer as i64,
    i16 => Integer as i64, i8 => Integer as i64, u64 => UInteger as u64, u32 => UInteger as u64,
    u16 => UInteger as u64, u8 => UInteger as u64);

impl From<bool> for FieldValue {
    fn from(x: bool) -> Self {
        FieldValue::Boolean(x)
    }
}
impl From<&str> for FieldValue {
    fn from(x: &str) -> Self {
        FieldValue::String(x.to_owned())
    }
}
impl From<String> for FieldValue {
    fn from(x: String) -> Self {
        FieldValue::String(x)
    }
}

/// The unit of written timestamps.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Precision {
    #[default]
    Nanoseconds,
    Microseconds,
    Milliseconds,
    Seconds,
}

impl Precision {
    pub fn as_str(self) -> &'static str {
        match self {
            Precision::Nanoseconds => "ns",
            Precision::Microseconds => "u",
            Precision::Milliseconds => "ms",
            Precision::Seconds => "s",
        }
    }

    fn per_nanos(self) -> i128 {
        match self {
            Precision::Nanoseconds => 1,
            Precision::Microseconds => 1_000,
            Precision::Milliseconds => 1_000_000,
            Precision::Seconds => 1_000_000_000,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
enum Time {
    /// In the write's precision units.
    Raw(i64),
    At(SystemTime),
}

/// One point: a measurement, its tags, its fields and an optional time.
///
/// ```
/// use hyperbytedb_client::Point;
/// let p = Point::new("cpu").tag("host", "web1").field("usage", 12.5).field("cores", 8i64);
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct Point {
    measurement: String,
    tags: BTreeMap<String, String>,
    fields: BTreeMap<String, FieldValue>,
    time: Option<Time>,
}

impl Point {
    pub fn new(measurement: impl Into<String>) -> Self {
        Point {
            measurement: measurement.into(),
            tags: BTreeMap::new(),
            fields: BTreeMap::new(),
            time: None,
        }
    }

    #[must_use]
    pub fn tag(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.tags.insert(key.into(), value.into());
        self
    }

    #[must_use]
    pub fn field(mut self, key: impl Into<String>, value: impl Into<FieldValue>) -> Self {
        self.fields.insert(key.into(), value.into());
        self
    }

    /// A time in the write's precision units.
    #[must_use]
    pub fn timestamp(mut self, t: i64) -> Self {
        self.time = Some(Time::Raw(t));
        self
    }

    /// A wall-clock time, converted to the write's precision.
    #[must_use]
    pub fn at(mut self, t: SystemTime) -> Self {
        self.time = Some(Time::At(t));
        self
    }

    /// The point as one line, or `None` when it has no field left to write.
    pub fn to_line(&self, precision: Precision) -> Result<Option<String>> {
        let mut fields = String::new();
        for (k, v) in &self.fields {
            let Some(v) = field_value(v)? else { continue };
            if !fields.is_empty() {
                fields.push(',');
            }
            escape(&mut fields, k, ",= ")?;
            fields.push('=');
            fields.push_str(&v);
        }
        if fields.is_empty() {
            return Ok(None);
        }
        let mut line = String::new();
        escape(&mut line, &self.measurement, ", ")?;
        for (k, v) in &self.tags {
            if v.is_empty() {
                continue;
            }
            line.push(',');
            escape(&mut line, k, ",= ")?;
            line.push('=');
            escape(&mut line, v, ",= ")?;
        }
        line.push(' ');
        line.push_str(&fields);
        match &self.time {
            None => {}
            Some(Time::Raw(t)) => {
                let _ = write!(line, " {t}");
            }
            Some(Time::At(t)) => {
                let nanos = match t.duration_since(UNIX_EPOCH) {
                    Ok(d) => d.as_nanos() as i128,
                    Err(e) => -(e.duration().as_nanos() as i128),
                };
                let _ = write!(line, " {}", nanos.div_euclid(precision.per_nanos()));
            }
        }
        Ok(Some(line))
    }
}

fn escape(out: &mut String, s: &str, special: &str) -> Result<()> {
    for ch in s.chars() {
        if ch == '\n' {
            // A newline ends the line; it has no escape.
            return Err(Error::InvalidPoint(format!(
                "newline in line protocol name or tag: {s:?}"
            )));
        }
        if ch == '\\' || special.contains(ch) {
            out.push('\\');
        }
        out.push(ch);
    }
    Ok(())
}

fn field_value(v: &FieldValue) -> Result<Option<String>> {
    Ok(Some(match v {
        FieldValue::Float(f) if !f.is_finite() => return Ok(None),
        FieldValue::Float(f) => format!("{f:?}"),
        FieldValue::Integer(i) => format!("{i}i"),
        FieldValue::UInteger(u) if *u > i64::MAX as u64 => {
            return Err(Error::InvalidPoint(format!(
                "integer field {u} overflows int64"
            )));
        }
        FieldValue::UInteger(u) => format!("{u}i"),
        FieldValue::Boolean(b) => b.to_string(),
        FieldValue::String(s) => {
            if s.contains('\n') {
                // A newline ends the line even inside quotes; there is no escape.
                return Err(Error::InvalidPoint(format!(
                    "newline in line protocol string field: {s:?}"
                )));
            }
            format!("\"{}\"", s.replace('\\', "\\\\").replace('"', "\\\""))
        }
    }))
}

/// Points as line protocol, one per line; points with no fields dropped.
pub fn encode<'a>(
    points: impl IntoIterator<Item = &'a Point>,
    precision: Precision,
) -> Result<String> {
    let mut lines = Vec::new();
    for p in points {
        if let Some(line) = p.to_line(precision)? {
            lines.push(line);
        }
    }
    Ok(lines.join("\n"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn escapes_like_the_server() {
        let p = Point::new("cpu load,x")
            .tag("host name", "a,b=c")
            .tag("empty", "")
            .field("usage idle", 1.5)
            .field("msg\"", r#"say "hi" \ bye"#)
            .timestamp(123);
        assert_eq!(
            p.to_line(Precision::Nanoseconds).unwrap().unwrap(),
            r#"cpu\ load\,x,host\ name=a\,b\=c msg"="say \"hi\" \\ bye",usage\ idle=1.5 123"#
        );
    }

    #[test]
    fn field_types() {
        let p = Point::new("m")
            .field("f", 1.0)
            .field("i", 2i64)
            .field("u", 3u8)
            .field("b", true)
            .field("s", "x")
            .field("big", 1e21);
        // `{:?}` keeps a float a float: 1.0, not 1.
        assert_eq!(
            p.to_line(Precision::Nanoseconds).unwrap().unwrap(),
            r#"m b=true,big=1e21,f=1.0,i=2i,s="x",u=3i"#
        );
        assert!(Point::new("m")
            .field("u", u64::MAX)
            .to_line(Precision::Nanoseconds)
            .is_err());
    }

    #[test]
    fn non_finite_floats_are_dropped() {
        let p = Point::new("m")
            .field("a", f64::NAN)
            .field("b", f64::INFINITY)
            .field("d", 1.5);
        assert_eq!(
            p.to_line(Precision::Nanoseconds).unwrap().unwrap(),
            "m d=1.5"
        );
        assert_eq!(
            Point::new("m")
                .field("a", f64::NAN)
                .to_line(Precision::Nanoseconds)
                .unwrap(),
            None
        );
        let both = [
            Point::new("m").field("a", f64::NAN),
            Point::new("m").field("v", 1.5),
        ];
        assert_eq!(encode(&both, Precision::Nanoseconds).unwrap(), "m v=1.5");
    }

    #[test]
    fn system_times_follow_the_precision() {
        let t = UNIX_EPOCH + Duration::from_millis(1_791_806_400_500);
        let p = Point::new("m").field("v", 1.5).at(t);
        let at = |prec| p.to_line(prec).unwrap().unwrap();
        assert_eq!(at(Precision::Nanoseconds), "m v=1.5 1791806400500000000");
        assert_eq!(at(Precision::Microseconds), "m v=1.5 1791806400500000");
        assert_eq!(at(Precision::Milliseconds), "m v=1.5 1791806400500");
        assert_eq!(at(Precision::Seconds), "m v=1.5 1791806400");
        let before = Point::new("m")
            .field("v", 1.5)
            .at(UNIX_EPOCH - Duration::from_millis(1500));
        assert_eq!(
            before.to_line(Precision::Seconds).unwrap().unwrap(),
            "m v=1.5 -2",
            "floored, as a time"
        );
    }

    #[test]
    fn newlines_are_refused() {
        assert!(Point::new("m")
            .tag("host", "a\nb")
            .field("v", 1.0)
            .to_line(Precision::Nanoseconds)
            .is_err());
        assert!(Point::new("m")
            .field("msg", "two\nlines")
            .to_line(Precision::Nanoseconds)
            .is_err());
    }
}
