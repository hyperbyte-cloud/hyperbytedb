use std::collections::BTreeMap;
use std::hash::{Hash, Hasher};

use crate::domain::point::Point;

/// Unique identifier for a time series: measurement name + sorted tag key=value pairs.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SeriesKey {
    pub measurement: String,
    pub tags: BTreeMap<String, String>,
}

impl SeriesKey {
    pub fn new(measurement: &str, tags: &BTreeMap<String, String>) -> Self {
        Self {
            measurement: measurement.to_string(),
            tags: tags.clone(),
        }
    }

    /// Canonical string: "measurement,tag1=val1,tag2=val2"
    pub fn to_canonical(&self) -> String {
        let mut s = self.measurement.clone();
        for (k, v) in &self.tags {
            s.push(',');
            s.push_str(k);
            s.push('=');
            s.push_str(v);
        }
        s
    }

    /// Stable 64-bit identifier for this series. See [`series_id`].
    #[must_use]
    pub fn id(&self) -> u64 {
        series_id(&self.measurement, &self.tags)
    }
}

/// Deterministic 64-bit hash of a series key (measurement + sorted tag k=v pairs).
///
/// This is the physical `series_id` stored on every fact row and used as the key of
/// the per-measurement `_series` dimension table. It is **stable across processes and
/// nodes** for the same logical series, so each node can register series locally
/// without coordination. It deliberately does NOT fold in the timestamp (unlike the
/// coalescing `series_instant_hash`).
///
/// `tags` is a `BTreeMap`, so iteration is already sorted by key — matching
/// [`SeriesKey::to_canonical`]'s ordering. A `0xFF` separator between key and value and
/// a `0xFE` separator between pairs prevent concatenation collisions (e.g. `{"ab"="c"}`
/// vs `{"a"="bc"}`). An empty tag set yields a well-defined id (hash of the measurement
/// alone), so no-tag measurements still get a `series_id`.
#[must_use]
pub fn series_id(measurement: &str, tags: &BTreeMap<String, String>) -> u64 {
    let mut h = std::collections::hash_map::DefaultHasher::new();
    measurement.hash(&mut h);
    for (k, v) in tags {
        k.hash(&mut h);
        0xFFu8.hash(&mut h);
        v.hash(&mut h);
        0xFEu8.hash(&mut h);
    }
    h.finish()
}

/// ClickHouse `sipHash64` expression for a destination measurement's rolled-up
/// series key from grouped tag column references (sorted by tag key name).
///
/// Used by materialized views that drop tags from the source series (e.g. omit
/// `server_id`) so destination `series_id` values are distinct from the source.
#[must_use]
pub fn series_id_ch_sql(measurement: &str, sorted_tag_column_sql: &[String]) -> String {
    let meas = measurement.replace('\\', "\\\\").replace('\'', "\\'");
    let mut expr = format!("'{meas}'");
    for col in sorted_tag_column_sql {
        expr = format!("concat({expr}, char(255), toString({col}))");
    }
    format!("sipHash64({expr})")
}

/// Convenience wrapper: build [`series_id_ch_sql`] from logical tag keys and a
/// table alias prefix (`s` → `s.`column``).
#[must_use]
pub fn series_id_ch_sql_for_tags(
    measurement: &str,
    grouped_tag_keys: &[String],
    tag_column: impl Fn(&str) -> String,
    table_alias: &str,
) -> String {
    let mut keys = grouped_tag_keys.to_vec();
    keys.sort();
    let cols: Vec<String> = keys
        .iter()
        .map(|k| format!("{table_alias}.{}", tag_column(k)))
        .collect();
    series_id_ch_sql(measurement, &cols)
}

/// Convenience wrapper computing [`series_id`] straight from a [`Point`]. `Point::tags`
/// is already a sorted `BTreeMap`, so this is allocation-free.
#[must_use]
pub fn series_id_for_point(p: &Point) -> u64 {
    series_id(&p.measurement, &p.tags)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tags(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn series_id_is_deterministic_and_order_independent() {
        let a = series_id("cpu", &tags(&[("host", "h1"), ("region", "us")]));
        let b = series_id("cpu", &tags(&[("region", "us"), ("host", "h1")]));
        assert_eq!(a, b, "BTreeMap normalises order");
    }

    #[test]
    fn series_id_distinguishes_tags_and_measurements() {
        let base = series_id("cpu", &tags(&[("host", "h1")]));
        assert_ne!(base, series_id("cpu", &tags(&[("host", "h2")])));
        assert_ne!(base, series_id("mem", &tags(&[("host", "h1")])));
        // Empty tag set is well-defined and distinct from any tagged series.
        assert_ne!(base, series_id("cpu", &BTreeMap::new()));
    }

    #[test]
    fn series_id_separator_prevents_concatenation_collision() {
        // Without separators these would hash the same byte stream.
        assert_ne!(
            series_id("m", &tags(&[("ab", "c")])),
            series_id("m", &tags(&[("a", "bc")])),
        );
    }

    #[test]
    fn series_id_ch_sql_uses_sip_hash_and_tag_columns() {
        let sql = series_id_ch_sql("cpu_1m", &["s.\"host\"".to_string()]);
        assert!(sql.starts_with("sipHash64("));
        assert!(sql.contains("char(255)"));
        assert!(sql.contains("s.\"host\""));
    }
}
