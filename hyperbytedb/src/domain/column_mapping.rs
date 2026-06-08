//! Physical column naming: Influx allows the same identifier as a tag key and a field key.
//! ClickHouse requires unique column names, so we rename tag columns when they collide.

use std::collections::HashSet;
use std::hash::{Hash, Hasher};

use crate::domain::measurement::MeasurementMeta;

/// Prefix for tag columns when the tag key collides with a field name.
pub const TAG_COL_PREFIX: &str = "__tag__";

/// Physical column name for a tag key given known field names on the measurement.
#[must_use]
pub fn tag_column_name(tag_key: &str, field_names: &HashSet<&str>) -> String {
    if field_names.contains(tag_key) {
        format!("{TAG_COL_PREFIX}{tag_key}")
    } else {
        tag_key.to_string()
    }
}
/// Column name for a tag key when the only field in the batch is `field_name`.
#[must_use]
pub fn tag_col_name_for_columnar(tag_key: &str, field_name: &str) -> String {
    if tag_key == field_name {
        format!("{TAG_COL_PREFIX}{tag_key}")
    } else {
        tag_key.to_string()
    }
}

/// Metadata needed to map TimeseriesQL identifiers to physical ClickHouse column names.
#[derive(Debug, Clone, Default)]
pub struct ColumnMapping {
    pub tag_keys: HashSet<String>,
    pub field_names: HashSet<String>,
}

/// Fingerprint of measurement schema for query-side [`ColumnMapping`] cache invalidation.
#[must_use]
pub fn measurement_meta_fingerprint(m: &MeasurementMeta) -> u64 {
    let mut h = std::collections::hash_map::DefaultHasher::new();
    let mut fields: Vec<(&String, &u8)> = m.field_types.iter().collect();
    fields.sort_by_key(|(k, _)| *k);
    for (k, v) in fields {
        k.hash(&mut h);
        v.hash(&mut h);
    }
    let mut tags: Vec<_> = m.tag_keys.iter().collect();
    tags.sort();
    for k in tags {
        k.hash(&mut h);
    }
    h.finish()
}

impl ColumnMapping {
    #[must_use]
    pub fn from_measurement_meta(m: &MeasurementMeta) -> Self {
        Self {
            tag_keys: m.tag_keys.iter().cloned().collect(),
            field_names: m.field_types.keys().cloned().collect(),
        }
    }

    #[must_use]
    pub fn tag_column_name(&self, tag_key: &str) -> String {
        let fields: HashSet<&str> = self.field_names.iter().map(|s| s.as_str()).collect();
        tag_column_name(tag_key, &fields)
    }
    /// SELECT / aggregate: prefer field column when tag and field share a name.
    #[must_use]
    pub fn physical_select_identifier(&self, name: &str) -> String {
        if self.field_names.contains(name) {
            name.to_string()
        } else if self.tag_keys.contains(name) {
            self.tag_column_name(name)
        } else {
            name.to_string()
        }
    }
}
