use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::domain::rollup::{MeanRollupField, RollupCombine};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MeasurementMeta {
    pub name: String,
    pub field_types: HashMap<String, u8>,
    pub tag_keys: Vec<String>,
    /// Per-field merge semantics for MV / rollup destination measurements.
    #[serde(default)]
    pub field_rollups: HashMap<String, RollupCombine>,
    /// Logical fields exposed as `mean(x)` mapped to stored sum/count columns.
    #[serde(default)]
    pub mean_fields: HashMap<String, MeanRollupField>,
    /// True when this measurement is a materialized view destination.
    /// Raw writes to materialized destinations are rejected.
    #[serde(default)]
    pub materialized: bool,
    /// When materialized, the retention policy of the MV destination.
    /// Only writes whose RP matches this value are rejected; writes to
    /// other RPs (e.g. autogen) with the same measurement name are allowed.
    /// `None` (legacy entries) conservatively rejects all RPs.
    #[serde(default)]
    pub materialized_rp: Option<String>,
}

impl MeasurementMeta {
    pub fn field_types_as_tuples(&self) -> Vec<(String, u8)> {
        self.field_types
            .iter()
            .map(|(k, v)| (k.clone(), *v))
            .collect()
    }
}
