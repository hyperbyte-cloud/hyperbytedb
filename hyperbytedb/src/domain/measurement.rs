use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MeasurementMeta {
    pub name: String,
    pub field_types: HashMap<String, u8>,
    pub tag_keys: Vec<String>,
}

impl MeasurementMeta {
    pub fn field_types_as_tuples(&self) -> Vec<(String, u8)> {
        self.field_types
            .iter()
            .map(|(k, v)| (k.clone(), *v))
            .collect()
    }
}
