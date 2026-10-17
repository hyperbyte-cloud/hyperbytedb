//! A runtime setting's value (`SET <name> = <value>`).

use serde::{Deserialize, Serialize};

/// Untagged so a stored integer (`6`), boolean (`true`) or text (`"debug"`)
/// reads back as itself.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum SettingValue {
    Int(i64),
    Bool(bool),
    Text(String),
}

impl SettingValue {
    #[must_use]
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            SettingValue::Int(v) => Some(*v),
            _ => None,
        }
    }

    #[must_use]
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            SettingValue::Bool(v) => Some(*v),
            _ => None,
        }
    }

    #[must_use]
    pub fn as_text(&self) -> Option<&str> {
        match self {
            SettingValue::Text(v) => Some(v),
            _ => None,
        }
    }

    #[must_use]
    pub fn to_json(&self) -> serde_json::Value {
        match self {
            SettingValue::Int(v) => serde_json::Value::from(*v),
            SettingValue::Bool(v) => serde_json::Value::Bool(*v),
            SettingValue::Text(v) => serde_json::Value::String(v.clone()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn values_round_trip_through_json() {
        for (v, json) in [
            (SettingValue::Int(6), "6"),
            (SettingValue::Bool(true), "true"),
            (SettingValue::Text("debug".into()), "\"debug\""),
        ] {
            assert_eq!(serde_json::to_string(&v).unwrap(), json);
            assert_eq!(serde_json::from_str::<SettingValue>(json).unwrap(), v);
        }
    }
}
