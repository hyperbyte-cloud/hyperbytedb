use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum FieldValue {
    Float(f64),
    Integer(i64),
    UInteger(u64),
    String(String),
    Boolean(bool),
}

impl FieldValue {
    pub fn type_name(&self) -> &'static str {
        match self {
            FieldValue::Float(_) => "float",
            FieldValue::Integer(_) => "integer",
            FieldValue::UInteger(_) => "unsigned",
            FieldValue::String(_) => "string",
            FieldValue::Boolean(_) => "boolean",
        }
    }

    pub fn type_discriminant(&self) -> u8 {
        match self {
            FieldValue::Float(_) => 0,
            FieldValue::Integer(_) => 1,
            FieldValue::UInteger(_) => 2,
            FieldValue::String(_) => 3,
            FieldValue::Boolean(_) => 4,
        }
    }

    pub fn type_name_from_discriminant(d: u8) -> &'static str {
        match d {
            0 => "float",
            1 => "integer",
            2 => "unsigned",
            3 => "string",
            4 => "boolean",
            _ => "unknown",
        }
    }

    pub fn as_f64(&self) -> Option<f64> {
        match self {
            FieldValue::Float(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_i64(&self) -> Option<i64> {
        match self {
            FieldValue::Integer(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_u64(&self) -> Option<u64> {
        match self {
            FieldValue::UInteger(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            FieldValue::String(v) => Some(v.as_str()),
            _ => None,
        }
    }

    pub fn as_bool(&self) -> Option<bool> {
        match self {
            FieldValue::Boolean(v) => Some(*v),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Point {
    pub measurement: String,
    pub tags: BTreeMap<String, String>,
    pub fields: BTreeMap<String, FieldValue>,
    pub timestamp: i64, // always nanoseconds since epoch
}
