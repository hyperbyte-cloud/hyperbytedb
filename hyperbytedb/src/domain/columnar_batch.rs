//! The columnar MessagePack write batch (format v1): one measurement, one
//! float field, shared tags. A wire format the ingest path decodes and the
//! chDB sink writes, so it lives in the domain.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Wire map for columnar msgpack v1 (MessagePack map, not a row array).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnarMsgpackBatch {
    pub measurement: String,
    #[serde(default)]
    pub tags: BTreeMap<String, String>,
    pub field: String,
    pub values: Vec<f64>,
    pub timestamps: Option<Vec<i64>>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn batch_round_trips_through_msgpack() {
        let batch = ColumnarMsgpackBatch {
            measurement: "cpu".into(),
            tags: [("host".to_string(), "a".to_string())].into(),
            field: "v".into(),
            values: vec![1.0, 2.0],
            timestamps: Some(vec![1, 2]),
        };
        let bytes = rmp_serde::to_vec_named(&batch).unwrap();
        let back: ColumnarMsgpackBatch = rmp_serde::from_slice(&bytes).unwrap();
        assert_eq!(back.values, vec![1.0, 2.0]);
        assert_eq!(back.tags["host"], "a");
    }
}
