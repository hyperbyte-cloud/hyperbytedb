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

/// Nanosecond timestamps for every row in a columnar batch.
pub fn columnar_timestamps_ns(
    wire: &ColumnarMsgpackBatch,
    precision: Option<&str>,
) -> Result<Vec<i64>, crate::error::HyperbytedbError> {
    let n = wire.values.len();
    if n == 0 {
        return Ok(Vec::new());
    }

    let precision_val = crate::domain::database::Precision::from_str_opt(precision);

    if let Some(ref ts) = wire.timestamps {
        if ts.len() != n {
            return Err(crate::error::HyperbytedbError::ColumnarMsgpackParse {
                reason: format!(
                    "timestamps length {} does not match values length {}",
                    ts.len(),
                    n
                ),
            });
        }
        return Ok(ts.iter().map(|t| precision_val.to_nanos(*t)).collect());
    }

    let now = chrono::Utc::now()
        .timestamp_nanos_opt()
        .ok_or(crate::error::HyperbytedbError::WallClockTimestampUnavailable)?;
    Ok(vec![now; n])
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
