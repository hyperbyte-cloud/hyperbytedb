use async_trait::async_trait;
use std::sync::Arc;

use crate::domain::sharding::{ShardMap, ShardMapOp, ShardRegion};
use crate::error::HyperbytedbError;

#[async_trait]
pub trait ShardMapPort: Send + Sync {
    fn enabled(&self) -> bool;

    async fn snapshot(&self) -> Result<ShardMap, HyperbytedbError>;

    async fn locate(
        &self,
        db: &str,
        rp: &str,
        measurement: &str,
        series_id: u64,
    ) -> Result<Option<ShardRegion>, HyperbytedbError>;

    async fn regions_for_measurement(
        &self,
        db: &str,
        rp: &str,
        measurement: &str,
    ) -> Result<Vec<ShardRegion>, HyperbytedbError>;

    async fn apply_op(&self, op: ShardMapOp) -> Result<ShardMap, HyperbytedbError>;

    /// Returns true when this node holds a replica of any series for the measurement.
    async fn node_owns_measurement(
        &self,
        node_id: u64,
        db: &str,
        rp: &str,
        measurement: &str,
    ) -> Result<bool, HyperbytedbError>;
}

/// No-op implementation used when `[sharding] enabled = false`.
pub struct DisabledShardMap;

#[async_trait]
impl ShardMapPort for DisabledShardMap {
    fn enabled(&self) -> bool {
        false
    }

    async fn snapshot(&self) -> Result<ShardMap, HyperbytedbError> {
        Ok(ShardMap::default())
    }

    async fn locate(
        &self,
        _db: &str,
        _rp: &str,
        _measurement: &str,
        _series_id: u64,
    ) -> Result<Option<ShardRegion>, HyperbytedbError> {
        Ok(None)
    }

    async fn regions_for_measurement(
        &self,
        _db: &str,
        _rp: &str,
        _measurement: &str,
    ) -> Result<Vec<ShardRegion>, HyperbytedbError> {
        Ok(vec![])
    }

    async fn apply_op(&self, _op: ShardMapOp) -> Result<ShardMap, HyperbytedbError> {
        Err(HyperbytedbError::Internal(
            "sharding is disabled".into(),
        ))
    }

    async fn node_owns_measurement(
        &self,
        _node_id: u64,
        _db: &str,
        _rp: &str,
        _measurement: &str,
    ) -> Result<bool, HyperbytedbError> {
        Ok(true)
    }
}

pub type SharedShardMapPort = Arc<dyn ShardMapPort>;
