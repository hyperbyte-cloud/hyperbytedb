//! Default field values for [`super::router::AppState`] construction in tests.

use std::sync::Arc;

use crate::application::ingest_metadata::IngestCardinalityLimits;
use crate::application::shard_routing::ShardRoutingContext;
use crate::application::shard_scheduler::ShardScheduler;
use crate::config::ReplicationConfig;
use crate::domain::sharding::ShardLocationCache;
use crate::ports::sharding::ShardMapPort;

/// Sharding-related AppState fields when sharding is disabled (default for tests).
#[derive(Clone)]
pub struct ShardingAppDefaults {
    pub sharding_enabled: bool,
    pub shard_map: Option<Arc<dyn ShardMapPort>>,
    pub shard_location_cache: Arc<ShardLocationCache>,
    pub shard_routing: Option<Arc<ShardRoutingContext>>,
    pub shard_scheduler: Option<Arc<ShardScheduler>>,
    pub ingest_cardinality: IngestCardinalityLimits,
    pub cluster_replication: ReplicationConfig,
}

impl Default for ShardingAppDefaults {
    fn default() -> Self {
        Self {
            sharding_enabled: false,
            shard_map: None,
            shard_location_cache: Arc::new(ShardLocationCache::new()),
            shard_routing: None,
            shard_scheduler: None,
            ingest_cardinality: IngestCardinalityLimits::default(),
            cluster_replication: ReplicationConfig::default(),
        }
    }
}
