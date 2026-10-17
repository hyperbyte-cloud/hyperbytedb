//! Application pieces the server runs: the control runtime, region write
//! statistics and the region heartbeat reporter. The process itself is
//! started from [`crate::server`].

pub mod control;
mod region_write_stats;
pub(crate) mod shard_heartbeat;

pub use region_write_stats::RegionWriteStats;
