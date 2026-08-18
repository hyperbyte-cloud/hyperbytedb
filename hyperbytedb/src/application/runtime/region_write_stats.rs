//! Per-region write counters for shard heartbeat QPS reporting.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

#[derive(Default)]
pub struct RegionWriteStats {
    inner: Mutex<HashMap<u64, (u64, Instant)>>,
}

impl RegionWriteStats {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record(&self, region_id: u64, points: u64) {
        if points == 0 {
            return;
        }
        let mut guard = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let entry = guard.entry(region_id).or_insert((0, Instant::now()));
        entry.0 = entry.0.saturating_add(points);
    }

    /// Returns writes-per-second over the elapsed window and resets the counter.
    pub fn take_qps(&self, region_id: u64, window: Duration) -> u64 {
        let mut guard = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let Some((count, started)) = guard.remove(&region_id) else {
            return 0;
        };
        let secs = started.elapsed().max(window).as_secs_f64();
        if secs <= 0.0 {
            return count;
        }
        (count as f64 / secs).round() as u64
    }
}
