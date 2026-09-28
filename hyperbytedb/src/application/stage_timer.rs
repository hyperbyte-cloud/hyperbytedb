//! Per-stage latency for a request path, recorded into one histogram with a
//! `stage` label.

use std::time::Instant;

/// Records the time since the previous lap (or since creation) under a
/// stage name.
pub struct StageTimer {
    metric: &'static str,
    last: Instant,
}

impl StageTimer {
    #[must_use]
    pub fn new(metric: &'static str) -> Self {
        Self {
            metric,
            last: Instant::now(),
        }
    }

    /// Record the time since the last lap as `stage`, and start the next.
    pub fn lap(&mut self, stage: &'static str) {
        let now = Instant::now();
        metrics::histogram!(self.metric, "stage" => stage)
            .record(now.duration_since(self.last).as_secs_f64());
        self.last = now;
    }
}
