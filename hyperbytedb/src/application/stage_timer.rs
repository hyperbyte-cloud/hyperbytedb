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

/// Measure how late the async runtime wakes a task: sleep 10 ms, record how
/// much longer it took. Starved workers (blocked on a lock, or the whole
/// pod frozen by its CPU quota) show up here whatever the cause.
pub fn spawn_scheduling_probe() {
    tokio::spawn(async {
        let nominal = std::time::Duration::from_millis(10);
        loop {
            let start = Instant::now();
            tokio::time::sleep(nominal).await;
            let late = start.elapsed().saturating_sub(nominal);
            metrics::histogram!("hyperbytedb_runtime_wake_delay_seconds")
                .record(late.as_secs_f64());
            tokio::time::sleep(std::time::Duration::from_millis(40)).await;
        }
    });
}
