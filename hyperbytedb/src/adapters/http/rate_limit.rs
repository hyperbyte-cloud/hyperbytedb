use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

/// Token-bucket limiter that refills at `max_per_second` tokens per wall-clock second.
/// A rate of 0 lets everything through (`SET rate_limit_per_second = 0`).
#[derive(Debug)]
pub struct TokenBucket {
    max_per_second: AtomicU64,
    state: Mutex<TokenBucketState>,
}

#[derive(Debug)]
struct TokenBucketState {
    tokens: f64,
    last_refill: Instant,
}

impl TokenBucket {
    pub fn new(max_per_second: u64) -> Self {
        Self {
            max_per_second: AtomicU64::new(max_per_second),
            state: Mutex::new(TokenBucketState {
                tokens: max_per_second as f64,
                last_refill: Instant::now(),
            }),
        }
    }

    /// A new rate starts with a full bucket.
    pub fn set_rate(&self, max_per_second: u64) {
        if let Ok(mut state) = self.state.lock() {
            state.tokens = max_per_second as f64;
            state.last_refill = Instant::now();
        }
        self.max_per_second.store(max_per_second, Ordering::Relaxed);
    }

    /// Attempt to consume one token. Returns `true` when allowed.
    pub fn try_acquire(&self) -> bool {
        let rate = self.max_per_second.load(Ordering::Relaxed);
        if rate == 0 {
            return true;
        }
        let max = rate as f64;
        let Ok(mut state) = self.state.lock() else {
            return false;
        };
        let now = Instant::now();
        let elapsed = now.duration_since(state.last_refill).as_secs_f64();
        if elapsed > 0.0 {
            state.tokens = (state.tokens + elapsed * max).min(max);
            state.last_refill = now;
        }
        if state.tokens >= 1.0 {
            state.tokens -= 1.0;
            true
        } else {
            false
        }
    }
}

/// Per-endpoint limiters so `/write` and `/query` each get their own budget.
#[derive(Debug)]
pub struct EndpointRateLimiters {
    pub write: TokenBucket,
    pub query: TokenBucket,
}

impl EndpointRateLimiters {
    pub fn new(max_per_second: u64) -> Self {
        Self {
            write: TokenBucket::new(max_per_second),
            query: TokenBucket::new(max_per_second),
        }
    }

    pub fn set_rate(&self, max_per_second: u64) {
        self.write.set_rate(max_per_second);
        self.query.set_rate(max_per_second);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn a_rate_of_zero_is_unlimited_and_a_set_rate_applies() {
        let bucket = TokenBucket::new(0);
        for _ in 0..1_000 {
            assert!(bucket.try_acquire());
        }
        bucket.set_rate(2);
        assert!(bucket.try_acquire());
        assert!(!(0..10).all(|_| bucket.try_acquire()));
        bucket.set_rate(0);
        assert!(bucket.try_acquire());
    }

    #[test]
    fn allows_burst_up_to_capacity() {
        let bucket = TokenBucket::new(5);
        for _ in 0..5 {
            assert!(bucket.try_acquire());
        }
        assert!(!bucket.try_acquire());
    }

    #[test]
    fn refills_after_one_second() {
        let bucket = TokenBucket::new(5);
        for _ in 0..5 {
            assert!(bucket.try_acquire());
        }
        assert!(!bucket.try_acquire());
        thread::sleep(Duration::from_millis(1100));
        assert!(bucket.try_acquire());
    }

    #[test]
    fn write_and_query_have_independent_budgets() {
        let limiters = EndpointRateLimiters::new(2);
        assert!(limiters.write.try_acquire());
        assert!(limiters.write.try_acquire());
        assert!(!limiters.write.try_acquire());
        assert!(limiters.query.try_acquire());
    }
}
