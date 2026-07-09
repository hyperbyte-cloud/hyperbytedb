use std::sync::Mutex;
use std::time::Instant;

/// Token-bucket limiter that refills at `max_per_second` tokens per wall-clock second.
#[derive(Debug)]
pub struct TokenBucket {
    max_per_second: f64,
    state: Mutex<TokenBucketState>,
}

#[derive(Debug)]
struct TokenBucketState {
    tokens: f64,
    last_refill: Instant,
}

impl TokenBucket {
    pub fn new(max_per_second: u64) -> Self {
        let max = max_per_second as f64;
        Self {
            max_per_second: max,
            state: Mutex::new(TokenBucketState {
                tokens: max,
                last_refill: Instant::now(),
            }),
        }
    }

    /// Attempt to consume one token. Returns `true` when allowed.
    pub fn try_acquire(&self) -> bool {
        let Ok(mut state) = self.state.lock() else {
            return false;
        };
        let now = Instant::now();
        let elapsed = now.duration_since(state.last_refill).as_secs_f64();
        if elapsed > 0.0 {
            state.tokens = (state.tokens + elapsed * self.max_per_second).min(self.max_per_second);
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

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
