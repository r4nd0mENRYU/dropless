//! Per-tenant ingest rate limiting — an in-memory token bucket.
//!
//! Protects the ingest path (`POST /v1/messages`) from a single tenant
//! overwhelming the queue. Disabled by default (rps = 0); opt in with
//! `RATE_LIMIT_RPS` / `RATE_LIMIT_BURST`. This is per-process; a multi-node API
//! deployment would need a shared limiter (e.g. Redis) — noted as future work.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Instant;

/// A token-bucket rate limiter keyed by tenant.
pub struct RateLimiter {
    rps: f64,
    burst: f64,
    buckets: Mutex<HashMap<String, Bucket>>,
}

struct Bucket {
    tokens: f64,
    last: Instant,
}

impl RateLimiter {
    /// `rps` tokens/sec refill, up to `burst` capacity. `rps <= 0` disables it.
    pub fn new(rps: f64, burst: f64) -> Self {
        RateLimiter {
            rps,
            burst: burst.max(1.0),
            buckets: Mutex::new(HashMap::new()),
        }
    }

    /// Whether limiting is active.
    pub fn enabled(&self) -> bool {
        self.rps > 0.0
    }

    /// Try to consume one token for `key`. Returns `None` if allowed, or
    /// `Some(retry_after_secs)` (>= 1) if the bucket is empty.
    pub fn check(&self, key: &str) -> Option<f64> {
        self.check_at(key, Instant::now())
    }

    fn check_at(&self, key: &str, now: Instant) -> Option<f64> {
        if self.rps <= 0.0 {
            return None;
        }
        let mut buckets = self.buckets.lock().unwrap();
        let bucket = buckets.entry(key.to_string()).or_insert(Bucket {
            tokens: self.burst,
            last: now,
        });
        // Refill based on elapsed time, capped at burst.
        let elapsed = now.saturating_duration_since(bucket.last).as_secs_f64();
        bucket.tokens = (bucket.tokens + elapsed * self.rps).min(self.burst);
        bucket.last = now;

        if bucket.tokens >= 1.0 {
            bucket.tokens -= 1.0;
            None
        } else {
            // Seconds until one token is available again.
            Some(((1.0 - bucket.tokens) / self.rps).ceil().max(1.0))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn disabled_allows_everything() {
        let rl = RateLimiter::new(0.0, 0.0);
        assert!(!rl.enabled());
        for _ in 0..1000 {
            assert!(rl.check("t").is_none());
        }
    }

    #[test]
    fn burst_then_throttle_then_refill() {
        let rl = RateLimiter::new(10.0, 5.0); // 10/s, burst 5
        let t0 = Instant::now();
        // Burst of 5 is allowed immediately.
        for _ in 0..5 {
            assert!(rl.check_at("t", t0).is_none());
        }
        // The 6th in the same instant is throttled.
        let retry = rl.check_at("t", t0).expect("should be limited");
        assert!(retry >= 1.0);
        // After 1s, ~10 tokens refilled (capped at 5) → allowed again.
        let t1 = t0 + Duration::from_secs(1);
        assert!(rl.check_at("t", t1).is_none());
    }

    #[test]
    fn tenants_are_isolated() {
        let rl = RateLimiter::new(1.0, 1.0);
        let t0 = Instant::now();
        assert!(rl.check_at("a", t0).is_none());
        assert!(rl.check_at("a", t0).is_some()); // a is now empty
        assert!(rl.check_at("b", t0).is_none()); // b unaffected
    }
}
