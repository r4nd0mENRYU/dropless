//! Retry scheduling: exponential backoff with full jitter (AWS-style).
//!
//! Full jitter spreads retries across `[0, computed]` to avoid the thundering
//! herd when a downstream recovers. Bounded by [`RetryPolicy::max_delay`] and
//! [`RetryPolicy::max_attempts`].

use std::time::Duration;

use rand::Rng;

/// Configuration for the backoff schedule.
#[derive(Debug, Clone, Copy)]
pub struct RetryPolicy {
    /// Base delay used for the first backoff (attempt 1).
    pub base: Duration,
    /// Upper bound on a single backoff interval.
    pub max_delay: Duration,
    /// Maximum number of attempts before a delivery is dead-lettered.
    pub max_attempts: i32,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        RetryPolicy {
            base: Duration::from_secs(2),
            max_delay: Duration::from_secs(3600),
            max_attempts: 16,
        }
    }
}

impl RetryPolicy {
    /// Whether a delivery that has already made `attempt_count` attempts has
    /// any retries left.
    pub fn is_exhausted(&self, attempt_count: i32) -> bool {
        attempt_count >= self.max_attempts
    }

    /// Compute the (capped) ceiling delay for the given attempt number,
    /// before jitter is applied. `attempt` is 1-based.
    fn ceiling(&self, attempt: i32) -> Duration {
        // 2^(attempt-1) * base, saturating, then capped at max_delay.
        let exp = attempt.saturating_sub(1).clamp(0, 30) as u32;
        let factor = 1u64.checked_shl(exp).unwrap_or(u64::MAX);
        let millis = (self.base.as_millis() as u64).saturating_mul(factor);
        Duration::from_millis(millis).min(self.max_delay)
    }

    /// Backoff for the given (1-based) attempt with full jitter applied.
    pub fn backoff(&self, attempt: i32) -> Duration {
        let ceiling = self.ceiling(attempt);
        let ceil_ms = ceiling.as_millis() as u64;
        if ceil_ms == 0 {
            return Duration::ZERO;
        }
        let jittered = rand::thread_rng().gen_range(0..=ceil_ms);
        Duration::from_millis(jittered)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ceiling_grows_then_caps() {
        let p = RetryPolicy {
            base: Duration::from_secs(1),
            max_delay: Duration::from_secs(60),
            max_attempts: 100,
        };
        assert_eq!(p.ceiling(1), Duration::from_secs(1));
        assert_eq!(p.ceiling(2), Duration::from_secs(2));
        assert_eq!(p.ceiling(3), Duration::from_secs(4));
        // Eventually capped at max_delay.
        assert_eq!(p.ceiling(40), Duration::from_secs(60));
    }

    #[test]
    fn backoff_never_exceeds_ceiling() {
        let p = RetryPolicy::default();
        for attempt in 1..=20 {
            let b = p.backoff(attempt);
            assert!(b <= p.ceiling(attempt));
        }
    }

    #[test]
    fn exhaustion_respects_max_attempts() {
        let p = RetryPolicy {
            max_attempts: 3,
            ..RetryPolicy::default()
        };
        assert!(!p.is_exhausted(2));
        assert!(p.is_exhausted(3));
        assert!(p.is_exhausted(4));
    }
}
