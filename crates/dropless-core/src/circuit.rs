//! Per-endpoint circuit breaker.
//!
//! State is persisted on the `endpoints` row (`circuit_state`,
//! `circuit_open_until`, `consecutive_failures`); this module is the pure
//! decision logic the dispatcher applies. Keeping it side-effect-free makes it
//! unit-testable without a database.

use chrono::{DateTime, Duration, Utc};

use crate::model::Endpoint;

/// Circuit-breaker policy.
#[derive(Debug, Clone, Copy)]
pub struct CircuitPolicy {
    /// Consecutive failures that trip the breaker open.
    pub failure_threshold: i32,
    /// How long the breaker stays open before a half-open probe.
    pub open_duration: Duration,
}

impl Default for CircuitPolicy {
    fn default() -> Self {
        CircuitPolicy {
            failure_threshold: 5,
            open_duration: Duration::seconds(30),
        }
    }
}

/// Outcome of consulting the breaker before attempting a delivery.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Gate {
    /// Proceed with the delivery (circuit closed).
    Allow,
    /// Skip: the circuit is open — or a half-open probe is already in flight —
    /// until the given instant.
    Skip(DateTime<Utc>),
    /// The open window has elapsed: a single half-open probe may be attempted.
    /// The caller must atomically claim the probe (see
    /// [`crate::store::try_acquire_probe`]) before proceeding, so only one
    /// delivery probes a recovering endpoint.
    Probe,
}

/// New persisted circuit fields after recording an attempt outcome.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CircuitUpdate {
    /// `closed` | `open` | `half_open`.
    pub state: String,
    /// When an open circuit may probe again.
    pub open_until: Option<DateTime<Utc>>,
    /// Updated consecutive-failure counter.
    pub consecutive_failures: i32,
}

/// Decide whether to attempt delivery to `endpoint` at time `now`.
pub fn gate(endpoint: &Endpoint, now: DateTime<Utc>) -> Gate {
    match endpoint.circuit_state.as_str() {
        "open" => match endpoint.circuit_open_until {
            // Still within the open window → skip.
            Some(until) if now < until => Gate::Skip(until),
            // Window elapsed → a single half-open probe may be attempted.
            _ => Gate::Probe,
        },
        "half_open" => match endpoint.circuit_open_until {
            // A probe is in flight until its lease lapses; others skip.
            Some(until) if now < until => Gate::Skip(until),
            // The probe lease elapsed (the probing worker stalled or crashed) →
            // allow a fresh probe rather than wedging the endpoint half-open.
            _ => Gate::Probe,
        },
        // closed / anything unknown → allow.
        _ => Gate::Allow,
    }
}

/// Compute the new circuit fields after a successful attempt.
pub fn on_success() -> CircuitUpdate {
    CircuitUpdate {
        state: "closed".to_string(),
        open_until: None,
        consecutive_failures: 0,
    }
}

/// Compute the new circuit fields after a failed attempt.
pub fn on_failure(
    endpoint: &Endpoint,
    policy: &CircuitPolicy,
    now: DateTime<Utc>,
) -> CircuitUpdate {
    let failures = endpoint.consecutive_failures.saturating_add(1);
    if failures >= policy.failure_threshold {
        CircuitUpdate {
            state: "open".to_string(),
            open_until: Some(now + policy.open_duration),
            consecutive_failures: failures,
        }
    } else {
        CircuitUpdate {
            state: "closed".to_string(),
            open_until: None,
            consecutive_failures: failures,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    fn endpoint(state: &str, failures: i32, open_until: Option<DateTime<Utc>>) -> Endpoint {
        Endpoint {
            id: Uuid::now_v7(),
            tenant_id: "t1".into(),
            consumer_id: None,
            url: "http://localhost".into(),
            secret: "s".into(),
            disabled: false,
            circuit_state: state.into(),
            circuit_open_until: open_until,
            consecutive_failures: failures,
            event_types: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }

    #[test]
    fn closed_endpoint_is_allowed() {
        let e = endpoint("closed", 0, None);
        assert_eq!(gate(&e, Utc::now()), Gate::Allow);
    }

    #[test]
    fn open_endpoint_skips_until_window() {
        let now = Utc::now();
        let until = now + Duration::seconds(30);
        let e = endpoint("open", 5, Some(until));
        assert_eq!(gate(&e, now), Gate::Skip(until));
        // After the window, a single probe is allowed (not a blanket allow).
        assert_eq!(gate(&e, until), Gate::Probe);
    }

    #[test]
    fn half_open_skips_during_probe_then_reprobes() {
        let now = Utc::now();
        let until = now + Duration::seconds(30);
        let e = endpoint("half_open", 5, Some(until));
        // While the probe lease is live, other deliveries skip.
        assert_eq!(gate(&e, now), Gate::Skip(until));
        // If the probe stalled past its lease, a fresh probe is allowed.
        assert_eq!(gate(&e, until), Gate::Probe);
    }

    #[test]
    fn failures_trip_open_at_threshold() {
        let policy = CircuitPolicy::default();
        let e = endpoint("closed", policy.failure_threshold - 1, None);
        let upd = on_failure(&e, &policy, Utc::now());
        assert_eq!(upd.state, "open");
        assert!(upd.open_until.is_some());
    }

    #[test]
    fn success_resets() {
        let upd = on_success();
        assert_eq!(upd.state, "closed");
        assert_eq!(upd.consecutive_failures, 0);
    }
}
