//! Domain model: rows and enums shared across the engine.
//!
//! All structs derive [`sqlx::FromRow`] so they can be loaded with the
//! *runtime-checked* query API (`sqlx::query_as`) — no compile-time macros,
//! no `DATABASE_URL` required at build time.

use std::str::FromStr;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use uuid::Uuid;

use crate::error::CoreError;

/// Lifecycle state of a single delivery (one event → one endpoint).
///
/// Stored as `text` in Postgres and converted at the boundary so we never
/// depend on a compile-time-checked enum type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryStatus {
    /// Never attempted, or scheduled for a retry.
    Pending,
    /// Claimed by a worker and currently being attempted.
    InProgress,
    /// Delivered successfully (subscriber returned 2xx).
    Succeeded,
    /// Last attempt failed; will be retried after `next_attempt_at`.
    Failed,
    /// Exhausted retries; moved to the dead-letter table.
    Dead,
}

impl DeliveryStatus {
    /// The lowercase string stored in the database.
    pub fn as_str(&self) -> &'static str {
        match self {
            DeliveryStatus::Pending => "pending",
            DeliveryStatus::InProgress => "in_progress",
            DeliveryStatus::Succeeded => "succeeded",
            DeliveryStatus::Failed => "failed",
            DeliveryStatus::Dead => "dead",
        }
    }
}

impl FromStr for DeliveryStatus {
    type Err = CoreError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "pending" => Ok(DeliveryStatus::Pending),
            "in_progress" => Ok(DeliveryStatus::InProgress),
            "succeeded" => Ok(DeliveryStatus::Succeeded),
            "failed" => Ok(DeliveryStatus::Failed),
            "dead" => Ok(DeliveryStatus::Dead),
            other => Err(CoreError::Invalid(format!("delivery status {other:?}"))),
        }
    }
}

impl std::fmt::Display for DeliveryStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A tenant's *consumer* (a.k.a. application): one of the tenant's own
/// end-customers. The middle tier between a tenant and its endpoints — a
/// message published to a consumer fans out only to that consumer's endpoints.
#[derive(Debug, Clone, FromRow, Serialize)]
pub struct Consumer {
    /// UUIDv7 primary key.
    pub id: Uuid,
    /// Owning tenant.
    pub tenant_id: String,
    /// SaaS-assigned customer id, unique within the tenant (e.g. `merchant_42`).
    pub uid: String,
    /// Optional human-readable label.
    pub name: Option<String>,
    /// Creation timestamp.
    pub created_at: DateTime<Utc>,
    /// Last update timestamp.
    pub updated_at: DateTime<Utc>,
}

/// A message accepted from a producer — the source of truth.
#[derive(Debug, Clone, FromRow, Serialize)]
pub struct Event {
    /// UUIDv7 primary key.
    pub id: Uuid,
    /// Owning tenant.
    pub tenant_id: String,
    /// Owning consumer, or `None` for a tenant-level event.
    pub consumer_id: Option<Uuid>,
    /// Application-defined event type (e.g. `invoice.paid`).
    pub event_type: String,
    /// Raw JSON payload.
    pub payload: serde_json::Value,
    /// When the event was committed.
    pub created_at: DateTime<Utc>,
}

/// A subscriber endpoint that events fan out to.
#[derive(Debug, Clone, FromRow, Serialize)]
pub struct Endpoint {
    /// UUIDv7 primary key.
    pub id: Uuid,
    /// Owning tenant.
    pub tenant_id: String,
    /// Owning consumer, or `None` if it belongs to the tenant directly.
    pub consumer_id: Option<Uuid>,
    /// Destination URL.
    pub url: String,
    /// HMAC signing secret (Svix-compatible).
    #[serde(skip_serializing)]
    pub secret: String,
    /// Whether the endpoint is disabled (skipped by the dispatcher).
    pub disabled: bool,
    /// Circuit-breaker state: `closed` | `open` | `half_open`.
    pub circuit_state: String,
    /// When an open circuit may probe again.
    pub circuit_open_until: Option<DateTime<Utc>>,
    /// Consecutive failures (drives circuit opening).
    pub consecutive_failures: i32,
    /// Event-type subscriptions. `None` = receive ALL of the tenant's events;
    /// otherwise each entry is an exact type, a `prefix.*` wildcard, or `*`.
    pub event_types: Option<Vec<String>>,
    /// Creation timestamp.
    pub created_at: DateTime<Utc>,
    /// Last update timestamp.
    pub updated_at: DateTime<Utc>,
}

impl Endpoint {
    /// Whether this endpoint should receive an event of `event_type`.
    pub fn subscribes_to(&self, event_type: &str) -> bool {
        match &self.event_types {
            None => true,
            Some(list) if list.is_empty() => true,
            Some(list) => list.iter().any(|p| {
                if p == "*" {
                    true
                } else if let Some(prefix) = p.strip_suffix('*') {
                    event_type.starts_with(prefix)
                } else {
                    p == event_type
                }
            }),
        }
    }
}

/// One attempt to deliver one event to one endpoint — the unit of work and
/// the queue row. Status and schedule live alongside the work so claim and
/// state transitions happen in a single statement / transaction.
#[derive(Debug, Clone, FromRow, Serialize)]
pub struct Delivery {
    /// UUIDv7 primary key; also the immutable idempotency key.
    pub id: Uuid,
    /// The event being delivered.
    pub event_id: Uuid,
    /// The destination endpoint.
    pub endpoint_id: Uuid,
    /// Owning tenant (denormalized for fair dequeue).
    pub tenant_id: String,
    /// Status as stored (`text`); convert via [`Delivery::status`].
    pub status: String,
    /// Number of attempts made so far.
    pub attempt_count: i32,
    /// Earliest time the next attempt may run.
    pub next_attempt_at: DateTime<Utc>,
    /// Lock expiry while claimed by a worker.
    pub locked_until: Option<DateTime<Utc>>,
    /// Identifier of the worker holding the lock.
    pub locked_by: Option<String>,
    /// Value sent as the `Idempotency-Key` header (the delivery id).
    pub idempotency_key: String,
    /// Last error message, if any.
    pub last_error: Option<String>,
    /// Creation timestamp.
    pub created_at: DateTime<Utc>,
    /// Last update timestamp.
    pub updated_at: DateTime<Utc>,
}

impl Delivery {
    /// Parse the stored status string into a [`DeliveryStatus`].
    pub fn status(&self) -> Result<DeliveryStatus, CoreError> {
        self.status.parse()
    }
}

/// A permanent, append-only record of a single delivery attempt.
#[derive(Debug, Clone, FromRow, Serialize)]
pub struct DeliveryAttempt {
    /// UUIDv7 primary key.
    pub id: Uuid,
    /// The delivery this attempt belongs to.
    pub delivery_id: Uuid,
    /// 1-based attempt number.
    pub attempt_number: i32,
    /// HTTP status code returned, if the request completed.
    pub status_code: Option<i32>,
    /// Truncated response body for debugging.
    pub response_snippet: Option<String>,
    /// Transport/other error, if the request failed.
    pub error: Option<String>,
    /// When the attempt started.
    pub started_at: DateTime<Utc>,
    /// When the attempt finished.
    pub finished_at: DateTime<Utc>,
}

/// A configured inbound provider endpoint a tenant receives webhooks on.
#[derive(Debug, Clone, FromRow, Serialize)]
pub struct InboundSource {
    /// UUIDv7 primary key.
    pub id: Uuid,
    /// Owning tenant.
    pub tenant_id: String,
    /// URL path segment: `POST /ingest/{slug}` (globally unique).
    pub slug: String,
    /// `stripe` | `github` | `hmac`.
    pub provider: String,
    /// Verification secret (never serialized).
    #[serde(skip_serializing)]
    pub secret: String,
    /// Whether ingestion is paused.
    pub disabled: bool,
    /// Creation timestamp.
    pub created_at: DateTime<Utc>,
    /// Last update timestamp.
    pub updated_at: DateTime<Utc>,
}

/// A received inbound webhook — persisted raw before anything else.
#[derive(Debug, Clone, FromRow, Serialize)]
pub struct InboundEvent {
    /// UUIDv7 primary key.
    pub id: Uuid,
    /// The source it arrived on.
    pub source_id: Uuid,
    /// Owning tenant (denormalized).
    pub tenant_id: String,
    /// The provider's own event id (dedup key); may be absent.
    pub source_event_id: Option<String>,
    /// Resolved event type (provider-specific).
    pub event_type: String,
    /// The exact raw request body (byte-exact, `bytea`), for audit / replay.
    pub raw_body: Vec<u8>,
    /// The outbound event this was bridged into, if any.
    pub event_id: Option<Uuid>,
    /// When it was received.
    pub received_at: DateTime<Utc>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_roundtrips_through_string() {
        for s in [
            DeliveryStatus::Pending,
            DeliveryStatus::InProgress,
            DeliveryStatus::Succeeded,
            DeliveryStatus::Failed,
            DeliveryStatus::Dead,
        ] {
            assert_eq!(s.as_str().parse::<DeliveryStatus>().unwrap(), s);
        }
    }

    #[test]
    fn unknown_status_is_invalid() {
        assert!("nope".parse::<DeliveryStatus>().is_err());
    }

    fn ep(event_types: Option<Vec<String>>) -> Endpoint {
        Endpoint {
            id: Uuid::now_v7(),
            tenant_id: "t".into(),
            consumer_id: None,
            url: "http://x".into(),
            secret: "s".into(),
            disabled: false,
            circuit_state: "closed".into(),
            circuit_open_until: None,
            consecutive_failures: 0,
            event_types,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }

    #[test]
    fn endpoint_subscription_matching() {
        // None / empty = all events.
        assert!(ep(None).subscribes_to("anything"));
        assert!(ep(Some(vec![])).subscribes_to("anything"));
        // Exact match.
        let e = ep(Some(vec!["payment.succeeded".into()]));
        assert!(e.subscribes_to("payment.succeeded"));
        assert!(!e.subscribes_to("payment.failed"));
        // Prefix wildcard.
        let e = ep(Some(vec!["payment.*".into()]));
        assert!(e.subscribes_to("payment.failed"));
        assert!(!e.subscribes_to("order.created"));
        // Explicit catch-all and multi-pattern.
        assert!(ep(Some(vec!["*".into()])).subscribes_to("whatever"));
        let e = ep(Some(vec!["order.created".into(), "payment.*".into()]));
        assert!(e.subscribes_to("order.created"));
        assert!(e.subscribes_to("payment.refunded"));
        assert!(!e.subscribes_to("user.created"));
    }
}
