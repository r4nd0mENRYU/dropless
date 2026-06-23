//! The outbox: accept a message and fan it out to all subscribers in **one
//! transaction**.
//!
//! This is the code location of the core invariant. The API layer must only
//! return `2xx` *after* [`enqueue_message`] returns `Ok` — i.e. after the
//! event and all its deliveries are committed to Postgres. Because the event
//! row, the queue rows (deliveries), and their initial state are written in a
//! single transaction, a crash either commits all of it or none of it: there
//! is no partial fan-out and nothing is acknowledged that wasn't persisted.

use uuid::Uuid;

use crate::error::CoreResult;
use crate::store;

/// Result of enqueuing a message.
#[derive(Debug, Clone)]
pub struct Enqueued {
    /// The newly created event id.
    pub event_id: Uuid,
    /// The delivery ids created (one per active endpoint).
    pub delivery_ids: Vec<Uuid>,
}

/// Persist an event and fan out one delivery per active endpoint, atomically.
///
/// `consumer_id` selects the publishing scope: `Some` fans out only to that
/// consumer's endpoints (one of the tenant's end-customers); `None` publishes at
/// the tenant level (the tenant's own endpoints). The two scopes never cross.
///
/// Returns once everything is committed. The caller may then — and only then —
/// acknowledge the producer with `2xx`.
pub async fn enqueue_message(
    pool: &sqlx::PgPool,
    tenant_id: &str,
    consumer_id: Option<Uuid>,
    event_type: &str,
    payload: &serde_json::Value,
) -> CoreResult<Enqueued> {
    let event_id = Uuid::now_v7();
    let mut tx = pool.begin().await?;

    // 1) Persist the raw event (source of truth).
    store::insert_event(
        &mut *tx,
        event_id,
        tenant_id,
        consumer_id,
        event_type,
        payload,
    )
    .await?;

    // 2) Snapshot the publishing scope's active endpoints.
    let endpoints = store::active_endpoints_for_scope(&mut *tx, tenant_id, consumer_id).await?;

    // 3) Create a delivery (queue row) per endpoint that SUBSCRIBES to this
    //    event type; idempotency key == delivery id.
    let mut delivery_ids = Vec::with_capacity(endpoints.len());
    for endpoint in endpoints.iter().filter(|e| e.subscribes_to(event_type)) {
        let delivery_id = Uuid::now_v7();
        store::insert_delivery(&mut *tx, delivery_id, event_id, endpoint.id, tenant_id).await?;
        delivery_ids.push(delivery_id);
    }

    // 4) Commit. Only after this returns Ok may the caller return 2xx.
    //    (Waking dispatch workers is done by a coalesced NOTIFY *outside* this
    //    transaction — see `notify::Notifier` — because a NOTIFY per commit
    //    serializes commits under load and throttles ingest.)
    tx.commit().await?;

    Ok(Enqueued {
        event_id,
        delivery_ids,
    })
}
