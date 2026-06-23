//! The inbox: accept a verified inbound webhook and bridge it into the outbound
//! pipeline — raw-first, deduped, and atomic.
//!
//! The whole thing is one transaction, so the producer (the upstream provider)
//! is only acknowledged with `2xx` after the raw body is durably stored *and*
//! fanned out — the same zero-loss invariant as the outbound [`crate::outbox`].

use serde_json::Value;
use uuid::Uuid;

use crate::error::CoreResult;
use crate::model::InboundSource;
use crate::store;

/// Outcome of accepting an inbound webhook.
#[derive(Debug)]
pub enum Accepted {
    /// Already received (same source + provider event id) — a no-op.
    Duplicate,
    /// Newly accepted, persisted raw, and bridged to an outbound event.
    Bridged {
        /// The stored inbound event id.
        inbound_id: Uuid,
        /// The outbound event it was bridged into.
        event_id: Uuid,
        /// One delivery per active endpoint at bridge time.
        delivery_ids: Vec<Uuid>,
    },
}

/// Accept a **verified** inbound webhook atomically:
/// 1. persist the raw body (deduped on the provider's event id),
/// 2. bridge it into an outbound event + a delivery per active endpoint,
/// 3. link the inbound row to that event.
///
/// Returns once everything is committed; the caller may then return `2xx`.
pub async fn accept(
    pool: &sqlx::PgPool,
    source: &InboundSource,
    source_event_id: Option<&str>,
    event_type: &str,
    raw_body: &[u8],
    payload: &Value,
) -> CoreResult<Accepted> {
    let mut tx = pool.begin().await?;

    // 1) Persist raw, deduped. A duplicate provider event id is a no-op.
    let inbound_id = Uuid::now_v7();
    let inserted = store::insert_inbound_event(
        &mut *tx,
        inbound_id,
        source.id,
        &source.tenant_id,
        source_event_id,
        event_type,
        raw_body,
    )
    .await?;
    let Some(inbound_id) = inserted else {
        tx.rollback().await?;
        return Ok(Accepted::Duplicate);
    };

    // 2) Bridge into the outbound pipeline (new event + fan-out). Inbound
    //    sources are tenant-level, so the bridged event fans out to the tenant's
    //    own (consumer-less) endpoints.
    let event_id = Uuid::now_v7();
    store::insert_event(
        &mut *tx,
        event_id,
        &source.tenant_id,
        None,
        event_type,
        payload,
    )
    .await?;
    let endpoints = store::active_endpoints_for_scope(&mut *tx, &source.tenant_id, None).await?;
    let mut delivery_ids = Vec::with_capacity(endpoints.len());
    for endpoint in endpoints.iter().filter(|e| e.subscribes_to(event_type)) {
        let delivery_id = Uuid::now_v7();
        store::insert_delivery(
            &mut *tx,
            delivery_id,
            event_id,
            endpoint.id,
            &source.tenant_id,
        )
        .await?;
        delivery_ids.push(delivery_id);
    }

    // 3) Link, then commit.
    store::link_inbound_event(&mut *tx, inbound_id, event_id).await?;
    tx.commit().await?;

    Ok(Accepted::Bridged {
        inbound_id,
        event_id,
        delivery_ids,
    })
}
