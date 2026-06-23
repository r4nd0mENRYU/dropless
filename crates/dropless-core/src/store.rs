//! Postgres data access — the source of truth **and** the queue.
//!
//! Every query here uses the **runtime-checked** sqlx API
//! (`sqlx::query`, `sqlx::query_as::<_, T>`, `sqlx::query_scalar`) so that
//! `cargo build` / `cargo check` succeed without a live database and without
//! `DATABASE_URL`. No `query!` / `query_as!` compile-time macros are used.

use chrono::{DateTime, Utc};
use sqlx::{Executor, PgPool, Postgres};
use uuid::Uuid;

use crate::error::{CoreError, CoreResult};
use crate::model::{Consumer, Delivery, DeliveryAttempt, DeliveryStatus, Endpoint, Event};

/// Insert an endpoint. `consumer_id` scopes it to one of the tenant's consumers
/// (`None` = the endpoint belongs to the tenant directly).
pub async fn create_endpoint<'e, E>(
    executor: E,
    tenant_id: &str,
    consumer_id: Option<Uuid>,
    url: &str,
    secret: &str,
    event_types: Option<&[String]>,
) -> CoreResult<Endpoint>
where
    E: Executor<'e, Database = Postgres>,
{
    let id = Uuid::now_v7();
    let row = sqlx::query_as::<_, Endpoint>(
        r#"
        INSERT INTO endpoints (id, tenant_id, consumer_id, url, secret, event_types)
        VALUES ($1, $2, $3, $4, $5, $6)
        RETURNING *
        "#,
    )
    .bind(id)
    .bind(tenant_id)
    .bind(consumer_id)
    .bind(url)
    .bind(secret)
    .bind(event_types)
    .fetch_one(executor)
    .await?;
    Ok(row)
}

/// Enabled fan-out targets for one publishing scope: a tenant's consumer
/// (`Some`) or the tenant level (`None`). `IS NOT DISTINCT FROM` matches NULL to
/// NULL, so a tenant-level publish reaches only tenant-level endpoints and a
/// consumer publish reaches only that consumer's endpoints — never each other.
pub async fn active_endpoints_for_scope<'e, E>(
    executor: E,
    tenant_id: &str,
    consumer_id: Option<Uuid>,
) -> CoreResult<Vec<Endpoint>>
where
    E: Executor<'e, Database = Postgres>,
{
    let rows = sqlx::query_as::<_, Endpoint>(
        r#"
        SELECT * FROM endpoints
        WHERE tenant_id = $1 AND disabled = false
          AND consumer_id IS NOT DISTINCT FROM $2
        ORDER BY created_at
        "#,
    )
    .bind(tenant_id)
    .bind(consumer_id)
    .fetch_all(executor)
    .await?;
    Ok(rows)
}

/// All endpoints for a tenant, including disabled ones (for management APIs).
pub async fn list_endpoints(pool: &PgPool, tenant_id: &str) -> CoreResult<Vec<Endpoint>> {
    let rows = sqlx::query_as::<_, Endpoint>(
        "SELECT * FROM endpoints WHERE tenant_id = $1 ORDER BY created_at",
    )
    .bind(tenant_id)
    .fetch_all(pool)
    .await?;
    Ok(rows)
}

/// All endpoints belonging to a specific consumer (for `GET /v1/app/{uid}/endpoints`).
pub async fn list_endpoints_for_consumer(
    pool: &PgPool,
    consumer_id: Uuid,
) -> CoreResult<Vec<Endpoint>> {
    let rows = sqlx::query_as::<_, Endpoint>(
        "SELECT * FROM endpoints WHERE consumer_id = $1 ORDER BY created_at",
    )
    .bind(consumer_id)
    .fetch_all(pool)
    .await?;
    Ok(rows)
}

// ---- consumers (the application tier) ---------------------------------------

/// A consumer with its endpoint / event counts (for listing).
#[derive(Debug, Clone, serde::Serialize, sqlx::FromRow)]
pub struct ConsumerSummary {
    /// Consumer id.
    pub id: Uuid,
    /// Owning tenant.
    pub tenant_id: String,
    /// SaaS-assigned customer id.
    pub uid: String,
    /// Optional label.
    pub name: Option<String>,
    /// Registered endpoints.
    pub endpoints: i64,
    /// Events published to this consumer.
    pub events: i64,
    /// Creation timestamp.
    pub created_at: DateTime<Utc>,
}

/// Get-or-create a consumer by `(tenant, uid)`. Idempotent: re-creating an
/// existing uid returns the existing row (updating the label if a new one is
/// given), so a SaaS can provision a consumer on every signup without checking.
pub async fn create_consumer(
    pool: &PgPool,
    tenant_id: &str,
    uid: &str,
    name: Option<&str>,
) -> CoreResult<Consumer> {
    let row = sqlx::query_as::<_, Consumer>(
        r#"
        INSERT INTO consumers (id, tenant_id, uid, name)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (tenant_id, uid)
        DO UPDATE SET name = COALESCE($4, consumers.name), updated_at = now()
        RETURNING *
        "#,
    )
    .bind(Uuid::now_v7())
    .bind(tenant_id)
    .bind(uid)
    .bind(name)
    .fetch_one(pool)
    .await?;
    Ok(row)
}

/// Resolve a consumer by `(tenant, uid)`.
pub async fn get_consumer(
    pool: &PgPool,
    tenant_id: &str,
    uid: &str,
) -> CoreResult<Option<Consumer>> {
    let row =
        sqlx::query_as::<_, Consumer>("SELECT * FROM consumers WHERE tenant_id = $1 AND uid = $2")
            .bind(tenant_id)
            .bind(uid)
            .fetch_optional(pool)
            .await?;
    Ok(row)
}

/// List a tenant's consumers with endpoint / event counts, newest first.
pub async fn list_consumers(pool: &PgPool, tenant_id: &str) -> CoreResult<Vec<ConsumerSummary>> {
    let rows = sqlx::query_as::<_, ConsumerSummary>(
        r#"
        SELECT c.id, c.tenant_id, c.uid, c.name, c.created_at,
          (SELECT count(*)::bigint FROM endpoints e WHERE e.consumer_id = c.id) AS endpoints,
          (SELECT count(*)::bigint FROM events ev   WHERE ev.consumer_id = c.id) AS events
        FROM consumers c
        WHERE c.tenant_id = $1
        ORDER BY c.created_at DESC
        "#,
    )
    .bind(tenant_id)
    .fetch_all(pool)
    .await?;
    Ok(rows)
}

/// Partially update an endpoint (URL and/or disabled flag), scoped to its
/// tenant. `None` fields are left unchanged. Returns the updated row, or `None`
/// if no endpoint with that id belongs to the tenant.
pub async fn update_endpoint(
    pool: &PgPool,
    id: Uuid,
    tenant_id: &str,
    url: Option<&str>,
    disabled: Option<bool>,
    event_types: Option<&[String]>,
) -> CoreResult<Option<Endpoint>> {
    let row = sqlx::query_as::<_, Endpoint>(
        r#"
        UPDATE endpoints
        SET url = COALESCE($3, url),
            disabled = COALESCE($4, disabled),
            event_types = COALESCE($5, event_types),
            updated_at = now()
        WHERE id = $1 AND tenant_id = $2
        RETURNING *
        "#,
    )
    .bind(id)
    .bind(tenant_id)
    .bind(url)
    .bind(disabled)
    .bind(event_types)
    .fetch_optional(pool)
    .await?;
    Ok(row)
}

// ---- inbound gateway (v0.2) -------------------------------------------------

/// Register an inbound source. `slug` is globally unique (`POST /ingest/{slug}`).
pub async fn create_inbound_source(
    pool: &PgPool,
    tenant_id: &str,
    slug: &str,
    provider: &str,
    secret: &str,
) -> CoreResult<crate::model::InboundSource> {
    let row = sqlx::query_as::<_, crate::model::InboundSource>(
        r#"
        INSERT INTO inbound_sources (id, tenant_id, slug, provider, secret)
        VALUES ($1, $2, $3, $4, $5)
        RETURNING *
        "#,
    )
    .bind(Uuid::now_v7())
    .bind(tenant_id)
    .bind(slug)
    .bind(provider)
    .bind(secret)
    .fetch_one(pool)
    .await?;
    Ok(row)
}

/// Resolve an inbound source by its slug (for `POST /ingest/{slug}`).
pub async fn get_inbound_source_by_slug(
    pool: &PgPool,
    slug: &str,
) -> CoreResult<Option<crate::model::InboundSource>> {
    let row = sqlx::query_as::<_, crate::model::InboundSource>(
        "SELECT * FROM inbound_sources WHERE slug = $1",
    )
    .bind(slug)
    .fetch_optional(pool)
    .await?;
    Ok(row)
}

/// List a tenant's inbound sources.
pub async fn list_inbound_sources(
    pool: &PgPool,
    tenant_id: &str,
) -> CoreResult<Vec<crate::model::InboundSource>> {
    let rows = sqlx::query_as::<_, crate::model::InboundSource>(
        "SELECT * FROM inbound_sources WHERE tenant_id = $1 ORDER BY created_at",
    )
    .bind(tenant_id)
    .fetch_all(pool)
    .await?;
    Ok(rows)
}

/// Insert a received inbound event, deduping on `(source_id, source_event_id)`.
/// Returns the new id, or `None` if it was a duplicate (already accepted).
#[allow(clippy::too_many_arguments)]
pub async fn insert_inbound_event<'e, E>(
    executor: E,
    id: Uuid,
    source_id: Uuid,
    tenant_id: &str,
    source_event_id: Option<&str>,
    event_type: &str,
    raw_body: &[u8],
) -> CoreResult<Option<Uuid>>
where
    E: Executor<'e, Database = Postgres>,
{
    let row = sqlx::query_scalar::<_, Uuid>(
        r#"
        INSERT INTO inbound_events (id, source_id, tenant_id, source_event_id, event_type, raw_body)
        VALUES ($1, $2, $3, $4, $5, $6)
        ON CONFLICT (source_id, source_event_id) WHERE source_event_id IS NOT NULL DO NOTHING
        RETURNING id
        "#,
    )
    .bind(id)
    .bind(source_id)
    .bind(tenant_id)
    .bind(source_event_id)
    .bind(event_type)
    .bind(raw_body)
    .fetch_optional(executor)
    .await?;
    Ok(row)
}

/// Link an accepted inbound event to the outbound event it bridged into.
pub async fn link_inbound_event<'e, E>(
    executor: E,
    inbound_id: Uuid,
    event_id: Uuid,
) -> CoreResult<()>
where
    E: Executor<'e, Database = Postgres>,
{
    sqlx::query("UPDATE inbound_events SET event_id = $2 WHERE id = $1")
        .bind(inbound_id)
        .bind(event_id)
        .execute(executor)
        .await?;
    Ok(())
}

/// Insert an event row. `consumer_id` scopes it to a consumer (`None` = a
/// tenant-level event).
pub async fn insert_event<'e, E>(
    executor: E,
    id: Uuid,
    tenant_id: &str,
    consumer_id: Option<Uuid>,
    event_type: &str,
    payload: &serde_json::Value,
) -> CoreResult<Event>
where
    E: Executor<'e, Database = Postgres>,
{
    let row = sqlx::query_as::<_, Event>(
        r#"
        INSERT INTO events (id, tenant_id, consumer_id, event_type, payload)
        VALUES ($1, $2, $3, $4, $5)
        RETURNING *
        "#,
    )
    .bind(id)
    .bind(tenant_id)
    .bind(consumer_id)
    .bind(event_type)
    .bind(payload)
    .fetch_one(executor)
    .await?;
    Ok(row)
}

/// Insert a delivery row. The idempotency key is the delivery id itself, so it
/// is immutable and travels with the message on every (re)attempt.
pub async fn insert_delivery<'e, E>(
    executor: E,
    id: Uuid,
    event_id: Uuid,
    endpoint_id: Uuid,
    tenant_id: &str,
) -> CoreResult<()>
where
    E: Executor<'e, Database = Postgres>,
{
    sqlx::query(
        r#"
        INSERT INTO deliveries (id, event_id, endpoint_id, tenant_id, idempotency_key)
        VALUES ($1, $2, $3, $4, $5)
        "#,
    )
    .bind(id)
    .bind(event_id)
    .bind(endpoint_id)
    .bind(tenant_id)
    .bind(id.to_string())
    .execute(executor)
    .await?;
    Ok(())
}

/// Recent events for a tenant, newest first (for the dashboard message list).
pub async fn list_events(
    pool: &PgPool,
    tenant_id: &str,
    limit: i64,
    offset: i64,
) -> CoreResult<Vec<Event>> {
    let rows = sqlx::query_as::<_, Event>(
        "SELECT * FROM events WHERE tenant_id = $1 ORDER BY created_at DESC LIMIT $2 OFFSET $3",
    )
    .bind(tenant_id)
    .bind(limit)
    .bind(offset)
    .fetch_all(pool)
    .await?;
    Ok(rows)
}

/// Recent events for a single consumer, newest first.
pub async fn list_events_for_consumer(
    pool: &PgPool,
    consumer_id: Uuid,
    limit: i64,
    offset: i64,
) -> CoreResult<Vec<Event>> {
    let rows = sqlx::query_as::<_, Event>(
        "SELECT * FROM events WHERE consumer_id = $1 ORDER BY created_at DESC LIMIT $2 OFFSET $3",
    )
    .bind(consumer_id)
    .bind(limit)
    .bind(offset)
    .fetch_all(pool)
    .await?;
    Ok(rows)
}

/// Fetch a single event by id.
pub async fn get_event(pool: &PgPool, id: Uuid) -> CoreResult<Option<Event>> {
    let row = sqlx::query_as::<_, Event>("SELECT * FROM events WHERE id = $1")
        .bind(id)
        .fetch_optional(pool)
        .await?;
    Ok(row)
}

/// Fetch a single endpoint by id.
pub async fn get_endpoint<'e, E>(executor: E, id: Uuid) -> CoreResult<Option<Endpoint>>
where
    E: Executor<'e, Database = Postgres>,
{
    let row = sqlx::query_as::<_, Endpoint>("SELECT * FROM endpoints WHERE id = $1")
        .bind(id)
        .fetch_optional(executor)
        .await?;
    Ok(row)
}

/// Fetch a single delivery by id.
pub async fn get_delivery(pool: &PgPool, id: Uuid) -> CoreResult<Option<Delivery>> {
    let row = sqlx::query_as::<_, Delivery>("SELECT * FROM deliveries WHERE id = $1")
        .bind(id)
        .fetch_optional(pool)
        .await?;
    Ok(row)
}

/// All deliveries for an event (for `GET /v1/messages/:id`).
pub async fn deliveries_for_event(pool: &PgPool, event_id: Uuid) -> CoreResult<Vec<Delivery>> {
    let rows = sqlx::query_as::<_, Delivery>(
        "SELECT * FROM deliveries WHERE event_id = $1 ORDER BY created_at",
    )
    .bind(event_id)
    .fetch_all(pool)
    .await?;
    Ok(rows)
}

/// Attempt history for a delivery (for the dashboard timeline).
pub async fn attempts_for_delivery(
    pool: &PgPool,
    delivery_id: Uuid,
) -> CoreResult<Vec<DeliveryAttempt>> {
    let rows = sqlx::query_as::<_, DeliveryAttempt>(
        "SELECT * FROM delivery_attempts WHERE delivery_id = $1 ORDER BY attempt_number",
    )
    .bind(delivery_id)
    .fetch_all(pool)
    .await?;
    Ok(rows)
}

/// Atomically claim up to `batch` due deliveries for `worker_id` using
/// `FOR UPDATE SKIP LOCKED`. Claimed rows are flipped to `in_progress` and
/// locked for `lock_secs`, so no two workers ever own the same row.
///
/// The predicate also reclaims rows stuck in `in_progress` whose lock has
/// expired — this is the crash-recovery path: a worker that dies (e.g.
/// `kill -9`) mid-attempt leaves its row locked, and once the lease lapses any
/// worker picks it back up. That is why nothing is ever dropped.
pub async fn claim_due_deliveries(
    pool: &PgPool,
    worker_id: &str,
    batch: i64,
    lock_secs: i32,
) -> CoreResult<Vec<Delivery>> {
    let rows = sqlx::query_as::<_, Delivery>(
        r#"
        UPDATE deliveries AS d
        SET status = 'in_progress',
            locked_by = $1,
            locked_until = now() + ($2::int * interval '1 second'),
            updated_at = now()
        WHERE d.id IN (
            SELECT id FROM deliveries
            WHERE (status IN ('pending', 'failed') AND next_attempt_at <= now())
               OR (status = 'in_progress' AND locked_until < now())
            ORDER BY next_attempt_at
            FOR UPDATE SKIP LOCKED
            LIMIT $3
        )
        RETURNING d.*
        "#,
    )
    .bind(worker_id)
    .bind(lock_secs)
    .bind(batch)
    .fetch_all(pool)
    .await?;
    Ok(rows)
}

/// Append a permanent attempt record.
#[allow(clippy::too_many_arguments)]
pub async fn record_attempt<'e, E>(
    executor: E,
    delivery_id: Uuid,
    attempt_number: i32,
    status_code: Option<i32>,
    response_snippet: Option<&str>,
    error: Option<&str>,
    started_at: DateTime<Utc>,
    finished_at: DateTime<Utc>,
) -> CoreResult<()>
where
    E: Executor<'e, Database = Postgres>,
{
    sqlx::query(
        r#"
        INSERT INTO delivery_attempts
            (id, delivery_id, attempt_number, status_code, response_snippet, error, started_at, finished_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        "#,
    )
    .bind(Uuid::now_v7())
    .bind(delivery_id)
    .bind(attempt_number)
    .bind(status_code)
    .bind(response_snippet)
    .bind(error)
    .bind(started_at)
    .bind(finished_at)
    .execute(executor)
    .await?;
    Ok(())
}

/// Mark a delivery as succeeded and release its lock.
///
/// Guarded by lease ownership: the row is updated only while `worker_id` still
/// holds an unexpired lock. Returns `true` if the write was applied, `false` if
/// the lease had already been lost (the row was re-claimed by another worker
/// after the lease lapsed) — in which case the caller must abandon the result
/// rather than clobber the new owner's state.
pub async fn mark_succeeded<'e, E>(
    executor: E,
    id: Uuid,
    worker_id: &str,
    attempt_count: i32,
) -> CoreResult<bool>
where
    E: Executor<'e, Database = Postgres>,
{
    let result = sqlx::query(
        r#"
        UPDATE deliveries
        SET status = 'succeeded',
            attempt_count = $2,
            locked_by = NULL,
            locked_until = NULL,
            last_error = NULL,
            updated_at = now()
        WHERE id = $1 AND locked_by = $3 AND locked_until > now()
        "#,
    )
    .bind(id)
    .bind(attempt_count)
    .bind(worker_id)
    .execute(executor)
    .await?;
    Ok(result.rows_affected() == 1)
}

/// Mark a delivery as failed, schedule its next attempt, and release the lock.
///
/// Lease-guarded like [`mark_succeeded`]: returns `false` (a no-op) if the
/// lease was already lost to another worker.
pub async fn mark_failed<'e, E>(
    executor: E,
    id: Uuid,
    worker_id: &str,
    attempt_count: i32,
    next_attempt_at: DateTime<Utc>,
    error: &str,
) -> CoreResult<bool>
where
    E: Executor<'e, Database = Postgres>,
{
    let result = sqlx::query(
        r#"
        UPDATE deliveries
        SET status = 'failed',
            attempt_count = $2,
            next_attempt_at = $3,
            last_error = $4,
            locked_by = NULL,
            locked_until = NULL,
            updated_at = now()
        WHERE id = $1 AND locked_by = $5 AND locked_until > now()
        "#,
    )
    .bind(id)
    .bind(attempt_count)
    .bind(next_attempt_at)
    .bind(error)
    .bind(worker_id)
    .execute(executor)
    .await?;
    Ok(result.rows_affected() == 1)
}

/// Push a delivery's next attempt into the future without counting it as a
/// failure (used when the circuit is open). Releases the lock.
///
/// Lease-guarded like [`mark_succeeded`]: returns `false` (a no-op) if the
/// lease was already lost to another worker.
pub async fn defer_delivery<'e, E>(
    executor: E,
    id: Uuid,
    worker_id: &str,
    next_attempt_at: DateTime<Utc>,
) -> CoreResult<bool>
where
    E: Executor<'e, Database = Postgres>,
{
    let result = sqlx::query(
        r#"
        UPDATE deliveries
        SET status = 'failed',
            next_attempt_at = $2,
            locked_by = NULL,
            locked_until = NULL,
            updated_at = now()
        WHERE id = $1 AND locked_by = $3 AND locked_until > now()
        "#,
    )
    .bind(id)
    .bind(next_attempt_at)
    .bind(worker_id)
    .execute(executor)
    .await?;
    Ok(result.rows_affected() == 1)
}

/// Dead-letter a delivery **within the caller's transaction**: flip it to
/// `dead` and record a dead-letter row. Does not begin or commit — the caller
/// owns the transaction (so this can be coalesced with `record_attempt` into a
/// single fsync).
///
/// Lease-guarded like [`mark_succeeded`]: if the lease was already lost to
/// another worker the transition is skipped and **no** dead-letter row is
/// written (returns `false`); the caller should roll back, so a zombie worker
/// cannot dead-letter a delivery that a fresh owner is still working.
pub async fn mark_dead_on(
    conn: &mut sqlx::PgConnection,
    delivery: &Delivery,
    worker_id: &str,
    reason: &str,
) -> CoreResult<bool> {
    let result = sqlx::query(
        r#"
        UPDATE deliveries
        SET status = 'dead',
            last_error = $2,
            locked_by = NULL,
            locked_until = NULL,
            updated_at = now()
        WHERE id = $1 AND locked_by = $3 AND locked_until > now()
        "#,
    )
    .bind(delivery.id)
    .bind(reason)
    .bind(worker_id)
    .execute(&mut *conn)
    .await?;

    if result.rows_affected() == 0 {
        return Ok(false);
    }

    sqlx::query(
        r#"
        INSERT INTO dead_letters (id, delivery_id, event_id, endpoint_id, reason)
        VALUES ($1, $2, $3, $4, $5)
        "#,
    )
    .bind(Uuid::now_v7())
    .bind(delivery.id)
    .bind(delivery.event_id)
    .bind(delivery.endpoint_id)
    .bind(reason)
    .execute(&mut *conn)
    .await?;

    Ok(true)
}

/// Dead-letter a delivery in its own transaction — for paths not already inside
/// one (e.g. a vanished endpoint/event). See [`mark_dead_on`].
pub async fn mark_dead(
    pool: &PgPool,
    delivery: &Delivery,
    worker_id: &str,
    reason: &str,
) -> CoreResult<bool> {
    let mut tx = pool.begin().await?;
    let applied = mark_dead_on(&mut tx, delivery, worker_id, reason).await?;
    if applied {
        tx.commit().await?;
    } else {
        tx.rollback().await?;
    }
    Ok(applied)
}

/// Update the persisted circuit-breaker fields for an endpoint.
pub async fn update_circuit<'e, E>(
    executor: E,
    endpoint_id: Uuid,
    state: &str,
    open_until: Option<DateTime<Utc>>,
    consecutive_failures: i32,
) -> CoreResult<()>
where
    E: Executor<'e, Database = Postgres>,
{
    sqlx::query(
        r#"
        UPDATE endpoints
        SET circuit_state = $2,
            circuit_open_until = $3,
            consecutive_failures = $4,
            updated_at = now()
        WHERE id = $1
        "#,
    )
    .bind(endpoint_id)
    .bind(state)
    .bind(open_until)
    .bind(consecutive_failures)
    .execute(executor)
    .await?;
    Ok(())
}

/// Atomically claim the single half-open probe slot for an endpoint whose open
/// window has elapsed. Flips `open` / stalled-`half_open` → `half_open` and
/// stamps a fresh probe lease, returning `true` only for the one caller that
/// wins the transition. Concurrent callers see the row already `half_open` with
/// a live lease and get `false`, so a recovering endpoint receives a single
/// probe instead of the whole backlog at once.
pub async fn try_acquire_probe(
    pool: &PgPool,
    endpoint_id: Uuid,
    lease_secs: i32,
) -> CoreResult<bool> {
    let result = sqlx::query(
        r#"
        UPDATE endpoints
        SET circuit_state = 'half_open',
            circuit_open_until = now() + ($2::int * interval '1 second'),
            updated_at = now()
        WHERE id = $1
          AND circuit_state IN ('open', 'half_open')
          AND (circuit_open_until IS NULL OR circuit_open_until <= now())
        "#,
    )
    .bind(endpoint_id)
    .bind(lease_secs)
    .execute(pool)
    .await?;
    Ok(result.rows_affected() == 1)
}

/// Reset a delivery so it is re-queued immediately (manual replay). Returns
/// `false` if the delivery does not exist.
pub async fn reset_for_replay(pool: &PgPool, delivery_id: Uuid) -> CoreResult<bool> {
    let result = sqlx::query(
        r#"
        UPDATE deliveries
        SET status = 'pending',
            next_attempt_at = now(),
            locked_by = NULL,
            locked_until = NULL,
            updated_at = now()
        WHERE id = $1
        "#,
    )
    .bind(delivery_id)
    .execute(pool)
    .await?;
    Ok(result.rows_affected() > 0)
}

/// Count of deliveries grouped by status (for `/metrics`).
pub async fn status_counts(pool: &PgPool) -> CoreResult<Vec<(String, i64)>> {
    let rows = sqlx::query_as::<_, (String, i64)>(
        "SELECT status, count(*)::bigint FROM deliveries GROUP BY status",
    )
    .fetch_all(pool)
    .await?;
    Ok(rows)
}

/// Total events ever accepted (ingest volume, for `/metrics`).
pub async fn total_events(pool: &PgPool) -> CoreResult<i64> {
    let n = sqlx::query_scalar::<_, i64>("SELECT count(*)::bigint FROM events")
        .fetch_one(pool)
        .await?;
    Ok(n)
}

// ---- operator admin (cross-tenant) ------------------------------------------

/// Per-tenant rollup for the operator admin dashboard.
#[derive(Debug, Clone, serde::Serialize, sqlx::FromRow)]
pub struct TenantStat {
    /// Tenant id.
    pub tenant_id: String,
    /// Registered endpoints.
    pub endpoints: i64,
    /// Events accepted.
    pub events: i64,
    /// Deliveries that succeeded.
    pub succeeded: i64,
    /// Deliveries currently failed (retrying).
    pub failed: i64,
    /// Deliveries dead-lettered.
    pub dead: i64,
}

/// All tenants (from `api_keys`) with their delivery rollups, busiest first.
pub async fn admin_tenant_stats(pool: &PgPool) -> CoreResult<Vec<TenantStat>> {
    let rows = sqlx::query_as::<_, TenantStat>(
        r#"
        SELECT t.tenant_id AS tenant_id,
          (SELECT count(*)::bigint FROM endpoints e  WHERE e.tenant_id  = t.tenant_id) AS endpoints,
          (SELECT count(*)::bigint FROM events    ev WHERE ev.tenant_id = t.tenant_id) AS events,
          (SELECT count(*)::bigint FROM deliveries d WHERE d.tenant_id = t.tenant_id AND d.status='succeeded') AS succeeded,
          (SELECT count(*)::bigint FROM deliveries d WHERE d.tenant_id = t.tenant_id AND d.status='failed')    AS failed,
          (SELECT count(*)::bigint FROM deliveries d WHERE d.tenant_id = t.tenant_id AND d.status='dead')      AS dead
        FROM (SELECT DISTINCT tenant_id FROM api_keys) t
        ORDER BY events DESC, tenant_id
        "#,
    )
    .fetch_all(pool)
    .await?;
    Ok(rows)
}

/// A recently-broken delivery across all tenants (operator triage feed).
#[derive(Debug, Clone, serde::Serialize, sqlx::FromRow)]
pub struct RecentFailure {
    /// Owning tenant.
    pub tenant_id: String,
    /// The event type.
    pub event_type: String,
    /// The destination URL.
    pub endpoint_url: String,
    /// `failed` or `dead`.
    pub status: String,
    /// Attempts so far.
    pub attempt_count: i32,
    /// Last error message.
    pub last_error: Option<String>,
    /// When it last changed.
    pub updated_at: DateTime<Utc>,
    /// The delivery id (for replay).
    pub delivery_id: Uuid,
}

/// The most recent failed / dead deliveries across ALL tenants.
pub async fn admin_recent_failures(pool: &PgPool, limit: i64) -> CoreResult<Vec<RecentFailure>> {
    let rows = sqlx::query_as::<_, RecentFailure>(
        r#"
        SELECT d.tenant_id, ev.event_type, e.url AS endpoint_url, d.status,
               d.attempt_count, d.last_error, d.updated_at, d.id AS delivery_id
        FROM deliveries d
        JOIN events ev    ON ev.id = d.event_id
        JOIN endpoints e  ON e.id  = d.endpoint_id
        WHERE d.status IN ('failed', 'dead')
        ORDER BY d.updated_at DESC
        LIMIT $1
        "#,
    )
    .bind(limit)
    .fetch_all(pool)
    .await?;
    Ok(rows)
}

/// Look up the tenant that owns an API key (by sha256 hash). Returns the
/// tenant id if the key is valid.
pub async fn tenant_for_api_key(pool: &PgPool, key_hash: &str) -> CoreResult<Option<String>> {
    let tenant =
        sqlx::query_scalar::<_, String>("SELECT tenant_id FROM api_keys WHERE key_hash = $1")
            .bind(key_hash)
            .fetch_optional(pool)
            .await?;
    Ok(tenant)
}

/// Insert an API key (sha256 hash) for a tenant (seeding / tests).
pub async fn insert_api_key(pool: &PgPool, tenant_id: &str, key_hash: &str) -> CoreResult<()> {
    sqlx::query(
        "INSERT INTO api_keys (id, tenant_id, key_hash) VALUES ($1, $2, $3)
         ON CONFLICT (key_hash) DO NOTHING",
    )
    .bind(Uuid::now_v7())
    .bind(tenant_id)
    .bind(key_hash)
    .execute(pool)
    .await?;
    Ok(())
}

/// Require a delivery to exist, returning [`CoreError::NotFound`] otherwise.
pub async fn require_delivery(pool: &PgPool, id: Uuid) -> CoreResult<Delivery> {
    get_delivery(pool, id)
        .await?
        .ok_or_else(|| CoreError::NotFound(format!("delivery {id}")))
}

/// Convenience: parse a delivery's status, mapping errors into [`CoreError`].
pub fn delivery_status(delivery: &Delivery) -> CoreResult<DeliveryStatus> {
    delivery.status()
}
