//! The dispatch worker pool.
//!
//! Each worker claims due deliveries with `FOR UPDATE SKIP LOCKED`, signs and
//! POSTs them, and records the outcome — all coordination-free, since the row
//! lock grants single ownership. The crash-safety story lives here: a worker
//! that dies mid-attempt simply leaves a row whose lock expires and whose
//! `next_attempt_at` is in the past, so another worker re-claims it. At worst
//! a message is delivered twice (the subscriber dedupes on `Idempotency-Key`);
//! it is never dropped.

use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use reqwest::Client;
use sqlx::postgres::PgListener;
use sqlx::PgPool;
use tokio::sync::{watch, Notify};
use tracing::{debug, error, info, warn};

use crate::circuit::{self, CircuitPolicy, Gate};
use crate::error::CoreResult;
use crate::retry::RetryPolicy;
use crate::store;

/// Maximum number of response-body bytes stored per attempt.
const RESPONSE_SNIPPET_LEN: usize = 500;

/// How long to defer a delivery whose endpoint is currently disabled before
/// re-checking whether it has been re-enabled. Disabled endpoints pause (never
/// drop) their queued deliveries.
const DISABLED_RECHECK_SECS: i64 = 60;

/// Runtime configuration for the dispatcher.
#[derive(Debug, Clone)]
pub struct DispatcherConfig {
    /// Number of concurrent workers.
    pub worker_count: usize,
    /// Maximum deliveries claimed per poll.
    pub batch_size: i64,
    /// Lock lease length for a claimed delivery.
    pub lock_secs: i32,
    /// Per-request HTTP timeout.
    pub http_timeout: Duration,
    /// Sleep between polls when no work is available.
    pub poll_interval: Duration,
    /// Backoff / max-attempts policy.
    pub retry: RetryPolicy,
    /// Circuit-breaker policy.
    pub circuit: CircuitPolicy,
}

impl Default for DispatcherConfig {
    fn default() -> Self {
        DispatcherConfig {
            worker_count: 4,
            batch_size: 32,
            lock_secs: 30,
            http_timeout: Duration::from_secs(10),
            poll_interval: Duration::from_millis(500),
            retry: RetryPolicy::default(),
            circuit: CircuitPolicy::default(),
        }
    }
}

/// Build the shared HTTP client for the worker pool.
pub fn build_client(cfg: &DispatcherConfig) -> CoreResult<Client> {
    let client = Client::builder()
        .timeout(cfg.http_timeout)
        .user_agent(concat!("dropless/", env!("CARGO_PKG_VERSION")))
        .build()?;
    Ok(client)
}

/// Spawn `cfg.worker_count` workers plus one `LISTEN`er. They run until
/// `shutdown` flips to `true`. Returns the join handles so the caller can await
/// a clean drain.
pub fn spawn_workers(
    pool: PgPool,
    cfg: DispatcherConfig,
    shutdown: watch::Receiver<bool>,
) -> CoreResult<Vec<tokio::task::JoinHandle<()>>> {
    let client = build_client(&cfg)?;
    // Shared wake signal: the listener pulses it on each enqueue NOTIFY; idle
    // workers wait on it (or the poll fallback), so first-delivery latency
    // isn't bound by the poll interval.
    let wake = Arc::new(Notify::new());
    let mut handles = Vec::with_capacity(cfg.worker_count + 1);

    {
        let pool = pool.clone();
        let wake = wake.clone();
        let shutdown = shutdown.clone();
        handles.push(tokio::spawn(async move {
            listen_loop(pool, wake, shutdown).await;
        }));
    }

    for i in 0..cfg.worker_count {
        let worker_id = format!("worker-{i}");
        let pool = pool.clone();
        let client = client.clone();
        let cfg = cfg.clone();
        let shutdown = shutdown.clone();
        let wake = wake.clone();
        handles.push(tokio::spawn(async move {
            run_worker(pool, client, worker_id, cfg, shutdown, wake).await;
        }));
    }
    info!(workers = cfg.worker_count, "dispatcher started");
    Ok(handles)
}

/// Hold a single `LISTEN` connection and pulse `wake` on every enqueue
/// notification. Reconnects on error; the worker poll loop covers any gap.
async fn listen_loop(pool: PgPool, wake: Arc<Notify>, mut shutdown: watch::Receiver<bool>) {
    loop {
        if *shutdown.borrow() {
            return;
        }
        let mut listener = match PgListener::connect_with(&pool).await {
            Ok(l) => l,
            Err(e) => {
                warn!(error = %e, "listener connect failed; polling covers it, retrying");
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_secs(1)) => {}
                    _ = shutdown.changed() => {}
                }
                continue;
            }
        };
        if let Err(e) = listener.listen(crate::NOTIFY_CHANNEL).await {
            warn!(error = %e, "LISTEN failed; polling covers it, retrying");
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_secs(1)) => {}
                _ = shutdown.changed() => {}
            }
            continue;
        }
        debug!(
            channel = crate::NOTIFY_CHANNEL,
            "listening for enqueue notifications"
        );
        loop {
            tokio::select! {
                res = listener.recv() => match res {
                    Ok(_) => wake.notify_waiters(),
                    Err(e) => {
                        warn!(error = %e, "listener recv error; reconnecting");
                        break;
                    }
                },
                _ = shutdown.changed() => {
                    if *shutdown.borrow() {
                        return;
                    }
                }
            }
        }
    }
}

/// The per-worker loop: claim → process → repeat, with graceful shutdown.
pub async fn run_worker(
    pool: PgPool,
    client: Client,
    worker_id: String,
    cfg: DispatcherConfig,
    mut shutdown: watch::Receiver<bool>,
    wake: Arc<Notify>,
) {
    loop {
        if *shutdown.borrow() {
            debug!(%worker_id, "shutting down");
            break;
        }

        let claimed =
            match store::claim_due_deliveries(&pool, &worker_id, cfg.batch_size, cfg.lock_secs)
                .await
            {
                Ok(rows) => rows,
                Err(e) => {
                    error!(%worker_id, error = %e, "claim failed");
                    sleep_or_shutdown(&mut shutdown, cfg.poll_interval).await;
                    continue;
                }
            };

        if claimed.is_empty() {
            // Idle: wake on the next enqueue NOTIFY, or the poll fallback
            // (which also catches retries that become due over time).
            tokio::select! {
                _ = wake.notified() => {}
                _ = tokio::time::sleep(cfg.poll_interval) => {}
                _ = shutdown.changed() => {}
            }
            continue;
        }

        for delivery in claimed {
            // Honor shutdown between deliveries so a large batch can't delay it;
            // unprocessed claimed rows stay in_progress and are reclaimed later.
            if *shutdown.borrow() {
                debug!(%worker_id, "shutting down mid-batch");
                break;
            }
            if let Err(e) = process_one(&pool, &client, &cfg, &worker_id, &delivery).await {
                // A processing error (e.g. DB hiccup) leaves the lock to
                // expire; the row is retried. Never a drop.
                warn!(delivery_id = %delivery.id, error = %e, "processing error");
            }
        }
    }
}

/// Process a single claimed delivery end-to-end.
pub async fn process_one(
    pool: &PgPool,
    client: &Client,
    cfg: &DispatcherConfig,
    worker_id: &str,
    delivery: &crate::model::Delivery,
) -> CoreResult<()> {
    let now = Utc::now();

    // Load the endpoint; if it vanished, dead-letter rather than spin.
    let Some(endpoint) = store::get_endpoint(pool, delivery.endpoint_id).await? else {
        store::mark_dead(pool, delivery, worker_id, "endpoint no longer exists").await?;
        return Ok(());
    };

    // An operator-disabled endpoint must stop receiving deliveries, even ones
    // queued before it was disabled. Defer (never drop) so traffic resumes
    // automatically if the endpoint is re-enabled.
    if endpoint.disabled {
        debug!(delivery_id = %delivery.id, endpoint_id = %endpoint.id, "endpoint disabled, deferring");
        store::defer_delivery(
            pool,
            delivery.id,
            worker_id,
            now + chrono::Duration::seconds(DISABLED_RECHECK_SECS),
        )
        .await?;
        return Ok(());
    }

    // Consult the circuit breaker.
    match circuit::gate(&endpoint, now) {
        Gate::Skip(until) => {
            debug!(delivery_id = %delivery.id, "circuit open, deferring");
            store::defer_delivery(pool, delivery.id, worker_id, until).await?;
            return Ok(());
        }
        Gate::Probe => {
            // The open window elapsed: allow exactly ONE half-open probe. The
            // first worker to flip the endpoint to `half_open` wins; the rest
            // defer, so a recovering endpoint isn't hit by the whole backlog.
            let lease = cfg.circuit.open_duration.num_seconds().max(1) as i32;
            if store::try_acquire_probe(pool, endpoint.id, lease).await? {
                debug!(delivery_id = %delivery.id, endpoint_id = %endpoint.id, "half-open probe acquired");
            } else {
                debug!(delivery_id = %delivery.id, "half-open probe held by another worker, deferring");
                store::defer_delivery(
                    pool,
                    delivery.id,
                    worker_id,
                    now + cfg.circuit.open_duration,
                )
                .await?;
                return Ok(());
            }
        }
        Gate::Allow => {}
    }

    // Load the event payload.
    let Some(event) = store::get_event(pool, delivery.event_id).await? else {
        store::mark_dead(pool, delivery, worker_id, "event no longer exists").await?;
        return Ok(());
    };

    // Serialize and sign. Both are deterministic for a given delivery: if they
    // fail, a retry can never succeed, so dead-letter immediately rather than
    // leave a poison row that is reclaimed forever without progress. (Transient
    // DB errors, by contrast, propagate and are retried via lease expiry.)
    let body = match serde_json::to_vec(&event.payload) {
        Ok(body) => body,
        Err(e) => {
            warn!(delivery_id = %delivery.id, error = %e, "payload not serializable; dead-lettering");
            store::mark_dead(
                pool,
                delivery,
                worker_id,
                &format!("payload not serializable: {e}"),
            )
            .await?;
            return Ok(());
        }
    };
    let attempt_number = delivery.attempt_count + 1;
    let timestamp = now.timestamp();

    // Sign (Svix-compatible) and POST.
    let headers = match crate::signing::signature_headers(
        &endpoint.secret,
        &delivery.id.to_string(),
        timestamp,
        &body,
    ) {
        Ok(headers) => headers,
        Err(e) => {
            warn!(delivery_id = %delivery.id, error = %e, "cannot sign request; dead-lettering");
            store::mark_dead(
                pool,
                delivery,
                worker_id,
                &format!("cannot sign request: {e}"),
            )
            .await?;
            return Ok(());
        }
    };

    let started_at = Utc::now();
    let result = client
        .post(&endpoint.url)
        .header("content-type", "application/json")
        .header("idempotency-key", &delivery.idempotency_key)
        .header("webhook-id", &headers.id)
        .header("webhook-timestamp", &headers.timestamp)
        .header("webhook-signature", &headers.signature)
        .header("dropless-event-type", &event.event_type)
        .body(body)
        .send()
        .await;
    let finished_at = Utc::now();

    match result {
        Ok(resp) => {
            let status = resp.status();
            let code = status.as_u16() as i32;
            let text = resp.text().await.unwrap_or_default();
            let snippet = truncate(&text, RESPONSE_SNIPPET_LEN);

            if status.is_success() {
                // Coalesce the attempt log + status flip + circuit reset into a
                // SINGLE transaction, so each delivery costs one fsync, not three.
                let mut tx = pool.begin().await?;
                store::record_attempt(
                    &mut *tx,
                    delivery.id,
                    attempt_number,
                    Some(code),
                    Some(&snippet),
                    None,
                    started_at,
                    finished_at,
                )
                .await?;
                if store::mark_succeeded(&mut *tx, delivery.id, worker_id, attempt_number).await? {
                    let upd = circuit::on_success();
                    store::update_circuit(
                        &mut *tx,
                        endpoint.id,
                        &upd.state,
                        upd.open_until,
                        upd.consecutive_failures,
                    )
                    .await?;
                    tx.commit().await?;
                    debug!(delivery_id = %delivery.id, code, "delivered");
                } else {
                    // Lease lost — roll back so we don't leave a stray attempt row
                    // for a delivery another worker now owns.
                    tx.rollback().await?;
                    debug!(delivery_id = %delivery.id, "lease lost before success write; another worker owns this delivery");
                }
            } else {
                let err = format!("non-2xx status {code}");
                fail(
                    pool,
                    cfg,
                    worker_id,
                    delivery,
                    &endpoint,
                    attempt_number,
                    Some(code),
                    Some(&snippet),
                    &err,
                    started_at,
                    finished_at,
                )
                .await?;
            }
        }
        Err(e) => {
            let err = e.to_string();
            fail(
                pool,
                cfg,
                worker_id,
                delivery,
                &endpoint,
                attempt_number,
                None,
                None,
                &err,
                started_at,
                finished_at,
            )
            .await?;
        }
    }

    Ok(())
}

/// Record a failed attempt: append the attempt, schedule a retry (or
/// dead-letter if exhausted), and update the circuit breaker.
#[allow(clippy::too_many_arguments)]
async fn fail(
    pool: &PgPool,
    cfg: &DispatcherConfig,
    worker_id: &str,
    delivery: &crate::model::Delivery,
    endpoint: &crate::model::Endpoint,
    attempt_number: i32,
    status_code: Option<i32>,
    snippet: Option<&str>,
    error: &str,
    started_at: chrono::DateTime<Utc>,
    finished_at: chrono::DateTime<Utc>,
) -> CoreResult<()> {
    let exhausted = cfg.retry.is_exhausted(attempt_number);

    // Coalesce the attempt log + status transition + circuit update into a
    // SINGLE transaction (one fsync). Roll back wholesale if the lease is lost.
    let mut tx = pool.begin().await?;
    store::record_attempt(
        &mut *tx,
        delivery.id,
        attempt_number,
        status_code,
        snippet,
        Some(error),
        started_at,
        finished_at,
    )
    .await?;

    let applied = if exhausted {
        store::mark_dead_on(&mut tx, delivery, worker_id, error).await?
    } else {
        let next = Utc::now()
            + chrono::Duration::from_std(cfg.retry.backoff(attempt_number))
                .unwrap_or_else(|_| chrono::Duration::seconds(60));
        store::mark_failed(
            &mut *tx,
            delivery.id,
            worker_id,
            attempt_number,
            next,
            error,
        )
        .await?
    };

    if !applied {
        // Lease lost — another worker owns this delivery. Roll back (including
        // the attempt row) and don't skew the per-endpoint circuit on its behalf.
        tx.rollback().await?;
        debug!(delivery_id = %delivery.id, "lease lost before failure write; another worker owns this delivery");
        return Ok(());
    }

    let upd = circuit::on_failure(endpoint, &cfg.circuit, Utc::now());
    store::update_circuit(
        &mut *tx,
        endpoint.id,
        &upd.state,
        upd.open_until,
        upd.consecutive_failures,
    )
    .await?;
    tx.commit().await?;

    if exhausted {
        warn!(delivery_id = %delivery.id, attempts = attempt_number, "dead-lettered");
    } else {
        debug!(delivery_id = %delivery.id, attempt = attempt_number, "scheduled retry");
    }
    Ok(())
}

/// Sleep for `dur`, waking early if shutdown is signalled.
async fn sleep_or_shutdown(shutdown: &mut watch::Receiver<bool>, dur: Duration) {
    tokio::select! {
        _ = tokio::time::sleep(dur) => {}
        _ = shutdown.changed() => {}
    }
}

/// Truncate a string to at most `max` bytes on a char boundary.
fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max {
        return s.to_string();
    }
    let mut end = max;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    s[..end].to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn truncate_respects_char_boundaries() {
        assert_eq!(truncate("hello", 10), "hello");
        assert_eq!(truncate("hello", 3), "hel");
        // Multi-byte: '✓' is 3 bytes; truncating at 2 backs off to empty.
        assert_eq!(truncate("✓", 2), "");
    }
}
