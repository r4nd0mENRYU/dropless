//! Coalesced enqueue notifier.
//!
//! Wakes idle dispatch workers via Postgres `NOTIFY` the moment a message is
//! enqueued — but at most once per `window`. Issuing a `NOTIFY` inside (or per)
//! every enqueue transaction serializes commits on the async-notify queue lock,
//! which throttles ingest throughput under load. Coalescing keeps the
//! low-latency wake-up for light traffic while removing that contention under
//! bursts (one worker waking is enough; it then drains continuously).
//!
//! `NOTIFY` is best-effort: it is fire-and-forget, off the producer's `2xx`
//! path, and the dispatcher's poll loop is the correctness fallback. See
//! [`crate::NOTIFY_CHANNEL`].

use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use sqlx::PgPool;
use tracing::debug;

/// Rate-limited waker. Cheap to `ping` (an atomic load on the hot path).
pub struct Notifier {
    pool: PgPool,
    start: Instant,
    last_ms: AtomicI64,
    window_ms: i64,
}

impl Notifier {
    /// Build a notifier that emits at most one `NOTIFY` per `window_ms`.
    pub fn new(pool: PgPool, window_ms: i64) -> Arc<Self> {
        Arc::new(Notifier {
            pool,
            start: Instant::now(),
            last_ms: AtomicI64::new(i64::MIN),
            window_ms,
        })
    }

    /// Fire-and-forget, coalesced wake. Call **after** an enqueue has committed.
    ///
    /// Almost always just an atomic load + compare; only the first `ping` in
    /// each window spawns a detached task that issues the actual `NOTIFY`, so it
    /// never sits on the producer's response path.
    pub fn ping(self: &Arc<Self>) {
        let now = self.start.elapsed().as_millis() as i64;
        let last = self.last_ms.load(Ordering::Relaxed);
        if now.saturating_sub(last) < self.window_ms {
            return;
        }
        // Claim this window; if a concurrent ping won the race, let it send.
        if self
            .last_ms
            .compare_exchange(last, now, Ordering::Relaxed, Ordering::Relaxed)
            .is_err()
        {
            return;
        }
        let me = self.clone();
        tokio::spawn(async move {
            if let Err(e) = sqlx::query("SELECT pg_notify($1, '')")
                .bind(crate::NOTIFY_CHANNEL)
                .execute(&me.pool)
                .await
            {
                debug!(error = %e, "enqueue NOTIFY failed (poll loop covers it)");
            }
        });
    }
}
