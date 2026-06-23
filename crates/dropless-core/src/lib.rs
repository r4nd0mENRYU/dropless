//! # dropless-core
//!
//! The 0-loss webhook engine: domain model, Postgres store (source of truth
//! **and** queue), single-transaction outbox fan-out, the SKIP-LOCKED dispatch
//! worker pool, retry/backoff, circuit breaking, and Svix-compatible signing.
//!
//! ## Core invariant
//!
//! *Raw is committed to Postgres before any `2xx` is returned.* See
//! [`outbox::enqueue_message`]. Delivery is **at-least-once** with an immutable
//! `Idempotency-Key` per message — *effectively-once*, never *exactly-once*.
//!
//! ## Build constraint
//!
//! Every query uses the runtime-checked sqlx API, so this crate compiles
//! without a live database or `DATABASE_URL`.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod circuit;
pub mod dispatcher;
pub mod error;
pub mod inbound;
pub mod inbox;
pub mod model;
pub mod notify;
pub mod outbox;
pub mod retry;
pub mod signing;
pub mod store;

pub use error::{CoreError, CoreResult};

/// Postgres `LISTEN`/`NOTIFY` channel used to wake idle dispatch workers the
/// moment a message is enqueued. The poll loop is the fallback (for time-based
/// retries and any missed notification), so this is purely a latency optimization.
pub const NOTIFY_CHANNEL: &str = "dropless_deliveries";

/// Run all embedded migrations against the given pool.
///
/// `sqlx::migrate!` reads the `migrations/` directory at compile time (no
/// database needed to build) and applies them at runtime.
pub async fn migrate(pool: &sqlx::PgPool) -> Result<(), sqlx::migrate::MigrateError> {
    sqlx::migrate!("../../migrations").run(pool).await
}
