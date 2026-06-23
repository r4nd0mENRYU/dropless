//! Runtime configuration, sourced from CLI flags and environment variables.

use std::time::Duration;

use dropless_core::dispatcher::DispatcherConfig;
use dropless_core::retry::RetryPolicy;

/// Resolved server configuration.
#[derive(Debug, Clone)]
pub struct Config {
    /// Postgres connection string.
    pub database_url: String,
    /// Address the HTTP API binds to.
    pub bind_addr: String,
    /// Max database pool connections.
    pub max_connections: u32,
    /// Dispatcher tuning.
    pub dispatcher: DispatcherConfig,
}

impl Config {
    /// Build the dispatcher config from individual knobs.
    pub fn dispatcher_from(
        worker_count: usize,
        batch_size: i64,
        lock_secs: i32,
        http_timeout_secs: u64,
        max_attempts: i32,
        poll_interval_ms: u64,
    ) -> DispatcherConfig {
        DispatcherConfig {
            worker_count,
            batch_size,
            lock_secs,
            http_timeout: Duration::from_secs(http_timeout_secs),
            // Fallback cadence; LISTEN/NOTIFY handles low-latency first delivery.
            poll_interval: Duration::from_millis(poll_interval_ms),
            retry: RetryPolicy {
                max_attempts,
                ..RetryPolicy::default()
            },
            circuit: Default::default(),
        }
    }
}
