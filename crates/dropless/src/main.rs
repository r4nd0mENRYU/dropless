//! Dropless — self-hosted webhooks that never drop.
//!
//! Single binary: `serve` runs the API and/or the dispatch workers; `migrate`
//! applies schema; `create-api-key` / `create-endpoint` seed local testing.

mod api;
mod config;
mod ratelimit;

use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use clap::{Parser, Subcommand, ValueEnum};
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use tokio::sync::watch;
use tracing::{info, warn};

use crate::api::AppState;
use crate::config::Config;

/// Top-level CLI.
#[derive(Parser)]
#[command(
    name = "dropless",
    version,
    about = "Self-hosted webhooks that never drop"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Run the API server and/or dispatch workers.
    Serve(ServeArgs),
    /// Apply database migrations.
    Migrate(DbArgs),
    /// Create an API key for a tenant (prints nothing secret back).
    CreateApiKey(CreateApiKeyArgs),
    /// Create a subscriber endpoint for a tenant (optionally under a consumer).
    CreateEndpoint(CreateEndpointArgs),
    /// Create a consumer (an end-customer / application) for a tenant.
    CreateConsumer(CreateConsumerArgs),
    /// Create an inbound source (Stripe/GitHub/HMAC) for a tenant.
    CreateInboundSource(CreateInboundSourceArgs),
}

/// Which parts of the system this process runs.
#[derive(Copy, Clone, Debug, ValueEnum, PartialEq, Eq)]
enum Role {
    /// API server + dispatch workers (default, single-node).
    All,
    /// API server only.
    Api,
    /// Dispatch workers only.
    Worker,
}

#[derive(Parser)]
struct ServeArgs {
    /// Postgres connection string.
    #[arg(long, env = "DATABASE_URL")]
    database_url: String,
    /// Address to bind the HTTP API to.
    #[arg(long, env = "BIND_ADDR", default_value = "0.0.0.0:8080")]
    bind: String,
    /// Which role(s) to run.
    #[arg(long, value_enum, env = "DROPLESS_ROLE", default_value_t = Role::All)]
    role: Role,
    /// Max database pool connections.
    #[arg(long, env = "MAX_CONNECTIONS", default_value_t = 10)]
    max_connections: u32,
    /// Number of dispatch workers.
    #[arg(long, env = "WORKER_COUNT", default_value_t = 4)]
    workers: usize,
    /// Deliveries claimed per poll.
    #[arg(long, env = "WORKER_BATCH", default_value_t = 32)]
    batch: i64,
    /// Lock lease seconds for a claimed delivery.
    #[arg(long, env = "LOCK_SECS", default_value_t = 30)]
    lock_secs: i32,
    /// Per-request HTTP timeout (seconds).
    #[arg(long, env = "HTTP_TIMEOUT_SECS", default_value_t = 10)]
    http_timeout_secs: u64,
    /// Max delivery attempts before dead-lettering.
    #[arg(long, env = "MAX_ATTEMPTS", default_value_t = 16)]
    max_attempts: i32,
    /// Fallback poll interval (ms) when idle. LISTEN/NOTIFY handles the
    /// low-latency path; this catches time-based retries and missed notifies.
    #[arg(long, env = "POLL_INTERVAL_MS", default_value_t = 500)]
    poll_interval_ms: u64,
    /// If set, `/metrics` requires `Authorization: Bearer <token>`. Leave unset
    /// only if `/metrics` is firewalled (its counts are cross-tenant).
    #[arg(long, env = "METRICS_TOKEN")]
    metrics_token: Option<String>,
    /// Operator token for the cross-tenant admin console at `/admin`. If unset,
    /// the admin API and dashboard are disabled (404).
    #[arg(long, env = "ADMIN_TOKEN")]
    admin_token: Option<String>,
    /// Max seconds to wait for in-flight deliveries to drain on shutdown.
    /// After this, exit anyway — undrained rows are reclaimed on restart.
    #[arg(long, env = "SHUTDOWN_TIMEOUT_SECS", default_value_t = 25)]
    shutdown_timeout_secs: u64,
    /// Per-tenant ingest rate limit (requests/sec). 0 disables it.
    #[arg(long, env = "RATE_LIMIT_RPS", default_value_t = 0.0)]
    rate_limit_rps: f64,
    /// Token-bucket burst capacity. Defaults to 2× the rps when left at 0.
    #[arg(long, env = "RATE_LIMIT_BURST", default_value_t = 0.0)]
    rate_limit_burst: f64,
}

#[derive(Parser)]
struct DbArgs {
    /// Postgres connection string.
    #[arg(long, env = "DATABASE_URL")]
    database_url: String,
}

#[derive(Parser)]
struct CreateApiKeyArgs {
    #[arg(long, env = "DATABASE_URL")]
    database_url: String,
    /// Tenant the key belongs to.
    #[arg(long)]
    tenant: String,
    /// The plaintext key (only its sha256 hash is stored).
    #[arg(long)]
    key: String,
}

#[derive(Parser)]
struct CreateEndpointArgs {
    #[arg(long, env = "DATABASE_URL")]
    database_url: String,
    /// Tenant the endpoint belongs to.
    #[arg(long)]
    tenant: String,
    /// Destination URL.
    #[arg(long)]
    url: String,
    /// HMAC signing secret.
    #[arg(long)]
    secret: String,
    /// Comma-separated event types to deliver (exact, `prefix.*`, or `*`).
    /// Omit to deliver ALL events.
    #[arg(long, value_delimiter = ',')]
    event_types: Vec<String>,
    /// Attach the endpoint to this consumer (uid). Omit for a tenant-level endpoint.
    #[arg(long)]
    consumer: Option<String>,
}

#[derive(Parser)]
struct CreateConsumerArgs {
    #[arg(long, env = "DATABASE_URL")]
    database_url: String,
    /// Tenant the consumer belongs to.
    #[arg(long)]
    tenant: String,
    /// SaaS-assigned customer id, unique within the tenant.
    #[arg(long)]
    uid: String,
    /// Optional human-readable label.
    #[arg(long)]
    name: Option<String>,
}

#[derive(Parser)]
struct CreateInboundSourceArgs {
    #[arg(long, env = "DATABASE_URL")]
    database_url: String,
    /// Tenant the source belongs to.
    #[arg(long)]
    tenant: String,
    /// URL slug: `POST /ingest/{slug}` (globally unique).
    #[arg(long)]
    slug: String,
    /// Provider: stripe | github | hmac.
    #[arg(long)]
    provider: String,
    /// Verification secret (the provider's signing secret).
    #[arg(long)]
    secret: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,sqlx=warn".into()),
        )
        .init();

    match Cli::parse().command {
        Command::Serve(args) => serve(args).await,
        Command::Migrate(args) => {
            let pool = connect(&args.database_url, 2).await?;
            dropless_core::migrate(&pool)
                .await
                .context("migration failed")?;
            info!("migrations applied");
            Ok(())
        }
        Command::CreateApiKey(args) => {
            let pool = connect(&args.database_url, 2).await?;
            let hash = api::hash_key(&args.key);
            dropless_core::store::insert_api_key(&pool, &args.tenant, &hash).await?;
            info!(tenant = %args.tenant, "api key created");
            Ok(())
        }
        Command::CreateEndpoint(args) => {
            let pool = connect(&args.database_url, 2).await?;
            let event_types = (!args.event_types.is_empty()).then_some(args.event_types.as_slice());
            // Resolve an optional consumer uid to its id (must already exist).
            let consumer_id = match &args.consumer {
                Some(uid) => Some(
                    dropless_core::store::get_consumer(&pool, &args.tenant, uid)
                        .await?
                        .context("no such consumer for this tenant")?
                        .id,
                ),
                None => None,
            };
            let ep = dropless_core::store::create_endpoint(
                &pool,
                &args.tenant,
                consumer_id,
                &args.url,
                &args.secret,
                event_types,
            )
            .await?;
            info!(endpoint_id = %ep.id, tenant = %args.tenant, "endpoint created");
            println!("{}", ep.id);
            Ok(())
        }
        Command::CreateConsumer(args) => {
            let pool = connect(&args.database_url, 2).await?;
            let consumer = dropless_core::store::create_consumer(
                &pool,
                &args.tenant,
                &args.uid,
                args.name.as_deref(),
            )
            .await?;
            info!(consumer_id = %consumer.id, uid = %consumer.uid, "consumer created");
            println!("{}", consumer.id);
            Ok(())
        }
        Command::CreateInboundSource(args) => {
            // Validate the provider name before touching the DB.
            args.provider
                .parse::<dropless_core::inbound::Provider>()
                .context("invalid provider (expected stripe | github | hmac)")?;
            let pool = connect(&args.database_url, 2).await?;
            let src = dropless_core::store::create_inbound_source(
                &pool,
                &args.tenant,
                &args.slug,
                &args.provider,
                &args.secret,
            )
            .await?;
            info!(source_id = %src.id, slug = %src.slug, provider = %src.provider, "inbound source created");
            println!("{}", src.id);
            Ok(())
        }
    }
}

/// Open a Postgres pool.
async fn connect(url: &str, max_connections: u32) -> anyhow::Result<PgPool> {
    PgPoolOptions::new()
        .max_connections(max_connections)
        .connect(url)
        .await
        .context("failed to connect to Postgres")
}

/// Run the server according to the selected role, with graceful shutdown.
async fn serve(args: ServeArgs) -> anyhow::Result<()> {
    let dispatcher = Config::dispatcher_from(
        args.workers,
        args.batch,
        args.lock_secs,
        args.http_timeout_secs,
        args.max_attempts,
        args.poll_interval_ms,
    );

    // High#1 guard: the lease-ownership write guard makes a short lease *safe*
    // (no clobber, no loss), but if a claimed batch can't drain within the
    // lease, its tail is re-claimed and re-delivered. Warn so operators size it.
    let worst_case_secs = args.batch.max(1) as u64 * args.http_timeout_secs;
    if (args.lock_secs as u64) < worst_case_secs {
        warn!(
            lock_secs = args.lock_secs,
            batch = args.batch,
            http_timeout_secs = args.http_timeout_secs,
            "lock_secs < batch*http_timeout ({worst_case_secs}s): a slow batch may outlive its lease and be re-delivered (no loss, just extra duplicates). Raise --lock-secs or lower --batch."
        );
    }

    // Railway and most PaaS inject $PORT; bind to it when present.
    let bind_addr = match std::env::var("PORT") {
        Ok(port) if !port.is_empty() => format!("0.0.0.0:{port}"),
        _ => args.bind,
    };
    let cfg = Config {
        database_url: args.database_url,
        bind_addr,
        max_connections: args.max_connections,
        dispatcher,
    };

    let pool = connect(&cfg.database_url, cfg.max_connections).await?;

    // Apply migrations on startup unless disabled — handy for PaaS deploys with
    // no separate migrate step (the runtime image is distroless / shell-less).
    // Idempotent; sqlx serializes migration via an advisory lock, so concurrent
    // instances are safe.
    if std::env::var("AUTO_MIGRATE").as_deref() != Ok("false") {
        dropless_core::migrate(&pool)
            .await
            .context("startup migration failed")?;
        info!("migrations applied on startup");
    }

    // Shutdown broadcast: a single signal task flips this to `true`.
    let (tx, rx) = watch::channel(false);
    tokio::spawn(async move {
        wait_for_signal().await;
        info!("shutdown signal received, draining");
        let _ = tx.send(true);
    });

    let run_api = matches!(args.role, Role::All | Role::Api);
    let run_workers = matches!(args.role, Role::All | Role::Worker);

    let worker_handles = if run_workers {
        dropless_core::dispatcher::spawn_workers(pool.clone(), cfg.dispatcher.clone(), rx.clone())
            .context("failed to start workers")?
    } else {
        Vec::new()
    };

    if run_api {
        // Coalesce enqueue NOTIFYs to ~40/s max so bursts don't serialize
        // commits, while light traffic still wakes workers within ~25ms.
        let notifier = dropless_core::notify::Notifier::new(pool.clone(), 25);
        let burst = if args.rate_limit_burst > 0.0 {
            args.rate_limit_burst
        } else {
            args.rate_limit_rps * 2.0
        };
        let limiter = Arc::new(ratelimit::RateLimiter::new(args.rate_limit_rps, burst));
        if limiter.enabled() {
            info!(
                rps = args.rate_limit_rps,
                burst, "per-tenant ingest rate limiting enabled"
            );
        }
        let state = AppState {
            pool: pool.clone(),
            notifier,
            metrics_token: args.metrics_token.as_deref().map(Arc::from),
            admin_token: args.admin_token.as_deref().map(Arc::from),
            limiter,
        };
        let app = api::router(state);
        let listener = tokio::net::TcpListener::bind(&cfg.bind_addr)
            .await
            .with_context(|| format!("failed to bind {}", cfg.bind_addr))?;
        info!(addr = %cfg.bind_addr, role = ?args.role, "dropless listening");
        axum::serve(listener, app)
            .with_graceful_shutdown(shutdown_when(rx.clone()))
            .await
            .context("server error")?;
    } else {
        // Worker-only: block until shutdown is signalled.
        shutdown_when(rx.clone()).await;
    }

    // Drain workers, bounded so shutdown finishes within the orchestrator grace
    // period. Any in-flight rows not drained in time stay `in_progress` and are
    // reclaimed once their lease lapses on the next start — never lost.
    let drain = async {
        for handle in worker_handles {
            let _ = handle.await;
        }
    };
    match tokio::time::timeout(Duration::from_secs(args.shutdown_timeout_secs), drain).await {
        Ok(()) => info!("shutdown complete"),
        Err(_) => warn!(
            timeout_secs = args.shutdown_timeout_secs,
            "drain timed out; exiting (in-flight rows will be reclaimed on restart)"
        ),
    }
    Ok(())
}

/// Future that resolves once the shutdown watch flips to `true`.
async fn shutdown_when(mut rx: watch::Receiver<bool>) {
    while !*rx.borrow() {
        if rx.changed().await.is_err() {
            break;
        }
    }
}

/// Resolve on Ctrl-C or SIGTERM.
async fn wait_for_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };

    #[cfg(unix)]
    let terminate = async {
        use tokio::signal::unix::{signal, SignalKind};
        match signal(SignalKind::terminate()) {
            Ok(mut s) => {
                s.recv().await;
            }
            Err(_) => std::future::pending::<()>().await,
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}
