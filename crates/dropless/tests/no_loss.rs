//! Integration proof: **no delivery is ever dropped across a crash.**
//!
//! Requires a live Postgres (`DATABASE_URL`), so it is `#[ignore]`d and the
//! default `cargo test` (which must pass without a database) skips it. CI runs
//! it with `cargo test -- --ignored` against a Postgres service.
//!
//! Shape: enqueue N messages → start workers → **abort them mid-flight**
//! (simulating `kill -9`, no graceful drain) → wait for lock leases to lapse →
//! restart workers → assert every message eventually arrives exactly the set we
//! sent (duplicates are allowed and expected; drops are not).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::{extract::State, http::HeaderMap, routing::post, Router};
use dropless_core::dispatcher::DispatcherConfig;
use dropless_core::retry::RetryPolicy;
use dropless_core::{outbox, store};
use sqlx::postgres::PgPoolOptions;
use tokio::sync::{watch, Mutex};
use uuid::Uuid;

type Received = Arc<Mutex<HashMap<String, u32>>>;

const N: usize = 200;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires DATABASE_URL (run in CI integration job)"]
async fn no_loss_across_crash() {
    let database_url =
        std::env::var("DATABASE_URL").expect("set DATABASE_URL to run the integration proof");

    let pool = PgPoolOptions::new()
        .max_connections(10)
        .connect(&database_url)
        .await
        .expect("connect");
    dropless_core::migrate(&pool).await.expect("migrate");

    // Isolate this run from any existing data.
    let tenant = format!("it-{}", Uuid::now_v7());

    // ---- mock subscriber: records each webhook-id it receives ----
    let received: Received = Arc::new(Mutex::new(HashMap::new()));
    let app = Router::new()
        .route("/", post(record))
        .with_state(received.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind mock");
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });

    // One endpoint → exactly one delivery per message.
    store::create_endpoint(
        &pool,
        &tenant,
        None,
        &format!("http://{addr}/"),
        "whsec_dGVzdA==",
        None,
    )
    .await
    .expect("create endpoint");

    // ---- enqueue N messages (the outbox commits before we ever "ack") ----
    let mut expected = Vec::with_capacity(N);
    for i in 0..N {
        let enq = outbox::enqueue_message(
            &pool,
            &tenant,
            None,
            "it.event",
            &serde_json::json!({ "n": i }),
        )
        .await
        .expect("enqueue");
        expected.extend(enq.delivery_ids);
    }
    assert_eq!(expected.len(), N, "one delivery per message");

    let cfg = DispatcherConfig {
        worker_count: 4,
        batch_size: 16,
        lock_secs: 2, // short lease so reclaim after the "crash" is quick
        http_timeout: Duration::from_secs(5),
        poll_interval: Duration::from_millis(100),
        retry: RetryPolicy::default(),
        circuit: Default::default(),
    };

    // ---- run, then CRASH (abort = no graceful drain, like kill -9) ----
    let (tx1, rx1) = watch::channel(false);
    let handles =
        dropless_core::dispatcher::spawn_workers(pool.clone(), cfg.clone(), rx1).expect("spawn 1");
    tokio::time::sleep(Duration::from_millis(300)).await;
    for h in handles {
        h.abort();
    }
    drop(tx1);

    // The crash must have left deliveries mid-flight; otherwise this proof is
    // vacuous (it would pass even with no crash at all). Fail loudly if nothing
    // was in_progress, so a regression that lets deliveries drain too fast — or
    // breaks the reclaim path — cannot ship green.
    let in_flight: i64 = sqlx::query_scalar::<_, i64>(
        "SELECT count(*) FROM deliveries WHERE tenant_id = $1 AND status = 'in_progress'",
    )
    .bind(&tenant)
    .fetch_one(&pool)
    .await
    .expect("count in-flight");
    assert!(
        in_flight > 0,
        "crash should leave deliveries in_progress to exercise reclaim; found {in_flight}"
    );

    // Let the in-progress lock leases expire so the rows are reclaimable.
    tokio::time::sleep(Duration::from_millis(2_500)).await;

    // ---- restart workers and let them drain ----
    let (tx2, rx2) = watch::channel(false);
    let handles =
        dropless_core::dispatcher::spawn_workers(pool.clone(), cfg.clone(), rx2).expect("spawn 2");

    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let unique = received.lock().await.len();
        if unique >= N {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "timed out: only {unique}/{N} unique deliveries arrived"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    // Graceful stop.
    let _ = tx2.send(true);
    for h in handles {
        let _ = h.await;
    }

    // ---- assertions: drop 0; duplicates allowed ----
    let map = received.lock().await;
    let unique = map.len();
    let total: u32 = map.values().sum();
    let dups = total as usize - unique;
    assert_eq!(
        unique, N,
        "every message must arrive at least once (0 drops)"
    );

    // Cross-check against the database: no delivery left undelivered.
    let undelivered: i64 = sqlx::query_scalar::<_, i64>(
        "SELECT count(*) FROM deliveries WHERE tenant_id = $1 AND status <> 'succeeded'",
    )
    .bind(&tenant)
    .fetch_one(&pool)
    .await
    .expect("count");
    assert_eq!(undelivered, 0, "all deliveries should be 'succeeded'");

    eprintln!("no-loss proof: {unique} unique, {total} total, {dups} duplicate(s)");
}

async fn record(State(received): State<Received>, headers: HeaderMap) -> &'static str {
    // Hold each request briefly so deliveries are genuinely in flight when the
    // workers are aborted — otherwise they could all complete before the
    // "crash" and the proof would pass vacuously without exercising reclaim.
    tokio::time::sleep(Duration::from_millis(25)).await;
    let id = headers
        .get("webhook-id")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("MISSING")
        .to_string();
    *received.lock().await.entry(id).or_insert(0) += 1;
    "ok"
}
