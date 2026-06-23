//! Steady-state load generator for the 0-loss benchmark.
//!
//! Acts as BOTH the load source and the webhook receiver, so end-to-end
//! delivery latency is measured on a single monotonic clock: it timestamps when
//! each message is acked (committed → 201) and when the matching webhook lands,
//! then reports throughput and p50/p95/p99 latency plus drop / duplicate counts.
//!
//! Driven by `bench/bench.sh` (which starts the dropless server and seeds an
//! endpoint pointing at this receiver). All progress goes to **stderr**; a
//! single JSON object with the metrics is printed to **stdout**.
//!
//! Env:
//! - `BENCH_BASE_URL`     dropless API base (default `http://127.0.0.1:8090`)
//! - `BENCH_API_KEY`      bearer key (default `benchkey`)
//! - `BENCH_RECV_ADDR`    addr to listen on as the receiver (default `127.0.0.1:9090`)
//! - `BENCH_N`            messages to send (default `5000`)
//! - `BENCH_CONCURRENCY`  in-flight sends (default `64`)
//! - `BENCH_TIMEOUT_SECS` drain timeout (default `120`)

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::{extract::State, http::HeaderMap, routing::post, Router};
use tokio::sync::{Mutex, Semaphore};

/// webhook-id → first-receipt instant.
type Received = Arc<Mutex<HashMap<String, Instant>>>;

#[derive(Clone)]
struct RecvState {
    received: Received,
    total: Arc<AtomicU64>,
}

#[tokio::main]
async fn main() {
    let base = env("BENCH_BASE_URL", "http://127.0.0.1:8090");
    let key = env("BENCH_API_KEY", "benchkey");
    let recv_addr = env("BENCH_RECV_ADDR", "127.0.0.1:9090");
    let n: usize = env("BENCH_N", "5000").parse().expect("BENCH_N");
    let concurrency: usize = env("BENCH_CONCURRENCY", "64")
        .parse()
        .expect("BENCH_CONCURRENCY");
    // Optional inter-send pacing. With a small pace the offered rate stays below
    // delivery capacity, so e2e latency reflects true per-message latency (not
    // backlog) — used to show that LISTEN/NOTIFY beats the poll interval.
    let pace_ms: u64 = env("BENCH_PACE_MS", "0").parse().expect("BENCH_PACE_MS");
    let timeout_secs: u64 = env("BENCH_TIMEOUT_SECS", "120")
        .parse()
        .expect("BENCH_TIMEOUT_SECS");

    // ---- receiver: timestamps each unique webhook-id on first receipt ----
    let state = RecvState {
        received: Arc::new(Mutex::new(HashMap::with_capacity(n))),
        total: Arc::new(AtomicU64::new(0)),
    };
    let app = Router::new()
        .route("/", post(receive))
        .with_state(state.clone());
    let listener = tokio::net::TcpListener::bind(&recv_addr)
        .await
        .expect("bind receiver");
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    eprintln!("loadgen: receiver listening on {recv_addr}");

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .expect("client");

    // delivery-id → send instant; ack latencies (send → 201).
    let sent: Arc<Mutex<HashMap<String, Instant>>> =
        Arc::new(Mutex::new(HashMap::with_capacity(n)));
    let ack_lat_ms: Arc<Mutex<Vec<f64>>> = Arc::new(Mutex::new(Vec::with_capacity(n)));
    let send_fail = Arc::new(AtomicU64::new(0));
    let sem = Arc::new(Semaphore::new(concurrency));
    let url = format!("{base}/v1/messages");

    // ---- send N messages concurrently, recording ack latency + delivery id ----
    let t0 = Instant::now();
    let mut set = tokio::task::JoinSet::new();
    for i in 0..n {
        if pace_ms > 0 {
            tokio::time::sleep(Duration::from_millis(pace_ms)).await;
        }
        let permit = sem.clone().acquire_owned().await.expect("permit");
        let (client, url, key) = (client.clone(), url.clone(), key.clone());
        let (sent, ack_lat_ms, send_fail) = (sent.clone(), ack_lat_ms.clone(), send_fail.clone());
        set.spawn(async move {
            let _permit = permit;
            let body = serde_json::json!({ "event_type": "bench", "payload": { "n": i } });
            let send_t = Instant::now();
            let resp = client
                .post(&url)
                .header("authorization", format!("Bearer {key}"))
                .json(&body)
                .send()
                .await;
            let ack_t = Instant::now();
            match resp {
                Ok(r) if r.status().is_success() => {
                    if let Ok(v) = r.json::<serde_json::Value>().await {
                        if let Some(id) = v
                            .get("delivery_ids")
                            .and_then(|d| d.get(0))
                            .and_then(|x| x.as_str())
                        {
                            sent.lock().await.insert(id.to_string(), send_t);
                            ack_lat_ms.lock().await.push(ms(ack_t - send_t));
                            return;
                        }
                    }
                    send_fail.fetch_add(1, Ordering::Relaxed);
                }
                Ok(r) => {
                    eprintln!("loadgen: send {i} -> HTTP {}", r.status());
                    send_fail.fetch_add(1, Ordering::Relaxed);
                }
                Err(e) => {
                    eprintln!("loadgen: send {i} error: {e}");
                    send_fail.fetch_add(1, Ordering::Relaxed);
                }
            }
        });
    }
    while set.join_next().await.is_some() {}
    let send_done = Instant::now();
    let sent_count = sent.lock().await.len();
    eprintln!(
        "loadgen: acked {sent_count}/{n} sends in {:.2}s",
        (send_done - t0).as_secs_f64()
    );

    // ---- drain: wait until every sent message has been delivered (or timeout) ----
    let deadline = Instant::now() + Duration::from_secs(timeout_secs);
    loop {
        let u = state.received.lock().await.len();
        if u >= sent_count || Instant::now() >= deadline {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // ---- metrics ----
    let sent_map = sent.lock().await;
    let recv_map = state.received.lock().await;
    let unique = recv_map.len();
    let total_recv = state.total.load(Ordering::Relaxed);
    let dups = total_recv.saturating_sub(unique as u64);
    let drops = sent_count.saturating_sub(unique);

    let mut e2e_ms: Vec<f64> = Vec::with_capacity(unique);
    let mut last_recv = t0;
    for (id, rt) in recv_map.iter() {
        if let Some(st) = sent_map.get(id) {
            e2e_ms.push(ms(*rt - *st));
        }
        if *rt > last_recv {
            last_recv = *rt;
        }
    }

    let mut acks = ack_lat_ms.lock().await.clone();
    sort(&mut acks);
    sort(&mut e2e_ms);

    let send_secs = (send_done - t0).as_secs_f64().max(1e-6);
    let deliver_secs = (last_recv - t0).as_secs_f64().max(1e-6);
    let ingest_tput = sent_count as f64 / send_secs;
    let deliver_tput = unique as f64 / deliver_secs;

    let out = serde_json::json!({
        "requested": n,
        "concurrency": concurrency,
        "sent_acked": sent_count,
        "send_failures": send_fail.load(Ordering::Relaxed),
        "delivered_unique": unique,
        "delivered_total": total_recv,
        "dropped": drops,
        "duplicates": dups,
        "ingest_throughput_msg_s": round(ingest_tput, 0),
        "delivery_throughput_msg_s": round(deliver_tput, 0),
        "send_wall_s": round(send_secs, 3),
        "delivery_wall_s": round(deliver_secs, 3),
        "ack_latency_ms": { "p50": round(pct(&acks, 50.0), 2), "p95": round(pct(&acks, 95.0), 2), "p99": round(pct(&acks, 99.0), 2), "max": round(acks.last().copied().unwrap_or(0.0), 2) },
        "e2e_latency_ms": { "p50": round(pct(&e2e_ms, 50.0), 2), "p95": round(pct(&e2e_ms, 95.0), 2), "p99": round(pct(&e2e_ms, 99.0), 2), "max": round(e2e_ms.last().copied().unwrap_or(0.0), 2) },
    });
    println!("{out}");
    eprintln!("loadgen: done — delivered {unique}/{sent_count}, dropped {drops}, dups {dups}");
}

async fn receive(State(state): State<RecvState>, headers: HeaderMap) -> &'static str {
    state.total.fetch_add(1, Ordering::Relaxed);
    if let Some(id) = headers.get("webhook-id").and_then(|v| v.to_str().ok()) {
        let now = Instant::now();
        state
            .received
            .lock()
            .await
            .entry(id.to_string())
            .or_insert(now);
    }
    "ok"
}

fn env(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

fn ms(d: Duration) -> f64 {
    d.as_secs_f64() * 1000.0
}

fn sort(v: &mut [f64]) {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
}

/// Nearest-rank percentile over an already-sorted slice.
fn pct(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let rank = ((p / 100.0) * (sorted.len() as f64 - 1.0)).round() as usize;
    sorted[rank.min(sorted.len() - 1)]
}

fn round(x: f64, places: i32) -> f64 {
    let f = 10f64.powi(places);
    (x * f).round() / f
}
