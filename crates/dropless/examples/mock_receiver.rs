//! A tiny webhook receiver for the kill-9 proof.
//!
//! On every `POST`, it appends the `webhook-id` header to `MOCK_FILE` (one id
//! per line, flushed immediately) and returns `200`. An optional `MOCK_DELAY_MS`
//! widens the in-flight window so a `kill -9` lands while deliveries are mid-air.
//!
//! Env:
//! - `MOCK_ADDR`     (default `127.0.0.1:9099`)
//! - `MOCK_FILE`     (default `received.log`)
//! - `MOCK_DELAY_MS` (default `0`)

use std::io::Write;
use std::sync::Arc;
use std::time::Duration;

use axum::{extract::State, http::HeaderMap, routing::post, Router};
use tokio::sync::Mutex;

#[derive(Clone)]
struct Mock {
    file: Arc<Mutex<std::fs::File>>,
    delay: Duration,
}

#[tokio::main]
async fn main() {
    let addr = std::env::var("MOCK_ADDR").unwrap_or_else(|_| "127.0.0.1:9099".into());
    let path = std::env::var("MOCK_FILE").unwrap_or_else(|_| "received.log".into());
    let delay_ms: u64 = std::env::var("MOCK_DELAY_MS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);

    let file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .expect("open MOCK_FILE");

    let state = Mock {
        file: Arc::new(Mutex::new(file)),
        delay: Duration::from_millis(delay_ms),
    };

    let app = Router::new().route("/", post(receive)).with_state(state);
    let listener = tokio::net::TcpListener::bind(&addr).await.expect("bind");
    eprintln!("mock_receiver listening on {addr}, logging to {path}");
    axum::serve(listener, app).await.expect("serve");
}

async fn receive(State(mock): State<Mock>, headers: HeaderMap) -> &'static str {
    if !mock.delay.is_zero() {
        tokio::time::sleep(mock.delay).await;
    }
    let id = headers
        .get("webhook-id")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("MISSING");
    let mut f = mock.file.lock().await;
    let _ = writeln!(f, "{id}");
    let _ = f.flush();
    "ok"
}
