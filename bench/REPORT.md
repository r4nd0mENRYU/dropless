# Dropless — 0-loss benchmark report

> **Auto-generated** by `bench/bench.sh`. Measured, reproducible numbers only —
> never synthetic figures. Re-run: `DATABASE_URL=... make bench`.

## Environment

- Date (UTC): 2026-06-18T02:12:18Z
- OS: Linux 6.6.114.1-microsoft-standard-WSL2 (x86_64)
- CPUs: 8
- Postgres `fsync`: `on` · `synchronous_commit`: `on`
- Server: workers=8, batch=64, db_pool=32,
  lock_secs=30, poll_interval=5000ms (set large on purpose — see Phase 0)
- Load: concurrency=64

> **Caveat:** this run is on a WSL2 + Docker-volume dev box where `fsync` latency
> dominates delivery throughput. Treat the delivery rate as a **conservative floor**,
> not the engine's ceiling — re-run on target hardware/storage for representative
> numbers. Drops (the invariant) are environment-independent.

## Phase 0 — light-load latency (LISTEN/NOTIFY), N=100

Sends paced 50ms apart (well under delivery capacity), so e2e
latency is true per-message latency, not queue backlog. The fallback poll
interval is **5000ms** — a low p50 here means the worker woke on
the enqueue `NOTIFY`, not the poll loop (which alone would put p50 near half the
poll interval).

| stage | p50 | p95 | p99 | max |
|---|---|---|---|---|
| end-to-end delivery (send → subscriber) | 14.33 | 29.31 | 42.15 | 55.71 |

Delivered 100/100, 0 dropped.

## Phase A — steady-state (no crash), N=5000

| metric | value |
|---|---|
| acked (committed → 201) | 5000 / 5000 |
| send failures | 0 |
| delivered (unique) | 5000 |
| dropped | 0 |
| duplicates | 0 |
| ingest throughput | 756 msg/s |
| delivery throughput | 202 msg/s |

Latency (ms):

| stage | p50 | p95 | p99 | max |
|---|---|---|---|---|
| ingest ack (send → 201) | 79.39 | 122.48 | 147.09 | 199.99 |
| end-to-end delivery (send → subscriber) | 11720.26 | 17421.78 | 18148.02 | 18330.8 |

> Under this burst, ingest (756 msg/s) outran delivery (202 msg/s), so messages queued and **end-to-end latency here is backlog-dominated** — it measures queue wait under saturation, not per-message latency (Phase 0 shows the latter). Per-delivery writes are coalesced into one fsync'd transaction and delivery is push-driven (`LISTEN/NOTIFY`); the remaining throughput ceiling is the per-delivery read round-trips (`get_endpoint` + `get_event`) and worker/pool parallelism on this fsync-heavy dev box. Levers (roadmap, `PLAN.md`): fold those reads into the claim query, and raise workers/pool.

## Phase B — crash resilience (real `kill -9` mid-flight), N=2000

The dispatcher is `kill -9`'d while deliveries are in flight, then restarted;
in-progress rows are reclaimed once their lock lease lapses.

| metric | value |
|---|---|
| sent (unique) | 2000 |
| delivered (unique) | 2000 |
| **dropped** | **0** |
| duplicates | 0 |

**Drops must be 0** — the product's whole point. Duplicates are expected and are
the subscriber's to dedupe via the immutable `webhook-id` / `Idempotency-Key`:
**effectively-once, never exactly-once.**

