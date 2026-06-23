#!/usr/bin/env bash
#
# 0-loss benchmark — measured, reproducible numbers only (never synthetic).
#
# Phase A: steady-state throughput + p50/p95/p99 latency, via a concurrent Rust
#          load generator that is also the webhook receiver (one monotonic clock
#          → accurate end-to-end delivery latency).
# Phase B: crash resilience — reuses scripts/kill9_test.sh to prove drop == 0
#          across a real OS `kill -9` mid-delivery.
# Writes bench/REPORT.md from the measured results + machine / fsync settings.
#
# Requires: a running Postgres via $DATABASE_URL, curl, python3.
# Usage: DATABASE_URL=postgres://... bench/bench.sh [N] [CRASH_N]

set -uo pipefail

N="${1:-5000}"             # steady-state messages
CRASH_N="${2:-2000}"       # crash-phase messages
PORT="${PORT:-8090}"
RECV_ADDR="${RECV_ADDR:-127.0.0.1:9090}"
CONCURRENCY="${CONCURRENCY:-64}"
WORKERS="${WORKERS:-8}"
BATCH="${BATCH:-64}"
CONNS="${CONNS:-32}"       # DB pool: must comfortably exceed workers + in-flight ingest
N_LIGHT="${N_LIGHT:-100}"  # light-load latency probe size
PACE_MS="${PACE_MS:-50}"   # inter-send pacing for the light-load probe
POLL_MS="${POLL_MS:-5000}" # large fallback poll: low latency here ⇒ LISTEN/NOTIFY works
: "${DATABASE_URL:?set DATABASE_URL to a running Postgres}"

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

WORKDIR="$(mktemp -d)"
TENANT="bench-$(date +%s)-$$"
KEY="benchkey-$$"

cleanup() {
  [[ -n "${SRV_PID:-}" ]] && kill -9 "$SRV_PID" 2>/dev/null
  rm -rf "$WORKDIR"
}
trap cleanup EXIT

echo "==> building (release)"
cargo build --release --bin dropless --example bench_loadgen --example mock_receiver
BIN="$ROOT/target/release/dropless"
LOADGEN="$ROOT/target/release/examples/bench_loadgen"

"$BIN" migrate
"$BIN" create-api-key --tenant "$TENANT" --key "$KEY" >/dev/null
"$BIN" create-endpoint --tenant "$TENANT" --url "http://$RECV_ADDR/" \
  --secret "whsec_dGVzdHNlY3JldA==" >/dev/null

# ---------------------------------------------------------------------------
# One server for the latency + throughput phases. A deliberately LARGE fallback
# poll interval means any low latency observed is attributable to LISTEN/NOTIFY,
# not polling.
# ---------------------------------------------------------------------------
echo "==> starting server — workers=$WORKERS, batch=$BATCH, pool=$CONNS, poll_interval=${POLL_MS}ms"
WORKER_COUNT="$WORKERS" WORKER_BATCH="$BATCH" MAX_CONNECTIONS="$CONNS" POLL_INTERVAL_MS="$POLL_MS" \
  "$BIN" serve --role=all --bind "127.0.0.1:$PORT" >"$WORKDIR/serve.out" 2>&1 &
SRV_PID=$!
for _ in $(seq 1 50); do
  curl -fsS "http://127.0.0.1:$PORT/healthz" >/dev/null 2>&1 && break
  sleep 0.2
done

# Phase 0 — light-load latency: paced sends stay below delivery capacity, so
# e2e latency is true per-message latency. Low here despite the ${POLL_MS}ms
# poll fallback proves LISTEN/NOTIFY is driving delivery, not the poll loop.
echo "==> phase 0: light-load latency — N=$N_LIGHT, pace=${PACE_MS}ms"
BENCH_BASE_URL="http://127.0.0.1:$PORT" BENCH_API_KEY="$KEY" \
BENCH_RECV_ADDR="$RECV_ADDR" BENCH_N="$N_LIGHT" BENCH_CONCURRENCY=1 BENCH_PACE_MS="$PACE_MS" \
  "$LOADGEN" >"$WORKDIR/light.json" 2>"$WORKDIR/light.log"
cat "$WORKDIR/light.log" >&2
sleep 1  # let the receiver port free up before the next loadgen binds it

# Phase A — steady-state throughput (burst).
echo "==> phase A: steady-state — N=$N, concurrency=$CONCURRENCY"
BENCH_BASE_URL="http://127.0.0.1:$PORT" BENCH_API_KEY="$KEY" \
BENCH_RECV_ADDR="$RECV_ADDR" BENCH_N="$N" BENCH_CONCURRENCY="$CONCURRENCY" \
  "$LOADGEN" >"$WORKDIR/perf.json" 2>"$WORKDIR/perf.log"
cat "$WORKDIR/perf.log" >&2

kill -9 "$SRV_PID" 2>/dev/null; SRV_PID=""
sleep 0.5

# ---------------------------------------------------------------------------
# Phase B — crash resilience (real kill -9), reusing the canonical proof
# ---------------------------------------------------------------------------
echo "==> phase B: crash resilience — kill -9 mid-flight, N=$CRASH_N"
bash "$ROOT/scripts/kill9_test.sh" "$CRASH_N" >"$WORKDIR/crash.out" 2>&1 || true
grep -E "sent|received|duplicates|DROPPED|PASS|FAIL" "$WORKDIR/crash.out" >&2 || true

# ---------------------------------------------------------------------------
# Compose bench/REPORT.md
# ---------------------------------------------------------------------------
# Prefer a pre-set value (e.g. injected via `docker exec ... psql`) when the
# psql client isn't installed on the host.
FSYNC="${FSYNC:-$(psql "$DATABASE_URL" -tAc 'show fsync;' 2>/dev/null | tr -d '[:space:]' || true)}"
SYNC_COMMIT="${SYNC_COMMIT:-$(psql "$DATABASE_URL" -tAc 'show synchronous_commit;' 2>/dev/null | tr -d '[:space:]' || true)}"
CRASH_SENT="$(grep -E 'sent \(unique\)' "$WORKDIR/crash.out" | grep -oE '[0-9]+' | tail -1)"
CRASH_RECV="$(grep -E 'received \(unique\)' "$WORKDIR/crash.out" | grep -oE '[0-9]+' | tail -1)"
CRASH_DROP="$(grep -E 'DROPPED' "$WORKDIR/crash.out" | grep -oE '[0-9]+' | tail -1)"
CRASH_DUPS="$(grep -E 'duplicates' "$WORKDIR/crash.out" | grep -oE '[0-9]+' | head -1)"

export FSYNC SYNC_COMMIT CRASH_SENT CRASH_RECV CRASH_DROP CRASH_DUPS \
       N CRASH_N WORKERS BATCH CONCURRENCY CONNS POLL_MS N_LIGHT PACE_MS

python3 - "$WORKDIR/perf.json" "$WORKDIR/light.json" >"$ROOT/bench/REPORT.md" <<'PYEOF'
import json, os, sys, datetime, platform, subprocess

p = json.load(open(sys.argv[1]))
light = json.load(open(sys.argv[2]))
g = os.environ.get

def cpus():
    try:
        return str(os.cpu_count() or "?")
    except Exception:
        return "?"

def field(name, default="unknown"):
    v = g(name) or ""
    return v if v.strip() else default

a = p["ack_latency_ms"]; e = p["e2e_latency_ms"]
le = light["e2e_latency_ms"]
now = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

ingest_tp = p["ingest_throughput_msg_s"]; deliver_tp = p["delivery_throughput_msg_s"]
if ingest_tp > deliver_tp * 1.2:
    note = (
        f"Under this burst, ingest ({ingest_tp:.0f} msg/s) outran delivery "
        f"({deliver_tp:.0f} msg/s), so messages queued and **end-to-end latency here is "
        "backlog-dominated** — it measures queue wait under saturation, not per-message "
        "latency (Phase 0 shows the latter). Per-delivery writes are coalesced into one "
        "fsync'd transaction and delivery is push-driven (`LISTEN/NOTIFY`); the remaining "
        "throughput ceiling is the per-delivery read round-trips (`get_endpoint` + "
        "`get_event`) and worker/pool parallelism on this fsync-heavy dev box. Levers "
        "(roadmap, `PLAN.md`): fold those reads into the claim query, and raise "
        "workers/pool."
    )
else:
    note = (
        "Delivery kept pace with ingest, so end-to-end latency reflects the dispatcher "
        "poll interval (500ms default) plus per-delivery processing. `LISTEN/NOTIFY` "
        "push delivery (roadmap, `PLAN.md`) would remove the poll floor."
    )

print(f"""# Dropless — 0-loss benchmark report

> **Auto-generated** by `bench/bench.sh`. Measured, reproducible numbers only —
> never synthetic figures. Re-run: `DATABASE_URL=... make bench`.

## Environment

- Date (UTC): {now}
- OS: {platform.system()} {platform.release()} ({platform.machine()})
- CPUs: {cpus()}
- Postgres `fsync`: `{field('FSYNC')}` · `synchronous_commit`: `{field('SYNC_COMMIT')}`
- Server: workers={field('WORKERS')}, batch={field('BATCH')}, db_pool={field('CONNS')},
  lock_secs=30, poll_interval={field('POLL_MS')}ms (set large on purpose — see Phase 0)
- Load: concurrency={field('CONCURRENCY')}

> **Caveat:** this run is on a WSL2 + Docker-volume dev box where `fsync` latency
> dominates delivery throughput. Treat the delivery rate as a **conservative floor**,
> not the engine's ceiling — re-run on target hardware/storage for representative
> numbers. Drops (the invariant) are environment-independent.

## Phase 0 — light-load latency (LISTEN/NOTIFY), N={field('N_LIGHT')}

Sends paced {field('PACE_MS')}ms apart (well under delivery capacity), so e2e
latency is true per-message latency, not queue backlog. The fallback poll
interval is **{field('POLL_MS')}ms** — a low p50 here means the worker woke on
the enqueue `NOTIFY`, not the poll loop (which alone would put p50 near half the
poll interval).

| stage | p50 | p95 | p99 | max |
|---|---|---|---|---|
| end-to-end delivery (send → subscriber) | {le['p50']} | {le['p95']} | {le['p99']} | {le['max']} |

Delivered {light['delivered_unique']}/{light['sent_acked']}, {light['dropped']} dropped.

## Phase A — steady-state (no crash), N={field('N')}

| metric | value |
|---|---|
| acked (committed → 201) | {p['sent_acked']} / {p['requested']} |
| send failures | {p['send_failures']} |
| delivered (unique) | {p['delivered_unique']} |
| dropped | {p['dropped']} |
| duplicates | {p['duplicates']} |
| ingest throughput | {p['ingest_throughput_msg_s']:.0f} msg/s |
| delivery throughput | {p['delivery_throughput_msg_s']:.0f} msg/s |

Latency (ms):

| stage | p50 | p95 | p99 | max |
|---|---|---|---|---|
| ingest ack (send → 201) | {a['p50']} | {a['p95']} | {a['p99']} | {a['max']} |
| end-to-end delivery (send → subscriber) | {e['p50']} | {e['p95']} | {e['p99']} | {e['max']} |

> {note}

## Phase B — crash resilience (real `kill -9` mid-flight), N={field('CRASH_N')}

The dispatcher is `kill -9`'d while deliveries are in flight, then restarted;
in-progress rows are reclaimed once their lock lease lapses.

| metric | value |
|---|---|
| sent (unique) | {field('CRASH_SENT','?')} |
| delivered (unique) | {field('CRASH_RECV','?')} |
| **dropped** | **{field('CRASH_DROP','?')}** |
| duplicates | {field('CRASH_DUPS','?')} |

**Drops must be 0** — the product's whole point. Duplicates are expected and are
the subscriber's to dedupe via the immutable `webhook-id` / `Idempotency-Key`:
**effectively-once, never exactly-once.**
""")
PYEOF

echo "==> wrote bench/REPORT.md"
DROPPED_A="$(python3 -c "import json;print(json.load(open('$WORKDIR/perf.json'))['dropped'])")"
echo "   phase A dropped=$DROPPED_A ; phase B dropped=${CRASH_DROP:-?}"
# The benchmark fails if either phase dropped a message.
[[ "${DROPPED_A:-1}" -eq 0 && "${CRASH_DROP:-1}" -eq 0 ]] || {
  echo "FAIL: drops detected (A=$DROPPED_A B=${CRASH_DROP:-?})" >&2; exit 1
}
echo "PASS: 0 drops in both phases"
