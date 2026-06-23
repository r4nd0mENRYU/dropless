#!/usr/bin/env bash
#
# The kill -9 proof: SIGKILL the running node mid-delivery, restart it, and
# assert every message is still delivered — drop 0 (duplicates allowed).
#
# This is the product's central claim, exercised against a real process and a
# real SIGKILL (not a graceful shutdown). It is wired into CI so the property
# can never silently regress.
#
# Requires: a running Postgres reachable via $DATABASE_URL, curl, python3.
#
# Usage: DATABASE_URL=postgres://... scripts/kill9_test.sh [N]

set -uo pipefail

N="${1:-200}"
PORT="${PORT:-8088}"
MPORT="${MPORT:-9088}"
: "${DATABASE_URL:?set DATABASE_URL to a running Postgres}"

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

WORKDIR="$(mktemp -d)"
RECV="$WORKDIR/received.log"
SENT="$WORKDIR/sent.log"
: > "$RECV"
: > "$SENT"

TENANT="kill9-$(date +%s)-$$"
KEY="kill9key-$$"

cleanup() {
  [[ -n "${SRV_PID:-}" ]] && kill -9 "$SRV_PID" 2>/dev/null
  [[ -n "${MOCK_PID:-}" ]] && kill -9 "$MOCK_PID" 2>/dev/null
  rm -rf "$WORKDIR"
}
trap cleanup EXIT

echo "==> building"
cargo build --release --bin dropless --example mock_receiver

BIN="$ROOT/target/release/dropless"
MOCK="$ROOT/target/release/examples/mock_receiver"

echo "==> migrate + seed (tenant=$TENANT)"
"$BIN" migrate
"$BIN" create-api-key --tenant "$TENANT" --key "$KEY" >/dev/null
"$BIN" create-endpoint --tenant "$TENANT" --url "http://127.0.0.1:$MPORT/" \
  --secret "whsec_dGVzdHNlY3JldA==" >/dev/null

echo "==> start mock receiver (:$MPORT)"
MOCK_FILE="$RECV" MOCK_ADDR="127.0.0.1:$MPORT" MOCK_DELAY_MS=40 "$MOCK" >/dev/null 2>&1 &
MOCK_PID=$!
sleep 0.5

start_node() {
  MAX_ATTEMPTS=50 LOCK_SECS=2 "$BIN" serve --role=all --bind "127.0.0.1:$PORT" >"$WORKDIR/serve.out" 2>&1 &
  SRV_PID=$!
  for _ in $(seq 1 50); do
    curl -fsS "http://127.0.0.1:$PORT/healthz" >/dev/null 2>&1 && return 0
    sleep 0.2
  done
  echo "node failed to become healthy" >&2
  cat "$WORKDIR/serve.out" >&2
  exit 1
}

echo "==> start node A"
start_node

echo "==> sending $N messages"
for i in $(seq 1 "$N"); do
  resp="$(curl -s -XPOST "http://127.0.0.1:$PORT/v1/messages" \
    -H "Authorization: Bearer $KEY" \
    -H "Content-Type: application/json" \
    -d "{\"event_type\":\"load.test\",\"payload\":{\"n\":$i}}")"
  echo "$resp" | python3 -c "import sys,json; print(json.load(sys.stdin)['delivery_ids'][0])" >> "$SENT"
done
sent_count="$(sort -u "$SENT" | grep -c .)"
echo "    sent $sent_count unique deliveries"

# Let deliveries get in flight, then SIGKILL the node.
sleep 0.7
echo "==> kill -9 node A (pid $SRV_PID)"
kill -9 "$SRV_PID"
SRV_PID=""
# Wait out the 2s lock lease (LOCK_SECS=2 above) so node B can reclaim the
# rows node A had in flight, then restart.
sleep 3

echo "==> restart node B"
start_node

echo "==> waiting for drain"
deadline=$(( $(date +%s) + 90 ))
while :; do
  uniq="$(sort -u "$RECV" | grep -c .)"
  [[ "$uniq" -ge "$N" ]] && break
  if [[ "$(date +%s)" -ge "$deadline" ]]; then
    echo "TIMEOUT: only $uniq/$N unique deliveries arrived" >&2
    exit 1
  fi
  sleep 0.5
done

# Compare the set sent vs the set received.
missing="$(comm -23 <(sort -u "$SENT") <(sort -u "$RECV") | grep -c . || true)"
total="$(grep -c . "$RECV")"
uniq="$(sort -u "$RECV" | grep -c .)"
dups=$(( total - uniq ))

echo "----------------------------------------"
echo " sent (unique):     $sent_count"
echo " received (unique): $uniq"
echo " received (total):  $total"
echo " duplicates:        $dups   (expected: effectively-once)"
echo " DROPPED:           $missing"
echo "----------------------------------------"

if [[ "$missing" -ne 0 ]]; then
  echo "FAIL: $missing message(s) dropped" >&2
  exit 1
fi
echo "PASS: 0 dropped across kill -9 (duplicates are fine — subscribers dedupe on webhook-id)"
