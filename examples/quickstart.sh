#!/usr/bin/env bash
# End-to-end Dropless walkthrough: create a key + endpoint, publish, observe.
# Prereqs: Dropless on :8080 (docker compose up) and examples/receiver.py on :9000.
set -euo pipefail

API=${API:-http://localhost:8080}
RECEIVER=${RECEIVER:-http://host.docker.internal:9000/hook}   # receiver from the container's view
KEY=dev
TENANT=demo

echo "→ creating API key + endpoint (tenant=$TENANT)"
docker compose exec -T app dropless create-api-key  --tenant "$TENANT" --key "$KEY" >/dev/null 2>&1 || true
# The receiver verifies with secret whsec_dGVzdA== (see receiver.py).
docker compose exec -T app dropless create-endpoint --tenant "$TENANT" \
  --url "$RECEIVER" --secret "whsec_dGVzdA==" >/dev/null 2>&1 || true

echo "→ publishing an event"
RESP=$(curl -s -XPOST "$API/v1/messages" \
  -H "Authorization: Bearer $KEY" -H 'Content-Type: application/json' \
  -d '{"event_type":"invoice.paid","payload":{"amount":42,"currency":"usd"}}')
echo "  response: $RESP"
MSG_ID=$(printf '%s' "$RESP" | sed -n 's/.*"id":"\([^"]*\)".*/\1/p')

echo "→ the receiver terminal should now show a VERIFIED delivery."
sleep 2
echo "→ delivery status for message $MSG_ID:"
curl -s "$API/v1/messages/$MSG_ID" -H "Authorization: Bearer $KEY" \
  | python3 -c 'import sys,json; d=json.load(sys.stdin); [print("   ", x["status"], "→", x["endpoint_id"][:8]) for x in d.get("deliveries",[])]' 2>/dev/null \
  || echo "   (install python3 to pretty-print, or GET /v1/messages/$MSG_ID)"

echo
echo "Open http://localhost:8080 and paste the key '$KEY' to browse / replay in the dashboard."
