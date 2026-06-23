#!/usr/bin/env python3
"""A minimal webhook receiver that verifies Dropless's Svix-compatible signature.

Signed content is "{webhook-id}.{webhook-timestamp}.{body}", and the signature
header is "v1,<base64(HMAC_SHA256(secret, signed))>". A `whsec_`-prefixed secret
carries base64 key material (Svix convention).

    python3 examples/receiver.py 9000
"""
import base64
import hashlib
import hmac
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

# Must match the endpoint's signing secret. The quickstart uses this one.
SECRET = "whsec_dGVzdA=="  # base64("test")


def key_bytes(secret: str) -> bytes:
    if secret.startswith("whsec_"):
        try:
            return base64.b64decode(secret[6:])
        except Exception:
            return secret[6:].encode()
    return secret.encode()


def verify(headers, body: bytes) -> bool:
    msg_id = headers.get("webhook-id", "")
    ts = headers.get("webhook-timestamp", "")
    sig_header = headers.get("webhook-signature", "")
    signed = msg_id.encode() + b"." + ts.encode() + b"." + body
    expected = base64.b64encode(hmac.new(key_bytes(SECRET), signed, hashlib.sha256).digest()).decode()
    for token in sig_header.split():
        ver, _, sig = token.partition(",")
        if ver == "v1" and hmac.compare_digest(sig, expected):
            return True
    return False


class Handler(BaseHTTPRequestHandler):
    def do_POST(self):
        n = int(self.headers.get("content-length", 0))
        body = self.rfile.read(n) if n else b""
        ok = verify(self.headers, body)
        mark = "\033[32mVERIFIED\033[0m" if ok else "\033[31mBAD SIGNATURE\033[0m"
        print(f"[{mark}] event={self.headers.get('dropless-event-type','?')} "
              f"id={self.headers.get('webhook-id','?')[:8]} body={body.decode(errors='replace')}")
        # Reject unverified payloads; ack verified ones.
        self.send_response(200 if ok else 401)
        self.end_headers()
        self.wfile.write(b'{"ok":true}' if ok else b'{"error":"bad signature"}')

    def log_message(self, *args):
        pass


if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 9000
    print(f"receiver listening on http://localhost:{port}  (secret {SECRET})")
    ThreadingHTTPServer(("0.0.0.0", port), Handler).serve_forever()
