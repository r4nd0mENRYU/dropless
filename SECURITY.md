# Security Policy

## Reporting a vulnerability

Please **do not** open a public issue for security vulnerabilities.

Email **ssin6505@gmail.com** with details and, if possible, a minimal
reproduction. We aim to acknowledge within a few days and to fix confirmed
issues promptly. Responsible disclosure is appreciated and credited (with your
permission).

## Scope & notes

- **Inbound verification.** Provider signatures (Stripe / GitHub / generic HMAC)
  are checked with constant-time comparison. Replay protection deduplicates on
  **signed** material (never an unsigned, attacker-mutable header), so replaying
  the same signed bytes always collides. Unknown sources and bad signatures
  return an identical `401` (no slug/provider oracle).
- **Secrets.** API keys are stored only as SHA-256 hashes. Outbound webhook
  signing secrets are stored to sign requests and are returned to the caller
  exactly once on creation.
- **Edge responsibilities.** The app caps inbound body size and rate-limits
  `/v1/messages` per tenant, but volumetric / slow-loris / global concurrency
  defense is expected from a fronting reverse proxy or WAF, especially for the
  public `/ingest/{slug}` endpoint.
- **Self-hosting.** Gate `/metrics` with `METRICS_TOKEN` (its counts are
  cross-tenant), set `ADMIN_TOKEN` only if you want the cross-tenant `/admin`
  console, and terminate TLS at your proxy.
