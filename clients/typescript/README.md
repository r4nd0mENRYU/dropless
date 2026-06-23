# @dropless/sdk

TypeScript SDK for [Dropless](../../README.md) — zero-loss webhook delivery.
Zero dependencies; uses the global `fetch` and Web Crypto, so it runs in Node
18+, Deno, Bun, Cloudflare Workers, and browsers.

```bash
npm install @dropless/sdk
```

## Emit & inspect messages

```ts
import { Dropless } from "@dropless/sdk";

const dropless = new Dropless({
  baseUrl: "https://hooks.acme.internal",
  apiKey: process.env.DROPLESS_API_KEY!,
});

// Resolves only after the event + its deliveries are committed (the invariant).
const { id } = await dropless.messages.create({
  event_type: "invoice.paid",
  payload: { invoice: 1042, amount: 21000 },
});

const { event, deliveries } = await dropless.messages.get(id);
for (const d of deliveries) {
  if (d.status === "failed" || d.status === "dead") {
    await dropless.deliveries.replay(d.id);
  }
}

// Manage endpoints (the secret is returned only once, on create).
const ep = await dropless.endpoints.create({ url: "https://api.example.com/hook" });
console.log("store this:", ep.secret);
await dropless.endpoints.update(ep.id, { disabled: true });
```

## Verify incoming webhooks

Dropless signs every delivery Svix-style. Verify it against the endpoint's
signing secret — pass the **raw** request body, never re-serialized JSON:

```ts
import { Webhook, WebhookVerificationError } from "@dropless/sdk";

const wh = new Webhook(process.env.DROPLESS_SIGNING_SECRET!); // the whsec_… secret

// e.g. inside an Express/Hono/Next handler:
try {
  await wh.verify(rawBody, request.headers); // throws on any failure
  // ...trusted: handle the event...
} catch (e) {
  if (e instanceof WebhookVerificationError) return new Response("bad signature", { status: 400 });
  throw e;
}
```

`verify` checks the `v1` HMAC-SHA256 signature and a ±300s timestamp tolerance
(configurable via `{ toleranceSeconds }`). The verifier is cross-checked against
the Rust engine with a shared known-answer vector (see `test/`).

## Build & test

```bash
npm install
npm test     # tsc + node --test (includes the cross-language signature vector)
```
