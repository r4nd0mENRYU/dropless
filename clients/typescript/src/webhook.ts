// Svix-compatible webhook signature verification.
//
// Matches the Dropless engine byte-for-byte:
//   signed content = `${id}.${timestamp}.${body}`
//   header         = `v1,${base64(HMAC_SHA256(key, content))}`
//   key            = a `whsec_`-prefixed secret is base64-decoded (Svix), else
//                    the raw UTF-8 bytes are used.
//
// Uses Web Crypto (`globalThis.crypto.subtle`), so it runs unchanged in Node 18+,
// Deno, Bun, Cloudflare Workers, and browsers — no dependencies.

const enc = new TextEncoder();

function b64encode(bytes: Uint8Array): string {
  let s = "";
  for (const b of bytes) s += String.fromCharCode(b);
  return btoa(s);
}

function b64decode(s: string): Uint8Array {
  const bin = atob(s);
  const out = new Uint8Array(bin.length);
  for (let i = 0; i < bin.length; i++) out[i] = bin.charCodeAt(i);
  return out;
}

function keyBytes(secret: string): Uint8Array {
  if (secret.startsWith("whsec_")) {
    const rest = secret.slice("whsec_".length);
    try {
      return b64decode(rest);
    } catch {
      return enc.encode(rest);
    }
  }
  return enc.encode(secret);
}

/** Constant-time string comparison (avoids leaking match position via timing). */
function timingSafeEqual(a: string, b: string): boolean {
  if (a.length !== b.length) return false;
  let diff = 0;
  for (let i = 0; i < a.length; i++) diff |= a.charCodeAt(i) ^ b.charCodeAt(i);
  return diff === 0;
}

/** Thrown when a webhook signature is missing, malformed, expired, or invalid. */
export class WebhookVerificationError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "WebhookVerificationError";
  }
}

/** Headers a Dropless webhook carries (case-insensitive lookups supported). */
export interface WebhookHeaders {
  [key: string]: string | string[] | undefined;
}

function header(headers: WebhookHeaders, name: string): string | undefined {
  const direct = headers[name] ?? headers[name.toLowerCase()];
  const v = Array.isArray(direct) ? direct[0] : direct;
  if (v !== undefined) return v;
  // Fall back to a case-insensitive scan.
  const want = name.toLowerCase();
  for (const k of Object.keys(headers)) {
    if (k.toLowerCase() === want) {
      const hv = headers[k];
      return Array.isArray(hv) ? hv[0] : hv;
    }
  }
  return undefined;
}

export interface VerifyOptions {
  /** Reject if `webhook-timestamp` is older/newer than this many seconds. Default 300; pass 0 to disable. */
  toleranceSeconds?: number;
  /** Override "now" (unix seconds), for testing. */
  nowSeconds?: number;
}

/**
 * Verifier for a single endpoint's signing secret.
 *
 * ```ts
 * const wh = new Webhook(process.env.DROPLESS_SECRET!);
 * await wh.verify(rawBody, req.headers); // throws WebhookVerificationError on failure
 * ```
 */
export class Webhook {
  #key: Uint8Array;

  constructor(secret: string) {
    if (!secret) throw new WebhookVerificationError("a signing secret is required");
    this.#key = keyBytes(secret);
  }

  /** Compute the bare base64 v1 signature (no `v1,` prefix) for a payload. */
  async sign(id: string, timestamp: number, body: string): Promise<string> {
    // Copy into ArrayBuffer-backed views so the types satisfy `BufferSource`
    // regardless of TextEncoder's `ArrayBufferLike` element type.
    const key = await crypto.subtle.importKey(
      "raw",
      new Uint8Array(this.#key),
      { name: "HMAC", hash: "SHA-256" },
      false,
      ["sign"],
    );
    const data = new Uint8Array(enc.encode(`${id}.${timestamp}.${body}`));
    const mac = await crypto.subtle.sign("HMAC", key, data);
    return b64encode(new Uint8Array(mac));
  }

  /** Build the full `webhook-signature` header value (`v1,<sig>`). */
  async signatureHeader(id: string, timestamp: number, body: string): Promise<string> {
    return `v1,${await this.sign(id, timestamp, body)}`;
  }

  /**
   * Verify an incoming webhook. Resolves on success; throws
   * {@link WebhookVerificationError} on any failure. `body` MUST be the exact
   * raw request body string (not re-serialized JSON).
   */
  async verify(
    body: string,
    headers: WebhookHeaders,
    opts: VerifyOptions = {},
  ): Promise<void> {
    const id = header(headers, "webhook-id");
    const ts = header(headers, "webhook-timestamp");
    const sigHeader = header(headers, "webhook-signature");
    if (!id || !ts || !sigHeader) {
      throw new WebhookVerificationError("missing webhook-id/timestamp/signature header");
    }

    const tolerance = opts.toleranceSeconds ?? 300;
    if (tolerance > 0) {
      const now = opts.nowSeconds ?? Math.floor(Date.now() / 1000);
      const tsNum = Number(ts);
      if (!Number.isFinite(tsNum)) {
        throw new WebhookVerificationError("invalid webhook-timestamp");
      }
      if (Math.abs(now - tsNum) > tolerance) {
        throw new WebhookVerificationError("webhook-timestamp outside tolerance");
      }
    }

    const expected = await this.sign(id, Number(ts), body);
    // The header is one or more space-separated `vN,sig` tokens.
    for (const token of sigHeader.split(/\s+/)) {
      const comma = token.indexOf(",");
      if (comma < 0) continue;
      if (token.slice(0, comma) !== "v1") continue;
      if (timingSafeEqual(token.slice(comma + 1), expected)) return;
    }
    throw new WebhookVerificationError("no matching v1 signature");
  }
}
