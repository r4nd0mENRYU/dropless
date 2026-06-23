// Dropless TypeScript SDK — a thin, typed client over the REST API, plus a
// Svix-compatible webhook verifier. Zero dependencies; uses the global `fetch`
// (Node 18+, Deno, Bun, browsers).

import type {
  CreateEndpoint,
  CreateMessage,
  CreateMessageResponse,
  CreatedEndpoint,
  Delivery,
  DeliveryDetail,
  Endpoint,
  Event,
  ListParams,
  MessageDetail,
  UpdateEndpoint,
} from "./types.js";

export * from "./types.js";
export { Webhook, WebhookVerificationError } from "./webhook.js";
export type { WebhookHeaders, VerifyOptions } from "./webhook.js";

export interface DroplessOptions {
  /** Base URL of the Dropless server, e.g. `https://hooks.acme.internal`. */
  baseUrl: string;
  /** API key (from `dropless create-api-key`). Sent as `Authorization: Bearer`. */
  apiKey: string;
  /** Override the fetch implementation (e.g. for tests or custom agents). */
  fetch?: typeof fetch;
}

/** An API error with the HTTP status and the server's error message. */
export class DroplessError extends Error {
  constructor(
    public readonly status: number,
    message: string,
  ) {
    super(message);
    this.name = "DroplessError";
  }
}

export class Dropless {
  readonly #base: string;
  readonly #key: string;
  readonly #fetch: typeof fetch;

  constructor(opts: DroplessOptions) {
    if (!opts?.baseUrl) throw new Error("baseUrl is required");
    if (!opts?.apiKey) throw new Error("apiKey is required");
    this.#base = opts.baseUrl.replace(/\/+$/, "");
    this.#key = opts.apiKey;
    this.#fetch = opts.fetch ?? globalThis.fetch;
    if (!this.#fetch) throw new Error("no fetch implementation available; pass opts.fetch");
  }

  async #request<T>(method: string, path: string, body?: unknown): Promise<T> {
    const res = await this.#fetch(this.#base + path, {
      method,
      headers: {
        authorization: `Bearer ${this.#key}`,
        ...(body !== undefined ? { "content-type": "application/json" } : {}),
      },
      body: body !== undefined ? JSON.stringify(body) : undefined,
    });
    if (!res.ok) {
      let msg = `HTTP ${res.status}`;
      try {
        const j = (await res.json()) as { error?: string };
        if (j?.error) msg = j.error;
      } catch {
        /* non-JSON error body */
      }
      throw new DroplessError(res.status, msg);
    }
    if (res.status === 202 || res.status === 204) return undefined as T;
    return (await res.json()) as T;
  }

  #query(params?: ListParams): string {
    if (!params) return "";
    const q = new URLSearchParams();
    if (params.limit != null) q.set("limit", String(params.limit));
    if (params.offset != null) q.set("offset", String(params.offset));
    const s = q.toString();
    return s ? `?${s}` : "";
  }

  readonly messages = {
    /** Emit a message. Resolves only after it is committed (the core invariant). */
    create: (input: CreateMessage): Promise<CreateMessageResponse> =>
      this.#request("POST", "/v1/messages", input),
    /** Recent events for the tenant, newest first. */
    list: (params?: ListParams): Promise<Event[]> =>
      this.#request("GET", `/v1/messages${this.#query(params)}`),
    /** An event and its deliveries. */
    get: (id: string): Promise<MessageDetail> =>
      this.#request("GET", `/v1/messages/${encodeURIComponent(id)}`),
  };

  readonly deliveries = {
    /** A delivery and its full attempt timeline. */
    get: (id: string): Promise<DeliveryDetail> =>
      this.#request("GET", `/v1/deliveries/${encodeURIComponent(id)}`),
    /** Re-queue a delivery for immediate retry. */
    replay: (id: string): Promise<void> =>
      this.#request("POST", `/v1/deliveries/${encodeURIComponent(id)}/replay`, {}),
  };

  readonly endpoints = {
    /** Register an endpoint. The returned `secret` is shown ONLY here. */
    create: (input: CreateEndpoint): Promise<CreatedEndpoint> =>
      this.#request("POST", "/v1/endpoints", input),
    list: (): Promise<Endpoint[]> => this.#request("GET", "/v1/endpoints"),
    get: (id: string): Promise<Endpoint> =>
      this.#request("GET", `/v1/endpoints/${encodeURIComponent(id)}`),
    /** Update an endpoint's URL and/or disabled flag. */
    update: (id: string, patch: UpdateEndpoint): Promise<Endpoint> =>
      this.#request("PATCH", `/v1/endpoints/${encodeURIComponent(id)}`, patch),
  };
}

export type { Delivery };
