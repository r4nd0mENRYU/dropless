// Dropless API types. Mirrors openapi.yaml / the Rust domain model.

export type DeliveryStatus =
  | "pending"
  | "in_progress"
  | "succeeded"
  | "failed"
  | "dead";

export type CircuitState = "closed" | "open" | "half_open";

export interface Event {
  id: string;
  tenant_id: string;
  event_type: string;
  payload: unknown;
  created_at: string;
}

export interface Delivery {
  id: string;
  event_id: string;
  endpoint_id: string;
  tenant_id: string;
  status: DeliveryStatus;
  attempt_count: number;
  next_attempt_at: string;
  locked_until: string | null;
  locked_by: string | null;
  idempotency_key: string;
  last_error: string | null;
  created_at: string;
  updated_at: string;
}

export interface DeliveryAttempt {
  id: string;
  delivery_id: string;
  attempt_number: number;
  status_code: number | null;
  response_snippet: string | null;
  error: string | null;
  started_at: string;
  finished_at: string;
}

export interface Endpoint {
  id: string;
  tenant_id: string;
  url: string;
  disabled: boolean;
  circuit_state: CircuitState;
  circuit_open_until: string | null;
  consecutive_failures: number;
  created_at: string;
  updated_at: string;
}

export interface CreateMessage {
  event_type: string;
  payload: unknown;
}

export interface CreateMessageResponse {
  id: string;
  delivery_ids: string[];
}

export interface MessageDetail {
  event: Event;
  deliveries: Delivery[];
}

export interface DeliveryDetail {
  delivery: Delivery;
  attempts: DeliveryAttempt[];
}

export interface CreateEndpoint {
  url: string;
  /** Optional; a `whsec_…` secret is generated server-side if omitted. */
  secret?: string;
}

/** An endpoint plus its signing secret — the secret is returned ONLY on create. */
export interface CreatedEndpoint extends Endpoint {
  secret: string;
}

export interface UpdateEndpoint {
  url?: string;
  disabled?: boolean;
}

export interface ListParams {
  limit?: number;
  offset?: number;
}
