-- Dropless v0.1 schema: source of truth + queue, single Postgres dependency.

CREATE TABLE IF NOT EXISTS endpoints (
    id                   uuid PRIMARY KEY,
    tenant_id            text NOT NULL,
    url                  text NOT NULL,
    secret               text NOT NULL,
    disabled             boolean NOT NULL DEFAULT false,
    circuit_state        text NOT NULL DEFAULT 'closed',
    circuit_open_until   timestamptz,
    consecutive_failures integer NOT NULL DEFAULT 0,
    created_at           timestamptz NOT NULL DEFAULT now(),
    updated_at           timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS endpoints_tenant_idx ON endpoints (tenant_id);

-- The message: persisted before any 2xx is returned (source of truth).
CREATE TABLE IF NOT EXISTS events (
    id         uuid PRIMARY KEY,
    tenant_id  text NOT NULL,
    event_type text NOT NULL,
    payload    jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS events_tenant_idx ON events (tenant_id, created_at);

-- The queue AND the state: one row per (event, endpoint).
CREATE TABLE IF NOT EXISTS deliveries (
    id              uuid PRIMARY KEY,
    event_id        uuid NOT NULL REFERENCES events (id),
    endpoint_id     uuid NOT NULL REFERENCES endpoints (id),
    tenant_id       text NOT NULL,
    status          text NOT NULL DEFAULT 'pending',
    attempt_count   integer NOT NULL DEFAULT 0,
    next_attempt_at timestamptz NOT NULL DEFAULT now(),
    locked_until    timestamptz,
    locked_by       text,
    idempotency_key text NOT NULL,
    last_error      text,
    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now(),
    UNIQUE (event_id, endpoint_id)
);

-- Dequeue scan: only rows that are due and retryable.
CREATE INDEX IF NOT EXISTS deliveries_due_idx
    ON deliveries (next_attempt_at)
    WHERE status IN ('pending', 'failed');

-- Crash-recovery scan: reclaim in-progress rows whose lock has expired.
CREATE INDEX IF NOT EXISTS deliveries_locked_idx
    ON deliveries (locked_until)
    WHERE status = 'in_progress';

CREATE INDEX IF NOT EXISTS deliveries_event_idx ON deliveries (event_id);
CREATE INDEX IF NOT EXISTS deliveries_tenant_idx ON deliveries (tenant_id);

-- Permanent, append-only attempt log.
CREATE TABLE IF NOT EXISTS delivery_attempts (
    id               uuid PRIMARY KEY,
    delivery_id      uuid NOT NULL REFERENCES deliveries (id),
    attempt_number   integer NOT NULL,
    status_code      integer,
    response_snippet text,
    error            text,
    started_at       timestamptz NOT NULL,
    finished_at      timestamptz NOT NULL
);

CREATE INDEX IF NOT EXISTS delivery_attempts_delivery_idx
    ON delivery_attempts (delivery_id, attempt_number);

-- Exhausted deliveries, available for replay.
CREATE TABLE IF NOT EXISTS dead_letters (
    id          uuid PRIMARY KEY,
    delivery_id uuid NOT NULL REFERENCES deliveries (id),
    event_id    uuid NOT NULL,
    endpoint_id uuid NOT NULL,
    reason      text,
    created_at  timestamptz NOT NULL DEFAULT now()
);

-- API keys (sha256 hash) for the bearer-token middleware.
CREATE TABLE IF NOT EXISTS api_keys (
    id         uuid PRIMARY KEY,
    tenant_id  text NOT NULL,
    key_hash   text NOT NULL UNIQUE,
    created_at timestamptz NOT NULL DEFAULT now()
);
