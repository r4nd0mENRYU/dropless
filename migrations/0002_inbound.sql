-- 0002: inbound gateway (v0.2).
--
-- Receive provider webhooks (Stripe / GitHub / generic HMAC), persist the raw
-- body before doing anything else (zero-loss on ingest), dedup by the provider's
-- event id, and bridge verified events into the outbound pipeline.

CREATE TABLE inbound_sources (
    id            uuid PRIMARY KEY,
    tenant_id     text NOT NULL,
    slug          text NOT NULL UNIQUE,          -- URL segment: POST /ingest/{slug}
    provider      text NOT NULL,                 -- 'stripe' | 'github' | 'hmac'
    secret        text NOT NULL,                 -- verification secret
    disabled      boolean NOT NULL DEFAULT false,
    created_at    timestamptz NOT NULL DEFAULT now(),
    updated_at    timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX inbound_sources_tenant_idx ON inbound_sources (tenant_id);

CREATE TABLE inbound_events (
    id              uuid PRIMARY KEY,
    source_id       uuid NOT NULL REFERENCES inbound_sources (id),
    tenant_id       text NOT NULL,
    source_event_id text,                        -- provider event id (dedup key)
    event_type      text NOT NULL,
    raw_body        text NOT NULL,               -- exact received body (audit / replay)
    event_id        uuid REFERENCES events (id), -- the outbound event it bridged to
    received_at     timestamptz NOT NULL DEFAULT now()
);

-- Idempotent ingest: a given (source, provider-event-id) is accepted at most once.
CREATE UNIQUE INDEX inbound_events_dedup_idx
    ON inbound_events (source_id, source_event_id)
    WHERE source_event_id IS NOT NULL;

CREATE INDEX inbound_events_tenant_idx ON inbound_events (tenant_id, received_at DESC);
