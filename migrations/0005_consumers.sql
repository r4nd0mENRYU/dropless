-- 0005: the consumer (application) layer — the missing middle tier.
--
-- A tenant (our customer, e.g. a payments SaaS) has many *consumers* (the SaaS's
-- own end-customers, e.g. each merchant). Each consumer owns its own endpoints.
-- A message published to a consumer fans out ONLY to that consumer's endpoints,
-- so one merchant's events never reach another's.
--
-- Backward-compatible: existing endpoints/events have consumer_id = NULL, i.e.
-- they belong to the tenant directly (the tenant's own receivers). `POST
-- /v1/messages` keeps publishing at that tenant level; the new
-- `POST /v1/app/{uid}/messages` publishes to a specific consumer.

CREATE TABLE consumers (
    id         uuid PRIMARY KEY,
    tenant_id  text NOT NULL,
    uid        text NOT NULL,          -- SaaS-assigned customer id (e.g. "merchant_42")
    name       text,                   -- optional human label
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, uid)            -- a uid is unique within its tenant
);

CREATE INDEX consumers_tenant_idx ON consumers (tenant_id);

-- An endpoint may belong to a consumer (NULL = the tenant's own endpoint).
ALTER TABLE endpoints ADD COLUMN consumer_id uuid REFERENCES consumers (id);
CREATE INDEX endpoints_consumer_idx ON endpoints (consumer_id);

-- An event may be scoped to a consumer (NULL = a tenant-level event).
ALTER TABLE events ADD COLUMN consumer_id uuid REFERENCES consumers (id);
CREATE INDEX events_consumer_idx ON events (consumer_id);
