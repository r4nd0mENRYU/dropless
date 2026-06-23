-- 0004: per-endpoint event-type subscriptions.
--
-- NULL = receive ALL of the tenant's events (backward-compatible default).
-- A non-null array filters fan-out: an endpoint only gets an event whose type
-- matches one of its patterns (exact, a `prefix.*` wildcard, or `*` for all).

ALTER TABLE endpoints ADD COLUMN event_types text[];
