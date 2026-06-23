-- 0003: store inbound raw bodies byte-exact.
--
-- `text` cannot hold a NUL byte (a signed body containing 0x00 would 500 on
-- insert and wedge a retrying provider), and decoding via from_utf8_lossy
-- silently replaces non-UTF8 bytes — breaking the "exact received body"
-- (audit / re-verification) guarantee. `bytea` stores the bytes verbatim.

ALTER TABLE inbound_events
    ALTER COLUMN raw_body TYPE bytea USING convert_to(raw_body, 'UTF8');
