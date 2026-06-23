import test from "node:test";
import assert from "node:assert/strict";
import { Webhook, WebhookVerificationError } from "../dist/index.js";

const SECRET = "whsec_dGVzdHNlY3JldA=="; // base64("testsecret")

test("known-answer vector matches the Rust engine byte-for-byte", async () => {
  const wh = new Webhook(SECRET);
  // Identical fixed vector to dropless-core signing::tests::known_answer_vector.
  assert.equal(
    await wh.sign("msg_123", 1700000000, "{}"),
    "rzWSNvryAlNWGy2Hk3pzTIxDc3okDWfaqB4dQ9FWTqE=",
  );
  assert.equal(
    await wh.signatureHeader("msg_123", 1700000000, "{}"),
    "v1,rzWSNvryAlNWGy2Hk3pzTIxDc3okDWfaqB4dQ9FWTqE=",
  );
});

test("verify accepts a valid signature", async () => {
  const wh = new Webhook(SECRET);
  const ts = 1700000000;
  const body = JSON.stringify({ hello: "world" });
  const headers = {
    "webhook-id": "msg_x",
    "webhook-timestamp": String(ts),
    "webhook-signature": await wh.signatureHeader("msg_x", ts, body),
  };
  await wh.verify(body, headers, { nowSeconds: ts });
});

test("verify rejects a tampered body", async () => {
  const wh = new Webhook(SECRET);
  const ts = 1700000000;
  const headers = {
    "webhook-id": "msg_x",
    "webhook-timestamp": String(ts),
    "webhook-signature": await wh.signatureHeader("msg_x", ts, "original"),
  };
  await assert.rejects(
    () => wh.verify("tampered", headers, { nowSeconds: ts }),
    WebhookVerificationError,
  );
});

test("verify rejects an out-of-tolerance timestamp", async () => {
  const wh = new Webhook(SECRET);
  const ts = 1700000000;
  const headers = {
    "webhook-id": "m",
    "webhook-timestamp": String(ts),
    "webhook-signature": await wh.signatureHeader("m", ts, "{}"),
  };
  await assert.rejects(
    () => wh.verify("{}", headers, { nowSeconds: ts + 10_000 }),
    WebhookVerificationError,
  );
});

test("verify reads headers case-insensitively", async () => {
  const wh = new Webhook(SECRET);
  const ts = 1700000000;
  const body = "{}";
  const headers = {
    "Webhook-Id": "m",
    "Webhook-Timestamp": String(ts),
    "Webhook-Signature": await wh.signatureHeader("m", ts, body),
  };
  await wh.verify(body, headers, { nowSeconds: ts });
});

test("a raw (non-whsec_) secret also works", async () => {
  const wh = new Webhook("topsecret");
  const ts = 1700000000;
  const headers = {
    "webhook-id": "m",
    "webhook-timestamp": String(ts),
    "webhook-signature": await wh.signatureHeader("m", ts, "{}"),
  };
  await wh.verify("{}", headers, { nowSeconds: ts });
});
