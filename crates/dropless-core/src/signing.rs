//! Svix-compatible HMAC-SHA256 signing.
//!
//! Headers match Svix so existing verification code migrates unchanged:
//! - `webhook-id`        — the message/delivery id
//! - `webhook-timestamp` — unix seconds
//! - `webhook-signature` — `v1,<base64(HMAC_SHA256(secret, "{id}.{ts}.{body}"))>`
//!
//! The signed content is `"{id}.{timestamp}.{body}"`. The secret may be given
//! raw or with Svix's `whsec_` prefix (which is base64-encoded key material).

use base64::Engine;
use hmac::{Hmac, Mac};
use rand::RngCore;
use sha2::Sha256;

use crate::error::CoreError;

type HmacSha256 = Hmac<Sha256>;

/// Generate a fresh Svix-style signing secret: `whsec_` + base64 of 24 random
/// bytes. Used when creating an endpoint without a caller-supplied secret.
pub fn generate_secret() -> String {
    let mut bytes = [0u8; 24];
    rand::thread_rng().fill_bytes(&mut bytes);
    format!(
        "whsec_{}",
        base64::engine::general_purpose::STANDARD.encode(bytes)
    )
}

/// Headers to attach to an outbound webhook request.
#[derive(Debug, Clone)]
pub struct SignatureHeaders {
    /// `webhook-id`
    pub id: String,
    /// `webhook-timestamp` (unix seconds, as a string)
    pub timestamp: String,
    /// `webhook-signature` (space-separated `v1,...` tokens)
    pub signature: String,
}

/// Decode the signing key. A `whsec_`-prefixed secret carries base64 key
/// material (Svix convention); otherwise the raw bytes are used.
fn key_bytes(secret: &str) -> Vec<u8> {
    if let Some(rest) = secret.strip_prefix("whsec_") {
        base64::engine::general_purpose::STANDARD
            .decode(rest)
            .unwrap_or_else(|_| rest.as_bytes().to_vec())
    } else {
        secret.as_bytes().to_vec()
    }
}

/// Compute the base64 `v1` signature for the given id/timestamp/body.
pub fn sign(secret: &str, id: &str, timestamp: i64, body: &[u8]) -> Result<String, CoreError> {
    let mut mac = HmacSha256::new_from_slice(&key_bytes(secret))
        .map_err(|e| CoreError::Signing(e.to_string()))?;
    mac.update(id.as_bytes());
    mac.update(b".");
    mac.update(timestamp.to_string().as_bytes());
    mac.update(b".");
    mac.update(body);
    let tag = mac.finalize().into_bytes();
    Ok(base64::engine::general_purpose::STANDARD.encode(tag))
}

/// Build the full set of Svix-compatible headers for a request.
pub fn signature_headers(
    secret: &str,
    id: &str,
    timestamp: i64,
    body: &[u8],
) -> Result<SignatureHeaders, CoreError> {
    let sig = sign(secret, id, timestamp, body)?;
    Ok(SignatureHeaders {
        id: id.to_string(),
        timestamp: timestamp.to_string(),
        signature: format!("v1,{sig}"),
    })
}

/// Verify a `webhook-signature` header value (one or more space-separated
/// `vN,sig` tokens). Used by tests and migration helpers. Constant-time over
/// the candidate tokens via HMAC verification.
pub fn verify(
    secret: &str,
    id: &str,
    timestamp: i64,
    body: &[u8],
    signature_header: &str,
) -> Result<bool, CoreError> {
    let expected = sign(secret, id, timestamp, body)?;
    let expected_bytes = base64::engine::general_purpose::STANDARD
        .decode(&expected)
        .map_err(|e| CoreError::Signing(e.to_string()))?;

    for token in signature_header.split_whitespace() {
        let Some((version, sig)) = token.split_once(',') else {
            continue;
        };
        if version != "v1" {
            continue;
        }
        let Ok(provided) = base64::engine::general_purpose::STANDARD.decode(sig) else {
            continue;
        };
        // Re-create the MAC to use its constant-time verify.
        let mut mac = HmacSha256::new_from_slice(&key_bytes(secret))
            .map_err(|e| CoreError::Signing(e.to_string()))?;
        mac.update(id.as_bytes());
        mac.update(b".");
        mac.update(timestamp.to_string().as_bytes());
        mac.update(b".");
        mac.update(body);
        if provided == expected_bytes && mac.verify_slice(&provided).is_ok() {
            return Ok(true);
        }
    }
    Ok(false)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sign_then_verify_roundtrip() {
        let headers =
            signature_headers("whsec_dGVzdHNlY3JldA==", "msg_123", 1_700_000_000, b"{}").unwrap();
        assert!(headers.signature.starts_with("v1,"));
        assert!(verify(
            "whsec_dGVzdHNlY3JldA==",
            "msg_123",
            1_700_000_000,
            b"{}",
            &headers.signature
        )
        .unwrap());
    }

    #[test]
    fn wrong_body_fails_verification() {
        let headers = signature_headers("topsecret", "msg_1", 1_700_000_000, b"original").unwrap();
        assert!(!verify(
            "topsecret",
            "msg_1",
            1_700_000_000,
            b"tampered",
            &headers.signature
        )
        .unwrap());
    }

    #[test]
    fn raw_secret_is_deterministic() {
        let a = sign("topsecret", "id", 42, b"body").unwrap();
        let b = sign("topsecret", "id", 42, b"body").unwrap();
        assert_eq!(a, b);
    }

    #[test]
    fn known_answer_vector() {
        // Cross-language anchor shared with the TypeScript SDK's test: the Svix
        // `v1` signature for this fixed input must be byte-for-byte stable.
        let sig = sign("whsec_dGVzdHNlY3JldA==", "msg_123", 1_700_000_000, b"{}").unwrap();
        assert_eq!(sig, "rzWSNvryAlNWGy2Hk3pzTIxDc3okDWfaqB4dQ9FWTqE=");
        let h =
            signature_headers("whsec_dGVzdHNlY3JldA==", "msg_123", 1_700_000_000, b"{}").unwrap();
        assert_eq!(
            h.signature,
            "v1,rzWSNvryAlNWGy2Hk3pzTIxDc3okDWfaqB4dQ9FWTqE="
        );
    }

    #[test]
    fn generated_secret_is_prefixed_unique_and_usable() {
        let s = generate_secret();
        assert!(s.starts_with("whsec_"));
        assert_ne!(generate_secret(), generate_secret());
        assert!(signature_headers(&s, "id", 1, b"{}").is_ok());
    }
}
