//! Inbound webhook verification (v0.2 gateway).
//!
//! Pure signature verification for the providers Dropless can receive from.
//! Each returns `Ok(())` when the request is authentic and `Err(CoreError::Signing)`
//! otherwise. Header extraction and persistence live in the API layer; this
//! module is the crypto, kept side-effect-free and unit-tested with fixed
//! known-answer vectors.
//!
//! Key handling differs by provider, deliberately:
//! - **Stripe** / **GitHub** use the secret string *as-is* as the HMAC key and
//!   emit lowercase **hex** signatures.
//! - **hmac** is the Svix/Dropless scheme (base64, `whsec_` key decode) — so one
//!   Dropless can receive from another. It reuses [`crate::signing::verify`].

use std::str::FromStr;

use hmac::{Hmac, Mac};
use sha2::Sha256;

use crate::error::CoreError;

type HmacSha256 = Hmac<Sha256>;

/// A supported inbound provider.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Provider {
    /// Stripe: `Stripe-Signature: t=…,v1=<hex>`, key used as-is.
    Stripe,
    /// GitHub: `X-Hub-Signature-256: sha256=<hex>`, key used as-is.
    Github,
    /// Generic Svix/Dropless scheme (`webhook-id`/`-timestamp`/`-signature`).
    Hmac,
}

impl Provider {
    /// The lowercase string stored in `inbound_sources.provider`.
    pub fn as_str(&self) -> &'static str {
        match self {
            Provider::Stripe => "stripe",
            Provider::Github => "github",
            Provider::Hmac => "hmac",
        }
    }
}

impl FromStr for Provider {
    type Err = CoreError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "stripe" => Ok(Provider::Stripe),
            "github" => Ok(Provider::Github),
            "hmac" => Ok(Provider::Hmac),
            other => Err(CoreError::Invalid(format!("unknown provider {other:?}"))),
        }
    }
}

fn hmac_hex(key: &[u8], msg: &[u8]) -> String {
    // HMAC accepts any key length, so this never fails.
    let mut mac = HmacSha256::new_from_slice(key).expect("hmac accepts any key length");
    mac.update(msg);
    hex::encode(mac.finalize().into_bytes())
}

/// Constant-time byte comparison (no early return on first mismatch).
fn ct_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

fn reject(msg: &str) -> CoreError {
    CoreError::Signing(msg.to_string())
}

/// Verify a Stripe `Stripe-Signature` header (`t=<ts>,v1=<hex>,...`).
/// Signed payload is `"{t}.{body}"`; the key is the secret string as-is.
pub fn verify_stripe(
    secret: &str,
    sig_header: &str,
    body: &[u8],
    now: i64,
    tolerance_secs: i64,
) -> Result<(), CoreError> {
    let mut ts: Option<i64> = None;
    let mut v1s: Vec<&str> = Vec::new();
    for part in sig_header.split(',') {
        let Some((k, v)) = part.split_once('=') else {
            continue;
        };
        match k.trim() {
            "t" => ts = v.trim().parse().ok(),
            "v1" => v1s.push(v.trim()),
            _ => {}
        }
    }
    let ts = ts.ok_or_else(|| reject("stripe: missing timestamp"))?;
    if tolerance_secs > 0 && (now - ts).abs() > tolerance_secs {
        return Err(reject("stripe: timestamp outside tolerance"));
    }
    let mut signed = Vec::with_capacity(body.len() + 12);
    signed.extend_from_slice(ts.to_string().as_bytes());
    signed.push(b'.');
    signed.extend_from_slice(body);
    let expected = hmac_hex(secret.as_bytes(), &signed);
    if v1s.iter().any(|v| ct_eq(v.as_bytes(), expected.as_bytes())) {
        Ok(())
    } else {
        Err(reject("stripe: no matching v1 signature"))
    }
}

/// Verify a GitHub `X-Hub-Signature-256` header (`sha256=<hex>`).
/// The key is the secret string as-is; the body is signed verbatim.
pub fn verify_github(secret: &str, sig_header: &str, body: &[u8]) -> Result<(), CoreError> {
    let provided = sig_header.strip_prefix("sha256=").unwrap_or(sig_header);
    let expected = hmac_hex(secret.as_bytes(), body);
    if ct_eq(provided.as_bytes(), expected.as_bytes()) {
        Ok(())
    } else {
        Err(reject("github: signature mismatch"))
    }
}

/// Verify the Svix/Dropless scheme (`webhook-id` / `-timestamp` / `-signature`).
#[allow(clippy::too_many_arguments)]
pub fn verify_hmac(
    secret: &str,
    id: &str,
    timestamp: i64,
    body: &[u8],
    signature_header: &str,
    now: i64,
    tolerance_secs: i64,
) -> Result<(), CoreError> {
    if tolerance_secs > 0 && (now - timestamp).abs() > tolerance_secs {
        return Err(reject("hmac: timestamp outside tolerance"));
    }
    if crate::signing::verify(secret, id, timestamp, body, signature_header)? {
        Ok(())
    } else {
        Err(reject("hmac: signature mismatch"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn provider_roundtrips() {
        for p in [Provider::Stripe, Provider::Github, Provider::Hmac] {
            assert_eq!(p.as_str().parse::<Provider>().unwrap(), p);
        }
        assert!("nope".parse::<Provider>().is_err());
    }

    #[test]
    fn stripe_known_answer() {
        let body = br#"{"id":"evt_test"}"#;
        // KAT: HMAC-SHA256("whsec_teststripe", "1700000000.{body}") in hex.
        let sig =
            "t=1700000000,v1=23c92070e95c711d126a1723e6e2dbf2ce4212d610064f55c99c26d033e1db9e";
        assert!(verify_stripe("whsec_teststripe", sig, body, 1_700_000_000, 300).is_ok());
        // Extra scheme tokens are ignored; the v1 still matches.
        let sig2 = format!(
            "t=1700000000,v0=deadbeef,{}",
            "v1=23c92070e95c711d126a1723e6e2dbf2ce4212d610064f55c99c26d033e1db9e"
        );
        assert!(verify_stripe("whsec_teststripe", &sig2, body, 1_700_000_000, 300).is_ok());
        // Tampered body, wrong secret, and expired timestamp all reject.
        assert!(verify_stripe(
            "whsec_teststripe",
            sig,
            br#"{"id":"x"}"#,
            1_700_000_000,
            300
        )
        .is_err());
        assert!(verify_stripe("wrong", sig, body, 1_700_000_000, 300).is_err());
        assert!(verify_stripe("whsec_teststripe", sig, body, 1_700_000_000 + 1000, 300).is_err());
    }

    #[test]
    fn github_known_answer() {
        let body = br#"{"action":"opened"}"#;
        let sig = "sha256=c19c90e4a856826fb82a04cf9e5e8b155192345aaf57c755c716be650ee10cb3";
        assert!(verify_github("ghsecret", sig, body).is_ok());
        assert!(verify_github("ghsecret", sig, br#"{"action":"closed"}"#).is_err());
        assert!(verify_github("wrongsecret", sig, body).is_err());
    }

    #[test]
    fn hmac_roundtrip() {
        let h = crate::signing::signature_headers(
            "whsec_dGVzdHNlY3JldA==",
            "id1",
            1_700_000_000,
            b"{}",
        )
        .unwrap();
        assert!(verify_hmac(
            "whsec_dGVzdHNlY3JldA==",
            "id1",
            1_700_000_000,
            b"{}",
            &h.signature,
            1_700_000_000,
            300
        )
        .is_ok());
        assert!(verify_hmac(
            "whsec_dGVzdHNlY3JldA==",
            "id1",
            1_700_000_000,
            b"tampered",
            &h.signature,
            1_700_000_000,
            300
        )
        .is_err());
    }
}
