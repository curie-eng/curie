//! The connector caller key pair (ADR-0168 decision 7).
//!
//! The worker signs each sandbox's caller token with the private half, and
//! every hosted connector's caller proxy verifies it with the public half.
//! `cluster up` generates the pair once and carries it forward on every
//! upgrade, as it does the sealing key: a new pair would make every live
//! sandbox's token unverifiable until its runner was replaced.
//!
//! Ed25519 through `ed25519-dalek`, the named primitive the worker's PyNaCl
//! signs with, rather than a derivation assembled here. The wire both halves
//! read is frozen in `tests/vectors/connector-caller-token.json`.

use anyhow::{Context, Result};
use base64::Engine as _;
use ed25519_dalek::SigningKey;

/// The chart value holding the private half: a standard-base64 32-byte seed.
pub const CONNECTOR_CALLER_SIGNING_KEY: &str = "connectorCaller.signingKey";
/// The chart value holding the public half.
pub const CONNECTOR_CALLER_VERIFY_KEY: &str = "connectorCaller.verifyKey";
/// The public half the current one replaced, set only during a rotation.
pub const CONNECTOR_CALLER_PREVIOUS_VERIFY_KEY: &str = "connectorCaller.previousVerifyKey";
/// The operator's own Secret, which wins over the two values above.
pub const CONNECTOR_CALLER_EXISTING_SECRET: &str = "connectorCaller.existingSecret";

/// Every chart value this feature owns, for the preserved-across-upgrade set.
pub const CONNECTOR_CALLER_MANAGED_KEYS: &[&str] = &[
    CONNECTOR_CALLER_SIGNING_KEY,
    CONNECTOR_CALLER_VERIFY_KEY,
    CONNECTOR_CALLER_PREVIOUS_VERIFY_KEY,
    CONNECTOR_CALLER_EXISTING_SECRET,
    "connectorCaller.signingKeyKey",
    "connectorCaller.verifyKeyKey",
];

fn b64() -> base64::engine::general_purpose::GeneralPurpose {
    base64::engine::general_purpose::STANDARD
}

/// A key pair, base64-encoded for transport as chart values.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CallerKeypair {
    pub signing_key: String,
    pub verify_key: String,
}

/// The public half of a standard-base64 32-byte Ed25519 seed.
pub fn verify_key_of(signing_key: &str) -> Result<String> {
    let raw = b64()
        .decode(signing_key.trim())
        .context("the connector caller signing key is not valid base64")?;
    let len = raw.len();
    let seed: [u8; 32] = raw.try_into().map_err(|_| {
        anyhow::anyhow!("the connector caller signing key must be 32 bytes, got {len}")
    })?;
    Ok(b64().encode(SigningKey::from_bytes(&seed).verifying_key().to_bytes()))
}

/// Generate a key pair from the OS CSPRNG.
pub fn generate_keypair() -> Result<CallerKeypair> {
    let mut seed = [0u8; 32];
    getrandom::fill(&mut seed).context("the OS random source failed")?;
    let signing_key = b64().encode(seed);
    let verify_key = verify_key_of(&signing_key)?;
    Ok(CallerKeypair {
        signing_key,
        verify_key,
    })
}
