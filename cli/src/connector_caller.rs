//! The connector caller key pair (ADR-0168 decision 7) and the caller token
//! it signs, `cct2` (ADR 0239 identity contract 2).
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
//!
//! The token is `cct2.<payload>.<signature>`: the unpadded base64url of the
//! claims as Python's `json.dumps(claims, separators=(",", ":"),
//! sort_keys=True)` writes them (ASCII only, `\u` escapes), then the unpadded
//! base64url of the Ed25519 signature over the ASCII bytes of
//! `cct2.<payload>`. [`mint_caller_token`] mirrors the worker's minter
//! (`curie_worker.caller_token`) and [`verify_caller_token`] the proxy's
//! verifier (`curie_connector_proxy.caller`), byte for byte and refusal for
//! refusal (ADR 0194 parity). The run and work item pair and the deadline
//! bound stand from ADR 0178.

use std::collections::BTreeMap;
use std::fmt::{self, Write as _};

use anyhow::{bail, Context, Result};
use base64::Engine as _;
use ed25519_dalek::{Signature, Signer as _, SigningKey, VerifyingKey};
use serde::de::{Deserialize, Deserializer, Error as _, MapAccess, Visitor};
use serde_json::Value;

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

/// The key a standard-base64 32-byte Ed25519 seed names. Surrounding
/// whitespace (a mounted Secret's trailing newline) is ignored.
fn signing_key_of(signing_key: &str) -> Result<SigningKey> {
    let raw = b64()
        .decode(signing_key.trim())
        .context("the connector caller signing key is not valid base64")?;
    let len = raw.len();
    let seed: [u8; 32] = raw.try_into().map_err(|_| {
        anyhow::anyhow!("the connector caller signing key must be 32 bytes, got {len}")
    })?;
    Ok(SigningKey::from_bytes(&seed))
}

/// The public half of a standard-base64 32-byte Ed25519 seed.
pub fn verify_key_of(signing_key: &str) -> Result<String> {
    Ok(b64().encode(signing_key_of(signing_key)?.verifying_key().to_bytes()))
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

/// The most caller public keys a shared proxy lists: the current key, then the
/// one it replaced during a rotation.
pub const MAX_SHARED_CALLER_PUBLIC_KEYS: usize = 2;
/// The bytes of one Ed25519 public key.
const PUBLIC_KEY_BYTES: usize = 32;

/// Why a shared render refuses its caller public key list, named as the Python
/// render names it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CallerKeysRefusal {
    Empty,
    TooMany,
    Malformed,
    Duplicate,
}

impl CallerKeysRefusal {
    /// Every refusal, in the order the checks run.
    pub const ALL: [CallerKeysRefusal; 4] = [
        CallerKeysRefusal::Empty,
        CallerKeysRefusal::TooMany,
        CallerKeysRefusal::Malformed,
        CallerKeysRefusal::Duplicate,
    ];

    /// The refusal code the shared render reports.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            CallerKeysRefusal::Empty => "caller_public_keys_empty",
            CallerKeysRefusal::TooMany => "caller_public_keys_too_many",
            CallerKeysRefusal::Malformed => "caller_public_key_malformed",
            CallerKeysRefusal::Duplicate => "caller_public_keys_duplicate",
        }
    }
}

impl fmt::Display for CallerKeysRefusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The shared proxy's `CURIE_CALLER_PROXY_PUBLIC_KEYS` value for `keys`, or the
/// first refusal in [`CallerKeysRefusal::ALL`] order.
///
/// The Rust half of the shared render's key list rule
/// (`plugin_format.shared_connector_render.render_application`), frozen in
/// `tests/vectors/shared-render-caller-keys.json` and landed before any Rust
/// shared render exists (ADR 0239 relationship 6). Each key must be the
/// canonical padded standard base64 of 32 bytes, untrimmed, so one key has one
/// spelling; duplicates compare decoded bytes. Curve point validity is not
/// checked, as the Python half does not check it.
pub fn shared_caller_public_keys(keys: &[&str]) -> Result<String, CallerKeysRefusal> {
    if keys.is_empty() {
        return Err(CallerKeysRefusal::Empty);
    }
    if keys.len() > MAX_SHARED_CALLER_PUBLIC_KEYS {
        return Err(CallerKeysRefusal::TooMany);
    }
    let mut decoded: Vec<Vec<u8>> = Vec::with_capacity(keys.len());
    for key in keys {
        let Ok(raw) = b64().decode(key) else {
            return Err(CallerKeysRefusal::Malformed);
        };
        if raw.len() != PUBLIC_KEY_BYTES || b64().encode(&raw) != *key {
            return Err(CallerKeysRefusal::Malformed);
        }
        decoded.push(raw);
    }
    for (at, raw) in decoded.iter().enumerate() {
        if decoded[..at].contains(raw) {
            return Err(CallerKeysRefusal::Duplicate);
        }
    }
    Ok(keys.join(","))
}

/// The first segment of every caller token this wire mints and admits.
pub const CALLER_TOKEN_PREFIX: &str = "cct2";
/// Far above any minted token; bounds the work one header can ask for.
const MAX_TOKEN_CHARS: usize = 4096;
/// The longest nonblank installation, in UTF-8 bytes.
const MAX_INSTALLATION_BYTES: usize = 128;
/// ADR 0239: the code points a nonblank installation may not hold, as inclusive
/// ranges. General category Cc plus Unicode White_Space, written out so no
/// reader depends on a Unicode table version; the corpus's
/// `installation_forbidden` carries the same ranges. Cf characters such as
/// U+FEFF are accepted. A Rust string holds no surrogate, so the python
/// reader's surrogate refusal has no counterpart here.
const INSTALLATION_FORBIDDEN: [(u32, u32); 8] = [
    (0x0000, 0x0020), // Cc U+0000..U+001F, and White_Space U+0009..U+000D, U+0020
    (0x007F, 0x00A0), // Cc U+007F..U+009F, and White_Space U+0085, U+00A0
    (0x1680, 0x1680),
    (0x2000, 0x200A),
    (0x2028, 0x2029),
    (0x202F, 0x202F),
    (0x205F, 0x205F),
    (0x3000, 0x3000),
];
const SIGNATURE_BYTES: usize = 64;
const CLAIMS: [&str; 4] = ["agent", "agent_id", "exp", "installation"];
const RUN_CLAIMS: [&str; 2] = ["run", "work_item"];

fn b64url() -> base64::engine::general_purpose::GeneralPurpose {
    // No padding accepted and no stray trailing bits: one spelling per byte string.
    base64::engine::general_purpose::URL_SAFE_NO_PAD
}

/// What a caller token signs (ADR 0239 identity contract 2).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CallerClaims {
    /// Opaque installation text, compared byte for byte. Blank only for a
    /// dedicated standalone worker.
    pub installation: String,
    /// The agent's canonical lowercase hyphenated UUID.
    pub agent_id: String,
    /// The agent's display name, carried verbatim.
    pub agent: String,
    /// Absolute unix seconds; the token is valid while `exp > now`.
    pub exp: i64,
    /// The run, paired with `work_item` (ADR 0178).
    pub run: Option<String>,
    /// The work item, paired with `run` (ADR 0178).
    pub work_item: Option<String>,
}

/// Which proxy is verifying, and so which claims it compares.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VerifyMode {
    /// A dedicated proxy: the agent must be on `admits` (exact); the
    /// installation is not compared and may be blank.
    Dedicated { admits: Vec<String> },
    /// Shared execution: the claim's installation must equal this one exactly.
    Shared { installation: String },
}

/// Why a caller token is refused, named as the Python verifier names it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CallerRefusal {
    Missing,
    Invalid,
    Expired,
    NotAdmitted,
}

impl CallerRefusal {
    /// The refusal name the caller proxy reports.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            CallerRefusal::Missing => "missing",
            CallerRefusal::Invalid => "invalid",
            CallerRefusal::Expired => "expired",
            CallerRefusal::NotAdmitted => "not_admitted",
        }
    }
}

/// A nonblank installation under ADR 0239: at most 128 UTF-8 bytes and no code
/// point in `INSTALLATION_FORBIDDEN`.
fn installation_ok(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_INSTALLATION_BYTES
        && value.chars().all(|c| {
            let point = u32::from(c);
            !INSTALLATION_FORBIDDEN
                .iter()
                .any(|&(first, last)| (first..=last).contains(&point))
        })
}

/// Canonical lowercase hyphenated UUID text.
fn uuid_ok(value: &str) -> bool {
    let bytes = value.as_bytes();
    bytes.len() == 36
        && bytes.iter().enumerate().all(|(at, &byte)| match at {
            8 | 13 | 18 | 23 => byte == b'-',
            _ => byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte),
        })
}

/// Why `claims` is not a token's claims, or `None` when every field rule holds.
fn claims_fault(claims: &CallerClaims) -> Option<&'static str> {
    if !claims.installation.is_empty() && !installation_ok(&claims.installation) {
        return Some("installation must be blank or at most 128 UTF-8 bytes with no control or white space character");
    }
    if !uuid_ok(&claims.agent_id) {
        return Some("agent_id must be a lowercase hyphenated UUID");
    }
    if claims.agent.is_empty() {
        return Some("agent must not be blank");
    }
    if claims.exp < 0 {
        return Some("exp must not be negative");
    }
    match (&claims.run, &claims.work_item) {
        (None, None) => None,
        (Some(run), Some(work_item)) if uuid_ok(run) && uuid_ok(work_item) => None,
        (Some(_), Some(_)) => Some("run and work_item must be lowercase hyphenated UUIDs"),
        _ => Some("run and work_item are a pair or absent"),
    }
}

/// `value` as a JSON string the way Python's `json.dumps` writes it with
/// `ensure_ascii`: the short escapes, `\u00xx` for every other control and
/// DEL, and lowercase `\uxxxx` for every non-ASCII character, astral ones as
/// a surrogate pair.
fn write_json_string(out: &mut String, value: &str) {
    out.push('"');
    for ch in value.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{8}' => out.push_str("\\b"),
            '\u{c}' => out.push_str("\\f"),
            ' '..='~' => out.push(ch),
            _ => {
                let mut units = [0u16; 2];
                for unit in ch.encode_utf16(&mut units) {
                    // Writing to a String cannot fail.
                    let _ = write!(out, "\\u{unit:04x}");
                }
            }
        }
    }
    out.push('"');
}

/// The canonical payload: sorted keys, no whitespace, ASCII only.
fn canonical_payload(claims: &CallerClaims) -> String {
    let mut out = String::from("{\"agent\":");
    write_json_string(&mut out, &claims.agent);
    out.push_str(",\"agent_id\":");
    write_json_string(&mut out, &claims.agent_id);
    let _ = write!(out, ",\"exp\":{}", claims.exp);
    out.push_str(",\"installation\":");
    write_json_string(&mut out, &claims.installation);
    if let (Some(run), Some(work_item)) = (&claims.run, &claims.work_item) {
        out.push_str(",\"run\":");
        write_json_string(&mut out, run);
        out.push_str(",\"work_item\":");
        write_json_string(&mut out, work_item);
    }
    out.push('}');
    out
}

/// Mint a `cct2` caller token for `claims` with a standard-base64 32-byte seed.
///
/// Refuses the claims the worker's minter refuses; no refusal echoes the seed.
pub fn mint_caller_token(signing_key: &str, claims: &CallerClaims) -> Result<String> {
    if let Some(fault) = claims_fault(claims) {
        bail!("refusing to mint a connector caller token: {fault}");
    }
    let key = signing_key_of(signing_key)?;
    let payload = b64url().encode(canonical_payload(claims));
    let signed = format!("{CALLER_TOKEN_PREFIX}.{payload}");
    let signature = b64url().encode(key.sign(signed.as_bytes()).to_bytes());
    Ok(format!("{signed}.{signature}"))
}

/// A JSON object whose keys are unique; a repeated key fails the parse.
struct UniqueObject(BTreeMap<String, Value>);

impl<'de> Deserialize<'de> for UniqueObject {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct ObjectVisitor;

        impl<'de> Visitor<'de> for ObjectVisitor {
            type Value = UniqueObject;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a JSON object with unique keys")
            }

            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<UniqueObject, A::Error> {
                let mut fields = BTreeMap::new();
                while let Some(key) = map.next_key::<String>()? {
                    let value = map.next_value::<Value>()?;
                    if fields.insert(key, value).is_some() {
                        return Err(A::Error::custom("a duplicate key"));
                    }
                }
                Ok(UniqueObject(fields))
            }
        }

        deserializer.deserialize_map(ObjectVisitor)
    }
}

/// `exp` as a nonnegative `i64`, or `None`. A JSON number serde_json reads as
/// `f64` (`1e3`, `1.0`, `-0`) or as a `u64` past `i64::MAX` is refused, and
/// the text check keeps `-0` refused should `arbitrary_precision` ever be on.
fn exp_of(value: &Value) -> Option<i64> {
    let Value::Number(number) = value else {
        return None;
    };
    let exp = number.as_i64()?;
    (exp >= 0 && number.to_string() == exp.to_string()).then_some(exp)
}

fn text_of(value: &Value) -> Option<String> {
    match value {
        Value::String(text) => Some(text.clone()),
        _ => None,
    }
}

/// The claims of a signed payload, or `None` when any wire rule fails.
fn parsed_claims(payload: &[u8]) -> Option<CallerClaims> {
    // The minter's output is always ASCII; this also rules out a BOM.
    if !payload.is_ascii() {
        return None;
    }
    let UniqueObject(mut fields) = serde_json::from_slice(payload).ok()?;
    let keys: Vec<&str> = fields.keys().map(String::as_str).collect();
    let paired = keys.len() == CLAIMS.len() + RUN_CLAIMS.len();
    let mut expected: Vec<&str> = CLAIMS.to_vec();
    if paired {
        expected.extend(RUN_CLAIMS);
        expected.sort_unstable();
    }
    if keys != expected {
        return None;
    }
    let mut take = |key: &str| fields.remove(key).expect("the key set was checked");
    let installation = text_of(&take("installation"))?;
    let agent_id = text_of(&take("agent_id"))?;
    let agent = text_of(&take("agent"))?;
    let exp = exp_of(&take("exp"))?;
    let (run, work_item) = if paired {
        (
            Some(text_of(&take("run"))?),
            Some(text_of(&take("work_item"))?),
        )
    } else {
        (None, None)
    };
    let claims = CallerClaims {
        installation,
        agent_id,
        agent,
        exp,
        run,
        work_item,
    };
    claims_fault(&claims).is_none().then_some(claims)
}

/// True when any of the standard-base64 `public_keys` verifies `signature`
/// over `message`. A key that does not decode verifies nothing.
fn signed_by(public_keys: &[String], message: &[u8], signature: &Signature) -> bool {
    public_keys.iter().any(|text| {
        let Ok(raw) = b64().decode(text.trim()) else {
            return false;
        };
        let Ok(raw) = <[u8; 32]>::try_from(raw) else {
            return false;
        };
        VerifyingKey::from_bytes(&raw)
            .is_ok_and(|key| key.verify_strict(message, signature).is_ok())
    })
}

/// The wire-valid claims of `token`, in either mode, or `None`.
///
/// The signature is checked over the received `cct2.<payload>` text before
/// anything is parsed, and nothing is re-serialized.
fn token_claims(public_keys: &[String], token: &str) -> Option<CallerClaims> {
    if !token.is_ascii() || token.len() > MAX_TOKEN_CHARS {
        return None;
    }
    let mut parts = token.split('.');
    let (Some(prefix), Some(payload), Some(signature), None) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return None;
    };
    if prefix != CALLER_TOKEN_PREFIX {
        return None;
    }
    let payload_bytes = b64url().decode(payload).ok()?;
    let signature_bytes: [u8; SIGNATURE_BYTES] =
        b64url().decode(signature).ok()?.try_into().ok()?;
    let signed = &token[..prefix.len() + 1 + payload.len()];
    if !signed_by(
        public_keys,
        signed.as_bytes(),
        &Signature::from_bytes(&signature_bytes),
    ) {
        return None;
    }
    parsed_claims(&payload_bytes)
}

/// Admit `token` under `mode` at unix time `now`, or name the refusal.
///
/// `public_keys` are standard-base64 32-byte Ed25519 public keys; a token
/// signed by any of them verifies. Never panics on its input. In shared mode
/// a blank or invalid configured installation refuses every token as invalid.
pub fn verify_caller_token(
    public_keys: &[String],
    token: Option<&str>,
    mode: &VerifyMode,
    now: i64,
) -> Result<CallerClaims, CallerRefusal> {
    let token = match token {
        None | Some("") => return Err(CallerRefusal::Missing),
        Some(token) => token,
    };
    if let VerifyMode::Shared { installation } = mode {
        if !installation_ok(installation) {
            return Err(CallerRefusal::Invalid);
        }
    }
    let claims = token_claims(public_keys, token).ok_or(CallerRefusal::Invalid)?;
    match mode {
        VerifyMode::Dedicated { admits } => {
            if claims.exp <= now {
                Err(CallerRefusal::Expired)
            } else if !admits.contains(&claims.agent) {
                Err(CallerRefusal::NotAdmitted)
            } else {
                Ok(claims)
            }
        }
        VerifyMode::Shared { installation } => {
            if claims.installation.is_empty() {
                Err(CallerRefusal::Invalid)
            } else if claims.installation != *installation {
                Err(CallerRefusal::NotAdmitted)
            } else if claims.exp <= now {
                Err(CallerRefusal::Expired)
            } else {
                Ok(claims)
            }
        }
    }
}
