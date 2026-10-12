//! Forge credential expiry for `curie cluster status` and `curie doctor`
//! (#4397 decision 8, ADR 0197).
//!
//! Every `factory.codeHosts[]` / `factory.trackers[]` credential recorded on
//! the release is classified as a known expiry, unknown, or none. The fields
//! read are non-secret, so this works from the release values alone, with no
//! API call. `tests/vectors/forge-credential-expiry.json` pins the same
//! classification and warning text as the API's `credential_providers`.

use serde::{Serialize, Serializer};
use time::format_description::well_known::Rfc3339;
use time::{Duration, OffsetDateTime, UtcOffset};

/// A known expiry at most this many days away warns.
pub const WARN_BEFORE_DAYS: i64 = 14;

/// What one recorded credential says about when it stops working.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CredentialExpiry {
    Known(OffsetDateTime),
    /// The type can expire, but no expiry was recorded.
    Unknown,
    /// Re-minted (`github_app`) or recorded as never expiring.
    None,
}

impl CredentialExpiry {
    fn label(&self) -> &'static str {
        match self {
            CredentialExpiry::Known(_) => "known",
            CredentialExpiry::Unknown => "unknown",
            CredentialExpiry::None => "none",
        }
    }
}

/// The credential's `type`, defaulting as the chart does.
fn credential_type(credential: &serde_json::Value) -> &str {
    credential
        .get("type")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("github_app")
}

fn timestamp(credential: &serde_json::Value, key: &str) -> Option<OffsetDateTime> {
    credential
        .get(key)
        .and_then(serde_json::Value::as_str)
        .and_then(|text| OffsetDateTime::parse(text, &Rfc3339).ok())
}

/// Classify one recorded credential (the chart's camelCase shape).
pub fn classify(credential: &serde_json::Value) -> CredentialExpiry {
    let known_or_unknown = |key: &str| {
        timestamp(credential, key).map_or(CredentialExpiry::Unknown, CredentialExpiry::Known)
    };
    match credential_type(credential) {
        "oauth_client_credentials" => known_or_unknown("secretExpiresAt"),
        "static_token" => {
            if credential
                .get("neverExpires")
                .and_then(serde_json::Value::as_bool)
                == Some(true)
            {
                CredentialExpiry::None
            } else {
                known_or_unknown("expiresAt")
            }
        }
        _ => CredentialExpiry::None,
    }
}

fn utc(when: OffsetDateTime) -> OffsetDateTime {
    when.to_offset(UtcOffset::UTC)
}

/// The rotation warning for a known expiry that is past or at most
/// [`WARN_BEFORE_DAYS`] away; `None` otherwise.
pub fn warning(expiry: &CredentialExpiry, now: OffsetDateTime) -> Option<String> {
    let CredentialExpiry::Known(when) = *expiry else {
        return None;
    };
    let at = utc(when);
    let text = format!("{} {:02}:{:02} UTC", at.date(), at.hour(), at.minute());
    let remaining = when - now;
    if remaining <= Duration::ZERO {
        return Some(format!("the credential expired at {text}; rotate it"));
    }
    if remaining > Duration::days(WARN_BEFORE_DAYS) {
        return None;
    }
    let left = match remaining.whole_days() {
        0 => "less than a day".to_string(),
        1 => "1 day".to_string(),
        days => format!("{days} days"),
    };
    Some(format!(
        "the credential expires at {text} ({left} left); rotate it before then"
    ))
}

fn rfc3339<S: Serializer>(when: &Option<OffsetDateTime>, serializer: S) -> Result<S::Ok, S::Error> {
    match when {
        Some(when) => serializer.serialize_str(
            &utc(*when)
                .format(&Rfc3339)
                .map_err(serde::ser::Error::custom)?,
        ),
        None => serializer.serialize_none(),
    }
}

/// One configured credential, as status JSON lists it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ExpiryLine {
    /// `code_host` or `tracker`.
    pub section: &'static str,
    pub name: String,
    #[serde(rename = "type")]
    pub credential_type: String,
    /// `known`, `unknown` or `none`.
    pub expiry: &'static str,
    #[serde(serialize_with = "rfc3339")]
    pub expires_at: Option<OffsetDateTime>,
    pub warning: Option<String>,
}

impl ExpiryLine {
    /// `credential <section> <name> (<type>): known <date>` / `unknown` / `none`.
    pub fn summary(&self) -> String {
        let expiry = match self.expires_at {
            Some(when) => format!("known {}", utc(when).date()),
            None => self.expiry.to_string(),
        };
        format!(
            "credential {} {} ({}): {expiry}",
            self.section, self.name, self.credential_type
        )
    }

    /// The summary with the warning appended, when there is one.
    pub fn detail(&self) -> String {
        match &self.warning {
            Some(warning) => format!("{}; {warning}", self.summary()),
            None => self.summary(),
        }
    }
}

/// One line per code host, then per tracker, in recorded order. Empty when
/// the values carry no factory block.
pub fn report(values: &serde_json::Value, now: OffsetDateTime) -> Vec<ExpiryLine> {
    let Some(factory) = values.get("factory").filter(|f| f.is_object()) else {
        return Vec::new();
    };
    let mut lines = Vec::new();
    for (key, section) in [("codeHosts", "code_host"), ("trackers", "tracker")] {
        let entries = factory
            .get(key)
            .and_then(serde_json::Value::as_array)
            .map(Vec::as_slice)
            .unwrap_or_default();
        for entry in entries {
            let credential = entry.get("credential").unwrap_or(&serde_json::Value::Null);
            let expiry = classify(credential);
            lines.push(ExpiryLine {
                section,
                name: entry
                    .get("name")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or_default()
                    .to_string(),
                credential_type: credential_type(credential).to_string(),
                expiry: expiry.label(),
                expires_at: match expiry {
                    CredentialExpiry::Known(when) => Some(when),
                    _ => None,
                },
                warning: warning(&expiry, now),
            });
        }
    }
    lines
}
