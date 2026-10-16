//! Forge credential expiry for `curie cluster status` and `curie doctor`
//! (#4397 decision 8, ADR 0197).
//!
//! Every `factory.codeHosts[]` / `factory.trackers[]` credential recorded on
//! the release is classified as a known expiry, unknown, or none. The fields
//! read are non-secret, so this works from the release values alone, with no
//! API call. `tests/vectors/forge-credential-expiry.json` pins the same
//! classification and warning text as the API's `credential_providers`.
//!
//! Authentication (#4630) is a separate reading: `cluster status` merges the
//! API's `GET /forges/credentials/health` report into each line with
//! [`merge_authentication`]. Expiry classification never depends on it.

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
    /// `accepted`, `rejected` or `unknown` (#4630). Anything the API did not
    /// affirmatively report as accepted or rejected is `unknown`.
    pub authentication: &'static str,
    /// Why authentication is unknown or rejected (`unauthorized`,
    /// `credential_unreadable`, `not_found`, `unavailable`, `error`,
    /// `no_probe`, `not_probed`, or `api_unavailable` when the API was not read
    /// or omitted the entry). `None` when accepted, and before any merge.
    pub authentication_reason: Option<String>,
    /// When the API probed this credential (RFC 3339), if it did.
    pub observed_at: Option<String>,
}

/// The reason a line carries when the health report could not be read or
/// omitted the entry.
pub const API_UNAVAILABLE: &str = "api_unavailable";

/// The reason the API gives for credential types it does not probe; the one
/// unknown that does not warn.
pub const NOT_PROBED: &str = "not_probed";

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

    /// The `cluster status` line: the summary plus `; authentication <state>`,
    /// with ` (<reason>)` when the state is unknown.
    pub fn status_line(&self) -> String {
        let mut line = format!("{}; authentication {}", self.summary(), self.authentication);
        if self.authentication == "unknown" {
            if let Some(reason) = &self.authentication_reason {
                line.push_str(&format!(" ({reason})"));
            }
        }
        line
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
                authentication: "unknown",
                authentication_reason: None,
                observed_at: None,
            });
        }
    }
    lines
}

/// Merge the API's authentication report into `lines` by (section, name).
///
/// `entries` is `None` when the report could not be read; every line is then
/// unknown with [`API_UNAVAILABLE`], as is any line the report omits. Only an
/// exact `accepted` or `rejected` from the API is taken; any other state is
/// unknown, so a malformed report can never read as accepted.
pub fn merge_authentication(
    lines: &mut [ExpiryLine],
    entries: Option<&[crate::api::ForgeCredentialHealth]>,
) {
    for line in lines.iter_mut() {
        let entry = entries.and_then(|entries| {
            entries
                .iter()
                .find(|entry| entry.section == line.section && entry.name == line.name)
        });
        let Some(entry) = entry else {
            line.authentication = "unknown";
            line.authentication_reason = Some(API_UNAVAILABLE.to_string());
            line.observed_at = None;
            continue;
        };
        line.observed_at = Some(entry.observed_at.clone());
        match entry.authentication.as_str() {
            "accepted" => {
                line.authentication = "accepted";
                line.authentication_reason = None;
            }
            "rejected" => {
                line.authentication = "rejected";
                line.authentication_reason = Some(
                    entry
                        .reason
                        .clone()
                        .unwrap_or_else(|| "unauthorized".into()),
                );
            }
            _ => {
                line.authentication = "unknown";
                line.authentication_reason =
                    Some(entry.reason.clone().unwrap_or_else(|| "error".into()));
            }
        }
    }
}

/// Status diagnoses from merged lines: `(unhealthy, warnings)`.
///
/// A rejected credential is unhealthy. An unknown one warns unless the API
/// does not probe its type (`not_probed`). When the whole read failed
/// (`read_failure`), the [`API_UNAVAILABLE`] lines share one warning naming the
/// failure class rather than one each.
pub fn authentication_diagnoses(
    lines: &[ExpiryLine],
    read_failure: Option<&str>,
) -> (Vec<String>, Vec<String>) {
    let mut unhealthy = Vec::new();
    let mut warnings = Vec::new();
    if let Some(failure) = read_failure {
        if !lines.is_empty() {
            warnings.push(format!(
                "forge credential authentication is unknown ({API_UNAVAILABLE}): {failure}"
            ));
        }
    }
    for line in lines {
        let head = format!(
            "credential {} {} ({})",
            line.section, line.name, line.credential_type
        );
        match line.authentication {
            "rejected" => unhealthy.push(format!("{head}: authentication rejected")),
            "accepted" => {}
            _ => {
                let reason = line.authentication_reason.as_deref().unwrap_or("error");
                if reason == NOT_PROBED || (read_failure.is_some() && reason == API_UNAVAILABLE) {
                    continue;
                }
                warnings.push(format!("{head}: authentication unknown ({reason})"));
            }
        }
    }
    (unhealthy, warnings)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::ForgeCredentialHealth;

    const OBSERVED: &str = "2026-10-15T12:00:00Z";

    fn now() -> OffsetDateTime {
        OffsetDateTime::parse("2026-10-15T00:00:00Z", &Rfc3339).unwrap()
    }

    fn expiry_fixture_lines() -> Vec<ExpiryLine> {
        let values = serde_json::json!({"factory": {
            "codeHosts": [
                {"name": "github", "kind": "github",
                 "credential": {"type": "github_app", "appId": "1"}},
                {"name": "corp-gitlab", "kind": "gitlab",
                 "credential": {"type": "static_token", "neverExpires": true}}
            ],
            "trackers": [
                {"name": "corp-gitlab", "kind": "gitlab",
                 "credential": {"type": "static_token", "expiresAt": "2026-10-20T00:00:00Z"}},
                {"name": "jira", "kind": "jira_cloud",
                 "credential": {"type": "static_token"}}
            ]
        }});
        report(&values, now())
    }

    fn health_fixture_entry(
        section: &str,
        name: &str,
        authentication: &str,
        reason: Option<&str>,
    ) -> ForgeCredentialHealth {
        ForgeCredentialHealth {
            section: section.into(),
            name: name.into(),
            kind: "gitlab".into(),
            credential_type: "static_token".into(),
            authentication: authentication.into(),
            reason: reason.map(Into::into),
            observed_at: OBSERVED.into(),
        }
    }

    fn find_fixture_line<'a>(lines: &'a [ExpiryLine], section: &str, name: &str) -> &'a ExpiryLine {
        lines
            .iter()
            .find(|line| line.section == section && line.name == name)
            .unwrap()
    }

    #[test]
    fn merge_takes_accepted_and_rejected_by_section_and_name() {
        let mut lines = expiry_fixture_lines();
        let before: Vec<_> = lines
            .iter()
            .map(|l| (l.expiry, l.expires_at, l.warning.clone()))
            .collect();
        let entries = vec![
            health_fixture_entry("code_host", "github", "unknown", Some("not_probed")),
            health_fixture_entry("code_host", "corp-gitlab", "accepted", None),
            health_fixture_entry("tracker", "corp-gitlab", "rejected", Some("unauthorized")),
            health_fixture_entry("tracker", "jira", "unknown", Some("credential_unreadable")),
        ];
        merge_authentication(&mut lines, Some(&entries));
        let host = find_fixture_line(&lines, "code_host", "corp-gitlab");
        assert_eq!(
            (host.authentication, host.authentication_reason.as_deref()),
            ("accepted", None)
        );
        assert_eq!(host.observed_at.as_deref(), Some(OBSERVED));
        let tracker = find_fixture_line(&lines, "tracker", "corp-gitlab");
        assert_eq!(
            (
                tracker.authentication,
                tracker.authentication_reason.as_deref()
            ),
            ("rejected", Some("unauthorized"))
        );
        let jira = find_fixture_line(&lines, "tracker", "jira");
        assert_eq!(
            (jira.authentication, jira.authentication_reason.as_deref()),
            ("unknown", Some("credential_unreadable"))
        );
        let after: Vec<_> = lines
            .iter()
            .map(|l| (l.expiry, l.expires_at, l.warning.clone()))
            .collect();
        assert_eq!(before, after, "authentication never changes expiry");
    }

    #[test]
    fn rejected_degrades_health_and_unknown_warns_except_not_probed() {
        let mut lines = expiry_fixture_lines();
        let entries = vec![
            health_fixture_entry("code_host", "github", "unknown", Some("not_probed")),
            health_fixture_entry("code_host", "corp-gitlab", "accepted", None),
            health_fixture_entry("tracker", "corp-gitlab", "rejected", Some("unauthorized")),
            health_fixture_entry("tracker", "jira", "unknown", Some("no_probe")),
        ];
        merge_authentication(&mut lines, Some(&entries));
        let (unhealthy, warnings) = authentication_diagnoses(&lines, None);
        assert_eq!(
            unhealthy,
            vec!["credential tracker corp-gitlab (static_token): authentication rejected"]
        );
        assert_eq!(
            warnings,
            vec!["credential tracker jira (static_token): authentication unknown (no_probe)"]
        );
    }

    #[test]
    fn unrecognised_states_are_never_accepted() {
        let mut lines = expiry_fixture_lines();
        let entries = vec![
            health_fixture_entry("code_host", "corp-gitlab", "Accepted", None),
            health_fixture_entry("tracker", "corp-gitlab", "ok", None),
            health_fixture_entry("tracker", "jira", "", Some("unavailable")),
        ];
        merge_authentication(&mut lines, Some(&entries));
        for line in &lines {
            assert_eq!(line.authentication, "unknown", "{line:?}");
            assert!(line.authentication_reason.is_some(), "{line:?}");
            assert!(!line.status_line().contains("authentication accepted"));
            let json = serde_json::to_value(line).unwrap();
            assert_eq!(json["authentication"], "unknown", "{json}");
        }
        assert_eq!(
            find_fixture_line(&lines, "tracker", "jira")
                .authentication_reason
                .as_deref(),
            Some("unavailable")
        );
    }

    #[test]
    fn an_omitted_entry_is_api_unavailable_and_warns() {
        let mut lines = expiry_fixture_lines();
        let entries = vec![health_fixture_entry(
            "code_host",
            "corp-gitlab",
            "accepted",
            None,
        )];
        merge_authentication(&mut lines, Some(&entries));
        let jira = find_fixture_line(&lines, "tracker", "jira");
        assert_eq!(
            (jira.authentication, jira.authentication_reason.as_deref()),
            ("unknown", Some(API_UNAVAILABLE))
        );
        assert_eq!(jira.observed_at, None);
        // Same name in the other section does not match.
        assert_eq!(
            find_fixture_line(&lines, "tracker", "corp-gitlab").authentication,
            "unknown"
        );
        let (unhealthy, warnings) = authentication_diagnoses(&lines, None);
        assert!(unhealthy.is_empty());
        assert!(warnings.contains(
            &"credential tracker jira (static_token): authentication unknown (api_unavailable)"
                .to_string()
        ));
        assert_eq!(warnings.len(), 3, "{warnings:?}");
    }

    #[test]
    fn a_failed_read_marks_every_line_api_unavailable_with_one_warning() {
        let mut lines = expiry_fixture_lines();
        merge_authentication(&mut lines, None);
        for line in &lines {
            assert_eq!(
                (line.authentication, line.authentication_reason.as_deref()),
                ("unknown", Some(API_UNAVAILABLE)),
                "{line:?}"
            );
        }
        let (unhealthy, warnings) =
            authentication_diagnoses(&lines, Some("the API request failed"));
        assert!(unhealthy.is_empty());
        assert_eq!(
            warnings,
            vec!["forge credential authentication is unknown (api_unavailable): the API request failed"]
        );
    }

    #[test]
    fn status_line_appends_authentication_and_keeps_summary() {
        let mut lines = expiry_fixture_lines();
        let entries = vec![
            health_fixture_entry("code_host", "github", "unknown", Some("not_probed")),
            health_fixture_entry("code_host", "corp-gitlab", "accepted", None),
            health_fixture_entry("tracker", "corp-gitlab", "rejected", Some("unauthorized")),
        ];
        merge_authentication(&mut lines, Some(&entries));
        let text: Vec<String> = lines.iter().map(ExpiryLine::status_line).collect();
        assert_eq!(
            text,
            vec![
                "credential code_host github (github_app): none; authentication unknown (not_probed)",
                "credential code_host corp-gitlab (static_token): none; authentication accepted",
                "credential tracker corp-gitlab (static_token): known 2026-10-20; authentication rejected",
                "credential tracker jira (static_token): unknown; authentication unknown (api_unavailable)",
            ]
        );
        assert_eq!(
            find_fixture_line(&lines, "code_host", "corp-gitlab").summary(),
            "credential code_host corp-gitlab (static_token): none",
            "summary, which doctor uses, is unchanged"
        );
    }

    #[test]
    fn report_starts_unknown_with_no_reason() {
        for line in expiry_fixture_lines() {
            assert_eq!(line.authentication, "unknown");
            assert_eq!(line.authentication_reason, None);
            assert_eq!(line.observed_at, None);
        }
    }
}
