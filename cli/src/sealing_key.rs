//! Snapshot sealing key custody: the CLI bundle check's mirror of the API's
//! reserved name refusal.
//!
//! @spec ACTION-EXECUTOR-23, ACTION-EXECUTOR-16. ADR 0124 decision 1 requires
//! the sealing key to reach only the hosted connector. The API recognizes it by
//! two reserved names, `SNAPSHOT_SEALING_KEY` and
//! `SNAPSHOT_SEALING_KEYS_RETAINED`, and accepts only a `SecretRef`
//! (`{name, from_secret}`) on a connector; every other form is refused with
//! one reason (`curie_internal.sealing_key`,
//! `curie_api.bundles.sealing_key_custody_issues`).
//!
//! Rust cannot import that, so this module restates it and both sides read
//! `tests/vectors/sealing-key-custody.json`: the names, the refusal wording
//! verbatim, the reference grammar, and the refusals each bundle earns with
//! their locations. Change one side and the vector, or the other side's test
//! fails. The deploy preflight runs [`refuse_sealing_key_custody`] before it
//! reads a secret value or talks to the API, so a refused bundle never leaves
//! the operator's machine.

use std::path::Path;

use anyhow::Result;
use serde_norway::Value;

use crate::connector_build::CONNECTORS_FILE;

/// The current snapshot sealing key.
pub const SEALING_KEY_NAME: &str = "SNAPSHOT_SEALING_KEY";
/// Retired sealing keys kept while their records are undoable (ADR 0124
/// decision 3).
pub const SEALING_KEYS_RETAINED_NAME: &str = "SNAPSHOT_SEALING_KEYS_RETAINED";

/// Every reserved sealing key name, in sorted order.
pub const SEALING_KEY_NAMES: [&str; 2] = [SEALING_KEY_NAME, SEALING_KEYS_RETAINED_NAME];

/// The refusal, with `{name}` standing for the offending name. The API's
/// `SEALING_KEY_CUSTODY_REASON`, verbatim.
pub const SEALING_KEY_CUSTODY_REASON: &str = "{name} is a reserved snapshot sealing key: it must \
     reach only the hosted connector, so it may be declared only as a SecretRef \
     (`- name: {name}` with `from_secret:`) on that connector";

/// What the operator does instead, carried in the error's `fix`.
const CUSTODY_FIX: &str = "Declare the sealing key only as a SecretRef on the connector that \
     holds it (`- name: SNAPSHOT_SEALING_KEY` with `from_secret: <secret>`), and remove every \
     other declaration and reference of it from the bundle.";

/// Whether `name` is one of the reserved sealing key names.
pub fn is_sealing_key_name(name: &str) -> bool {
    SEALING_KEY_NAMES.contains(&name)
}

/// The refusal message for `name` declared in a form other than a SecretRef.
pub fn custody_reason(name: &str) -> String {
    SEALING_KEY_CUSTODY_REASON.replace("{name}", name)
}

/// The reserved names `text` references for expansion, in sorted order.
///
/// A reference is `$`, an optional `{`, optional whitespace, then the name,
/// ending where a variable name would (not before `[A-Za-z0-9_]`). That covers
/// `$NAME`, `${NAME}`, `${ NAME }`, `${NAME:-x}`, `${NAME-x}` and a reference
/// nested in another's default, and keeps a longer name that merely starts with
/// a reserved one (`SNAPSHOT_SEALING_KEYRING`) a different variable. The API's
/// pattern is `\$\{?\s*NAME(?![A-Za-z0-9_])`; this is the same match without a
/// lookahead, which Rust's `regex` lacks.
pub fn sealing_key_references(text: &str) -> Vec<String> {
    SEALING_KEY_NAMES
        .iter()
        .filter(|name| references(text, name))
        .map(|name| name.to_string())
        .collect()
}

fn references(text: &str, name: &str) -> bool {
    text.match_indices('$').any(|(at, _)| {
        let rest = &text[at + 1..];
        let rest = rest.strip_prefix('{').unwrap_or(rest);
        let rest = rest.trim_start();
        rest.strip_prefix(name).is_some_and(|after| {
            !after
                .chars()
                .next()
                .is_some_and(|c| c.is_ascii_alphanumeric() || c == '_')
        })
    })
}

/// One refused declaration: the reserved name, where the bundle declares it,
/// and the API's reason. Never the declared value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CustodyIssue {
    pub name: String,
    pub location: String,
    pub message: String,
}

/// Every declaration of a reserved sealing key name other than a SecretRef,
/// in the API's order and with its locations.
///
/// A `plugin.json` `secrets` name, then per connector: a plain `secrets` name,
/// an `env`, `secret_files` or `sealed_secrets` key, a `bearer_secret` that is
/// or references the name, and a reference in `headers`, `url` or
/// `unhosted_url`. Reads leniently like the API: a manifest or
/// `connectors.yaml` that does not parse adds nothing here, because the checks
/// that own those files report it.
pub fn custody_issues(bundle_dir: &Path) -> Result<Vec<CustodyIssue>> {
    let mut issues = Vec::new();
    let mut refuse = |name: &str, location: String| {
        issues.push(CustodyIssue {
            name: name.to_string(),
            location,
            message: custody_reason(name),
        });
    };

    if let Ok((_path, manifest)) = crate::scaffold::load_manifest_json(bundle_dir) {
        if let Some(declared) = manifest.get("secrets").and_then(|v| v.as_array()) {
            for (i, name) in declared.iter().enumerate() {
                if let Some(name) = name.as_str().filter(|name| is_sealing_key_name(name)) {
                    refuse(name, format!("plugin.json (secrets[{i}])"));
                }
            }
        }
    }

    let path = bundle_dir.join(CONNECTORS_FILE);
    let Ok(body) = std::fs::read_to_string(&path) else {
        return Ok(issues);
    };
    // The typed parse decides whether the file is well formed; the untyped
    // walk keeps the document's order, which is the order the API reports in.
    if crate::connector_build::parse_connectors(&body).is_err() {
        return Ok(issues);
    }
    let Ok(document) = serde_norway::from_str::<Value>(&body) else {
        return Ok(issues);
    };
    let Some(connectors) = document.get("connectors").and_then(Value::as_mapping) else {
        return Ok(issues);
    };
    for (connector, spec) in connectors {
        let Some(connector) = connector.as_str() else {
            continue;
        };
        let at = |field: &str| format!("{CONNECTORS_FILE} (connectors.{connector}.{field})");
        for declared in sequence(spec, "secrets") {
            if let Some(name) = declared.as_str().filter(|name| is_sealing_key_name(name)) {
                refuse(name, at("secrets"));
            }
        }
        for form in ["env", "secret_files", "sealed_secrets"] {
            for (name, _value) in mapping(spec, form) {
                if is_sealing_key_name(name) {
                    refuse(name, at(form));
                }
            }
        }
        if let Some(bearer) = spec.get("bearer_secret").and_then(Value::as_str) {
            let named = if is_sealing_key_name(bearer) {
                vec![bearer.to_string()]
            } else {
                sealing_key_references(bearer)
            };
            for name in named {
                refuse(&name, at("bearer_secret"));
            }
        }
        let mut expanded: Vec<(String, &str)> = mapping(spec, "headers")
            .into_iter()
            .filter_map(|(key, value)| value.as_str().map(|text| (format!("headers.{key}"), text)))
            .collect();
        for field in ["url", "unhosted_url"] {
            if let Some(text) = spec.get(field).and_then(Value::as_str) {
                expanded.push((field.to_string(), text));
            }
        }
        for (field, text) in expanded {
            for name in sealing_key_references(text) {
                refuse(&name, at(&field));
            }
        }
    }
    Ok(issues)
}

fn sequence<'a>(spec: &'a Value, field: &str) -> &'a [Value] {
    spec.get(field)
        .and_then(Value::as_sequence)
        .map_or(&[], Vec::as_slice)
}

fn mapping<'a>(spec: &'a Value, field: &str) -> Vec<(&'a str, &'a Value)> {
    spec.get(field)
        .and_then(Value::as_mapping)
        .map(|map| {
            map.iter()
                .filter_map(|(key, value)| key.as_str().map(|key| (key, value)))
                .collect()
        })
        .unwrap_or_default()
}

/// Refuse a bundle that declares a reserved sealing key outside a SecretRef,
/// as a usage error (exit 2) naming each declaration's location with the API's
/// reason. The message carries names and locations only, never a value.
pub fn refuse_sealing_key_custody(bundle_dir: &Path) -> Result<()> {
    let issues = custody_issues(bundle_dir)?;
    if issues.is_empty() {
        return Ok(());
    }
    let listed = issues
        .iter()
        .map(|issue| format!("{}: {}", issue.location, issue.message))
        .collect::<Vec<_>>()
        .join("; ");
    Err(anyhow::Error::from(
        crate::exit::CliError::usage(format!(
            "the bundle declares a reserved snapshot sealing key outside a SecretRef: {listed}"
        ))
        .with_fix(CUSTODY_FIX),
    ))
}
