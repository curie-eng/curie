//! Versioned installed Helm configuration and pure migrations (issue #2299).
//!
//! The supported upgrade command reads user-supplied Helm values, runs these
//! migrations, and overlays the result. Operators do not choose
//! `--reuse-values` versus `--reset-then-reuse-values`.

use anyhow::{bail, Context, Result};
use serde_json::{json, Map, Value};

/// Schema version stamped onto every v0.9.0 install and upgrade.
pub const TARGET_SCHEMA_VERSION: &str = "0.9.0";

/// Result of migrating one installed configuration document.
#[derive(Debug, Clone)]
pub struct MigrationOutcome {
    pub schema_version: String,
    pub migrated_from: Option<String>,
    pub values: Value,
    pub changes: Vec<String>,
}

#[derive(Clone, Copy)]
enum Coerce {
    String,
    Number,
    Integer,
    Bool,
}

/// Exact extraEnv name -> first-class Helm key successors.
const EXTRA_ENV_SUCCESSORS: &[(&str, &str, Coerce)] = &[
    (
        "CURIE_RUNNER_TOTAL_TIMEOUT_S",
        "worker.runnerTotalTimeoutSeconds",
        Coerce::Number,
    ),
    (
        "SLACK_API_BASE_URL",
        "worker.slackApiBaseUrl",
        Coerce::String,
    ),
    ("CURIE_SHIMMER", "worker.shimmer", Coerce::Bool),
    ("CURIE_STATUS_TEXT", "worker.statusText", Coerce::String),
    (
        "CURIE_CLAIM_TIMEOUT_SECONDS",
        "worker.claimTimeoutSeconds",
        Coerce::Number,
    ),
    (
        "CURIE_ROUTE_TTL_SECONDS",
        "worker.routeTtlSeconds",
        Coerce::Integer,
    ),
    (
        "CURIE_SUSPENDED_ROUTE_TTL_SECONDS",
        "worker.suspendedRouteTtlSeconds",
        Coerce::Integer,
    ),
    (
        "CURIE_DELIVERY_BUDGET_S",
        "worker.deliveryBudgetSeconds",
        Coerce::Integer,
    ),
    (
        "CURIE_SLACK_TRUSTED_ORIGINS",
        "worker.slackTrustedOrigins",
        Coerce::String,
    ),
];

const EXTRA_ENV_LISTS: &[&str] = &[
    "worker.extraEnv",
    "api.extraEnv",
    "dispatcher.extraEnv",
    "agentSandbox.runner.extraEnv",
];

/// Irregular existingSecret -> inline credential paths. The regular case is
/// `{field}ExistingSecret` -> `{field}` on the same object.
const INLINE_EXCEPTIONS: &[(&str, &[&str])] = &[
    ("api.githubAppExistingSecret", &["api.githubAppPrivateKey"]),
    ("postgres.existingSecret", &["postgres.auth.password"]),
    ("valkey.existingSecret", &["valkey.password"]),
    ("clickhouse.existingSecret", &["clickhouse.auth.password"]),
    (
        "rustfs.existingSecret",
        &["rustfs.auth.rootPassword", "rustfs.auth.secretKey"],
    ),
    (
        "langfuse.existingSecret",
        &[
            "langfuse.salt",
            "langfuse.encryptionKey",
            "langfuse.nextauthSecret",
        ],
    ),
];

/// Exact extraEnv name -> first-class Helm key successors.
pub fn extra_env_successors() -> &'static [(&'static str, &'static str)] {
    const PAIRS: &[(&str, &str)] = &[
        (
            "CURIE_RUNNER_TOTAL_TIMEOUT_S",
            "worker.runnerTotalTimeoutSeconds",
        ),
        ("SLACK_API_BASE_URL", "worker.slackApiBaseUrl"),
        ("CURIE_SHIMMER", "worker.shimmer"),
        ("CURIE_STATUS_TEXT", "worker.statusText"),
        ("CURIE_CLAIM_TIMEOUT_SECONDS", "worker.claimTimeoutSeconds"),
        ("CURIE_ROUTE_TTL_SECONDS", "worker.routeTtlSeconds"),
        (
            "CURIE_SUSPENDED_ROUTE_TTL_SECONDS",
            "worker.suspendedRouteTtlSeconds",
        ),
        ("CURIE_DELIVERY_BUDGET_S", "worker.deliveryBudgetSeconds"),
        ("CURIE_SLACK_TRUSTED_ORIGINS", "worker.slackTrustedOrigins"),
    ];
    PAIRS
}

/// Migrate user-supplied Helm values from a supported v0.8.x install to the
/// v0.9.0 schema. Pure: no cluster I/O.
pub fn migrate_installed_config(
    mut values: Value,
    installed_chart: Option<&str>,
) -> Result<MigrationOutcome> {
    if !values.is_object() {
        if values.is_null() {
            values = json!({});
        } else {
            bail!("installed Helm values must be an object");
        }
    }

    let source = infer_schema_version(&values, installed_chart)?;
    if !is_supported_source(&source) {
        bail!("installed configuration schema {source} is not a supported v0.8.x upgrade source");
    }

    let mut changes = Vec::new();
    promote_extra_env(&mut values, &mut changes)?;
    strip_inline_secrets(&mut values, &mut changes);

    let migrated_from = if source == TARGET_SCHEMA_VERSION {
        get_path(&values, "config.migratedFrom")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
    } else {
        Some(source.clone())
    };

    set_path(
        &mut values,
        "config.schemaVersion",
        Value::String(TARGET_SCHEMA_VERSION.to_string()),
    );
    match &migrated_from {
        Some(from) => set_path(
            &mut values,
            "config.migratedFrom",
            Value::String(from.clone()),
        ),
        None => {
            if get_path(&values, "config.migratedFrom")
                .and_then(Value::as_str)
                .unwrap_or("")
                .is_empty()
            {
                remove_path(&mut values, "config.migratedFrom");
            }
        }
    }

    Ok(MigrationOutcome {
        schema_version: TARGET_SCHEMA_VERSION.to_string(),
        migrated_from,
        values,
        changes,
    })
}

/// Redacted upgrade-plan lines. Must never include credential values.
pub fn redacted_upgrade_plan(outcome: &MigrationOutcome) -> Vec<String> {
    let from = outcome
        .migrated_from
        .as_deref()
        .unwrap_or(&outcome.schema_version);
    let mut lines = vec![format!(
        "config schema: {from} -> {}",
        outcome.schema_version
    )];
    lines.extend(outcome.changes.iter().cloned());
    lines
}

fn infer_schema_version(values: &Value, installed_chart: Option<&str>) -> Result<String> {
    if let Some(version) = get_path(values, "config.schemaVersion")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        return Ok(version.to_string());
    }
    if let Some(chart) = installed_chart {
        let name = chart.rsplit('/').next().unwrap_or(chart);
        let version = name.strip_prefix("curie-").unwrap_or(name).trim();
        if !version.is_empty() && version != "curie" {
            return Ok(version.to_string());
        }
    }
    Ok("0.8".to_string())
}

fn is_supported_source(version: &str) -> bool {
    version == TARGET_SCHEMA_VERSION || version == "0.8" || version.starts_with("0.8.")
}

fn promote_extra_env(values: &mut Value, changes: &mut Vec<String>) -> Result<()> {
    let mut seen: Map<String, Value> = Map::new();
    for list_path in EXTRA_ENV_LISTS {
        let Some(Value::Array(items)) = get_path(values, list_path).cloned() else {
            continue;
        };
        let mut kept = Vec::new();
        for item in items {
            let Some(name) = item.get("name").and_then(Value::as_str) else {
                kept.push(item);
                continue;
            };
            let Some((_, helm_key, coerce)) = EXTRA_ENV_SUCCESSORS
                .iter()
                .find(|(env, _, _)| *env == name)
                .copied()
            else {
                kept.push(item);
                continue;
            };
            if item.get("valueFrom").is_some() {
                bail!(
                    "legacy extraEnv {name} uses valueFrom and cannot migrate to {helm_key}; \
                     remove the extraEnv entry or set {helm_key} directly"
                );
            }
            let incoming = coerce_extra_env(item.get("value"), coerce)
                .with_context(|| format!("legacy extraEnv {name} cannot migrate to {helm_key}"))?;
            if let Some(previous) = seen.get(name) {
                if !json_equivalent(previous, &incoming) {
                    bail!(
                        "legacy extraEnv {name} conflicts with {helm_key}; remove one before upgrading"
                    );
                }
            }
            if let Some(existing) = get_path(values, helm_key) {
                if !json_equivalent(existing, &incoming) {
                    bail!(
                        "legacy extraEnv {name} conflicts with {helm_key}; remove one before upgrading"
                    );
                }
                changes.push(format!("extraEnv {name} dropped; matches {helm_key}"));
            } else {
                set_path(values, helm_key, incoming.clone());
                changes.push(format!("extraEnv {name} -> {helm_key}"));
            }
            seen.insert(name.to_string(), incoming);
        }
        if kept.is_empty() {
            set_path(values, list_path, json!([]));
        } else {
            set_path(values, list_path, Value::Array(kept));
        }
    }
    Ok(())
}

fn coerce_extra_env(value: Option<&Value>, coerce: Coerce) -> Result<Value> {
    let raw = match value {
        None | Some(Value::Null) => bail!("missing value"),
        Some(Value::String(s)) => s.clone(),
        Some(Value::Number(n)) => n.to_string(),
        Some(Value::Bool(b)) => b.to_string(),
        Some(_) => bail!("unsupported extraEnv value shape"),
    };
    match coerce {
        Coerce::String => Ok(Value::String(raw)),
        Coerce::Number => {
            let parsed: f64 = raw
                .parse()
                .map_err(|_| anyhow::anyhow!("value is not a number"))?;
            if parsed.fract() == 0.0 && parsed.abs() <= i64::MAX as f64 {
                Ok(json!(parsed as i64))
            } else {
                Ok(json!(parsed))
            }
        }
        Coerce::Integer => {
            let parsed: i64 = raw
                .parse()
                .map_err(|_| anyhow::anyhow!("value is not an integer"))?;
            Ok(json!(parsed))
        }
        Coerce::Bool => match raw.to_ascii_lowercase().as_str() {
            "true" | "1" => Ok(Value::Bool(true)),
            "false" | "0" => Ok(Value::Bool(false)),
            _ => bail!("value is not a boolean"),
        },
    }
}

fn json_equivalent(left: &Value, right: &Value) -> bool {
    if left == right {
        return true;
    }
    match (left, right) {
        (Value::String(s), Value::Number(n)) | (Value::Number(n), Value::String(s)) => {
            s.parse::<f64>().ok() == n.as_f64()
        }
        (Value::String(s), Value::Bool(b)) | (Value::Bool(b), Value::String(s)) => {
            match s.to_ascii_lowercase().as_str() {
                "true" | "1" => *b,
                "false" | "0" => !*b,
                _ => false,
            }
        }
        (Value::Number(a), Value::Number(b)) => a.as_f64() == b.as_f64(),
        _ => false,
    }
}

fn strip_inline_secrets(values: &mut Value, changes: &mut Vec<String>) {
    let mut refs = Vec::new();
    collect_existing_secret_refs(values, "", &mut refs);
    for secret_key in refs {
        changes.push(format!("preserved external secret {secret_key}"));
        for inline in inline_keys_for(&secret_key) {
            if get_path(values, &inline).is_some() {
                remove_path(values, &inline);
                changes.push(format!(
                    "omitted inline {inline} because {secret_key} is set"
                ));
            }
        }
    }
}

fn collect_existing_secret_refs(value: &Value, prefix: &str, out: &mut Vec<String>) {
    let Some(map) = value.as_object() else {
        return;
    };
    for (key, child) in map {
        let path = if prefix.is_empty() {
            key.clone()
        } else {
            format!("{prefix}.{key}")
        };
        let is_ref = key == "existingSecret" || key.ends_with("ExistingSecret");
        if is_ref {
            if let Some(name) = child.as_str().map(str::trim).filter(|s| !s.is_empty()) {
                let _ = name;
                out.push(path.clone());
            }
        }
        if child.is_object() {
            collect_existing_secret_refs(child, &path, out);
        }
    }
}

fn inline_keys_for(secret_key: &str) -> Vec<String> {
    for (key, inlines) in INLINE_EXCEPTIONS {
        if *key == secret_key {
            return inlines.iter().map(|s| (*s).to_string()).collect();
        }
    }
    let leaf = secret_key.rsplit('.').next().unwrap_or(secret_key);
    let Some(field) = leaf.strip_suffix("ExistingSecret") else {
        return Vec::new();
    };
    match secret_key.rsplit_once('.') {
        Some((parent, _)) => vec![format!("{parent}.{field}")],
        None => vec![field.to_string()],
    }
}

fn get_path<'a>(value: &'a Value, dotted: &str) -> Option<&'a Value> {
    let mut cursor = value;
    for part in dotted.split('.') {
        cursor = cursor.get(part)?;
    }
    Some(cursor)
}

fn set_path(value: &mut Value, dotted: &str, new_val: Value) {
    let mut parts: Vec<&str> = dotted.split('.').collect();
    let Some(last) = parts.pop() else {
        return;
    };
    let mut cursor = value;
    for part in parts {
        if !cursor.is_object() {
            *cursor = json!({});
        }
        let map = cursor.as_object_mut().expect("object path");
        cursor = map.entry(part.to_string()).or_insert_with(|| json!({}));
    }
    if !cursor.is_object() {
        *cursor = json!({});
    }
    cursor
        .as_object_mut()
        .expect("object path")
        .insert(last.to_string(), new_val);
}

fn remove_path(value: &mut Value, dotted: &str) {
    let mut parts: Vec<&str> = dotted.split('.').collect();
    let Some(last) = parts.pop() else {
        return;
    };
    let mut cursor = value;
    for part in parts {
        cursor = match cursor.get_mut(part) {
            Some(next) => next,
            None => return,
        };
    }
    if let Some(obj) = cursor.as_object_mut() {
        obj.remove(last);
    }
}

#[cfg(test)]
mod successor_table_tests {
    use super::*;

    #[test]
    fn public_successor_table_matches_promotion_table() {
        assert_eq!(EXTRA_ENV_SUCCESSORS.len(), extra_env_successors().len());
        for (env, key, _) in EXTRA_ENV_SUCCESSORS {
            assert!(
                extra_env_successors()
                    .iter()
                    .any(|(public_env, public_key)| public_env == env && public_key == key),
                "extra_env_successors() missing {env} -> {key}"
            );
        }
    }
}
