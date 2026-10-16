//! Versioned installed Helm configuration and pure migrations (issue #2299).
//!
//! The supported upgrade command reads user-supplied Helm values, runs these
//! migrations, and overlays the result. Operators do not choose
//! `--reuse-values` versus `--reset-then-reuse-values`.

use crate::factory_intake::normalize_identifier_value;
use anyhow::{bail, Context, Result};
use serde_json::{json, Map, Value};
use std::collections::{BTreeMap, BTreeSet};

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

#[derive(Debug, Clone)]
pub struct ResolvedFactoryRepository {
    pub path: String,
    pub project_id: String,
}

#[derive(Debug, Clone)]
pub enum FactoryMigrationContext {
    NotRequired,
    LegacyGitHub {
        effective_legacy: Value,
        repositories: Vec<ResolvedFactoryRepository>,
    },
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
    factory: &FactoryMigrationContext,
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
    migrate_factory(&mut values, factory, &mut changes)?;
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

pub(crate) fn legacy_factory_keys() -> Vec<String> {
    serde_norway::from_str(include_str!(
        "../../charts/curie/files/legacy-factory-keys.yaml"
    ))
    .expect("legacy factory key registry")
}

const FACTORY_ENV: &[(&str, &str, Coerce)] = &[
    (
        "GITHUB_FACTORY_INGRESS_ENABLED",
        "githubFactoryIngressEnabled",
        Coerce::Bool,
    ),
    (
        "GITHUB_FACTORY_INTAKE",
        "githubFactoryIntake",
        Coerce::String,
    ),
    (
        "GITHUB_FACTORY_POLL_INTERVAL_S",
        "githubFactoryPollIntervalSeconds",
        Coerce::Number,
    ),
    ("GITHUB_FACTORY_LABEL", "githubFactoryLabel", Coerce::String),
    (
        "GITHUB_FACTORY_MENTION",
        "githubFactoryMention",
        Coerce::String,
    ),
    (
        "GITHUB_FACTORY_CARD_BASE_URL",
        "githubFactoryCardBaseUrl",
        Coerce::String,
    ),
    (
        "GITHUB_FACTORY_CI_WAIT_S",
        "githubFactoryCiWaitSeconds",
        Coerce::Integer,
    ),
];

pub(crate) fn has_legacy_factory_input(values: &Value) -> bool {
    legacy_factory_keys()
        .iter()
        .any(|key| get_path(values, key).is_some_and(|v| !v.is_null()))
        || ["api.extraEnv", "worker.extraEnv"].iter().any(|path| {
            get_path(values, path)
                .and_then(Value::as_array)
                .is_some_and(|items| {
                    let removed = removed_factory_env();
                    items.iter().any(|item| {
                        item.get("name")
                            .and_then(Value::as_str)
                            .is_some_and(|name| removed.iter().any(|old| old == name))
                    })
                })
        })
}

fn removed_factory_env() -> Vec<String> {
    serde_norway::from_str(include_str!(
        "../../charts/curie/files/legacy-factory-env.yaml"
    ))
    .expect("legacy factory environment registry")
}

pub(crate) fn normalized_factory_input(values: &Value) -> Result<Value> {
    let mut normalized = values.clone();
    for list in ["api.extraEnv", "worker.extraEnv"] {
        let Some(items) = get_path(values, list).and_then(Value::as_array) else {
            continue;
        };
        let removed = removed_factory_env();
        let mut retained = Vec::new();
        let mut changed = false;
        for item in items {
            let Some(name) = item.get("name").and_then(Value::as_str) else {
                retained.push(item.clone());
                continue;
            };
            if !removed.iter().any(|old| old == name) {
                retained.push(item.clone());
                continue;
            }
            let setting = format!("{list} {name}");
            if list != "api.extraEnv" || item.get("valueFrom").is_some() {
                bail!("{setting} cannot migrate to factory.bindings");
            }
            let (key, incoming) = if let Some((_, key, coerce)) =
                FACTORY_ENV.iter().find(|(env, _, _)| *env == name)
            {
                (
                    *key,
                    coerce_extra_env(item.get("value"), *coerce)
                        .map_err(|_| anyhow::anyhow!("{setting} cannot migrate"))?,
                )
            } else {
                let key = match name {
                    "GITHUB_FACTORY_BASES" => "githubFactoryBases",
                    "GITHUB_FACTORY_PYTHON_CI" => "githubFactoryPythonCi",
                    "GITHUB_FACTORY_METADATA_CI" => "githubFactoryMetadataCi",
                    _ => bail!("{setting} has no factory successor"),
                };
                let raw = item
                    .get("value")
                    .and_then(Value::as_str)
                    .ok_or_else(|| anyhow::anyhow!("{setting} must contain a policy object"))?;
                let raw: Value = serde_json::from_str(raw)
                    .map_err(|_| anyhow::anyhow!("{setting} must contain a policy object"))?;
                (key, raw)
            };
            let path = format!("api.{key}");
            if let Some(existing) = get_path(&normalized, &path).filter(|v| !v.is_null()) {
                let equal = if key.ends_with("Ci") || key.ends_with("Bases") {
                    let existing = policy_map(existing, key)?;
                    let incoming = policy_map(&incoming, key)?;
                    existing.len() == incoming.len()
                        && existing.iter().all(|(repo, policy)| {
                            incoming
                                .get(repo)
                                .is_some_and(|other| factory_equal(policy, other))
                        })
                } else {
                    existing == &incoming
                };
                if !equal {
                    bail!("{setting} conflicts with {path}");
                }
            }
            set_path(&mut normalized, &path, incoming);
            changed = true;
        }
        if changed {
            set_path(&mut normalized, list, json!(retained));
        }
    }
    Ok(normalized)
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct LegacyBases {
    bases: Vec<String>,
    default_base: Option<String>,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct LegacyPythonCi {
    check: String,
    paths: Vec<String>,
    #[serde(rename = "pendingCheckPrefix")]
    pending: Option<String>,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct LegacyMetadataCi {
    #[serde(default)]
    checks: Vec<String>,
    #[serde(default)]
    statuses: Vec<String>,
}

fn policy_map(raw: &Value, key: &str) -> Result<BTreeMap<String, Value>> {
    let setting = format!("api.{key}");
    let entries = raw
        .as_object()
        .ok_or_else(|| anyhow::anyhow!("{setting} must be an object"))?;
    let mut result = BTreeMap::new();
    for (repo, policy) in entries {
        if repo.ends_with("/*") || crate::api::validate_allowlist_entry(repo).is_err() {
            bail!("{setting} contains an invalid repository key");
        }
        let invalid = || anyhow::anyhow!("{setting} contains an invalid policy");
        let policy = match key {
            "githubFactoryBases" => {
                let p: LegacyBases =
                    serde_json::from_value(policy.clone()).map_err(|_| invalid())?;
                if p.bases.is_empty()
                    || p.bases.iter().any(|base| {
                        base.is_empty()
                            || base.chars().any(char::is_whitespace)
                            || base.starts_with('-')
                            || base.contains("..")
                    })
                    || p.bases.iter().collect::<BTreeSet<_>>().len() != p.bases.len()
                    || p.default_base
                        .as_ref()
                        .is_some_and(|base| !p.bases.contains(base))
                {
                    return Err(invalid());
                }
                json!({"bases": p.bases, "defaultBase": p.default_base})
            }
            "githubFactoryPythonCi" => {
                let p: LegacyPythonCi =
                    serde_json::from_value(policy.clone()).map_err(|_| invalid())?;
                if p.check.trim().is_empty()
                    || p.paths.is_empty()
                    || p.paths.iter().any(|path| {
                        path.trim().is_empty() || path.starts_with('/') || path.ends_with('/')
                    })
                    || p.pending.as_ref().is_some_and(String::is_empty)
                {
                    return Err(invalid());
                }
                json!({"key": p.check, "paths": p.paths, "pendingKeyPrefix": p.pending})
            }
            "githubFactoryMetadataCi" => {
                let p: LegacyMetadataCi =
                    serde_json::from_value(policy.clone()).map_err(|_| invalid())?;
                if (p.checks.is_empty() && p.statuses.is_empty())
                    || p.checks
                        .iter()
                        .any(|key| key.trim().is_empty() || key.starts_with("status:"))
                    || p.statuses.iter().any(|key| key.trim().is_empty())
                {
                    return Err(invalid());
                }
                let mut keys = Vec::new();
                for key in p
                    .checks
                    .into_iter()
                    .chain(p.statuses.into_iter().map(|key| format!("status:{key}")))
                {
                    if !keys.contains(&key) {
                        keys.push(key);
                    }
                }
                json!(keys)
            }
            _ => unreachable!(),
        };
        let folded = repo.to_ascii_lowercase();
        if let Some(previous) = result.get(&folded) {
            if !factory_equal(previous, &policy) {
                bail!("{setting} has conflicting repository keys");
            }
        } else {
            result.insert(folded, policy);
        }
    }
    Ok(result)
}

pub(crate) struct LegacyFactory {
    pub enabled: bool,
    pub intake: String,
    pub poll: Value,
    pub label: String,
    pub mention: String,
    pub card: String,
    pub wait: u64,
    bases: BTreeMap<String, Value>,
    python: BTreeMap<String, Value>,
    metadata: BTreeMap<String, Value>,
    publication_egress: bool,
}

impl LegacyFactory {
    pub(crate) fn parse(values: &Value) -> Result<Self> {
        let scalar = |key: &str, default: Value| -> Value {
            get_path(values, &format!("api.{key}"))
                .filter(|v| !v.is_null())
                .cloned()
                .unwrap_or(default)
        };
        let string = |key: &str, default: &str| -> Result<String> {
            scalar(key, json!(default))
                .as_str()
                .map(str::to_string)
                .ok_or_else(|| anyhow::anyhow!("api.{key} must be a string"))
        };
        let enabled = scalar("githubFactoryIngressEnabled", json!(false))
            .as_bool()
            .ok_or_else(|| anyhow::anyhow!("api.githubFactoryIngressEnabled must be a boolean"))?;
        let intake = string("githubFactoryIntake", "poll")?;
        if !matches!(intake.as_str(), "poll" | "webhook") {
            bail!("api.githubFactoryIntake is invalid");
        }
        let poll = scalar("githubFactoryPollIntervalSeconds", json!(45));
        if !poll.as_f64().is_some_and(|v| v.is_finite() && v > 0.0) {
            bail!("api.githubFactoryPollIntervalSeconds must be positive");
        }
        let label = string("githubFactoryLabel", "")?;
        if label.chars().count() > 50 || label.chars().any(char::is_whitespace) {
            bail!("api.githubFactoryLabel is invalid");
        }
        let mention = string("githubFactoryMention", "")?;
        if !mention.is_empty() && !crate::factory_intake::valid_github_login(&mention) {
            bail!("api.githubFactoryMention is invalid");
        }
        let card = string("githubFactoryCardBaseUrl", "")?;
        let environment = get_path(values, "api.environment")
            .and_then(Value::as_str)
            .unwrap_or("dev");
        if !crate::factory_intake::card_base_url_valid(&card, environment)
            || (!card.is_empty()
                && reqwest::Url::parse(&card)
                    .is_ok_and(|u| !u.username().is_empty() || u.password().is_some()))
        {
            bail!("api.githubFactoryCardBaseUrl is invalid");
        }
        let wait = scalar("githubFactoryCiWaitSeconds", json!(1200))
            .as_u64()
            .filter(|n| (1..=10800).contains(n))
            .ok_or_else(|| {
                anyhow::anyhow!("api.githubFactoryCiWaitSeconds must be an integer in range")
            })?;
        Ok(Self {
            enabled,
            intake,
            poll,
            label,
            mention,
            card,
            wait,
            bases: policy_map(
                &scalar("githubFactoryBases", json!({})),
                "githubFactoryBases",
            )?,
            python: policy_map(
                &scalar("githubFactoryPythonCi", json!({})),
                "githubFactoryPythonCi",
            )?,
            metadata: policy_map(
                &scalar("githubFactoryMetadataCi", json!({})),
                "githubFactoryMetadataCi",
            )?,
            publication_egress: get_path(values, "worker.publication.githubHttpsCidrs")
                .is_some_and(|v| !v.is_null()),
        })
    }

    pub(crate) fn needs_identity(&self) -> bool {
        self.enabled
            || !self.label.is_empty()
            || !self.mention.is_empty()
            || self.poll.as_f64() != Some(45.0)
            || !self.bases.is_empty()
            || !self.python.is_empty()
            || !self.metadata.is_empty()
            || self.publication_egress
    }
}

pub(crate) fn factory_api_origin(values: &Value) -> Result<(String, String)> {
    let raw = get_path(values, "api.githubApiUrl")
        .filter(|v| !v.is_null())
        .map(|v| {
            v.as_str()
                .ok_or_else(|| anyhow::anyhow!("api.githubApiUrl must be a URL"))
        })
        .transpose()?
        .unwrap_or(crate::github_app::DEFAULT_GITHUB_API_URL);
    let url =
        reqwest::Url::parse(raw).map_err(|_| anyhow::anyhow!("api.githubApiUrl must be a URL"))?;
    let hostname = url
        .host_str()
        .ok_or_else(|| anyhow::anyhow!("api.githubApiUrl must name a host"))?;
    if !(url.scheme() == "https"
        || (url.scheme() == "http" && matches!(hostname, "localhost" | "127.0.0.1")))
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
        || !hostname
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'.' | b'-'))
    {
        bail!("api.githubApiUrl must be an HTTPS API origin");
    }
    let host = if hostname == "api.github.com" && url.port().is_none() {
        "github.com".to_string()
    } else if let Some(port) = url.port() {
        format!("{hostname}:{port}")
    } else {
        hostname.to_string()
    };
    Ok((raw.trim_end_matches('/').to_string(), host))
}

pub(crate) fn factory_allowlist(values: &Value) -> Result<Vec<String>> {
    let raw = get_path(values, "api.githubRepoAllowlist")
        .and_then(Value::as_array)
        .filter(|entries| !entries.is_empty())
        .ok_or_else(|| anyhow::anyhow!("api.githubRepoAllowlist must name repositories"))?;
    raw.iter()
        .map(|entry| {
            let entry = entry.as_str().ok_or_else(|| {
                anyhow::anyhow!("api.githubRepoAllowlist must contain repository paths")
            })?;
            if entry.ends_with("/*") || crate::api::validate_allowlist_entry(entry).is_err() {
                bail!("api.githubRepoAllowlist requires unambiguous repository paths");
            }
            Ok(entry.to_string())
        })
        .collect()
}

pub(crate) fn factory_credential(values: &Value) -> Result<Value> {
    let app_id = get_path(values, "api.githubAppId")
        .map(|v| match v {
            Value::String(s) => s.clone(),
            Value::Number(n) => n.to_string(),
            _ => String::new(),
        })
        .filter(|id| {
            !id.is_empty()
                && id.bytes().all(|c| c.is_ascii_digit())
                && id.parse::<u64>().is_ok_and(|id| id > 0)
        })
        .ok_or_else(|| anyhow::anyhow!("api.githubAppId must be a positive integer"))?;
    for key in [
        "api.githubAppExistingSecret",
        "api.githubAppExistingSecretKey",
    ] {
        if !get_path(values, key)
            .and_then(Value::as_str)
            .is_some_and(|s| !s.trim().is_empty())
        {
            bail!("{key} must reference the installed App credential");
        }
    }
    let (name, key) =
        crate::github_app::configured_existing_secret(Some(values)).ok_or_else(|| {
            anyhow::anyhow!(
                "api.githubAppExistingSecret must reference the installed App credential"
            )
        })?;
    Ok(
        json!({"type": "github_app", "appId": app_id, "existingSecret": name, "existingSecretKey": key}),
    )
}

fn factory_equal(left: &Value, right: &Value) -> bool {
    if let (Some(left), Some(right)) = (left.as_array(), right.as_array()) {
        let left_set = left
            .iter()
            .filter_map(Value::as_str)
            .collect::<BTreeSet<_>>();
        let right_set = right
            .iter()
            .filter_map(Value::as_str)
            .collect::<BTreeSet<_>>();
        return left_set.len() == left.len()
            && right_set.len() == right.len()
            && left.iter().all(Value::is_string)
            && right.iter().all(Value::is_string)
            && left_set == right_set;
    }
    left == right
}

fn merge_factory_expected(current: &mut Value, expected: &Value, setting: &str) -> Result<()> {
    if let (Some(current), Some(expected)) = (current.as_object_mut(), expected.as_object()) {
        for (key, value) in expected {
            if let Some(retained) = current.get_mut(key) {
                if key == "metadataRerunKeys" {
                    if !factory_equal(retained, value) {
                        bail!("{setting} conflicts with legacy factory policy");
                    }
                } else if retained.as_str().zip(value.as_str()).is_some_and(|(a, b)| {
                    match key.as_str() {
                        "path" | "host" => a.eq_ignore_ascii_case(b),
                        "apiUrl" => {
                            reqwest::Url::parse(a.trim_end_matches('/')).ok()
                                == reqwest::Url::parse(b.trim_end_matches('/')).ok()
                        }
                        _ => false,
                    }
                }) {
                    continue;
                } else {
                    merge_factory_expected(retained, value, setting)?;
                }
            } else if !value.is_null() {
                current.insert(key.clone(), value.clone());
            }
        }
        return Ok(());
    }
    if current != expected {
        bail!("{setting} conflicts with legacy factory configuration");
    }
    Ok(())
}

fn factory_list(factory: &Value, section: &str) -> Result<Vec<Value>> {
    let setting = format!("factory.{section}");
    let list = match factory.get(section) {
        None | Some(Value::Null) => Vec::new(),
        Some(Value::Array(list)) => list.clone(),
        _ => bail!("{setting} must be a list"),
    };
    let mut names = BTreeSet::new();
    for entry in &list {
        if !entry.is_object()
            || !entry
                .get("name")
                .and_then(Value::as_str)
                .is_some_and(|name| valid_factory_name(name) && names.insert(name.to_string()))
        {
            bail!("{setting} requires unique named entries");
        }
    }
    Ok(list)
}

fn valid_factory_name(name: &str) -> bool {
    (1..=64).contains(&name.len())
        && name.bytes().all(|c| {
            c.is_ascii_lowercase() || c.is_ascii_digit() || matches!(c, b'.' | b'_' | b'-')
        })
        && name
            .as_bytes()
            .first()
            .is_some_and(u8::is_ascii_alphanumeric)
        && name
            .as_bytes()
            .last()
            .is_some_and(u8::is_ascii_alphanumeric)
}

fn factory_connection(
    factory: &mut Value,
    section: &str,
    host: &str,
    expected: Value,
) -> Result<String> {
    let setting = format!("factory.{section}");
    let mut entries = factory_list(factory, section)?;
    let matching: Vec<_> = entries
        .iter()
        .enumerate()
        .filter(|(_, entry)| {
            entry["kind"] == "github"
                && entry["host"]
                    .as_str()
                    .is_some_and(|value| value.eq_ignore_ascii_case(host))
        })
        .map(|(i, _)| i)
        .collect();
    if matching.len() > 1 {
        bail!("{setting} has ambiguous GitHub connections");
    }
    let name = if let Some(&index) = matching.first() {
        let mut expected = expected;
        let name = entries[index]["name"]
            .as_str()
            .expect("validated name")
            .to_string();
        expected["name"] = json!(name);
        merge_factory_expected(&mut entries[index], &expected, &setting)?;
        name
    } else {
        let name = expected["name"]
            .as_str()
            .expect("generated name")
            .to_string();
        if entries.iter().any(|entry| entry["name"] == name) {
            bail!("{setting} generated name conflicts with a retained entry");
        }
        entries.push(expected);
        name
    };
    factory[section] = json!(entries);
    Ok(name)
}

fn factory_egress(
    values: &Value,
    factory: &mut Value,
    host: &str,
    origin: &str,
) -> Result<[Option<Value>; 2]> {
    let mut routes: [Option<Value>; 2] = [None, None];
    let mut publication = None;
    for (index, section) in ["codeHosts", "trackers"].into_iter().enumerate() {
        for entry in factory_list(factory, section)? {
            if entry["kind"] == "github"
                && entry["host"]
                    .as_str()
                    .is_some_and(|value| value.eq_ignore_ascii_case(host))
            {
                if let Some(egress) = entry.get("egress") {
                    validate_factory_egress(egress, &format!("factory.{section}.egress"))?;
                    if routes[index].replace(egress.clone()).is_some() {
                        bail!("factory.{section} has ambiguous retained egress");
                    }
                }
            }
        }
    }
    if let Some(cidrs) =
        get_path(values, "worker.publication.githubHttpsCidrs").filter(|v| !v.is_null())
    {
        let cidrs: Vec<String> = serde_json::from_value(cidrs.clone())
            .map_err(|_| anyhow::anyhow!("worker.publication.githubHttpsCidrs must be CIDRs"))?;
        let legacy = json!(cidrs
            .iter()
            .map(|cidr| json!({"cidr": cidr, "ports": [{"protocol": "TCP", "port": 443}]}))
            .collect::<Vec<_>>());
        validate_factory_egress(&legacy, "worker.publication.githubHttpsCidrs")?;
        if routes[0].as_ref().is_some_and(|route| route != &legacy) {
            bail!("worker.publication.githubHttpsCidrs conflicts with factory.codeHosts.egress");
        }
        publication = Some(legacy.clone());
        routes[0] = Some(legacy);
    }
    if host == "github.com" {
        return Ok(routes);
    }
    let url = reqwest::Url::parse(origin)
        .map_err(|_| anyhow::anyhow!("api.githubApiUrl cannot select factory egress"))?;
    let port = url
        .port_or_known_default()
        .ok_or_else(|| anyhow::anyhow!("api.githubApiUrl cannot select factory egress"))?;
    let literal_ip = url.host_str().and_then(|host| host.parse().ok());
    let unresolved = || {
        anyhow::anyhow!(
            "api.githubApiUrl requires explicit factory.codeHosts.egress and factory.trackers.egress"
        )
    };
    let connectors = get_path(values, "agentSandbox.connectorEgress").and_then(Value::as_object);
    if routes.iter().any(Option::is_none) {
        let mut inferred = None;
        if let Some(connectors) = connectors {
            for entries in connectors.values() {
                if entries.as_array().is_some_and(|entries| {
                    !entries.is_empty()
                        && entries.iter().all(|entry| {
                            entry
                                .get("ports")
                                .and_then(Value::as_array)
                                .is_some_and(|ports| {
                                    !ports.is_empty()
                                        && ports.iter().all(|p| {
                                            p["port"].as_u64() == Some(u64::from(port))
                                                && p.get("protocol").is_none_or(|v| v == "TCP")
                                        })
                                })
                        })
                }) && (publication.as_ref().is_some_and(|route| {
                    factory_egress_covers(entries, route) && factory_egress_covers(route, entries)
                }) || literal_ip.is_some_and(|ip| {
                    entries.as_array().is_some_and(|entries| {
                        entries
                            .iter()
                            .all(|entry| factory_egress_reaches_ip(entry, ip))
                    })
                })) {
                    validate_factory_egress(entries, "agentSandbox.connectorEgress")?;
                    if inferred.as_ref().is_some_and(|prior| {
                        !factory_egress_covers(prior, entries)
                            || !factory_egress_covers(entries, prior)
                    }) {
                        return Err(unresolved());
                    }
                    inferred = Some(entries.clone());
                }
            }
        }
        for route in &mut routes {
            if route.is_none() {
                *route = inferred.clone();
            }
        }
    }
    if routes.iter().any(Option::is_none) {
        return Err(unresolved());
    }
    if factory
        .get("sandboxEgress")
        .and_then(Value::as_object)
        .is_some_and(|agents| agents.values().any(|on| on == &Value::Bool(true)))
    {
        return Ok(routes);
    }
    if let Some(connectors) = connectors {
        let mut sandbox_routes: Vec<Value> = routes.iter().flatten().cloned().collect();
        for section in ["codeHosts", "trackers"] {
            for entry in factory_list(factory, section)? {
                if entry["kind"] == "github"
                    && entry["host"]
                        .as_str()
                        .is_some_and(|value| value.eq_ignore_ascii_case(host))
                {
                    continue;
                }
                match entry.get("egress") {
                    Some(egress)
                        if egress.as_array().is_some_and(|entries| !entries.is_empty()) =>
                    {
                        if validate_factory_egress(egress, "factory.egress").is_err() {
                            return Ok(routes);
                        }
                        sandbox_routes.push(egress.clone());
                    }
                    Some(egress) if !egress.is_null() && !egress.is_array() => return Ok(routes),
                    _ if entry["kind"] == "github" => return Ok(routes),
                    _ => {}
                }
            }
        }
        for (agent, entries) in connectors {
            if factory
                .get("sandboxEgress")
                .and_then(Value::as_object)
                .is_some_and(|agents| agents.contains_key(agent))
            {
                continue;
            }
            if validate_factory_egress(entries, "agentSandbox.connectorEgress").is_ok()
                && sandbox_routes
                    .iter()
                    .all(|route| factory_egress_covers(entries, route))
            {
                let enrollment = factory
                    .as_object_mut()
                    .expect("factory object")
                    .entry("sandboxEgress")
                    .or_insert_with(|| json!({}));
                let agents = enrollment.as_object_mut().ok_or_else(|| {
                    anyhow::anyhow!("factory.sandboxEgress must preserve agent enrollment")
                })?;
                agents.insert(agent.clone(), json!(true));
            }
        }
    }
    Ok(routes)
}

fn factory_egress_covers(granted: &Value, required: &Value) -> bool {
    let (Some(granted), Some(required)) = (granted.as_array(), required.as_array()) else {
        return false;
    };
    !required.is_empty()
        && required.iter().all(|required| {
            let Some(ports) = required.get("ports").and_then(Value::as_array) else {
                return false;
            };
            granted.iter().any(|granted| {
                let Some(grants) = granted.get("ports").and_then(Value::as_array) else {
                    return false;
                };
                granted.get("cidr") == required.get("cidr")
                    && granted.get("except") == required.get("except")
                    && !ports.is_empty()
                    && ports.iter().all(|port| {
                        port.get("endPort").is_none()
                            && grants.iter().any(|grant| {
                                grant.get("endPort").is_none()
                                    && grant.get("port") == port.get("port")
                                    && grant
                                        .get("protocol")
                                        .and_then(Value::as_str)
                                        .unwrap_or("TCP")
                                        == port
                                            .get("protocol")
                                            .and_then(Value::as_str)
                                            .unwrap_or("TCP")
                            })
                    })
            })
        })
}

fn factory_egress_reaches_ip(entry: &Value, ip: std::net::IpAddr) -> bool {
    entry
        .get("cidr")
        .and_then(Value::as_str)
        .is_some_and(|cidr| factory_cidr_contains(cidr, ip))
        && entry.get("except").is_none_or(|except| {
            except.as_array().is_some_and(|except| {
                !except.is_empty()
                    && except.iter().all(|cidr| {
                        cidr.as_str().is_some_and(|cidr| {
                            crate::ops::validate_web_egress_cidrs(&[cidr.to_string()]).is_ok()
                                && !factory_cidr_contains(cidr, ip)
                        })
                    })
            })
        })
}

fn factory_cidr_contains(cidr: &str, ip: std::net::IpAddr) -> bool {
    let Some((network, prefix)) = cidr.split_once('/') else {
        return false;
    };
    let (Ok(network), Ok(prefix)) = (network.parse(), prefix.parse::<u8>()) else {
        return false;
    };
    match (network, ip) {
        (std::net::IpAddr::V4(network), std::net::IpAddr::V4(ip)) if prefix <= 32 => {
            let mask = u32::MAX.checked_shl(32 - u32::from(prefix)).unwrap_or(0);
            u32::from(network) & mask == u32::from(ip) & mask
        }
        (std::net::IpAddr::V6(network), std::net::IpAddr::V6(ip)) if prefix <= 128 => {
            let mask = u128::MAX.checked_shl(128 - u32::from(prefix)).unwrap_or(0);
            u128::from(network) & mask == u128::from(ip) & mask
        }
        _ => false,
    }
}

fn validate_factory_egress(value: &Value, setting: &str) -> Result<()> {
    let invalid = || anyhow::anyhow!("{setting} must preserve explicit CIDRs and ports");
    let entries = value
        .as_array()
        .filter(|entries| !entries.is_empty())
        .ok_or_else(invalid)?;
    for entry in entries {
        let cidr = entry
            .get("cidr")
            .and_then(Value::as_str)
            .ok_or_else(invalid)?;
        crate::ops::validate_web_egress_cidrs(&[cidr.to_string()]).map_err(|_| invalid())?;
        let ports = entry
            .get("ports")
            .and_then(Value::as_array)
            .filter(|ports| !ports.is_empty())
            .ok_or_else(invalid)?;
        if ports.iter().any(|port| {
            !port["port"]
                .as_u64()
                .is_some_and(|n| (1..=65535).contains(&n))
                || port.get("endPort").is_some()
                || port
                    .get("protocol")
                    .is_some_and(|p| !matches!(p.as_str(), Some("TCP" | "UDP" | "SCTP")))
        }) {
            return Err(invalid());
        }
    }
    Ok(())
}

fn migrate_factory(
    values: &mut Value,
    context: &FactoryMigrationContext,
    changes: &mut Vec<String>,
) -> Result<()> {
    if !has_legacy_factory_input(values) {
        for key in legacy_factory_keys() {
            remove_path(values, &key);
        }
        return Ok(());
    }
    let FactoryMigrationContext::LegacyGitHub {
        effective_legacy,
        repositories,
    } = context
    else {
        bail!("api.githubFactory settings require factory.bindings migration context");
    };
    let supplied = normalized_factory_input(values)?;
    let effective = normalized_factory_input(effective_legacy)?;
    let legacy = LegacyFactory::parse(&effective)?;
    let mut factory = match values.get("factory") {
        None | Some(Value::Null) => json!({}),
        Some(value) if value.is_object() => value.clone(),
        _ => bail!("factory must be an object"),
    };
    for (old, new, value) in [
        (
            "githubFactoryIngressEnabled",
            "enabled",
            json!(legacy.enabled),
        ),
        ("githubFactoryIntake", "intake", json!(legacy.intake)),
        (
            "githubFactoryCardBaseUrl",
            "cardBaseUrl",
            json!(legacy.card),
        ),
        (
            "githubFactoryCiWaitSeconds",
            "ciWaitSeconds",
            json!(legacy.wait),
        ),
    ] {
        if legacy.needs_identity()
            || get_path(&supplied, &format!("api.{old}")).is_some_and(|v| !v.is_null())
        {
            merge_factory_expected(
                &mut factory,
                &json!({(new): value}),
                &format!("factory.{new}"),
            )?;
        }
    }
    if legacy.needs_identity() {
        if legacy.label.is_empty() {
            bail!("api.githubFactoryLabel is required for factory.trackers");
        }
        if legacy.mention.is_empty() {
            bail!("api.githubFactoryMention is required for factory.trackers");
        }
        let allowlist = factory_allowlist(&effective)?;
        let credential = factory_credential(&effective)?;
        let (origin, host) = factory_api_origin(&effective)?;
        let mut resolved = BTreeMap::new();
        let mut identities = BTreeMap::new();
        for repo in repositories {
            if repo.path.ends_with("/*")
                || crate::api::validate_allowlist_entry(&repo.path).is_err()
                || !repo.project_id.bytes().all(|c| c.is_ascii_digit())
                || !repo.project_id.parse::<u64>().is_ok_and(|id| id > 0)
                || !allowlist
                    .iter()
                    .any(|path| path.eq_ignore_ascii_case(&repo.path))
            {
                bail!("api.githubRepoAllowlist cannot resolve factory.bindings");
            }
            let path = repo.path.to_ascii_lowercase();
            if resolved.get(&path).is_some_and(|id| id != &repo.project_id)
                || identities
                    .get(&repo.project_id)
                    .is_some_and(|previous| previous != &path)
            {
                bail!("api.githubRepoAllowlist has inconsistent factory.bindings identity");
            }
            resolved.insert(path.clone(), repo.project_id.clone());
            identities.insert(repo.project_id.clone(), path);
        }
        if resolved.is_empty()
            || allowlist
                .iter()
                .any(|repo| !resolved.contains_key(&repo.to_ascii_lowercase()))
        {
            bail!("api.githubRepoAllowlist requires complete factory.bindings identity");
        }
        for (key, policies) in [
            ("githubFactoryBases", &legacy.bases),
            ("githubFactoryPythonCi", &legacy.python),
            ("githubFactoryMetadataCi", &legacy.metadata),
        ] {
            if policies.keys().any(|repo| !resolved.contains_key(repo)) {
                bail!("api.{key} contains policy outside api.githubRepoAllowlist");
            }
        }
        let [code_egress, tracker_egress] =
            factory_egress(&effective, &mut factory, &host, &origin)?;
        let mut entry = json!({"name": "github", "kind": "github", "host": host, "apiUrl": origin, "credential": credential});
        if let Some(egress) = code_egress {
            entry["egress"] = egress;
        }
        let code_host = factory_connection(&mut factory, "codeHosts", &host, entry.clone())?;
        entry.as_object_mut().expect("connection").remove("egress");
        if let Some(egress) = tracker_egress {
            entry["egress"] = egress;
        }
        entry["label"] = json!(legacy.label);
        entry["mention"] = json!(legacy.mention);
        entry["pollIntervalSeconds"] = legacy.poll.clone();
        let tracker = factory_connection(&mut factory, "trackers", &host, entry)?;
        let mut bindings = factory_list(&factory, "bindings")?;
        for (path, id) in resolved {
            let mut repo = json!({"alias": "repository", "codeHost": code_host, "projectId": id, "path": path,
                "bases": [], "defaultBase": null, "ci": {"required": null, "metadataRerunKeys": []}});
            if let Some(bases) = legacy.bases.get(&path) {
                repo["bases"] = bases["bases"].clone();
                repo["defaultBase"] = bases["defaultBase"].clone();
            }
            if let Some(required) = legacy.python.get(&path) {
                repo["ci"]["required"] = required.clone();
            }
            if let Some(keys) = legacy.metadata.get(&path) {
                repo["ci"]["metadataRerunKeys"] = keys.clone();
            }
            let matching: Vec<_> = bindings
                .iter()
                .enumerate()
                .filter(|(_, binding)| {
                    binding["tracker"] == tracker
                        && (normalize_identifier_value(&binding["trackerScopeId"]).as_deref()
                            == Some(id.as_str())
                            || binding.get("repos").and_then(Value::as_array).is_some_and(
                                |repos| {
                                    repos.iter().any(|repo| {
                                        repo["codeHost"] == code_host
                                            && (normalize_identifier_value(&repo["projectId"])
                                                .as_deref()
                                                == Some(id.as_str())
                                                || repo["path"]
                                                    .as_str()
                                                    .is_some_and(|p| p.eq_ignore_ascii_case(&path)))
                                    })
                                },
                            ))
                })
                .map(|(i, _)| i)
                .collect();
            if matching.len() > 1 {
                bail!("factory.bindings has ambiguous repository bindings");
            }
            if let Some(&index) = matching.first() {
                let binding = &mut bindings[index];
                let retained = binding
                    .get("repos")
                    .and_then(Value::as_array)
                    .filter(|repos| repos.len() == 1)
                    .ok_or_else(|| {
                        anyhow::anyhow!(
                            "factory.bindings must preserve native repository authority"
                        )
                    })?;
                repo["alias"] = retained[0].get("alias").cloned().ok_or_else(|| {
                    anyhow::anyhow!("factory.bindings requires repository aliases")
                })?;
                if normalize_identifier_value(&retained[0]["projectId"]).as_deref()
                    == Some(id.as_str())
                {
                    repo["projectId"] = retained[0]["projectId"].clone();
                }
                let mut expected = json!({"tracker": tracker, "trackerScopeId": id});
                if normalize_identifier_value(&binding["trackerScopeId"]).as_deref()
                    == Some(id.as_str())
                {
                    expected["trackerScopeId"] = binding["trackerScopeId"].clone();
                }
                if binding
                    .get("startAuthority")
                    .is_some_and(|a| a != &json!({"type": "write_check"}))
                    || binding
                        .get("defaultRepo")
                        .is_some_and(|alias| !alias.is_null() && alias != &repo["alias"])
                    || binding
                        .get("components")
                        .is_some_and(|v| v.as_object().is_none_or(|v| !v.is_empty()))
                {
                    bail!("factory.bindings conflicts with native write authority");
                }
                merge_factory_expected(binding, &expected, "factory.bindings")?;
                merge_factory_expected(&mut binding["repos"][0], &repo, "factory.bindings")?;
            } else {
                let name = format!("github-{id}");
                if bindings.iter().any(|binding| binding["name"] == name) {
                    bail!("factory.bindings generated name conflicts with a retained entry");
                }
                bindings.push(
                    json!({"name": name, "tracker": tracker, "trackerScopeId": id,
                    "startAuthority": {"type": "write_check"}, "repos": [repo]}),
                );
            }
        }
        factory["bindings"] = json!(bindings);
        if get_path(values, "worker.publication.githubHttpsCidrs").is_some() {
            remove_path(values, "worker.publication.githubHttpsCidrs");
            changes.push(
                "worker.publication.githubHttpsCidrs -> factory.codeHosts.egress".to_string(),
            );
        }
    }
    if get_path(values, "factory.sandboxEgress") != factory.get("sandboxEgress") {
        changes.push("agentSandbox.connectorEgress -> factory.sandboxEgress".to_string());
    }
    values["factory"] = factory;
    for path in ["api.extraEnv", "worker.extraEnv"] {
        if get_path(values, path) != get_path(&supplied, path) {
            if let Some(retained) = get_path(&supplied, path) {
                set_path(values, path, retained.clone());
            }
            changes.push(format!(
                "{path} legacy factory settings -> factory.bindings"
            ));
        }
    }
    for key in legacy_factory_keys() {
        if get_path(&supplied, &key).is_some() {
            changes.push(format!("{key} -> factory configuration"));
        }
        remove_path(values, &key);
    }
    Ok(())
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

/// @spec CLUSTER-VALUES-FILES c1-c3
pub(crate) fn clear_replaced_secret_refs(retained: &mut Value, later: &Value) {
    let mut refs = Vec::new();
    collect_existing_secret_refs(retained, "", &mut refs);
    for reference in refs {
        if get_path(later, &reference).is_some() {
            continue;
        }
        if inline_keys_for(&reference).iter().any(|inline| {
            get_path(later, inline).is_some_and(|value| {
                !value.is_null() && value.as_str().is_none_or(|s| !s.is_empty())
            })
        }) {
            remove_path(retained, &reference);
            let key = format!("{reference}Key");
            if get_path(later, &key).is_none() {
                remove_path(retained, &key);
            }
        }
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

pub(crate) fn set_path(value: &mut Value, dotted: &str, new_val: Value) {
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

pub(crate) fn remove_path(value: &mut Value, dotted: &str) {
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
