//! Versioned installed-configuration migrations (issue #2299).
//!
//! Fixtures under `data/upgrade-config/` are user-supplied Helm values from
//! released v0.8.x charts, not current-chart defaults.

use curie::config_migrate::{
    extra_env_successors, migrate_installed_config, redacted_upgrade_plan, MigrationOutcome,
    TARGET_SCHEMA_VERSION,
};
use serde_json::{json, Value};

fn load_fixture(name: &str) -> (String, Value) {
    let raw = match name {
        "v0.8.0" => include_str!("data/upgrade-config/v0.8.0-user-values.json"),
        "v0.8.1" => include_str!("data/upgrade-config/v0.8.1-user-values.json"),
        "v0.8.2" => include_str!("data/upgrade-config/v0.8.2-user-values.json"),
        "v0.8.3" => include_str!("data/upgrade-config/v0.8.3-user-values.json"),
        "v0.8.4" => include_str!("data/upgrade-config/v0.8.4-user-values.json"),
        "v0.8.5" => include_str!("data/upgrade-config/v0.8.5-user-values.json"),
        other => panic!("unknown fixture {other}"),
    };
    let mut value: Value = serde_json::from_str(raw).expect("fixture json");
    let released = value
        .get("_fixture")
        .and_then(|f| f.get("releasedChart"))
        .and_then(|v| v.as_str())
        .unwrap_or(name)
        .trim_start_matches('v')
        .to_string();
    if let Some(obj) = value.as_object_mut() {
        obj.remove("_fixture");
    }
    (released, value)
}

fn extra_env_names(values: &Value, path: &[&str]) -> Vec<String> {
    let mut cursor = values;
    for part in path {
        cursor = match cursor.get(*part) {
            Some(next) => next,
            None => return Vec::new(),
        };
    }
    cursor
        .as_array()
        .map(|items| {
            items
                .iter()
                .filter_map(|item| {
                    item.get("name")
                        .and_then(|n| n.as_str())
                        .map(str::to_string)
                })
                .collect()
        })
        .unwrap_or_default()
}

fn number_at(values: &Value, path: &[&str]) -> Option<f64> {
    let mut cursor = values;
    for part in path {
        cursor = cursor.get(*part)?;
    }
    cursor.as_f64().or_else(|| cursor.as_str()?.parse().ok())
}

fn string_at(values: &Value, path: &[&str]) -> Option<String> {
    let mut cursor = values;
    for part in path {
        cursor = cursor.get(*part)?;
    }
    match cursor {
        Value::String(s) => Some(s.clone()),
        Value::Null => None,
        other => Some(other.to_string()),
    }
}

fn assert_redacted(outcome: &MigrationOutcome, forbidden: &[&str]) {
    let plan = redacted_upgrade_plan(outcome).join("\n");
    let err_debug = format!("{outcome:?}");
    for secret in forbidden {
        assert!(
            !plan.contains(secret),
            "redacted plan leaked {secret:?}: {plan}"
        );
        assert!(
            !err_debug.contains(secret),
            "debug output leaked {secret:?}: {err_debug}"
        );
    }
}

#[test]
fn successor_table_names_runner_timeout_red_on_revert() {
    assert!(
        extra_env_successors()
            .iter()
            .any(|(env, key)| *env == "CURIE_RUNNER_TOTAL_TIMEOUT_S"
                && *key == "worker.runnerTotalTimeoutSeconds"),
        "red-on-revert: CURIE_RUNNER_TOTAL_TIMEOUT_S must map to worker.runnerTotalTimeoutSeconds"
    );
    assert!(
        extra_env_successors()
            .iter()
            .any(|(env, key)| *env == "SLACK_API_BASE_URL" && *key == "worker.slackApiBaseUrl"),
        "red-on-revert: SLACK_API_BASE_URL must map to worker.slackApiBaseUrl"
    );
}

#[test]
fn every_released_v0_8_fixture_migrates_to_v0_9() {
    for name in ["v0.8.0", "v0.8.1", "v0.8.2", "v0.8.3", "v0.8.4", "v0.8.5"] {
        let (released, values) = load_fixture(name);
        let chart = format!("curie-{released}");
        let outcome = migrate_installed_config(values, Some(&chart)).unwrap_or_else(|err| {
            panic!("{name} migration failed: {err:#}");
        });
        assert_eq!(
            outcome.schema_version, TARGET_SCHEMA_VERSION,
            "{name} must stamp the v0.9.0 schema"
        );
        assert_eq!(
            string_at(&outcome.values, &["config", "schemaVersion"]).as_deref(),
            Some(TARGET_SCHEMA_VERSION),
            "{name} must persist config.schemaVersion"
        );
        assert_eq!(
            string_at(&outcome.values, &["config", "migratedFrom"]).as_deref(),
            Some(released.as_str()),
            "{name} must record migratedFrom"
        );
        assert!(
            !extra_env_names(&outcome.values, &["worker", "extraEnv"])
                .iter()
                .any(|n| n == "CURIE_RUNNER_TOTAL_TIMEOUT_S"),
            "{name} must drop promoted extraEnv CURIE_RUNNER_TOTAL_TIMEOUT_S"
        );
        assert!(
            number_at(&outcome.values, &["worker", "runnerTotalTimeoutSeconds"]).is_some(),
            "{name} must set worker.runnerTotalTimeoutSeconds"
        );
        assert_eq!(
            string_at(&outcome.values, &["ui", "deploy"]).as_deref(),
            Some("false"),
            "{name} must keep the operator ui.deploy override"
        );
        let plan = redacted_upgrade_plan(&outcome).join("\n");
        assert!(
            plan.contains(&format!(
                "config schema: {released} -> {TARGET_SCHEMA_VERSION}"
            )),
            "{name} plan must expose schema versions: {plan}"
        );
        assert_redacted(
            &outcome,
            &[
                "xoxb-test-token-must-not-leak",
                "sk-ant-test-must-not-leak",
                "ghp_test-token-must-not-leak",
                "adapter-secret-must-not-leak",
                "MII-test",
                "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=",
            ],
        );
    }
}

#[test]
fn v0_8_0_promotes_slack_base_url_and_keeps_generic_extra_env() {
    let (_, values) = load_fixture("v0.8.0");
    let outcome = migrate_installed_config(values, Some("curie-0.8.0")).unwrap();
    assert_eq!(
        string_at(&outcome.values, &["worker", "slackApiBaseUrl"]).as_deref(),
        Some("https://slack.example.com")
    );
    let names = extra_env_names(&outcome.values, &["worker", "extraEnv"]);
    assert!(names.contains(&"PROVIDER_BASE_URL".to_string()));
    assert!(!names.contains(&"SLACK_API_BASE_URL".to_string()));
}

#[test]
fn v0_8_4_preserves_external_secret_refs_and_drops_inline() {
    let (_, values) = load_fixture("v0.8.4");
    let outcome = migrate_installed_config(values, Some("curie-0.8.4")).unwrap();
    assert_eq!(
        string_at(
            &outcome.values,
            &["dispatcher", "slack", "botTokenExistingSecret"]
        )
        .as_deref(),
        Some("acme-slack")
    );
    assert_eq!(
        string_at(
            &outcome.values,
            &["dispatcher", "slack", "botTokenExistingSecretKey"]
        )
        .as_deref(),
        Some("botToken")
    );
    assert!(
        string_at(&outcome.values, &["dispatcher", "slack", "botToken"]).is_none(),
        "inline botToken must not be restored when existingSecret is set"
    );
    assert_eq!(
        string_at(
            &outcome.values,
            &["agentSandbox", "runner", "credentialsExistingSecret"]
        )
        .as_deref(),
        Some("acme-model")
    );
    assert!(
        string_at(&outcome.values, &["agentSandbox", "runner", "credentials"]).is_none(),
        "inline model credentials must not be restored when existingSecret is set"
    );
}

#[test]
fn extra_env_conflict_with_first_class_is_rejected_before_mutation() {
    let values = json!({
        "worker": {
            "runnerTotalTimeoutSeconds": 600,
            "extraEnv": [
                {"name": "CURIE_RUNNER_TOTAL_TIMEOUT_S", "value": "120"}
            ]
        }
    });
    let err = migrate_installed_config(values, Some("curie-0.8.4")).unwrap_err();
    let message = format!("{err:#}");
    assert!(
        message.contains("CURIE_RUNNER_TOTAL_TIMEOUT_S"),
        "conflict must name the extraEnv entry: {message}"
    );
    assert!(
        message.contains("worker.runnerTotalTimeoutSeconds"),
        "conflict must name the first-class successor: {message}"
    );
    assert!(
        !message.contains("120") && !message.contains("600"),
        "conflict must not print the colliding values: {message}"
    );
}

#[test]
fn extra_env_value_from_successor_is_rejected() {
    let values = json!({
        "worker": {
            "extraEnv": [{
                "name": "CURIE_RUNNER_TOTAL_TIMEOUT_S",
                "valueFrom": {"secretKeyRef": {"name": "acme-timeout", "key": "seconds"}}
            }]
        }
    });
    let err = migrate_installed_config(values, Some("curie-0.8.4")).unwrap_err();
    let message = format!("{err:#}");
    assert!(message.contains("valueFrom"), "{message}");
    assert!(
        message.contains("CURIE_RUNNER_TOTAL_TIMEOUT_S"),
        "{message}"
    );
    assert!(!message.contains("acme-timeout"), "{message}");
}

#[test]
fn matching_extra_env_and_first_class_drops_extra_env() {
    let values = json!({
        "worker": {
            "runnerTotalTimeoutSeconds": 120,
            "extraEnv": [
                {"name": "CURIE_RUNNER_TOTAL_TIMEOUT_S", "value": "120"},
                {"name": "PROVIDER_BASE_URL", "value": "https://provider.example.com/v1"}
            ]
        }
    });
    let outcome = migrate_installed_config(values, Some("curie-0.8.4")).unwrap();
    assert_eq!(
        number_at(&outcome.values, &["worker", "runnerTotalTimeoutSeconds"]),
        Some(120.0)
    );
    let names = extra_env_names(&outcome.values, &["worker", "extraEnv"]);
    assert_eq!(names, vec!["PROVIDER_BASE_URL".to_string()]);
}

#[test]
fn migration_is_idempotent() {
    let (_, values) = load_fixture("v0.8.4");
    let first = migrate_installed_config(values, Some("curie-0.8.4")).unwrap();
    let second = migrate_installed_config(first.values.clone(), Some("curie-0.9.0")).unwrap();
    assert_eq!(first.values, second.values);
    assert_eq!(second.schema_version, TARGET_SCHEMA_VERSION);
}

#[test]
fn new_defaults_are_not_frozen_as_operator_intent() {
    let values = json!({
        "ui": {"deploy": false},
        "worker": {
            "extraEnv": [{"name": "PROVIDER_BASE_URL", "value": "https://provider.example.com/v1"}]
        }
    });
    let outcome = migrate_installed_config(values, Some("curie-0.8.5")).unwrap();
    assert!(
        outcome.values.pointer("/worker/upgradeDrain").is_none(),
        "absent chart defaults must stay absent so helm applies the target default"
    );
    assert_eq!(
        string_at(&outcome.values, &["ui", "deploy"]).as_deref(),
        Some("false")
    );
}

#[test]
fn unsupported_schema_is_rejected() {
    let values = json!({"config": {"schemaVersion": "0.7.3"}, "ui": {"deploy": false}});
    let err = migrate_installed_config(values, Some("curie-0.7.3")).unwrap_err();
    let message = format!("{err:#}");
    assert!(message.contains("0.7.3"), "{message}");
    assert!(message.contains("v0.8"), "{message}");
}

#[test]
fn already_versioned_v0_9_is_a_no_op_stamp() {
    let values = json!({
        "config": {"schemaVersion": "0.9.0"},
        "ui": {"deploy": false},
        "worker": {"runnerTotalTimeoutSeconds": 180}
    });
    let outcome = migrate_installed_config(values.clone(), None).unwrap();
    assert_eq!(outcome.values["ui"], values["ui"]);
    assert_eq!(
        outcome.values["worker"]["runnerTotalTimeoutSeconds"],
        values["worker"]["runnerTotalTimeoutSeconds"]
    );
    assert_eq!(
        string_at(&outcome.values, &["config", "schemaVersion"]).as_deref(),
        Some(TARGET_SCHEMA_VERSION)
    );
}

fn chart_dir() -> std::path::PathBuf {
    std::path::PathBuf::from(concat!(env!("CARGO_MANIFEST_DIR"), "/../charts/curie"))
}

/// helm absence is not a chart regression; `.github/workflows/helm-ci.yaml`
/// gates the chart for real. Same posture as `chart_fullname_parity.rs`.
fn helm_is_absent() -> bool {
    if std::process::Command::new("helm")
        .arg("version")
        .output()
        .is_err()
    {
        eprintln!("skipping: helm is not on PATH");
        return true;
    }
    false
}

fn helm_template_with_values(values: &Value) -> std::process::Output {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("values.json");
    std::fs::write(&file, serde_json::to_vec_pretty(values).unwrap()).unwrap();
    std::process::Command::new("helm")
        .arg("template")
        .arg("curie")
        .arg(chart_dir())
        .arg("-f")
        .arg(&file)
        .output()
        .expect("run helm template")
}

/// #4397: the factory reads no `GITHUB_FACTORY_*` environment, so a v0.12.x
/// install that still sets the metadata CI policy through `api.extraEnv` is
/// refused by the chart, naming its `factory.*` successor, instead of
/// upgrading with the policy silently dropped.
#[test]
fn removed_metadata_ci_extra_env_is_refused_by_the_chart() {
    if helm_is_absent() {
        return;
    }
    let values = json!({
        "api": {
            "extraEnv": [{
                "name": "GITHUB_FACTORY_METADATA_CI",
                "value": "{\"acme/widgets\":{\"checks\":[\"sentinel-4397-do-not-print\"]}}"
            }]
        }
    });
    let output = helm_template_with_values(&values);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        !output.status.success(),
        "the chart must refuse the removed entry"
    );
    assert!(
        stderr.contains("api.extraEnv GITHUB_FACTORY_METADATA_CI is no longer read")
            && stderr.contains("factory.bindings"),
        "the refusal must name the entry and its successor: {stderr}"
    );
    assert!(!stderr.contains("sentinel-4397-do-not-print"), "{stderr}");
}

/// Reserved names that map to a single Helm key yet need no extraEnv
/// migration: no released guide ever told an operator to set them through
/// extraEnv. Adding a reservation to `charts/curie/files/reserved-env.yaml`
/// forces a choice: a successor in `config_migrate.rs`, or a line here.
const RESERVED_WITHOUT_MIGRATION: &[&str] = &[
    // Next feature settings were introduced as Helm values, not extraEnv overrides.
    "CURIE_ACTION_EXECUTOR_ENABLED",
    "CURIE_ACTION_EXECUTOR_MAX_CONCURRENT_SANDBOXES",
    "CURIE_ATTACHMENT_THREAD_MAX_BYTES",
    "CURIE_ATTACHMENT_THREAD_MAX_FILES",
    "CURIE_REMEDIATION_ENABLED",
    "CURIE_TEST_INSTALLATION_DRIVERS",
    "CURIE_TEST_INSTALLATION_ENABLED",
    "CURIE_TEST_INSTALLATION_THREAD_TURN_LIMIT",
    // E2E build and registry settings likewise have chart-owned values from introduction.
    "CURIE_E2E_BUILDER_IMAGE",
    "CURIE_E2E_BUILD_CACHE_REPO",
    "CURIE_E2E_BUILD_TIMEOUT_SECONDS",
    "CURIE_E2E_GIT_IMAGE",
    "CURIE_E2E_PUSH_IMAGE",
    "CURIE_E2E_REAPER_INTERVAL_S",
    "CURIE_E2E_REGISTRY",
    "CURIE_E2E_REGISTRY_INSECURE",
    "CURIE_E2E_REGISTRY_TOKEN_HOSTS",
    "CURIE_E2E_SOURCE_HOSTS",
    // Trust file paths are generated from a mounted ConfigMap, not migrated extraEnv paths.
    "CURIE_CODE_HOST_CA_BUNDLE",
    "CURIE_REPO_CA_BUNDLE",
    "GIT_SSL_CAINFO",
    "REQUESTS_CA_BUNDLE",
    "SSL_CERT_FILE",
    "ANTHROPIC_BASE_URL",
    "API_KEY",
    "APPROVAL_SWEEP_INTERVAL_S",
    "BUNDLE_BUCKET",
    "COMMIT_POLL_INTERVAL_S",
    "CURIE_ADAPTER_CREDENTIALS",
    "CURIE_AGENT_CONNECTOR_SECRET_POOLS",
    "CURIE_API_KEY",
    "CURIE_API_PREFLIGHT_TIMEOUT_SECONDS",
    "CURIE_API_URL",
    "CURIE_APPROVAL_CHAT_ATTESTER_SECRET",
    "CURIE_APPROVAL_RECOVERY_ENABLED",
    "CURIE_ATTACHMENT_ENABLED",
    "CURIE_ATTACHMENT_MAX_FILE_BYTES",
    "CURIE_ATTACHMENT_REFERENCE_TTL_SECONDS",
    "CURIE_ATTACHMENT_RETENTION_TTL_SECONDS",
    "CURIE_BUDGET",
    "CURIE_CONNECTOR_APP_NAME",
    "CURIE_CONNECTOR_CALLER_PREVIOUS_PUBLIC_KEY",
    "CURIE_CONNECTOR_CALLER_PUBLIC_KEY",
    "CURIE_CONNECTOR_CALLER_SIGNING_KEY",
    "CURIE_CONNECTOR_PROXY_IMAGE",
    "CURIE_CONNECTOR_PROXY_IMAGE_PULL_POLICY",
    "CURIE_CONNECTOR_PROXY_IMAGE_PULL_SECRETS",
    "CURIE_CONNECTOR_RECONCILE",
    "CURIE_CONNECTOR_RECONCILE_INTERVAL_S",
    "CURIE_CREDENTIALS",
    "CURIE_DELIVERY_LEASE_HEARTBEAT_S",
    "CURIE_DELIVERY_LEASE_TTL_S",
    "CURIE_DELIVERY_SHUTDOWN_RESERVE_S",
    "CURIE_E2E_CONNECTOR_ENABLED",
    "CURIE_E2E_CONNECTOR_IMAGE",
    "CURIE_E2E_NAMESPACE_PREFIX",
    "CURIE_E2E_OWNER_LABEL_KEY",
    "CURIE_E2E_OWNER_LABEL_VALUE",
    "CURIE_E2E_POD_SECURITY",
    "CURIE_E2E_SERVICE_ACCOUNT",
    "CURIE_E2E_SERVICE_ACCOUNT_NAMESPACE",
    "CURIE_E2E_TTL_SECONDS",
    "CURIE_E2E_WORKER_CLUSTER_ROLE",
    "CURIE_FAKE_MODEL",
    "CURIE_INTERNAL_WORKER_TOKEN",
    "CURIE_MODEL",
    "CURIE_PLUGIN_DIR",
    "CURIE_PUBLICATION_ALLOW_DEPENDENCY_ADDITIONS",
    "CURIE_PUBLICATION_CPU_LIMIT",
    "CURIE_PUBLICATION_CPU_REQUEST",
    "CURIE_PUBLICATION_ENABLED",
    "CURIE_PUBLICATION_EPHEMERAL_LIMIT",
    "CURIE_PUBLICATION_EPHEMERAL_REQUEST",
    "CURIE_PUBLICATION_GIT_COMMAND_TIMEOUT_SECONDS",
    "CURIE_PUBLICATION_GIT_USER_EMAIL",
    "CURIE_PUBLICATION_GIT_USER_NAME",
    "CURIE_PUBLICATION_IMAGE_PULL_POLICY",
    "CURIE_PUBLICATION_IMAGE_PULL_SECRETS",
    "CURIE_PUBLICATION_JOB_ACTIVE_DEADLINE_SECONDS",
    "CURIE_PUBLICATION_LEASE_SECONDS",
    "CURIE_PUBLICATION_MEMORY_LIMIT",
    "CURIE_PUBLICATION_MEMORY_REQUEST",
    "CURIE_PUBLICATION_NAMESPACE",
    "CURIE_PUBLICATION_OWNER_NAME",
    "CURIE_PUBLICATION_PATCH_MAX_BYTES",
    "CURIE_PUBLICATION_PRIORITY_CLASS_NAME",
    "CURIE_PUBLICATION_PROTECTED_PATHS",
    "CURIE_PUBLICATION_RECONCILE_INTERVAL_SECONDS",
    "CURIE_PUBLICATION_RECONCILE_MAX_ATTEMPTS",
    "CURIE_PUBLICATION_RESULT_MAX_ATTEMPTS",
    "CURIE_PUBLICATION_RETENTION_SECONDS",
    "CURIE_PUBLICATION_SERVICE_ACCOUNT_NAME",
    "CURIE_RUNNER_IMAGE",
    "CURIE_RUNNER_PORT",
    "CURIE_SANDBOX_QUOTA_LIMITS_CPU",
    "CURIE_SANDBOX_QUOTA_LIMITS_MEMORY",
    "CURIE_SANDBOX_QUOTA_REQUESTS_CPU",
    "CURIE_SANDBOX_QUOTA_REQUESTS_MEMORY",
    "CURIE_SEALING_PREVIOUS_PRIVATE_KEY",
    "CURIE_SEALING_PRIVATE_KEY",
    "CURIE_SLACK_IDENTITIES",
    "CURIE_STREAM_RETENTION_MIN_AGE_S",
    "CURIE_TERMINATION_GRACE_PERIOD_S",
    "CURIE_TURN_RECEIPT",
    "CURIE_WARM_POOL",
    "CURIE_WORKER_MAX_CONCURRENCY",
    "CURIE_WORKSPACE_ARCHIVE_TIMEOUT_SECONDS",
    "CURIE_WORKSPACE_BUCKET",
    "CURIE_WORKSPACE_CLONE_TIMEOUT_SECONDS",
    "CURIE_WORKSPACE_ENABLED",
    "CURIE_WORKSPACE_MAX_ARCHIVE_BYTES",
    "CURIE_WORKSPACE_MAX_CHECKOUT_BYTES",
    "CURIE_WORKSPACE_MAX_COMPRESSION_RATIO",
    "CURIE_WORKSPACE_MAX_CONCURRENT_CLONES",
    "CURIE_WORKSPACE_MAX_MEMBERS",
    "CURIE_WORKSPACE_REFERENCE_TTL_SECONDS",
    "CURIE_WORKSPACE_SCRATCH_ROOT",
    "CURIE_WORKSPACE_TOTAL_TIMEOUT_SECONDS",
    "CURIE_WORKSPACE_UPLOAD_TIMEOUT_SECONDS",
    "CURIE_WORK_ITEM_MAX_TURNS",
    "ENVIRONMENT",
    // The factory settings arrived with #4397 as chart values, never extraEnv.
    "FACTORY_CI_WAIT_S",
    "FACTORY_ENABLED",
    "FACTORY_INTAKE",
    "GITHUB_API_URL",
    "GITHUB_APP_ID",
    "GITHUB_APP_PRIVATE_KEY",
    "GITHUB_CLONE_BASE",
    "GITHUB_REPO_ALLOWLIST",
    "GITHUB_REVIEW_INGRESS_ENABLED",
    "GITHUB_REVIEW_RECONCILER_INTERVAL_S",
    "GITHUB_TOKEN",
    "GITHUB_WEBHOOK_SECRET",
    "GIT_CONFIG_COUNT",
    "GIT_CONFIG_KEY_0",
    "GIT_CONFIG_VALUE_0",
    "HOME",
    "LANGFUSE_PUBLIC_KEY",
    "LANGFUSE_SECRET_KEY",
    "LOG_LEVEL",
    "ORG_NAME",
    "RESUME_RECONCILER_BATCH_LIMIT",
    "RESUME_RECONCILER_ENABLED",
    "RESUME_RECONCILER_GRACE_SECONDS",
    "RESUME_RECONCILER_INTERVAL_SECONDS",
    "S3_ACCESS_KEY",
    "S3_SECRET_KEY",
    "SLACK_APP_TOKEN",
    "SLACK_BOT_TOKEN",
    "SLACK_SIGNING_SECRET",
    "SLACK_USERGROUP_CACHE_TTL_S",
    "TRANSCRIPT_MAX_THREAD_BYTES",
    "VALKEY_HOST",
    "VALKEY_PORT",
    "VALKEY_TLS",
];

/// `charts/curie/files/reserved-env.yaml`: workload -> (NAME -> mapping).
fn reserved_env() -> std::collections::BTreeMap<String, std::collections::BTreeMap<String, String>>
{
    let path = chart_dir().join("files/reserved-env.yaml");
    let raw = std::fs::read_to_string(&path)
        .unwrap_or_else(|err| panic!("read {}: {err}", path.display()));
    serde_norway::from_str(&raw).unwrap_or_else(|err| panic!("parse {}: {err}", path.display()))
}

/// A mapping naming exactly one Helm key, as opposed to prose
/// ("the Helm release name") or alternatives ("a / b", "a or b").
fn is_single_helm_key(mapping: &str) -> bool {
    regex::Regex::new(r"^[A-Za-z][A-Za-z0-9_-]*(\.[A-Za-z0-9_-]+)*$")
        .unwrap()
        .is_match(mapping)
}

/// AC6: every single-key reservation is either migrated or consciously listed.
#[test]
fn every_single_key_reservation_has_a_successor_or_is_allowlisted() {
    let reserved = reserved_env();
    let successors = extra_env_successors();
    let mut unaccounted = Vec::new();
    let mut mismatched = Vec::new();
    for (workload, names) in &reserved {
        for (name, mapping) in names {
            if !is_single_helm_key(mapping) {
                continue;
            }
            match successors.iter().find(|(env, _)| env == name) {
                Some((_, key)) if key != mapping => mismatched.push(format!(
                    "{workload}.{name}: successor {key}, reserved {mapping}"
                )),
                Some(_) => {}
                None if RESERVED_WITHOUT_MIGRATION.contains(&name.as_str()) => {}
                None => unaccounted.push(format!("{workload}.{name} -> {mapping}")),
            }
        }
    }
    assert!(
        unaccounted.is_empty(),
        "reserved names with neither an extraEnv successor nor a \
         RESERVED_WITHOUT_MIGRATION entry: {unaccounted:#?}"
    );
    assert!(
        mismatched.is_empty(),
        "a successor must migrate to the key the chart reserves: {mismatched:#?}"
    );
}

/// AC6 hygiene: an allowlisted name must still be reserved, and must not also
/// be a successor, or the allowlist silently stops meaning anything.
#[test]
fn reserved_without_migration_has_no_stale_or_duplicate_entries() {
    let reserved = reserved_env();
    let successors = extra_env_successors();
    for name in RESERVED_WITHOUT_MIGRATION {
        assert!(
            reserved
                .values()
                .any(|names| names.get(*name).is_some_and(|m| is_single_helm_key(m))),
            "{name} is allowlisted but no longer a single-key reservation"
        );
        assert!(
            !successors.iter().any(|(env, _)| env == name),
            "{name} is both allowlisted and a successor"
        );
    }
}
