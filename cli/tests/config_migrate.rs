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

// ---------------------------------------------------------------------------
// #4322: the v0.12.1/v0.12.2 guide told operators to set the factory metadata
// CI policy as `api.extraEnv` GITHUB_FACTORY_METADATA_CI. v0.12.3 made the name
// chart owned (`api.githubFactoryMetadataCi`), so migration must move it.
// ---------------------------------------------------------------------------

const METADATA_CI_ENV: &str = "GITHUB_FACTORY_METADATA_CI";
const METADATA_CI_KEY: &str = "api.githubFactoryMetadataCi";

/// A substring of the operator's JSON value. No refusal or plan line may
/// carry it: change lines name variables and keys, never values.
const METADATA_CI_SENTINEL: &str = "sentinel-4322-do-not-print";

/// A realistic metadata CI policy that satisfies the chart's
/// `api.githubFactoryMetadataCi` schema: owner/name keys, each with
/// non-empty `checks` and `statuses` string arrays.
fn metadata_ci_policy() -> Value {
    json!({
        "acme/widgets": {
            "checks": ["pr-body", METADATA_CI_SENTINEL],
            "statuses": ["ci/lint"]
        },
        "acme/gadgets": {"checks": ["Validate PR title"]}
    })
}

/// The value as an operator wrote it in extraEnv: a JSON document in a string,
/// pretty printed the way the guide's example was.
fn metadata_ci_extra_env_value() -> String {
    serde_json::to_string_pretty(&metadata_ci_policy()).unwrap()
}

fn legacy_metadata_ci_values(entry: Value) -> Value {
    json!({
        "config": {"schemaVersion": "0.9.0"},
        "api": {
            "extraEnv": [
                {"name": "PROVIDER_BASE_URL", "value": "https://provider.example.com/v1"},
                entry,
                {"name": "LOG_FORMAT", "value": "json"}
            ]
        }
    })
}

fn assert_metadata_ci_refusal(values: Value, case: &str) {
    let err = migrate_installed_config(values, Some("curie-0.12.2"))
        .err()
        .unwrap_or_else(|| panic!("{case}: legacy {METADATA_CI_ENV} must refuse to migrate"));
    let message = format!("{err:#}");
    assert!(
        message.contains(METADATA_CI_ENV),
        "{case}: refusal must name the variable: {message}"
    );
    assert!(
        message.contains(METADATA_CI_KEY),
        "{case}: refusal must name the successor key: {message}"
    );
    assert!(
        !message.contains(METADATA_CI_SENTINEL),
        "{case}: refusal must not print the value: {message}"
    );
}

/// AC1.
#[test]
fn metadata_ci_extra_env_migrates_to_object_key() {
    let values = legacy_metadata_ci_values(json!({
        "name": METADATA_CI_ENV,
        "value": metadata_ci_extra_env_value()
    }));
    let outcome = migrate_installed_config(values, Some("curie-0.12.2"))
        .unwrap_or_else(|err| panic!("legacy metadata CI must migrate: {err:#}"));
    assert_eq!(
        extra_env_names(&outcome.values, &["api", "extraEnv"]),
        vec!["PROVIDER_BASE_URL".to_string(), "LOG_FORMAT".to_string()],
        "the legacy entry must go and every other entry must keep its order: {}",
        outcome.values
    );
    assert_eq!(
        outcome.values.pointer("/api/githubFactoryMetadataCi"),
        Some(&metadata_ci_policy()),
        "the successor must hold the parsed object, not the JSON string: {}",
        outcome.values
    );
    let plan = redacted_upgrade_plan(&outcome);
    assert!(
        plan.iter()
            .any(|line| line == &format!("extraEnv {METADATA_CI_ENV} -> {METADATA_CI_KEY}")),
        "the plan must record the move: {plan:?}"
    );
    assert!(
        plan.iter().all(|line| !line.contains(METADATA_CI_SENTINEL)),
        "no plan line may carry the value: {plan:?}"
    );
}

/// AC2: a string that parses but is not an object.
#[test]
fn metadata_ci_extra_env_non_object_json_refuses() {
    let value = serde_json::to_string(&json!([METADATA_CI_SENTINEL])).unwrap();
    assert_metadata_ci_refusal(
        legacy_metadata_ci_values(json!({"name": METADATA_CI_ENV, "value": value})),
        "JSON array",
    );
}

/// AC2: a string that is not JSON at all. serde's own error can quote input, so
/// this pins that the refusal never echoes it.
#[test]
fn metadata_ci_extra_env_non_json_refuses() {
    let value = format!("not json {METADATA_CI_SENTINEL}");
    assert_metadata_ci_refusal(
        legacy_metadata_ci_values(json!({"name": METADATA_CI_ENV, "value": value})),
        "non-JSON string",
    );
}

/// AC2: valueFrom cannot be migrated to an inline object.
#[test]
fn metadata_ci_extra_env_value_from_refuses() {
    let values = legacy_metadata_ci_values(json!({
        "name": METADATA_CI_ENV,
        "valueFrom": {"configMapKeyRef": {"name": METADATA_CI_SENTINEL, "key": "policy"}}
    }));
    let err = migrate_installed_config(values.clone(), Some("curie-0.12.2"))
        .err()
        .unwrap_or_else(|| panic!("valueFrom {METADATA_CI_ENV} must refuse"));
    assert!(format!("{err:#}").contains("valueFrom"), "{err:#}");
    assert_metadata_ci_refusal(values, "valueFrom");
}

/// AC2: an existing successor that disagrees is ambiguous.
#[test]
fn metadata_ci_extra_env_conflicting_key_refuses() {
    let mut values = legacy_metadata_ci_values(json!({
        "name": METADATA_CI_ENV,
        "value": metadata_ci_extra_env_value()
    }));
    values["api"]["githubFactoryMetadataCi"] =
        json!({"acme/widgets": {"checks": ["sentinel-4322-existing"]}});
    let err = migrate_installed_config(values.clone(), Some("curie-0.12.2"))
        .err()
        .unwrap_or_else(|| panic!("a conflicting {METADATA_CI_KEY} must refuse"));
    let message = format!("{err:#}");
    assert!(message.contains("conflicts"), "{message}");
    assert!(
        !message.contains("sentinel-4322-existing"),
        "the existing value must not be printed either: {message}"
    );
    assert_metadata_ci_refusal(values, "conflicting key");
}

/// AC2: an existing successor equal to the parsed value drops the entry.
#[test]
fn metadata_ci_extra_env_equal_key_is_dropped() {
    let mut values = legacy_metadata_ci_values(json!({
        "name": METADATA_CI_ENV,
        "value": metadata_ci_extra_env_value()
    }));
    values["api"]["githubFactoryMetadataCi"] = metadata_ci_policy();
    let outcome = migrate_installed_config(values, Some("curie-0.12.2"))
        .unwrap_or_else(|err| panic!("an equal successor must not refuse: {err:#}"));
    assert_eq!(
        extra_env_names(&outcome.values, &["api", "extraEnv"]),
        vec!["PROVIDER_BASE_URL".to_string(), "LOG_FORMAT".to_string()],
        "{}",
        outcome.values
    );
    assert_eq!(
        outcome.values.pointer("/api/githubFactoryMetadataCi"),
        Some(&metadata_ci_policy())
    );
    let plan = redacted_upgrade_plan(&outcome);
    assert!(
        plan.iter().any(|line| line
            == &format!("extraEnv {METADATA_CI_ENV} dropped; matches {METADATA_CI_KEY}")),
        "{plan:?}"
    );
    assert!(
        plan.iter().all(|line| !line.contains(METADATA_CI_SENTINEL)),
        "{plan:?}"
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

/// The rendered `GITHUB_FACTORY_METADATA_CI` value of the api Deployment's
/// container. A line read, not a YAML parse of the whole render: the render
/// carries duplicate label keys that strict parsers reject (see
/// `chart_fullname_parity.rs`). The scalar itself is parsed as YAML.
fn rendered_api_metadata_ci(render: &str) -> String {
    let api = render
        .split("\n---\n")
        .filter(|doc| {
            doc.contains("# Source: curie/templates/api.yaml") && doc.contains("\nkind: Deployment")
        })
        .collect::<Vec<_>>();
    assert_eq!(api.len(), 1, "expected one api Deployment in the render");
    let lines: Vec<&str> = api[0].lines().collect();
    let found: Vec<String> = lines
        .windows(2)
        .filter(|pair| pair[0].trim() == format!("- name: {METADATA_CI_ENV}"))
        .map(|pair| {
            let scalar = pair[1]
                .trim()
                .strip_prefix("value: ")
                .unwrap_or_else(|| panic!("{METADATA_CI_ENV} must be a plain value: {}", pair[1]));
            serde_norway::from_str::<String>(scalar).expect("env value is a YAML string")
        })
        .collect();
    assert_eq!(
        found.len(),
        1,
        "expected exactly one {METADATA_CI_ENV} in the api Deployment: {found:?}"
    );
    found.into_iter().next().unwrap()
}

/// AC5: the chart renders the migrated overlay, and the api container's env
/// equals the operator's original JSON.
#[test]
fn migrated_metadata_ci_renders_into_the_api_container() {
    if helm_is_absent() {
        return;
    }
    let values = legacy_metadata_ci_values(json!({
        "name": METADATA_CI_ENV,
        "value": metadata_ci_extra_env_value()
    }));
    let outcome = migrate_installed_config(values, Some("curie-0.12.2"))
        .unwrap_or_else(|err| panic!("legacy metadata CI must migrate: {err:#}"));
    let output = helm_template_with_values(&outcome.values);
    assert!(
        output.status.success(),
        "the chart must render the migrated overlay: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let rendered = rendered_api_metadata_ci(&String::from_utf8(output.stdout).unwrap());
    let parsed: Value = serde_json::from_str(&rendered)
        .unwrap_or_else(|err| panic!("{METADATA_CI_ENV} must be JSON ({err}): {rendered}"));
    assert_eq!(parsed, metadata_ci_policy());
}

/// AC5 control: the unmigrated overlay is exactly what the chart refuses, so
/// the positive render above is not passing on a chart that ignores extraEnv.
#[test]
fn unmigrated_metadata_ci_extra_env_is_refused_by_the_chart() {
    if helm_is_absent() {
        return;
    }
    let values = legacy_metadata_ci_values(json!({
        "name": METADATA_CI_ENV,
        "value": metadata_ci_extra_env_value()
    }));
    let output = helm_template_with_values(&values);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        !output.status.success(),
        "the chart must refuse the legacy entry"
    );
    assert!(
        stderr.contains(&format!(
            "api.extraEnv contains chart-owned environment variable {METADATA_CI_ENV}"
        )) && stderr.contains(METADATA_CI_KEY),
        "the refusal must be the chart-owned reservation: {stderr}"
    );
}

/// Reserved names that map to a single Helm key yet need no extraEnv
/// migration: no released guide ever told an operator to set them through
/// extraEnv. Adding a reservation to `charts/curie/files/reserved-env.yaml`
/// forces a choice: a successor in `config_migrate.rs`, or a line here.
const RESERVED_WITHOUT_MIGRATION: &[&str] = &[
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
    "CURIE_GITHUB_API_URL",
    "CURIE_INTERNAL_WORKER_TOKEN",
    "CURIE_MODEL",
    "CURIE_PLUGIN_DIR",
    "CURIE_PUBLICATION_ALLOW_DEPENDENCY_ADDITIONS",
    "CURIE_PUBLICATION_CPU_LIMIT",
    "CURIE_PUBLICATION_CPU_REQUEST",
    "CURIE_PUBLICATION_ENABLED",
    "CURIE_PUBLICATION_EPHEMERAL_LIMIT",
    "CURIE_PUBLICATION_EPHEMERAL_REQUEST",
    "CURIE_PUBLICATION_GITHUB_API_URL",
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
    "GITHUB_API_URL",
    "GITHUB_APP_ID",
    "GITHUB_APP_PRIVATE_KEY",
    "GITHUB_CLONE_BASE",
    "GITHUB_FACTORY_BASES",
    "GITHUB_FACTORY_CARD_BASE_URL",
    "GITHUB_FACTORY_CI_WAIT_S",
    "GITHUB_FACTORY_INGRESS_ENABLED",
    "GITHUB_FACTORY_INTAKE",
    "GITHUB_FACTORY_LABEL",
    "GITHUB_FACTORY_MENTION",
    "GITHUB_FACTORY_POLL_INTERVAL_S",
    "GITHUB_FACTORY_PYTHON_CI",
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
