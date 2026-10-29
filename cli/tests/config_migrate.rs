//! Versioned installed-configuration migrations (issue #2299).
//!
//! Fixtures under `data/upgrade-config/` are user-supplied Helm values from
//! released v0.8.x charts, not current-chart defaults.

use curie::config_migrate::{
    extra_env_successors, migrate_installed_config, redacted_upgrade_plan, FactoryMigrationContext,
    MigrationOutcome, ResolvedFactoryRepository, TARGET_SCHEMA_VERSION,
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
        let outcome =
            migrate_installed_config(values, Some(&chart), &FactoryMigrationContext::NotRequired)
                .unwrap_or_else(|err| {
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
    let outcome = migrate_installed_config(
        values,
        Some("curie-0.8.0"),
        &FactoryMigrationContext::NotRequired,
    )
    .unwrap();
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
    let outcome = migrate_installed_config(
        values,
        Some("curie-0.8.4"),
        &FactoryMigrationContext::NotRequired,
    )
    .unwrap();
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
    let err = migrate_installed_config(
        values,
        Some("curie-0.8.4"),
        &FactoryMigrationContext::NotRequired,
    )
    .unwrap_err();
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
    let err = migrate_installed_config(
        values,
        Some("curie-0.8.4"),
        &FactoryMigrationContext::NotRequired,
    )
    .unwrap_err();
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
    let outcome = migrate_installed_config(
        values,
        Some("curie-0.8.4"),
        &FactoryMigrationContext::NotRequired,
    )
    .unwrap();
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
    let first = migrate_installed_config(
        values,
        Some("curie-0.8.4"),
        &FactoryMigrationContext::NotRequired,
    )
    .unwrap();
    let second = migrate_installed_config(
        first.values.clone(),
        Some("curie-0.9.0"),
        &FactoryMigrationContext::NotRequired,
    )
    .unwrap();
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
    let outcome = migrate_installed_config(
        values,
        Some("curie-0.8.5"),
        &FactoryMigrationContext::NotRequired,
    )
    .unwrap();
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
    let err = migrate_installed_config(
        values,
        Some("curie-0.7.3"),
        &FactoryMigrationContext::NotRequired,
    )
    .unwrap_err();
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
    let outcome =
        migrate_installed_config(values.clone(), None, &FactoryMigrationContext::NotRequired)
            .unwrap();
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

const LEGACY_FACTORY_KEYS: &[&str] = &[
    "githubFactoryIngressEnabled",
    "githubFactoryIntake",
    "githubFactoryPollIntervalSeconds",
    "githubFactoryLabel",
    "githubFactoryMention",
    "githubFactoryCardBaseUrl",
    "githubFactoryCiWaitSeconds",
    "githubFactoryPythonCi",
    "githubFactoryBases",
    "githubFactoryMetadataCi",
];

pub(super) fn legacy_factory_values() -> Value {
    json!({
        "config": {"schemaVersion": "0.9.0"},
        "api": {
            "githubFactoryIngressEnabled": true,
            "githubFactoryIntake": "webhook",
            "githubFactoryPollIntervalSeconds": 83,
            "githubFactoryLabel": "triage-ready",
            "githubFactoryMention": "acme-maintainer",
            "githubFactoryCardBaseUrl": "https://cards.example.com/factory",
            "githubFactoryCiWaitSeconds": 2345,
            "githubFactoryBases": {
                "ACME-CORP/ACME-BOT": {
                    "bases": ["release/2026", "main"],
                    "default_base": "release/2026"
                }
            },
            "githubFactoryPythonCi": {
                "Acme-Corp/Acme-Bot": {
                    "check": "required-python",
                    "paths": ["apps", "packages/python"],
                    "pendingCheckPrefix": "python-shard-"
                }
            },
            "githubFactoryMetadataCi": {
                "acme-corp/acme-bot": {
                    "checks": ["body-check", "fix-pin"],
                    "statuses": ["approval", "status:retained-context"]
                }
            },
            "githubAppId": "17",
            "githubAppExistingSecret": "acme-factory-app",
            "githubAppExistingSecretKey": "signing.pem",
            "githubRepoAllowlist": ["acme-corp/acme-bot"],
            "githubApiUrl": "https://api.github.com",
            "githubCloneBase": "https://github.com"
        }
    })
}

pub(super) fn factory_context(values: &Value) -> FactoryMigrationContext {
    FactoryMigrationContext::LegacyGitHub {
        effective_legacy: values.clone(),
        repositories: vec![ResolvedFactoryRepository {
            path: "acme-corp/acme-bot".into(),
            project_id: "73".into(),
        }],
    }
}

fn migrated_factory(values: Value) -> MigrationOutcome {
    let context = factory_context(&values);
    migrate_installed_config(values, Some("curie-0.12.5"), &context)
        .expect("configured legacy factory migrates")
}

fn only_github_entry<'a>(values: &'a Value, section: &str) -> &'a Value {
    let entries: Vec<_> = values["factory"][section]
        .as_array()
        .expect("typed factory entries")
        .iter()
        .filter(|entry| entry["kind"] == "github" && entry["host"] == "github.com")
        .collect();
    assert_eq!(entries.len(), 1, "one GitHub connection per section");
    entries[0]
}

fn only_github_repo(values: &Value) -> (&Value, &Value) {
    let bindings: Vec<_> = values["factory"]["bindings"]
        .as_array()
        .expect("typed bindings")
        .iter()
        .filter(|binding| binding["trackerScopeId"] == "73")
        .collect();
    assert_eq!(bindings.len(), 1, "one binding for the immutable scope");
    let repos = bindings[0]["repos"].as_array().expect("bound repositories");
    assert_eq!(
        repos.len(),
        1,
        "native intake stays scoped to one repository"
    );
    (bindings[0], &repos[0])
}

fn assert_legacy_factory_removed(values: &Value) {
    for key in LEGACY_FACTORY_KEYS {
        assert!(
            values["api"].get(key).is_none(),
            "api.{key} must be removed"
        );
    }
}

fn assert_factory_error(values: Value, setting: &str, forbidden: &[&str]) {
    let context = factory_context(&values);
    let error = migrate_installed_config(values, Some("curie-0.12.5"), &context)
        .expect_err("invalid legacy configuration must refuse");
    let rendered = format!("{error}\n{error:#}\n{error:?}");
    assert!(
        rendered.contains(setting),
        "error must name {setting}: {rendered}"
    );
    for value in forbidden {
        assert!(!rendered.contains(value), "error must omit input values");
    }
    for value in [
        "acme-corp/acme-bot",
        "ACME-CORP/ACME-BOT",
        "triage-ready",
        "acme-maintainer",
        "acme-factory-app",
        "required-python",
        "release/2026",
        "secret-sentinel-4628",
    ] {
        assert!(!rendered.contains(value), "error must name settings only");
    }
}

fn equal_typed_factory() -> Value {
    let credential = json!({
        "type": "github_app",
        "appId": "17",
        "existingSecret": "acme-factory-app",
        "existingSecretKey": "signing.pem"
    });
    json!({
        "enabled": true,
        "intake": "webhook",
        "cardBaseUrl": "https://cards.example.com/factory",
        "ciWaitSeconds": 2345,
        "codeHosts": [{
            "name": "retained-code",
            "kind": "github",
            "host": "github.com",
            "apiUrl": "https://api.github.com",
            "credential": credential.clone()
        }],
        "trackers": [{
            "name": "retained-issues",
            "kind": "github",
            "host": "github.com",
            "apiUrl": "https://api.github.com",
            "credential": credential,
            "label": "triage-ready",
            "mention": "acme-maintainer",
            "pollIntervalSeconds": 83
        }],
        "bindings": [{
            "name": "retained-work",
            "tracker": "retained-issues",
            "trackerScopeId": "73",
            "startAuthority": {"type": "write_check"},
            "repos": [{
                "alias": "bot-source",
                "codeHost": "retained-code",
                "projectId": "73",
                "path": "acme-corp/acme-bot",
                "bases": ["release/2026", "main"],
                "defaultBase": "release/2026",
                "ci": {
                    "required": {
                        "key": "required-python",
                        "paths": ["apps", "packages/python"],
                        "pendingKeyPrefix": "python-shard-"
                    },
                    "metadataRerunKeys": [
                        "body-check", "fix-pin", "status:approval",
                        "status:status:retained-context"
                    ]
                }
            }]
        }]
    })
}

fn retained_repository_runner() -> Value {
    json!({
        "image": format!("registry.example.com/acme/runner@sha256:{}", "a".repeat(64)),
        "base": format!("sha256:{}", "b".repeat(64)),
        "platformDigests": {
            "linux/amd64": format!("sha256:{}", "c".repeat(64)),
            "linux/arm64": format!("sha256:{}", "d".repeat(64))
        },
        "tools": ["uv", "pnpm"]
    })
}

#[test]
fn factory_migrates_all_ten_nondefault_keys_with_native_identity() {
    let source = legacy_factory_values();
    let outcome = migrated_factory(source.clone());
    assert_eq!(outcome.schema_version, "0.9.0");
    assert_eq!(outcome.values["config"], source["config"]);
    assert_legacy_factory_removed(&outcome.values);
    assert_eq!(outcome.values["factory"]["enabled"], true);
    assert_eq!(outcome.values["factory"]["intake"], "webhook");
    assert_eq!(outcome.values["factory"]["ciWaitSeconds"], 2345);
    assert_eq!(
        outcome.values["factory"]["cardBaseUrl"],
        "https://cards.example.com/factory"
    );
    let host = only_github_entry(&outcome.values, "codeHosts");
    let tracker = only_github_entry(&outcome.values, "trackers");
    let expected_credential = equal_typed_factory()["codeHosts"][0]["credential"].clone();
    assert_eq!(host["apiUrl"], "https://api.github.com");
    assert_eq!(host["credential"], expected_credential);
    assert_eq!(tracker["credential"], expected_credential);
    assert_eq!(tracker["apiUrl"], host["apiUrl"]);
    assert_eq!(tracker["label"], "triage-ready");
    assert_eq!(tracker["mention"], "acme-maintainer");
    assert_eq!(tracker["pollIntervalSeconds"], 83);
    let (binding, repo) = only_github_repo(&outcome.values);
    assert_eq!(binding["tracker"], tracker["name"]);
    assert_eq!(repo["codeHost"], host["name"]);
    assert_eq!(repo["projectId"], binding["trackerScopeId"]);
    assert_eq!(repo["path"], "acme-corp/acme-bot");
    assert_eq!(repo["bases"], json!(["release/2026", "main"]));
    assert_eq!(repo["defaultBase"], "release/2026");
    assert_eq!(
        repo["ci"],
        equal_typed_factory()["bindings"][0]["repos"][0]["ci"]
    );
    assert_eq!(
        binding
            .get("startAuthority")
            .cloned()
            .unwrap_or_else(|| json!({"type": "write_check"})),
        json!({"type": "write_check"})
    );
    let mut retained_api = source["api"].clone();
    for key in LEGACY_FACTORY_KEYS {
        retained_api.as_object_mut().unwrap().remove(*key);
    }
    assert_eq!(outcome.values["api"], retained_api);
    let plan = redacted_upgrade_plan(&outcome).join("\n");
    for key in LEGACY_FACTORY_KEYS {
        assert!(
            plan.contains(&format!("api.{key}")),
            "plan must name api.{key}"
        );
    }
    for value in [
        "acme-corp/acme-bot",
        "triage-ready",
        "acme-maintainer",
        "acme-factory-app",
        "required-python",
        "cards.example.com",
        "2345",
    ] {
        assert!(!plan.contains(value), "plan must omit operator values");
    }
}

#[test]
fn legacy_factory_without_required_ci_validates_chart_schema() {
    let schema: Value = serde_json::from_str(include_str!("../../charts/curie/values.schema.json"))
        .expect("committed chart values schema");
    let validator = jsonschema::validator_for(&schema).expect("chart values schema compiles");
    for retained_binding in [false, true] {
        for metadata_only in [false, true] {
            let mut values = legacy_factory_values();
            let api = values["api"].as_object_mut().unwrap();
            api.remove("githubFactoryPythonCi");
            if !metadata_only {
                api.remove("githubFactoryMetadataCi");
            }
            let mut typed = equal_typed_factory();
            let retained_repo = &mut typed["bindings"][0]["repos"][0];
            if metadata_only {
                retained_repo["ci"]
                    .as_object_mut()
                    .unwrap()
                    .remove("required");
            } else {
                retained_repo.as_object_mut().unwrap().remove("ci");
            }
            if retained_binding {
                values["factory"] = typed.clone();
            }
            let outcome = migrated_factory(values);
            validator
                .validate(&outcome.values)
                .expect("full migrated values without required CI must satisfy the chart schema");
            assert_legacy_factory_removed(&outcome.values);
            if retained_binding {
                assert_eq!(outcome.values["factory"], typed);
            }
            let (_, repo) = only_github_repo(&outcome.values);
            if metadata_only {
                let ci = repo.get("ci").expect("metadata-only CI policy is retained");
                assert!(ci.get("required").is_none());
                assert_eq!(
                    ci["metadataRerunKeys"],
                    json!([
                        "body-check",
                        "fix-pin",
                        "status:approval",
                        "status:status:retained-context"
                    ])
                );
            } else {
                assert!(repo.get("ci").is_none());
            }
        }
    }
}

#[test]
fn legacy_factory_without_ci_refuses_retained_required_ci_with_setting_names_only() {
    let mut values = legacy_factory_values();
    let api = values["api"].as_object_mut().unwrap();
    api.remove("githubFactoryPythonCi");
    api.remove("githubFactoryMetadataCi");
    values["factory"] = equal_typed_factory();
    values["factory"]["bindings"][0]["repos"][0]["ci"] = json!({
        "required": {
            "key": "required-ci-sentinel-4753",
            "paths": ["required-path-sentinel-4753"],
            "pendingKeyPrefix": "pending-test"
        }
    });
    assert_factory_error(
        values,
        "factory.bindings",
        &[
            "required-ci-sentinel-4753",
            "required-path-sentinel-4753",
            "pending-test",
        ],
    );
}

#[test]
fn legacy_factory_without_ci_refuses_retained_metadata_ci_with_setting_names_only() {
    let mut values = legacy_factory_values();
    let api = values["api"].as_object_mut().unwrap();
    api.remove("githubFactoryPythonCi");
    api.remove("githubFactoryMetadataCi");
    values["factory"] = equal_typed_factory();
    values["factory"]["bindings"][0]["repos"][0]["ci"] = json!({
        "metadataRerunKeys": ["metadata-ci-sentinel-4753", "status:metadata-status-sentinel-4753"]
    });
    assert_factory_error(
        values,
        "factory.bindings",
        &["metadata-ci-sentinel-4753", "metadata-status-sentinel-4753"],
    );
}

#[test]
fn legacy_factory_metadata_only_refuses_retained_required_ci_with_setting_names_only() {
    let mut values = legacy_factory_values();
    values["api"]
        .as_object_mut()
        .unwrap()
        .remove("githubFactoryPythonCi");
    values["factory"] = equal_typed_factory();
    values["factory"]["bindings"][0]["repos"][0]["ci"]["required"] = json!({
        "key": "required-ci-sentinel-4753",
        "paths": ["required-path-sentinel-4753"],
        "pendingKeyPrefix": "pending-test"
    });
    assert_factory_error(
        values,
        "factory.bindings",
        &[
            "required-ci-sentinel-4753",
            "required-path-sentinel-4753",
            "pending-test",
            "body-check",
            "fix-pin",
            "status:approval",
            "status:status:retained-context",
        ],
    );
}

#[test]
fn factory_migration_reuses_equal_typed_entries_with_arbitrary_names() {
    let mut values = legacy_factory_values();
    let typed = equal_typed_factory();
    values["factory"] = typed.clone();
    let outcome = migrated_factory(values);
    assert_eq!(outcome.values["factory"], typed);
    assert_legacy_factory_removed(&outcome.values);
    let second = migrate_installed_config(
        outcome.values.clone(),
        Some("curie-0.13.0"),
        &FactoryMigrationContext::NotRequired,
    )
    .expect("typed configuration is already migrated");
    assert_eq!(second.values, outcome.values);
    assert!(second
        .changes
        .iter()
        .all(|line| !line.contains("api.githubFactory")));
}

#[test]
fn factory_migration_reuses_equal_numeric_typed_identities_without_changing_types() {
    for scope_id in [json!(73), json!(73.0)] {
        for project_id in [json!(73), json!(73.0)] {
            let mut values = legacy_factory_values();
            let mut typed = equal_typed_factory();
            typed["bindings"][0]["trackerScopeId"] = scope_id.clone();
            typed["bindings"][0]["repos"][0]["projectId"] = project_id;
            values["factory"] = typed.clone();
            let outcome = migrated_factory(values);
            assert_eq!(outcome.values["factory"], typed);
            assert_legacy_factory_removed(&outcome.values);
            let second = migrate_installed_config(
                outcome.values.clone(),
                Some("curie-0.13.0"),
                &FactoryMigrationContext::NotRequired,
            )
            .expect("numeric typed identities are already migrated");
            assert_eq!(second.values, outcome.values);
            assert!(second
                .changes
                .iter()
                .all(|line| !line.contains("api.githubFactory")));
        }
    }
}

#[test]
fn factory_migration_preserves_unrelated_numeric_bindings() {
    for id in [json!(91), json!(91.0)] {
        let mut values = legacy_factory_values();
        let mut typed = equal_typed_factory();
        let mut retained = typed["bindings"][0].clone();
        retained["trackerScopeId"] = id.clone();
        retained["repos"][0]["projectId"] = id;
        retained["repos"][0]["path"] = json!("acme-corp/other");
        retained["repos"][0]["runner"] = retained_repository_runner();
        values["factory"] = typed.clone();
        values["factory"]["bindings"] = json!([retained.clone()]);
        let mut generated = typed["bindings"][0].clone();
        generated["name"] = json!("github-73");
        generated["repos"][0]["alias"] = json!("repository");
        typed["bindings"] = json!([retained, generated]);
        let outcome = migrated_factory(values);
        assert_eq!(outcome.values["factory"], typed);
        assert_legacy_factory_removed(&outcome.values);
    }
}

#[test]
fn factory_preserves_unrelated_settings_secrets_agents_and_repository_runners() {
    let mut values = legacy_factory_values();
    values["factory"] = equal_typed_factory();
    values["factory"]["sandboxEgress"] = json!({"acme-bot": true, "acme-review": true});
    values["factory"]["bindings"][0]["repos"][0]["runner"] = retained_repository_runner();
    values["factory"]["codeHosts"][0]["egress"] = json!([{
        "cidr": "192.0.2.0/24", "ports": [{"port": 443, "protocol": "TCP"}]
    }]);
    values["factory"]["trackers"][0]["egress"] =
        values["factory"]["codeHosts"][0]["egress"].clone();
    let context = factory_context(&values);
    values["api"]["githubAppPrivateKey"] = json!("secret-sentinel-4628");
    values["api"]["githubTokenExistingSecret"] = json!("acme-clone");
    values["api"]["githubTokenExistingSecretKey"] = json!("token");
    values["api"]["githubToken"] = json!("secret-sentinel-4628");
    values["api"]["githubWebhookSecret"] = json!("retained-webhook-sentinel-4628");
    values["api"]["githubReviewIngressEnabled"] = json!(false);
    values["api"]["replicas"] = json!(3);
    values["agentSandbox"] = json!({
        "runner": {"model": "claude-sonnet-4-6", "credentialsExistingSecret": "acme-model",
            "budget": {"maxOutputTokensPerRun": 12000, "maxUsdPerDay": 3.5}},
        "runnerImages": {"acme-bot": "ghcr.io/acme/acme-bot-runner:example"}
    });
    values["worker"] = json!({"concurrency": 7, "runnerTotalTimeoutSeconds": 901});
    values["resourceQuota"] = json!({"enabled": true,
        "hard": {"requestsCpu": "250m", "limitsMemory": "3Gi"}});
    let outcome = migrate_installed_config(values.clone(), Some("curie-0.12.5"), &context).unwrap();
    assert_eq!(outcome.values["factory"], values["factory"]);
    for key in ["agentSandbox", "worker", "resourceQuota"] {
        assert_eq!(outcome.values[key], values[key], "preserve {key}");
    }
    for key in [
        "githubAppExistingSecret",
        "githubAppExistingSecretKey",
        "githubTokenExistingSecret",
        "githubTokenExistingSecretKey",
        "githubRepoAllowlist",
        "githubApiUrl",
        "githubCloneBase",
        "githubWebhookSecret",
        "githubReviewIngressEnabled",
        "replicas",
    ] {
        assert_eq!(
            outcome.values["api"][key], values["api"][key],
            "preserve api.{key}"
        );
    }
    assert!(outcome.values["api"].get("githubAppPrivateKey").is_none());
    assert!(outcome.values["api"].get("githubToken").is_none());
    assert_redacted(&outcome, &["secret-sentinel-4628"]);
    assert!(!redacted_upgrade_plan(&outcome)
        .join("\n")
        .contains("retained-webhook-sentinel-4628"));
}

#[test]
fn factory_changed_lists_retain_every_unrelated_entry_and_order() {
    let mut values = legacy_factory_values();
    let host = json!({
        "name": "other-code", "kind": "gitlab", "host": "gitlab.example.com",
        "apiUrl": "https://gitlab.example.com/api/v4",
        "credential": {"type": "static_token", "existingSecret": "acme-gitlab", "existingSecretKey": "token"},
        "egress": [{"cidr": "198.51.100.0/24", "ports": [{"port": 443, "protocol": "TCP"}]}]
    });
    let tracker = json!({
        "name": "other-issues", "kind": "gitlab", "host": "gitlab.example.com",
        "apiUrl": "https://gitlab.example.com/api/v4", "label": "queued", "mention": "reviewer",
        "credential": host["credential"].clone(), "pollIntervalSeconds": 59
    });
    let binding = json!({
        "name": "other-work", "tracker": "other-issues", "trackerScopeId": "91",
        "repos": [{"alias": "other", "codeHost": "other-code", "projectId": "91",
            "path": "acme-corp/other", "bases": ["stable", "main"], "defaultBase": "stable",
            "runner": retained_repository_runner()}]
    });
    let mut second_host = host.clone();
    second_host["name"] = json!("other-code-two");
    second_host["host"] = json!("gitlab-two.example.com");
    second_host["apiUrl"] = json!("https://gitlab-two.example.com/api/v4");
    let mut second_tracker = tracker.clone();
    second_tracker["name"] = json!("other-issues-two");
    second_tracker["host"] = second_host["host"].clone();
    second_tracker["apiUrl"] = second_host["apiUrl"].clone();
    let mut second_binding = binding.clone();
    second_binding["name"] = json!("other-work-two");
    second_binding["tracker"] = json!("other-issues-two");
    second_binding["trackerScopeId"] = json!("92");
    second_binding["repos"][0]["codeHost"] = json!("other-code-two");
    second_binding["repos"][0]["projectId"] = json!("92");
    values["factory"] = json!({"codeHosts": [host, second_host], "trackers": [tracker, second_tracker],
        "bindings": [binding, second_binding],
        "sandboxEgress": {"acme-bot": true, "acme-other": true}});
    let retained = values["factory"].clone();
    let outcome = migrated_factory(values);
    for section in ["codeHosts", "trackers", "bindings"] {
        let entries = outcome.values["factory"][section].as_array().unwrap();
        assert_eq!(entries.len(), 3, "retain unrelated {section}");
        let unrelated: Vec<_> = entries
            .iter()
            .filter(|entry| {
                retained[section]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|old| old["name"] == entry["name"])
            })
            .collect();
        assert_eq!(
            unrelated,
            retained[section]
                .as_array()
                .unwrap()
                .iter()
                .collect::<Vec<_>>()
        );
    }
    assert_eq!(
        outcome.values["factory"]["sandboxEgress"],
        retained["sandboxEgress"]
    );
    let (_, repo) = only_github_repo(&outcome.values);
    assert_eq!(repo["bases"], json!(["release/2026", "main"]));
}

#[test]
fn factory_fresh_typed_install_is_unchanged() {
    let values = json!({"config": {"schemaVersion": "0.9.0"}, "factory": equal_typed_factory(),
        "api": {"githubAppId": "17", "githubCloneBase": "https://github.com"}});
    let outcome = migrate_installed_config(
        values.clone(),
        Some("curie-0.13.0"),
        &FactoryMigrationContext::NotRequired,
    )
    .unwrap();
    assert_eq!(outcome.values, values);
    assert!(outcome
        .changes
        .iter()
        .all(|line| !line.contains("api.githubFactory")));
}

#[test]
fn factory_default_only_supplied_keys_strip_without_creating_blank_connections() {
    let values = json!({"config": {"schemaVersion": "0.9.0"}, "api": {
        "githubFactoryIngressEnabled": false, "githubFactoryIntake": "poll",
        "githubFactoryPollIntervalSeconds": 45, "githubFactoryLabel": "",
        "githubFactoryMention": "", "githubFactoryCardBaseUrl": "",
        "githubFactoryCiWaitSeconds": 1200, "githubFactoryBases": {},
        "githubFactoryPythonCi": {}, "githubFactoryMetadataCi": {}
    }});
    let context = FactoryMigrationContext::LegacyGitHub {
        effective_legacy: values.clone(),
        repositories: vec![],
    };
    let outcome = migrate_installed_config(values, Some("curie-0.12.5"), &context).unwrap();
    assert_legacy_factory_removed(&outcome.values);
    assert_eq!(outcome.values["factory"]["enabled"], false);
    assert_eq!(outcome.values["factory"]["intake"], "poll");
    assert_eq!(outcome.values["factory"]["ciWaitSeconds"], 1200);
    assert_eq!(outcome.values["factory"]["cardBaseUrl"], "");
    for section in ["codeHosts", "trackers", "bindings"] {
        assert!(
            outcome.values["factory"]
                .get(section)
                .is_none_or(|v| v == &json!([])),
            "disabled defaults must not create {section}"
        );
    }
}

#[test]
fn factory_computed_only_defaults_do_not_become_supplied_intent() {
    let values = json!({"config": {"schemaVersion": "0.9.0"}, "api": {"replicas": 2}});
    let outcome = migrate_installed_config(
        values.clone(),
        Some("curie-0.12.5"),
        &FactoryMigrationContext::NotRequired,
    )
    .unwrap();
    assert_eq!(outcome.values, values);
    assert!(outcome.values.get("factory").is_none());
    assert!(outcome.changes.is_empty());
}

#[test]
fn factory_null_only_legacy_keys_are_removed_without_creating_factory() {
    let mut values = json!({"config": {"schemaVersion": "0.9.0"}, "api": {"replicas": 2}});
    for key in LEGACY_FACTORY_KEYS {
        values["api"][*key] = Value::Null;
    }
    let outcome = migrate_installed_config(
        values,
        Some("curie-0.12.5"),
        &FactoryMigrationContext::NotRequired,
    )
    .unwrap();
    assert_legacy_factory_removed(&outcome.values);
    assert_eq!(outcome.values["api"], json!({"replicas": 2}));
    assert!(outcome.values.get("factory").is_none());
}

#[test]
fn factory_explicit_false_keeps_configured_policies_and_connections() {
    let mut values = legacy_factory_values();
    values["api"]["githubFactoryIngressEnabled"] = json!(false);
    let outcome = migrated_factory(values);
    assert_eq!(outcome.values["factory"]["enabled"], false);
    assert_eq!(
        only_github_entry(&outcome.values, "trackers")["label"],
        "triage-ready"
    );
    let (_, repo) = only_github_repo(&outcome.values);
    assert_eq!(
        repo["ci"],
        equal_typed_factory()["bindings"][0]["repos"][0]["ci"]
    );
    assert_legacy_factory_removed(&outcome.values);
}

#[test]
fn factory_supplied_legacy_input_requires_an_explicit_context() {
    for values in [
        legacy_factory_values(),
        json!({"config": {"schemaVersion": "0.9.0"}, "api": {"githubFactoryIngressEnabled": false}}),
        json!({"config": {"schemaVersion": "0.9.0"}, "api": {"extraEnv": [{
            "name": "GITHUB_FACTORY_METADATA_CI", "value": "secret-sentinel-4628"
        }]}}),
    ] {
        let error = migrate_installed_config(
            values,
            Some("curie-0.12.5"),
            &FactoryMigrationContext::NotRequired,
        )
        .expect_err("supplied legacy input cannot bypass factory migration");
        let message = format!("{error:#}\n{error:?}");
        assert!(message.contains("api."));
        assert!(!message.contains("secret-sentinel-4628"));
    }
}

#[test]
fn factory_disabled_scalar_intent_does_not_copy_computed_defaults() {
    let supplied = json!({"config": {"schemaVersion": "0.9.0"},
        "api": {"githubFactoryIngressEnabled": false, "replicas": 2}});
    let context = FactoryMigrationContext::LegacyGitHub {
        effective_legacy: json!({"api": {
            "githubFactoryIngressEnabled": false, "githubFactoryIntake": "poll",
            "githubFactoryPollIntervalSeconds": 45, "githubFactoryLabel": "",
            "githubFactoryMention": "", "githubFactoryCardBaseUrl": "",
            "githubFactoryCiWaitSeconds": 1200, "githubFactoryBases": {},
            "githubFactoryPythonCi": {}, "githubFactoryMetadataCi": {}
        }}),
        repositories: vec![],
    };
    let outcome = migrate_installed_config(supplied, Some("curie-0.12.5"), &context).unwrap();
    assert_eq!(outcome.values["factory"], json!({"enabled": false}));
    assert_eq!(outcome.values["api"], json!({"replicas": 2}));
}

#[test]
fn factory_uses_source_effective_settings_without_merging_them_again() {
    let effective = legacy_factory_values();
    let mut supplied = effective.clone();
    for key in LEGACY_FACTORY_KEYS
        .iter()
        .filter(|key| **key != "githubFactoryIngressEnabled")
    {
        supplied["api"].as_object_mut().unwrap().remove(*key);
    }
    supplied["worker"] = json!({"concurrency": 9});
    let context = factory_context(&effective);
    let outcome = migrate_installed_config(supplied, Some("curie-0.12.5"), &context).unwrap();
    let tracker = only_github_entry(&outcome.values, "trackers");
    assert_eq!(tracker["label"], effective["api"]["githubFactoryLabel"]);
    assert_eq!(tracker["pollIntervalSeconds"], 83);
    assert_eq!(outcome.values["factory"]["ciWaitSeconds"], 2345);
    assert_eq!(outcome.values["worker"], json!({"concurrency": 9}));
    assert_legacy_factory_removed(&outcome.values);
}

#[test]
fn factory_disabled_policy_still_requires_resolved_identity() {
    let mut values = legacy_factory_values();
    values["api"]["githubFactoryIngressEnabled"] = json!(false);
    let context = FactoryMigrationContext::LegacyGitHub {
        effective_legacy: values.clone(),
        repositories: vec![],
    };
    let error = migrate_installed_config(values, Some("curie-0.12.5"), &context)
        .expect_err("disabled configured policy must not disappear");
    let message = format!("{error:#}\n{error:?}");
    assert!(message.contains("api.githubRepoAllowlist") || message.contains("factory.bindings"));
    assert!(!message.contains("acme-corp/acme-bot"));
}

#[test]
fn factory_poll_intake_and_source_poll_default_are_preserved() {
    let mut values = legacy_factory_values();
    values["api"]["githubFactoryIntake"] = json!("poll");
    values["api"]
        .as_object_mut()
        .unwrap()
        .remove("githubFactoryPollIntervalSeconds");
    let outcome = migrated_factory(values);
    assert_eq!(outcome.values["factory"]["intake"], "poll");
    assert_eq!(
        only_github_entry(&outcome.values, "trackers")["pollIntervalSeconds"],
        45
    );
}

#[test]
fn factory_optional_default_base_and_pending_prefix_remain_optional() {
    for explicit_null in [false, true] {
        let mut values = legacy_factory_values();
        let bases = &mut values["api"]["githubFactoryBases"]["ACME-CORP/ACME-BOT"];
        bases.as_object_mut().unwrap().remove("default_base");
        if explicit_null {
            bases["default_base"] = Value::Null;
        }
        let python = &mut values["api"]["githubFactoryPythonCi"]["Acme-Corp/Acme-Bot"];
        python.as_object_mut().unwrap().remove("pendingCheckPrefix");
        if explicit_null {
            python["pendingCheckPrefix"] = Value::Null;
        }
        let outcome = migrated_factory(values);
        let (_, repo) = only_github_repo(&outcome.values);
        assert_eq!(repo["bases"], json!(["release/2026", "main"]));
        assert!(
            repo.get("defaultBase").is_none_or(Value::is_null),
            "an omitted default means the repository default"
        );
        assert!(repo["ci"]["required"]
            .get("pendingKeyPrefix")
            .is_none_or(Value::is_null));
    }
}

#[test]
fn factory_metadata_checks_statuses_and_duplicate_names_keep_distinct_meanings() {
    for (policy, expected) in [
        (
            json!({"checks": ["body-check", "fix-pin"]}),
            vec!["body-check", "fix-pin"],
        ),
        (
            json!({"statuses": ["approval", "status:approval"]}),
            vec!["status:approval", "status:status:approval"],
        ),
        (
            json!({"checks": ["build"], "statuses": ["build"]}),
            vec!["build", "status:build"],
        ),
        (
            json!({"checks": ["body-check", "body-check"], "statuses": ["approval", "approval"]}),
            vec!["body-check", "status:approval"],
        ),
    ] {
        let mut values = legacy_factory_values();
        values["api"]["githubFactoryMetadataCi"] = json!({"acme-corp/acme-bot": policy});
        let outcome = migrated_factory(values);
        let (_, repo) = only_github_repo(&outcome.values);
        let keys = repo["ci"]["metadataRerunKeys"].as_array().unwrap();
        let actual: std::collections::BTreeSet<_> =
            keys.iter().map(|key| key.as_str().unwrap()).collect();
        let expected: std::collections::BTreeSet<_> = expected.into_iter().collect();
        assert_eq!(
            keys.len(),
            expected.len(),
            "typed metadata keys must be unique"
        );
        assert_eq!(actual, expected);
    }
}

#[test]
fn factory_metadata_check_names_with_status_prefix_are_refused() {
    for policy in [
        json!({"checks": ["status:secret-sentinel-4628"]}),
        json!({"checks": ["status:secret-sentinel-4628"], "statuses": ["secret-sentinel-4628"]}),
    ] {
        let mut values = legacy_factory_values();
        values["api"]["githubFactoryMetadataCi"] = json!({"acme-corp/acme-bot": policy});
        assert_factory_error(values, "api.githubFactoryMetadataCi", &[]);
    }
}

#[test]
fn factory_equal_casefolded_policy_keys_coalesce_without_losing_branch_case() {
    let mut values = legacy_factory_values();
    values["api"]["githubFactoryBases"]["ACME-CORP/ACME-BOT"] = json!({
        "bases": ["Release/October", "main"], "default_base": "Release/October"
    });
    for key in [
        "githubFactoryBases",
        "githubFactoryPythonCi",
        "githubFactoryMetadataCi",
    ] {
        let policy = values["api"][key]
            .as_object()
            .unwrap()
            .values()
            .next()
            .unwrap()
            .clone();
        values["api"][key]["aCME-cORP/aCME-bOT"] = policy;
    }
    let outcome = migrated_factory(values);
    let (_, repo) = only_github_repo(&outcome.values);
    assert_eq!(repo["bases"], json!(["Release/October", "main"]));
    assert_eq!(repo["defaultBase"], "Release/October");
    assert_eq!(repo["ci"]["required"]["key"], "required-python");
}

#[test]
fn factory_conflicting_casefolded_policy_keys_refuse_without_values() {
    for (key, policy) in [
        (
            "githubFactoryBases",
            json!({"bases": ["secret-sentinel-4628"]}),
        ),
        (
            "githubFactoryPythonCi",
            json!({"check": "secret-sentinel-4628", "paths": ["apps"]}),
        ),
        (
            "githubFactoryMetadataCi",
            json!({"checks": ["secret-sentinel-4628"]}),
        ),
    ] {
        let mut values = legacy_factory_values();
        values["api"][key]["aCME-cORP/aCME-bOT"] = policy;
        assert_factory_error(values, &format!("api.{key}"), &["aCME-cORP/aCME-bOT"]);
    }
}

#[test]
fn factory_conflicting_typed_successors_refuse_with_setting_names_only() {
    for (path, replacement, setting) in [
        ("/factory/enabled", json!(false), "factory.enabled"),
        ("/factory/intake", json!("poll"), "factory.intake"),
        (
            "/factory/ciWaitSeconds",
            json!(999),
            "factory.ciWaitSeconds",
        ),
        (
            "/factory/cardBaseUrl",
            json!("https://secret-sentinel-4628.example.com"),
            "factory.cardBaseUrl",
        ),
        (
            "/factory/trackers/0/label",
            json!("secret-sentinel-4628"),
            "factory.trackers",
        ),
        (
            "/factory/trackers/0/mention",
            json!("secret-sentinel-4628"),
            "factory.trackers",
        ),
        (
            "/factory/trackers/0/pollIntervalSeconds",
            json!(99),
            "factory.trackers",
        ),
        (
            "/factory/codeHosts/0/credential/appId",
            json!("secret-sentinel-4628"),
            "factory.codeHosts",
        ),
        (
            "/factory/codeHosts/0/credential/existingSecret",
            json!("secret-sentinel-4628"),
            "factory.codeHosts",
        ),
        (
            "/factory/trackers/0/credential/existingSecretKey",
            json!("secret-sentinel-4628"),
            "factory.trackers",
        ),
        (
            "/factory/bindings/0/repos/0/projectId",
            json!("secret-sentinel-4628"),
            "factory.bindings",
        ),
        (
            "/factory/bindings/0/repos/0/path",
            json!("acme-corp/secret-sentinel-4628"),
            "factory.bindings",
        ),
        (
            "/factory/bindings/0/repos/0/bases",
            json!(["main"]),
            "factory.bindings",
        ),
        (
            "/factory/bindings/0/repos/0/defaultBase",
            json!("main"),
            "factory.bindings",
        ),
        (
            "/factory/bindings/0/repos/0/ci/required/key",
            json!("secret-sentinel-4628"),
            "factory.bindings",
        ),
        (
            "/factory/bindings/0/repos/0/ci/required/paths",
            json!(["secret-sentinel-4628"]),
            "factory.bindings",
        ),
        (
            "/factory/bindings/0/repos/0/ci/required/pendingKeyPrefix",
            json!("secret-sentinel-4628"),
            "factory.bindings",
        ),
        (
            "/factory/bindings/0/repos/0/ci/metadataRerunKeys",
            json!(["secret-sentinel-4628"]),
            "factory.bindings",
        ),
        (
            "/factory/bindings/0/startAuthority/type",
            json!("allowlist"),
            "factory.bindings",
        ),
    ] {
        let mut values = legacy_factory_values();
        values["factory"] = equal_typed_factory();
        *values.pointer_mut(path).expect("typed fixture field") = replacement;
        assert_factory_error(values, setting, &["999"]);
    }
}

#[test]
fn factory_numeric_tracker_scope_conflicts_refuse_with_setting_names_only() {
    for id in [json!(73), json!(73.0)] {
        let mut values = legacy_factory_values();
        values["factory"] = equal_typed_factory();
        values["factory"]["bindings"][0]["trackerScopeId"] = id.clone();
        values["factory"]["bindings"][0]["repos"][0]["projectId"] = id;
        values["factory"]["bindings"][0]["repos"][0]["path"] = json!("acme-corp/other");
        assert_factory_error(
            values,
            "factory.bindings",
            &[
                "73",
                "retained-work",
                "retained-issues",
                "retained-code",
                "bot-source",
                "acme-corp/other",
                "github.com",
            ],
        );
    }
}

#[test]
fn factory_numeric_project_id_conflicts_refuse_with_setting_names_only() {
    for id in [json!(73), json!(73.0)] {
        let mut values = legacy_factory_values();
        values["factory"] = equal_typed_factory();
        values["factory"]["bindings"][0]["trackerScopeId"] = json!("91");
        values["factory"]["bindings"][0]["repos"][0]["projectId"] = id;
        values["factory"]["bindings"][0]["repos"][0]["path"] = json!("acme-corp/other");
        assert_factory_error(
            values,
            "factory.bindings",
            &[
                "73",
                "91",
                "retained-work",
                "retained-issues",
                "retained-code",
                "bot-source",
                "acme-corp/other",
                "github.com",
            ],
        );
    }
}

#[test]
fn factory_malformed_legacy_settings_refuse_without_parser_or_input_leaks() {
    for (key, malformed) in [
        ("githubFactoryIngressEnabled", json!("secret-sentinel-4628")),
        ("githubFactoryIntake", json!("secret-sentinel-4628")),
        ("githubFactoryPollIntervalSeconds", json!(0)),
        (
            "githubFactoryPollIntervalSeconds",
            json!("secret-sentinel-4628"),
        ),
        (
            "githubFactoryLabel",
            json!("secret-sentinel-4628 with spaces"),
        ),
        (
            "githubFactoryMention",
            json!("secret-sentinel-4628 with spaces"),
        ),
        (
            "githubFactoryCardBaseUrl",
            json!("https://user:secret-sentinel-4628@cards.example.com"),
        ),
        (
            "githubFactoryCardBaseUrl",
            json!("https://cards.example.com?token=secret-sentinel-4628"),
        ),
        ("githubFactoryCiWaitSeconds", json!(0)),
        ("githubFactoryCiWaitSeconds", json!(10801)),
        ("githubFactoryCiWaitSeconds", json!(1.5)),
        ("githubFactoryBases", json!([])),
        ("githubFactoryPythonCi", json!("secret-sentinel-4628")),
        ("githubFactoryMetadataCi", json!(["secret-sentinel-4628"])),
    ] {
        let mut values = legacy_factory_values();
        values["api"][key] = malformed;
        assert_factory_error(values, &format!("api.{key}"), &[]);
    }
}

#[test]
fn factory_malformed_policy_objects_refuse_each_invalid_field() {
    for (key, policy) in [
        ("githubFactoryBases", json!({"bases": []})),
        (
            "githubFactoryBases",
            json!({"bases": ["bad..secret-sentinel-4628"]}),
        ),
        ("githubFactoryBases", json!({"bases": ["main", "main"]})),
        (
            "githubFactoryBases",
            json!({"bases": ["main"], "default_base": "secret-sentinel-4628"}),
        ),
        (
            "githubFactoryBases",
            json!({"bases": ["main"], "secret-sentinel-4628": true}),
        ),
        (
            "githubFactoryPythonCi",
            json!({"check": "", "paths": ["apps"]}),
        ),
        (
            "githubFactoryPythonCi",
            json!({"check": "body-check", "paths": []}),
        ),
        (
            "githubFactoryPythonCi",
            json!({"check": "body-check", "paths": "secret-sentinel-4628"}),
        ),
        (
            "githubFactoryPythonCi",
            json!({"check": "body-check", "paths": ["/secret-sentinel-4628"]}),
        ),
        (
            "githubFactoryPythonCi",
            json!({"check": "body-check", "paths": ["secret-sentinel-4628/"]}),
        ),
        (
            "githubFactoryPythonCi",
            json!({"check": "body-check", "paths": [17]}),
        ),
        (
            "githubFactoryPythonCi",
            json!({"check": "body-check", "paths": ["apps"], "pendingCheckPrefix": ""}),
        ),
        (
            "githubFactoryPythonCi",
            json!({"check": "body-check", "paths": ["apps"], "secret-sentinel-4628": true}),
        ),
        ("githubFactoryMetadataCi", json!({})),
        ("githubFactoryMetadataCi", json!({"checks": [""]})),
        (
            "githubFactoryMetadataCi",
            json!({"statuses": "secret-sentinel-4628"}),
        ),
        ("githubFactoryMetadataCi", json!({"checks": [17]})),
        (
            "githubFactoryMetadataCi",
            json!({"checks": ["body-check"], "secret-sentinel-4628": true}),
        ),
    ] {
        let mut values = legacy_factory_values();
        values["api"][key] = json!({"acme-corp/acme-bot": policy});
        assert_factory_error(values, &format!("api.{key}"), &[]);
    }
}

#[test]
fn factory_source_urls_and_repository_names_fail_closed() {
    for (key, invalid) in [
        (
            "githubApiUrl",
            json!("https://user:secret-sentinel-4628@api.example.com"),
        ),
        (
            "githubApiUrl",
            json!("https://api.example.com?secret-sentinel-4628"),
        ),
        (
            "githubApiUrl",
            json!("ftp://secret-sentinel-4628.example.com"),
        ),
        ("githubRepoAllowlist", json!(["secret-sentinel-4628"])),
        ("githubRepoAllowlist", json!("secret-sentinel-4628")),
        ("githubRepoAllowlist", json!(["acme-corp/*"])),
        ("githubAppId", json!("secret-sentinel-4628")),
        ("githubAppExistingSecret", json!("")),
        ("githubAppExistingSecretKey", json!("")),
    ] {
        let mut values = legacy_factory_values();
        values["api"][key] = invalid;
        assert_factory_error(values, &format!("api.{key}"), &[]);
    }
}

#[test]
fn factory_policy_repository_keys_must_be_valid_paths() {
    for key in [
        "githubFactoryBases",
        "githubFactoryPythonCi",
        "githubFactoryMetadataCi",
    ] {
        let mut values = legacy_factory_values();
        let policy = values["api"][key]
            .as_object()
            .unwrap()
            .values()
            .next()
            .unwrap()
            .clone();
        values["api"][key] = json!({"secret-sentinel-4628": policy});
        assert_factory_error(values, &format!("api.{key}"), &[]);
    }
}

#[test]
fn factory_valid_card_urls_and_ci_wait_boundaries_are_preserved() {
    for (card_url, wait) in [("", 1), ("http://127.0.0.1:8080/cards", 10800)] {
        let mut values = legacy_factory_values();
        values["api"]["environment"] = json!("dev");
        values["api"]["githubFactoryCardBaseUrl"] = json!(card_url);
        values["api"]["githubFactoryCiWaitSeconds"] = json!(wait);
        let outcome = migrated_factory(values);
        assert_eq!(outcome.values["factory"]["cardBaseUrl"], card_url);
        assert_eq!(outcome.values["factory"]["ciWaitSeconds"], wait);
    }
}

#[test]
fn factory_equal_typed_metadata_key_sets_preserve_existing_order() {
    let mut values = legacy_factory_values();
    values["factory"] = equal_typed_factory();
    values["factory"]["bindings"][0]["repos"][0]["ci"]["metadataRerunKeys"] = json!([
        "status:status:retained-context",
        "fix-pin",
        "status:approval",
        "body-check"
    ]);
    let expected = values["factory"].clone();
    let outcome = migrated_factory(values);
    assert_eq!(outcome.values["factory"], expected);
}

#[test]
fn factory_ambiguous_host_tracker_and_binding_candidates_refuse() {
    for section in ["codeHosts", "trackers", "bindings"] {
        let mut values = legacy_factory_values();
        values["factory"] = equal_typed_factory();
        let mut duplicate = values["factory"][section][0].clone();
        duplicate["name"] = json!("secret-sentinel-4628");
        values["factory"][section]
            .as_array_mut()
            .unwrap()
            .push(duplicate);
        assert_factory_error(values, &format!("factory.{section}"), &[]);
    }
}

#[test]
fn factory_inconsistent_or_duplicate_resolved_identity_refuses() {
    for repositories in [
        vec![("acme-corp/acme-bot", "0")],
        vec![("acme-corp/acme-bot", "secret-sentinel-4628")],
        vec![("acme-corp/acme-bot", "73"), ("ACME-CORP/ACME-BOT", "74")],
        vec![
            ("acme-corp/acme-bot", "73"),
            ("acme-corp/secret-sentinel-4628", "73"),
        ],
    ] {
        let mut values = legacy_factory_values();
        if repositories
            .iter()
            .any(|(path, _)| *path == "acme-corp/secret-sentinel-4628")
        {
            values["api"]["githubRepoAllowlist"] =
                json!(["acme-corp/acme-bot", "acme-corp/secret-sentinel-4628"]);
        }
        let context = FactoryMigrationContext::LegacyGitHub {
            effective_legacy: values.clone(),
            repositories: repositories
                .into_iter()
                .map(|(path, project_id)| ResolvedFactoryRepository {
                    path: path.into(),
                    project_id: project_id.into(),
                })
                .collect(),
        };
        let error = migrate_installed_config(values, Some("curie-0.12.5"), &context)
            .expect_err("unusable immutable identity must refuse");
        let message = format!("{error:#}\n{error:?}");
        assert!(
            message.contains("api.githubRepoAllowlist") || message.contains("factory.bindings")
        );
        assert!(!message.contains("acme-corp/acme-bot"));
        assert!(!message.contains("secret-sentinel-4628"));
    }
}

#[test]
fn factory_equal_duplicate_resolutions_coalesce_by_immutable_identity() {
    let mut values = legacy_factory_values();
    values["api"]["githubRepoAllowlist"] = json!(["Acme-Corp/Acme-Bot", "acme-corp/acme-bot"]);
    let context = FactoryMigrationContext::LegacyGitHub {
        effective_legacy: values.clone(),
        repositories: vec![
            ResolvedFactoryRepository {
                path: "acme-corp/acme-bot".into(),
                project_id: "73".into(),
            },
            ResolvedFactoryRepository {
                path: "acme-corp/acme-bot".into(),
                project_id: "73".into(),
            },
        ],
    };
    let outcome = migrate_installed_config(values.clone(), Some("curie-0.12.5"), &context).unwrap();
    assert_eq!(
        outcome.values["api"]["githubRepoAllowlist"],
        values["api"]["githubRepoAllowlist"]
    );
    assert_eq!(
        outcome.values["factory"]["bindings"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    let (_, repo) = only_github_repo(&outcome.values);
    assert_eq!(repo["projectId"], "73");
    assert_eq!(repo["path"], "acme-corp/acme-bot");
}

#[test]
fn factory_each_resolved_repository_gets_its_own_policy_and_native_scope() {
    let mut values = legacy_factory_values();
    values["api"]["githubRepoAllowlist"] = json!(["acme-corp/acme-bot", "acme-corp/other"]);
    values["api"]["githubFactoryBases"]["acme-corp/other"] =
        json!({"bases": ["stable", "main"], "default_base": "stable"});
    let context = FactoryMigrationContext::LegacyGitHub {
        effective_legacy: values.clone(),
        repositories: vec![
            ResolvedFactoryRepository {
                path: "acme-corp/other".into(),
                project_id: "91".into(),
            },
            ResolvedFactoryRepository {
                path: "acme-corp/acme-bot".into(),
                project_id: "73".into(),
            },
        ],
    };
    let outcome = migrate_installed_config(values.clone(), Some("curie-0.12.5"), &context).unwrap();
    assert_eq!(
        outcome.values["api"]["githubRepoAllowlist"],
        values["api"]["githubRepoAllowlist"]
    );
    let bindings = outcome.values["factory"]["bindings"].as_array().unwrap();
    assert_eq!(bindings.len(), 2);
    let other = bindings
        .iter()
        .find(|binding| binding["trackerScopeId"] == "91")
        .unwrap();
    assert_eq!(other["repos"].as_array().unwrap().len(), 1);
    assert_eq!(other["repos"][0]["projectId"], "91");
    assert_eq!(other["repos"][0]["path"], "acme-corp/other");
    assert_eq!(other["repos"][0]["bases"], json!(["stable", "main"]));
    assert_eq!(other["repos"][0]["defaultBase"], "stable");
    assert!(other["repos"][0].get("ci").is_none());
    let (_, repo) = only_github_repo(&outcome.values);
    assert_eq!(repo["ci"]["required"]["key"], "required-python");
}

#[test]
fn factory_metadata_extra_env_migrates_and_preserves_unrelated_entries_in_order() {
    for retain_first_class in [false, true] {
        let mut values = legacy_factory_values();
        let policy = values["api"]["githubFactoryMetadataCi"].clone();
        if !retain_first_class {
            values["api"]
                .as_object_mut()
                .unwrap()
                .remove("githubFactoryMetadataCi");
        }
        let before = json!({"name": "CUSTOM_BEFORE", "valueFrom": {"secretKeyRef": {"name": "acme-before", "key": "value"}}});
        let after = json!({"name": "CUSTOM_AFTER", "value": "retained-value"});
        values["api"]["extraEnv"] = json!([
            before.clone(), {"name": "GITHUB_FACTORY_METADATA_CI", "value": policy.to_string()}, after.clone()
        ]);
        let outcome = migrated_factory(values);
        assert_eq!(outcome.values["api"]["extraEnv"], json!([before, after]));
        assert_legacy_factory_removed(&outcome.values);
        let (_, repo) = only_github_repo(&outcome.values);
        assert_eq!(
            repo["ci"]["metadataRerunKeys"],
            equal_typed_factory()["bindings"][0]["repos"][0]["ci"]["metadataRerunKeys"]
        );
    }
}

#[test]
fn factory_metadata_extra_env_conflict_malformed_and_value_from_refuse_safely() {
    for entry in [
        json!({"name": "GITHUB_FACTORY_METADATA_CI", "value": "{secret-sentinel-4628"}),
        json!({"name": "GITHUB_FACTORY_METADATA_CI", "value": "{\"acme-corp/acme-bot\":{\"checks\":[\"secret-sentinel-4628\"]}}"}),
        json!({"name": "GITHUB_FACTORY_METADATA_CI", "valueFrom": {"secretKeyRef": {"name": "secret-sentinel-4628", "key": "policy"}}}),
    ] {
        let mut values = legacy_factory_values();
        values["api"]["extraEnv"] = json!([entry]);
        assert_factory_error(values, "GITHUB_FACTORY_METADATA_CI", &[]);
    }
}

#[test]
fn factory_duplicate_metadata_extra_env_entries_cannot_overwrite_policy() {
    for equal in [true, false] {
        let mut values = legacy_factory_values();
        let policy = values["api"]
            .as_object_mut()
            .unwrap()
            .remove("githubFactoryMetadataCi")
            .unwrap();
        let second = if equal {
            policy.clone()
        } else {
            json!({"acme-corp/acme-bot": {"checks": ["secret-sentinel-4628"]}})
        };
        values["api"]["extraEnv"] = json!([
            {"name": "GITHUB_FACTORY_METADATA_CI", "value": policy.to_string()},
            {"name": "GITHUB_FACTORY_METADATA_CI", "value": second.to_string()}
        ]);
        if equal {
            let outcome = migrated_factory(values);
            assert_eq!(outcome.values["api"]["extraEnv"], json!([]));
            let (_, repo) = only_github_repo(&outcome.values);
            assert_eq!(
                repo["ci"]["metadataRerunKeys"],
                equal_typed_factory()["bindings"][0]["repos"][0]["ci"]["metadataRerunKeys"]
            );
        } else {
            assert_factory_error(values, "GITHUB_FACTORY_METADATA_CI", &[]);
        }
    }
}

#[test]
fn factory_removed_environment_without_a_successor_is_not_silently_dropped() {
    for name in [
        "GITHUB_FACTORY_RECONCILE_INTERVAL_S",
        "GITHUB_FACTORY_RECONCILE_GRACE_S",
    ] {
        let mut values = legacy_factory_values();
        values["api"]["extraEnv"] = json!([{"name": name, "value": "secret-sentinel-4628"}]);
        assert_factory_error(values, name, &[]);
    }
}

#[test]
fn factory_policies_outside_the_resolved_allowlist_cannot_be_discarded() {
    for key in [
        "githubFactoryBases",
        "githubFactoryPythonCi",
        "githubFactoryMetadataCi",
    ] {
        let mut values = legacy_factory_values();
        let policy = values["api"][key]
            .as_object()
            .unwrap()
            .values()
            .next()
            .unwrap()
            .clone();
        values["api"][key]["acme-corp/secret-sentinel-4628"] = policy;
        assert_factory_error(values, &format!("api.{key}"), &[]);
    }
}

#[test]
fn factory_numeric_app_id_becomes_a_string_without_changing_the_api_setting() {
    let mut values = legacy_factory_values();
    values["api"]["githubAppId"] = json!(17);
    let outcome = migrated_factory(values);
    assert_eq!(outcome.values["api"]["githubAppId"], 17);
    for section in ["codeHosts", "trackers"] {
        assert_eq!(
            only_github_entry(&outcome.values, section)["credential"]["appId"],
            "17"
        );
    }
}

#[test]
fn factory_enterprise_host_port_egress_and_enrollment_are_preserved() {
    let mut values = legacy_factory_values();
    values["api"]["githubApiUrl"] = json!("https://github.example.com:8443/api/v3");
    values["api"]["githubCloneBase"] = json!("https://github.example.com:8443");
    values["factory"] = equal_typed_factory();
    for section in ["codeHosts", "trackers"] {
        values["factory"][section][0]["host"] = json!("github.example.com:8443");
        values["factory"][section][0]["apiUrl"] = json!("https://github.example.com:8443/api/v3");
        values["factory"][section][0]["egress"] = json!([{
            "cidr": "192.0.2.8/32", "ports": [{"port": 8443, "protocol": "TCP"}]
        }]);
    }
    values["factory"]["sandboxEgress"] = json!({"acme-bot": true});
    let expected = values["factory"].clone();
    let outcome = migrated_factory(values.clone());
    assert_eq!(outcome.values["factory"], expected);
    assert_eq!(
        outcome.values["api"]["githubApiUrl"],
        values["api"]["githubApiUrl"]
    );
    assert_eq!(
        outcome.values["api"]["githubCloneBase"],
        values["api"]["githubCloneBase"]
    );
    assert_legacy_factory_removed(&outcome.values);
}

fn factory_egress_rules(cidrs: &[&str], port: u16) -> Value {
    json!(cidrs
        .iter()
        .map(|cidr| json!({
            "cidr": cidr, "ports": [{"protocol": "TCP", "port": port}]
        }))
        .collect::<Vec<_>>())
}

fn enterprise_factory_values(origin: &str) -> Value {
    let mut values = legacy_factory_values();
    values["api"]["githubApiUrl"] = json!(origin);
    values
}

fn assert_factory_egress_diagnostic(message: &str, forbidden: &[&str]) {
    for setting in [
        "api.githubApiUrl",
        "factory.codeHosts.egress",
        "factory.trackers.egress",
    ] {
        assert!(
            message.contains(setting),
            "refusal must name {setting}: {message}"
        );
    }
    for value in forbidden.iter().copied().chain([
        "acme-corp/acme-bot",
        "acme-factory-app",
        "triage-ready",
        "acme-maintainer",
    ]) {
        assert!(
            !message.contains(value),
            "egress refusal must contain setting names only"
        );
    }
}

fn assert_factory_routes(values: &Value, origin: &str, code: &Value, tracker: &Value) {
    for (section, expected) in [("codeHosts", code), ("trackers", tracker)] {
        let entries: Vec<_> = values["factory"][section]
            .as_array()
            .unwrap()
            .iter()
            .filter(|entry| entry["kind"] == "github" && entry["apiUrl"] == origin)
            .collect();
        assert_eq!(
            entries.len(),
            1,
            "one GitHub connection for the retained origin"
        );
        assert_eq!(
            &entries[0]["egress"], expected,
            "preserve the proven {section} route"
        );
    }
}

fn assert_factory_enrollment_plan(outcome: &MigrationOutcome, forbidden: &[&str]) {
    let plan = redacted_upgrade_plan(outcome).join("\n");
    assert!(
        plan.contains("factory.sandboxEgress"),
        "plan must name automatic enrollment"
    );
    for value in forbidden.iter().copied().chain([
        "acme-factory",
        "acme-corp/acme-bot",
        "triage-ready",
        "acme-maintainer",
        "required-python",
        "cards.example.com",
        "2345",
        "signing.pem",
        "retained-code",
        "retained-issues",
        "retained-work",
        "bot-source",
    ]) {
        assert!(!plan.contains(value), "plan must omit operator values");
    }
}

#[test]
fn factory_egress_unrelated_same_port_connector_refuses_without_disclosing_values() {
    for origin in [
        "https://ghes-egress-sentinel-4628.example.com/api/v3",
        "https://203.0.113.25/api/v3",
    ] {
        let mut values = enterprise_factory_values(origin);
        values["agentSandbox"]["connectorEgress"] = json!({
            "acme-unrelated-4628": factory_egress_rules(&["198.51.100.19/32"], 443)
        });
        let context = factory_context(&values);
        let error = migrate_installed_config(values, Some("curie-0.12.5"), &context)
            .expect_err("a matching port cannot identify the forge endpoint");
        assert_factory_egress_diagnostic(
            &format!("{error}\n{error:#}\n{error:?}"),
            &[
                origin,
                "ghes-egress-sentinel-4628",
                "203.0.113.25",
                "198.51.100.19",
                "acme-unrelated-4628",
            ],
        );
    }
}

#[test]
fn factory_egress_mixed_connector_routes_cannot_widen_publication() {
    let origin = "https://192.0.2.25/api/v3";
    for cidrs in [
        ["192.0.2.25/32", "198.51.100.19/32"],
        ["198.51.100.19/32", "192.0.2.25/32"],
    ] {
        let mut values = enterprise_factory_values(origin);
        values["agentSandbox"]["connectorEgress"] = json!({
            "acme-mixed-4628": factory_egress_rules(&cidrs, 443)
        });
        let context = factory_context(&values);
        let error = migrate_installed_config(values, Some("curie-0.12.5"), &context)
            .expect_err("one origin route cannot authorize an unrelated publication route");
        assert_factory_egress_diagnostic(
            &format!("{error}\n{error:#}\n{error:?}"),
            &[origin, "192.0.2.25", "198.51.100.19", "acme-mixed-4628"],
        );
    }
}

#[test]
fn factory_egress_different_origin_covering_connector_lists_are_ambiguous() {
    let origin = "https://192.0.2.25/api/v3";
    for cidrs in [
        ["192.0.2.0/24", "192.0.2.25/32"],
        ["192.0.2.25/32", "192.0.2.0/24"],
    ] {
        let mut values = enterprise_factory_values(origin);
        values["agentSandbox"]["connectorEgress"] = json!({
            "acme-first-4628": factory_egress_rules(&[cidrs[0]], 443),
            "acme-second-4628": factory_egress_rules(&[cidrs[1]], 443)
        });
        let context = factory_context(&values);
        let error = migrate_installed_config(values, Some("curie-0.12.5"), &context)
            .expect_err("different origin-covering lists require explicit factory routes");
        assert_factory_egress_diagnostic(
            &format!("{error}\n{error:#}\n{error:?}"),
            &[
                origin,
                "192.0.2.0",
                "192.0.2.25",
                "acme-first-4628",
                "acme-second-4628",
            ],
        );
    }
}

#[test]
fn factory_egress_literal_api_ip_excluded_by_connector_except_refuses() {
    let origin = "https://192.0.2.25/api/v3";
    let mut route = factory_egress_rules(&["192.0.2.0/24"], 443);
    route[0]["except"] = json!(["192.0.2.25/32"]);
    let mut values = enterprise_factory_values(origin);
    values["agentSandbox"]["connectorEgress"] = json!({"acme-excluded-4628": route});
    let context = factory_context(&values);
    let error = migrate_installed_config(values.clone(), Some("curie-0.12.5"), &context)
        .expect_err("an excluded origin is unreachable through the connector route");
    assert_factory_egress_diagnostic(
        &format!("{error}\n{error:#}\n{error:?}"),
        &[origin, "192.0.2.0", "192.0.2.25", "acme-excluded-4628"],
    );

    values["agentSandbox"]["connectorEgress"]["acme-excluded-4628"][0]["except"] =
        json!(["192.0.2.26/32"]);
    let route = values["agentSandbox"]["connectorEgress"]["acme-excluded-4628"].clone();
    let outcome = migrated_factory(values.clone());
    assert_factory_routes(&outcome.values, origin, &route, &route);
    assert_eq!(
        outcome.values["agentSandbox"]["connectorEgress"],
        values["agentSandbox"]["connectorEgress"]
    );
    assert_legacy_factory_removed(&outcome.values);
}

#[test]
fn factory_egress_empty_connector_except_refuses_without_disclosing_values() {
    let origin = "https://192.0.2.25/api/v3";
    let mut route = factory_egress_rules(&["192.0.2.0/24"], 443);
    route[0]["except"] = json!([]);
    let mut values = enterprise_factory_values(origin);
    values["agentSandbox"]["connectorEgress"] = json!({"acme-factory": route});
    let context = factory_context(&values);
    let error = migrate_installed_config(values, Some("curie-0.12.5"), &context)
        .expect_err("empty exclusions violate the retained egress security contract");
    assert_factory_egress_diagnostic(
        &format!("{error}\n{error:#}\n{error:?}"),
        &[origin, "192.0.2.0", "192.0.2.25", "acme-factory"],
    );
}

#[test]
fn factory_egress_malformed_connector_except_refuses_without_disclosing_values() {
    let origin = "https://192.0.2.25/api/v3";
    for except in [
        json!("secret-sentinel-4628"),
        json!(["secret-sentinel-4628"]),
        json!({"cidr": "192.0.2.26/32"}),
    ] {
        let mut route = factory_egress_rules(&["192.0.2.0/24"], 443);
        route[0]["except"] = except;
        let mut values = enterprise_factory_values(origin);
        values["agentSandbox"]["connectorEgress"] = json!({"acme-malformed-4628": route});
        let context = factory_context(&values);
        let error = migrate_installed_config(values, Some("curie-0.12.5"), &context)
            .expect_err("malformed exclusions cannot prove that the origin is reachable");
        assert_factory_egress_diagnostic(
            &format!("{error}\n{error:#}\n{error:?}"),
            &[
                origin,
                "192.0.2.0",
                "192.0.2.25",
                "acme-malformed-4628",
                "secret-sentinel-4628",
            ],
        );
    }
}

#[test]
fn factory_egress_publication_route_cannot_authorize_an_unrelated_connector_or_agent() {
    let origin = "https://github.example.com/api/v3";
    let mut values = enterprise_factory_values(origin);
    values["worker"]["publication"]["githubHttpsCidrs"] = json!(["192.0.2.25/32"]);
    values["agentSandbox"]["connectorEgress"] = json!({
        "acme-unrelated-4628": factory_egress_rules(&["198.51.100.19/32"], 443)
    });
    let context = factory_context(&values);
    let error = migrate_installed_config(values, Some("curie-0.12.5"), &context).expect_err(
        "publication egress cannot identify an unrelated connector as the tracker route",
    );
    assert_factory_egress_diagnostic(
        &format!("{error}\n{error:#}\n{error:?}"),
        &[
            "github.example.com",
            "192.0.2.25",
            "198.51.100.19",
            "acme-unrelated-4628",
        ],
    );
}

#[test]
fn factory_egress_literal_api_ip_inside_a_connector_cidr_is_a_valid_neighbor() {
    let origin = "https://192.0.2.25/api/v3";
    let route = factory_egress_rules(&["192.0.2.0/24"], 443);
    let mut values = enterprise_factory_values(origin);
    values["agentSandbox"]["connectorEgress"] = json!({"acme-factory": route.clone()});
    let outcome = migrated_factory(values.clone());
    assert_factory_routes(&outcome.values, origin, &route, &route);
    assert_eq!(
        outcome.values["agentSandbox"]["connectorEgress"],
        values["agentSandbox"]["connectorEgress"]
    );
    assert_eq!(
        outcome.values["factory"]["sandboxEgress"],
        json!({"acme-factory": true})
    );
    assert_factory_enrollment_plan(&outcome, &[origin, "192.0.2.25", "192.0.2.0"]);
    assert_legacy_factory_removed(&outcome.values);
}

#[test]
fn factory_egress_ipv6_api_authority_refuses_without_disclosing_values() {
    let origin = "https://[2001:db8:4628::25]/api/v3";
    let route = factory_egress_rules(&["2001:db8:4628::/64"], 443);
    let mut values = enterprise_factory_values(origin);
    values["api"]["githubWebhookSecret"] = json!("secret-sentinel-4628");
    values["agentSandbox"]["connectorEgress"] = json!({"acme-factory": route});
    assert_factory_error(
        values,
        "api.githubApiUrl",
        &[origin, "2001:db8:4628", "acme-factory"],
    );
}

#[test]
fn factory_egress_publication_matching_connector_cidrs_are_a_valid_neighbor() {
    let origin = "https://github.example.com/api/v3";
    let route = factory_egress_rules(&["192.0.2.25/32", "192.0.2.26/32"], 443);
    let mut values = enterprise_factory_values(origin);
    values["worker"]["publication"]["githubHttpsCidrs"] = json!(["192.0.2.25/32", "192.0.2.26/32"]);
    values["agentSandbox"]["connectorEgress"] = json!({
        "acme-factory": route.clone(),
        "acme-unrelated-4628": factory_egress_rules(&["198.51.100.19/32"], 443)
    });
    let outcome = migrated_factory(values.clone());
    assert_factory_routes(&outcome.values, origin, &route, &route);
    assert_eq!(
        outcome.values["agentSandbox"]["connectorEgress"],
        values["agentSandbox"]["connectorEgress"]
    );
    assert_ne!(
        outcome
            .values
            .pointer("/factory/sandboxEgress/acme-unrelated-4628"),
        Some(&json!(true))
    );
    assert!(outcome
        .values
        .pointer("/worker/publication/githubHttpsCidrs")
        .is_none());
    assert_eq!(
        outcome.values["factory"]["sandboxEgress"]["acme-factory"],
        true
    );
    let plan = redacted_upgrade_plan(&outcome).join("\n");
    for setting in [
        "worker.publication.githubHttpsCidrs",
        "factory.sandboxEgress",
    ] {
        assert!(plan.contains(setting), "plan must name {setting}");
    }
    for value in [
        origin,
        "github.example.com",
        "192.0.2.25",
        "192.0.2.26",
        "198.51.100.19",
        "acme-factory",
        "acme-unrelated-4628",
        "acme-corp/acme-bot",
        "triage-ready",
        "acme-maintainer",
        "required-python",
        "cards.example.com",
        "2345",
    ] {
        assert!(!plan.contains(value), "plan must omit operator values");
    }
    assert_legacy_factory_removed(&outcome.values);
}

#[test]
fn factory_egress_explicit_split_routes_do_not_enroll_an_agent_with_only_one_existing_route() {
    let origin = "https://github.example.com/api/v3";
    let code = factory_egress_rules(&["192.0.2.25/32"], 443);
    let tracker = factory_egress_rules(&["198.51.100.19/32"], 443);
    for (existing_route, explicit_false) in [
        (&code, false),
        (&tracker, false),
        (&code, true),
        (&tracker, true),
    ] {
        let mut values = enterprise_factory_values(origin);
        values["factory"] = equal_typed_factory();
        for (section, route) in [("codeHosts", &code), ("trackers", &tracker)] {
            values["factory"][section][0]["host"] = json!("github.example.com");
            values["factory"][section][0]["apiUrl"] = json!(origin);
            values["factory"][section][0]["egress"] = route.clone();
        }
        values["factory"]["sandboxEgress"] = json!({"acme-existing": true});
        if explicit_false {
            values["factory"]["sandboxEgress"]["acme-unrelated-4628"] = json!(false);
        }
        values["agentSandbox"]["connectorEgress"] = json!({"acme-unrelated-4628": existing_route});
        let outcome = migrated_factory(values.clone());
        assert_factory_routes(&outcome.values, origin, &code, &tracker);
        assert_eq!(outcome.values["factory"], values["factory"],
            "preserve explicit routes and enrollment without granting their union to a connector-only agent");
        assert_eq!(
            outcome.values["agentSandbox"]["connectorEgress"],
            values["agentSandbox"]["connectorEgress"]
        );
        assert_legacy_factory_removed(&outcome.values);
    }
}

#[test]
fn factory_egress_automatic_enrollment_requires_both_split_routes() {
    let origin = "https://github.example.com/api/v3";
    let code = factory_egress_rules(&["192.0.2.25/32"], 443);
    let tracker = factory_egress_rules(&["198.51.100.19/32"], 443);
    let both = factory_egress_rules(&["192.0.2.25/32", "198.51.100.19/32"], 443);
    for (connector, enrolled) in [(&code, false), (&tracker, false), (&both, true)] {
        let mut values = enterprise_factory_values(origin);
        values["factory"] = equal_typed_factory();
        for (section, route) in [("codeHosts", &code), ("trackers", &tracker)] {
            values["factory"][section][0]["host"] = json!("github.example.com");
            values["factory"][section][0]["apiUrl"] = json!(origin);
            values["factory"][section][0]["egress"] = route.clone();
        }
        assert!(values["factory"].get("sandboxEgress").is_none());
        values["agentSandbox"]["connectorEgress"] = json!({"acme-factory": connector});
        let outcome = migrated_factory(values.clone());
        let mut expected_factory = values["factory"].clone();
        if enrolled {
            expected_factory["sandboxEgress"] = json!({"acme-factory": true});
        }
        assert_eq!(
            outcome.values["factory"], expected_factory,
            "automatic enrollment requires every code-host and tracker route"
        );
        assert_factory_routes(&outcome.values, origin, &code, &tracker);
        assert_eq!(
            outcome.values["agentSandbox"]["connectorEgress"],
            values["agentSandbox"]["connectorEgress"]
        );
        if enrolled {
            assert_factory_enrollment_plan(
                &outcome,
                &[origin, "github.example.com", "192.0.2.25", "198.51.100.19"],
            );
        }
        assert_legacy_factory_removed(&outcome.values);
    }
}

#[test]
fn factory_egress_automatic_enrollment_requires_retained_gitlab_routes() {
    let origin = "https://github.example.com/api/v3";
    let code = factory_egress_rules(&["192.0.2.25/32"], 443);
    let tracker = factory_egress_rules(&["198.51.100.19/32"], 443);
    let both = factory_egress_rules(&["192.0.2.25/32", "198.51.100.19/32"], 443);
    let all = factory_egress_rules(
        &["192.0.2.25/32", "198.51.100.19/32", "203.0.113.25/32"],
        443,
    );
    for section in ["codeHosts", "trackers"] {
        for (connector, enrolled) in [(&both, false), (&all, true)] {
            let mut values = enterprise_factory_values(origin);
            values["factory"] = equal_typed_factory();
            for (github_section, route) in [("codeHosts", &code), ("trackers", &tracker)] {
                values["factory"][github_section][0]["host"] = json!("github.example.com");
                values["factory"][github_section][0]["apiUrl"] = json!(origin);
                values["factory"][github_section][0]["egress"] = route.clone();
            }
            let mut gitlab = json!({
                "name": "acme-gitlab", "kind": "gitlab", "host": "gitlab.example.com",
                "apiUrl": "https://gitlab.example.com/api/v4",
                "credential": {"type": "static_token", "existingSecret": "acme-gitlab", "existingSecretKey": "token"},
                "egress": factory_egress_rules(&["203.0.113.25/32"], 443)
            });
            if section == "trackers" {
                gitlab["label"] = json!("queued");
                gitlab["mention"] = json!("reviewer");
                gitlab["pollIntervalSeconds"] = json!(59);
            }
            values["factory"][section]
                .as_array_mut()
                .unwrap()
                .push(gitlab);
            assert!(values["factory"].get("sandboxEgress").is_none());
            values["agentSandbox"]["connectorEgress"] = json!({"acme-factory": connector});
            let outcome = migrated_factory(values.clone());
            let mut expected_factory = values["factory"].clone();
            if enrolled {
                expected_factory["sandboxEgress"] = json!({"acme-factory": true});
            }
            assert_eq!(
                outcome.values["factory"], expected_factory,
                "automatic enrollment requires the GitHub routes and the retained GitLab route"
            );
            assert_factory_routes(&outcome.values, origin, &code, &tracker);
            assert_eq!(
                outcome.values["agentSandbox"]["connectorEgress"],
                values["agentSandbox"]["connectorEgress"]
            );
            if enrolled {
                assert_factory_enrollment_plan(
                    &outcome,
                    &[
                        origin,
                        "github.example.com",
                        "192.0.2.25",
                        "198.51.100.19",
                        "203.0.113.25",
                        "acme-gitlab",
                        "gitlab.example.com",
                        "queued",
                        "reviewer",
                    ],
                );
            }
            assert_legacy_factory_removed(&outcome.values);
        }
    }
}

#[test]
fn factory_egress_automatic_enrollment_refuses_a_second_github_host_without_egress() {
    let origin = "https://github.example.com/api/v3";
    let route = factory_egress_rules(&["192.0.2.25/32"], 443);
    for section in ["codeHosts", "trackers"] {
        let mut values = enterprise_factory_values(origin);
        values["factory"] = equal_typed_factory();
        for github_section in ["codeHosts", "trackers"] {
            values["factory"][github_section][0]["host"] = json!("github.example.com");
            values["factory"][github_section][0]["apiUrl"] = json!(origin);
            values["factory"][github_section][0]["egress"] = route.clone();
        }
        let mut second = values["factory"][section][0].clone();
        second["name"] = json!("acme-second-github");
        second["host"] = json!("github-other.example.com");
        second["apiUrl"] = json!("https://github-other.example.com/api/v3");
        second.as_object_mut().unwrap().remove("egress");
        values["factory"][section]
            .as_array_mut()
            .unwrap()
            .push(second);
        assert!(values["factory"].get("sandboxEgress").is_none());
        values["agentSandbox"]["connectorEgress"] = json!({"acme-factory": route.clone()});
        let outcome = migrated_factory(values.clone());
        assert_eq!(
            outcome.values["factory"], values["factory"],
            "a host with implicit egress prevents safe automatic enrollment"
        );
        assert_factory_routes(&outcome.values, origin, &route, &route);
        assert_eq!(
            outcome.values["agentSandbox"]["connectorEgress"],
            values["agentSandbox"]["connectorEgress"]
        );
        assert_legacy_factory_removed(&outcome.values);
    }
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
    "CURIE_ACTION_EXECUTOR_CONNECTOR_READS",
    "CURIE_ACTION_EXECUTOR_ENABLED",
    "CURIE_ACTION_EXECUTOR_MAX_CONCURRENT_SANDBOXES",
    "CURIE_ATTACHMENT_THREAD_MAX_BYTES",
    "CURIE_ATTACHMENT_THREAD_MAX_FILES",
    // HTTP-INVOCATION-EXECUTION-20: operator file directories were introduced as Helm values.
    "CURIE_HTTP_IDENTITY_DIR",
    "CURIE_HTTP_RECEIVER_DIR",
    // #5005 / ADR 0244: new chart-owned lazy settings have no released extraEnv source.
    // Broker origins are derived from the private Service, not migrated into a values map.
    "ATTACHMENT_BROKER_URL",
    "ATTACHMENT_TRANSFER_TIMEOUT_SECONDS",
    "CURIE_ATTACHMENT_BROKER_HOST",
    "CURIE_ATTACHMENT_BROKER_MAX_CONCURRENT",
    "CURIE_ATTACHMENT_BROKER_PORT",
    "CURIE_ATTACHMENT_MODE",
    "CURIE_ATTACHMENT_TRANSFER_TIMEOUT_SECONDS",
    "CURIE_REMEDIATION_ENABLED",
    // The ADR 0231 remediation authority is a chart-managed credential from introduction.
    "CURIE_REMEDIATION_AUTHORITY_KEY",
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
    // The new settle setting has no prior extraEnv values to migrate.
    "FACTORY_BASE_LABEL_SETTLE_S",
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
