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
