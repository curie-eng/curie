//! Exercise the actual CLI upgrade driver across its external Helm/Kubernetes
//! process boundary. These recording executables are plumbing regressions;
//! the released-upgrade matrix remains the real cluster acceptance gate.

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::process::{Command, Output};

use serde_json::{json, Value};

struct Fixture {
    temp: tempfile::TempDir,
}

impl Fixture {
    fn new() -> Self {
        let temp = tempfile::tempdir().unwrap();
        for name in ["helm", "kubectl"] {
            let path = temp.path().join(name);
            fs::write(&path, include_str!("data/upgrade-driver.py")).unwrap();
            fs::set_permissions(path, fs::Permissions::from_mode(0o755)).unwrap();
        }
        fs::write(temp.path().join("values.json"), "{}").unwrap();
        fs::write(
            temp.path().join("candidate-chart"),
            "immutable chart fixture",
        )
        .unwrap();
        Self { temp }
    }

    fn path(&self, name: &str) -> PathBuf {
        self.temp.path().join(name)
    }

    fn values(&self, values: Value) {
        fs::write(
            self.path("values.json"),
            serde_json::to_vec(&values).unwrap(),
        )
        .unwrap();
    }

    fn run(&self, scenario: &str) -> Output {
        self.run_args(scenario, &[])
    }

    fn command(&self, scenario: &str, extra: &[&str]) -> Command {
        let mut command = Command::new(env!("CARGO_BIN_EXE_curie"));
        command
            .args([
                "--json",
                "cluster",
                "upgrade",
                "--to",
                "0.9.0",
                "--chart",
                self.path("candidate-chart").to_str().unwrap(),
                "--namespace",
                "upgrade-test",
                "--release",
                "acme-bot",
                "--yes",
            ])
            .args(extra)
            .env(
                "PATH",
                format!("{}:/usr/bin:/bin", self.temp.path().display()),
            )
            .env("UPGRADE_DRIVER_ROOT", self.temp.path())
            .env("UPGRADE_DRIVER_SCENARIO", scenario)
            .env("TMPDIR", self.temp.path())
            .env("XDG_STATE_HOME", self.path("state"));
        command
    }

    fn run_args(&self, scenario: &str, extra: &[&str]) -> Output {
        self.command(scenario, extra).output().unwrap()
    }

    fn run_status(&self) -> Output {
        Command::new(env!("CARGO_BIN_EXE_curie"))
            .args([
                "--json",
                "cluster",
                "status",
                "--namespace",
                "upgrade-test",
                "--release",
                "acme-bot",
            ])
            .env(
                "PATH",
                format!("{}:/usr/bin:/bin", self.temp.path().display()),
            )
            .env("UPGRADE_DRIVER_ROOT", self.temp.path())
            .env("UPGRADE_DRIVER_SCENARIO", "healthy")
            .output()
            .unwrap()
    }

    fn calls(&self) -> String {
        fs::read_to_string(self.path("calls.jsonl")).unwrap_or_default()
    }

    fn assert_refused_without_upgrade(&self, output: &Output) {
        assert!(
            !output.status.success(),
            "must refuse: {}",
            String::from_utf8_lossy(&output.stdout)
        );
        assert!(
            !self.calls().contains("\"helm\", \"upgrade\""),
            "must not apply Helm after refusal"
        );
    }
}

#[test]
fn checkpoint_failure_stops_before_helm_mutation() {
    let fixture = Fixture::new();
    let output = fixture.run("checkpoint-fails");
    fixture.assert_refused_without_upgrade(&output);
    assert!(String::from_utf8_lossy(&output.stdout).contains("checkpoint"));
}

#[test]
fn retained_configuration_conflict_refuses_before_checkpoint_or_upgrade() {
    let fixture = Fixture::new();
    fixture.values(json!({"worker": {
        "runnerTotalTimeoutSeconds": 90,
        "extraEnv": [{"name": "CURIE_RUNNER_TOTAL_TIMEOUT_S", "value": "120"}]
    }}));
    let output = fixture.run("healthy");
    fixture.assert_refused_without_upgrade(&output);
    for mutation in ["apply", "create", "replace"] {
        assert!(!fixture
            .calls()
            .contains(&format!("\"kubectl\", \"{mutation}\"")));
    }
}

#[test]
fn actual_upgrade_uses_migrated_retained_values_and_external_secret_references() {
    let fixture = Fixture::new();
    fixture.values(json!({
        "worker": {
            "adapterCredentialsExistingSecret": "acme-adapter",
            "adapterCredentialsExistingSecretKey": "adapter-key",
            "adapterCredentials": "fixture-inline-must-not-return",
            "extraEnv": [{"name": "CURIE_RUNNER_TOTAL_TIMEOUT_S", "value": "1200"}]
        },
        "ui": {"deploy": false}
    }));
    let output = fixture.run("healthy");
    assert!(
        fixture.path("applied-values.json").exists(),
        "upgrade reached apply: {}",
        String::from_utf8_lossy(&output.stdout)
    );
    assert!(
        output.status.success(),
        "healthy upgrade failed: {}",
        String::from_utf8_lossy(&output.stdout)
    );
    let values: Value =
        serde_json::from_slice(&fs::read(fixture.path("applied-values.json")).unwrap()).unwrap();
    assert_eq!(values["config"]["schemaVersion"], "0.9.0");
    assert_eq!(values["worker"]["runnerTotalTimeoutSeconds"], 1200.0);
    assert_eq!(
        values["worker"]["adapterCredentialsExistingSecret"],
        "acme-adapter"
    );
    assert_eq!(
        values["worker"]["adapterCredentialsExistingSecretKey"],
        "adapter-key"
    );
    assert!(values["worker"].get("adapterCredentials").is_none());
    assert_eq!(values["ui"]["deploy"], false);
    assert!(!String::from_utf8_lossy(&output.stdout).contains("fixture-inline-must-not-return"));
}

#[test]
fn chart_target_mismatch_refuses_before_checkpoint_or_upgrade() {
    let fixture = Fixture::new();
    let output = fixture.run("wrong-chart");
    fixture.assert_refused_without_upgrade(&output);
    for mutation in ["apply", "create", "replace"] {
        assert!(!fixture
            .calls()
            .contains(&format!("\"kubectl\", \"{mutation}\"")));
    }
}

#[test]
fn helm_read_error_cannot_be_reclassified_as_fresh_install() {
    let fixture = Fixture::new();
    let output = fixture.run("helm-forbidden");
    fixture.assert_refused_without_upgrade(&output);
    for mutation in ["apply", "create", "replace"] {
        assert!(!fixture
            .calls()
            .contains(&format!("\"kubectl\", \"{mutation}\"")));
    }
}

#[test]
fn malformed_checkpoint_cannot_be_discarded_and_overwritten() {
    let fixture = Fixture::new();
    fs::write(fixture.path("record.json"), "not valid json").unwrap();
    let output = fixture.run("healthy");
    fixture.assert_refused_without_upgrade(&output);
    assert_eq!(
        fs::read_to_string(fixture.path("record.json")).unwrap(),
        "not valid json"
    );
}

#[test]
fn healthy_replica_counts_do_not_hide_wrong_image_or_stale_generation() {
    for scenario in ["wrong-image", "stale-generation", "wrong-manifest"] {
        let fixture = Fixture::new();
        let output = fixture.run(scenario);
        assert!(!output.status.success(), "{scenario} cannot report success");
        let result: Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(result["status"], "failed", "{scenario}");
        assert_eq!(result["convergence"]["exact"], false, "{scenario}");
        assert_eq!(result["known_good_version"], "0.8.5", "{scenario}");
    }
}

#[test]
fn failed_real_canary_cannot_commit_a_healthy_looking_release() {
    let fixture = Fixture::new();
    let output = fixture.run("canary-fails");
    assert_eq!(output.status.code(), Some(1));
    let result: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(result["status"], "failed");
    assert_eq!(result["canary"]["passed"], false);
    assert_eq!(result["known_good_version"], "0.8.5");
}

#[test]
fn secret_string_data_is_compared_to_persisted_data_without_disclosure() {
    let fixture = Fixture::new();
    let output = fixture.run("secret-string-data");
    assert!(
        output.status.success(),
        "persisted Secret bytes must converge: {}",
        String::from_utf8_lossy(&output.stdout)
    );
    assert!(!String::from_utf8_lossy(&output.stdout).contains("fixture-secret-value"));
}

#[test]
fn failed_canary_resumes_the_same_attempt_without_reapplying_helm() {
    let fixture = Fixture::new();
    let failed = fixture.run("canary-fails");
    assert_eq!(failed.status.code(), Some(1));
    let resumed = fixture.run("healthy");
    assert!(
        resumed.status.success(),
        "{}",
        String::from_utf8_lossy(&resumed.stdout)
    );
    let result: Value = serde_json::from_slice(&resumed.stdout).unwrap();
    assert_eq!(result["resumed"], true);
    assert_eq!(fixture.calls().matches("\"helm\", \"upgrade\"").count(), 1);
    assert_eq!(result["from_version"], "0.8.5");
}

#[test]
fn every_persisted_phase_resumes_without_replaying_a_completed_apply() {
    // Drain and migration are Helm-owned hooks within Apply. Their individual
    // interruption matrix is a separate real-cluster gate, not this fixture.
    for phase in ["validate", "apply", "converge", "canary", "commit"] {
        let fixture = Fixture::new();
        let interrupted = fixture.run(&format!("interrupt-after-{phase}"));
        assert!(!interrupted.status.success(), "{phase} must interrupt");
        let resumed = fixture.run("healthy");
        assert!(
            resumed.status.success(),
            "{phase}: {}",
            String::from_utf8_lossy(&resumed.stdout)
        );
        let result: Value = serde_json::from_slice(&resumed.stdout).unwrap();
        assert_eq!(result["resumed"], true, "{phase}");
        assert_eq!(
            fixture.calls().matches("\"helm\", \"upgrade\"").count(),
            1,
            "{phase}"
        );
    }
}

#[test]
fn stale_checkpoint_version_refuses_concurrent_coordinators() {
    // Kubernetes updates must carry resourceVersion; stale writes return 409:
    // https://kubernetes.io/docs/reference/using-api/api-concepts/#resource-versions
    let fixture = Fixture::new();
    assert!(fixture.run("healthy").status.success());
    let conflicting = fixture.run("checkpoint-conflict");
    assert!(
        !conflicting.status.success(),
        "stale checkpoint cannot be overwritten"
    );
    assert!(String::from_utf8_lossy(&conflicting.stdout).contains("checkpoint"));
    assert_eq!(fixture.calls().matches("\"helm\", \"upgrade\"").count(), 1);
}

#[test]
fn schema_contract_and_unverifiable_database_refuse_before_any_mutation() {
    for scenario in [
        "schema-contract",
        "schema-unknown",
        "schema-probe-fails",
        "schema-metadata-mismatch",
    ] {
        let fixture = Fixture::new();
        let output = fixture.run(scenario);
        fixture.assert_refused_without_upgrade(&output);
        for mutation in ["apply", "create", "replace"] {
            assert!(
                !fixture
                    .calls()
                    .contains(&format!("\"kubectl\", \"{mutation}\"")),
                "{scenario}"
            );
        }
    }
}

#[test]
fn explicit_forward_only_allows_pending_contract_and_records_decision() {
    let fixture = Fixture::new();
    fixture.values(json!({"api": {"migrate": {"forwardOnly": true}}}));
    let output = fixture.run("schema-contract");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stdout)
    );
    let record: Value =
        serde_json::from_slice(&fs::read(fixture.path("record.json")).unwrap()).unwrap();
    assert!(record["plan"]
        .as_array()
        .unwrap()
        .iter()
        .any(|line| line.as_str().unwrap().contains("forward-only")));
}

#[test]
fn failed_helm_hook_cannot_record_unexecuted_phases_as_completed() {
    let fixture = Fixture::new();
    let failed = fixture.run("helm-hook-fails");
    assert!(!failed.status.success());
    let record: Value =
        serde_json::from_slice(&fs::read(fixture.path("record.json")).unwrap()).unwrap();
    assert_eq!(record["completed"], json!(["plan", "validate"]));
    assert_eq!(record["drain_completed"], false);
}

#[test]
fn lost_success_reply_reconciles_the_helm_revision_without_a_second_apply() {
    let fixture = Fixture::new();
    assert!(!fixture.run("helm-success-reply-lost").status.success());
    let resumed = fixture.run("healthy");
    assert!(
        resumed.status.success(),
        "{}",
        String::from_utf8_lossy(&resumed.stdout)
    );
    assert_eq!(fixture.calls().matches("\"helm\", \"upgrade\"").count(), 1);
}

#[test]
fn every_retained_platform_image_pin_must_match_the_target() {
    for component in ["api", "worker", "dispatcher", "ui", "mailAdapter"] {
        let fixture = Fixture::new();
        fixture.values(json!({component: {"image": {"tag": "0.8.4"}}}));
        fixture.assert_refused_without_upgrade(&fixture.run("healthy"));
    }
}

#[test]
fn missing_owned_object_is_a_failed_convergence_with_recovery() {
    let fixture = Fixture::new();
    let failed = fixture.run("missing-object");
    assert!(!failed.status.success());
    let result: Value = serde_json::from_slice(&failed.stdout).unwrap();
    assert_eq!(result["status"], "failed");
    assert_eq!(result["convergence"]["manifest_matches"], false);
    assert!(result["fail_forward"]["command"]
        .as_str()
        .unwrap()
        .contains("upgrade"));
}

#[test]
fn corrupt_checkpoint_status_cannot_invent_a_known_good_idle_release() {
    let fixture = Fixture::new();
    fs::write(fixture.path("record.json"), "not valid json").unwrap();
    let status = fixture.run_status();
    let result: Value = serde_json::from_slice(&status.stdout).unwrap();
    assert_eq!(result["upgrade"]["status"], "unavailable");
    assert!(result["upgrade"]["known_good_version"].is_null());
}

#[test]
fn operator_forward_only_flag_is_a_real_upgrade_input() {
    let fixture = Fixture::new();
    let output = fixture.run_args("schema-contract", &["--forward-only"]);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let values: Value =
        serde_json::from_slice(&fs::read(fixture.path("applied-values.json")).unwrap()).unwrap();
    assert_eq!(values["api"]["migrate"]["forwardOnly"], true);
}

#[test]
fn installed_release_with_missing_schema_tracking_is_not_an_empty_install() {
    let fixture = Fixture::new();
    fixture.assert_refused_without_upgrade(&fixture.run("schema-null"));
}

#[test]
fn missing_helm_hook_evidence_cannot_commit_the_target() {
    let fixture = Fixture::new();
    assert!(!fixture.run("missing-hooks").status.success());
}

#[test]
fn same_revision_id_with_different_source_content_refuses_before_mutation() {
    let fixture = Fixture::new();
    fixture.assert_refused_without_upgrade(&fixture.run("schema-content-mismatch"));
}

#[test]
fn successful_api_responses_cannot_hide_lost_retained_agent_identities() {
    let fixture = Fixture::new();
    let failed = fixture.run("lost-agents");
    assert!(!failed.status.success());
    let result: Value = serde_json::from_slice(&failed.stdout).unwrap();
    assert_eq!(result["status"], "failed");
    assert_eq!(result["canary"]["passed"], false);
}

#[test]
fn offline_dry_run_does_not_claim_to_have_inspected_the_source() {
    let fixture = Fixture::new();
    let output = fixture.run_args("healthy", &["--dry-run"]);
    assert!(output.status.success());
    assert!(fixture.calls().is_empty());
    assert!(String::from_utf8_lossy(&output.stdout).contains("not inspected"));
    let text = String::from_utf8_lossy(&output.stdout);
    assert!(text.contains("helm status acme-bot -n upgrade-test -o json"));
    assert!(text.contains("helm get metadata acme-bot -n upgrade-test --revision"));
    assert!(
        text.contains("Command template (revision resolved from source Helm status at execution)")
    );
}

#[test]
fn failed_helm_revision_is_not_inferred_as_known_good() {
    let fixture = Fixture::new();
    assert!(!fixture.run("helm-failed").status.success());
    let record: Value =
        serde_json::from_slice(&fs::read(fixture.path("record.json")).unwrap()).unwrap();
    assert!(record["known_good_version"].is_null());
}

#[test]
fn resumed_verification_cannot_reuse_a_stale_convergence_result() {
    for phase in ["converge", "canary", "commit"] {
        let fixture = Fixture::new();
        assert!(!fixture
            .run(&format!("interrupt-after-{phase}"))
            .status
            .success());
        let resumed = fixture.run("wrong-image");
        assert!(
            !resumed.status.success(),
            "{phase}: stale proof cannot commit drifted images"
        );
        assert_eq!(fixture.calls().matches("\"helm\", \"upgrade\"").count(), 1);
    }
}

#[test]
fn successful_same_version_rerun_revalidates_without_reapplying_helm() {
    let fixture = Fixture::new();
    assert!(fixture.run("healthy").status.success());
    let rerun = fixture.run("healthy");
    assert!(rerun.status.success());
    assert_eq!(fixture.calls().matches("\"helm\", \"upgrade\"").count(), 1);
    let result: Value = serde_json::from_slice(&rerun.stdout).unwrap();
    assert_eq!(result["unchanged"], true);
}

#[test]
fn new_attempt_checkpoints_agent_identities_added_since_previous_success() {
    let fixture = Fixture::new();
    assert!(fixture.run("healthy").status.success());
    let rerun = fixture.run("additional-agents");
    assert!(
        rerun.status.success(),
        "{}",
        String::from_utf8_lossy(&rerun.stdout)
    );
}

#[test]
fn malformed_durable_phase_state_is_preserved_before_any_new_mutation() {
    for corruption in ["phase-order", "status"] {
        let fixture = Fixture::new();
        assert!(fixture.run("healthy").status.success());
        let mut record: Value =
            serde_json::from_slice(&fs::read(fixture.path("record.json")).unwrap()).unwrap();
        if corruption == "phase-order" {
            record["completed"] = json!(["plan", "apply"]);
        } else {
            record["status"] = "invented-status".into();
        }
        let bytes = serde_json::to_vec(&record).unwrap();
        fs::write(fixture.path("record.json"), &bytes).unwrap();
        fs::write(fixture.path("calls.jsonl"), "").unwrap();
        fixture.assert_refused_without_upgrade(&fixture.run("healthy"));
        assert_eq!(fs::read(fixture.path("record.json")).unwrap(), bytes);
    }
}

#[test]
fn retained_runner_image_pin_must_match_the_upgrade_target() {
    let fixture = Fixture::new();
    fixture.values(json!({"agentSandbox": {"runner": {"tag": "0.8.4"}}}));
    fixture.assert_refused_without_upgrade(&fixture.run("healthy"));
}

#[test]
fn malformed_forward_only_parent_returns_structured_refusal_without_mutation() {
    for api in [json!("invalid"), json!({"migrate": "invalid"})] {
        let fixture = Fixture::new();
        fixture.values(json!({"api": api}));
        let output = fixture.run_args("healthy", &["--forward-only"]);
        fixture.assert_refused_without_upgrade(&output);
        let result: Value = serde_json::from_slice(&output.stdout)
            .expect("invalid retained values must produce a structured error, never panic");
        assert!(result["error"].as_str().unwrap().contains("api"));
        assert!(!fixture.path("record.json").exists());
    }
}

#[test]
fn matching_incomplete_upgrade_recovers_schema_probe_without_a_running_api() {
    let fixture = Fixture::new();
    assert!(!fixture.run("helm-hook-fails").status.success());
    fs::write(fixture.path("api-unavailable"), "true").unwrap();
    let output = fixture.run("recovery-api-down");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stdout)
    );
    assert!(fixture.calls().contains("upgrade-database-recovery"));
    let result: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(result["status"], "succeeded");
    assert_eq!(result["known_good_version"], "0.9.0");
}

#[test]
fn unavailable_api_recovery_refuses_unverified_checkpoint_artifact_or_database() {
    for scenario in [
        "recovery-no-checkpoint",
        "recovery-changed-chart",
        "recovery-db-mismatch",
        "recovery-db-fails",
    ] {
        let fixture = Fixture::new();
        if scenario != "recovery-no-checkpoint" {
            assert!(!fixture.run("helm-hook-fails").status.success());
        }
        if scenario == "recovery-changed-chart" {
            fs::write(fixture.path("candidate-chart"), "different artifact").unwrap();
        }
        fs::write(fixture.path("api-unavailable"), "true").unwrap();
        fs::write(fixture.path("calls.jsonl"), "").unwrap();
        let previous = fs::read(fixture.path("record.json")).ok();
        let output = fixture.run(scenario);
        fixture.assert_refused_without_upgrade(&output);
        assert_eq!(
            previous,
            fs::read(fixture.path("record.json")).ok(),
            "{scenario}"
        );
        if scenario != "recovery-db-fails" {
            assert!(
                !fixture.calls().contains("upgrade-database-recovery"),
                "{scenario}"
            );
        }
    }
}

#[test]
fn running_pod_image_identity_is_observed_and_stale_or_missing_images_refuse() {
    let healthy = Fixture::new();
    let output = healthy.run("healthy");
    assert!(output.status.success());
    let result: Value = serde_json::from_slice(&output.stdout).unwrap();
    let images = result["convergence"]["observed_images"]
        .as_array()
        .expect("actual running image observations are part of convergence evidence");
    assert_eq!(images.len(), 1);
    assert_eq!(images[0]["container"], "api");
    assert!(images[0]["image_id"]
        .as_str()
        .unwrap()
        .ends_with(&"d".repeat(64)));
    for scenario in [
        "wrong-running-image",
        "missing-running-image-id",
        "missing-running-pod",
        "stale-extra-pod",
    ] {
        let fixture = Fixture::new();
        let output = fixture.run(scenario);
        assert_eq!(output.status.code(), Some(1), "{scenario}");
        let result: Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(result["status"], "failed", "{scenario}");
        assert_eq!(result["convergence"]["images"], false, "{scenario}");
        assert_eq!(result["known_good_version"], "0.8.5", "{scenario}");
    }
}

#[test]
fn recovery_replans_from_the_actual_new_database_revision() {
    let fixture = Fixture::new();
    assert!(!fixture.run("helm-hook-fails").status.success());
    fs::write(fixture.path("api-unavailable"), "true").unwrap();
    let output = fixture.run("recovery-db-advanced");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stdout)
    );
    let record: Value =
        serde_json::from_slice(&fs::read(fixture.path("record.json")).unwrap()).unwrap();
    assert_eq!(record["schema_decision"]["current_revision"], "0040");
    assert_eq!(record["schema_decision"]["pending"], json!([]));
}

#[test]
fn recovery_refuses_wrong_catalog_ambiguous_owner_or_running_api_probe_failure() {
    for scenario in [
        "recovery-db-unknown",
        "recovery-db-byo",
        "recovery-running-api-probe-fails",
        "recovery-duplicate-db",
        "recovery-foreign-namespace",
        "recovery-live-catalog-mismatch",
    ] {
        let fixture = Fixture::new();
        if scenario == "recovery-db-byo" {
            fixture.values(json!({"postgres": {"deploy": false}}));
        }
        assert!(!fixture.run("helm-hook-fails").status.success());
        fs::write(fixture.path("api-unavailable"), "true").unwrap();
        fs::write(fixture.path("calls.jsonl"), "").unwrap();
        let before = fs::read(fixture.path("record.json")).unwrap();
        let output = fixture.run(scenario);
        fixture.assert_refused_without_upgrade(&output);
        assert_eq!(
            before,
            fs::read(fixture.path("record.json")).unwrap(),
            "{scenario}"
        );
        if !matches!(
            scenario,
            "recovery-db-unknown" | "recovery-live-catalog-mismatch"
        ) {
            assert!(
                !fixture.calls().contains("upgrade-database-recovery"),
                "{scenario}"
            );
        }
    }
}

#[test]
fn init_container_image_identity_uses_the_same_running_image_contract() {
    let fixture = Fixture::new();
    let output = fixture.run("init-image-healthy");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stdout)
    );
    let result: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(
        result["convergence"]["observed_images"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    let missing = Fixture::new();
    let output = missing.run("init-image-missing-id");
    assert_eq!(output.status.code(), Some(1));
    let result: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(result["convergence"]["images"], false);
}

#[test]
fn target_schema_metadata_must_bind_the_effective_api_image_before_any_write() {
    for scenario in [
        "metadata-image-missing",
        "metadata-image-empty",
        "metadata-image-invalid",
        "metadata-image-mismatch",
        "rendered-api-image-mismatch",
        "rendered-api-missing",
        "rendered-api-duplicate",
    ] {
        let fixture = Fixture::new();
        let output = fixture.run(scenario);
        fixture.assert_refused_without_upgrade(&output);
        assert!(
            !fixture.path("record.json").exists(),
            "{scenario}: wrote checkpoint before image validation"
        );
        let json: Value = serde_json::from_slice(&output.stdout).unwrap();
        assert!(
            json["fix"].as_str().is_some_and(|fix| !fix.is_empty()),
            "{scenario}: {json}"
        );
    }
}

#[test]
fn retained_repository_override_cannot_self_authorize_schema_compatibility() {
    let fixture = Fixture::new();
    fixture.values(json!({"api":{"image":{"repository":"example.com/acme-api", "tag":"0.9.0"}}}));
    let output = fixture.run("healthy");
    fixture.assert_refused_without_upgrade(&output);
    assert!(!fixture.path("record.json").exists());
    let json: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert!(
        json["fix"].as_str().is_some_and(|fix| !fix.is_empty()),
        "{json}"
    );

    let fixture = Fixture::new();
    fixture.values(
        json!({"api":{"image":{"repository":"ghcr.io/curie-eng/curie-api", "tag":"0.9.0"}}}),
    );
    let output = fixture.run("healthy");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stdout)
    );
}

// Kernel semantics: https://man7.org/linux/man-pages/man2/PR_SET_PDEATHSIG.2const.html
// Keep the process supervisor test-only so reversing the product's new libc
// dependency still compiles and fails on the actual unguarded behavior.
#[cfg(target_os = "linux")]
fn assert_upgrade_owner_death(signal: &str) {
    let fixture = Fixture::new();
    fs::write(
        fixture.path("owner-supervisor.py"),
        include_str!("data/upgrade-owner-supervisor.py"),
    )
    .unwrap();
    let command = fixture.command("owner-death", &[]);
    let output = Command::new("/usr/bin/python3")
        .arg(fixture.path("owner-supervisor.py"))
        .arg(signal)
        .arg(env!("CARGO_BIN_EXE_curie"))
        .args(command.get_args())
        .envs(command.get_envs().map(|(key, value)| (key, value.unwrap())))
        .env("TMPDIR", fixture.temp.path())
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "owned process proof failed: {} / {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let proof: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(proof["owner_exited"], true);
    assert_eq!(proof["direct_child_exited"], true);
    assert_eq!(proof["after_owner_mutation"], false);
    assert_eq!(proof["cleanup_complete"], true);
}

#[cfg(target_os = "linux")]
#[test]
fn sigkill_of_upgrade_cli_stops_owned_helm_before_later_mutation() {
    assert_upgrade_owner_death("SIGKILL");
}

#[cfg(target_os = "linux")]
#[test]
fn sigterm_of_upgrade_cli_stops_owned_helm_before_later_mutation() {
    assert_upgrade_owner_death("SIGTERM");
}

#[cfg(target_os = "linux")]
#[test]
fn sigkill_after_apply_resumes_same_command_and_rechecks_live_images() {
    for scenario in ["resume-after-apply", "resume-after-apply-drift"] {
        let fixture = Fixture::new();
        fs::write(
            fixture.path("owner-supervisor.py"),
            include_str!("data/upgrade-owner-supervisor.py"),
        )
        .unwrap();
        let command = fixture.command("pause-after-apply", &[]);
        let output = Command::new("/usr/bin/python3")
            .arg(fixture.path("owner-supervisor.py"))
            .arg(scenario)
            .arg(env!("CARGO_BIN_EXE_curie"))
            .args(command.get_args())
            .envs(command.get_envs().map(|(key, value)| (key, value.unwrap())))
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let proof: Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(proof["owner_killed"], true);
        assert_eq!(proof["old_observer_exited"], true);
        assert_eq!(proof["cleanup_complete"], true);
        assert_eq!(proof["helm_applies"], 1);
        assert_eq!(proof["fresh_convergence"], true, "{proof}");
        assert_eq!(proof["resume_output"]["resumed"], true);
        if scenario == "resume-after-apply" {
            assert_eq!(proof["resume_exit"], 0);
            assert_eq!(proof["resume_output"]["status"], "succeeded");
            assert_eq!(proof["fresh_canary"], true);
            assert_eq!(proof["record"]["known_good_version"], "0.9.0");
        } else {
            assert_eq!(proof["resume_exit"], 1);
            assert_eq!(proof["resume_output"]["convergence"]["images"], false);
            assert_ne!(proof["record"]["known_good_version"], "0.9.0");
        }
    }
}

#[cfg(target_os = "linux")]
#[test]
fn overlapping_upgrade_refuses_before_checkpoint_and_reacquires_after_owner_exit() {
    let fixture = Fixture::new();
    let supervisor = fixture.path("operation-supervisor.py");
    fs::write(
        &supervisor,
        include_str!("data/upgrade-operation-supervisor.py"),
    )
    .unwrap();
    let command = fixture.command("owner-death", &[]);
    let mut process = Command::new("/usr/bin/python3");
    process
        .arg(supervisor)
        .arg(command.get_program())
        .args(command.get_args());
    for (key, value) in command.get_envs() {
        process.env(key, value.unwrap());
    }
    let output = process.output().unwrap();
    assert!(
        output.status.success(),
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let proof: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(proof["overlap_exit"], 3);
    assert_eq!(proof["alias_overlap_exit"], 3);
    assert_eq!(proof["independent_target_exit"], 0);
    assert_eq!(proof["direct_child_holds_same_lock_inode"], true);
    assert_eq!(proof["checkpoint_unchanged"], true);
    assert_eq!(proof["retry_exit"], 0);
    assert_eq!(proof["cleanup_complete"], true);
}

#[test]
fn upgrade_uses_one_private_target_snapshot_despite_ambient_context_change() {
    let fixture = Fixture::new();
    let output = fixture.run("context-drift");
    assert!(
        output.status.success(),
        "{} {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let snapshots: Vec<Value> = fs::read_to_string(fixture.path("snapshot-observations.jsonl"))
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert!(snapshots.len() > 10);
    assert!(snapshots
        .iter()
        .all(|item| item["mode"] == 0o600 && item["server"] == "https://cluster.example.com"));
    assert!(snapshots
        .iter()
        .all(|item| item["path"] == snapshots[0]["path"]));
    assert!(!std::path::Path::new(snapshots[0]["path"].as_str().unwrap()).exists());
    assert!(!String::from_utf8_lossy(&output.stdout).contains("fixture-kubeconfig-token"));
    assert!(!String::from_utf8_lossy(&output.stderr).contains("fixture-kubeconfig-token"));
}

#[cfg(target_os = "linux")]
#[test]
fn unsafe_local_upgrade_state_refuses_without_checkpoint_or_helm_mutation() {
    let fixture = Fixture::new();
    fs::create_dir_all(fixture.path("state")).unwrap();
    fs::create_dir(fixture.path("redirected-state")).unwrap();
    std::os::unix::fs::symlink(
        fixture.path("redirected-state"),
        fixture.path("state/curie"),
    )
    .unwrap();
    let output = fixture.run("healthy");
    fixture.assert_refused_without_upgrade(&output);
    assert!(!fixture.path("record.json").exists());
    let value: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert!(value["fix"].as_str().is_some_and(|fix| !fix.is_empty()));
    assert!(fs::read_dir(fixture.path("redirected-state"))
        .unwrap()
        .next()
        .is_none());
}

#[test]
fn unproven_upgrade_target_refuses_before_checkpoint_and_redacts_raw_credentials() {
    for scenario in [
        "target-config-forbidden",
        "target-config-malformed",
        "target-config-unbound",
        "target-config-ambiguous",
        "target-namespace-forbidden",
        "target-namespace-wrong",
        "target-namespace-missing-uid",
    ] {
        let fixture = Fixture::new();
        let output = fixture.run(scenario);
        fixture.assert_refused_without_upgrade(&output);
        assert!(!fixture.path("record.json").exists());
        let value: Value = serde_json::from_slice(&output.stdout).unwrap();
        assert!(value["fix"].as_str().is_some_and(|fix| !fix.is_empty()));
        assert!(!String::from_utf8_lossy(&output.stdout).contains("fixture-kubeconfig-token"));
        assert!(!String::from_utf8_lossy(&output.stderr).contains("fixture-kubeconfig-token"));
        assert!(!fs::read_dir(fixture.temp.path()).unwrap().any(|item| item
            .unwrap()
            .file_name()
            .to_string_lossy()
            .starts_with("curie-helm-values-")));
    }
}

#[cfg(target_os = "linux")]
#[test]
fn redirected_or_shared_upgrade_lock_refuses_before_new_checkpoint_write() {
    for mode in ["symlink", "hardlink", "world-readable"] {
        let fixture = Fixture::new();
        assert!(fixture.run("healthy").status.success());
        let before = fs::read(fixture.path("record.json")).unwrap();
        let directory = fixture.path("state/curie/upgrades");
        let lock = fs::read_dir(&directory)
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path();
        match mode {
            "symlink" => {
                fs::remove_file(&lock).unwrap();
                fs::write(fixture.path("redirected-lock"), "untouched").unwrap();
                std::os::unix::fs::symlink(fixture.path("redirected-lock"), &lock).unwrap();
            }
            "hardlink" => fs::hard_link(&lock, fixture.path("second-lock-name")).unwrap(),
            _ => fs::set_permissions(&lock, fs::Permissions::from_mode(0o644)).unwrap(),
        }
        fs::write(fixture.path("calls.jsonl"), "").unwrap();
        let output = fixture.run("healthy");
        fixture.assert_refused_without_upgrade(&output);
        assert_eq!(fs::read(fixture.path("record.json")).unwrap(), before);
        if mode == "symlink" {
            assert_eq!(
                fs::read_to_string(fixture.path("redirected-lock")).unwrap(),
                "untouched"
            );
        }
    }
}

#[cfg(target_os = "linux")]
#[test]
fn world_writable_upgrade_state_refuses_before_creating_ownership() {
    let fixture = Fixture::new();
    fs::create_dir(fixture.path("state")).unwrap();
    fs::set_permissions(fixture.path("state"), fs::Permissions::from_mode(0o777)).unwrap();
    let output = fixture.run("healthy");
    fixture.assert_refused_without_upgrade(&output);
    assert!(!fixture.path("state/curie").exists());
    assert!(!fixture.path("record.json").exists());
}

#[test]
fn upgrade_offline_plan_names_capture_namespace_read_and_ownership_without_processes() {
    let fixture = Fixture::new();
    let output = fixture
        .command("healthy", &["--dry-run"])
        .env("PATH", "/missing-tools")
        .output()
        .unwrap();
    assert!(output.status.success());
    let value: Value = serde_json::from_slice(&output.stdout).unwrap();
    let text = value.to_string();
    assert!(text.contains("kubectl config view --minify --raw -o json --flatten"));
    assert!(text.contains("kubectl get namespace upgrade-test -o json"));
    assert!(text.contains("same-host ownership"));
    assert!(text.contains("templates/worker-upgrade-drain.yaml"));
    assert!(text.contains("templates/schema-migrate.yaml"));
    assert!(text.contains("kubectl get jobs,pods"));
    assert!(text.contains("jsonpath-as-json={.metadata}"));
    assert!(text.contains("exact original pending Helm revision"));
    assert!(fixture.calls().is_empty());
    assert!(!fixture.path("state").exists());
}

#[test]
fn helm_target_overrides_refuse_before_upgrade_target_discovery_without_leaking_values() {
    // Helm3.16.4 EnvSettings binds each of these independently of KUBECONFIG:
    // https://github.com/helm/helm/blob/v3.16.4/pkg/cli/environment.go
    // Actual Helm endpoint probe contacted only HELM_KUBEAPISERVER's endpoint
    // with a conflicting kubeconfig, and only the captured endpoint once unset.
    for name in [
        "HELM_KUBEAPISERVER",
        "HELM_KUBECONTEXT",
        "HELM_KUBECAFILE",
        "HELM_KUBETOKEN",
        "HELM_KUBEASUSER",
        "HELM_KUBEASGROUPS",
        "HELM_KUBEINSECURE_SKIP_TLS_VERIFY",
        "HELM_KUBETLS_SERVER_NAME",
    ] {
        let fixture = Fixture::new();
        let output = fixture
            .command("healthy", &[])
            .env(name, "fixture-helm-override-private-value")
            .output()
            .unwrap();
        assert_eq!(
            output.status.code(),
            Some(2),
            "{name}: {}",
            String::from_utf8_lossy(&output.stdout)
        );
        let value: Value = serde_json::from_slice(&output.stdout).unwrap();
        assert!(value["error"].as_str().unwrap().contains(name));
        assert!(value["fix"].as_str().unwrap().contains("kubeconfig"));
        assert!(!String::from_utf8_lossy(&output.stdout)
            .contains("fixture-helm-override-private-value"));
        assert!(!String::from_utf8_lossy(&output.stderr)
            .contains("fixture-helm-override-private-value"));
        assert!(!fixture.path("ambient-context-changed").exists());
        assert!(!fixture.path("record.json").exists());
        assert!(!fixture.path("state").exists());
        let empty = fixture
            .command("healthy", &[])
            .env(name, "")
            .output()
            .unwrap();
        assert!(
            empty.status.success(),
            "{name}: {}",
            String::from_utf8_lossy(&empty.stdout)
        );
    }
}

#[test]
fn source_known_good_uses_exact_revision_metadata_when_status_omits_chart() {
    let fixture = Fixture::new();
    let output = fixture.run("canary-fails");
    assert_eq!(output.status.code(), Some(1));
    let result: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(result["known_good_version"], "0.8.5");
    let calls = fixture.calls();
    assert!(calls.contains("\"helm\", \"get\", \"metadata\""));
    assert!(calls.contains("\"--revision\", \"1\""));
}

#[test]
fn mismatched_source_metadata_refuses_before_checkpoint_or_upgrade() {
    for scenario in [
        "source-metadata-denied",
        "source-metadata-malformed",
        "source-metadata-wrong-name",
        "source-metadata-wrong-namespace",
        "source-metadata-wrong-revision",
        "source-metadata-wrong-chart",
        "source-metadata-wrong-version",
        "source-metadata-failed",
        "source-metadata-missing-revision",
    ] {
        let fixture = Fixture::new();
        let output = fixture.run(scenario);
        fixture.assert_refused_without_upgrade(&output);
        assert_eq!(output.status.code(), Some(1), "{scenario}");
        let result: Value = serde_json::from_slice(&output.stdout).unwrap();
        assert!(
            result["fix"]
                .as_str()
                .is_some_and(|value| !value.is_empty()),
            "{scenario}"
        );
        assert!(!fixture.path("record.json").exists(), "{scenario}");
        for stream in [&output.stdout, &output.stderr] {
            assert!(!String::from_utf8_lossy(stream)
                .contains("synthetic-source-metadata-secret-sentinel"));
        }
    }
}

#[test]
fn invalid_source_status_identity_refuses_before_metadata_and_checkpoint() {
    for scenario in [
        "source-status-wrong-name",
        "source-status-wrong-namespace",
        "source-status-missing-revision",
        "source-status-zero-revision",
        "source-status-string-revision",
    ] {
        let fixture = Fixture::new();
        let output = fixture.run(scenario);
        fixture.assert_refused_without_upgrade(&output);
        assert_eq!(output.status.code(), Some(1));
        let result: Value = serde_json::from_slice(&output.stdout).unwrap();
        assert!(result["fix"]
            .as_str()
            .is_some_and(|value| !value.is_empty()));
        assert!(!fixture.calls().contains("\"helm\", \"get\", \"metadata\""));
        assert!(!fixture.path("record.json").exists());
    }
}

#[test]
fn bounded_source_metadata_read_stops_its_child_and_preserves_recovery() {
    let fixture = Fixture::new();
    let started = std::time::Instant::now();
    let output = fixture.run("source-metadata-hung");
    assert!(started.elapsed() < std::time::Duration::from_secs(20));
    fixture.assert_refused_without_upgrade(&output);
    assert_eq!(output.status.code(), Some(1));
    let result: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert!(result["error"]
        .as_str()
        .unwrap()
        .contains("metadata read timed out"));
    assert!(result["fix"]
        .as_str()
        .is_some_and(|value| !value.is_empty()));
    assert!(!fixture.path("record.json").exists());
    for stream in [&output.stdout, &output.stderr] {
        assert!(
            !String::from_utf8_lossy(stream).contains("synthetic-source-metadata-secret-sentinel")
        );
    }
    let pid = fs::read_to_string(fixture.path("metadata-pid")).unwrap();
    let absent = Command::new("/usr/bin/python3").args(["-c", "import os,sys; p=int(sys.argv[1]);\ntry: os.kill(p,0)\nexcept ProcessLookupError: sys.exit(0)\nsys.exit(1)", &pid]).status().unwrap();
    assert!(absent.success(), "timed-out metadata child must be absent");
}

#[test]
fn late_pending_upgrade_requires_durable_fresh_operation_before_target_forward_recovery() {
    // Helm3.16.4 persists upgrade --labels before hooks, but rollback copies them.
    // https://github.com/helm/helm/blob/v3.16.4/pkg/action/upgrade.go
    // https://github.com/helm/helm/blob/v3.16.4/pkg/action/rollback.go
    let fixture = Fixture::new();
    let first = fixture.run("late-pending");
    assert!(!first.status.success());
    let record: Value =
        serde_json::from_slice(&fs::read(fixture.path("record.json")).unwrap()).unwrap();
    let operation = &record["operation"];
    assert!(operation["id"]
        .as_str()
        .is_some_and(|id| uuid::Uuid::parse_str(id).is_ok()));
    assert_eq!(operation["expected_revision"], 2);
    let calls: Vec<Vec<String>> = fixture
        .calls()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    let upgrade = calls
        .iter()
        .find(|args| args[0] == "helm" && args[1] == "upgrade")
        .unwrap();
    assert!(upgrade.iter().any(|arg| arg == "--labels"));
    assert_eq!(record["completed"], json!(["plan", "validate"]));
    let second = fixture.run("late-pending");
    assert!(
        second.status.success(),
        "{}",
        String::from_utf8_lossy(&second.stdout)
    );
    let calls: Vec<Vec<String>> = fixture
        .calls()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(
        calls
            .iter()
            .filter(|args| args[0] == "helm" && args[1] == "upgrade")
            .count(),
        1
    );
    let rollback = calls
        .iter()
        .find(|args| args[0] == "helm" && args[1] == "rollback")
        .unwrap();
    assert_eq!(rollback[3], "2");
    assert!(!rollback
        .iter()
        .any(|arg| matches!(arg.as_str(), "--no-hooks" | "--force")));
    let final_record: Value =
        serde_json::from_slice(&fs::read(fixture.path("record.json")).unwrap()).unwrap();
    assert_eq!(final_record["status"], "succeeded");
    assert!(final_record["resumed"].as_bool().unwrap());
}

#[test]
fn pending_forward_recovery_refuses_missing_or_uncertain_phase_and_owner_evidence() {
    for scenario in [
        "pending-rollback",
        "missing-local-witness",
        "truncated-local-witness",
        "wrong-checkpoint-uid",
        "wrong-release-uid",
        "wrong-marker",
        "wrong-revision",
        "active-hook",
        "missing-hook",
        "orphan-hook-pod",
        "wrong-pod-owner",
        "terminating-hook",
        "unknown-hook",
        "schema-not-target",
        "malformed-hook-active",
        "replacement-hook-uid",
        "ephemeral-hook-running",
        "changed-pending-manifest",
    ] {
        let fixture = Fixture::new();
        assert!(!fixture.run("late-pending").status.success());
        if matches!(
            scenario,
            "missing-local-witness" | "truncated-local-witness"
        ) {
            let locks = fixture.path("state/curie/upgrades");
            for entry in fs::read_dir(locks).unwrap() {
                let path = entry.unwrap().path();
                if path.is_file() {
                    fs::write(
                        path,
                        if scenario == "missing-local-witness" {
                            ""
                        } else {
                            "{"
                        },
                    )
                    .unwrap();
                }
            }
        }
        let before = fs::read(fixture.path("record.json")).unwrap();
        let output = fixture.run(scenario);
        assert!(!output.status.success(), "{scenario}");
        let value: Value = serde_json::from_slice(&output.stdout).unwrap();
        assert!(
            value["fix"].as_str().is_some_and(|fix| !fix.is_empty()),
            "{scenario}: {value}"
        );
        assert!(
            !fixture.calls().lines().any(|line| {
                let args: Vec<String> = serde_json::from_str(line).unwrap();
                args[0] == "helm" && args[1] == "rollback"
            }),
            "{scenario}"
        );
        assert_eq!(
            before,
            fs::read(fixture.path("record.json")).unwrap(),
            "{scenario}"
        );
    }
}

#[test]
fn forward_recovery_accepts_zero_exit_pending_only_after_original_completion_binding() {
    let fixture = Fixture::new();
    // Helm3.16.4 action.recordRelease logs persistence failure and continues.
    // https://github.com/helm/helm/blob/v3.16.4/pkg/action/action.go
    let first = fixture.run("zero-exit-pending");
    assert!(!first.status.success());
    let record: Value =
        serde_json::from_slice(&fs::read(fixture.path("record.json")).unwrap()).unwrap();
    assert!(record["operation"]["pending_uid"].as_str().is_some());
    let second = fixture.run("late-pending");
    assert!(
        second.status.success(),
        "{}",
        String::from_utf8_lossy(&second.stdout)
    );
    assert_eq!(fixture.calls().matches("\"helm\", \"rollback\"").count(), 1);
}

#[test]
fn forward_recovery_rechecks_original_job_source_and_target_metadata_identity() {
    for (scenario, expected) in [
        (
            "replacement-hook-uid",
            "original retained hook Job UID changed",
        ),
        ("replaced-source-release", "source release UID changed"),
        (
            "source-metadata-wrong-chart",
            "pending target chart metadata changed",
        ),
    ] {
        let fixture = Fixture::new();
        assert!(!fixture.run("late-pending").status.success());
        let before = fs::read(fixture.path("record.json")).unwrap();
        let output = fixture.run(scenario);
        assert!(!output.status.success(), "{scenario}");
        let value: Value = serde_json::from_slice(&output.stdout).unwrap();
        assert!(
            value["error"].as_str().unwrap().contains(expected),
            "{scenario}: {value}"
        );
        assert!(!fixture.calls().contains("\"helm\", \"rollback\""));
        assert_eq!(before, fs::read(fixture.path("record.json")).unwrap());
    }
}

#[test]
fn forward_recovery_ignores_only_test_hooks_and_refuses_additional_executable_hooks() {
    let fixture = Fixture::new();
    assert!(!fixture.run("late-pending").status.success());
    let output = fixture.run("test-only-hook");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stdout)
    );
    for scenario in ["extra-upgrade-hook", "extra-rollback-hook"] {
        let fixture = Fixture::new();
        assert!(!fixture.run("late-pending").status.success());
        let before = fs::read(fixture.path("record.json")).unwrap();
        let output = fixture.run(scenario);
        assert!(!output.status.success());
        assert!(!fixture.calls().contains("\"helm\", \"rollback\""));
        assert_eq!(before, fs::read(fixture.path("record.json")).unwrap());
    }
}

#[test]
fn contract_refusal_returns_actionable_forward_only_recovery_before_mutation() {
    let fixture = Fixture::new();
    let output = fixture.run("schema-contract");
    fixture.assert_refused_without_upgrade(&output);
    assert_eq!(output.status.code(), Some(1));
    let result: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert!(result["error"].as_str().unwrap().contains("contract"));
    assert!(result["fix"]
        .as_str()
        .is_some_and(|fix| fix.contains("--forward-only")));
    assert!(!fixture.path("record.json").exists());
}

#[test]
fn upgrade_pinned_image_uses_actual_repository_digest_authority() {
    let fixture = Fixture::new();
    let output = fixture.run("pinned-image-healthy");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stdout)
    );
    let result: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(result["status"], "succeeded");
    assert_eq!(
        result["convergence"]["observed_images"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    assert!(!fixture.calls().contains("\"get\", \"node\""));
    for scenario in [
        "pinned-image-wrong-digest",
        "pinned-image-wrong-repository",
        "pinned-image-opaque",
        "pinned-image-pod-drift",
    ] {
        let negative = Fixture::new();
        let output = negative.run(scenario);
        assert_eq!(output.status.code(), Some(1), "{scenario}");
        let result: Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(result["convergence"]["images"], false, "{scenario}");
        assert_eq!(result["known_good_version"], "0.8.5");
    }
}

#[test]
fn upgrade_alias_requires_unique_same_node_inventory() {
    let fixture = Fixture::new();
    let output = fixture.run("alias-image-healthy");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stdout)
    );
    assert_eq!(
        fixture
            .calls()
            .matches("\"get\", \"node\", \"acme-node\"")
            .count(),
        1
    );
    let observations = fs::read_to_string(fixture.path("snapshot-observations.jsonl")).unwrap();
    assert!(!observations.is_empty());
    for line in observations.lines() {
        let observation: Value = serde_json::from_str(line).unwrap();
        assert_eq!(observation["server"], "https://cluster.example.com");
        assert_eq!(observation["mode"], 0o600);
        assert!(!PathBuf::from(observation["path"].as_str().unwrap()).exists());
    }
    for scenario in [
        "alias-image-split",
        "alias-image-ambiguous",
        "alias-image-missing",
        "alias-image-wrong-node",
        "alias-image-wrong-digest",
        "alias-image-wrong-repository",
        "alias-image-opaque",
        "alias-image-pod-drift",
        "alias-image-denied",
    ] {
        let negative = Fixture::new();
        let output = negative.run(scenario);
        assert_eq!(output.status.code(), Some(1), "{scenario}");
        let result: Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_ne!(result["status"], "succeeded", "{scenario}");
        if matches!(scenario, "alias-image-denied" | "alias-image-wrong-node") {
            assert!(result["error"]
                .as_str()
                .unwrap()
                .contains("Node image inventory"));
            assert!(result["fix"].as_str().unwrap().contains("get-node"));
        }
        assert!(!String::from_utf8_lossy(&output.stdout).contains("fixture-private-node-denial"));
        assert!(!String::from_utf8_lossy(&output.stderr).contains("fixture-private-node-denial"));
    }
}

#[test]
fn upgrade_dry_run_names_conditional_serving_node_read_without_executing_it() {
    let fixture = Fixture::new();
    let output = fixture.run_args("alias-image-denied", &["--dry-run"]);
    assert!(output.status.success());
    let result: Value = serde_json::from_slice(&output.stdout).unwrap();
    let plan = result.to_string();
    assert!(plan.contains("kubectl get node '<pod-node>' -o json"));
    assert!(plan.contains("not an executable argument"));
    assert!(fixture.calls().is_empty());
}
