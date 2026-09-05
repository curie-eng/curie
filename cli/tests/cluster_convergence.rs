//! Drive the real up/status consumer with recording Helm/Kubernetes processes.
//! Kubernetes Deployment conditions and ContainerStatus shapes follow
//! https://kubernetes.io/docs/concepts/workloads/controllers/deployment/#failed-deployment
//! and https://kubernetes.io/docs/reference/kubernetes-api/workload-resources/pod-v1/.

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::process::{Command, Output};

use serde_json::Value;

struct Fixture(tempfile::TempDir);

impl Fixture {
    fn new() -> Self {
        let temp = tempfile::tempdir().unwrap();
        for name in ["helm", "kubectl"] {
            let path = temp.path().join(name);
            fs::write(&path, include_str!("data/convergence-driver.py")).unwrap();
            fs::set_permissions(path, fs::Permissions::from_mode(0o755)).unwrap();
        }
        fs::write(
            temp.path().join("mixed-rollout.json"),
            include_str!("data/mixed-rollout.json"),
        )
        .unwrap();
        Self(temp)
    }

    fn run(&self, verb: &str, scenario: &str) -> Output {
        self.run_mode(verb, scenario, true, false)
    }

    fn run_mode(&self, verb: &str, scenario: &str, json: bool, dry_run: bool) -> Output {
        let mut command = Command::new(env!("CARGO_BIN_EXE_curie"));
        if json {
            command.arg("--json");
        }
        command.args([
            "cluster",
            verb,
            "--namespace",
            "convergence-test",
            "--release",
            "acme-bot",
        ]);
        if dry_run {
            command.arg("--dry-run");
        }
        if verb == "up" {
            command.args([
                "--chart",
                "charts/curie",
                "--dev",
                "--fake-model",
                "--set",
                "agentSandbox.controller.deploy=false",
                "--set",
                "priorityClasses.platform.create=false",
                "--set",
                "priorityClasses.sandbox.create=false",
            ]);
        }
        command
            .current_dir(
                std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                    .parent()
                    .unwrap(),
            )
            .env("PATH", format!("{}:/usr/bin:/bin", self.0.path().display()))
            .env("CONVERGENCE_DRIVER_ROOT", self.0.path())
            .env("CONVERGENCE_DRIVER_SCENARIO", scenario)
            .output()
            .unwrap()
    }

    fn json(output: &Output) -> Value {
        serde_json::from_slice(&output.stdout).unwrap_or_else(|error| {
            panic!(
                "invalid JSON ({error}): {} / {}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            )
        })
    }
}

#[test]
fn up_and_status_reject_available_old_ready_plus_failed_replacement() {
    for verb in ["up", "status"] {
        let fixture = Fixture::new();
        let output = fixture.run(verb, "mixed");
        assert_eq!(
            output.status.code(),
            Some(1),
            "{verb}: {} / {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        let json = Fixture::json(&output);
        assert!(
            json.to_string().contains("ProgressDeadlineExceeded"),
            "{json}"
        );
        if verb == "status" {
            assert_eq!(json["healthy"], false);
        }
    }
}

#[test]
fn available_only_negative_control_passes_the_broken_rollout_fixture() {
    let fixture: Value = serde_json::from_str(include_str!("data/mixed-rollout.json")).unwrap();
    let conditions = fixture["status"]["conditions"].as_array().unwrap();
    assert!(conditions
        .iter()
        .any(|c| c["type"] == "Available" && c["status"] == "True"));
    assert!(conditions.iter().any(|c| c["type"] == "Progressing"
        && c["status"] == "False"
        && c["reason"] == "ProgressDeadlineExceeded"));
    assert_eq!(fixture["status"]["readyReplicas"], 1);
    assert_eq!(fixture["status"]["unavailableReplicas"], 1);
}

#[test]
fn exact_target_custom_image_and_statefulset_succeed_on_both_paths() {
    for verb in ["up", "status"] {
        let output = Fixture::new().run(verb, "healthy");
        assert!(
            output.status.success(),
            "{verb}: {} / {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        let json = Fixture::json(&output);
        if verb == "status" {
            assert_eq!(json["healthy"], true);
        }
    }
}

#[test]
fn status_checks_generation_replicas_images_surplus_and_hook_outcomes() {
    for scenario in [
        "stale-generation",
        "updated-short",
        "ready-short",
        "unavailable",
        "surplus",
        "wrong-image",
        "missing-image-status",
        "hook-failed",
        "init-failed",
        "container-failed",
        "statefulset-old",
        "missing-workload",
        "target-drift",
        "release-failed",
    ] {
        let output = Fixture::new().run("status", scenario);
        assert_eq!(
            output.status.code(),
            Some(1),
            "{scenario}: {}",
            String::from_utf8_lossy(&output.stdout)
        );
        let json = Fixture::json(&output);
        assert_eq!(json["healthy"], false, "{scenario}: {json}");
        assert!(
            !json["pods"]["unhealthy"].as_array().unwrap().is_empty(),
            "{scenario}: {json}"
        );
        assert!(!String::from_utf8_lossy(&output.stdout).contains("PRIVATE_MESSAGE_SENTINEL"));
        assert!(!String::from_utf8_lossy(&output.stderr).contains("PRIVATE_MESSAGE_SENTINEL"));
    }
}

#[test]
fn status_surfaces_safe_terminal_codes_without_messages_or_secret_values() {
    for (scenario, reason) in [
        ("init-failed", "CrashLoopBackOff"),
        ("container-failed", "OOMKilled"),
        ("hook-failed", "BackoffLimitExceeded"),
    ] {
        let output = Fixture::new().run("status", scenario);
        let json = Fixture::json(&output);
        assert!(json.to_string().contains(reason), "{scenario}: {json}");
        assert!(!json.to_string().contains("PRIVATE_MESSAGE_SENTINEL"));
    }
}

#[test]
fn post_helm_up_waits_for_observed_target_instead_of_accepting_first_snapshot() {
    let fixture = Fixture::new();
    let output = fixture.run("up", "converges-later");
    assert!(
        output.status.success(),
        "{} / {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let count: u64 = fs::read_to_string(fixture.0.path().join("observations"))
        .unwrap()
        .parse()
        .unwrap();
    assert!(
        count >= 2,
        "up must observe convergence after the first stale snapshot"
    );
}

#[test]
fn unrelated_same_named_hook_in_another_namespace_does_not_fail_status() {
    let output = Fixture::new().run("status", "foreign-hook");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stdout)
    );
    assert_eq!(Fixture::json(&output)["healthy"], true);
}

#[test]
fn failed_helm_up_reports_the_actual_hook_reason_and_redacts_pod_reason_text() {
    let output = Fixture::new().run("up", "helm-hook-fails");
    assert_eq!(output.status.code(), Some(1));
    assert!(Fixture::json(&output)
        .to_string()
        .contains("BackoffLimitExceeded"));
    let output = Fixture::new().run("status", "private-pod-reason");
    assert_eq!(output.status.code(), Some(1));
    assert!(!Fixture::json(&output)
        .to_string()
        .contains("PRIVATE_MESSAGE_SENTINEL"));
}

#[test]
fn legitimate_live_scaling_keeps_target_identity_and_rejects_surplus() {
    let output = Fixture::new().run("status", "scaled");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stdout)
    );
    assert_eq!(Fixture::json(&output)["healthy"], true);
    for scenario in ["scaled-surplus", "empty-target"] {
        let output = Fixture::new().run("status", scenario);
        assert_eq!(
            output.status.code(),
            Some(1),
            "{scenario}: {}",
            String::from_utf8_lossy(&output.stdout)
        );
        assert_eq!(Fixture::json(&output)["healthy"], false);
    }
}

#[test]
fn healthy_admission_sidecar_preserves_named_target_image_verification() {
    let output = Fixture::new().run("status", "healthy-sidecar");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stdout)
    );
    let output = Fixture::new().run("status", "unready-sidecar");
    assert_eq!(output.status.code(), Some(1));
    assert_eq!(Fixture::json(&output)["healthy"], false);
}

#[test]
fn bounded_post_helm_read_stops_its_hung_process() {
    let fixture = Fixture::new();
    let started = std::time::Instant::now();
    let output = fixture.run("up", "hung-read");
    assert_eq!(output.status.code(), Some(1));
    assert!(Fixture::json(&output).to_string().contains("timed out"));
    assert!(started.elapsed() < std::time::Duration::from_secs(15));
    let pid = fs::read_to_string(fixture.0.path().join("hung-pid")).unwrap();
    // run_capture's kill_on_drop must clean the timed-out external process.
    for _ in 0..20 {
        if !std::path::Path::new("/proc").join(pid.trim()).exists() {
            return;
        }
        std::thread::sleep(std::time::Duration::from_millis(25));
    }
    panic!("timed-out convergence process survived");
}

#[test]
fn failed_status_keeps_the_human_report_and_recovery_hint() {
    let output = Fixture::new().run_mode("status", "mixed", false, false);
    assert_eq!(output.status.code(), Some(1));
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stdout.contains("namespace convergence-test"),
        "{stdout} / {stderr}"
    );
    assert!(stdout.contains("revision 2"), "{stdout}");
    assert!(stdout.contains("acme-bot-api-new"), "{stdout}");
    assert!(stderr.contains("ProgressDeadlineExceeded"), "{stderr}");
    assert!(
        stderr.contains("Fix:") && stderr.contains("curie cluster up"),
        "{stderr}"
    );
    assert!(!stdout.contains("PRIVATE_MESSAGE_SENTINEL"));
    assert!(!stderr.contains("PRIVATE_MESSAGE_SENTINEL"));
    let healthy = Fixture::new().run_mode("status", "healthy", false, false);
    assert!(healthy.status.success());
    assert!(String::from_utf8_lossy(&healthy.stdout).contains("revision 2"));
    let json = Fixture::new().run("status", "mixed");
    assert_eq!(
        json.stdout
            .split(|byte| *byte == b'\n')
            .filter(|line| !line.is_empty())
            .count(),
        1
    );
    assert_eq!(Fixture::json(&json)["healthy"], false);
}

#[test]
fn offline_dry_runs_expose_the_real_convergence_readset_and_dynamic_inputs() {
    for verb in ["up", "status"] {
        let fixture = Fixture::new();
        let output = fixture.run_mode(verb, "mixed", true, true);
        assert!(
            output.status.success(),
            "{} / {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        let json = Fixture::json(&output);
        let plan = json.to_string();
        assert!(
            plan.contains("helm status acme-bot -n convergence-test -o json"),
            "{verb}: {json}"
        );
        assert!(
            plan.contains("helm get manifest acme-bot -n convergence-test --revision"),
            "{verb}: {json}"
        );
        assert!(
            plan.contains("deployments,statefulsets,daemonsets,pods,jobs"),
            "{verb}: {json}"
        );
        assert!(plan.contains("resolved at runtime"), "{verb}: {json}");
        assert!(
            plan.contains("kubectl get node '<pod-node>' -o json"),
            "{verb}: {json}"
        );
        assert!(plan.contains("requiring get-node access"), "{verb}: {json}");
        if verb == "up" {
            assert!(plan.contains("300 seconds"), "{json}");
        }
        assert!(
            !fixture.0.path().join("calls.jsonl").exists(),
            "dry-run executed a cluster process"
        );
    }
}

#[test]
fn pinned_images_use_reported_manifest_identity_for_regular_and_init_containers() {
    for scenario in ["pinned-healthy", "pinned-pullable"] {
        for verb in ["status", "up"] {
            let output = Fixture::new().run(verb, scenario);
            assert!(
                output.status.success(),
                "{verb}/{scenario}: {} / {}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            if verb == "status" {
                assert_eq!(Fixture::json(&output)["healthy"], true);
            }
        }
    }
}

#[test]
fn pinned_images_reject_mismatched_or_unproven_identity_and_unhealthy_init() {
    for scenario in [
        "pinned-wrong-digest",
        "pinned-wrong-repository",
        "pinned-opaque-id",
        "pinned-pod-drift",
        "pinned-init-failed",
    ] {
        let output = Fixture::new().run("status", scenario);
        assert_eq!(
            output.status.code(),
            Some(1),
            "{scenario}: {}",
            String::from_utf8_lossy(&output.stdout)
        );
        assert_eq!(Fixture::json(&output)["healthy"], false, "{scenario}");
    }
}

#[test]
fn timed_out_observation_retains_last_safe_cause_and_recovery() {
    let fixture = Fixture::new();
    let started = std::time::Instant::now();
    let output = fixture.run("up", "degraded-then-hung");
    assert_eq!(output.status.code(), Some(1));
    let json = Fixture::json(&output);
    assert!(
        json["error"].as_str().unwrap().contains("ImagePullBackOff"),
        "{json}"
    );
    assert!(
        json["error"].as_str().unwrap().contains("timed out"),
        "{json}"
    );
    assert!(
        json["fix"]
            .as_str()
            .unwrap()
            .contains("curie cluster status"),
        "{json}"
    );
    assert!(!json.to_string().contains("PRIVATE_MESSAGE_SENTINEL"));
    assert!(!String::from_utf8_lossy(&output.stderr).contains("PRIVATE_MESSAGE_SENTINEL"));
    assert!(started.elapsed() < std::time::Duration::from_secs(20));
    let pid = fs::read_to_string(fixture.0.path().join("hung-pid")).unwrap();
    for _ in 0..20 {
        if !std::path::Path::new("/proc").join(pid.trim()).exists() {
            return;
        }
        std::thread::sleep(std::time::Duration::from_millis(25));
    }
    panic!("timed-out observation process survived");
}

#[test]
fn sibling_tag_alias_requires_same_node_image_inventory_identity() {
    // Actual Kubernetes 1.31/containerd run: one Node.status.images entry
    // groups the requested tag, reported sibling tag and exact imageID.
    // https://kubernetes.io/docs/reference/kubernetes-api/cluster-resources/node-v1/#NodeStatus
    for verb in ["status", "up"] {
        let fixture = Fixture::new();
        let output = fixture.run(verb, "alias-healthy");
        assert!(
            output.status.success(),
            "{verb}: {}",
            String::from_utf8_lossy(&output.stdout)
        );
        if verb == "status" {
            assert_eq!(Fixture::json(&output)["healthy"], true);
        }
        let calls = fs::read_to_string(fixture.0.path().join("calls.jsonl")).unwrap();
        assert!(calls.contains("\"kubectl\", \"get\", \"node\", \"acme-node\""));
    }
}

#[test]
fn sibling_alias_refuses_unbound_inventory_and_unhealthy_or_stale_containers() {
    for scenario in [
        "alias-split-entry",
        "alias-ambiguous",
        "alias-missing-entry",
        "alias-wrong-node",
        "alias-no-reported-alias",
        "alias-forbidden",
        "alias-wrong-digest",
        "alias-wrong-repository",
        "alias-opaque-id",
        "alias-pod-drift",
        "alias-init-failed",
    ] {
        let output = Fixture::new().run("status", scenario);
        assert_eq!(
            output.status.code(),
            Some(1),
            "{scenario}: {}",
            String::from_utf8_lossy(&output.stdout)
        );
        assert_eq!(Fixture::json(&output)["healthy"], false, "{scenario}");
        assert!(!String::from_utf8_lossy(&output.stdout).contains("PRIVATE_MESSAGE_SENTINEL"));
        assert!(!String::from_utf8_lossy(&output.stderr).contains("PRIVATE_MESSAGE_SENTINEL"));
    }
}

#[test]
fn denied_alias_node_read_returns_structured_up_recovery() {
    let output = Fixture::new().run("up", "alias-forbidden");
    assert_eq!(output.status.code(), Some(1));
    let result = Fixture::json(&output); // Rejects trailing or multiple objects.
    assert!(result["error"]
        .as_str()
        .unwrap()
        .contains("get-node read access"));
    let fix = result["fix"]
        .as_str()
        .expect("denied node read needs structured recovery");
    assert!(
        fix.contains("Node") && fix.contains("digest-pinned") && fix.contains("rerun"),
        "{result}"
    );
    assert!(!String::from_utf8_lossy(&output.stdout).contains("PRIVATE_MESSAGE_SENTINEL"));
    assert!(!String::from_utf8_lossy(&output.stderr).contains("PRIVATE_MESSAGE_SENTINEL"));
}
