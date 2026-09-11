//! Guards for `curie dev lease-expiry-cluster-proof` (#2453).
//!
//! The live scenarios need a task-owned kind cluster, the published worker
//! image that carries #2433, and the authorized test Slack route. These tests
//! pin the command surface and the soak-refusal / default-reclaim / Slack-missing
//! guards so a contributor cannot point the proof at the permanent soak, shorten
//! the 900 s backstop, or run without the test Slack credentials.

use std::fs;
use std::path::PathBuf;
use std::process::Command;

fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_curie")
}

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("..")
}

fn output_text(output: &std::process::Output) -> String {
    String::from_utf8_lossy(&output.stdout).into_owned() + &String::from_utf8_lossy(&output.stderr)
}

fn script() -> PathBuf {
    repo_root().join("cli/scripts/lease-expiry-cluster-proof.sh")
}

#[test]
fn lease_expiry_cluster_proof_script_is_present_and_executable() {
    let path = script();
    assert!(path.is_file(), "missing {}", path.display());
    let mode = fs::metadata(&path).expect("stat").permissions();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert!(
            mode.mode() & 0o111 != 0,
            "lease-expiry-cluster-proof.sh must be executable"
        );
    }
}

#[test]
fn lease_expiry_cluster_proof_self_test_refuses_soak_and_short_backstop() {
    let output = Command::new("bash")
        .arg(script())
        .arg("--self-test")
        .current_dir(repo_root())
        .output()
        .expect("run lease-expiry-cluster-proof --self-test");
    assert!(
        output.status.success(),
        "self-test failed\n{}",
        output_text(&output)
    );
    let text = output_text(&output);
    assert!(
        text.contains("soak namespace curie refused"),
        "self-test must refuse the permanent soak namespace\n{text}"
    );
    assert!(
        text.contains("soak release curie refused"),
        "self-test must refuse the permanent soak release\n{text}"
    );
    assert!(
        text.contains("reclaim_min_idle override refused"),
        "self-test must refuse shortening reclaim_min_idle_ms\n{text}"
    );
    assert!(
        text.contains("missing slack credentials refused"),
        "self-test must refuse a run with no test Slack tokens\n{text}"
    );
    assert!(
        text.contains("default reclaim_min_idle_ms 900000 preserved"),
        "self-test must name the default 900000 backstop\n{text}"
    );
}

#[test]
fn lease_expiry_cluster_proof_refuses_soak_namespace() {
    let output = Command::new(bin())
        .args(["dev", "lease-expiry-cluster-proof"])
        .env("CURIE_BIN", bin())
        .env("CURIE_E2E_NAMESPACE", "curie")
        .env("CURIE_E2E_RELEASE", "t2453")
        .env("SLACK_BOT_TOKEN", "xoxb-EXAMPLE-not-a-real-token")
        .env("SLACK_APP_TOKEN", "xapp-EXAMPLE-not-a-real-token")
        .env("SLACK_TEST_CHANNEL", "C0EXAMPLE1")
        .current_dir(repo_root())
        .output()
        .expect("run lease-expiry-cluster-proof against soak namespace");
    assert!(
        !output.status.success(),
        "must refuse namespace curie\n{}",
        output_text(&output)
    );
    let text = output_text(&output);
    assert!(
        text.contains("curie") && (text.contains("soak") || text.contains("refuse")),
        "refusal must name the soak namespace\n{text}"
    );
}

#[test]
fn lease_expiry_cluster_proof_refuses_missing_slack() {
    let output = Command::new(bin())
        .args(["dev", "lease-expiry-cluster-proof"])
        .env("CURIE_BIN", bin())
        .env("CURIE_E2E_NAMESPACE", "acme-2453")
        .env("CURIE_E2E_RELEASE", "t2453")
        .env_remove("SLACK_BOT_TOKEN")
        .env_remove("SLACK_APP_TOKEN")
        .env_remove("SLACK_TEST_CHANNEL")
        .current_dir(repo_root())
        .output()
        .expect("run lease-expiry-cluster-proof without slack");
    assert!(
        !output.status.success(),
        "must refuse missing Slack credentials\n{}",
        output_text(&output)
    );
    let text = output_text(&output);
    assert!(
        text.to_lowercase().contains("slack"),
        "refusal must name Slack\n{text}"
    );
}
