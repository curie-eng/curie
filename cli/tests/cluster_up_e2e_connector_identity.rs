//! `curie cluster up --e2e-connector-identity` installs the end to end
//! connector's identity through the owner release's chart (ADR 0176 decision 4,
//! #3243). The grant itself is pinned by
//! `charts/curie/ci/e2e-connector-identity-assertions.sh` and proven live by
//! `charts/curie/ci/runtime/e2e-connector-identity-runtime.sh`.

use std::process::Command;

const ENABLE: &str = "--set e2eConnectorIdentity.enabled=true";

fn dry_run(extra: &[&str]) -> String {
    let home = tempfile::tempdir().expect("temp home");
    let output = Command::new(env!("CARGO_BIN_EXE_curie"))
        .args(["cluster", "up", "--dry-run", "--namespace", "test-ns"])
        .args([
            "--chart",
            concat!(env!("CARGO_MANIFEST_DIR"), "/../charts/curie"),
        ])
        .args(extra)
        .env("HOME", home.path())
        .env("XDG_CONFIG_HOME", home.path())
        .env_remove("CURIE_CREDENTIALS")
        .env_remove("CURIE_MODEL_CREDENTIALS")
        .env_remove("CURIE_NAMESPACE")
        .output()
        .expect("run curie");
    assert!(
        output.status.success(),
        "dry run failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

#[test]
fn flag_enables_the_identity_on_the_helm_command() {
    let plan = dry_run(&["--e2e-connector-identity"]);
    let helm = plan
        .lines()
        .find(|line| line.starts_with("helm upgrade --install"))
        .unwrap_or_else(|| panic!("no helm command in plan:\n{plan}"));
    assert!(helm.contains(ENABLE), "flag not forwarded: {helm}");
}

#[test]
fn identity_is_absent_without_the_flag() {
    let plan = dry_run(&[]);
    assert!(
        !plan.contains("e2eConnectorIdentity"),
        "identity enabled without the flag:\n{plan}"
    );
}
