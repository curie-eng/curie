//! The fix pin for #2496: `curie doctor` must name the commit poller on a
//! private install with no webhook exposure.
//!
//! # Why this test is deliberately black-box
//!
//! This file is the `Fix pin:` selector for #2496, and
//! `cli/scripts/verify-fix-pin.sh` proves a pin by REVERSING every product
//! file in the change and requiring the selected test to fail *as a test* in
//! that tree. A pin that references the new API (`curie::delivery::*`, the new
//! `doctor::Facts` fields, `doctor::observe_delivery`) does not fail as a test
//! there -- the whole test target fails to COMPILE, which the gate cannot
//! attribute to the selected test and which proves nothing about the
//! regression. `cli/tests/delivery_diagnostics.rs` and
//! `cli/tests/delivery_observation.rs` are the unit-level coverage and are
//! intentionally left as they are; they simply cannot serve as the pin.
//!
//! So this test touches only `std`, `serde_json`, `tempfile` and
//! `env!("CARGO_BIN_EXE_curie")`: it compiles identically against the pre-fix
//! `cli/src` and, with the fix reversed, the binary prints the old
//! ingress-only advice and the assertion below fails. Do not "clean this up"
//! by importing the delivery module -- that would unpin the fix.

use std::ffi::OsString;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::Command;

fn write_executable(path: &Path, body: &str) {
    fs::write(path, body).expect("write stub executable");
    let mut permissions = fs::metadata(path)
        .expect("read stub metadata")
        .permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(path, permissions).expect("make stub executable");
}

/// A serving `curie/curie` release with **no ingress, no NodePort** and a
/// computed `api.commitPollIntervalSeconds` of `0` -- the exact symptom shape
/// reported in #2496. The interval lives in `helm get values --all` (COMPUTED)
/// because a supplied read cannot tell "off" from "never set" (#1950).
fn install_private_cluster_stubs(tools: &Path) {
    write_executable(tools.join("docker").as_path(), "#!/bin/sh\nexit 0\n");
    write_executable(
        tools.join("kubectl").as_path(),
        r#"#!/bin/sh
case "$*" in
  "config current-context") printf '%s\n' 'delivery-pin-context' ;;
  *"config view"*) printf '%s\n' 'https://127.0.0.1:6443' ;;
  *component=api*) printf '%s\n' 'curie-api' ;;
  *"get svc curie-ui"*) printf '%s\n' '{"spec":{"type":"ClusterIP","ports":[{"port":80}]}}' ;;
  *"get deployments,statefulsets -n "*)
    printf '%s\n' '{"items":[{"kind":"Deployment","status":{"readyReplicas":1}}]}'
    ;;
  *) : ;;
esac
exit 0
"#,
    );
    write_executable(
        tools.join("helm").as_path(),
        r#"#!/bin/sh
case "$*" in
  version*) printf 'v3.14.0+gstub\n' ;;
  list*) printf '%s\n' '[{"name":"curie","namespace":"curie","status":"deployed","chart":"curie-0.8.2"}]' ;;
  *"--all"*) printf '%s\n' '{"api":{"commitPollIntervalSeconds":0}}' ;;
  "get values"*) printf '%s\n' '{}' ;;
  *) : ;;
esac
exit 0
"#,
    );
}

fn stub_path(tools: &Path) -> OsString {
    let mut entries = vec![tools.to_path_buf()];
    entries.extend(["/bin", "/usr/bin"].iter().map(PathBuf::from));
    std::env::join_paths(entries).expect("join stub PATH")
}

/// A private install has no webhook path at all, so doctor must report push
/// delivery as MISSING *and* its fix must name the poller
/// (`api.commitPollIntervalSeconds`) -- ingress-only advice is unusable on an
/// install that cannot have public ingress, which is the reported half of
/// #2496.
#[test]
fn doctor_names_the_commit_poller_for_a_private_install() {
    let temp = tempfile::tempdir().expect("tempdir");
    let tools = temp.path().join("tools");
    let home = temp.path().join("home");
    let cwd = temp.path().join("cwd");
    for dir in [&tools, &home, &cwd] {
        fs::create_dir_all(dir).expect("create fixture dir");
    }
    install_private_cluster_stubs(&tools);

    let output = Command::new(env!("CARGO_BIN_EXE_curie"))
        .current_dir(&cwd)
        .args(["--color=never", "--json", "doctor"])
        .env_clear()
        .env("PATH", stub_path(&tools))
        .env("HOME", &home)
        .env("LC_ALL", "C")
        .output()
        .expect("run curie doctor");
    let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
    let stderr = String::from_utf8_lossy(&output.stderr).into_owned();

    let report: serde_json::Value = serde_json::from_str(&stdout)
        .unwrap_or_else(|e| panic!("stdout must be doctor JSON, got {stdout:?}: {e}\n{stderr}"));
    let check = report["checks"]
        .as_array()
        .expect("checks array")
        .iter()
        .find(|c| c["id"] == "webhook")
        .cloned()
        .unwrap_or_else(|| panic!("push-delivery check missing from {stdout}"));

    assert_eq!(
        check["state"], "missing",
        "no exposure and no poller is not a delivery path: {check}"
    );
    // The load-bearing assertion. Before #2496 this fix named only
    // `--set api.ingress.enabled=true`, so a private install was told to do
    // the one thing it cannot do.
    let fix = check["fix"].as_str().unwrap_or_default();
    assert!(
        fix.contains("api.commitPollIntervalSeconds"),
        "the fix must offer the no-webhook path for a private install: {check}"
    );
}
