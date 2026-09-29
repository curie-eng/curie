//! Integration (#2349): `curie doctor` must not report `ok` for a Helm release
//! whose latest revision is `failed`, or whose namespace has zero ready
//! workloads, and must not exit 0 in those cases.
//!
//! `evaluate` cannot see this: gather currently classifies any helm-list hit
//! as Installed from the chart name alone. These run the real binary against
//! stubbed kubectl/helm.

#[path = "support/executable.rs"]
mod test_executable;

use std::ffi::OsString;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_curie")
}

fn stub_path(tools: &Path) -> OsString {
    let mut entries = vec![tools.to_path_buf()];
    entries.extend(["/bin", "/usr/bin"].iter().map(PathBuf::from));
    std::env::join_paths(entries).expect("join stub PATH")
}

fn install_tools(tools: &Path) {
    fs::create_dir_all(tools).expect("tools dir");
    test_executable::install(tools.join("docker").as_path(), "#!/bin/sh\nexit 0\n");
    test_executable::install(
        tools.join("kubectl").as_path(),
        r#"#!/bin/sh
case "$*" in
  "config current-context") printf '%s\n' 'doctor-stub-context' ;;
  *"get deployments,statefulsets"*"app.kubernetes.io/instance=curie-demo"*)
    printf '%s\n' "$CURIE_TEST_DOCTOR_WORKLOADS"
    ;;
  *"get deployments,statefulsets"*)
    # Namespace-wide listing would count a neighbor release. Returning a
    # ready replica here makes a forgotten selector look healthy.
    printf '%s\n' '{"items":[{"kind":"Deployment","status":{"readyReplicas":9}}]}'
    ;;
  *"component=worker"*) printf '%s\n' '{"items":[]}' ;;
  *) printf 'unexpected kubectl invocation: %s\n' "$*" >&2; exit 64 ;;
esac
"#,
    );
    test_executable::install(
        tools.join("helm").as_path(),
        r#"#!/bin/sh
case "$*" in
  version*) printf 'v3.14.0+gstub\n' ;;
  list*) printf '%s\n' "$CURIE_TEST_DOCTOR_HELM_LIST" ;;
  *"--all"*) printf '%s\n' '{}' ;;
  "get values"*) printf '%s\n' '{}' ;;
  *) printf 'unexpected helm invocation: %s\n' "$*" >&2; exit 64 ;;
esac
"#,
    );
}

fn run_doctor(
    dir: &Path,
    tools: &Path,
    helm_list: &str,
    workloads: &str,
) -> (Option<i32>, serde_json::Value, String) {
    let home = dir.join("home");
    fs::create_dir_all(home.join(".config/curie")).expect("home config");
    let output = Command::new(bin())
        .current_dir(dir)
        .args([
            "--color=never",
            "--json",
            "doctor",
            "--namespace",
            "curie-demo",
            "--release",
            "curie-demo",
        ])
        .env("PATH", stub_path(tools))
        .env("HOME", &home)
        .env("CURIE_CONFIG_DIR", home.join(".config/curie"))
        .env("LC_ALL", "C")
        .env("CURIE_TEST_DOCTOR_HELM_LIST", helm_list)
        .env("CURIE_TEST_DOCTOR_WORKLOADS", workloads)
        .env_remove("CURIE_API_URL")
        .env_remove("CURIE_API_KEY")
        .env_remove("CURIE_CREDENTIALS")
        .env_remove("ANTHROPIC_API_KEY")
        .output()
        .expect("run curie doctor");
    let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
    let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
    let json: serde_json::Value = serde_json::from_str(&stdout)
        .unwrap_or_else(|e| panic!("stdout must be JSON, got {stdout:?} stderr {stderr:?}: {e}"));
    (output.status.code(), json, stderr)
}

fn release_check(json: &serde_json::Value) -> &serde_json::Value {
    json["checks"]
        .as_array()
        .expect("checks array")
        .iter()
        .find(|c| c["id"] == "release")
        .expect("release check")
}

#[test]
fn doctor_fails_a_helm_release_whose_latest_revision_is_failed() {
    let temp = tempfile::tempdir().expect("tempdir");
    let tools = temp.path().join("tools");
    install_tools(&tools);

    let (code, json, stderr) = run_doctor(
        temp.path(),
        &tools,
        r#"[{"name":"curie-demo","chart":"curie-0.8.5","status":"failed"}]"#,
        r#"{"items":[]}"#,
    );
    let release = release_check(&json);
    assert_eq!(
        release["state"], "missing",
        "failed revision must not be ok: {json} stderr {stderr}"
    );
    assert!(
        release["detail"].as_str().unwrap_or("").contains("failed"),
        "row must name the failed revision: {release}"
    );
    assert_eq!(code, Some(1), "must not exit 0: {json} stderr {stderr}");
    assert_eq!(json["ready"], false);
    for id in ["slack", "clone-credential", "webhook", "repo-binding"] {
        let check = json["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|c| c["id"] == id)
            .unwrap_or_else(|| panic!("missing {id}"));
        assert_eq!(
            check["state"], "not_applicable",
            "{id} must not offer setup against a dead release: {check}"
        );
    }
}

#[test]
fn doctor_fails_a_deployed_release_with_zero_ready_workloads() {
    let temp = tempfile::tempdir().expect("tempdir");
    let tools = temp.path().join("tools");
    install_tools(&tools);

    let (code, json, stderr) = run_doctor(
        temp.path(),
        &tools,
        r#"[{"name":"curie-demo","chart":"curie-0.8.5","status":"deployed"}]"#,
        r#"{"items":[]}"#,
    );
    let release = release_check(&json);
    assert_eq!(
        release["state"], "missing",
        "zero ready workloads must not be ok: {json} stderr {stderr}"
    );
    assert!(
        release["detail"]
            .as_str()
            .unwrap_or("")
            .contains("zero ready workloads"),
        "row must name the empty serving set: {release}"
    );
    assert_eq!(code, Some(1), "must not exit 0: {json} stderr {stderr}");
}

#[test]
fn doctor_keeps_exit_zero_for_a_serving_release_with_setup_gaps() {
    let temp = tempfile::tempdir().expect("tempdir");
    let tools = temp.path().join("tools");
    install_tools(&tools);

    let (code, json, stderr) = run_doctor(
        temp.path(),
        &tools,
        r#"[{"name":"curie-demo","chart":"curie-0.8.5","status":"deployed"}]"#,
        r#"{"items":[{"kind":"Deployment","status":{"readyReplicas":1}}]}"#,
    );
    let release = release_check(&json);
    assert_eq!(
        release["state"], "ok",
        "a deployed release with ready workloads is ok: {json} stderr {stderr}"
    );
    assert_eq!(
        code,
        Some(0),
        "missing Slack is a setup gap, not a serving failure: {json} stderr {stderr}"
    );
}
