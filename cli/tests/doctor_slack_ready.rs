//! Integration (#2497): `curie doctor` must not treat unset Slack as unreadiness
//! on a serving release, must still fail for an absent release, an unavailable
//! cluster API, and broken configured Slack, and human output must agree with
//! `--json` on the summary that points at `curie cluster message`.
//!
//! These run the real binary against stubbed kubectl/helm. They do not infer
//! readiness from a Helm record alone: the serving-set listing is part of the
//! stub, and a failed or empty serving set is a separate case in
//! `doctor_release_health.rs`.

use std::ffi::OsString;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::Command;

fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_curie")
}

fn write_executable(path: &Path, body: &str) {
    fs::write(path, body).expect("write stub executable");
    let mut permissions = fs::metadata(path)
        .expect("read stub metadata")
        .permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(path, permissions).expect("make executable");
}

fn stub_path(tools: &Path) -> OsString {
    let mut entries = vec![tools.to_path_buf()];
    entries.extend(["/bin", "/usr/bin"].iter().map(PathBuf::from));
    std::env::join_paths(entries).expect("join stub PATH")
}

const SERVING_VALUES: &str =
    r#"{"api":{"githubAppId":"1","ingress":{"enabled":true,"host":"api.example.com"}}}"#;
const BROKEN_SLACK_VALUES: &str = r#"{"dispatcher":{"slack":{"botToken":"x"}},"api":{"githubAppId":"1","ingress":{"enabled":true,"host":"api.example.com"}}}"#;

fn install_tools(tools: &Path) {
    fs::create_dir_all(tools).expect("tools dir");
    write_executable(tools.join("docker").as_path(), "#!/bin/sh\nexit 0\n");
    write_executable(
        tools.join("kubectl").as_path(),
        r#"#!/bin/sh
case "$*" in
  "config current-context") printf '%s\n' 'doctor-stub-context' ;;
  *"get deployments,statefulsets"*"app.kubernetes.io/instance=curie-demo"*)
    printf '%s\n' "$CURIE_TEST_DOCTOR_WORKLOADS"
    ;;
  *"get deployments,statefulsets"*)
    printf '%s\n' '{"items":[{"kind":"Deployment","status":{"readyReplicas":9}}]}'
    ;;
  *"component=worker"*) printf '%s\n' '{"items":[]}' ;;
  *) printf 'unexpected kubectl invocation: %s\n' "$*" >&2; exit 64 ;;
esac
"#,
    );
    write_executable(
        tools.join("helm").as_path(),
        r#"#!/bin/sh
case "$*" in
  version*) printf 'v3.14.0+gstub\n' ;;
  list*)
    if [ -n "${CURIE_TEST_DOCTOR_HELM_FAIL:-}" ]; then
      exit 1
    fi
    printf '%s\n' "$CURIE_TEST_DOCTOR_HELM_LIST"
    ;;
  *"--all"*|"get values"*) printf '%s\n' "$CURIE_TEST_DOCTOR_HELM_VALUES" ;;
  *) printf 'unexpected helm invocation: %s\n' "$*" >&2; exit 64 ;;
esac
"#,
    );
}

fn write_bundle(dir: &Path) {
    let plugin = dir.join(".claude-plugin");
    fs::create_dir_all(&plugin).expect("plugin dir");
    fs::write(
        plugin.join("plugin.json"),
        r#"{"name":"acme-bot","version":"0.0.1"}"#,
    )
    .expect("plugin.json");
}

struct DoctorRun {
    code: Option<i32>,
    json: serde_json::Value,
    human: String,
    stderr: String,
}

fn run_doctor(
    dir: &Path,
    tools: &Path,
    helm_list: &str,
    values: &str,
    helm_fail: bool,
) -> DoctorRun {
    let home = dir.join("home");
    fs::create_dir_all(home.join(".config/curie")).expect("home config");
    let workloads = r#"{"items":[{"kind":"Deployment","status":{"readyReplicas":1}}]}"#;
    let common = |json: bool| {
        let mut cmd = Command::new(bin());
        cmd.current_dir(dir)
            .env("PATH", stub_path(tools))
            .env("HOME", &home)
            .env("CURIE_CONFIG_DIR", home.join(".config/curie"))
            .env("LC_ALL", "C")
            .env("CURIE_CREDENTIALS", "sk-ant-PLACEHOLDER")
            .env("CURIE_TEST_DOCTOR_HELM_LIST", helm_list)
            .env("CURIE_TEST_DOCTOR_HELM_VALUES", values)
            .env("CURIE_TEST_DOCTOR_WORKLOADS", workloads)
            .env_remove("CURIE_API_URL")
            .env_remove("CURIE_API_KEY")
            .env_remove("ANTHROPIC_API_KEY");
        if helm_fail {
            cmd.env("CURIE_TEST_DOCTOR_HELM_FAIL", "1");
        }
        if json {
            cmd.args(["--color=never", "--json"]);
        } else {
            cmd.args(["--color=never"]);
        }
        cmd.args([
            "doctor",
            "--namespace",
            "curie-demo",
            "--release",
            "curie-demo",
        ]);
        cmd.output().expect("run curie doctor")
    };

    let json_out = common(true);
    let human_out = common(false);
    let stdout = String::from_utf8_lossy(&json_out.stdout).into_owned();
    let stderr = String::from_utf8_lossy(&json_out.stderr).into_owned();
    let json: serde_json::Value = serde_json::from_str(&stdout)
        .unwrap_or_else(|e| panic!("stdout must be JSON, got {stdout:?} stderr {stderr:?}: {e}"));
    DoctorRun {
        code: json_out.status.code(),
        json,
        human: String::from_utf8_lossy(&human_out.stdout).into_owned(),
        stderr,
    }
}

fn check<'a>(json: &'a serde_json::Value, id: &str) -> &'a serde_json::Value {
    json["checks"]
        .as_array()
        .expect("checks array")
        .iter()
        .find(|c| c["id"] == id)
        .unwrap_or_else(|| panic!("missing {id} in {json}"))
}

fn assert_human_json_agree(run: &DoctorRun) {
    let json_summary = run.json["summary"].as_str().expect("summary string");
    assert!(
        run.human.contains(json_summary),
        "human output must carry the json summary\njson: {json_summary}\nhuman:\n{}\nstderr: {}",
        run.human,
        run.stderr
    );
}

#[test]
fn slack_free_serving_release_is_ready_and_points_at_cluster_message() {
    let temp = tempfile::tempdir().expect("tempdir");
    let tools = temp.path().join("tools");
    install_tools(&tools);
    write_bundle(temp.path());

    let run = run_doctor(
        temp.path(),
        &tools,
        r#"[{"name":"curie-demo","chart":"curie-0.8.5","status":"deployed"}]"#,
        SERVING_VALUES,
        false,
    );
    assert_eq!(run.code, Some(0), "stderr: {}", run.stderr);
    assert_eq!(
        check(&run.json, "slack")["state"],
        "not_applicable",
        "unset Slack is optional: {}",
        run.json
    );
    let missing: Vec<&str> = run.json["checks"]
        .as_array()
        .expect("checks")
        .iter()
        .filter(|c| c["state"] == "missing")
        .filter_map(|c| c["id"].as_str())
        .collect();
    assert!(
        missing.is_empty(),
        "this fixture is a serving Slack-free release; no check should be missing: {missing:?}\n{}",
        run.json
    );
    assert_eq!(run.json["ready"], true, "{}", run.json);
    let summary = run.json["summary"].as_str().unwrap_or("");
    assert!(
        summary.contains("cluster message"),
        "must point at cluster message: {summary}"
    );
    assert!(
        !summary.contains("no way to be reached"),
        "that claim is the bug: {summary}"
    );
    assert!(
        !summary.contains("Answering in Slack"),
        "Slack is unset: {summary}"
    );
    assert!(run.human.contains("cluster message"), "{}", run.human);
    assert_human_json_agree(&run);
}

#[test]
fn absent_release_is_not_ready() {
    let temp = tempfile::tempdir().expect("tempdir");
    let tools = temp.path().join("tools");
    install_tools(&tools);
    write_bundle(temp.path());

    let run = run_doctor(temp.path(), &tools, "[]", SERVING_VALUES, false);
    assert_eq!(run.json["ready"], false, "{}", run.json);
    let summary = run.json["summary"].as_str().unwrap_or("");
    assert!(
        !summary.contains("cluster message"),
        "no release means cluster message is not the next step: {summary}"
    );
    assert_eq!(check(&run.json, "release")["state"], "missing");
}

#[test]
fn unavailable_helm_api_is_not_ready() {
    let temp = tempfile::tempdir().expect("tempdir");
    let tools = temp.path().join("tools");
    install_tools(&tools);
    write_bundle(temp.path());

    let run = run_doctor(
        temp.path(),
        &tools,
        r#"[{"name":"curie-demo","chart":"curie-0.8.5","status":"deployed"}]"#,
        SERVING_VALUES,
        true,
    );
    assert_eq!(run.json["ready"], false, "{}", run.json);
    assert_eq!(check(&run.json, "cluster")["state"], "missing");
}

#[test]
fn broken_configured_slack_is_not_ready() {
    let temp = tempfile::tempdir().expect("tempdir");
    let tools = temp.path().join("tools");
    install_tools(&tools);
    write_bundle(temp.path());

    let run = run_doctor(
        temp.path(),
        &tools,
        r#"[{"name":"curie-demo","chart":"curie-0.8.5","status":"deployed"}]"#,
        BROKEN_SLACK_VALUES,
        false,
    );
    assert_eq!(
        check(&run.json, "slack")["state"],
        "missing",
        "{}",
        run.json
    );
    assert_eq!(run.json["ready"], false, "{}", run.json);
    let summary = run.json["summary"].as_str().unwrap_or("");
    assert!(
        summary.contains("cluster message"),
        "half-configured Slack still leaves cluster message as the reach path: {summary}"
    );
    assert_human_json_agree(&run);
}
