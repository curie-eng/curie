//! Integration (#1367): `curie doctor` discovers its API connection the way
//! sibling cluster verbs do, so a bare invocation can check repo binding.
//!
//! `zip(--api-url, --api-key)` in the dispatch arm is structurally invisible
//! to `gather()`: handing it `None` is the same whether the operator omitted
//! both flags or the handler refused to look them up. These run the real
//! binary against stubbed kubectl/helm and a wire-level platform API.

mod support;

#[path = "support/executable.rs"]
mod test_executable;

use std::ffi::OsString;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use support::{serve, MockServer, Response};

fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_curie")
}

/// Stub docker/kubectl/helm for a default `curie/curie` install.
///
/// `node_port` is the mock platform API's host port. Discovery prefers the UI
/// `/api` proxy on that NodePort, same as `ops::discover_api_url`.
fn install_cluster_stubs(tools: &Path, node_port: u16, api_key: &str) {
    test_executable::install(tools.join("docker").as_path(), "#!/bin/sh\nexit 0\n");
    test_executable::install(
        tools.join("kubectl").as_path(),
        &format!(
            r#"#!/bin/sh
case "$*" in
  "config current-context") printf '%s\n' 'doctor-stub-context' ;;
  *"config view"*) printf '%s\n' 'https://127.0.0.1:6443' ;;
  *component=api*) printf '%s\n' 'curie-api' ;;
  *"get svc curie-ui"*)
    printf '%s\n' '{{"spec":{{"type":"NodePort","ports":[{{"port":80,"nodePort":{node_port}}}]}}}}'
    ;;
  *"get secret"*go-template*) printf '%s\n' '{api_key}' ;;
  *"get secret"*) printf '%s\n' 'curie-secrets' ;;
  *"get svc curie-api"*)
    printf '%s\n' '{{"spec":{{"type":"NodePort","ports":[{{"port":8000,"nodePort":{node_port}}}]}}}}'
    ;;
  *nodePort*) printf '%s\n' '{node_port}' ;;
  *"get deployments,statefulsets -n "*) printf '%s\n' '{{"items":[{{"kind":"Deployment","status":{{"readyReplicas":1}}}}]}}' ;;
  *) printf 'unexpected kubectl invocation: %s\n' "$*" >&2; exit 64 ;;
esac
"#,
            node_port = node_port,
            api_key = api_key
        ),
    );
    test_executable::install(
        tools.join("helm").as_path(),
        r#"#!/bin/sh
case "$*" in
  version*) printf 'v3.14.0+gstub\n' ;;
  list*) printf '%s\n' '[{"name":"curie","chart":"curie-0.8.2"}]' ;;
  *"--all"*) printf '%s\n' '{"dispatcher":{"slack":{"appToken":"x","botToken":"x"}},"api":{"githubApp":{"appId":"1"}}}' ;;
  "get values"*) printf '%s\n' '{"dispatcher":{"slack":{"appToken":"x","botToken":"x"}}}' ;;
  *) printf 'unexpected helm invocation: %s\n' "$*" >&2; exit 64 ;;
esac
"#,
    );
}

/// Cluster tools that answer helm/kubectl enough for doctor to leave the
/// laptop rung, but cannot discover a platform API URL or key.
fn install_cluster_without_api(tools: &Path) {
    test_executable::install(tools.join("docker").as_path(), "#!/bin/sh\nexit 0\n");
    test_executable::install(
        tools.join("kubectl").as_path(),
        r#"#!/bin/sh
case "$*" in
  "config current-context") printf '%s\n' 'doctor-stub-context' ;;
  *"get deployments,statefulsets -n "*) printf '%s\n' '{"items":[{"kind":"Deployment","status":{"readyReplicas":1}}]}' ;;
  *) printf 'unexpected kubectl invocation: %s\n' "$*" >&2; exit 64 ;;
esac
"#,
    );
    test_executable::install(
        tools.join("helm").as_path(),
        r#"#!/bin/sh
case "$*" in
  version*) printf 'v3.14.0+gstub\n' ;;
  list*) printf '%s\n' '[{"name":"curie","chart":"curie-0.8.2"}]' ;;
  *"--all"*) printf '%s\n' '{}' ;;
  "get values"*) printf '%s\n' '{}' ;;
  *) printf 'unexpected helm invocation: %s\n' "$*" >&2; exit 64 ;;
esac
"#,
    );
}

fn stub_path(tools: &Path) -> OsString {
    let mut entries = vec![tools.to_path_buf()];
    entries.extend(["/bin", "/usr/bin"].iter().map(PathBuf::from));
    std::env::join_paths(entries).expect("join stub PATH")
}

fn bound_agents_json() -> String {
    r#"[{"id":"11111111-1111-1111-1111-111111111111","name":"acme-bot","channels":[{"kind":"slack","address":"C0EXAMPLE1"}],"memory":false,"repo_full_name":"acme-corp/acme-bot"}]"#.to_string()
}

fn serve_agents() -> MockServer {
    let body = bound_agents_json();
    serve(move |req| {
        if req.method == "GET" && (req.path == "/agents" || req.path == "/api/agents") {
            return Response::json(200, &body);
        }
        Response::json(404, "{\"error\":\"unexpected\"}")
    })
}

fn run_doctor(
    dir: &Path,
    path: &OsString,
    extra_env: &[(&str, &str)],
    args: &[&str],
) -> (Option<i32>, String, String) {
    let mut cmd = Command::new(bin());
    cmd.current_dir(dir)
        .args(args)
        .env("PATH", path)
        .env("LC_ALL", "C")
        .env_remove("CURIE_API_URL")
        .env_remove("CURIE_API_KEY");
    for (key, value) in extra_env {
        cmd.env(key, value);
    }
    let output = cmd.output().expect("run curie doctor");
    (
        output.status.code(),
        String::from_utf8_lossy(&output.stdout).into_owned(),
        String::from_utf8_lossy(&output.stderr).into_owned(),
    )
}

fn repo_binding(stdout: &str) -> serde_json::Value {
    let json: serde_json::Value = serde_json::from_str(stdout)
        .unwrap_or_else(|e| panic!("stdout must be doctor JSON, got {stdout:?}: {e}"));
    json["checks"]
        .as_array()
        .expect("checks array")
        .iter()
        .find(|c| c["id"] == "repo-binding")
        .cloned()
        .expect("repo-binding check")
}

/// The missing-flag path: a cluster answers, but discovery cannot find a URL
/// or key, and the operator passed neither flag. doctor must still report
/// (exit 0), and repo-binding stays the unread NotApplicable — never a usage
/// error for omitted flags.
#[test]
fn bare_doctor_does_not_require_api_flags() {
    let temp = tempfile::tempdir().expect("tempdir");
    let tools = temp.path().join("tools");
    fs::create_dir_all(&tools).expect("tools dir");
    install_cluster_without_api(&tools);

    let (code, stdout, stderr) = run_doctor(
        temp.path(),
        &stub_path(&tools),
        &[],
        &["--color=never", "--json", "doctor"],
    );
    assert_eq!(code, Some(0), "stdout: {stdout}\nstderr: {stderr}");
    let binding = repo_binding(&stdout);
    assert_eq!(
        binding["state"], "not_applicable",
        "unread API is a fact, not a failure: {binding}"
    );
}

/// The discovered-connection path: neither --api-url nor --api-key, but the
/// release exposes a NodePort UI proxy and an apiKey Secret. Bare doctor must
/// reach GET /agents and report the binding.
#[test]
fn bare_doctor_discovers_api_and_checks_repo_binding() {
    let server = serve_agents();
    let port: u16 = server
        .base_url
        .rsplit(':')
        .next()
        .expect("port")
        .parse()
        .expect("port number");
    let temp = tempfile::tempdir().expect("tempdir");
    let tools = temp.path().join("tools");
    fs::create_dir_all(&tools).expect("tools dir");
    install_cluster_stubs(&tools, port, "curie-stub-key");

    let (code, stdout, stderr) = run_doctor(
        temp.path(),
        &stub_path(&tools),
        &[],
        &["--color=never", "--json", "doctor"],
    );
    assert_eq!(code, Some(0), "stdout: {stdout}\nstderr: {stderr}");
    let binding = repo_binding(&stdout);
    assert_eq!(
        binding["state"], "ok",
        "discovered API must drive repo-binding: {binding}\nstderr: {stderr}"
    );
    assert!(
        binding["detail"]
            .as_str()
            .is_some_and(|d| d.contains("acme-bot") || d.contains("all bound")),
        "must report the bound agent: {binding}"
    );
    let requests = server.recorded();
    assert!(
        requests.iter().any(|r| r.method == "GET"
            && (r.path == "/agents" || r.path == "/api/agents")
            && r.header("X-API-Key") == Some("curie-stub-key")),
        "doctor must call GET /agents with the discovered key: {requests:?}"
    );
}

/// One flag is not both: --api-url without --api-key used to zip to None.
/// The key must be discovered from the release Secret.
#[test]
fn doctor_with_only_api_url_discovers_the_key() {
    let server = serve_agents();
    let temp = tempfile::tempdir().expect("tempdir");
    let tools = temp.path().join("tools");
    fs::create_dir_all(&tools).expect("tools dir");
    // node_port is unused for URL (explicit) but the secret read still needs
    // a working kubectl stub.
    install_cluster_stubs(&tools, 1, "curie-stub-key");

    let (code, stdout, stderr) = run_doctor(
        temp.path(),
        &stub_path(&tools),
        &[],
        &[
            "--color=never",
            "--json",
            "doctor",
            "--api-url",
            &server.base_url,
        ],
    );
    assert_eq!(code, Some(0), "stdout: {stdout}\nstderr: {stderr}");
    let binding = repo_binding(&stdout);
    assert_eq!(
        binding["state"], "ok",
        "explicit URL plus discovered key must reach the API: {binding}\nstderr: {stderr}"
    );
}

/// The other half of zip: --api-key without --api-url must still discover the
/// NodePort URL.
#[test]
fn doctor_with_only_api_key_discovers_the_url() {
    let server = serve_agents();
    let port: u16 = server
        .base_url
        .rsplit(':')
        .next()
        .expect("port")
        .parse()
        .expect("port number");
    let temp = tempfile::tempdir().expect("tempdir");
    let tools = temp.path().join("tools");
    fs::create_dir_all(&tools).expect("tools dir");
    install_cluster_stubs(&tools, port, "unused-secret-key");

    let (code, stdout, stderr) = run_doctor(
        temp.path(),
        &stub_path(&tools),
        &[],
        &[
            "--color=never",
            "--json",
            "doctor",
            "--api-key",
            "curie-stub-key",
        ],
    );
    assert_eq!(code, Some(0), "stdout: {stdout}\nstderr: {stderr}");
    let binding = repo_binding(&stdout);
    assert_eq!(
        binding["state"], "ok",
        "explicit key plus discovered URL must reach the API: {binding}\nstderr: {stderr}"
    );
    let requests = server.recorded();
    assert!(
        requests
            .iter()
            .any(|r| r.header("X-API-Key") == Some("curie-stub-key")),
        "must send the explicit key, not the secret: {requests:?}"
    );
}
