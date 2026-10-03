//! Environment selection through the real cluster deploy command (#3504).
//! The platform API and Kubernetes tools are the external peers of this box.

#![cfg(unix)]

mod support;
#[path = "support/executable.rs"]
mod test_executable;

use curie::scaffold::scaffold;
use serde_json::{json, Value};
use std::path::Path;
use std::process::{Command, Output};
use support::{serve, MockServer, Request, Response};

const AGENT_ID: &str = "11111111-1111-1111-1111-111111111111";
const VERSION_ID: &str = "22222222-2222-2222-2222-222222222222";
const DEPLOYMENT_ID: &str = "33333333-3333-3333-3333-333333333333";
const LABEL: &str = "0.11.1";

fn agent() -> Value {
    json!({
        "id": AGENT_ID,
        "name": "acme-bot",
        "channels": [{"kind": "slack", "address": "C0EXAMPLE1"}],
        "created_at": "2026-09-29T00:00:00Z",
        "memory": false
    })
}

fn deployment(environment: &str, version_id: &str) -> Value {
    json!({
        "id": DEPLOYMENT_ID,
        "agent_id": AGENT_ID,
        "version_id": version_id,
        "environment": environment,
        "status": "active",
        "deployed_at": "2026-09-29T00:00:00Z"
    })
}

fn response(req: &Request, first_deploy: bool) -> Response {
    match (req.method.as_str(), req.path.as_str()) {
        ("GET", "/agents") => Response::json(
            200,
            &if first_deploy {
                json!([])
            } else {
                json!([agent()])
            }
            .to_string(),
        ),
        ("POST", "/agents") => {
            assert!(first_deploy, "redeploy must reuse the existing agent");
            Response::json(201, &agent().to_string())
        }
        ("GET", path) if path == format!("/deployments?agent_id={AGENT_ID}") => {
            let history = if first_deploy {
                json!([])
            } else {
                json!([deployment("prod", "44444444-4444-4444-4444-444444444444")])
            };
            Response::json(200, &history.to_string())
        }
        ("POST", path) if path == format!("/agents/{AGENT_ID}/versions") => Response::json(
            201,
            &json!({
                "id": VERSION_ID,
                "agent_id": AGENT_ID,
                "version_label": LABEL,
                "created_by": "tester",
                "created_at": "2026-09-29T00:00:00Z"
            })
            .to_string(),
        ),
        ("PUT", path) if path == format!("/agents/{AGENT_ID}/versions/{VERSION_ID}/bundle") => {
            Response::json(
                201,
                &json!({
                    "version_id": VERSION_ID,
                    "bundle_ref": "bundles/acme-bot.tar.gz",
                    "bundle_sha256": "sha-acme-bot",
                    "size_bytes": 100
                })
                .to_string(),
            )
        }
        ("POST", "/deployments") => {
            let body: Value = serde_json::from_slice(&req.body).expect("deployment body is JSON");
            let environment = body["environment"]
                .as_str()
                .expect("environment is a string");
            Response::json(201, &deployment(environment, VERSION_ID).to_string())
        }
        ("PATCH", path) if path == format!("/agents/{AGENT_ID}") => {
            Response::json(200, &agent().to_string())
        }
        ("GET", path) if path.contains("/versions/") && path.contains("/connectors?") => {
            Response::json(
                200,
                &json!({
                    "manifests": [],
                    "owned_secret_name": "",
                    "owned_secret_keys": [],
                    "mcp_entries": {},
                    "version_id": VERSION_ID,
                    "triggers": []
                })
                .to_string(),
            )
        }
        (method, path) => Response::json(500, &format!("unexpected {method} {path}")),
    }
}

fn install_tools(tools: &Path) {
    test_executable::install_in(
        tools,
        "kubectl",
        r#"#!/bin/sh
case "$*" in
  "config view --minify --raw -o json")
    printf '%s\n' '{"clusters":[{"cluster":{"server":"https://cluster.example.com","certificate-authority-data":"Y2E="}}]}' ;;
  *"get deployment"*) printf '%s' 'curie' ;;
  *"delete deployment,service,networkpolicy,secret"*) exit 0 ;;
  *) printf 'unexpected kubectl invocation: %s\n' "$*" >&2; exit 64 ;;
esac
"#,
    );
    test_executable::install_in(
        tools,
        "helm",
        r#"#!/bin/sh
case "$*" in
  "get values "*) printf '%s\n' '{}' ;;
  *) printf 'unexpected helm invocation: %s\n' "$*" >&2; exit 64 ;;
esac
"#,
    );
}

fn run_deploy(first_deploy: bool, environment: Option<&str>) -> (Output, MockServer) {
    let plugin = tempfile::tempdir().expect("plugin tempdir");
    scaffold(plugin.path(), "acme-bot").expect("scaffold bundle");
    let tools = tempfile::tempdir().expect("tools tempdir");
    install_tools(tools.path());
    let config = tempfile::tempdir().expect("config tempdir");
    let mut paths = vec![tools.path().to_path_buf()];
    paths.extend(std::env::split_paths(
        &std::env::var_os("PATH").unwrap_or_default(),
    ));
    let path = std::env::join_paths(paths).expect("join test PATH");
    let server = serve(move |req| response(req, first_deploy));
    let mut command = Command::new(env!("CARGO_BIN_EXE_curie"));
    command
        .current_dir(plugin.path())
        .args(["--color=never", "cluster", "deploy", "--plugin-dir"])
        .arg(plugin.path())
        .args([
            "--api-url",
            &server.base_url,
            "--api-key",
            "test-key",
            "--namespace",
            "test-3504",
            "--release",
            "curie",
            "--slack-channel",
            "C0EXAMPLE1",
            "--label",
            LABEL,
        ])
        .env("PATH", path)
        .env("CURIE_CONFIG_DIR", config.path())
        .env_remove("CURIE_API_URL")
        .env_remove("CURIE_API_KEY");
    if let Some(environment) = environment {
        command.args(["--env", environment]);
    }
    (command.output().expect("run cluster deploy"), server)
}

fn operator_output(output: &Output) -> String {
    let text = format!(
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(output.status.success(), "cluster deploy failed: {text}");
    text.to_lowercase()
}

fn assert_deployment_environment(server: &MockServer, expected: &str) {
    let requests = server.recorded();
    let deployments: Vec<_> = requests
        .iter()
        .filter(|req| req.method == "POST" && req.path == "/deployments")
        .collect();
    assert_eq!(
        deployments.len(),
        1,
        "deploy must activate exactly one version"
    );
    let body: Value =
        serde_json::from_slice(&deployments[0].body).expect("deployment body is JSON");
    assert_eq!(body["agent_id"], AGENT_ID);
    assert_eq!(body["version_id"], VERSION_ID);
    assert_eq!(body["environment"], expected);
    assert_eq!(deployments[0].header("x-api-key"), Some("test-key"));
}

#[test]
fn cluster_redeploy_infers_prod_and_announces_it() {
    let (output, server) = run_deploy(false, None);
    let text = operator_output(&output);
    assert_deployment_environment(&server, "prod");
    assert!(
        text.lines()
            .any(|line| line.contains("inferred") && line.contains("prod")),
        "operator output must announce the inferred prod environment: {text}"
    );
}

#[test]
fn first_cluster_deploy_without_env_defaults_to_dev() {
    let (output, server) = run_deploy(true, None);
    let text = operator_output(&output);
    assert_deployment_environment(&server, "dev");
    assert!(
        text.contains("dev"),
        "operator output must report dev: {text}"
    );
}

#[test]
fn explicit_dev_on_prod_agent_announces_a_second_environment() {
    let (output, server) = run_deploy(false, Some("dev"));
    let text = operator_output(&output);
    assert_deployment_environment(&server, "dev");
    assert!(
        text.lines().any(|line| {
            line.contains("second environment") && line.contains("dev") && line.contains("prod")
        }),
        "operator output must name the second environment and existing prod deployment: {text}"
    );
}
