//! Operator contract for hook configuration and the deliberate secret read.
//! The binary drives the real CLI parser and HTTP client. Only its external API
//! peer is replaced, so each assertion can inspect the request on the wire.

mod support;

use std::fs;
use std::process::{Command, Output};
use std::sync::{Arc, Mutex};

use serde_json::{json, Value};
use support::{serve, MockServer, Response};

const AGENT_ID: &str = "11111111-1111-1111-1111-111111111111";
const AGENT_NAME: &str = "acme-bot";
const API_KEY: &str = "fixture-operator-key";
const HOOK_SECRET: &str = "fixture-derived-hook-secret";

fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_curie")
}

fn agent() -> Value {
    json!({
        "id": AGENT_ID,
        "name": AGENT_NAME,
        "channels": [{"kind": "slack", "address": "C0EXAMPLE1"}],
        "memory": false,
        "created_at": "2026-09-27T00:00:00Z",
        "hook_partitions": {"alertmanager": {"pointer": "/old_partition"}},
        "source_bindings": {
            "alertmanager": {
                "workload_pointer": "/old_workload",
                "map": {
                    "old": {"repository": "acme-corp/old", "revision": "1111111"}
                }
            }
        }
    })
}

struct Fixture {
    api: MockServer,
    state: Arc<Mutex<Value>>,
}

impl Fixture {
    fn new() -> Self {
        let state = Arc::new(Mutex::new(agent()));
        let handler_state = Arc::clone(&state);
        let api = serve(move |request| {
            assert_eq!(request.header("x-api-key"), Some(API_KEY));
            match (request.method.as_str(), request.path.as_str()) {
                ("GET", "/agents") => {
                    let current = handler_state.lock().unwrap().clone();
                    Response::json(200, &json!([current]).to_string())
                }
                ("GET", path) if path == format!("/agents/{AGENT_ID}/hook-secret") => {
                    Response::json(200, &json!({"secret": HOOK_SECRET}).to_string())
                }
                ("PATCH", path) if path == format!("/agents/{AGENT_ID}") => {
                    let body: Value = serde_json::from_slice(&request.body).expect("PATCH JSON");
                    let fields = body.as_object().expect("PATCH object");
                    let mut stored = handler_state.lock().unwrap();
                    for (key, value) in fields {
                        stored[key.as_str()] = value.clone();
                    }
                    Response::json(200, &stored.to_string())
                }
                other => panic!("unexpected API request: {other:?}"),
            }
        });
        Self { api, state }
    }

    fn run(&self, tier: &str, args: &[&str]) -> Output {
        let mut command = Command::new(bin());
        command
            .args([tier, "hooks"])
            .args(args)
            .args(["--api-url", &self.api.base_url, "--api-key", API_KEY])
            .env_remove("CURIE_API_URL")
            .env_remove("CURIE_API_KEY")
            .env_remove("CURIE_NAMESPACE")
            .env_remove("CURIE_RELEASE")
            .env("NO_COLOR", "1");
        command.output().expect("run hook operator command")
    }
}

fn success_json(output: &Output) -> Value {
    assert!(
        output.status.success(),
        "stdout: {}; stderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).expect("one JSON result")
}

fn requests(api: &MockServer) -> Vec<(String, String)> {
    api.recorded()
        .iter()
        .map(|request| (request.method.clone(), request.path.clone()))
        .collect()
}

#[test]
fn local_show_replace_and_clear_one_map_preserves_the_other() {
    let fixture = Fixture::new();
    let initial = success_json(&fixture.run("local", &["show", AGENT_NAME, "--json"]));
    assert_eq!(
        initial["hook_partitions"]["alertmanager"]["pointer"],
        "/old_partition"
    );
    assert_eq!(
        initial["source_bindings"]["alertmanager"]["workload_pointer"],
        "/old_workload"
    );

    let temp = tempfile::tempdir().expect("temporary config directory");
    let config = temp.path().join("hooks.json");
    let replacement = json!({
        "hook_partitions": {"alertmanager": {"pointer": "/curie_partition"}},
        "source_bindings": {
            "alertmanager": {
                "workload_pointer": "/commonLabels/curie_workload",
                "map": {
                    "api": {
                        "repository": "acme-corp/api",
                        "revision": "abcdef0"
                    }
                }
            }
        }
    });
    fs::write(&config, replacement.to_string()).expect("write configuration input");
    let config_path = config.to_str().expect("UTF8 config path");
    let changed = success_json(&fixture.run(
        "local",
        &["configure", AGENT_NAME, "--file", config_path, "--json"],
    ));
    assert_eq!(changed["hook_partitions"], replacement["hook_partitions"]);
    assert_eq!(changed["source_bindings"], replacement["source_bindings"]);

    let clear = json!({"hook_partitions": {}});
    fs::write(&config, clear.to_string()).expect("write clear input");
    let cleared = success_json(&fixture.run(
        "local",
        &["configure", AGENT_ID, "--file", config_path, "--json"],
    ));
    assert_eq!(cleared["hook_partitions"], json!({}));
    assert_eq!(cleared["source_bindings"], replacement["source_bindings"]);
    assert_eq!(
        fixture.state.lock().unwrap()["source_bindings"],
        replacement["source_bindings"]
    );

    let recorded = fixture.api.recorded();
    let patches: Vec<Value> = recorded
        .iter()
        .filter(|request| request.method == "PATCH")
        .map(|request| serde_json::from_slice(&request.body).expect("PATCH body JSON"))
        .collect();
    assert_eq!(patches, [replacement, clear]);
    assert_eq!(
        requests(&fixture.api),
        [
            ("GET".into(), "/agents".into()),
            ("GET".into(), "/agents".into()),
            ("PATCH".into(), format!("/agents/{AGENT_ID}")),
            ("GET".into(), "/agents".into()),
            ("PATCH".into(), format!("/agents/{AGENT_ID}")),
        ]
    );
}

#[test]
fn cluster_uses_the_operator_endpoint_to_clear_only_source_bindings() {
    let fixture = Fixture::new();
    let initial = success_json(&fixture.run("cluster", &["show", AGENT_ID, "--json"]));
    assert_eq!(
        initial["hook_partitions"]["alertmanager"]["pointer"],
        "/old_partition"
    );
    assert_eq!(
        initial["source_bindings"]["alertmanager"]["workload_pointer"],
        "/old_workload"
    );

    let temp = tempfile::tempdir().expect("temporary config directory");
    let config = temp.path().join("hooks.json");
    let replacement = json!({
        "source_bindings": {
            "alertmanager": {
                "workload_pointer": "/commonLabels/curie_workload",
                "map": {
                    "worker": {
                        "repository": "acme-corp/worker",
                        "revision": "abcdef1"
                    }
                }
            }
        }
    });
    fs::write(&config, replacement.to_string()).expect("write replacement input");
    let replaced = success_json(&fixture.run(
        "cluster",
        &[
            "configure",
            AGENT_NAME,
            "--file",
            config.to_str().expect("UTF8 config path"),
            "--json",
        ],
    ));
    assert_eq!(replaced["source_bindings"], replacement["source_bindings"]);
    assert_eq!(
        replaced["hook_partitions"]["alertmanager"]["pointer"],
        "/old_partition"
    );

    fs::write(&config, r#"{"source_bindings":{}}"#).expect("write clear input");
    let output = success_json(&fixture.run(
        "cluster",
        &[
            "configure",
            AGENT_NAME,
            "--file",
            config.to_str().expect("UTF8 config path"),
            "--json",
        ],
    ));
    assert_eq!(output["source_bindings"], json!({}));
    assert_eq!(
        output["hook_partitions"]["alertmanager"]["pointer"],
        "/old_partition"
    );
    let recorded = fixture.api.recorded();
    let patches: Vec<_> = recorded
        .iter()
        .filter(|request| request.method == "PATCH")
        .collect();
    assert_eq!(patches.len(), 2);
    let replace_body: Value = serde_json::from_slice(&patches[0].body).expect("PATCH body JSON");
    let clear_body: Value = serde_json::from_slice(&patches[1].body).expect("PATCH body JSON");
    assert_eq!(replace_body, replacement);
    assert_eq!(clear_body, json!({"source_bindings": {}}));
    assert!(patches
        .iter()
        .all(|request| request.header("x-api-key") == Some(API_KEY)));
}

#[test]
fn secret_read_resolves_name_and_id_and_displays_only_the_derived_secret() {
    for (tier, identifier) in [("local", AGENT_NAME), ("cluster", AGENT_ID)] {
        let fixture = Fixture::new();
        let output = success_json(&fixture.run(tier, &["secret", identifier, "--json"]));
        assert_eq!(output["secret"], HOOK_SECRET);
        assert_eq!(
            requests(&fixture.api),
            [
                ("GET".into(), "/agents".into()),
                ("GET".into(), format!("/agents/{AGENT_ID}/hook-secret")),
            ]
        );

        let human = fixture.run(tier, &["secret", identifier]);
        assert!(human.status.success(), "human read must succeed");
        assert!(
            String::from_utf8_lossy(&human.stdout).contains(HOOK_SECRET),
            "the deliberate human read must show the secret"
        );
    }
}

#[test]
fn malformed_config_is_rejected_before_any_api_request() {
    let fixture = Fixture::new();
    let temp = tempfile::tempdir().expect("temporary config directory");
    let config = temp.path().join("hooks.json");
    fs::write(&config, r#"{"source_bindings":[]}"#).expect("write malformed input");
    let output = fixture.run(
        "local",
        &[
            "configure",
            AGENT_NAME,
            "--file",
            config.to_str().expect("UTF8 config path"),
            "--json",
        ],
    );
    assert!(!output.status.success(), "an array is not a binding map");
    assert!(
        fixture.api.recorded().is_empty(),
        "invalid input must not dial the API"
    );
}

#[test]
fn secret_dry_run_redacts_credentials_and_does_not_read_the_api() {
    let fixture = Fixture::new();
    let output =
        success_json(&fixture.run("cluster", &["secret", AGENT_NAME, "--dry-run", "--json"]));
    let rendered = output.to_string();
    assert!(!rendered.contains(API_KEY), "plan exposed the API key");
    assert!(
        !rendered.contains(HOOK_SECRET),
        "plan exposed the hook secret"
    );
    assert!(
        fixture.api.recorded().is_empty(),
        "dry run must not dial the API"
    );
}

#[test]
fn missing_agent_and_failed_secret_read_exit_without_disclosing_a_secret() {
    let missing = serve(
        |request| match (request.method.as_str(), request.path.as_str()) {
            ("GET", "/agents") => Response::json(200, "[]"),
            other => panic!("unexpected request: {other:?}"),
        },
    );
    let missing_output = Command::new(bin())
        .args([
            "local",
            "hooks",
            "secret",
            "absent",
            "--api-url",
            &missing.base_url,
            "--api-key",
            API_KEY,
            "--json",
        ])
        .env_remove("CURIE_API_URL")
        .env_remove("CURIE_API_KEY")
        .output()
        .expect("run missing agent read");
    assert!(!missing_output.status.success());
    assert_eq!(requests(&missing), [("GET".into(), "/agents".into())]);

    let failing = serve(
        |request| match (request.method.as_str(), request.path.as_str()) {
            ("GET", "/agents") => Response::json(200, &json!([agent()]).to_string()),
            ("GET", path) if path == format!("/agents/{AGENT_ID}/hook-secret") => {
                Response::json(503, r#"{"detail":"service unavailable"}"#)
            }
            other => panic!("unexpected request: {other:?}"),
        },
    );
    let failed_output = Command::new(bin())
        .args([
            "local",
            "hooks",
            "secret",
            AGENT_NAME,
            "--api-url",
            &failing.base_url,
            "--api-key",
            API_KEY,
            "--json",
        ])
        .env_remove("CURIE_API_URL")
        .env_remove("CURIE_API_KEY")
        .output()
        .expect("run failed secret read");
    assert!(!failed_output.status.success());
    let visible = format!(
        "{}{}",
        String::from_utf8_lossy(&failed_output.stdout),
        String::from_utf8_lossy(&failed_output.stderr)
    );
    assert!(!visible.contains(HOOK_SECRET));
    assert!(!visible.contains(API_KEY));
    assert_eq!(
        requests(&failing),
        [
            ("GET".into(), "/agents".into()),
            ("GET".into(), format!("/agents/{AGENT_ID}/hook-secret")),
        ]
    );
}
