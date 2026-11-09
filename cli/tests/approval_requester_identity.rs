//! Integration: `curie <tier> approvals <agent> --requester-identity
//! <on|off|show> [--expected-version N]` (ADR 0270 decision 1,
//! `@spec CONNECTOR-REQUESTER-1`).
//!
//! The operator's connector requester identity flag lives on the agent row,
//! versioned by `connector_requester_identity_version`, and is written only by
//! a `PATCH /agents/{id}` compare-and-set that carries nothing else. These tests
//! drive the BUILT BINARY against an in-process mock of the platform API that
//! models that compare-and-set, so they cover clap wiring, the read-then-write
//! that supplies the current version, the exact PATCH body, the `--json`
//! readback validated against the committed `approvals.schema.json`, the 409
//! recovery, the usage refusals and the dry-run plan.

use crate::support;

use std::process::Command;
use std::sync::{Arc, Mutex};
use support::{serve, MockServer, Response};

const AGENT_ID: &str = "66666666-6666-6666-6666-666666666666";
const AGENT_NAME: &str = "acme-bot";
const FLAG: &str = "connector_requester_identity";
const EXPECTED: &str = "connector_requester_identity_expected_version";
const VERSION: &str = "connector_requester_identity_version";
const CONFLICT: &str = "agent.requester_identity_version_conflict";

fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_curie")
}

fn agent_path() -> String {
    format!("/agents/{AGENT_ID}")
}

#[derive(Clone, Copy, Debug, PartialEq)]
struct Stored {
    enabled: bool,
    version: u64,
}

fn agent_json(stored: Stored) -> serde_json::Value {
    let mut agent = serde_json::json!({
        "id": AGENT_ID,
        "name": AGENT_NAME,
        "channels": [{"kind": "slack", "address": "C0EXAMPLE0"}],
        "approval_required_tools": null,
        "approval_routes": null,
        "memory": false,
        "created_at": "2026-11-07T00:00:00Z",
    });
    agent[FLAG] = stored.enabled.into();
    agent[VERSION] = stored.version.into();
    agent
}

fn conflict_body() -> String {
    serde_json::json!({
        "detail": {
            "code": CONFLICT,
            "message": "connector requester identity version changed; retry the read",
        }
    })
    .to_string()
}

/// A mock API holding one agent. A PATCH is refused unless it carries exactly
/// the flag and its expected version (422, like the real route), and with 409
/// unless the expected version is the stored one. With `race` set, every PATCH
/// is a 409: another operator wrote between the CLI's read and its write.
fn api(initial: Stored, race: bool) -> (MockServer, Arc<Mutex<Stored>>) {
    let state = Arc::new(Mutex::new(initial));
    let server_state = Arc::clone(&state);
    let server = serve(move |req| {
        let path = req.path.split('?').next().unwrap_or_default().to_string();
        let mut stored = server_state.lock().expect("state lock");
        match (req.method.as_str(), path.as_str()) {
            ("GET", "/agents") => {
                Response::json(200, &serde_json::json!([agent_json(*stored)]).to_string())
            }
            ("GET", p) if p == agent_path() => {
                Response::json(200, &agent_json(*stored).to_string())
            }
            ("GET", "/deployments") => Response::json(200, "[]"),
            ("PATCH", p) if p == agent_path() => {
                let body: serde_json::Value =
                    serde_json::from_slice(&req.body).expect("the PATCH body is JSON");
                let keys: Vec<&str> = body
                    .as_object()
                    .map(|o| o.keys().map(String::as_str).collect())
                    .unwrap_or_default();
                if keys.len() != 2 || !keys.contains(&FLAG) || !keys.contains(&EXPECTED) {
                    return Response::json(
                        422,
                        r#"{"detail":"requester_identity_write_must_be_alone"}"#,
                    );
                }
                let (Some(enabled), Some(expected)) =
                    (body[FLAG].as_bool(), body[EXPECTED].as_u64())
                else {
                    return Response::json(422, r#"{"detail":"malformed flag write"}"#);
                };
                if race || expected != stored.version {
                    return Response::json(409, &conflict_body());
                }
                *stored = Stored {
                    enabled,
                    version: stored.version + 1,
                };
                Response::json(200, &agent_json(*stored).to_string())
            }
            _ => Response::json(405, r#"{"detail":"unexpected request"}"#),
        }
    });
    (server, state)
}

struct Run {
    code: i32,
    stdout: String,
    stderr: String,
}

fn run(argv: &[&str]) -> Run {
    let config = tempfile::tempdir().expect("private config dir");
    let output = Command::new(bin())
        .args(argv)
        .env("CURIE_CONFIG_DIR", config.path())
        .env_remove("CURIE_API_URL")
        .env_remove("CURIE_API_KEY")
        .env_remove("CURIE_APPROVAL_PRINCIPAL_TOKEN")
        .env("KUBECONFIG", "/nonexistent/curie-test-kubeconfig")
        .output()
        .unwrap_or_else(|e| panic!("run curie {}: {e}", argv.join(" ")));
    Run {
        code: output.status.code().unwrap_or(-1),
        stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
        stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
    }
}

fn args<'a>(tier: &'a str, base_url: &'a str, extra: &[&'a str]) -> Vec<&'a str> {
    let mut argv = vec![tier, "approvals", AGENT_NAME];
    argv.extend_from_slice(extra);
    argv.extend_from_slice(&["--api-url", base_url, "--api-key", "k"]);
    argv
}

fn patches(server: &MockServer) -> Vec<serde_json::Value> {
    server
        .recorded()
        .into_iter()
        .filter(|r| r.method == "PATCH")
        .map(|r| {
            assert_eq!(r.path, agent_path(), "the write goes to the agent row");
            serde_json::from_slice(&r.body).expect("the PATCH body is JSON")
        })
        .collect()
}

fn one_json(run: &Run) -> serde_json::Value {
    serde_json::from_str(run.stdout.trim()).unwrap_or_else(|e| {
        panic!(
            "stdout must be one JSON object: {e}\nstdout: {}\nstderr: {}",
            run.stdout, run.stderr
        )
    })
}

fn assert_matches_schema(schema_json: &str, value: &serde_json::Value) {
    let schema: serde_json::Value = serde_json::from_str(schema_json).expect("the schema parses");
    let validator = jsonschema::validator_for(&schema).expect("the schema compiles");
    let errors: Vec<String> = validator
        .iter_errors(value)
        .map(|e| e.to_string())
        .collect();
    assert!(
        errors.is_empty(),
        "payload violates the schema: {errors:?}\n{value}"
    );
}

fn approvals_schema() -> &'static str {
    include_str!("../schema/approvals.schema.json")
}

fn error_schema() -> &'static str {
    include_str!("../schema/error.schema.json")
}

fn readback(enabled: bool, version: u64) -> serde_json::Value {
    let mut value = serde_json::json!({"agent": AGENT_NAME, "agent_id": AGENT_ID});
    value[FLAG] = enabled.into();
    value[VERSION] = version.into();
    value
}

fn flag_write(enabled: bool, expected: u64) -> serde_json::Value {
    let mut value = serde_json::json!({});
    value[FLAG] = enabled.into();
    value[EXPECTED] = expected.into();
    value
}

#[test]
fn show_reads_the_flag_and_writes_nothing() {
    let (server, _) = api(
        Stored {
            enabled: true,
            version: 6,
        },
        false,
    );
    let run = run(&args(
        "cluster",
        &server.base_url,
        &["--requester-identity", "show", "--json"],
    ));
    assert_eq!(run.code, 0, "{}{}", run.stdout, run.stderr);
    let value = one_json(&run);
    assert_eq!(value, readback(true, 6));
    assert_matches_schema(approvals_schema(), &value);
    assert!(patches(&server).is_empty(), "a show must never write");
}

#[test]
fn show_is_wired_on_the_local_tier_too() {
    let (server, _) = api(
        Stored {
            enabled: false,
            version: 1,
        },
        false,
    );
    let run = run(&args(
        "local",
        &server.base_url,
        &["--requester-identity", "show", "--json"],
    ));
    assert_eq!(run.code, 0, "{}{}", run.stdout, run.stderr);
    assert_eq!(one_json(&run), readback(false, 1));
    assert!(patches(&server).is_empty(), "a show must never write");
}

#[test]
fn on_without_a_version_reads_the_agent_and_sends_its_current_version() {
    let (server, state) = api(
        Stored {
            enabled: false,
            version: 4,
        },
        false,
    );
    let run = run(&args(
        "cluster",
        &server.base_url,
        &["--requester-identity", "on", "--json"],
    ));
    assert_eq!(run.code, 0, "{}{}", run.stdout, run.stderr);
    // The flag write is alone in its PATCH: the API refuses any other field.
    assert_eq!(patches(&server), vec![flag_write(true, 4)]);
    let recorded = server.recorded();
    let first_patch = recorded
        .iter()
        .position(|r| r.method == "PATCH")
        .expect("one PATCH was sent");
    assert!(
        recorded[..first_patch].iter().any(|r| r.method == "GET"),
        "the current version must be read before the write: {:?}",
        recorded
            .iter()
            .map(|r| format!("{} {}", r.method, r.path))
            .collect::<Vec<_>>()
    );
    let value = one_json(&run);
    assert_eq!(value, readback(true, 5));
    assert_matches_schema(approvals_schema(), &value);
    assert_eq!(
        *state.lock().unwrap(),
        Stored {
            enabled: true,
            version: 5
        }
    );
}

#[test]
fn off_with_an_explicit_version_sends_that_version() {
    let (server, _) = api(
        Stored {
            enabled: true,
            version: 7,
        },
        false,
    );
    let run = run(&args(
        "cluster",
        &server.base_url,
        &[
            "--requester-identity",
            "off",
            "--expected-version",
            "7",
            "--json",
        ],
    ));
    assert_eq!(run.code, 0, "{}{}", run.stdout, run.stderr);
    assert_eq!(patches(&server), vec![flag_write(false, 7)]);
    let value = one_json(&run);
    assert_eq!(value, readback(false, 8));
    assert_matches_schema(approvals_schema(), &value);
}

#[test]
fn a_stale_explicit_version_is_a_recovery_not_a_silent_retry() {
    let initial = Stored {
        enabled: false,
        version: 4,
    };
    let (server, state) = api(initial, false);
    let run = run(&args(
        "cluster",
        &server.base_url,
        &[
            "--requester-identity",
            "on",
            "--expected-version",
            "3",
            "--json",
        ],
    ));
    assert_ne!(run.code, 0, "{}{}", run.stdout, run.stderr);
    let value = one_json(&run);
    assert_matches_schema(error_schema(), &value);
    assert!(
        value["error"].as_str().is_some_and(|e| !e.is_empty()),
        "{value}"
    );
    assert!(
        value["fix"].as_str().is_some_and(|f| !f.trim().is_empty()),
        "a version conflict must carry a recovery: {value}"
    );
    // The operator named the version; the CLI must not swap in a fresh one.
    assert_eq!(patches(&server), vec![flag_write(true, 3)]);
    assert_eq!(*state.lock().unwrap(), initial);
}

#[test]
fn a_write_raced_between_read_and_patch_is_a_recovery() {
    let initial = Stored {
        enabled: false,
        version: 4,
    };
    let (server, state) = api(initial, true);
    let run = run(&args(
        "cluster",
        &server.base_url,
        &["--requester-identity", "on", "--json"],
    ));
    assert_ne!(run.code, 0, "{}{}", run.stdout, run.stderr);
    let value = one_json(&run);
    assert_matches_schema(error_schema(), &value);
    assert!(
        value["fix"].as_str().is_some_and(|f| !f.trim().is_empty()),
        "a version conflict must carry a recovery: {value}"
    );
    assert_eq!(patches(&server), vec![flag_write(true, 4)]);
    assert_eq!(*state.lock().unwrap(), initial);
}

#[test]
fn requester_identity_with_route_or_gate_flags_is_a_usage_error() {
    for other in [
        &["--gate", "send_email"][..],
        &["--clear"][..],
        &["--route-resolution", "deal_desk=C0EXAMPLE1"][..],
        &["--clear-routes"][..],
    ] {
        let (server, state) = api(
            Stored {
                enabled: false,
                version: 1,
            },
            false,
        );
        let mut extra = vec!["--requester-identity", "on"];
        extra.extend_from_slice(other);
        let run = run(&args("cluster", &server.base_url, &extra));
        assert_eq!(run.code, 2, "{other:?}: {}{}", run.stdout, run.stderr);
        assert!(patches(&server).is_empty(), "{other:?} must send no PATCH");
        assert_eq!(
            *state.lock().unwrap(),
            Stored {
                enabled: false,
                version: 1
            }
        );
    }
}

#[test]
fn an_unknown_mode_is_a_usage_error_before_any_request() {
    let (server, _) = api(
        Stored {
            enabled: false,
            version: 1,
        },
        false,
    );
    let run = run(&args(
        "cluster",
        &server.base_url,
        &["--requester-identity", "maybe"],
    ));
    assert_eq!(run.code, 2, "{}{}", run.stdout, run.stderr);
    assert!(server.recorded().is_empty());
}

#[test]
fn a_dry_run_prints_the_patch_plan_and_sends_nothing() {
    let (server, state) = api(
        Stored {
            enabled: false,
            version: 2,
        },
        false,
    );
    let run = run(&args(
        "cluster",
        &server.base_url,
        &[
            "--requester-identity",
            "on",
            "--expected-version",
            "2",
            "--dry-run",
            "--json",
        ],
    ));
    assert_eq!(run.code, 0, "{}{}", run.stdout, run.stderr);
    let value = one_json(&run);
    assert_matches_schema(approvals_schema(), &value);
    assert_eq!(value["dry_run"], true, "{value}");
    let plan = value["plan"]
        .as_array()
        .expect("a dry run carries its plan lines")
        .iter()
        .filter_map(serde_json::Value::as_str)
        .collect::<Vec<_>>()
        .join("\n");
    assert!(plan.contains("PATCH"), "{plan}");
    assert!(plan.contains(FLAG), "{plan}");
    assert!(patches(&server).is_empty(), "a dry run must send no PATCH");
    assert_eq!(
        *state.lock().unwrap(),
        Stored {
            enabled: false,
            version: 2
        }
    );
}
