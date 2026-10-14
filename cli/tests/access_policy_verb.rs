//! Integration: `curie cluster access-policy` (ADR 0220 section 8, #4585).
//!
//! An agent may name an invocation team and an approver team. This verb shows
//! and changes that policy through `GET`/`PUT /agents/{id}/access-policy`. The
//! tests drive the BUILT BINARY against an in-process mock of the platform API,
//! so they cover clap wiring, the read-then-write with `expected_version`, the
//! merge that keeps an unmentioned role, and the `--json` payload validated
//! against the committed `access-policy.schema.json`.

use crate::support;

use std::process::Command;
use std::sync::{Arc, Mutex};
use support::{serve, MockServer, Response};

const AGENT_ID: &str = "55555555-5555-5555-5555-555555555555";
const AGENT_NAME: &str = "acme-bot";
const REQUESTERS: &str = "00000000-0000-0000-0000-0000000000b1";
const APPROVERS: &str = "00000000-0000-0000-0000-0000000000b2";

fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_curie")
}

fn policy_path() -> String {
    format!("/agents/{AGENT_ID}/access-policy")
}

fn agent_json() -> String {
    format!(
        r#"{{"id":"{AGENT_ID}","name":"{AGENT_NAME}","channels":[],"created_at":"2026-10-14T00:00:00Z","memory":false}}"#
    )
}

#[derive(Clone)]
struct Stored {
    invocation: Option<String>,
    approver: Option<String>,
    version: u64,
}

fn policy_json(stored: &Stored) -> String {
    serde_json::json!({
        "agent_id": AGENT_ID,
        "tenant_id": "00000000-0000-0000-0000-000000000001",
        "invocation_team_id": stored.invocation,
        "approver_team_id": stored.approver,
        "version": stored.version,
        "updated_at": if stored.version == 0 { serde_json::Value::Null } else { "2026-10-14T00:00:00Z".into() },
    })
    .to_string()
}

/// A mock API holding one agent and its policy. A PUT is refused with 409
/// unless it names the stored version, like the real route.
fn api(initial: Stored) -> MockServer {
    let state = Arc::new(Mutex::new(initial));
    serve(move |req| {
        let (m, p) = (req.method.as_str(), req.path.as_str());
        let mut stored = state.lock().expect("state lock");
        match m {
            "GET" if p == "/agents" => Response::json(200, &format!("[{}]", agent_json())),
            "GET" if p == policy_path() => Response::json(200, &policy_json(&stored)),
            "PUT" if p == policy_path() => {
                let body: serde_json::Value =
                    serde_json::from_slice(&req.body).expect("the PUT body is JSON");
                if body["expected_version"].as_u64() != Some(stored.version) {
                    return Response::json(409, r#"{"detail":"version mismatch"}"#);
                }
                stored.invocation = body["invocation_team_id"].as_str().map(str::to_string);
                stored.approver = body["approver_team_id"].as_str().map(str::to_string);
                stored.version += 1;
                Response::json(200, &policy_json(&stored))
            }
            _ => Response::json(405, r#"{"detail":"unexpected request"}"#),
        }
    })
}

struct Run {
    code: i32,
    stdout: String,
    stderr: String,
}

fn run(argv: &[&str]) -> Run {
    let output = Command::new(bin())
        .args(argv)
        .env_remove("CURIE_API_URL")
        .env_remove("CURIE_API_KEY")
        .env("KUBECONFIG", "/nonexistent/curie-test-kubeconfig")
        .output()
        .unwrap_or_else(|e| panic!("run curie {}: {e}", argv.join(" ")));
    Run {
        code: output.status.code().unwrap_or(-1),
        stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
        stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
    }
}

fn args<'a>(base_url: &'a str, extra: &[&'a str]) -> Vec<&'a str> {
    let mut argv = vec!["cluster", "access-policy", AGENT_NAME];
    argv.extend_from_slice(extra);
    argv.extend_from_slice(&["--api-url", base_url, "--api-key", "k"]);
    argv
}

fn puts(server: &MockServer) -> Vec<serde_json::Value> {
    server
        .recorded()
        .into_iter()
        .filter(|r| r.method == "PUT")
        .map(|r| serde_json::from_slice(&r.body).expect("the PUT body is JSON"))
        .collect()
}

fn assert_matches_schema(value: &serde_json::Value) {
    let schema: serde_json::Value =
        serde_json::from_str(include_str!("../schema/access-policy.schema.json"))
            .expect("access-policy.schema.json parses");
    let validator = jsonschema::validator_for(&schema).expect("schema compiles");
    let errors: Vec<String> = validator
        .iter_errors(value)
        .map(|e| e.to_string())
        .collect();
    assert!(
        errors.is_empty(),
        "payload violates the schema: {errors:?}\n{value}"
    );
}

fn empty() -> Stored {
    Stored {
        invocation: None,
        approver: None,
        version: 0,
    }
}

#[test]
fn no_flag_shows_the_policy_and_writes_nothing() {
    let server = api(empty());
    let json = run(&args(&server.base_url, &["--json"]));
    assert_eq!(json.code, 0, "{}{}", json.stdout, json.stderr);
    let value: serde_json::Value = serde_json::from_str(&json.stdout).expect("one JSON object");
    assert_matches_schema(&value);
    assert_eq!(value["version"], 0);
    assert!(value["invocation_team_id"].is_null());
    assert_eq!(value["changed"], false);
    assert!(puts(&server).is_empty(), "a show must never write");
}

#[test]
fn setting_one_role_sends_the_read_version_and_keeps_the_other() {
    let server = api(Stored {
        invocation: None,
        approver: Some(APPROVERS.to_string()),
        version: 4,
    });
    let run = run(&args(
        &server.base_url,
        &["--invocation-team", REQUESTERS, "--json"],
    ));
    assert_eq!(run.code, 0, "{}{}", run.stdout, run.stderr);
    assert_eq!(
        puts(&server),
        vec![serde_json::json!({
            "invocation_team_id": REQUESTERS,
            "approver_team_id": APPROVERS,
            "expected_version": 4,
        })]
    );
    let value: serde_json::Value = serde_json::from_str(&run.stdout).expect("one JSON object");
    assert_matches_schema(&value);
    assert_eq!(value["version"], 5);
    assert_eq!(value["changed"], true);
}

#[test]
fn clearing_a_role_sends_an_explicit_null() {
    let server = api(Stored {
        invocation: Some(REQUESTERS.to_string()),
        approver: Some(APPROVERS.to_string()),
        version: 1,
    });
    let run = run(&args(
        &server.base_url,
        &["--clear-approver-team", "--json"],
    ));
    assert_eq!(run.code, 0, "{}{}", run.stdout, run.stderr);
    let sent = puts(&server);
    assert_eq!(sent.len(), 1);
    assert!(sent[0]["approver_team_id"].is_null());
    assert_eq!(sent[0]["invocation_team_id"], REQUESTERS);
}

#[test]
fn a_malformed_team_id_is_a_usage_error_before_any_request() {
    let server = api(empty());
    let run = run(&args(&server.base_url, &["--invocation-team", "finance"]));
    assert_eq!(run.code, 2, "{}{}", run.stdout, run.stderr);
    assert!(server.recorded().is_empty());
}

#[test]
fn set_and_clear_of_one_role_conflict() {
    let server = api(empty());
    let run = run(&args(
        &server.base_url,
        &["--approver-team", APPROVERS, "--clear-approver-team"],
    ));
    assert_eq!(run.code, 2, "{}{}", run.stdout, run.stderr);
    assert!(server.recorded().is_empty());
}

#[test]
fn a_dry_run_makes_no_request() {
    let server = api(empty());
    let run = run(&args(
        &server.base_url,
        &["--approver-team", APPROVERS, "--dry-run", "--json"],
    ));
    assert_eq!(run.code, 0, "{}{}", run.stdout, run.stderr);
    let value: serde_json::Value = serde_json::from_str(&run.stdout).expect("one JSON object");
    assert_matches_schema(&value);
    assert_eq!(value["dry_run"], true);
    assert!(server.recorded().is_empty());
}
