//! The remediation operator surface beyond the policy: receipts, breakers and
//! qualification (AUTOMATED-REMEDIATION-20, -11, -22, -23).
//!
//! Three verb families under BOTH `curie local` and `curie cluster`, driven
//! against a wire-level stub of the platform API as `remediation_policy_cli.rs`
//! does. Every verb prints exactly one JSON object under `--json`; every error
//! is the ADR-0021 `{"error","fix"}` object with its exit class (1 failure,
//! 2 usage, 3 transient or unreachable).
//!
//! Receipts (`remediation`; read only, platform key, no principal):
//! - `list [<agent>] [--state <state>] [--limit <n>]`
//!   -> `GET /remediation-nominations[?agent_id=<id>][&state=..][&limit=..]`;
//!   `<agent>` is a name or id resolved as the other verbs resolve it, and with
//!   none no agent lookup is made. Output `{"nominations":[<row>, ..]}`.
//! - `show <nomination id>` -> `GET /remediation-nominations/{id}`; the output
//!   is the API row unchanged, for every terminal state. A malformed id, an
//!   unknown state (not one of the API's `nomination_states`) or a limit
//!   outside 1 to 200 is a usage error before any request.
//!
//! Breakers (`remediation-policy breakers <agent> <hook> [--state open|closed|all]`):
//! `GET /agents/{id}/hooks/{hook}/remediation-policy/breakers[?state=..]`, read
//! only, output `{"breakers":[<row>, ..]}`, so an operator finds the id
//! `close-breaker` needs.
//!
//! Qualification (`remediation-qualification`, task 15's routes):
//! - `record <agent> <qualification id> --hook <h> --action <a> --generation <n>
//!   --evidence-file <path> --worst-case <text>`
//!   -> `PUT /agents/{id}/remediation-qualifications/{qid}` body
//!   `{"hook","action","generation":"<n>","evidence":<file json unchanged>,"worst_case"}`.
//! - `start-run <agent> <qualification id> --hook <h> --action <a> --target <literal>`
//!   -> `POST .../{qid}/verifier-runs` body `{"hook","action","target"}` and no other
//!   field (a JSON number or boolean literal is sent as that type, anything else
//!   as a string).
//! - `show-run <agent> <qualification id> <run id>` -> `GET .../verifier-runs/{run}`.
//!
//! The two writes send the operator principal from `CURIE_APPROVAL_PRINCIPAL_TOKEN`
//! in `X-Curie-Approval-Principal`; without one they exit 2 before ANY request
//! (not even the agent lookup) and, at the cluster tier, before connection
//! discovery. Ids must be canonical lowercase UUIDs. The token is never printed.

mod support;

use std::process::{Command, Output};

use serde_json::{json, Value};
use support::{serve, MockServer, Request, Response};

const TEST_API_KEY: &str = "test-platform-key";
const OPERATOR_PRINCIPAL: &str = "apr.test.operator-principal-that-must-not-leak";
const AGENT_ID: &str = "55555555-5555-5555-5555-555555555555";
const AGENT_NAME: &str = "acme-bot";
const HOOK: &str = "alerts";
const NOMINATION: &str = "66666666-6666-4666-8666-666666666666";
const UNKNOWN_NOMINATION: &str = "77777777-7777-4777-8777-777777777777";
const QUALIFICATION: &str = "88888888-8888-4888-8888-888888888888";
const RUN: &str = "99999999-9999-4999-8999-999999999999";
const BREAKER: &str = "22222222-2222-4222-8222-222222222222";

fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_curie")
}

fn text(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).into_owned() + &String::from_utf8_lossy(&output.stderr)
}

fn agent_json() -> String {
    format!(
        r#"{{"id":"{AGENT_ID}","name":"{AGENT_NAME}","channels":[],"created_at":"2026-10-01T00:00:00Z","memory":false}}"#
    )
}

/// One nomination row exactly as `GET /remediation-nominations/{id}` answers.
fn row(id: &str, state: &str, stage: &str, authority: &str, code: Value, outcome: Value) -> Value {
    json!({
        "id": id,
        "agent_id": AGENT_ID,
        "hook": HOOK,
        "kind": "remediate",
        "action": "scale-out-api",
        "target": "k8s:\"example-api\"",
        "state": state,
        "stage": stage,
        "authority": authority,
        "code": code,
        "verification_outcome": outcome,
        "approval_id": null,
        "execution_id": null,
        "created_at": "2026-10-07T00:00:00Z",
        "decided_at": "2026-10-07T00:05:00Z",
    })
}

/// A terminal row of each kind the receipt can end in.
fn terminal_rows() -> Vec<Value> {
    vec![
        row(NOMINATION, "refused", "refused", "none", json!("unknown_action"), Value::Null),
        row("a0000000-0000-4000-8000-000000000001", "finished", "verified", "policy", Value::Null, json!("verified")),
        row("a0000000-0000-4000-8000-000000000002", "finished", "not-recovered", "approval", Value::Null, json!("not-recovered")),
        row("a0000000-0000-4000-8000-000000000003", "finished", "verifier-unavailable", "policy", Value::Null, json!("verifier-unavailable")),
        row("a0000000-0000-4000-8000-000000000004", "finished", "superseded", "policy", Value::Null, json!("superseded")),
        row("a0000000-0000-4000-8000-000000000005", "rejected", "approval_requested", "none", json!("out_of_bounds"), Value::Null),
        row("a0000000-0000-4000-8000-000000000006", "expired", "approval_requested", "none", json!("breaker_open"), Value::Null),
    ]
}

fn breaker_row(closed: bool) -> Value {
    json!({
        "id": BREAKER,
        "agent_id": AGENT_ID,
        "connector": "k8s",
        "tool": "scale_deployment",
        "target": "k8s:\"example-api\"",
        "opened_at": "2026-10-07T00:00:00Z",
        "closed_at": if closed { json!("2026-10-07T01:00:00Z") } else { Value::Null },
        "closed_by": if closed { json!("operator@example.com") } else { Value::Null },
        "close_reason": if closed { json!("fixed") } else { Value::Null },
    })
}

fn qualification_out() -> Value {
    json!({
        "id": QUALIFICATION,
        "agent_id": AGENT_ID,
        "hook": HOOK,
        "action": "scale-out-api",
        "generation": "4",
        "connector": "k8s",
        "tool": "scale_deployment",
        "connector_digest": format!("sha256:{}", "ab".repeat(32)),
        "verifier_sha256": "cd".repeat(32),
        "reversibility": "reversible",
        "recorded_by": "operator@example.com",
        "worst_case": "scales one deployment to six replicas",
        "evidence": {},
        "created_at": "2026-10-07T00:00:00Z",
    })
}

fn run_out(outcome: Value) -> Value {
    json!({
        "id": RUN,
        "qualification_id": QUALIFICATION,
        "hook": HOOK,
        "action": "scale-out-api",
        "target": "example-api",
        "generation": "4",
        "started_by": "operator@example.com",
        "started_at": "2026-10-07T00:00:00Z",
        "outcome": outcome,
        "decided_at": Value::Null,
    })
}

fn refusal(status: u16, code: &str) -> Response {
    Response::json(status, &json!({ "detail": { "code": code } }).to_string())
}

fn policy_base() -> String {
    format!("/agents/{AGENT_ID}/hooks/{HOOK}/remediation-policy")
}

fn qualification_base() -> String {
    format!("/agents/{AGENT_ID}/remediation-qualifications/{QUALIFICATION}")
}

fn api() -> MockServer {
    serve(|request: &Request| {
        let method = request.method.as_str();
        let (route, query) = request.path.split_once('?').unwrap_or((&request.path, ""));
        match (method, route) {
            ("GET", "/agents") => Response::json(200, &format!("[{}]", agent_json())),
            ("GET", p) if p == format!("/agents/{AGENT_ID}") => Response::json(200, &agent_json()),
            ("GET", "/remediation-nominations") => {
                Response::json(200, &Value::Array(terminal_rows()).to_string())
            }
            ("GET", p) if p == format!("/remediation-nominations/{UNKNOWN_NOMINATION}") => {
                Response::json(404, r#"{"detail":"remediation nomination not found"}"#)
            }
            ("GET", p) if p.starts_with("/remediation-nominations/") => {
                let id = p.trim_start_matches("/remediation-nominations/");
                match terminal_rows().into_iter().find(|r| r["id"] == id) {
                    Some(found) => Response::json(200, &found.to_string()),
                    None => Response::json(404, r#"{"detail":"remediation nomination not found"}"#),
                }
            }
            ("GET", p) if p == format!("{}/breakers", policy_base()) => {
                let closed = query.contains("state=closed");
                Response::json(200, &json!([breaker_row(closed)]).to_string())
            }
            ("PUT", p) if p == qualification_base() => {
                Response::json(200, &qualification_out().to_string())
            }
            ("POST", p) if p == format!("{}/verifier-runs", qualification_base()) => {
                Response::json(201, &run_out(Value::Null).to_string())
            }
            ("GET", p) if p == format!("{}/verifier-runs/{RUN}", qualification_base()) => {
                Response::json(200, &run_out(json!("verified")).to_string())
            }
            _ => Response::json(405, r#"{"detail":"unexpected request"}"#),
        }
    })
}

fn refusing_api(status: u16, code: &'static str) -> MockServer {
    serve(move |request: &Request| {
        let route = request.path.split('?').next().unwrap_or(&request.path);
        match (request.method.as_str(), route) {
            ("GET", "/agents") => Response::json(200, &format!("[{}]", agent_json())),
            ("GET", p) if p == format!("/agents/{AGENT_ID}") => Response::json(200, &agent_json()),
            _ => refusal(status, code),
        }
    })
}

fn run_at(
    tier: &str,
    group: &str,
    args: &[&str],
    base_url: &str,
    principal: Option<&str>,
) -> Output {
    let mut command = Command::new(bin());
    command.arg(tier).arg(group).args(args).args([
        "--api-url",
        base_url,
        "--api-key",
        TEST_API_KEY,
        "--json",
    ]);
    command
        .env_remove("CURIE_API_URL")
        .env_remove("CURIE_API_KEY")
        .env_remove("CURIE_APPROVAL_PRINCIPAL_TOKEN")
        .env("KUBECONFIG", "/nonexistent/curie-test-kubeconfig")
        .env("NO_COLOR", "1");
    if let Some(principal) = principal {
        command.env("CURIE_APPROVAL_PRINCIPAL_TOKEN", principal);
    }
    command
        .output()
        .unwrap_or_else(|err| panic!("run curie {tier} {group} {}: {err}", args.join(" ")))
}

fn run(tier: &str, group: &str, args: &[&str], server: &MockServer, principal: Option<&str>) -> Output {
    run_at(tier, group, args, &server.base_url, principal)
}

fn one_object(output: &Output, what: &str) -> Value {
    let stdout = String::from_utf8_lossy(&output.stdout);
    let mut values = serde_json::Deserializer::from_str(&stdout).into_iter::<Value>();
    let first = match values.next() {
        Some(Ok(value)) => value,
        other => panic!("{what}: --json stdout must be one JSON object, got {other:?}\n{}", text(output)),
    };
    assert!(values.next().is_none(), "{what}: exactly one JSON value\n{stdout}");
    assert!(first.is_object(), "{what}: must be an object, not {first}");
    first
}

fn assert_error_object(value: &Value, what: &str) {
    assert!(
        value["error"].as_str().is_some_and(|e| !e.is_empty()),
        "{what}: an error carries a non-empty `error`: {value}"
    );
    assert!(value.get("fix").is_some(), "{what}: an error carries `fix`: {value}");
}

fn assert_valid(schema_file: &str, value: &Value, what: &str) {
    let path = format!("{}/schema/{schema_file}", env!("CARGO_MANIFEST_DIR"));
    let schema: Value = serde_json::from_str(
        &std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {path}: {e}")),
    )
    .unwrap_or_else(|e| panic!("{path} is JSON: {e}"));
    let validator = jsonschema::validator_for(&schema).expect("schema compiles");
    assert!(
        validator.is_valid(value),
        "{what}: does not validate against {schema_file}: {value}\nerrors: {:?}",
        validator.iter_errors(value).map(|e| e.to_string()).collect::<Vec<_>>()
    );
}

fn dead_url() -> String {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind a free port");
    let port = listener.local_addr().expect("local addr").port();
    drop(listener);
    format!("http://127.0.0.1:{port}")
}

fn query_pairs(path: &str) -> Vec<(String, String)> {
    path.split_once('?')
        .map(|(_, q)| q)
        .unwrap_or("")
        .split('&')
        .filter(|kv| !kv.is_empty())
        .map(|kv| {
            let (k, v) = kv.split_once('=').unwrap_or((kv, ""));
            (k.to_owned(), v.to_owned())
        })
        .collect()
}

fn child(node: &Value, name: &str) -> Option<Value> {
    node["subcommands"].as_array()?.iter().find(|c| c["name"] == name).cloned()
}

// --------------------------------------------------------------------------
// Surface
// --------------------------------------------------------------------------

// @spec AUTOMATED-REMEDIATION-20 @spec AUTOMATED-REMEDIATION-22
#[test]
fn both_tiers_expose_the_groups_and_verbs() {
    for tier in ["local", "cluster"] {
        for (group, verbs) in [
            ("remediation", vec!["list", "show"]),
            ("remediation-policy", vec!["breakers"]),
            ("remediation-qualification", vec!["record", "start-run", "show-run"]),
        ] {
            let output = Command::new(bin())
                .args([tier, group, "--help"])
                .output()
                .unwrap_or_else(|err| panic!("run {tier} {group} --help: {err}"));
            let help = text(&output);
            assert!(output.status.success(), "{tier} {group} --help must render:\n{help}");
            for verb in verbs {
                assert!(
                    help.lines().any(|l| l.trim_start().starts_with(&format!("{verb} ")) || l.trim() == verb),
                    "{tier} {group} must expose `{verb}`; help:\n{help}"
                );
            }
        }
    }
}

// @spec AUTOMATED-REMEDIATION-20 @spec AUTOMATED-REMEDIATION-22
#[test]
fn committed_manifest_records_the_groups_under_both_tiers() {
    let manifest: Value = serde_json::from_str(include_str!("../command-manifest.json"))
        .expect("cli/command-manifest.json parses");
    for tier in ["local", "cluster"] {
        let tier_node = child(&manifest, tier).unwrap_or_else(|| panic!("manifest has {tier}"));
        for (group, verbs) in [
            ("remediation", vec!["list", "show"]),
            ("remediation-policy", vec!["breakers"]),
            ("remediation-qualification", vec!["record", "start-run", "show-run"]),
        ] {
            let node = child(&tier_node, group)
                .unwrap_or_else(|| panic!("manifest must record `{tier} {group}`"));
            for verb in verbs {
                assert!(child(&node, verb).is_some(), "manifest must record `{tier} {group} {verb}`");
            }
        }
    }
}

// --------------------------------------------------------------------------
// Receipts
// --------------------------------------------------------------------------

// @spec AUTOMATED-REMEDIATION-20
/// "The CLI `show` output matches the API row for every terminal state."
#[test]
fn show_prints_the_api_row_unchanged_for_every_terminal_state() {
    for tier in ["local", "cluster"] {
        for expected in terminal_rows() {
            let id = expected["id"].as_str().unwrap();
            let server = api();
            let output = run(tier, "remediation", &["show", id], &server, None);
            let what = format!("{tier} remediation show {id}");
            assert_eq!(output.status.code(), Some(0), "{what}:\n{}", text(&output));
            let value = one_object(&output, &what);
            assert_eq!(value, expected, "{what}: the API row, field for field");
            assert_valid("remediation-nomination.schema.json", &value, &what);
            let requests = server.recorded();
            assert_eq!(requests.len(), 1, "{what}: one read");
            assert_eq!(requests[0].method, "GET");
            assert_eq!(requests[0].path, format!("/remediation-nominations/{id}"));
            assert_eq!(requests[0].header("X-Api-Key"), Some(TEST_API_KEY));
            assert!(requests[0].header("X-Curie-Approval-Principal").is_none());
        }
    }
}

// @spec AUTOMATED-REMEDIATION-20
#[test]
fn list_with_no_agent_reads_every_nomination_without_an_agent_lookup() {
    for tier in ["local", "cluster"] {
        let server = api();
        let output = run(tier, "remediation", &["list"], &server, None);
        let what = format!("{tier} remediation list");
        assert_eq!(output.status.code(), Some(0), "{what}:\n{}", text(&output));
        let value = one_object(&output, &what);
        assert_eq!(value["nominations"], Value::Array(terminal_rows()), "{what}");
        assert_valid("remediation-nominations.schema.json", &value, &what);
        let requests = server.recorded();
        assert_eq!(requests.len(), 1, "{what}: no agent lookup");
        assert_eq!(requests[0].path, "/remediation-nominations");
    }
}

// @spec AUTOMATED-REMEDIATION-20
#[test]
fn list_resolves_an_agent_name_or_id_and_sends_the_filters() {
    for tier in ["local", "cluster"] {
        for agent in [AGENT_NAME, AGENT_ID] {
            let server = api();
            let output = run(
                tier,
                "remediation",
                &["list", agent, "--state", "refused", "--limit", "5"],
                &server,
                None,
            );
            let what = format!("{tier} remediation list {agent}");
            assert_eq!(output.status.code(), Some(0), "{what}:\n{}", text(&output));
            one_object(&output, &what);
            let read = server
                .recorded()
                .into_iter()
                .find(|r| r.path.starts_with("/remediation-nominations"))
                .expect("a nomination read");
            let mut pairs = query_pairs(&read.path);
            pairs.sort();
            assert_eq!(
                pairs,
                vec![
                    ("agent_id".to_owned(), AGENT_ID.to_owned()),
                    ("limit".to_owned(), "5".to_owned()),
                    ("state".to_owned(), "refused".to_owned()),
                ],
                "{what}"
            );
        }
    }
}

// @spec AUTOMATED-REMEDIATION-20
#[test]
fn a_malformed_receipt_input_is_a_usage_error_before_any_request() {
    for tier in ["local", "cluster"] {
        for args in [
            vec!["show", "not-a-uuid"],
            vec!["show", "66666666-6666-4666-8666-66666666666G"],
            vec!["list", "--state", "example_unknown_state"],
            vec!["list", "--limit", "0"],
            vec!["list", "--limit", "201"],
        ] {
            let server = api();
            let output = run(tier, "remediation", &args, &server, None);
            let what = format!("{tier} remediation {}", args.join(" "));
            assert_eq!(output.status.code(), Some(2), "{what}:\n{}", text(&output));
            assert_error_object(&one_object(&output, &what), &what);
            assert!(server.recorded().is_empty(), "{what}: no request");
        }
    }
}

// @spec AUTOMATED-REMEDIATION-20
#[test]
fn an_unknown_nomination_and_an_unreachable_api_are_one_error_object_each() {
    for tier in ["local", "cluster"] {
        let server = api();
        let output = run(tier, "remediation", &["show", UNKNOWN_NOMINATION], &server, None);
        let what = format!("{tier} remediation show (unknown)");
        assert_eq!(output.status.code(), Some(1), "{what}:\n{}", text(&output));
        let value = one_object(&output, &what);
        assert_error_object(&value, &what);
        assert!(value["error"].as_str().unwrap().contains("not found"), "{what}: {value}");

        let output = run_at(tier, "remediation", &["list"], &dead_url(), None);
        let what = format!("{tier} remediation list (no listener)");
        assert_eq!(output.status.code(), Some(3), "{what}:\n{}", text(&output));
        assert_error_object(&one_object(&output, &what), &what);
    }
}

// --------------------------------------------------------------------------
// Breakers
// --------------------------------------------------------------------------

// @spec AUTOMATED-REMEDIATION-11
#[test]
fn breakers_lists_the_ids_close_breaker_needs_with_no_principal() {
    for tier in ["local", "cluster"] {
        for (args, state) in [
            (vec!["breakers", AGENT_NAME, HOOK], None),
            (vec!["breakers", AGENT_ID, HOOK, "--state", "closed"], Some("closed")),
        ] {
            let server = api();
            let output = run(tier, "remediation-policy", &args, &server, None);
            let what = format!("{tier} remediation-policy {}", args.join(" "));
            assert_eq!(output.status.code(), Some(0), "{what}:\n{}", text(&output));
            let value = one_object(&output, &what);
            assert_eq!(
                value["breakers"],
                json!([breaker_row(state == Some("closed"))]),
                "{what}"
            );
            assert_valid("remediation-breakers.schema.json", &value, &what);
            let read = server
                .recorded()
                .into_iter()
                .find(|r| r.path.starts_with(&format!("{}/breakers", policy_base())))
                .expect("a breaker read");
            assert_eq!(read.method, "GET");
            assert_eq!(read.header("X-Api-Key"), Some(TEST_API_KEY));
            assert!(read.header("X-Curie-Approval-Principal").is_none());
            assert_eq!(
                query_pairs(&read.path),
                state.map(|s| vec![("state".to_owned(), s.to_owned())]).unwrap_or_default(),
                "{what}"
            );
        }
    }
}

// @spec AUTOMATED-REMEDIATION-11
#[test]
fn breakers_refuses_an_unknown_state_before_any_request() {
    for tier in ["local", "cluster"] {
        let server = api();
        let output = run(
            tier,
            "remediation-policy",
            &["breakers", AGENT_NAME, HOOK, "--state", "weird"],
            &server,
            None,
        );
        assert_eq!(output.status.code(), Some(2), "{tier}:\n{}", text(&output));
        assert_error_object(&one_object(&output, tier), tier);
        assert!(server.recorded().is_empty());
    }
}

// --------------------------------------------------------------------------
// Qualification
// --------------------------------------------------------------------------

fn record_args<'a>(evidence_file: &'a str) -> Vec<&'a str> {
    vec![
        "record", AGENT_NAME, QUALIFICATION, "--hook", HOOK, "--action", "scale-out-api",
        "--generation", "4", "--evidence-file", evidence_file, "--worst-case",
        "scales one deployment to six replicas",
    ]
}

fn start_args<'a>(target: &'a str) -> Vec<&'a str> {
    vec![
        "start-run", AGENT_NAME, QUALIFICATION, "--hook", HOOK, "--action", "scale-out-api",
        "--target", target,
    ]
}

fn evidence() -> Value {
    json!({
        "restore_execution_id": "bbbbbbbb-0000-4000-8000-000000000001",
        "conflict_execution_id": "bbbbbbbb-0000-4000-8000-000000000002",
        "verified_run_id": "bbbbbbbb-0000-4000-8000-000000000003",
        "not_recovered_run_id": "bbbbbbbb-0000-4000-8000-000000000004"
    })
}

// @spec AUTOMATED-REMEDIATION-22 @spec AUTOMATED-REMEDIATION-23
#[test]
fn record_puts_the_qualification_unchanged_under_the_principal() {
    for tier in ["local", "cluster"] {
        let dir = tempfile::tempdir().expect("tempdir");
        let file = dir.path().join("evidence.json");
        std::fs::write(&file, evidence().to_string()).unwrap();
        let server = api();
        let output = run(
            tier,
            "remediation-qualification",
            &record_args(file.to_str().unwrap()),
            &server,
            Some(OPERATOR_PRINCIPAL),
        );
        let what = format!("{tier} remediation-qualification record");
        assert_eq!(output.status.code(), Some(0), "{what}:\n{}", text(&output));
        let value = one_object(&output, &what);
        assert_eq!(value, qualification_out(), "{what}");
        assert_valid("remediation-qualification.schema.json", &value, &what);
        let writes: Vec<Request> =
            server.recorded().into_iter().filter(|r| r.method != "GET").collect();
        assert_eq!(writes.len(), 1, "{what}");
        assert_eq!(writes[0].method, "PUT");
        assert_eq!(writes[0].path, qualification_base());
        assert_eq!(writes[0].header("X-Curie-Approval-Principal"), Some(OPERATOR_PRINCIPAL));
        assert_eq!(writes[0].header("X-Api-Key"), Some(TEST_API_KEY));
        let body: Value = serde_json::from_slice(&writes[0].body).expect("body is JSON");
        assert_eq!(
            body,
            json!({
                "hook": HOOK,
                "action": "scale-out-api",
                "generation": "4",
                "evidence": evidence(),
                "worst_case": "scales one deployment to six replicas",
            }),
            "{what}"
        );
        assert!(!text(&output).contains(OPERATOR_PRINCIPAL), "{what}: token never printed");
    }
}

// @spec AUTOMATED-REMEDIATION-22
#[test]
fn start_run_posts_exactly_hook_action_and_target_under_the_principal() {
    for tier in ["local", "cluster"] {
        for (literal, sent) in [("example-api", json!("example-api")), ("7", json!(7)), ("true", json!(true))] {
            let server = api();
            let output = run(
                tier,
                "remediation-qualification",
                &start_args(literal),
                &server,
                Some(OPERATOR_PRINCIPAL),
            );
            let what = format!("{tier} remediation-qualification start-run {literal}");
            assert_eq!(output.status.code(), Some(0), "{what}:\n{}", text(&output));
            let value = one_object(&output, &what);
            assert_eq!(value["id"], RUN, "{what}");
            assert_valid("remediation-verifier-run.schema.json", &value, &what);
            let post = server
                .recorded()
                .into_iter()
                .find(|r| r.method == "POST")
                .expect("a POST");
            assert_eq!(post.path, format!("{}/verifier-runs", qualification_base()));
            assert_eq!(post.header("X-Curie-Approval-Principal"), Some(OPERATOR_PRINCIPAL));
            let body: Value = serde_json::from_slice(&post.body).expect("body is JSON");
            assert_eq!(
                body,
                json!({"hook": HOOK, "action": "scale-out-api", "target": sent}),
                "{what}: no tool, argument, connector or verifier field"
            );
            assert!(!text(&output).contains(OPERATOR_PRINCIPAL), "{what}");
        }
    }
}

// @spec AUTOMATED-REMEDIATION-22
#[test]
fn show_run_reads_the_run_and_needs_no_principal() {
    for tier in ["local", "cluster"] {
        let server = api();
        let output = run(
            tier,
            "remediation-qualification",
            &["show-run", AGENT_NAME, QUALIFICATION, RUN],
            &server,
            None,
        );
        let what = format!("{tier} remediation-qualification show-run");
        assert_eq!(output.status.code(), Some(0), "{what}:\n{}", text(&output));
        let value = one_object(&output, &what);
        assert_eq!(value["outcome"], "verified", "{what}");
        assert_valid("remediation-verifier-run.schema.json", &value, &what);
        assert!(server.recorded().iter().all(|r| r.method == "GET"), "{what}");
    }
}

// @spec AUTOMATED-REMEDIATION-22 @spec AUTOMATED-REMEDIATION-23
/// Without an operator principal both writes exit 2 before ANY request, not even
/// the agent lookup, and a blank value counts as none.
#[test]
fn the_writes_without_a_principal_are_refused_before_any_request() {
    for tier in ["local", "cluster"] {
        let dir = tempfile::tempdir().expect("tempdir");
        let file = dir.path().join("evidence.json");
        std::fs::write(&file, evidence().to_string()).unwrap();
        for principal in [None, Some(""), Some("   ")] {
            for (name, args) in [
                ("record", record_args(file.to_str().unwrap())),
                ("start-run", start_args("example-api")),
            ] {
                let server = api();
                let output = run(tier, "remediation-qualification", &args, &server, principal);
                let what = format!("{tier} remediation-qualification {name} (principal {principal:?})");
                assert_eq!(output.status.code(), Some(2), "{what}:\n{}", text(&output));
                let value = one_object(&output, &what);
                assert_error_object(&value, &what);
                assert!(
                    value.to_string().contains("CURIE_APPROVAL_PRINCIPAL_TOKEN"),
                    "{what}: the error names the env var: {value}"
                );
                assert!(server.recorded().is_empty(), "{what}: no request at all");
            }
        }
    }
}

// @spec AUTOMATED-REMEDIATION-22
#[test]
fn a_malformed_qualification_input_is_a_usage_error_before_any_request() {
    for tier in ["local", "cluster"] {
        let dir = tempfile::tempdir().expect("tempdir");
        let good = dir.path().join("evidence.json");
        std::fs::write(&good, evidence().to_string()).unwrap();
        let broken = dir.path().join("broken.json");
        std::fs::write(&broken, "{not json").unwrap();
        let mut bad_id = record_args(good.to_str().unwrap());
        bad_id[2] = "NOT-A-UUID";
        let mut bad_generation = record_args(good.to_str().unwrap());
        bad_generation[8] = "four";
        let cases: Vec<(&str, Vec<&str>)> = vec![
            ("record", bad_id),
            ("record", bad_generation),
            ("record", record_args(broken.to_str().unwrap())),
            ("record", record_args("/nonexistent/evidence.json")),
        ];
        for (name, args) in cases {
            let server = api();
            let output = run(tier, "remediation-qualification", &args, &server, Some(OPERATOR_PRINCIPAL));
            let what = format!("{tier} remediation-qualification {name} {args:?}");
            assert_eq!(output.status.code(), Some(2), "{what}:\n{}", text(&output));
            assert_error_object(&one_object(&output, &what), &what);
            assert!(server.recorded().is_empty(), "{what}: no request");
        }
    }
}

// @spec AUTOMATED-REMEDIATION-22 @spec AUTOMATED-REMEDIATION-23
#[test]
fn qualification_refusals_are_one_error_object_with_the_api_code() {
    let dir = tempfile::tempdir().expect("tempdir");
    let file = dir.path().join("evidence.json");
    std::fs::write(&file, evidence().to_string()).unwrap();
    for tier in ["local", "cluster"] {
        for (status, code, exit) in [
            (403, "operator_principal_required", 1),
            (409, "qualification_conflict", 1),
            (422, "qualification_evidence_invalid", 2),
        ] {
            let server = refusing_api(status, code);
            let output = run(
                tier,
                "remediation-qualification",
                &record_args(file.to_str().unwrap()),
                &server,
                Some(OPERATOR_PRINCIPAL),
            );
            let what = format!("{tier} remediation-qualification record ({code})");
            assert_eq!(output.status.code(), Some(exit), "{what}:\n{}", text(&output));
            let value = one_object(&output, &what);
            assert_error_object(&value, &what);
            assert!(value["error"].as_str().unwrap().contains(code), "{what}: {value}");
            assert!(!text(&output).contains(OPERATOR_PRINCIPAL), "{what}");
        }
    }
}
