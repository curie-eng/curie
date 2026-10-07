//! The remediation operator surface at the CLI boundary
//! (AUTOMATED-REMEDIATION-3, AUTOMATED-REMEDIATION-20).
//!
//! Two verb groups under BOTH `curie local` and `curie cluster`:
//!
//! `remediation-policy` (AUTOMATED-REMEDIATION-3), `<agent>` a name or id:
//!
//! - `show <agent> <hook>`
//!   -> `GET    /agents/{id}/hooks/{hook}/remediation-policy`
//! - `apply <agent> <hook> --file <path> --expected-generation <n> [--operation-id <uuid>]`
//!   -> `PUT    .../remediation-policy` body
//!   `{"expected_generation": "<n>", "operation_id": "<uuid>", "policy": <file>}`
//! - `arm|disarm <agent> <hook> --expected-generation <n> [--operation-id <uuid>]`
//!   -> `POST   .../remediation-policy/arm|disarm` body
//!   `{"expected_generation": "<n>", "operation_id": "<uuid>"}`
//! - `remove <agent> <hook> --expected-generation <n> [--operation-id <uuid>]`
//!   -> `DELETE .../remediation-policy?expected_generation=<n>&operation_id=<uuid>`
//! - `close-breaker <agent> <hook> <breaker id>`
//!   -> `POST   .../remediation-policy/breakers/{breaker id}/close`
//!
//! Every write sends the ADR 0106 operator principal from
//! `CURIE_APPROVAL_PRINCIPAL_TOKEN` in `X-Curie-Approval-Principal`, and is a
//! usage error (exit 2) before any request without one. `apply` validates the
//! document with the mirrored validator first and refuses with the API's code
//! and path (`tests/vectors/remediation-policy.json`) before any request.
//!
//! `remediation` (AUTOMATED-REMEDIATION-20): `list` and `show <nomination id>`,
//! the operator receipt. The API has no nomination read route yet, so only the
//! surface and the local id check are pinned here.
//!
//! These drive the compiled binary against a wire-level stub of the platform
//! API, as `actions_cli.rs` does. Every error is the ADR-0021 `{"error","fix"}`
//! object and every verb prints exactly one JSON object under `--json`.

mod support;

use std::path::Path;
use std::process::{Command, Output};

use serde_json::{json, Value};
use support::{serve, MockServer, Request, Response};

const TEST_API_KEY: &str = "test-platform-key";
const OPERATOR_PRINCIPAL: &str = "apr.test.operator-principal-that-must-not-leak";

const AGENT_ID: &str = "55555555-5555-5555-5555-555555555555";
const AGENT_NAME: &str = "acme-bot";
const HOOK: &str = "alerts";
/// A hook the stub has no policy for (`remediation_policy_absent`).
const ABSENT_HOOK: &str = "unbound";
/// A hook whose writes the stub refuses as stale.
const STALE_HOOK: &str = "stale";
/// A hook whose writes the stub refuses at the store (`route_unknown`, 422).
const ROUTE_HOOK: &str = "badroute";

const OPERATION_ID: &str = "11111111-1111-4111-8111-111111111111";
const BREAKER_ID: &str = "22222222-2222-4222-8222-222222222222";
const NOMINATION_ID: &str = "33333333-3333-4333-8333-333333333333";
const MALFORMED_ID: &str = "not-a-uuid";

fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_curie")
}

fn text(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).into_owned() + &String::from_utf8_lossy(&output.stderr)
}

fn policy_vector() -> Value {
    serde_json::from_str(include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../tests/vectors/remediation-policy.json"
    )))
    .expect("parse tests/vectors/remediation-policy.json")
}

fn base_document() -> Value {
    policy_vector()["valid"]
        .as_array()
        .expect("valid cases")
        .iter()
        .find(|c| c["name"] == "base_document")
        .expect("base_document case")["document"]
        .clone()
}

fn agent_json() -> String {
    format!(
        r#"{{"id":"{AGENT_ID}","name":"{AGENT_NAME}","channels":[],"created_at":"2026-10-01T00:00:00Z","memory":false}}"#
    )
}

/// One `RemediationPolicyOut` as
/// `apps/api/src/curie_api/schemas/remediation_policy.py` serializes it.
fn policy_out(hook: &str, generation: &str, armed: bool, active: bool) -> Value {
    json!({
        "agent_id": AGENT_ID,
        "hook": hook,
        "generation": generation,
        "armed": armed,
        "active": active,
        "bound_by": "operator@example.com",
        "policy": if active { base_document() } else { json!({}) },
        "updated_at": "2026-10-07T00:00:00Z",
    })
}

/// A refusal exactly as the policy routes send one.
fn refusal(status: u16, code: &str, path: Option<&str>, message: Option<&str>) -> Response {
    let mut detail = json!({ "code": code });
    if let Some(path) = path {
        detail["path"] = json!(path);
    }
    if let Some(message) = message {
        detail["message"] = json!(message);
    }
    Response::json(status, &json!({ "detail": detail }).to_string())
}

fn base(hook: &str) -> String {
    format!("/agents/{AGENT_ID}/hooks/{hook}/remediation-policy")
}

/// A stub of the platform API. Every request is recorded; anything unexpected
/// answers 405 so it surfaces as a failure.
fn api() -> MockServer {
    serve(|request: &Request| {
        let method = request.method.as_str();
        let (route, _query) = request.path.split_once('?').unwrap_or((&request.path, ""));
        let stale = |r: &str| r.starts_with(&base(STALE_HOOK));
        let bad_route = |r: &str| r.starts_with(&base(ROUTE_HOOK));
        match (method, route) {
            ("GET", "/agents") => Response::json(200, &format!("[{}]", agent_json())),
            ("GET", p) if p == format!("/agents/{AGENT_ID}") => Response::json(200, &agent_json()),
            ("GET", p) if p == base(HOOK) => {
                Response::json(200, &policy_out(HOOK, "4", true, true).to_string())
            }
            ("GET", p) if p == base(ABSENT_HOOK) => {
                refusal(404, "remediation_policy_absent", None, None)
            }
            (_, p) if stale(p) => refusal(
                409,
                "stale_policy_generation",
                None,
                Some("the current generation is 9"),
            ),
            (_, p) if bad_route(p) => refusal(
                422,
                "route_unknown",
                Some("/route"),
                Some("is not one of the agent's approval routes"),
            ),
            ("PUT", p) if p == base(HOOK) => {
                Response::json(200, &policy_out(HOOK, "5", true, true).to_string())
            }
            ("POST", p) if p == format!("{}/arm", base(HOOK)) => {
                Response::json(200, &policy_out(HOOK, "5", true, true).to_string())
            }
            ("POST", p) if p == format!("{}/disarm", base(HOOK)) => {
                Response::json(200, &policy_out(HOOK, "5", false, true).to_string())
            }
            ("DELETE", p) if p == base(HOOK) => {
                Response::json(200, &policy_out(HOOK, "5", false, false).to_string())
            }
            ("POST", p) if p == format!("{}/breakers/{BREAKER_ID}/close", base(HOOK)) => {
                Response::json(
                    200,
                    &json!({"breaker_id": BREAKER_ID, "state": "closed"}).to_string(),
                )
            }
            _ => Response::json(405, r#"{"detail":"unexpected request"}"#),
        }
    })
}

/// Run `curie <tier> remediation-policy <args..> --api-url .. --api-key ..`
/// with no ambient connection or principal from the developer's shell.
fn run_group(
    tier: &str,
    group: &str,
    args: &[&str],
    server: &MockServer,
    principal: Option<&str>,
    json_mode: bool,
) -> Output {
    let mut command = Command::new(bin());
    command.arg(tier).arg(group).args(args).args([
        "--api-url",
        &server.base_url,
        "--api-key",
        TEST_API_KEY,
    ]);
    if json_mode {
        command.arg("--json");
    }
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

fn run(tier: &str, args: &[&str], server: &MockServer, principal: Option<&str>) -> Output {
    run_group(tier, "remediation-policy", args, server, principal, true)
}

/// Stdout under `--json` is exactly one JSON object.
fn one_object(output: &Output, what: &str) -> Value {
    let stdout = String::from_utf8_lossy(&output.stdout);
    let mut values = serde_json::Deserializer::from_str(&stdout).into_iter::<Value>();
    let first = match values.next() {
        Some(Ok(value)) => value,
        Some(Err(err)) => panic!(
            "{what}: --json stdout must be one JSON object, got a parse error {err}\n{}",
            text(output)
        ),
        None => panic!(
            "{what}: --json stdout must be one JSON object, got nothing\n{}",
            text(output)
        ),
    };
    assert!(
        values.next().is_none(),
        "{what}: --json stdout must hold exactly one JSON value\n{stdout}"
    );
    assert!(
        first.is_object(),
        "{what}: --json stdout must be an object, not {first}"
    );
    first
}

fn assert_error_object(value: &Value, what: &str) {
    assert!(
        value["error"].as_str().is_some_and(|e| !e.is_empty()),
        "{what}: an error carries a non-empty `error`: {value}"
    );
    assert!(
        value.get("fix").is_some(),
        "{what}: an error carries a `fix` key: {value}"
    );
}

fn writes(server: &MockServer) -> Vec<Request> {
    server
        .recorded()
        .into_iter()
        .filter(|r| r.method != "GET")
        .collect()
}

fn write_file(dir: &Path, name: &str, contents: &str) -> String {
    let path = dir.join(name);
    std::fs::write(&path, contents).expect("write policy file");
    path.to_string_lossy().into_owned()
}

fn is_uuid(value: &str) -> bool {
    let parts: Vec<&str> = value.split('-').collect();
    parts.len() == 5
        && parts.iter().map(|p| p.len()).collect::<Vec<_>>() == [8, 4, 4, 4, 12]
        && parts
            .iter()
            .all(|p| p.chars().all(|c| c.is_ascii_hexdigit()))
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

// --------------------------------------------------------------------------
// Surface: both tiers expose both groups, and the manifest records them
// --------------------------------------------------------------------------

const POLICY_VERBS: [&str; 6] = ["show", "apply", "arm", "disarm", "remove", "close-breaker"];
const RECEIPT_VERBS: [&str; 2] = ["list", "show"];

// @spec AUTOMATED-REMEDIATION-3 @spec AUTOMATED-REMEDIATION-20
#[test]
fn local_and_cluster_expose_the_remediation_groups() {
    for tier in ["local", "cluster"] {
        let parent = Command::new(bin())
            .args([tier, "--help"])
            .output()
            .unwrap_or_else(|err| panic!("run {tier} --help: {err}"));
        let parent_help = text(&parent);
        for group in ["remediation-policy", "remediation"] {
            assert!(
                parent_help
                    .lines()
                    .any(|line| line.trim_start().starts_with(&format!("{group} "))),
                "{tier} --help must list the {group} group:\n{parent_help}"
            );
        }
        for (group, verbs) in [
            ("remediation-policy", &POLICY_VERBS[..]),
            ("remediation", &RECEIPT_VERBS[..]),
        ] {
            let output = Command::new(bin())
                .args([tier, group, "--help"])
                .output()
                .unwrap_or_else(|err| panic!("run {tier} {group} --help: {err}"));
            let help = text(&output);
            assert!(
                output.status.success(),
                "{tier} {group} --help must render:\n{help}"
            );
            for verb in verbs {
                assert!(
                    help.lines()
                        .any(|line| line.trim_start().starts_with(&format!("{verb} "))
                            || line.trim() == *verb),
                    "{tier} {group} must expose `{verb}`; help:\n{help}"
                );
            }
        }
    }
}

// @spec AUTOMATED-REMEDIATION-3 @spec AUTOMATED-REMEDIATION-20
/// The committed CLI manifest is regenerated with both groups under both tiers.
#[test]
fn committed_manifest_records_the_remediation_groups_under_both_tiers() {
    let manifest: Value = serde_json::from_str(include_str!("../command-manifest.json"))
        .expect("cli/command-manifest.json parses");
    let child = |node: &Value, name: &str| -> Option<Value> {
        node["subcommands"]
            .as_array()?
            .iter()
            .find(|c| c["name"] == name)
            .cloned()
    };
    for tier in ["local", "cluster"] {
        let tier_node = child(&manifest, tier).unwrap_or_else(|| panic!("manifest has {tier}"));
        for (group, verbs) in [
            ("remediation-policy", &POLICY_VERBS[..]),
            ("remediation", &RECEIPT_VERBS[..]),
        ] {
            let node = child(&tier_node, group)
                .unwrap_or_else(|| panic!("manifest must record `{tier} {group}`"));
            for verb in verbs {
                assert!(
                    child(&node, verb).is_some(),
                    "manifest must record `{tier} {group} {verb}`: {node}"
                );
            }
        }
    }
}

// --------------------------------------------------------------------------
// show
// --------------------------------------------------------------------------

// @spec AUTOMATED-REMEDIATION-3
#[test]
fn show_reads_the_current_generation_without_a_principal() {
    for tier in ["local", "cluster"] {
        for agent in [AGENT_NAME, AGENT_ID] {
            let server = api();
            let output = run(tier, &["show", agent, HOOK], &server, None);
            let what = format!("{tier} remediation-policy show {agent}");
            assert_eq!(output.status.code(), Some(0), "{what}:\n{}", text(&output));
            let value = one_object(&output, &what);
            let policy = value
                .get("policy")
                .filter(|p| p.is_object() && p.get("route").is_none());
            let row = if policy.is_some() {
                &value["policy"]
            } else {
                &value
            };
            assert_eq!(row["generation"], "4", "{what}: {value}");
            assert_eq!(row["armed"], true, "{what}: {value}");
            assert_eq!(row["active"], true, "{what}: {value}");
            assert_eq!(row["bound_by"], "operator@example.com", "{what}: {value}");
            let reads: Vec<Request> = server
                .recorded()
                .into_iter()
                .filter(|r| r.path.starts_with(&base(HOOK)))
                .collect();
            assert_eq!(reads.len(), 1, "{what}: one policy read");
            assert_eq!(reads[0].method, "GET");
            assert_eq!(reads[0].header("X-Api-Key"), Some(TEST_API_KEY));
            assert!(writes(&server).is_empty(), "{what}: a read writes nothing");
        }
    }
}

// @spec AUTOMATED-REMEDIATION-3
#[test]
fn show_of_an_unbound_hook_is_one_error_object_naming_the_code() {
    for tier in ["local", "cluster"] {
        let server = api();
        let output = run(tier, &["show", AGENT_NAME, ABSENT_HOOK], &server, None);
        let what = format!("{tier} remediation-policy show (absent)");
        assert_eq!(output.status.code(), Some(1), "{what}:\n{}", text(&output));
        let value = one_object(&output, &what);
        assert_error_object(&value, &what);
        assert!(
            value["error"]
                .as_str()
                .unwrap()
                .contains("remediation_policy_absent"),
            "{what}: the error names the API's code: {value}"
        );
    }
}

// --------------------------------------------------------------------------
// writes: request shapes and the principal
// --------------------------------------------------------------------------

// @spec AUTOMATED-REMEDIATION-3
#[test]
fn apply_puts_the_document_unchanged_with_cas_and_the_principal() {
    for tier in ["local", "cluster"] {
        let dir = tempfile::tempdir().expect("tempdir");
        let file = write_file(dir.path(), "policy.json", &base_document().to_string());
        let server = api();
        let output = run(
            tier,
            &[
                "apply",
                AGENT_NAME,
                HOOK,
                "--file",
                &file,
                "--expected-generation",
                "4",
                "--operation-id",
                OPERATION_ID,
            ],
            &server,
            Some(OPERATOR_PRINCIPAL),
        );
        let what = format!("{tier} remediation-policy apply");
        assert_eq!(output.status.code(), Some(0), "{what}:\n{}", text(&output));
        let value = one_object(&output, &what);
        assert_eq!(value["generation"], "5", "{what}: {value}");

        let sent = writes(&server);
        assert_eq!(sent.len(), 1, "{what}: one write");
        let request = &sent[0];
        assert_eq!(request.method, "PUT");
        assert_eq!(request.path, base(HOOK));
        assert_eq!(
            request.header("X-Curie-Approval-Principal"),
            Some(OPERATOR_PRINCIPAL)
        );
        assert_eq!(request.header("X-Api-Key"), Some(TEST_API_KEY));
        let body: Value = serde_json::from_slice(&request.body).expect("PUT body is JSON");
        assert_eq!(
            body,
            json!({
                "expected_generation": "4",
                "operation_id": OPERATION_ID,
                "policy": base_document(),
            }),
            "{what}: the body is the CAS pair and the document unchanged"
        );
        assert!(
            !text(&output).contains(OPERATOR_PRINCIPAL),
            "{what}: token never printed"
        );
    }
}

// @spec AUTOMATED-REMEDIATION-3
#[test]
fn arm_disarm_and_remove_send_cas_and_the_principal() {
    for tier in ["local", "cluster"] {
        for (verb, method, suffix, armed) in [
            ("arm", "POST", "/arm", true),
            ("disarm", "POST", "/disarm", false),
            ("remove", "DELETE", "", false),
        ] {
            let server = api();
            let output = run(
                tier,
                &[
                    verb,
                    AGENT_NAME,
                    HOOK,
                    "--expected-generation",
                    "4",
                    "--operation-id",
                    OPERATION_ID,
                ],
                &server,
                Some(OPERATOR_PRINCIPAL),
            );
            let what = format!("{tier} remediation-policy {verb}");
            assert_eq!(output.status.code(), Some(0), "{what}:\n{}", text(&output));
            let value = one_object(&output, &what);
            assert_eq!(value["generation"], "5", "{what}: {value}");
            assert_eq!(value["armed"], armed, "{what}: {value}");

            let sent = writes(&server);
            assert_eq!(sent.len(), 1, "{what}: one write");
            let request = &sent[0];
            assert_eq!(request.method, method, "{what}");
            let (route, _) = request.path.split_once('?').unwrap_or((&request.path, ""));
            assert_eq!(route, format!("{}{suffix}", base(HOOK)), "{what}");
            assert_eq!(
                request.header("X-Curie-Approval-Principal"),
                Some(OPERATOR_PRINCIPAL),
                "{what}"
            );
            if method == "DELETE" {
                let mut pairs = query_pairs(&request.path);
                pairs.sort();
                assert_eq!(
                    pairs,
                    vec![
                        ("expected_generation".to_owned(), "4".to_owned()),
                        ("operation_id".to_owned(), OPERATION_ID.to_owned()),
                    ],
                    "{what}: removal carries CAS in the query, as the route reads it"
                );
            } else {
                let body: Value = serde_json::from_slice(&request.body).expect("body is JSON");
                assert_eq!(
                    body,
                    json!({"expected_generation": "4", "operation_id": OPERATION_ID}),
                    "{what}"
                );
            }
            assert!(
                !text(&output).contains(OPERATOR_PRINCIPAL),
                "{what}: token never printed"
            );
        }
    }
}

// @spec AUTOMATED-REMEDIATION-2 @spec AUTOMATED-REMEDIATION-3
/// Without `--operation-id` each write mints a fresh UUID, so a retry the
/// operator issues by hand is a new intent unless they pass the id back.
#[test]
fn a_write_without_an_operation_id_mints_a_uuid() {
    let server = api();
    let output = run(
        "local",
        &["arm", AGENT_NAME, HOOK, "--expected-generation", "4"],
        &server,
        Some(OPERATOR_PRINCIPAL),
    );
    assert_eq!(output.status.code(), Some(0), "{}", text(&output));
    let sent = writes(&server);
    assert_eq!(sent.len(), 1);
    let body: Value = serde_json::from_slice(&sent[0].body).expect("body is JSON");
    let id = body["operation_id"].as_str().unwrap_or_default();
    assert!(is_uuid(id), "a minted operation_id is a UUID: {body}");
}

// @spec AUTOMATED-REMEDIATION-3
#[test]
fn close_breaker_posts_with_the_principal() {
    for tier in ["local", "cluster"] {
        let server = api();
        let output = run(
            tier,
            &["close-breaker", AGENT_NAME, HOOK, BREAKER_ID],
            &server,
            Some(OPERATOR_PRINCIPAL),
        );
        let what = format!("{tier} remediation-policy close-breaker");
        assert_eq!(output.status.code(), Some(0), "{what}:\n{}", text(&output));
        one_object(&output, &what);
        let sent = writes(&server);
        assert_eq!(sent.len(), 1, "{what}: one write");
        assert_eq!(sent[0].method, "POST");
        assert_eq!(
            sent[0].path,
            format!("{}/breakers/{BREAKER_ID}/close", base(HOOK))
        );
        assert_eq!(
            sent[0].header("X-Curie-Approval-Principal"),
            Some(OPERATOR_PRINCIPAL),
            "{what}"
        );
    }
}

fn write_argv<'a>(verb: &'a str, file: &'a str) -> Vec<&'a str> {
    match verb {
        "apply" => vec![
            "apply",
            AGENT_NAME,
            HOOK,
            "--file",
            file,
            "--expected-generation",
            "4",
        ],
        "close-breaker" => vec!["close-breaker", AGENT_NAME, HOOK, BREAKER_ID],
        _ => vec![verb, AGENT_NAME, HOOK, "--expected-generation", "4"],
    }
}

// @spec AUTOMATED-REMEDIATION-3
/// Every write without a principal (absent or blank) is a usage error naming
/// the env var, raised before any request, the agent lookup included.
#[test]
fn every_write_without_a_principal_is_refused_before_any_request() {
    let dir = tempfile::tempdir().expect("tempdir");
    let file = write_file(dir.path(), "policy.json", &base_document().to_string());
    for tier in ["local", "cluster"] {
        for verb in ["apply", "arm", "disarm", "remove", "close-breaker"] {
            for principal in [None, Some("   ")] {
                let server = api();
                let output = run(tier, &write_argv(verb, &file), &server, principal);
                let what = format!("{tier} remediation-policy {verb} principal={principal:?}");
                assert_eq!(output.status.code(), Some(2), "{what}:\n{}", text(&output));
                let value = one_object(&output, &what);
                assert_error_object(&value, &what);
                assert!(
                    value.to_string().contains("CURIE_APPROVAL_PRINCIPAL_TOKEN"),
                    "{what}: the error names the env-backed credential: {value}"
                );
                assert!(
                    server.recorded().is_empty(),
                    "{what}: no request is sent: {:?}",
                    server
                        .recorded()
                        .iter()
                        .map(|r| &r.path)
                        .collect::<Vec<_>>()
                );
            }
        }
    }
}

// @spec AUTOMATED-REMEDIATION-3
/// API refusals are one error object carrying the API's code, with the
/// ADR-0021 class: 409 and 404 fail (1), a 422 store refusal is usage (2).
#[test]
fn api_refusals_emit_one_error_object_with_the_api_code() {
    let dir = tempfile::tempdir().expect("tempdir");
    let file = write_file(dir.path(), "policy.json", &base_document().to_string());
    for tier in ["local", "cluster"] {
        for (hook, verb, exit, code) in [
            (STALE_HOOK, "arm", 1, "stale_policy_generation"),
            (STALE_HOOK, "apply", 1, "stale_policy_generation"),
            (ROUTE_HOOK, "apply", 2, "route_unknown"),
        ] {
            let server = api();
            let mut argv = write_argv(verb, &file);
            argv[2] = hook;
            let output = run(tier, &argv, &server, Some(OPERATOR_PRINCIPAL));
            let what = format!("{tier} remediation-policy {verb} ({code})");
            assert_eq!(
                output.status.code(),
                Some(exit),
                "{what}:\n{}",
                text(&output)
            );
            let value = one_object(&output, &what);
            assert_error_object(&value, &what);
            assert!(
                value["error"].as_str().unwrap().contains(code),
                "{what}: the error names the API's code: {value}"
            );
            assert!(
                !text(&output).contains(OPERATOR_PRINCIPAL),
                "{what}: token never printed"
            );
        }
    }
}

// --------------------------------------------------------------------------
// apply: mirrored validation refuses with the API's reason, before any request
// --------------------------------------------------------------------------

// @spec AUTOMATED-REMEDIATION-3
/// Every invalid document of the frozen vector is refused by `apply` before any
/// request, exit 2, with the API's code and path in the one error object.
#[test]
fn apply_refuses_each_invalid_vector_document_with_the_api_code_before_any_request() {
    let vector = policy_vector();
    let dir = tempfile::tempdir().expect("tempdir");
    for case in vector["invalid"].as_array().expect("invalid cases") {
        let name = case["name"].as_str().unwrap();
        let code = case["code"].as_str().unwrap();
        let path = case["path"].as_str().unwrap();
        let file = write_file(
            dir.path(),
            &format!("{name}.json"),
            &case["document"].to_string(),
        );
        let server = api();
        let output = run(
            "local",
            &write_argv("apply", &file),
            &server,
            Some(OPERATOR_PRINCIPAL),
        );
        let what = format!("local remediation-policy apply ({name})");
        assert_eq!(output.status.code(), Some(2), "{what}:\n{}", text(&output));
        let value = one_object(&output, &what);
        assert_error_object(&value, &what);
        let error = value["error"].as_str().unwrap();
        assert!(
            error.contains(code),
            "{what}: the error names {code}: {value}"
        );
        if !path.is_empty() {
            assert!(
                error.contains(path),
                "{what}: the error names {path}: {value}"
            );
        }
        assert!(
            server.recorded().is_empty(),
            "{what}: refused before any request"
        );
    }
}

// @spec AUTOMATED-REMEDIATION-2 @spec AUTOMATED-REMEDIATION-3
#[test]
fn apply_refuses_a_non_finite_policy_text_before_any_request() {
    let vector = policy_vector();
    let dir = tempfile::tempdir().expect("tempdir");
    for case in vector["invalid_texts"].as_array().expect("invalid texts") {
        let name = case["name"].as_str().unwrap();
        let code = case["code"].as_str().unwrap();
        let file = write_file(
            dir.path(),
            &format!("{name}.json"),
            case["text"].as_str().unwrap(),
        );
        let server = api();
        let output = run(
            "local",
            &write_argv("apply", &file),
            &server,
            Some(OPERATOR_PRINCIPAL),
        );
        let what = format!("local remediation-policy apply ({name})");
        assert_eq!(output.status.code(), Some(2), "{what}:\n{}", text(&output));
        let value = one_object(&output, &what);
        assert_error_object(&value, &what);
        assert!(
            value["error"].as_str().unwrap().contains(code),
            "{what}: {value}"
        );
        assert!(
            server.recorded().is_empty(),
            "{what}: refused before any request"
        );
    }
}

// @spec AUTOMATED-REMEDIATION-3
/// The spec's named acceptance case at both tiers: an over-ceiling limit is
/// refused with the API's reason, before any request.
#[test]
fn both_tiers_refuse_an_over_ceiling_limit_with_the_api_reason() {
    let vector = policy_vector();
    let case = vector["invalid"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["name"] == "per_policy_per_hour_above_ceiling")
        .expect("over-ceiling case");
    let dir = tempfile::tempdir().expect("tempdir");
    let file = write_file(dir.path(), "over.json", &case["document"].to_string());
    for tier in ["local", "cluster"] {
        let server = api();
        let output = run(
            tier,
            &write_argv("apply", &file),
            &server,
            Some(OPERATOR_PRINCIPAL),
        );
        let what = format!("{tier} remediation-policy apply (over ceiling)");
        assert_eq!(output.status.code(), Some(2), "{what}:\n{}", text(&output));
        let value = one_object(&output, &what);
        assert_error_object(&value, &what);
        let error = value["error"].as_str().unwrap();
        assert!(
            error.contains("policy_limit_out_of_bounds")
                && error.contains("/limits/per_policy_per_hour"),
            "{what}: {value}"
        );
        assert!(
            server.recorded().is_empty(),
            "{what}: refused before any request"
        );
    }
}

// @spec AUTOMATED-REMEDIATION-3
#[test]
fn apply_of_an_unreadable_file_is_one_usage_error() {
    let server = api();
    let output = run(
        "local",
        &write_argv("apply", "/nonexistent/curie-remediation-policy.json"),
        &server,
        Some(OPERATOR_PRINCIPAL),
    );
    assert_eq!(output.status.code(), Some(2), "{}", text(&output));
    let value = one_object(&output, "apply missing file");
    assert_error_object(&value, "apply missing file");
    assert!(server.recorded().is_empty());
}

// --------------------------------------------------------------------------
// cluster tier: input errors before connection discovery
// --------------------------------------------------------------------------

fn run_cluster_undiscovered(args: &[&str], principal: Option<&str>) -> Output {
    let dir = tempfile::tempdir().expect("tempdir");
    let empty_path = dir.path().join("empty-path");
    std::fs::create_dir_all(&empty_path).expect("create empty PATH");
    let mut command = Command::new(bin());
    command
        .args(["cluster", "remediation-policy"])
        .args(args)
        .arg("--json")
        .current_dir(dir.path())
        .env_clear()
        .env("HOME", dir.path())
        .env("PATH", &empty_path)
        .env("KUBECONFIG", dir.path().join("no-kubeconfig"))
        .env("NO_COLOR", "1")
        .env("CI", "1");
    if let Some(principal) = principal {
        command.env("CURIE_APPROVAL_PRINCIPAL_TOKEN", principal);
    }
    command
        .output()
        .unwrap_or_else(|err| panic!("run curie cluster remediation-policy: {err}"))
}

fn assert_no_connection_discovery(output: &Output, what: &str) {
    let shown = text(output);
    for marker in ["Helm", "helm", "kubectl", "port-forward", "kube context"] {
        assert!(
            !shown.contains(marker),
            "{what}: the input error must be raised before connection discovery, \
             but the output mentions {marker:?}:\n{shown}"
        );
    }
}

// @spec AUTOMATED-REMEDIATION-3
#[test]
fn cluster_tier_refuses_input_errors_before_discovering_the_connection() {
    let vector = policy_vector();
    let case = vector["invalid"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["name"] == "per_policy_per_hour_above_ceiling")
        .unwrap();
    let dir = tempfile::tempdir().expect("tempdir");
    let bad = write_file(dir.path(), "over.json", &case["document"].to_string());
    let good = write_file(dir.path(), "good.json", &base_document().to_string());

    let refused_document =
        run_cluster_undiscovered(&write_argv("apply", &bad), Some(OPERATOR_PRINCIPAL));
    let no_principal = run_cluster_undiscovered(&write_argv("apply", &good), None);
    for (output, what, needle) in [
        (
            &refused_document,
            "invalid document",
            "policy_limit_out_of_bounds",
        ),
        (
            &no_principal,
            "no principal",
            "CURIE_APPROVAL_PRINCIPAL_TOKEN",
        ),
    ] {
        let what = format!("cluster remediation-policy apply ({what}, no --api-url)");
        assert_eq!(output.status.code(), Some(2), "{what}:\n{}", text(output));
        let value = one_object(output, &what);
        assert_error_object(&value, &what);
        assert!(value.to_string().contains(needle), "{what}: {value}");
        assert_no_connection_discovery(output, &what);
    }
}

// --------------------------------------------------------------------------
// remediation: the operator receipt (AUTOMATED-REMEDIATION-20)
// --------------------------------------------------------------------------

// @spec AUTOMATED-REMEDIATION-20
/// A malformed nomination id is a usage error naming the id, one error object,
/// before any request, at both tiers.
#[test]
fn remediation_show_refuses_a_malformed_nomination_id_before_any_request() {
    for tier in ["local", "cluster"] {
        let server = api();
        let output = run_group(
            tier,
            "remediation",
            &["show", MALFORMED_ID],
            &server,
            None,
            true,
        );
        let what = format!("{tier} remediation show {MALFORMED_ID}");
        assert_eq!(output.status.code(), Some(2), "{what}:\n{}", text(&output));
        let value = one_object(&output, &what);
        assert_error_object(&value, &what);
        assert!(
            value["error"].as_str().unwrap().contains(MALFORMED_ID),
            "{what}: {value}"
        );
        assert!(server.recorded().is_empty(), "{what}: no request");
    }
    // A well-formed id parses (the route behind it is not pinned here).
    let server = api();
    let output = run_group(
        "local",
        "remediation",
        &["show", NOMINATION_ID],
        &server,
        None,
        true,
    );
    one_object(&output, "local remediation show <uuid>");
}
