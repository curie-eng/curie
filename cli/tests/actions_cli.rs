//! The `actions` operator surface at the CLI boundary (ACTION-EXECUTOR-23).
//!
//! A new verb group under BOTH `curie local` and `curie cluster`:
//!
//! - `actions list [--agent <name|id>]` -> `GET  /actions?agent_id=<uuid>`
//! - `actions show <id>`                -> `GET  /actions/{id}`
//! - `actions undo <id>`                -> `POST /actions/{id}/undo`
//!   (ADR-0106 principal in `X-Curie-Approval-Principal`, body `{}`)
//! - `actions execution <id>`           -> `GET  /action-executions/{id}`
//!   (platform key; the receipt of ACTION-EXECUTOR-18)
//!
//! These drive the compiled binary against a wire-level stub of the platform
//! API, as `approval_principal.rs` and `callers_verb.rs` do: a unit test over a
//! constructed command value cannot catch clap wiring, a dropped principal
//! header, or a verb that prints nothing under `--json`.
//!
//! JSON shapes these tests pin (the handler's typed `CliOutput`s):
//!
//! - `list`      -> `{"actions": [ {.., "id", "undoable"}, .. ]}`
//! - `show`      -> an object carrying `undoable` (top level or under `action`)
//! - `undo`      -> `{"execution_id", "state", ..}` and nothing of the record's
//!   sealed snapshot
//! - `execution` -> `{"execution_id", "state", "code", ..}`, where `code` is the
//!   row's `refusal_code` or `failure_code` (null when confirmed)
//! - any error   -> the ADR-0021 `{"error", "fix"}` object

mod support;

use std::process::{Command, Output};

use serde_json::{json, Value};
use support::{serve, MockServer, Request, Response};

const TEST_API_KEY: &str = "test-platform-key";
const OPERATOR_PRINCIPAL: &str = "apr.test.operator-principal-that-must-not-leak";

const AGENT_ID: &str = "55555555-5555-5555-5555-555555555555";
const AGENT_NAME: &str = "acme-bot";

/// An undoable action whose undo the stub grants.
const ACTION_ID: &str = "11111111-1111-1111-1111-111111111111";
/// A second, not undoable action that only appears in the list.
const PLAIN_ACTION_ID: &str = "22222222-2222-2222-2222-222222222222";
/// An action whose undo the API refuses with 409 (restore in flight).
const IN_FLIGHT_ACTION_ID: &str = "33333333-3333-3333-3333-333333333333";
/// An action whose undo the API refuses with 403 (unauthorized).
const FORBIDDEN_ACTION_ID: &str = "44444444-4444-4444-4444-444444444444";
/// An id the API answers 404 for on every route.
const MISSING_ID: &str = "99999999-9999-9999-9999-999999999999";
/// Not a UUID: the CLI may refuse it itself or pass the API's 422 through;
/// both are an ADR-0021 usage error.
const MALFORMED_ID: &str = "not-a-uuid";
/// An action whose undo the API refuses with 503 because the executor is off.
const EXECUTOR_OFF_ACTION_ID: &str = "88888888-8888-8888-8888-888888888888";

/// The execution the granted undo creates.
const EXECUTION_ID: &str = "66666666-6666-6666-6666-666666666666";
const CONFIRMED_EXECUTION_ID: &str = "77777777-7777-7777-7777-777777777771";
const FAILED_EXECUTION_ID: &str = "77777777-7777-7777-7777-777777777772";
const INDETERMINATE_EXECUTION_ID: &str = "77777777-7777-7777-7777-777777777773";
const REFUSED_EXECUTION_ID: &str = "77777777-7777-7777-7777-777777777774";

/// Stand-in for sealed snapshot material. It rides on the action row (as the
/// API's `prior_state` envelope does) AND in an unexpected field of the undo
/// response, so an `undo` that prints either is caught.
const SNAPSHOT_MARKER: &str = "sealed-envelope-ciphertext-must-never-print";

const IN_FLIGHT_REASON: &str = "a restore of this action already exists";
const UNAUTHORIZED_REASON: &str = "not authorized to undo this action";
/// `_EXECUTOR_DISABLED_REASON` in `apps/api/src/curie_api/routers/actions.py`.
const EXECUTOR_DISABLED_REASON: &str = "the action executor is not enabled on this installation";

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

/// One `ActionOut` row exactly as `apps/api/src/curie_api/schemas/actions.py`
/// serializes it, carrying the sealed `prior_state` envelope.
fn action_json(id: &str, undoable: bool) -> Value {
    json!({
        "id": id,
        "agent_id": AGENT_ID,
        "conversation_id": "thread-1",
        "call_id": "call-1",
        "tool": "scale_deployment",
        "arguments": {"name": "web", "replicas": 3},
        "result": {"ok": true},
        "prior_state": {"kid": "kid-1", "ciphertext": SNAPSHOT_MARKER},
        "post_state": {"sealed": SNAPSHOT_MARKER},
        "target": {"kind": "deployment", "name": "web"},
        "detail": null,
        "gate_approval_id": null,
        "status": "succeeded",
        "dedupe_key": format!("event-1:{id}"),
        "created_at": "2026-10-01T00:00:00Z",
        "completed_at": "2026-10-01T00:00:01Z",
        "undone_at": null,
        "undone_by": null,
        "undoable": undoable,
    })
}

/// One `ExecutionOut` row exactly as
/// `apps/api/src/curie_api/schemas/action_executions.py` serializes it.
fn execution_json(
    id: &str,
    state: &str,
    refusal_code: Option<&str>,
    failure_code: Option<&str>,
) -> Value {
    json!({
        "id": id,
        "kind": "restore",
        "state": state,
        "agent_id": AGENT_ID,
        "connector": "reference-reversible",
        "tool": "restore",
        "subject_action_id": ACTION_ID,
        "requested_by": "U0OPERATOR",
        "attempt": 1,
        "lease_owner": null,
        "lease_expires_at": null,
        "refusal_code": refusal_code,
        "failure_code": failure_code,
        "dispatched_at": null,
        "finished_at": "2026-10-01T00:05:00Z",
        "created_at": "2026-10-01T00:04:00Z",
    })
}

/// The terminal receipts: (execution id, state, the code the receipt must name).
fn terminal_receipts() -> Vec<(&'static str, &'static str, Option<&'static str>)> {
    vec![
        (CONFIRMED_EXECUTION_ID, "confirmed", None),
        (
            FAILED_EXECUTION_ID,
            "failed",
            Some("version_conflict_at_write"),
        ),
        (
            INDETERMINATE_EXECUTION_ID,
            "indeterminate",
            Some("response_lost"),
        ),
        (
            REFUSED_EXECUTION_ID,
            "refused",
            Some("connector_digest_unavailable"),
        ),
    ]
}

fn detail(status: u16, reason: &str) -> Response {
    Response::json(status, &json!({ "detail": reason }).to_string())
}

/// A stub of the platform API holding the fixtures above. Every request is
/// recorded; anything unexpected answers 405 so it surfaces as a failure.
fn api() -> MockServer {
    serve(|request: &Request| {
        let method = request.method.as_str();
        let path = request.path.as_str();
        let (route, _query) = path.split_once('?').unwrap_or((path, ""));
        match (method, route) {
            ("GET", "/agents") => Response::json(200, &format!("[{}]", agent_json())),
            ("GET", p) if p == format!("/agents/{AGENT_ID}") => Response::json(200, &agent_json()),
            ("GET", "/actions") => Response::json(
                200,
                &json!([
                    action_json(ACTION_ID, true),
                    action_json(PLAIN_ACTION_ID, false)
                ])
                .to_string(),
            ),
            ("GET", p) if p == format!("/actions/{ACTION_ID}") => {
                Response::json(200, &action_json(ACTION_ID, true).to_string())
            }
            ("GET", p) if p == format!("/actions/{PLAIN_ACTION_ID}") => {
                Response::json(200, &action_json(PLAIN_ACTION_ID, false).to_string())
            }
            ("GET", p) if p == format!("/actions/{MISSING_ID}") => detail(404, "action not found"),
            ("POST", p) if p == format!("/actions/{ACTION_ID}/undo") => Response::json(
                202,
                // `ActionUndoOut` plus a field the API never sends: snapshot
                // material in an unexpected place must still not be printed.
                &json!({
                    "execution_id": EXECUTION_ID,
                    "state": "requested",
                    "prior_state": {"kid": "kid-1", "ciphertext": SNAPSHOT_MARKER},
                })
                .to_string(),
            ),
            ("POST", p) if p == format!("/actions/{IN_FLIGHT_ACTION_ID}/undo") => {
                detail(409, IN_FLIGHT_REASON)
            }
            ("POST", p) if p == format!("/actions/{FORBIDDEN_ACTION_ID}/undo") => {
                detail(403, UNAUTHORIZED_REASON)
            }
            ("POST", p) if p == format!("/actions/{EXECUTOR_OFF_ACTION_ID}/undo") => {
                detail(503, EXECUTOR_DISABLED_REASON)
            }
            ("POST", p) if p == format!("/actions/{MISSING_ID}/undo") => {
                detail(404, "action not found")
            }
            ("POST", p) if p == format!("/actions/{MALFORMED_ID}/undo") => Response::json(
                422,
                r#"{"detail":[{"type":"uuid_parsing","loc":["path","action_id"],"msg":"Input should be a valid UUID","input":"not-a-uuid"}]}"#,
            ),
            ("GET", p) if p == format!("/action-executions/{MISSING_ID}") => {
                detail(404, "action execution not found")
            }
            ("GET", p) if p == format!("/action-executions/{EXECUTION_ID}") => Response::json(
                200,
                &execution_json(EXECUTION_ID, "requested", None, None).to_string(),
            ),
            ("GET", p) if p.starts_with("/action-executions/") => {
                let id = &p["/action-executions/".len()..];
                match terminal_receipts()
                    .into_iter()
                    .find(|(eid, _, _)| *eid == id)
                {
                    Some((eid, state, code)) => {
                        let (refusal, failure) = if state == "refused" {
                            (code, None)
                        } else {
                            (None, code)
                        };
                        Response::json(
                            200,
                            &execution_json(eid, state, refusal, failure).to_string(),
                        )
                    }
                    None => detail(404, "action execution not found"),
                }
            }
            _ => detail(405, "unexpected request"),
        }
    })
}

/// Run `curie <tier> actions <args..> --api-url .. --api-key ..` with no
/// ambient connection or principal inherited from the developer's shell.
fn run(
    tier: &str,
    args: &[&str],
    server: &MockServer,
    principal: Option<&str>,
    json_mode: bool,
) -> Output {
    let mut command = Command::new(bin());
    command.arg(tier).arg("actions").args(args).args([
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
        .unwrap_or_else(|err| panic!("run curie {tier} actions {}: {err}", args.join(" ")))
}

/// Stdout under `--json` is exactly one JSON object: not empty, not an array,
/// not an object followed by a second value.
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

/// An ADR-0021 error object: `error` is a non-empty message and `fix` is
/// present (a hint or null).
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

fn contains_key(value: &Value, key: &str) -> bool {
    match value {
        Value::Object(map) => map.iter().any(|(k, v)| k == key || contains_key(v, key)),
        Value::Array(items) => items.iter().any(|v| contains_key(v, key)),
        _ => false,
    }
}

fn undo_requests(server: &MockServer) -> Vec<Request> {
    server
        .recorded()
        .into_iter()
        .filter(|r| r.method == "POST" && r.path.ends_with("/undo"))
        .collect()
}

// --------------------------------------------------------------------------
// Surface: both parents expose the group, and the manifest records it
// --------------------------------------------------------------------------

// @spec ACTION-EXECUTOR-23
/// Both tiers expose the same `actions` group with the same four verbs; a verb
/// that exists only at `cluster` would make the local rehearsal impossible.
#[test]
fn local_and_cluster_expose_the_actions_group_with_four_verbs() {
    for tier in ["local", "cluster"] {
        let parent = Command::new(bin())
            .args([tier, "--help"])
            .output()
            .unwrap_or_else(|err| panic!("run {tier} --help: {err}"));
        let parent_help = text(&parent);
        assert!(
            parent_help
                .lines()
                .any(|line| line.trim_start().starts_with("actions ")),
            "{tier} --help must list the actions group:\n{parent_help}"
        );

        let group = Command::new(bin())
            .args([tier, "actions", "--help"])
            .output()
            .unwrap_or_else(|err| panic!("run {tier} actions --help: {err}"));
        let help = text(&group);
        assert!(
            group.status.success(),
            "{tier} actions --help must render:\n{help}"
        );
        for verb in ["list", "show", "undo", "execution"] {
            assert!(
                help.lines()
                    .any(|line| line.trim_start().starts_with(&format!("{verb} "))
                        || line.trim() == verb),
                "{tier} actions must expose `{verb}`; help:\n{help}"
            );
        }
    }
}

// @spec ACTION-EXECUTOR-23
/// The committed CLI manifest is regenerated with the group. Together with the
/// existing drift gate (`command_manifest_matches_committed_artifact`) this pins
/// that the live grammar and the committed artifact both carry it.
#[test]
fn committed_manifest_records_actions_under_both_tiers() {
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
        let actions = child(&tier_node, "actions")
            .unwrap_or_else(|| panic!("manifest must record `{tier} actions`"));
        for verb in ["list", "show", "undo", "execution"] {
            assert!(
                child(&actions, verb).is_some(),
                "manifest must record `{tier} actions {verb}`: {actions}"
            );
        }
    }
}

// --------------------------------------------------------------------------
// list / show: GET /actions and GET /actions/{id}, surfacing `undoable`
// --------------------------------------------------------------------------

// @spec ACTION-EXECUTOR-23
#[test]
fn list_filters_by_agent_and_surfaces_undoable_at_both_tiers() {
    for tier in ["local", "cluster"] {
        let server = api();
        let output = run(tier, &["list", "--agent", AGENT_NAME], &server, None, true);
        assert_eq!(
            output.status.code(),
            Some(0),
            "{tier} actions list must succeed:\n{}",
            text(&output)
        );
        let value = one_object(&output, &format!("{tier} actions list"));
        let actions = value["actions"]
            .as_array()
            .unwrap_or_else(|| panic!("{tier} list object carries an `actions` array: {value}"));
        let undoable: Vec<(String, bool)> = actions
            .iter()
            .map(|a| {
                (
                    a["id"].as_str().unwrap_or_default().to_string(),
                    a["undoable"]
                        .as_bool()
                        .unwrap_or_else(|| panic!("each listed action carries `undoable`: {a}")),
                )
            })
            .collect();
        assert_eq!(
            undoable,
            vec![
                (ACTION_ID.to_string(), true),
                (PLAIN_ACTION_ID.to_string(), false)
            ],
            "{tier} list reports each action's derived undoable as the API read it"
        );

        let listed: Vec<Request> = server
            .recorded()
            .into_iter()
            .filter(|r| {
                r.method == "GET" && (r.path == "/actions" || r.path.starts_with("/actions?"))
            })
            .collect();
        assert_eq!(listed.len(), 1, "{tier} list reads GET /actions once");
        assert!(
            listed[0].path.contains(&format!("agent_id={AGENT_ID}")),
            "{tier} list --agent filters by the resolved agent id: {}",
            listed[0].path
        );
        assert_eq!(
            listed[0].header("X-Api-Key"),
            Some(TEST_API_KEY),
            "{tier} list reads with the platform key"
        );
    }
}

// @spec ACTION-EXECUTOR-23
#[test]
fn list_human_output_names_undoable() {
    let server = api();
    let output = run(
        "local",
        &["list", "--agent", AGENT_NAME],
        &server,
        None,
        false,
    );
    assert_eq!(output.status.code(), Some(0), "{}", text(&output));
    let shown = text(&output);
    assert!(
        shown.contains(ACTION_ID) && shown.contains(PLAIN_ACTION_ID),
        "list names each action: {shown}"
    );
    assert!(
        shown.to_lowercase().contains("undoable"),
        "list surfaces undoable to a human: {shown}"
    );
}

// @spec ACTION-EXECUTOR-23
#[test]
fn show_reads_one_action_and_surfaces_undoable_at_both_tiers() {
    for tier in ["local", "cluster"] {
        for (id, expected) in [(ACTION_ID, true), (PLAIN_ACTION_ID, false)] {
            let server = api();
            let output = run(tier, &["show", id], &server, None, true);
            assert_eq!(
                output.status.code(),
                Some(0),
                "{tier} actions show must succeed:\n{}",
                text(&output)
            );
            let value = one_object(&output, &format!("{tier} actions show"));
            let undoable = value
                .get("undoable")
                .or_else(|| value.get("action").and_then(|a| a.get("undoable")))
                .and_then(Value::as_bool)
                .unwrap_or_else(|| panic!("{tier} show surfaces `undoable`: {value}"));
            assert_eq!(undoable, expected, "{tier} show {id}: {value}");
            let reads: Vec<String> = server
                .recorded()
                .into_iter()
                .filter(|r| r.method == "GET")
                .map(|r| r.path)
                .collect();
            assert!(
                reads.contains(&format!("/actions/{id}")),
                "{tier} show reads GET /actions/{{id}}: {reads:?}"
            );
        }
    }
}

// @spec ACTION-EXECUTOR-23
#[test]
fn show_and_execution_of_a_missing_id_emit_one_error_object_exit_1() {
    for tier in ["local", "cluster"] {
        for verb in ["show", "execution"] {
            let server = api();
            let output = run(tier, &[verb, MISSING_ID], &server, None, true);
            assert_eq!(
                output.status.code(),
                Some(1),
                "{tier} actions {verb} of a missing id is a failure (ADR-0021 exit 1):\n{}",
                text(&output)
            );
            let value = one_object(&output, &format!("{tier} actions {verb} 404"));
            assert_error_object(&value, &format!("{tier} actions {verb} 404"));
            assert!(
                value["error"].as_str().unwrap().contains("not found"),
                "{tier} {verb}: the error names the miss: {value}"
            );
        }
    }
}

// --------------------------------------------------------------------------
// undo: the principal, the route, the execution id and state, never a snapshot
// --------------------------------------------------------------------------

// @spec ACTION-EXECUTOR-23 @spec ACTION-EXECUTOR-3 @spec ACTION-EXECUTOR-18
#[test]
fn undo_sends_the_operator_principal_and_prints_execution_id_and_state() {
    for tier in ["local", "cluster"] {
        let server = api();
        let output = run(
            tier,
            &["undo", ACTION_ID],
            &server,
            Some(OPERATOR_PRINCIPAL),
            true,
        );
        let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
        let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
        assert_eq!(
            output.status.code(),
            Some(0),
            "{tier} granted undo must succeed:\n{stdout}{stderr}"
        );
        let value = one_object(&output, &format!("{tier} actions undo"));
        assert_eq!(value["execution_id"], EXECUTION_ID, "{tier} undo: {value}");
        assert_eq!(value["state"], "requested", "{tier} undo: {value}");

        let undos = undo_requests(&server);
        assert_eq!(undos.len(), 1, "{tier} undo POSTs the ruling exactly once");
        let request = &undos[0];
        assert_eq!(request.path, format!("/actions/{ACTION_ID}/undo"));
        assert_eq!(
            request.header("X-Curie-Approval-Principal"),
            Some(OPERATOR_PRINCIPAL),
            "{tier} undo authenticates with the operator principal header, as approvals --resolve does"
        );
        let body: Value = serde_json::from_slice(&request.body).unwrap_or_else(|err| {
            panic!("{tier} undo body must be JSON (the route requires one): {err}")
        });
        assert_eq!(
            body,
            json!({}),
            "{tier} undo body asserts no actor and supplies no observation: {body}"
        );

        assert!(
            !stdout.contains(SNAPSHOT_MARKER) && !stderr.contains(SNAPSHOT_MARKER),
            "{tier} undo never prints snapshot material:\nstdout: {stdout}\nstderr: {stderr}"
        );
        assert!(
            !contains_key(&value, "prior_state") && !contains_key(&value, "target"),
            "{tier} undo output carries no snapshot or target: {value}"
        );
        assert!(
            !stdout.contains(OPERATOR_PRINCIPAL) && !stderr.contains(OPERATOR_PRINCIPAL),
            "{tier} undo never prints the principal token"
        );
    }
}

// @spec ACTION-EXECUTOR-23 @spec ACTION-EXECUTOR-18
#[test]
fn undo_human_output_names_execution_and_state_without_snapshot() {
    let server = api();
    let output = run(
        "local",
        &["undo", ACTION_ID],
        &server,
        Some(OPERATOR_PRINCIPAL),
        false,
    );
    let shown = text(&output);
    assert_eq!(output.status.code(), Some(0), "{shown}");
    assert!(
        shown.contains(EXECUTION_ID),
        "undo names the execution id: {shown}"
    );
    assert!(shown.contains("requested"), "undo names the state: {shown}");
    assert!(
        !shown.contains(SNAPSHOT_MARKER),
        "undo never prints snapshot material: {shown}"
    );
}

// @spec ACTION-EXECUTOR-23
#[test]
fn undo_without_a_principal_is_refused_before_http_with_a_fix() {
    for tier in ["local", "cluster"] {
        let server = api();
        let output = run(tier, &["undo", ACTION_ID], &server, None, true);
        assert_eq!(
            output.status.code(),
            Some(2),
            "{tier} undo without a principal is a usage error:\n{}",
            text(&output)
        );
        let value = one_object(&output, &format!("{tier} actions undo without principal"));
        assert_error_object(&value, &format!("{tier} undo without principal"));
        let rendered = value.to_string();
        assert!(
            rendered.contains("CURIE_APPROVAL_PRINCIPAL_TOKEN"),
            "{tier}: the error names the env-backed credential: {value}"
        );
        assert!(
            undo_requests(&server).is_empty(),
            "{tier}: no ruling is requested without a principal"
        );
    }
}

// @spec ACTION-EXECUTOR-23 @spec ACTION-EXECUTOR-3
/// A refusal is still exactly one JSON object, carrying the API's stated reason
/// and the ADR-0021 class: 409/403/404 are failures (1), 422 is usage (2).
#[test]
fn undo_refusals_emit_one_error_object_with_the_api_reason_and_exit_class() {
    let cases: [(&str, i32, Option<&str>); 4] = [
        (IN_FLIGHT_ACTION_ID, 1, Some(IN_FLIGHT_REASON)),
        (FORBIDDEN_ACTION_ID, 1, Some(UNAUTHORIZED_REASON)),
        (MISSING_ID, 1, Some("not found")),
        (MALFORMED_ID, 2, None),
    ];
    for tier in ["local", "cluster"] {
        for (id, code, reason) in cases {
            let server = api();
            let output = run(tier, &["undo", id], &server, Some(OPERATOR_PRINCIPAL), true);
            let what = format!("{tier} actions undo {id}");
            assert_eq!(
                output.status.code(),
                Some(code),
                "{what}: ADR-0021 exit class:\n{}",
                text(&output)
            );
            let value = one_object(&output, &what);
            assert_error_object(&value, &what);
            if let Some(reason) = reason {
                assert!(
                    value["error"].as_str().unwrap().contains(reason),
                    "{what}: the error states the API's reason: {value}"
                );
            }
            assert!(
                !text(&output).contains(OPERATOR_PRINCIPAL),
                "{what}: a refusal never prints the principal token"
            );
        }
    }
}

// --------------------------------------------------------------------------
// execution: the receipt of every terminal state
// --------------------------------------------------------------------------

// @spec ACTION-EXECUTOR-23 @spec ACTION-EXECUTOR-18
#[test]
fn execution_receipt_names_state_and_code_for_each_terminal_state() {
    for tier in ["local", "cluster"] {
        for (id, state, code) in terminal_receipts() {
            let server = api();
            let output = run(tier, &["execution", id], &server, None, true);
            let what = format!("{tier} actions execution ({state})");
            assert_eq!(
                output.status.code(),
                Some(0),
                "{what}: reading a receipt succeeds whatever the execution's outcome:\n{}",
                text(&output)
            );
            let value = one_object(&output, &what);
            assert_eq!(value["execution_id"], id, "{what}: {value}");
            assert_eq!(value["state"], state, "{what}: {value}");
            match code {
                Some(code) => assert_eq!(value["code"], code, "{what}: {value}"),
                None => assert!(
                    value["code"].is_null(),
                    "{what}: a confirmed receipt carries no code: {value}"
                ),
            }

            let reads: Vec<Request> = server
                .recorded()
                .into_iter()
                .filter(|r| r.path == format!("/action-executions/{id}"))
                .collect();
            assert_eq!(
                reads.len(),
                1,
                "{what}: reads GET /action-executions/{{id}}"
            );
            assert_eq!(reads[0].method, "GET");
            assert_eq!(
                reads[0].header("X-Api-Key"),
                Some(TEST_API_KEY),
                "{what}: the receipt route is on the platform key"
            );
        }
    }
}

// @spec ACTION-EXECUTOR-23 @spec ACTION-EXECUTOR-18
#[test]
fn execution_human_receipt_names_state_and_code() {
    for (id, state, code) in terminal_receipts() {
        let server = api();
        let output = run("local", &["execution", id], &server, None, false);
        let shown = text(&output);
        assert_eq!(output.status.code(), Some(0), "{state}: {shown}");
        assert!(
            shown.contains(state),
            "the receipt names state {state}: {shown}"
        );
        if let Some(code) = code {
            assert!(
                shown.contains(code),
                "the receipt names code {code}: {shown}"
            );
        }
    }
}

// --------------------------------------------------------------------------
// Review round 1
// --------------------------------------------------------------------------

// @spec ACTION-EXECUTOR-23 @spec ACTION-EXECUTOR-1 @spec ACTION-EXECUTOR-20
/// M1. The executor-disabled 503 is a configuration refusal, not an outage:
/// only an operator changing the installation can change the answer, so it
/// must not carry the retryable class (3). It is a failure (1) whose fix names
/// the setting that enables the executor.
#[test]
fn undo_refused_because_the_executor_is_disabled_is_not_retryable_and_names_the_setting() {
    for tier in ["local", "cluster"] {
        let server = api();
        let output = run(
            tier,
            &["undo", EXECUTOR_OFF_ACTION_ID],
            &server,
            Some(OPERATOR_PRINCIPAL),
            true,
        );
        let what = format!("{tier} actions undo (executor disabled)");
        assert_eq!(
            output.status.code(),
            Some(1),
            "{what}: a disabled executor is a failure (exit 1), never the retryable exit 3:\n{}",
            text(&output)
        );
        let value = one_object(&output, &what);
        assert_error_object(&value, &what);
        assert!(
            value["error"]
                .as_str()
                .unwrap()
                .contains(EXECUTOR_DISABLED_REASON),
            "{what}: the error states the API's reason: {value}"
        );
        let fix = value["fix"]
            .as_str()
            .unwrap_or_else(|| panic!("{what}: the refusal carries a fix: {value}"));
        assert!(
            fix.contains("CURIE_ACTION_EXECUTOR_ENABLED") || fix.contains("actionExecutor.enabled"),
            "{what}: the fix names how to enable the executor (the chart value \
             actionExecutor.enabled or CURIE_ACTION_EXECUTOR_ENABLED): {fix}"
        );
    }
}

/// Run `curie cluster actions ..` with NO `--api-url`/`--api-key`, so the
/// cluster tier would have to discover its connection, with an empty `PATH`
/// (no helm, no kubectl) and no kubeconfig, as `cluster_target.rs` does.
fn run_cluster_undiscovered(args: &[&str], principal: Option<&str>) -> Output {
    let dir = tempfile::tempdir().expect("tempdir");
    let empty_path = dir.path().join("empty-path");
    std::fs::create_dir_all(&empty_path).expect("create empty PATH");
    let mut command = Command::new(bin());
    command
        .args(["cluster", "actions"])
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
        .unwrap_or_else(|err| panic!("run curie cluster actions {}: {err}", args.join(" ")))
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

// @spec ACTION-EXECUTOR-23
/// M2. At the cluster tier without `--api-url`, a malformed id is a usage error
/// naming the id, raised before any Helm, kube or port-forward step.
#[test]
fn cluster_tier_refuses_a_malformed_id_before_discovering_the_connection() {
    for verb in ["show", "undo", "execution"] {
        let output = run_cluster_undiscovered(&[verb, MALFORMED_ID], Some(OPERATOR_PRINCIPAL));
        let what = format!("cluster actions {verb} {MALFORMED_ID} (no --api-url)");
        assert_eq!(
            output.status.code(),
            Some(2),
            "{what}: a malformed id is a usage error:\n{}",
            text(&output)
        );
        let value = one_object(&output, &what);
        assert_error_object(&value, &what);
        assert!(
            value["error"].as_str().unwrap().contains(MALFORMED_ID),
            "{what}: the error names the malformed id: {value}"
        );
        assert_no_connection_discovery(&output, &what);
    }
}

// @spec ACTION-EXECUTOR-23 @spec ACTION-EXECUTOR-3
/// M2. At the cluster tier without `--api-url`, a missing principal is a usage
/// error naming CURIE_APPROVAL_PRINCIPAL_TOKEN, raised before the release's key
/// Secret is read or a tunnel opened.
#[test]
fn cluster_tier_refuses_a_missing_principal_before_discovering_the_connection() {
    let output = run_cluster_undiscovered(&["undo", ACTION_ID], None);
    let what = "cluster actions undo without a principal (no --api-url)";
    assert_eq!(
        output.status.code(),
        Some(2),
        "{what}: a missing principal is a usage error:\n{}",
        text(&output)
    );
    let value = one_object(&output, what);
    assert_error_object(&value, what);
    assert!(
        value.to_string().contains("CURIE_APPROVAL_PRINCIPAL_TOKEN"),
        "{what}: the error names the env-backed credential: {value}"
    );
    assert_no_connection_discovery(&output, what);
}

// @spec ACTION-EXECUTOR-23
/// M3. `list --conversation <id>` scopes the read with the API's
/// `conversation_id` filter, and never asks for more than the API's cap.
#[test]
fn list_filters_by_conversation_at_both_tiers() {
    for tier in ["local", "cluster"] {
        let server = api();
        let output = run(
            tier,
            &["list", "--conversation", "thread-1"],
            &server,
            None,
            true,
        );
        assert_eq!(
            output.status.code(),
            Some(0),
            "{tier} actions list --conversation must succeed:\n{}",
            text(&output)
        );
        one_object(&output, &format!("{tier} actions list --conversation"));
        let listed: Vec<Request> = server
            .recorded()
            .into_iter()
            .filter(|r| r.method == "GET" && r.path.starts_with("/actions?"))
            .collect();
        assert_eq!(listed.len(), 1, "{tier} list reads GET /actions once");
        let path = &listed[0].path;
        assert!(
            path.contains("conversation_id=thread-1"),
            "{tier} list --conversation passes conversation_id: {path}"
        );
        if let Some(limit) = path
            .split(['?', '&'])
            .find_map(|pair| pair.strip_prefix("limit="))
        {
            let limit: u32 = limit.parse().expect("limit is a number");
            assert!(
                (1..=200).contains(&limit),
                "{tier} list asks for at most the API's cap: {path}"
            );
        }
    }
}

// @spec ACTION-EXECUTOR-23
/// M3. The API returns the OLDEST actions first and caps a page at 200. When a
/// page comes back full, both outputs say the list may be truncated, and the
/// human one says which end was kept.
#[test]
fn a_full_page_is_reported_as_possibly_truncated_oldest_first() {
    let server = serve(|request: &Request| {
        let (route, _) = request.path.split_once('?').unwrap_or((&request.path, ""));
        match (request.method.as_str(), route) {
            ("GET", "/agents") => Response::json(200, &format!("[{}]", agent_json())),
            ("GET", "/actions") => {
                let rows: Vec<Value> = (0..200)
                    .map(|n| action_json(&format!("11111111-1111-1111-1111-{n:012}"), false))
                    .collect();
                Response::json(200, &Value::Array(rows).to_string())
            }
            _ => detail(405, "unexpected request"),
        }
    });

    let json_out = run(
        "local",
        &["list", "--agent", AGENT_NAME],
        &server,
        None,
        true,
    );
    assert_eq!(json_out.status.code(), Some(0), "{}", text(&json_out));
    let value = one_object(&json_out, "list of a full page");
    assert_eq!(
        value["truncated"],
        Value::Bool(true),
        "a full page of 200 is reported truncated: {}",
        value["truncated"]
    );

    let human = run(
        "local",
        &["list", "--agent", AGENT_NAME],
        &server,
        None,
        false,
    );
    assert_eq!(human.status.code(), Some(0), "{}", text(&human));
    let shown = text(&human).to_lowercase();
    assert!(
        shown.contains("200") && (shown.contains("more") || shown.contains("truncat")),
        "the human list says a full page may be truncated: {shown}"
    );
    assert!(
        shown.contains("oldest"),
        "the human list says the kept page is the OLDEST actions, so an operator \
         looking for a recent one knows to narrow the filter: {shown}"
    );
}

// @spec ACTION-EXECUTOR-23
/// L1. `list` and `show` never print snapshot material, in either mode, even
/// though every stub row carries a sealed `prior_state` and a `post_state`.
#[test]
fn list_and_show_never_print_snapshot_material() {
    for tier in ["local", "cluster"] {
        for args in [vec!["list", "--agent", AGENT_NAME], vec!["show", ACTION_ID]] {
            for json_mode in [true, false] {
                let server = api();
                let output = run(tier, &args, &server, None, json_mode);
                let what = format!("{tier} actions {} (json={json_mode})", args.join(" "));
                assert_eq!(output.status.code(), Some(0), "{what}:\n{}", text(&output));
                assert!(
                    !text(&output).contains(SNAPSHOT_MARKER),
                    "{what}: never prints snapshot material:\n{}",
                    text(&output)
                );
                if json_mode {
                    let value = one_object(&output, &what);
                    assert!(
                        !contains_key(&value, "prior_state") && !contains_key(&value, "post_state"),
                        "{what}: carries no prior_state or post_state: {value}"
                    );
                }
            }
        }
    }
}

// @spec ACTION-EXECUTOR-18
/// L2. The receipt hand-projects the `ActionExecution` mirror (renaming `id`,
/// folding `refusal_code`/`failure_code` into `code`), so the pair must be
/// declared to the emit-parity gate in `cli/api-mirrors.json`; the gate only
/// checks declared pairs. The folded codes are declared omissions with a reason.
#[test]
fn the_receipt_projection_is_declared_to_the_emit_parity_gate() {
    let mirrors: Value = serde_json::from_str(include_str!("../api-mirrors.json"))
        .expect("cli/api-mirrors.json parses");
    let entry = mirrors["emits"]
        .as_array()
        .expect("api-mirrors.json has an emits array")
        .iter()
        .find(|e| e["output"] == "ActionsOutput" && e["struct"] == "ActionExecution")
        .unwrap_or_else(|| {
            panic!("emits must declare the (ActionsOutput, ActionExecution) projection")
        });
    let omissions = entry["omissions"].as_array().cloned().unwrap_or_default();
    for field in ["refusal_code", "failure_code"] {
        let declared = omissions
            .iter()
            .find(|o| o["field"] == field)
            .unwrap_or_else(|| {
                panic!("{field} folds into `code` and is a declared omission: {entry}")
            });
        assert!(
            declared["why"]
                .as_str()
                .is_some_and(|why| !why.trim().is_empty()),
            "{field}: the omission states why: {declared}"
        );
    }
}

// @spec ACTION-EXECUTOR-23
/// L3. A principal token with a trailing newline (an `$(cat file)` export)
/// behaves at `actions undo` exactly as it does at `approvals --resolve`: the
/// same exit class, one error object, no request reaching the API, and the
/// token never printed.
#[test]
fn a_principal_with_a_trailing_newline_behaves_as_it_does_for_approvals_resolve() {
    let token = format!("{OPERATOR_PRINCIPAL}\n");
    let observe = |argv: &[&str]| -> (Option<i32>, Value, usize, String) {
        let server = serve(|_| detail(500, "unexpected request"));
        let output = Command::new(bin())
            .args(argv)
            .args([
                "--api-url",
                &server.base_url,
                "--api-key",
                TEST_API_KEY,
                "--json",
            ])
            .env_remove("CURIE_API_URL")
            .env_remove("CURIE_API_KEY")
            .env("CURIE_APPROVAL_PRINCIPAL_TOKEN", &token)
            .env("NO_COLOR", "1")
            .output()
            .unwrap_or_else(|err| panic!("run curie {}: {err}", argv.join(" ")));
        let what = argv.join(" ");
        let value = one_object(&output, &what);
        assert_error_object(&value, &what);
        (
            output.status.code(),
            value,
            server.recorded().len(),
            text(&output),
        )
    };

    let (approvals_code, approvals_value, approvals_sent, approvals_text) = observe(&[
        "local",
        "approvals",
        "weather",
        "--resolve",
        "22222222-2222-2222-2222-222222222222",
    ]);
    let (undo_code, undo_value, undo_sent, undo_text) =
        observe(&["local", "actions", "undo", ACTION_ID]);

    assert_eq!(
        undo_code, approvals_code,
        "undo classifies a newline-terminated token as approvals --resolve does\n\
         approvals: {approvals_value}\nundo: {undo_value}"
    );
    assert_eq!(
        undo_sent, approvals_sent,
        "undo sends what approvals --resolve sends for the same token"
    );
    assert_eq!(
        undo_value["fix"].is_null(),
        approvals_value["fix"].is_null(),
        "undo carries a fix exactly when approvals --resolve does\n\
         approvals: {approvals_value}\nundo: {undo_value}"
    );
    for (what, shown) in [("approvals", approvals_text), ("undo", undo_text)] {
        assert!(
            !shown.contains(OPERATOR_PRINCIPAL),
            "{what}: the token is never printed: {shown}"
        );
    }
}
