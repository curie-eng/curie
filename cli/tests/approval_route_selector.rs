//! Integration: `curie <tier> approvals <agent>` registers, reads back and
//! preserves ADR 0271 approver-ticket selectors on approval routes.
//!
//! A route binding may carry `approver_ticket` (connector, tool, key id, public
//! key and a ceiling naming the approver role version) and `separation`. A route
//! write that touches a selector is a compare-and-set on the agent's
//! `connector_requester_identity_version`, sent as `approval_routes_expected_version`
//! beside `approval_routes`. These tests drive the BUILT BINARY against an
//! in-process mock of the platform API and cover the exact PATCH body, the local
//! refusal of an unknown key, the usage refusal of a stray version flag, the
//! `--list-routes --json` readback validated against the committed schema, the
//! round trip of a registration through an unrelated single-route edit, and the
//! 409 recovery.

use crate::support;

use std::process::Command;
use std::sync::{Arc, Mutex};
use support::{serve, MockServer, Response};

const AGENT_ID: &str = "77777777-7777-7777-7777-777777777777";
const AGENT_NAME: &str = "acme-bot";
const ROUTES_EXPECTED: &str = "approval_routes_expected_version";
const VERSION: &str = "connector_requester_identity_version";
const CONFLICT: &str = "approval_routes.version_conflict";

fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_curie")
}

fn agent_path() -> String {
    format!("/agents/{AGENT_ID}")
}

fn public_key() -> String {
    // Unpadded base64url of 32 bytes.
    "A".repeat(43)
}

/// The server-owned pinned role the API returns inside a registration's ceiling.
/// The real API stores and returns it as canonical JSON text (a string whose
/// content is the role JSON), not as a nested object: here a migrated,
/// restricted approver role with one `issuer_subject` entry, in the API's
/// canonical render (sorted keys, no whitespace).
fn pinned_role() -> serde_json::Value {
    serde_json::Value::String(
        r#"{"filter":{"entries":[{"issuer":"https://idp.example.test","kind":"issuer_subject","subject":"approver-1"}]},"legacy_state":"migrated","raw_account_revocation":"separate_removal_required","schema_version":1}"#
            .to_string(),
    )
}

/// A registration as an operator writes it.
fn registration() -> serde_json::Value {
    serde_json::json!({
        "connector": "ledger",
        "tool": "post_entry",
        "key_id": "ledger-key-1",
        "public_key": public_key(),
        "ceiling": {
            "approver_role_version": 3,
            "users": [{"issuer": "https://idp.example.test", "subject": "approver-1"}],
        },
    })
}

/// The same registration as the API reads it back: the write fields plus the
/// pinned role.
fn registration_readback() -> serde_json::Value {
    let mut value = registration();
    value["ceiling"]["approver_role"] = pinned_role();
    value
}

fn route_with_ticket() -> serde_json::Value {
    serde_json::json!({
        "resolution": {"kind": "slack", "address": "C0EXAMPLE1"},
        "approver_ticket": registration(),
        "separation": {"exclude_requester": false},
    })
}

fn plain_route(channel: &str) -> serde_json::Value {
    serde_json::json!({"resolution": {"kind": "slack", "address": channel}})
}

fn agent_json(routes: &serde_json::Value, version: u64) -> serde_json::Value {
    let mut agent = serde_json::json!({
        "id": AGENT_ID,
        "name": AGENT_NAME,
        "channels": [{"kind": "slack", "address": "C0EXAMPLE0"}],
        "approval_required_tools": null,
        "approval_routes": routes,
        "memory": false,
        "connector_requester_identity": true,
        "created_at": "2026-11-08T00:00:00Z",
    });
    agent[VERSION] = version.into();
    agent
}

struct Api {
    server: MockServer,
    routes: Arc<Mutex<serde_json::Value>>,
}

/// A mock API holding one agent. A PATCH is a 409 `approval_routes.version_conflict`
/// when it carries an expected version that is not the stored one, or always when
/// `always_conflict` is set (another operator wrote between the read and the write).
/// An accepted PATCH stores the routes, pinning `ceiling.approver_role` on every
/// registration like the real API, and bumps the version.
fn api(initial_routes: serde_json::Value, version: u64, always_conflict: bool) -> Api {
    let routes = Arc::new(Mutex::new(initial_routes));
    let state = Arc::new(Mutex::new(version));
    let (server_routes, server_state) = (Arc::clone(&routes), Arc::clone(&state));
    let server = serve(move |req| {
        let path = req.path.split('?').next().unwrap_or_default().to_string();
        let mut stored_routes = server_routes.lock().expect("routes lock");
        let mut stored_version = server_state.lock().expect("version lock");
        match (req.method.as_str(), path.as_str()) {
            ("GET", "/agents") => Response::json(
                200,
                &serde_json::json!([agent_json(&stored_routes, *stored_version)]).to_string(),
            ),
            ("GET", p) if p == agent_path() => Response::json(
                200,
                &agent_json(&stored_routes, *stored_version).to_string(),
            ),
            ("GET", "/deployments") => Response::json(200, "[]"),
            ("PATCH", p) if p == agent_path() => {
                let body: serde_json::Value =
                    serde_json::from_slice(&req.body).expect("the PATCH body is JSON");
                let expected = body
                    .get(ROUTES_EXPECTED)
                    .and_then(serde_json::Value::as_u64);
                if always_conflict || expected.is_some_and(|e| e != *stored_version) {
                    return Response::json(
                        409,
                        &serde_json::json!({"detail": {
                            "code": CONFLICT,
                            "message": "the agent's selector configuration version changed",
                        }})
                        .to_string(),
                    );
                }
                let mut next = body["approval_routes"].clone();
                if let Some(map) = next.as_object_mut() {
                    for route in map.values_mut() {
                        if route.get("approver_ticket").is_some() {
                            route["approver_ticket"]["ceiling"]["approver_role"] = pinned_role();
                        }
                    }
                }
                *stored_routes = next;
                *stored_version += 1;
                Response::json(
                    200,
                    &agent_json(&stored_routes, *stored_version).to_string(),
                )
            }
            _ => Response::json(405, r#"{"detail":"unexpected request"}"#),
        }
    });
    Api { server, routes }
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

fn args<'a>(base_url: &'a str, extra: &[&'a str]) -> Vec<&'a str> {
    let mut argv = vec!["cluster", "approvals", AGENT_NAME];
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

fn write_routes(dir: &tempfile::TempDir, routes: &serde_json::Value) -> String {
    let path = dir.path().join("routes.json");
    std::fs::write(&path, routes.to_string()).expect("write the routes file");
    path.to_string_lossy().into_owned()
}

#[test]
fn routes_file_registration_is_sent_verbatim_with_the_version_read_in_the_same_command() {
    let api = api(serde_json::Value::Null, 5, false);
    let dir = tempfile::tempdir().expect("tempdir");
    let routes = serde_json::json!({"ledger_post": route_with_ticket()});
    let file = write_routes(&dir, &routes);

    let run = run(&args(&api.server.base_url, &["--routes-from", &file]));
    assert_eq!(run.code, 0, "{}{}", run.stdout, run.stderr);

    // Only the route map and its expected version travel together (D3).
    assert_eq!(
        patches(&api.server),
        vec![serde_json::json!({"approval_routes": routes, ROUTES_EXPECTED: 5})]
    );
    let recorded = api.server.recorded();
    let patch_at = recorded
        .iter()
        .position(|r| r.method == "PATCH")
        .expect("one PATCH was sent");
    assert!(
        recorded[..patch_at].iter().any(|r| r.method == "GET"),
        "the current version must be read before the write"
    );
}

#[test]
fn an_omitted_separation_and_ceiling_users_stay_omitted_on_the_wire() {
    let api = api(serde_json::Value::Null, 2, false);
    let dir = tempfile::tempdir().expect("tempdir");
    let mut ticket = registration();
    ticket["ceiling"]
        .as_object_mut()
        .unwrap()
        .remove("users")
        .unwrap();
    let routes = serde_json::json!({"ledger_post": {
        "resolution": {"kind": "slack", "address": "C0EXAMPLE1"},
        "approver_ticket": ticket,
    }});
    let file = write_routes(&dir, &routes);

    let run = run(&args(&api.server.base_url, &["--routes-from", &file]));
    assert_eq!(run.code, 0, "{}{}", run.stdout, run.stderr);
    // The server owns the default; the CLI must not fill `separation` in.
    assert_eq!(
        patches(&api.server),
        vec![serde_json::json!({"approval_routes": routes, ROUTES_EXPECTED: 2})]
    );
}

#[test]
fn an_explicit_routes_expected_version_replaces_the_one_read() {
    let api = api(serde_json::Value::Null, 5, false);
    let dir = tempfile::tempdir().expect("tempdir");
    let routes = serde_json::json!({"ledger_post": route_with_ticket()});
    let file = write_routes(&dir, &routes);

    let run = run(&args(
        &api.server.base_url,
        &["--routes-from", &file, "--routes-expected-version", "3"],
    ));
    // The operator named version 3; the agent is at 5, so the API refuses and
    // the CLI must not swap in a fresh version.
    assert_ne!(run.code, 0, "{}{}", run.stdout, run.stderr);
    assert_eq!(
        patches(&api.server),
        vec![serde_json::json!({"approval_routes": routes, ROUTES_EXPECTED: 3})]
    );
    assert!(api.routes.lock().unwrap().is_null(), "nothing was stored");
}

#[test]
fn an_explicit_routes_expected_version_that_matches_is_accepted() {
    let api = api(serde_json::Value::Null, 5, false);
    let dir = tempfile::tempdir().expect("tempdir");
    let routes = serde_json::json!({"ledger_post": route_with_ticket()});
    let file = write_routes(&dir, &routes);

    let run = run(&args(
        &api.server.base_url,
        &["--routes-from", &file, "--routes-expected-version", "5"],
    ));
    assert_eq!(run.code, 0, "{}{}", run.stdout, run.stderr);
    assert_eq!(
        patches(&api.server),
        vec![serde_json::json!({"approval_routes": routes, ROUTES_EXPECTED: 5})]
    );
}

#[test]
fn an_unknown_key_in_a_selector_is_refused_locally_before_any_patch() {
    type Mutate = fn(&mut serde_json::Value);
    let cases: [(&str, Mutate); 4] = [
        ("approver_tickets", |route| {
            route["approver_ticket"]["approver_tickets"] = serde_json::json!(true);
        }),
        ("ceilings", |route| {
            route["approver_ticket"]["ceiling"]["ceilings"] = serde_json::json!(1);
        }),
        ("email", |route| {
            route["approver_ticket"]["ceiling"]["users"][0]["email"] =
                serde_json::json!("approver@example.test");
        }),
        ("exempt_group", |route| {
            route["separation"]["exempt_group"] = serde_json::json!("S0EXAMPLE1");
        }),
    ];
    for (unknown, mutate) in cases {
        let dir = tempfile::tempdir().expect("tempdir");

        // The neighboring valid file writes.
        let valid_api = api(serde_json::Value::Null, 1, false);
        let valid = write_routes(&dir, &serde_json::json!({"r": route_with_ticket()}));
        let ok = run(&args(
            &valid_api.server.base_url,
            &["--routes-from", &valid],
        ));
        assert_eq!(ok.code, 0, "{unknown}: {}{}", ok.stdout, ok.stderr);
        assert_eq!(patches(&valid_api.server).len(), 1, "{unknown}");

        // The same file with one unknown key is refused before any PATCH.
        let refused_api = api(serde_json::Value::Null, 1, false);
        let mut route = route_with_ticket();
        mutate(&mut route);
        let bad = write_routes(&dir, &serde_json::json!({"r": route}));
        let refused = run(&args(
            &refused_api.server.base_url,
            &["--routes-from", &bad],
        ));
        assert_ne!(
            refused.code, 0,
            "{unknown}: {}{}",
            refused.stdout, refused.stderr
        );
        assert!(
            patches(&refused_api.server).is_empty(),
            "{unknown}: a refused file must send no PATCH"
        );
        let text = format!("{}{}", refused.stdout, refused.stderr);
        assert!(
            text.contains(unknown),
            "{unknown}: the refusal must name the unknown key:\n{text}"
        );
    }
}

#[test]
fn routes_expected_version_without_a_route_write_flag_is_a_usage_refusal() {
    for extra in [
        &["--routes-expected-version", "3"][..],
        &["--routes-expected-version", "3", "--list-routes"][..],
        &["--routes-expected-version", "3", "--gate", "send_email"][..],
    ] {
        let api = api(serde_json::Value::Null, 3, false);
        let run = run(&args(&api.server.base_url, extra));
        assert_eq!(run.code, 2, "{extra:?}: {}{}", run.stdout, run.stderr);
        assert!(
            patches(&api.server).is_empty(),
            "{extra:?} must send no PATCH"
        );
    }

    // The neighboring valid case: with a route write flag the version is sent.
    let api = api(serde_json::Value::Null, 3, false);
    let run = run(&args(
        &api.server.base_url,
        &[
            "--route-resolution",
            "deal_desk=C0EXAMPLE1",
            "--routes-expected-version",
            "3",
        ],
    ));
    assert_eq!(run.code, 0, "{}{}", run.stdout, run.stderr);
    let sent = patches(&api.server);
    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0][ROUTES_EXPECTED], 3, "{}", sent[0]);
}

#[test]
fn list_routes_json_includes_the_selector_the_pin_and_separation() {
    let stored = serde_json::json!({
        "ledger_post": {
            "resolution": {"kind": "slack", "address": "C0EXAMPLE1"},
            "approver_ticket": registration_readback(),
            "separation": {"exclude_requester": true},
        },
        "deal_desk": plain_route("C0EXAMPLE2"),
    });
    let api = api(stored.clone(), 4, false);
    let run = run(&args(&api.server.base_url, &["--list-routes", "--json"]));
    assert_eq!(run.code, 0, "{}{}", run.stdout, run.stderr);
    let value = one_json(&run);

    let ticket = &value["routes"]["ledger_post"]["approver_ticket"];
    assert_eq!(*ticket, registration_readback(), "{value}");
    assert_eq!(
        ticket["ceiling"]["approver_role"],
        pinned_role(),
        "the server-owned pin must reach the operator: {value}"
    );
    assert_eq!(
        value["routes"]["ledger_post"]["separation"],
        serde_json::json!({"exclude_requester": true}),
        "{value}"
    );
    // A route with no registration keeps its existing shape.
    assert!(
        value["routes"]["deal_desk"]
            .get("approver_ticket")
            .is_none(),
        "{value}"
    );
    assert_matches_schema(approvals_schema(), &value);
    assert!(patches(&api.server).is_empty(), "a list must never write");
}

#[test]
fn a_single_route_edit_preserves_another_routes_registration() {
    let other = serde_json::json!({
        "resolution": {"kind": "slack", "address": "C0EXAMPLE1"},
        "approver_ticket": registration_readback(),
        "separation": {"exclude_requester": false},
    });
    let api = api(
        serde_json::json!({"ledger_post": other.clone(), "deal_desk": plain_route("C0EXAMPLE2")}),
        6,
        false,
    );
    let run = run(&args(
        &api.server.base_url,
        &["--route-resolution", "deal_desk=C0EXAMPLE3"],
    ));
    assert_eq!(run.code, 0, "{}{}", run.stdout, run.stderr);

    let sent = patches(&api.server);
    assert_eq!(sent.len(), 1, "{sent:?}");
    let routes = &sent[0]["approval_routes"];
    // The registration, its separation and the echoed pin round-trip unchanged.
    assert_eq!(routes["ledger_post"], other, "{}", sent[0]);
    assert_eq!(
        routes["ledger_post"]["approver_ticket"]["ceiling"]["approver_role"],
        pinned_role()
    );
    assert_eq!(
        routes["deal_desk"],
        plain_route("C0EXAMPLE3"),
        "the edited route carries the new target: {}",
        sent[0]
    );
    assert_eq!(sent[0][ROUTES_EXPECTED], 6, "{}", sent[0]);
}

#[test]
fn a_single_route_edit_preserves_a_carried_routes_deadline_and_its_explicit_null() {
    // The deadline (ADR 0273) is presence-sensitive: a number, an explicit
    // null, or absent are three stored states, and a carried route must
    // write back the one it read.
    for deadline in [serde_json::json!(900), serde_json::Value::Null] {
        let other = serde_json::json!({
            "resolution": {"kind": "slack", "address": "C0EXAMPLE1"},
            "expires_in_seconds": deadline,
            "approver_ticket": registration_readback(),
            "separation": {"exclude_requester": true},
        });
        let api = api(
            serde_json::json!({"ledger_post": other.clone(), "deal_desk": plain_route("C0EXAMPLE2")}),
            6,
            false,
        );
        let run = run(&args(
            &api.server.base_url,
            &["--route-resolution", "deal_desk=C0EXAMPLE3"],
        ));
        assert_eq!(run.code, 0, "{}{}", run.stdout, run.stderr);
        let sent = patches(&api.server);
        assert_eq!(sent.len(), 1, "{sent:?}");
        assert_eq!(
            sent[0]["approval_routes"]["ledger_post"], other,
            "deadline {deadline}: {}",
            sent[0]
        );
    }
}

#[test]
fn a_same_route_edit_keeps_the_selector_routes_stored_deadline() {
    // Editing the selector route itself must not drop its deadline (ADR 0273
    // decision 1): a number, an explicit null and an absent key are three
    // stored states, and the flag edit can set none of them.
    for deadline in [
        Some(serde_json::json!(900)),
        Some(serde_json::Value::Null),
        None,
    ] {
        let mut stored = serde_json::json!({
            "resolution": {"kind": "slack", "address": "C0EXAMPLE1"},
            "approver_ticket": registration_readback(),
            "separation": {"exclude_requester": true},
        });
        if let Some(deadline) = &deadline {
            stored["expires_in_seconds"] = deadline.clone();
        }
        let api = api(serde_json::json!({"ledger_post": stored.clone()}), 6, false);
        let run = run(&args(
            &api.server.base_url,
            &["--route-resolution", "ledger_post=C0EXAMPLE3"],
        ));
        assert_eq!(run.code, 0, "{}{}", run.stdout, run.stderr);
        let sent = patches(&api.server);
        assert_eq!(sent.len(), 1, "{sent:?}");
        let mut expected = stored;
        expected["resolution"] = serde_json::json!({"kind": "slack", "address": "C0EXAMPLE3"});
        assert_eq!(
            sent[0]["approval_routes"]["ledger_post"], expected,
            "deadline {deadline:?}: {}",
            sent[0]
        );
    }
}

#[test]
fn a_single_route_edit_on_an_agent_without_selectors_sends_only_that_route() {
    let api = api(
        serde_json::json!({"deal_desk": plain_route("C0EXAMPLE2")}),
        6,
        false,
    );
    let run = run(&args(
        &api.server.base_url,
        &["--route-resolution", "deal_desk=C0EXAMPLE3"],
    ));
    assert_eq!(run.code, 0, "{}{}", run.stdout, run.stderr);
    let sent = patches(&api.server);
    assert_eq!(sent.len(), 1, "{sent:?}");
    assert_eq!(
        sent[0]["approval_routes"],
        serde_json::json!({"deal_desk": plain_route("C0EXAMPLE3")})
    );
}

#[test]
fn a_version_conflict_tells_the_operator_to_reread_and_retry_and_names_the_code() {
    let dir = tempfile::tempdir().expect("tempdir");
    let routes = serde_json::json!({"ledger_post": route_with_ticket()});
    let file = write_routes(&dir, &routes);

    // Another operator wrote between the CLI's read and its write.
    let raced = api(serde_json::Value::Null, 5, true);
    let run = run(&args(
        &raced.server.base_url,
        &["--routes-from", &file, "--json"],
    ));
    assert_ne!(run.code, 0, "{}{}", run.stdout, run.stderr);
    let text = format!("{}{}", run.stdout, run.stderr).to_lowercase();
    assert!(text.contains(CONFLICT), "the code must be named:\n{text}");
    assert!(text.contains("re-read"), "must say to re-read:\n{text}");
    assert!(text.contains("retry"), "must say to retry:\n{text}");
    // Sent once with the version it read, never retried with a fresh one.
    assert_eq!(
        patches(&raced.server),
        vec![serde_json::json!({"approval_routes": routes, ROUTES_EXPECTED: 5})]
    );
    assert!(raced.routes.lock().unwrap().is_null(), "nothing was stored");

    // The neighboring case: no race, the same file writes.
    let calm = api(serde_json::Value::Null, 5, false);
    let ok = run_ok(&calm, &file);
    assert_eq!(ok.code, 0, "{}{}", ok.stdout, ok.stderr);
}

fn run_ok(api: &Api, file: &str) -> Run {
    run(&args(
        &api.server.base_url,
        &["--routes-from", file, "--json"],
    ))
}
