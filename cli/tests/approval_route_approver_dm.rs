//! Integration: `curie <tier> approvals <agent>` writes, validates, lists and
//! preserves the ADR 0272 `{"mode": "approver_dm", "identity": "<slug>"}` route
//! resolution.
//!
//! An `approver_dm` route delivers its card to the one approver the ADR 0271
//! selector picks, through the named Slack identity. It is only meaningful with
//! an `approver_ticket` and cannot carry a `notification`. These tests drive the
//! BUILT BINARY against an in-process mock of the platform API and cover the
//! exact PATCH body, the local refusals (no PATCH reaches the API), the listing
//! readback, the round trip through an unrelated flag-only edit, and a malformed
//! stored resolution that must never be silently rewritten.

use crate::support;

use std::process::Command;
use std::sync::{Arc, Mutex};
use support::{serve, MockServer, Response};

const AGENT_ID: &str = "77777777-7777-7777-7777-777777777777";
const AGENT_NAME: &str = "acme-bot";
const ROUTES_EXPECTED: &str = "approval_routes_expected_version";
const VERSION: &str = "connector_requester_identity_version";

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

fn pinned_role() -> serde_json::Value {
    serde_json::Value::String(
        r#"{"filter":{"entries":[{"issuer":"https://idp.example.test","kind":"issuer_subject","subject":"approver-1"}]},"legacy_state":"migrated","raw_account_revocation":"separate_removal_required","schema_version":1}"#
            .to_string(),
    )
}

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

fn registration_readback() -> serde_json::Value {
    let mut value = registration();
    value["ceiling"]["approver_role"] = pinned_role();
    value
}

fn dm_resolution() -> serde_json::Value {
    serde_json::json!({"mode": "approver_dm", "identity": "default"})
}

/// An approver_dm route as an operator writes it.
fn dm_route() -> serde_json::Value {
    serde_json::json!({
        "resolution": dm_resolution(),
        "approver_ticket": registration(),
    })
}

/// The same route as the API reads it back, with the pinned role echoed and a
/// deadline and separation block that a carried route must keep.
fn dm_route_readback() -> serde_json::Value {
    serde_json::json!({
        "resolution": dm_resolution(),
        "expires_in_seconds": 900,
        "approver_ticket": registration_readback(),
        "separation": {"exclude_requester": true},
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
}

/// A mock API holding one agent. An accepted PATCH stores the routes verbatim
/// (pinning `ceiling.approver_role` on every registration like the real API)
/// and bumps the version.
fn api(initial_routes: serde_json::Value, version: u64) -> Api {
    let routes = Arc::new(Mutex::new(initial_routes));
    let state = Arc::new(Mutex::new(version));
    let server = serve(move |req| {
        let path = req.path.split('?').next().unwrap_or_default().to_string();
        let mut stored_routes = routes.lock().expect("routes lock");
        let mut stored_version = state.lock().expect("version lock");
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
    Api { server }
}

struct Run {
    code: i32,
    stdout: String,
    stderr: String,
}

impl Run {
    fn text(&self) -> String {
        format!("{}{}", self.stdout, self.stderr)
    }
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

fn assert_matches_schema(value: &serde_json::Value) {
    let schema: serde_json::Value =
        serde_json::from_str(include_str!("../schema/approvals.schema.json"))
            .expect("the schema parses");
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

fn write_routes(dir: &tempfile::TempDir, routes: &serde_json::Value) -> String {
    let path = dir.path().join("routes.json");
    std::fs::write(&path, routes.to_string()).expect("write the routes file");
    path.to_string_lossy().into_owned()
}

#[test]
fn an_approver_dm_route_with_a_ticket_is_sent_exactly_and_succeeds() {
    let api = api(serde_json::Value::Null, 5);
    let dir = tempfile::tempdir().expect("tempdir");
    let routes = serde_json::json!({"ledger_post": dm_route()});
    let file = write_routes(&dir, &routes);

    let run = run(&args(&api.server.base_url, &["--routes-from", &file]));
    assert_eq!(run.code, 0, "{}", run.text());

    let sent = patches(&api.server);
    assert_eq!(
        sent,
        vec![serde_json::json!({"approval_routes": routes, ROUTES_EXPECTED: 5})]
    );
    // The resolution is exactly the two-key form: no kind, address or extras.
    assert_eq!(
        sent[0]["approval_routes"]["ledger_post"]["resolution"],
        serde_json::json!({"mode": "approver_dm", "identity": "default"}),
        "{}",
        sent[0]
    );
}

#[test]
fn an_approver_dm_route_that_is_static_or_notifying_is_refused_locally_naming_approver_dm() {
    let dir = tempfile::tempdir().expect("tempdir");

    // The neighboring valid file writes.
    let valid_api = api(serde_json::Value::Null, 1);
    let valid = write_routes(&dir, &serde_json::json!({"ledger_post": dm_route()}));
    let ok = run(&args(
        &valid_api.server.base_url,
        &["--routes-from", &valid],
    ));
    assert_eq!(ok.code, 0, "{}", ok.text());
    assert_eq!(patches(&valid_api.server).len(), 1);

    let mut with_notification = dm_route();
    with_notification["notification"] =
        serde_json::json!({"kind": "slack", "address": "C0EXAMPLE2"});
    let cases = [
        (
            "no approver_ticket",
            serde_json::json!({"resolution": dm_resolution()}),
        ),
        ("a notification", with_notification),
    ];
    for (label, route) in cases {
        let refused_api = api(serde_json::Value::Null, 1);
        let bad = write_routes(&dir, &serde_json::json!({"ledger_post": route}));
        let refused = run(&args(
            &refused_api.server.base_url,
            &["--routes-from", &bad],
        ));
        assert_ne!(refused.code, 0, "{label}: {}", refused.text());
        assert!(
            patches(&refused_api.server).is_empty(),
            "{label}: a refused file must send no PATCH"
        );
        assert!(
            refused.recorded_requests(&refused_api).is_empty(),
            "{label}: the refusal is local, before any API call"
        );
        let text = refused.text();
        assert!(
            text.contains("approver_dm"),
            "{label}: the refusal must name approver_dm:\n{text}"
        );
        assert!(
            text.contains("ledger_post"),
            "{label}: the refusal must name the route:\n{text}"
        );
    }
}

impl Run {
    /// Every request the mock saw, for the "before any API call" assertion.
    fn recorded_requests(&self, api: &Api) -> Vec<String> {
        api.server
            .recorded()
            .into_iter()
            .map(|r| format!("{} {}", r.method, r.path))
            .collect()
    }
}

#[test]
fn list_routes_shows_the_mode_and_identity_and_json_round_trips_the_resolution() {
    let stored = serde_json::json!({
        "ledger_post": dm_route_readback(),
        "deal_desk": plain_route("C0EXAMPLE2"),
    });
    let api = api(stored, 4);

    let human = run(&args(&api.server.base_url, &["--list-routes"]));
    assert_eq!(human.code, 0, "{}", human.text());
    assert!(
        human.stdout.contains("approver_dm"),
        "the listing must show the mode:\n{}",
        human.stdout
    );
    assert!(
        human.stdout.contains("default"),
        "the listing must show the identity:\n{}",
        human.stdout
    );

    let json = run(&args(&api.server.base_url, &["--list-routes", "--json"]));
    assert_eq!(json.code, 0, "{}", json.text());
    let value = one_json(&json);
    assert_eq!(
        value["routes"]["ledger_post"]["resolution"],
        dm_resolution(),
        "the stored resolution must read back unchanged: {value}"
    );
    assert_eq!(
        value["routes"]["ledger_post"]["approver_ticket"],
        registration_readback(),
        "{value}"
    );
    assert_matches_schema(&value);
    assert!(patches(&api.server).is_empty(), "a list must never write");
}

#[test]
fn a_flag_only_edit_of_another_route_carries_the_approver_dm_route_unchanged() {
    let dm = dm_route_readback();
    let api = api(
        serde_json::json!({"ledger_post": dm.clone(), "deal_desk": plain_route("C0EXAMPLE2")}),
        6,
    );
    let run = run(&args(
        &api.server.base_url,
        &["--route-resolution", "deal_desk=C0EXAMPLE3"],
    ));
    assert_eq!(run.code, 0, "{}", run.text());

    let sent = patches(&api.server);
    assert_eq!(sent.len(), 1, "{sent:?}");
    let routes = &sent[0]["approval_routes"];
    // Mode, identity, selector with its pin, deadline and separation all
    // round-trip; nothing is dropped or rewritten to another resolution form.
    assert_eq!(routes["ledger_post"], dm, "{}", sent[0]);
    assert_eq!(
        routes["ledger_post"]["resolution"],
        dm_resolution(),
        "{}",
        sent[0]
    );
    assert_eq!(
        routes["deal_desk"],
        plain_route("C0EXAMPLE3"),
        "{}",
        sent[0]
    );
    assert_eq!(sent[0][ROUTES_EXPECTED], 6, "{}", sent[0]);
}

#[test]
fn a_malformed_stored_approver_dm_resolution_is_never_silently_rewritten() {
    let malformed = [
        (
            "an extra key",
            serde_json::json!({"mode": "approver_dm", "identity": "default", "channel": "C0EXAMPLE9"}),
        ),
        (
            "a missing identity",
            serde_json::json!({"mode": "approver_dm"}),
        ),
    ];
    for (label, resolution) in malformed {
        let mut stored_route = dm_route_readback();
        stored_route["resolution"] = resolution;
        let api = api(
            serde_json::json!({"ledger_post": stored_route.clone(), "deal_desk": plain_route("C0EXAMPLE2")}),
            6,
        );
        let run = run(&args(
            &api.server.base_url,
            &["--route-resolution", "deal_desk=C0EXAMPLE3"],
        ));

        // Either the CLI refuses and sends nothing, or it carries the stored
        // route byte for byte. It never repairs, drops or reshapes it.
        let sent = patches(&api.server);
        if sent.is_empty() {
            assert_ne!(
                run.code,
                0,
                "{label}: no write means the command must say why:\n{}",
                run.text()
            );
        } else {
            assert_eq!(sent.len(), 1, "{label}: {sent:?}");
            assert_eq!(
                sent[0]["approval_routes"]["ledger_post"], stored_route,
                "{label}: a malformed route must not be rewritten: {}",
                sent[0]
            );
        }
    }
}
