//! Integration: `<tier> approvals <agent>` writes and reads the agent's
//! approval ROUTE bindings (#1052).
//!
//! Until this verb existed, binding a route to a channel was a hand-written
//! `PATCH /agents/{id}`, against the repo's own "one entry point: `curie
//! <command>`" rule, and neither the CLI nor the console could do it.
//!
//! Driven through the real `commands::approvals` handler and a real `ApiClient`
//! against a wire-level stub.
//! The regressions worth catching only show up in the actual outgoing PATCH
//! body, or in the ABSENCE of one:
//!
//! - a write must send the whole map (the field replaces, it does not merge);
//! - the interactive `resolution` target, text-only `notification` target, and
//!   `approvers` must stay separate on the wire;
//! - and a malformed input must send NOTHING. A half-written binding map is a
//!   silently widened or narrowed approver set, which is exactly the failure the
//!   approval gate exists to prevent, so validation runs before the connection
//!   is opened and the negative tests assert on `recorded()` being empty.

mod support;

use curie::commands::{approvals, AgentActionOpts, ApprovalCmd, ApprovalsOutput};
use curie::credcheck::check_channel_id;
use curie::ui::CliOutput;
use std::sync::{Arc, Mutex};
use support::{serve, MockServer, Response};

const TEST_API_KEY: &str = "test-key";
const AGENT_ID: &str = "ag_1";

/// `GET /agents` payload: the handler resolves the agent by name first.
fn agents_list(routes_json: &str) -> String {
    format!(
        r#"[{{"id":"{AGENT_ID}","name":"deal-desk","channels":[{{"kind":"slack","address":"C0EXAMPLE0"}}],"approval_required_tools":null,"approval_routes":{routes_json},"memory":false}}]"#
    )
}

/// `PATCH /agents/{id}` echo: the updated agent the CLI renders from.
fn patched_agent(routes_json: &str) -> String {
    format!(
        r#"{{"id":"{AGENT_ID}","name":"deal-desk","channels":[{{"kind":"slack","address":"C0EXAMPLE0"}}],"approval_required_tools":null,"approval_routes":{routes_json},"memory":false}}"#
    )
}

/// A stub answering the two endpoints this verb touches.
fn stub(list_routes: &'static str, patch_echo: &'static str) -> MockServer {
    serve(move |req| match req.path.split('?').next().unwrap() {
        "/agents" => Response::json(200, &agents_list(list_routes)),
        "/deployments" => Response::json(200, "[]"),
        p if p == format!("/agents/{AGENT_ID}") => Response::json(200, &patched_agent(patch_echo)),
        other => panic!("unexpected request: {other}"),
    })
}

/// How `crud.update_agent_approval_routes` renders stored bindings: an unset
/// map is the literal `null`, since it stores `routes or None`.
fn render_routes(state: &Option<serde_json::Value>) -> String {
    match state {
        Some(routes) => routes.to_string(),
        None => "null".to_string(),
    }
}

/// A stub that MODELS the router instead of echoing a canned response, so a
/// clear that does not take is visible in what the next read returns.
///
/// It holds the agent's bindings as server-side state and mirrors the guard at
/// `apps/api/src/curie_api/routers/agents.py:154`, which is
/// `if data.approval_routes is not None`. Pydantic decodes an omitted key and an
/// explicit JSON `null` to the same `None`, so both of those spellings skip the
/// guard and leave the bindings alone; only an explicit object reaches crud, and
/// an empty one clears. That collapse of `null` into "omitted" is the whole
/// defect, so the stub encodes it rather than papering over it.
/// `initial_routes` is the agent's starting bindings and must be a bound map
/// (e.g. `{}` or `{"deal_desk":{...}}`), not `null`.
fn router_stub(initial_routes: &str) -> MockServer {
    let initial: serde_json::Value =
        serde_json::from_str(initial_routes).expect("the seed bindings must be valid JSON");
    let state: Arc<Mutex<Option<serde_json::Value>>> = Arc::new(Mutex::new(Some(initial)));

    serve(move |req| match req.path.split('?').next().unwrap() {
        "/agents" => {
            let current = render_routes(&state.lock().unwrap());
            Response::json(200, &agents_list(&current))
        }
        "/deployments" => Response::json(200, "[]"),
        p if p.strip_prefix("/agents/") == Some(AGENT_ID) => {
            let body: serde_json::Value =
                serde_json::from_slice(&req.body).expect("PATCH body must be valid JSON");
            let mut current = state.lock().unwrap();
            match body.get("approval_routes") {
                // Key absent: the field was omitted, so the guard is skipped and
                // the bindings are left alone.
                None => {}
                // Explicit null: Pydantic hands the router the SAME `None` an
                // omitted key gives, so the guard is skipped here too and the
                // bindings survive.
                Some(serde_json::Value::Null) => {}
                // An explicit empty object passes the guard and reaches crud,
                // which stores `routes or None`, i.e. NULL.
                Some(serde_json::Value::Object(routes)) if routes.is_empty() => *current = None,
                // Any other explicit value passes the guard and replaces the map.
                Some(routes) => *current = Some(routes.clone()),
            }
            let updated = render_routes(&current);
            Response::json(200, &patched_agent(&updated))
        }
        other => panic!("unexpected request: {other}"),
    })
}

async fn run(server: &MockServer, cmd: ApprovalCmd) -> anyhow::Result<ApprovalsOutput> {
    approvals(
        AgentActionOpts {
            api_url: server.base_url.clone(),
            api_key: TEST_API_KEY.to_string(),
            agent: "deal-desk".to_string(),
            dry_run: false,
        },
        vec![],
        false,
        cmd,
    )
    .await
}

fn patch_body(server: &MockServer) -> serde_json::Value {
    let recorded = server.recorded();
    let patch = recorded
        .iter()
        .find(|r| r.path.starts_with(&format!("/agents/{AGENT_ID}")))
        .expect("the PATCH endpoint must have been called");
    serde_json::from_slice(&patch.body).expect("PATCH body must be valid JSON")
}

/// The payload a `--dry-run` plan promises, parsed back out of its
/// `approval_routes=` clause. Read from the plan the code emitted rather than
/// restated, so the comparison is against the real plan and not a literal.
fn planned_routes_payload(lines: &[String]) -> serde_json::Value {
    let line = lines
        .iter()
        .find(|l| l.contains("approval_routes="))
        .unwrap_or_else(|| panic!("the plan must name the approval_routes payload, got {lines:?}"));
    let rest = line
        .split_once("approval_routes=")
        .expect("the line was chosen for containing it")
        .1;
    let payload = rest.split_once(" (").map_or(rest, |(p, _)| p);
    serde_json::from_str(payload)
        .unwrap_or_else(|err| panic!("the plan's payload must be JSON, got {payload:?}: {err}"))
}

/// The shared assertion for both cleared-routes tests: the output must be the
/// `Routes` variant AND its map must be empty. `context` distinguishes the two
/// call sites' panic messages (flag-driven vs file-driven).
fn assert_routes_cleared(out: ApprovalsOutput, context: &str) {
    match out {
        ApprovalsOutput::Routes { routes, .. } => assert!(
            routes.is_empty(),
            "{context}: the agent still has bindings {routes:?}"
        ),
        ApprovalsOutput::DryRun(_) => panic!("expected the Routes output, not a dry-run plan"),
        ApprovalsOutput::Gates { .. } => panic!("expected the Routes output, not the gate view"),
        ApprovalsOutput::Pending { .. } => {
            panic!("expected the Routes output, not pending approvals")
        }
        ApprovalsOutput::Resolved { .. } => panic!("expected the Routes output, not a resolution"),
        ApprovalsOutput::OperatorPrincipal { .. } => {
            panic!("expected the Routes output, not an operator-principal mint")
        }
        ApprovalsOutput::ConsoleLoginCode { .. } => {
            panic!("expected the Routes output, not a console login-code mint")
        }
    }
}

/// No PATCH reached the server. The assertion for every rejected input: the
/// point is not only that the command failed, but that it failed before writing.
fn assert_no_write(server: &MockServer) {
    let wrote = server
        .recorded()
        .iter()
        .any(|r| r.path.starts_with(&format!("/agents/{AGENT_ID}")));
    assert!(
        !wrote,
        "a rejected invocation must send no PATCH; recorded: {:?}",
        server
            .recorded()
            .iter()
            .map(|r| r.path.clone())
            .collect::<Vec<_>>()
    );
}

/// Run one rejected flag invocation and prove it fails before any PATCH.
async fn rejected(cmd: ApprovalCmd, reason: &str) -> String {
    let server = stub("null", "null");
    let err = match run(&server, cmd).await {
        Ok(_) => panic!("{reason}"),
        Err(err) => err,
    };
    assert_no_write(&server);
    err.to_string()
}

/// Write one strict `--routes-from` input and reuse the same no-PATCH proof.
async fn rejected_routes_file(route: serde_json::Value, reason: &str) -> String {
    rejected_routes_file_with(route, ApprovalCmd::default(), reason).await
}

/// Write one strict file, apply any flag overrides, and prove rejection is
/// still decided from the complete map before a PATCH.
async fn rejected_routes_file_with(
    route: serde_json::Value,
    mut cmd: ApprovalCmd,
    reason: &str,
) -> String {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("routes.json");
    std::fs::write(&path, route.to_string()).expect("write routes file");
    cmd.routes_from = Some(path);
    rejected(cmd, reason).await
}

#[test]
fn old_route_flag_is_unknown() {
    let out = std::process::Command::new(env!("CARGO_BIN_EXE_curie"))
        .args([
            "local",
            "approvals",
            "deal-desk",
            "--route",
            "deal_desk=C0EXAMPLE1",
        ])
        .output()
        .expect("run curie");

    assert_eq!(out.status.code(), Some(2), "retired syntax must be usage");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("unexpected argument '--route'")
            || stderr.contains("unrecognized option '--route'"),
        "the retired flag must not remain an alias or fallback:\n{stderr}"
    );
}

#[test]
fn tolerant_response_binding_cannot_be_reused_as_a_patch_body() {
    let api = include_str!("../src/api.rs");
    assert!(
        api.contains("pub struct ApprovalRouteBindingResponse"),
        "the API read model must be explicitly display-only"
    );
    assert!(
        api.contains("pub struct ApprovalRouteBindingWrite"),
        "the PATCH body needs a separate strict writer"
    );
    let setter = api
        .split("pub async fn set_approval_routes")
        .nth(1)
        .expect("set_approval_routes exists")
        .split('{')
        .next()
        .expect("set_approval_routes signature");
    assert!(
        setter.contains("ApprovalRouteBindingWrite"),
        "set_approval_routes must accept only the strict write DTO: {setter}"
    );
    assert!(
        !setter.contains("ApprovalRouteBindingResponse"),
        "a tolerant/redacted response must never become the PATCH body: {setter}"
    );
    assert!(
        !api.contains("From<ApprovalRouteBindingResponse> for ApprovalRouteBindingWrite"),
        "no conversion may turn the tolerant response into a writer"
    );
}

#[tokio::test]
async fn routes_from_builds_the_strict_split_route_shape() {
    let bound = r#"{"deal_desk":{"resolution":{"kind":"slack","address":"C0EXAMPLE1"},"notification":{"kind":"slack","address":"C0EXAMPLE2"}}}"#;
    let server = stub("null", bound);
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("routes.json");
    std::fs::write(&path, bound).expect("write routes file");

    let out = run(
        &server,
        ApprovalCmd {
            routes_from: Some(path),
            ..ApprovalCmd::default()
        },
    )
    .await
    .expect("a well-formed split route file should succeed");

    let body = patch_body(&server);
    assert_eq!(
        body["approval_routes"]["deal_desk"]["resolution"],
        serde_json::json!({"kind":"slack", "address":"C0EXAMPLE1"}),
        "the PATCH must carry the one interactive target, got {body:?}"
    );
    assert_eq!(
        body["approval_routes"]["deal_desk"]["notification"],
        serde_json::json!({"kind":"slack", "address":"C0EXAMPLE2"}),
        "the PATCH must carry the separate text-only target, got {body:?}"
    );
    // No approvers block was asked for, so none may be sent: an explicit null is
    // a different statement from an omitted key against a model that forbids
    // extras, and it would read as "an approvers block that declares nobody".
    assert!(
        body["approval_routes"]["deal_desk"]
            .get("approvers")
            .is_none(),
        "a route without declared approvers must send no approvers key, got {body:?}"
    );

    match out {
        ApprovalsOutput::Routes { routes, .. } => {
            let binding = &routes["deal_desk"];
            assert_eq!(binding.resolution.address, "C0EXAMPLE1");
            assert_eq!(
                binding
                    .notification
                    .as_ref()
                    .map(|target| target.address.as_str()),
                Some("C0EXAMPLE2")
            );
            assert!(routes["deal_desk"].approvers.is_none());
        }
        ApprovalsOutput::DryRun(_) => panic!("expected the Routes output, not a dry-run plan"),
        ApprovalsOutput::Gates { .. } => panic!("expected the Routes output, not the gate view"),
        ApprovalsOutput::Pending { .. } => {
            panic!("expected the Routes output, not pending approvals")
        }
        ApprovalsOutput::Resolved { .. } => panic!("expected the Routes output, not a resolution"),
        ApprovalsOutput::OperatorPrincipal { .. } => {
            panic!("expected the Routes output, not an operator-principal mint")
        }
        ApprovalsOutput::ConsoleLoginCode { .. } => {
            panic!("expected the Routes output, not a console login-code mint")
        }
    }
}

#[test]
fn guided_channel_validation_accepts_slack_channel_kinds() {
    for channel in ["C0EXAMPLE1", "D0EXAMPLE1", "G0EXAMPLE1"] {
        check_channel_id(channel)
            .unwrap_or_else(|error| panic!("guided validation rejected {channel}: {error}"));
    }
}

#[tokio::test]
async fn direct_and_group_channels_reach_the_route_request() {
    for (channel, bound) in [
        (
            "C0EXAMPLE1",
            r#"{"team":{"resolution":{"kind":"slack","address":"C0EXAMPLE1"}}}"#,
        ),
        (
            "D0EXAMPLE1",
            r#"{"team":{"resolution":{"kind":"slack","address":"D0EXAMPLE1"}}}"#,
        ),
        (
            "G0EXAMPLE1",
            r#"{"team":{"resolution":{"kind":"slack","address":"G0EXAMPLE1"}}}"#,
        ),
    ] {
        let server = stub("null", bound);
        run(
            &server,
            ApprovalCmd {
                route_resolution: vec![format!("team={channel}")],
                ..ApprovalCmd::default()
            },
        )
        .await
        .unwrap_or_else(|error| panic!("route validation rejected {channel}: {error}"));

        let body = patch_body(&server);
        assert_eq!(
            body["approval_routes"]["team"]["resolution"]["address"],
            channel
        );
    }
}

#[tokio::test]
async fn route_approvers_narrows_who_without_moving_where() {
    // The ADR-0034 property, asserted on the wire: resolution and `approvers` are
    // independent axes, so narrowing WHO must leave WHERE untouched and must not
    // collapse the two into one field.
    let bound = r#"{"finance":{"resolution":{"kind":"slack","address":"C0EXAMPLE3"},"approvers":{"group":"S0FINGRP0"}}}"#;
    let server = stub("null", bound);

    let out = run(
        &server,
        ApprovalCmd {
            route_resolution: vec!["finance=C0EXAMPLE3".to_string()],
            route_approvers: vec!["finance=group:S0FINGRP0".to_string()],
            ..ApprovalCmd::default()
        },
    )
    .await
    .expect("a well-formed group binding should succeed");

    let body = patch_body(&server);
    assert_eq!(
        body["approval_routes"]["finance"]["resolution"]["address"],
        "C0EXAMPLE3"
    );
    assert_eq!(
        body["approval_routes"]["finance"]["approvers"]["group"],
        "S0FINGRP0"
    );
    assert!(
        body["approval_routes"]["finance"]["approvers"]
            .get("users")
            .is_none(),
        "a group binding must not also send a users key, got {body:?}"
    );

    match out {
        ApprovalsOutput::Routes { routes, .. } => {
            let binding = &routes["finance"];
            assert_eq!(binding.resolution.address, "C0EXAMPLE3");
            assert_eq!(
                binding.approvers.as_ref().and_then(|a| a.group.as_deref()),
                Some("S0FINGRP0")
            );
        }
        ApprovalsOutput::DryRun(_) => panic!("expected the Routes output, not a dry-run plan"),
        ApprovalsOutput::Gates { .. } => panic!("expected the Routes output, not the gate view"),
        ApprovalsOutput::Pending { .. } => {
            panic!("expected the Routes output, not pending approvals")
        }
        ApprovalsOutput::Resolved { .. } => panic!("expected the Routes output, not a resolution"),
        ApprovalsOutput::OperatorPrincipal { .. } => {
            panic!("expected the Routes output, not an operator-principal mint")
        }
        ApprovalsOutput::ConsoleLoginCode { .. } => {
            panic!("expected the Routes output, not a console login-code mint")
        }
    }
}

#[tokio::test]
async fn route_approvers_users_forwards_the_whole_list() {
    let bound = r#"{"cro_cfo":{"resolution":{"kind":"slack","address":"C0EXAMPLE4"},"approvers":{"users":["U0CRO00000","W0CFO00000"]}}}"#;
    let server = stub("null", bound);

    run(
        &server,
        ApprovalCmd {
            route_resolution: vec!["cro_cfo=C0EXAMPLE4".to_string()],
            // A W-prefixed id is a valid enterprise-grid user, so the shape check
            // must accept it rather than assuming every user id starts with U.
            route_approvers: vec!["cro_cfo=users:U0CRO00000, W0CFO00000".to_string()],
            ..ApprovalCmd::default()
        },
    )
    .await
    .expect("a well-formed user-list binding should succeed");

    let body = patch_body(&server);
    assert_eq!(
        body["approval_routes"]["cro_cfo"]["approvers"]["users"],
        serde_json::json!(["U0CRO00000", "W0CFO00000"]),
        "both user ids must reach the wire, whitespace trimmed, got {body:?}"
    );
}

#[tokio::test]
async fn list_routes_reads_without_writing() {
    let bound = r#"{"deal_desk":{"resolution":{"kind":"slack","address":"C0EXAMPLE1"}}}"#;
    let server = stub(bound, bound);

    let out = run(
        &server,
        ApprovalCmd {
            list_routes: true,
            ..ApprovalCmd::default()
        },
    )
    .await
    .expect("--list-routes should succeed");

    assert_no_write(&server);
    match out {
        ApprovalsOutput::Routes { agent, routes } => {
            assert_eq!(agent, "deal-desk");
            assert_eq!(routes["deal_desk"].resolution.address, "C0EXAMPLE1");
        }
        ApprovalsOutput::DryRun(_) => panic!("expected the Routes output, not a dry-run plan"),
        ApprovalsOutput::Gates { .. } => panic!("expected the Routes output, not the gate view"),
        ApprovalsOutput::Pending { .. } => {
            panic!("expected the Routes output, not pending approvals")
        }
        ApprovalsOutput::Resolved { .. } => panic!("expected the Routes output, not a resolution"),
        ApprovalsOutput::OperatorPrincipal { .. } => {
            panic!("expected the Routes output, not an operator-principal mint")
        }
        ApprovalsOutput::ConsoleLoginCode { .. } => {
            panic!("expected the Routes output, not a console login-code mint")
        }
    }
}

#[tokio::test]
async fn list_routes_json_preserves_an_empty_resolution_address() {
    // Response decoding is tolerant enough to report a malformed stored target
    // without rewriting it into a plausible channel.
    let routes = r#"{"intentionally_empty":{"resolution":{"kind":"slack","address":""}}}"#;
    let server = stub(routes, routes);

    let out = run(
        &server,
        ApprovalCmd {
            list_routes: true,
            ..ApprovalCmd::default()
        },
    )
    .await
    .expect("--list-routes should preserve an empty stored address");

    assert_no_write(&server);
    let json = out.to_json();
    assert_eq!(
        json["routes"]["intentionally_empty"]["resolution"]["address"], "",
        "an explicit empty API address must remain an empty string in list-routes JSON"
    );
}

#[tokio::test]
async fn clear_routes_sends_an_empty_object_not_null() {
    // The router guards with `if data.approval_routes is not None`, and Pydantic
    // decodes an explicit JSON null and an omitted key to the same `None`, so
    // both spellings mean "leave the bindings alone" and the clear never runs.
    // An empty object is the only spelling that passes the guard and reaches
    // `crud.update_agent_approval_routes`, whose `routes or None` is a STORAGE
    // normalization applied after the guard, not the wire contract (#1071).
    let server = stub(
        r#"{"deal_desk":{"resolution":{"kind":"slack","address":"C0EXAMPLE1"}}}"#,
        "null",
    );

    run(
        &server,
        ApprovalCmd {
            clear_routes: true,
            ..ApprovalCmd::default()
        },
    )
    .await
    .expect("--clear-routes should succeed");

    let body = patch_body(&server);
    let routes = body.get("approval_routes").unwrap_or_else(|| {
        panic!("clear must send the approval_routes key; an omitted key reads as \"leave the bindings alone\", got {body:?}")
    });
    assert_eq!(
        routes,
        &serde_json::json!({}),
        "clear must send an empty object, got {body:?}"
    );
}

#[tokio::test]
async fn clear_routes_actually_clears_the_bindings() {
    // The effect, not the bytes: drive the clear against a stub that applies the
    // router's guard, then read what it hands back. A spelling the guard skips
    // leaves the binding in place, so the operator is told "updated" while the
    // approver set they meant to revoke is still live.
    let server =
        router_stub(r#"{"deal_desk":{"resolution":{"kind":"slack","address":"C0EXAMPLE1"}}}"#);

    let out = run(
        &server,
        ApprovalCmd {
            clear_routes: true,
            ..ApprovalCmd::default()
        },
    )
    .await
    .expect("--clear-routes should succeed");

    assert_routes_cleared(out, "the clear did not take");
}

#[tokio::test]
async fn a_routes_file_holding_an_empty_map_clears_the_bindings() {
    // The second entry point. `--clear-routes` is refused alongside
    // `--routes-from`, so a `{}` file reaches the empty map through
    // `build_route_bindings` rather than through the flag's direct empty map.
    // Both must land on the same wire spelling.
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("routes.json");
    std::fs::write(&path, "{}").expect("write routes file");

    let server =
        router_stub(r#"{"deal_desk":{"resolution":{"kind":"slack","address":"C0EXAMPLE1"}}}"#);

    let out = run(
        &server,
        ApprovalCmd {
            routes_from: Some(path),
            ..ApprovalCmd::default()
        },
    )
    .await
    .expect("a routes file holding an empty map should succeed");

    assert_routes_cleared(out, "the file-driven clear did not take");
}

#[tokio::test]
async fn the_dry_run_plan_names_the_payload_the_real_clear_sends() {
    // A plan an operator reads before running the real thing is only worth
    // reading if it names the same request. Both halves are read back from what
    // the code produced; comparing two literals would prove nothing.
    let planned = {
        let server = stub(
            r#"{"deal_desk":{"resolution":{"kind":"slack","address":"C0EXAMPLE1"}}}"#,
            "null",
        );
        let out = approvals(
            AgentActionOpts {
                api_url: server.base_url.clone(),
                api_key: TEST_API_KEY.to_string(),
                agent: "deal-desk".to_string(),
                dry_run: true,
            },
            vec![],
            false,
            ApprovalCmd {
                clear_routes: true,
                ..ApprovalCmd::default()
            },
        )
        .await
        .expect("--clear-routes --dry-run should succeed");

        assert_no_write(&server);
        match out {
            ApprovalsOutput::DryRun(plan) => planned_routes_payload(&plan.lines),
            ApprovalsOutput::Gates { .. } => {
                panic!("expected the DryRun output, not the gate view")
            }
            ApprovalsOutput::Pending { .. } => {
                panic!("expected the DryRun output, not pending approvals")
            }
            ApprovalsOutput::Resolved { .. } => {
                panic!("expected the DryRun output, not a resolution")
            }
            ApprovalsOutput::Routes { .. } => {
                panic!("expected the DryRun output, not route bindings")
            }
            ApprovalsOutput::OperatorPrincipal { .. } => {
                panic!("expected the DryRun output, not an operator-principal mint")
            }
            ApprovalsOutput::ConsoleLoginCode { .. } => {
                panic!("expected the DryRun output, not a console login-code mint")
            }
        }
    };

    let server = stub(
        r#"{"deal_desk":{"resolution":{"kind":"slack","address":"C0EXAMPLE1"}}}"#,
        "null",
    );
    run(
        &server,
        ApprovalCmd {
            clear_routes: true,
            ..ApprovalCmd::default()
        },
    )
    .await
    .expect("--clear-routes should succeed");
    let sent = patch_body(&server)["approval_routes"].clone();

    assert_eq!(
        planned, sent,
        "the plan promised approval_routes={planned} but the request sent {sent}"
    );
}

#[tokio::test]
async fn dry_run_redacts_notification_endpoint_path_query_and_fragment() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("routes.json");
    std::fs::write(
        &path,
        serde_json::json!({
            "finance": {
                "resolution": {"kind": "slack", "address": "C0EXAMPLE3"},
                "notification": {
                    "kind": "email",
                    "address": "approvals@example.com",
                    "endpoint": "https://adapter.example.com/private/replies?token=secret#credential",
                    "adapter": "mail"
                }
            }
        })
        .to_string(),
    )
    .expect("write routes file");
    let server = stub("null", "null");

    let out = approvals(
        AgentActionOpts {
            api_url: server.base_url.clone(),
            api_key: TEST_API_KEY.to_string(),
            agent: "deal-desk".to_string(),
            dry_run: true,
        },
        vec![],
        false,
        ApprovalCmd {
            routes_from: Some(path),
            ..ApprovalCmd::default()
        },
    )
    .await
    .expect("a valid notification route should produce a dry-run plan");

    assert_no_write(&server);
    let ApprovalsOutput::DryRun(plan) = out else {
        panic!("expected the DryRun output");
    };
    let planned = planned_routes_payload(&plan.lines);
    let notification = &planned["finance"]["notification"];
    assert_eq!(notification["endpoint"], "https://adapter.example.com");
    assert_eq!(notification["kind"], "email");
    assert_eq!(notification["address"], "approvals@example.com");
    assert_eq!(notification["adapter"], "mail");
    let rendered = planned.to_string();
    for secret_detail in ["private/replies", "token=secret", "credential"] {
        assert!(
            !rendered.contains(secret_detail),
            "dry-run output leaked endpoint detail {secret_detail:?}: {rendered}"
        );
    }
}

#[tokio::test]
async fn a_malformed_channel_writes_nothing() {
    let server = stub("null", "null");

    let Err(err) = run(
        &server,
        ApprovalCmd {
            // The second route is well-formed; the first is not. Neither may be
            // written, or the resulting map is one an operator never asked for.
            route_resolution: vec![
                "deal_desk=#managers".to_string(),
                "finance=C0EXAMPLE3".to_string(),
            ],
            ..ApprovalCmd::default()
        },
    )
    .await
    else {
        panic!("a #name channel must be refused");
    };

    assert!(
        err.to_string().contains("not a Slack channel ID"),
        "unexpected error: {err}"
    );
    assert_no_write(&server);
}

#[tokio::test]
async fn a_channel_id_where_a_usergroup_belongs_writes_nothing() {
    let server = stub("null", "null");

    let Err(err) = run(
        &server,
        ApprovalCmd {
            route_resolution: vec!["finance=C0EXAMPLE3".to_string()],
            route_approvers: vec!["finance=group:C0EXAMPLE3".to_string()],
            ..ApprovalCmd::default()
        },
    )
    .await
    else {
        panic!("a C-prefixed id is a channel, not a user group");
    };

    assert!(
        err.to_string().contains("not a Slack user-group ID"),
        "unexpected error: {err}"
    );
    assert_no_write(&server);
}

#[tokio::test]
async fn approvers_without_a_resolution_writes_nothing() {
    // A write replaces the whole map, so narrowing a route the invocation never
    // gives a channel would produce a binding with nowhere to post.
    let server = stub("null", "null");

    let Err(err) = run(
        &server,
        ApprovalCmd {
            route_approvers: vec!["finance=group:S0FINGRP0".to_string()],
            ..ApprovalCmd::default()
        },
    )
    .await
    else {
        panic!("approvers with no resolution must be refused");
    };

    assert!(
        err.to_string().contains("no resolution"),
        "unexpected error: {err}"
    );
    assert_no_write(&server);
}

#[tokio::test]
async fn route_flags_cannot_be_mixed_with_gate_or_record_flags() {
    // Three different objects behind one verb (tool gates, pending records, route
    // bindings). Mixing them in one invocation makes the write's
    // replace-the-whole-map semantics ambiguous, so it is refused, not guessed.
    let server = stub("null", "null");

    let Err(err) = run(
        &server,
        ApprovalCmd {
            route_resolution: vec!["finance=C0EXAMPLE3".to_string()],
            list: true,
            ..ApprovalCmd::default()
        },
    )
    .await
    else {
        panic!("route flags and --list address different objects");
    };

    assert!(
        err.to_string().contains("cannot be combined"),
        "unexpected error: {err}"
    );
    assert_no_write(&server);
}

#[tokio::test]
async fn routes_from_seeds_the_map_and_flags_override_it() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("routes.json");
    std::fs::write(
        &path,
        r#"{"deal_desk":{"resolution":{"kind":"slack","address":"C0EXAMPLE5"}},"finance":{"resolution":{"kind":"slack","address":"C0EXAMPLE3"},"approvers":{"group":"S0FINGRP0"}}}"#,
    )
    .expect("write routes file");

    let bound = r#"{"deal_desk":{"resolution":{"kind":"slack","address":"C0EXAMPLE1"}},"finance":{"resolution":{"kind":"slack","address":"C0EXAMPLE3"},"approvers":{"group":"S0FINGRP0"}}}"#;
    let server = stub("null", bound);

    run(
        &server,
        ApprovalCmd {
            routes_from: Some(path),
            route_resolution: vec!["deal_desk=C0EXAMPLE1".to_string()],
            ..ApprovalCmd::default()
        },
    )
    .await
    .expect("a committed file plus a command-line override should succeed");

    let body = patch_body(&server);
    assert_eq!(
        body["approval_routes"]["deal_desk"]["resolution"]["address"], "C0EXAMPLE1",
        "the flag must win over the file's value, got {body:?}"
    );
    assert_eq!(
        body["approval_routes"]["finance"]["approvers"]["group"], "S0FINGRP0",
        "a route only the file names must survive, got {body:?}"
    );
}

#[tokio::test]
async fn routes_from_preserves_non_slack_notification_transport_in_the_write_dto() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("routes.json");
    let route = serde_json::json!({
        "finance": {
            "resolution": {"kind": "slack", "address": "C0EXAMPLE3"},
            "notification": {
                "kind": "email",
                "address": "approvals@example.com",
                "endpoint": "https://adapter.example.com/replies",
                "adapter": "mail"
            }
        }
    });
    std::fs::write(&path, route.to_string()).expect("write");
    let leaked_echo = route.to_string();
    let leaked_echo: &'static str = Box::leak(leaked_echo.into_boxed_str());
    let server = stub("null", leaked_echo);

    let out = run(
        &server,
        ApprovalCmd {
            routes_from: Some(path),
            ..ApprovalCmd::default()
        },
    )
    .await
    .expect("a complete adapter notification route should succeed");

    let body = patch_body(&server);
    assert_eq!(
        body["approval_routes"]["finance"]["notification"], route["finance"]["notification"],
        "the strict PATCH DTO must retain both adapter credentials"
    );
    let json = out.to_json();
    assert!(
        json["routes"]["finance"]["notification"]
            .get("endpoint")
            .is_none(),
        "the tolerant display DTO must redact notification transport: {json}"
    );
    assert!(
        json["routes"]["finance"]["notification"]
            .get("adapter")
            .is_none(),
        "the tolerant display DTO must redact adapter identity: {json}"
    );
}

#[tokio::test]
async fn routes_from_rejections_fail_before_write() {
    // #1072: the typo that matters. `approver` (for `approvers`) used to be
    // silently stripped, and the write that landed was a channel-only binding --
    // which falls back to card-channel membership, so an operator who meant to
    // narrow authority to one group had instead granted it to everyone in the
    // channel. The API guards this with `extra="forbid"`; #1057 made the CLI a
    // second writer that re-serialized a parsed struct, so the operator's bytes
    // never reached that guard. The sibling typo inside `approvers` must fail at
    // the same strict boundary.
    for (route, reason, needles) in [
        (
            serde_json::json!({"finance": {"channel": "C0EXAMPLE3"}}),
            "the retired fused shape must be refused, not migrated at runtime",
            &["channel", "finance"][..],
        ),
        (
            serde_json::json!({
                "finance": {
                    "resolution": {"kind": "slack", "address": "C0EXAMPLE3"},
                    "notification": {"kind": "email", "address": "approvals@example.com"}
                }
            }),
            "a non-Slack notification without transport must be refused",
            &["endpoint", "adapter"][..],
        ),
        (
            serde_json::json!({
                "finance": {
                    "resolution": {"kind": "slack", "address": "C0EXAMPLE3"},
                    "notification": {"kind": "email", "address": "approvals@example.com", "endpoint": "https://adapter.example.com/replies"}
                }
            }),
            "an endpoint without an adapter must be refused",
            &["endpoint", "adapter"][..],
        ),
        (
            serde_json::json!({
                "finance": {
                    "resolution": {"kind": "slack", "address": "C0EXAMPLE3"},
                    "notification": {"kind": "email", "address": "approvals@example.com", "adapter": "mail"}
                }
            }),
            "an adapter without an endpoint must be refused",
            &["endpoint", "adapter"][..],
        ),
        (
            serde_json::json!({
                "finance": {
                    "resolution": {"kind": "email", "address": "approvals@example.com"}
                }
            }),
            "a non-Slack resolution must be refused",
            &["slack"][..],
        ),
        (
            serde_json::json!({
                "finance": {
                    "resolution": {"kind": "slack", "address": "C0EXAMPLE3"},
                    "notification": {"kind": "slack", "address": "C0EXAMPLE2", "interactive": true}
                }
            }),
            "an unknown notification key must be refused",
            &["interactive"][..],
        ),
        (
            serde_json::json!({
                "deal_desk": {
                    "resolution": {"kind": "slack", "address": "C0EXAMPLE6"},
                    "approver": {"group": "S0DESKGRP"}
                }
            }),
            "an unknown key at the binding level must be refused, not stripped",
            &["approver", "deal_desk"][..],
        ),
        (
            serde_json::json!({
                "finance": {
                    "resolution": {"kind": "slack", "address": "C0EXAMPLE3"},
                    "approvers": {"grup": "S0FINGRP0"}
                }
            }),
            "an unknown key inside approvers must be refused",
            &["grup"][..],
        ),
        (
            serde_json::json!({
                "finance": {
                    "resolution": {"kind": "slack", "address": "finance-room"}
                }
            }),
            "a bad channel in the file must be refused",
            &["not a Slack channel ID"][..],
        ),
        (
            serde_json::json!({"legacy": {}}),
            "a binding without resolution must be refused",
            &["resolution"][..],
        ),
    ] {
        let message = rejected_routes_file(route, reason).await;
        let lowercase = message.to_lowercase();
        assert!(
            needles
                .iter()
                .all(|needle| lowercase.contains(&needle.to_lowercase())),
            "{reason}; expected {needles:?} in {message:?}"
        );
    }
}

#[tokio::test]
async fn resolution_override_cannot_duplicate_file_notification() {
    let message = rejected_routes_file_with(
        serde_json::json!({
            "finance": {
                "resolution": {"kind": "slack", "address": "C0EXAMPLE3"},
                "notification": {"kind": "slack", "address": "C0EXAMPLE4"}
            }
        }),
        ApprovalCmd {
            route_resolution: vec!["finance=C0EXAMPLE4".to_string()],
            ..ApprovalCmd::default()
        },
        "the final binding map must be revalidated after flag overrides",
    )
    .await;
    assert!(
        message.contains("must differ from resolution"),
        "the override-created duplicate must fail closed: {message}"
    );
}

#[tokio::test]
async fn an_api_response_tolerates_a_field_the_cli_does_not_model() {
    // The other half of the #1072 asymmetry, and the reason the fix is a second
    // struct rather than `deny_unknown_fields` on the existing one: the RESPONSE
    // side must keep decoding a binding a newer server has added a field to,
    // or an older CLI breaks against a newer platform.
    let bound = r#"{"deal_desk":{"resolution":{"kind":"slack","address":"C0EXAMPLE1"},"notification":{"kind":"email","address":"approvals@example.com"},"escalation_after_s":3600}}"#;
    let server = stub(bound, bound);

    let out = run(
        &server,
        ApprovalCmd {
            list_routes: true,
            ..ApprovalCmd::default()
        },
    )
    .await
    .expect("an unmodeled server field must not break the read");

    match out {
        ApprovalsOutput::Routes { routes, .. } => {
            let json = serde_json::to_value(&routes["deal_desk"]).unwrap();
            assert_eq!(json["resolution"]["address"], "C0EXAMPLE1");
            assert_eq!(json["notification"]["address"], "approvals@example.com");
            assert!(json["notification"].get("endpoint").is_none());
            assert!(json["notification"].get("adapter").is_none());
        }
        ApprovalsOutput::DryRun(_) => panic!("expected the Routes output, not a dry-run plan"),
        ApprovalsOutput::Gates { .. } => panic!("expected the Routes output, not the gate view"),
        ApprovalsOutput::Pending { .. } => {
            panic!("expected the Routes output, not pending approvals")
        }
        ApprovalsOutput::Resolved { .. } => panic!("expected the Routes output, not a resolution"),
        ApprovalsOutput::OperatorPrincipal { .. } => {
            panic!("expected the Routes output, not an operator-principal mint")
        }
        ApprovalsOutput::ConsoleLoginCode { .. } => {
            panic!("expected the Routes output, not a console login-code mint")
        }
    }
}

#[tokio::test]
async fn a_malformed_routes_file_writes_nothing() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("routes.json");
    // Valid JSON, invalid binding: the file path must be validated with the same
    // rules the flag path uses, or the two input forms disagree about what a
    // legal binding is.
    std::fs::write(&path, r#"{"finance":{"channel":"finance-room"}}"#).expect("write");

    let server = stub("null", "null");

    let Err(err) = run(
        &server,
        ApprovalCmd {
            routes_from: Some(path),
            ..ApprovalCmd::default()
        },
    )
    .await
    else {
        panic!("a bad channel in the file must be refused");
    };

    assert!(
        err.to_string().contains("unknown field `channel`"),
        "unexpected error: {err}"
    );
    assert_no_write(&server);
}

#[tokio::test]
async fn a_routes_file_without_a_channel_writes_nothing() {
    // API responses preserve a missing channel for diagnosis, but an operator
    // supplied route must name where its approval card is posted.
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("routes.json");
    std::fs::write(&path, r#"{"legacy":{}}"#).expect("write");

    let server = stub("null", "null");

    let Err(err) = run(
        &server,
        ApprovalCmd {
            routes_from: Some(path),
            ..ApprovalCmd::default()
        },
    )
    .await
    else {
        panic!("a route file binding without a channel must be refused");
    };

    assert!(
        err.to_string().contains("resolution"),
        "the error must identify the required field: {err}"
    );
    assert_no_write(&server);
}
