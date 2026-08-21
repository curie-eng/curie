//! Integration: `<tier> approvals <agent>` writes and reads the agent's
//! approval ROUTE bindings (#1052).
//!
//! Until this verb existed, binding a route to a channel was a hand-written
//! `PATCH /agents/{id}`, against the repo's own "one entry point: `curie
//! <command>`" rule, and neither the CLI nor the console could do it.
//!
//! Driven through the real `commands::approvals` handler and a real `ApiClient`
//! against a wire-level stub, mirroring `approvals_resolve_actor_channel.rs`.
//! The regressions worth catching only show up in the actual outgoing PATCH
//! body, or in the ABSENCE of one:
//!
//! - a write must send the whole map (the field replaces, it does not merge);
//! - `channel` and `approvers` must stay separate on the wire, since ADR-0034
//!   exists to keep WHERE a card posts unfused from WHO may resolve it;
//! - and a malformed input must send NOTHING. A half-written binding map is a
//!   silently widened or narrowed approver set, which is exactly the failure the
//!   approval gate exists to prevent, so validation runs before the connection
//!   is opened and the negative tests assert on `recorded()` being empty.

mod support;

use curie::commands::{approvals, AgentActionOpts, ApprovalCmd, ApprovalsOutput};
use curie::credcheck::check_channel_id;
use std::sync::{Arc, Mutex};
use support::{serve, MockServer, Response};

const TEST_API_KEY: &str = "test-key";
const AGENT_ID: &str = "ag_1";

/// `GET /agents` payload: the handler resolves the agent by name first.
fn agents_list(routes_json: &str) -> String {
    format!(
        r#"[{{"id":"{AGENT_ID}","name":"deal-desk","channels":[{{"kind":"slack","address":"C0INTAKE00"}}],"approval_required_tools":null,"approval_routes":{routes_json}}}]"#
    )
}

/// `PATCH /agents/{id}` echo: the updated agent the CLI renders from.
fn patched_agent(routes_json: &str) -> String {
    format!(
        r#"{{"id":"{AGENT_ID}","name":"deal-desk","channels":[{{"kind":"slack","address":"C0INTAKE00"}}],"approval_required_tools":null,"approval_routes":{routes_json}}}"#
    )
}

/// A stub answering the two endpoints this verb touches.
fn stub(list_routes: &'static str, patch_echo: &'static str) -> MockServer {
    serve(move |req| match req.path.split('?').next().unwrap() {
        "/agents" => Response::json(200, &agents_list(list_routes)),
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
        _ => panic!("expected the Routes output"),
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

#[tokio::test]
async fn route_flag_writes_the_channel_binding() {
    let bound = r#"{"deal_desk":{"channel":"C0MANAGERS"}}"#;
    let server = stub("null", bound);

    let out = run(
        &server,
        ApprovalCmd {
            route: vec!["deal_desk=C0MANAGERS".to_string()],
            ..ApprovalCmd::default()
        },
    )
    .await
    .expect("a well-formed --route should succeed");

    let body = patch_body(&server);
    assert_eq!(
        body["approval_routes"]["deal_desk"]["channel"], "C0MANAGERS",
        "the PATCH must carry the binding, got {body:?}"
    );
    // No approvers block was asked for, so none may be sent: an explicit null is
    // a different statement from an omitted key against a model that forbids
    // extras, and it would read as "an approvers block that declares nobody".
    assert!(
        body["approval_routes"]["deal_desk"]
            .get("approvers")
            .is_none(),
        "a channel-only write must send no approvers key, got {body:?}"
    );

    match out {
        ApprovalsOutput::Routes { routes, .. } => {
            assert_eq!(routes["deal_desk"].channel, "C0MANAGERS");
            assert!(routes["deal_desk"].approvers.is_none());
        }
        _ => panic!("expected the Routes output"),
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
        ("C0EXAMPLE1", r#"{"team":{"channel":"C0EXAMPLE1"}}"#),
        ("D0EXAMPLE1", r#"{"team":{"channel":"D0EXAMPLE1"}}"#),
        ("G0EXAMPLE1", r#"{"team":{"channel":"G0EXAMPLE1"}}"#),
    ] {
        let server = stub("null", bound);
        run(
            &server,
            ApprovalCmd {
                route: vec![format!("team={channel}")],
                ..ApprovalCmd::default()
            },
        )
        .await
        .unwrap_or_else(|error| panic!("route validation rejected {channel}: {error}"));

        let body = patch_body(&server);
        assert_eq!(body["approval_routes"]["team"]["channel"], channel);
    }
}

#[tokio::test]
async fn route_approvers_narrows_who_without_moving_where() {
    // The ADR-0034 property, asserted on the wire: `channel` and `approvers` are
    // independent axes, so narrowing WHO must leave WHERE untouched and must not
    // collapse the two into one field.
    let bound = r#"{"finance":{"channel":"C0FINANCE0","approvers":{"group":"S0FINGRP0"}}}"#;
    let server = stub("null", bound);

    let out = run(
        &server,
        ApprovalCmd {
            route: vec!["finance=C0FINANCE0".to_string()],
            route_approvers: vec!["finance=group:S0FINGRP0".to_string()],
            ..ApprovalCmd::default()
        },
    )
    .await
    .expect("a well-formed group binding should succeed");

    let body = patch_body(&server);
    assert_eq!(body["approval_routes"]["finance"]["channel"], "C0FINANCE0");
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
            assert_eq!(binding.channel, "C0FINANCE0");
            assert_eq!(
                binding.approvers.as_ref().and_then(|a| a.group.as_deref()),
                Some("S0FINGRP0")
            );
        }
        _ => panic!("expected the Routes output"),
    }
}

#[tokio::test]
async fn route_approvers_users_forwards_the_whole_list() {
    let bound =
        r#"{"cro_cfo":{"channel":"C0EXEC0000","approvers":{"users":["U0CRO00000","W0CFO00000"]}}}"#;
    let server = stub("null", bound);

    run(
        &server,
        ApprovalCmd {
            route: vec!["cro_cfo=C0EXEC0000".to_string()],
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
    let bound = r#"{"deal_desk":{"channel":"C0MANAGERS"}}"#;
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
            assert_eq!(routes["deal_desk"].channel, "C0MANAGERS");
        }
        _ => panic!("expected the Routes output"),
    }
}

#[tokio::test]
async fn clear_routes_sends_an_empty_object_not_null() {
    // The router guards with `if data.approval_routes is not None`, and Pydantic
    // decodes an explicit JSON null and an omitted key to the same `None`, so
    // both spellings mean "leave the bindings alone" and the clear never runs.
    // An empty object is the only spelling that passes the guard and reaches
    // `crud.update_agent_approval_routes`, whose `routes or None` is a STORAGE
    // normalization applied after the guard, not the wire contract (#1071).
    let server = stub(r#"{"deal_desk":{"channel":"C0MANAGERS"}}"#, "null");

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
    let server = router_stub(r#"{"deal_desk":{"channel":"C0MANAGERS"}}"#);

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

    let server = router_stub(r#"{"deal_desk":{"channel":"C0MANAGERS"}}"#);

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
        let server = stub(r#"{"deal_desk":{"channel":"C0MANAGERS"}}"#, "null");
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
            _ => panic!("expected the DryRun output"),
        }
    };

    let server = stub(r#"{"deal_desk":{"channel":"C0MANAGERS"}}"#, "null");
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
async fn a_malformed_channel_writes_nothing() {
    let server = stub("null", "null");

    let Err(err) = run(
        &server,
        ApprovalCmd {
            // The second route is well-formed; the first is not. Neither may be
            // written, or the resulting map is one an operator never asked for.
            route: vec![
                "deal_desk=#managers".to_string(),
                "finance=C0FINANCE0".to_string(),
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
            route: vec!["finance=C0FINANCE0".to_string()],
            route_approvers: vec!["finance=group:C0FINANCE0".to_string()],
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
async fn approvers_without_a_channel_writes_nothing() {
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
        panic!("approvers with no channel must be refused");
    };

    assert!(
        err.to_string().contains("no channel"),
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
            route: vec!["finance=C0FINANCE0".to_string()],
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
        r#"{"deal_desk":{"channel":"C0OLDROOM0"},"finance":{"channel":"C0FINANCE0","approvers":{"group":"S0FINGRP0"}}}"#,
    )
    .expect("write routes file");

    let bound = r#"{"deal_desk":{"channel":"C0MANAGERS"},"finance":{"channel":"C0FINANCE0","approvers":{"group":"S0FINGRP0"}}}"#;
    let server = stub("null", bound);

    run(
        &server,
        ApprovalCmd {
            routes_from: Some(path),
            route: vec!["deal_desk=C0MANAGERS".to_string()],
            ..ApprovalCmd::default()
        },
    )
    .await
    .expect("a committed file plus a command-line override should succeed");

    let body = patch_body(&server);
    assert_eq!(
        body["approval_routes"]["deal_desk"]["channel"], "C0MANAGERS",
        "the flag must win over the file's value, got {body:?}"
    );
    assert_eq!(
        body["approval_routes"]["finance"]["approvers"]["group"], "S0FINGRP0",
        "a route only the file names must survive, got {body:?}"
    );
}

#[tokio::test]
async fn a_routes_file_with_an_unknown_binding_key_writes_nothing() {
    // #1072: the typo that matters. `approver` (for `approvers`) used to be
    // silently stripped, and the write that landed was a channel-only binding --
    // which falls back to card-channel membership, so an operator who meant to
    // narrow authority to one group had instead granted it to everyone in the
    // channel. The API guards this with `extra="forbid"`; #1057 made the CLI a
    // second writer that re-serialized a parsed struct, so the operator's bytes
    // never reached that guard.
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("routes.json");
    std::fs::write(
        &path,
        r#"{"deal_desk":{"channel":"C0GENERAL0","approver":{"group":"S0DESKGRP"}}}"#,
    )
    .expect("write");

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
        panic!("an unknown key at the binding level must be refused, not stripped");
    };

    let msg = err.to_string();
    assert!(
        msg.contains("approver"),
        "the error must name the key: {msg}"
    );
    assert!(
        msg.contains("deal_desk"),
        "the error must name the route carrying it: {msg}"
    );
    assert_no_write(&server);
}

#[tokio::test]
async fn a_routes_file_with_an_unknown_approvers_key_writes_nothing() {
    // The sibling case one level down. Already refused before #1072 (the
    // approvers block was validated when present), and it must stay refused --
    // the fix moved where parsing happens, so this pins that it did not regress.
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("routes.json");
    std::fs::write(
        &path,
        r#"{"finance":{"channel":"C0FINANCE0","approvers":{"grup":"S0FINGRP0"}}}"#,
    )
    .expect("write");

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
        panic!("an unknown key inside approvers must be refused");
    };

    assert!(err.to_string().contains("grup"), "unexpected error: {err}");
    assert_no_write(&server);
}

#[tokio::test]
async fn an_api_response_tolerates_a_field_the_cli_does_not_model() {
    // The other half of the #1072 asymmetry, and the reason the fix is a second
    // struct rather than `deny_unknown_fields` on the existing one: the RESPONSE
    // side must keep decoding a binding a newer server has added a field to,
    // or an older CLI breaks against a newer platform.
    let bound = r#"{"deal_desk":{"channel":"C0MANAGERS","escalation_after_s":3600}}"#;
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
            assert_eq!(routes["deal_desk"].channel, "C0MANAGERS");
        }
        _ => panic!("expected the Routes output"),
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
        err.to_string().contains("not a Slack channel ID"),
        "unexpected error: {err}"
    );
    assert_no_write(&server);
}
