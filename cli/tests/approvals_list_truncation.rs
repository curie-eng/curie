//! Integration: `<tier> approvals <agent> --list` must request the server's max
//! page size and surface an explicit truncation signal when the result hits
//! that cap, so a `--json` consumer can tell a complete pending list from a
//! possibly-truncated one (#670).
//!
//! Driven through the real `approvals` handler and a real `ApiClient` against a
//! wire-level stub: the regression this guards is "the CLI sends no `limit`
//! and silently renders a capped 50-row page as the complete list," which only
//! shows up by inspecting the actual outgoing request path, not by
//! hand-constructing `ApprovalsOutput::Pending`.

mod support;

use curie::api::ApiClient;
use curie::commands::{approvals, AgentActionOpts, ApprovalCmd, ApprovalsOutput};
use curie::ui::CliOutput;
use support::{serve, MockServer, Response};

const AGENT_ID: &str = "11111111-1111-1111-1111-111111111111";
const TEST_API_KEY: &str = "test-key";

fn agents_response() -> Response {
    Response::json(
        200,
        &format!(
            r##"[{{"id":"{AGENT_ID}","name":"weather","channels":[{{"kind":"slack","address":"#weather"}}],"approval_required_tools":[]}}]"##
        ),
    )
}

/// `n` synthetic pending records, wire-shaped like `ApprovalOut` (only the
/// fields the CLI reads; the rest are ignored by serde, same as elsewhere in
/// this suite).
fn pending_records_json(n: usize) -> String {
    let records: Vec<String> = (0..n)
        .map(|i| {
            format!(
                r#"{{"id":"ap_{i}","author":"U1","route":null,"gate_kind":null,"granted_tool":"Bash","status":"pending","conversation_id":"C1-thread-{i}","summary":"do the thing","expires_at":null,"resolved_by":null}}"#
            )
        })
        .collect();
    format!("[{}]", records.join(","))
}

async fn list_pending(server: &MockServer) -> ApprovalsOutput {
    approvals(
        AgentActionOpts {
            api_url: server.base_url.clone(),
            api_key: TEST_API_KEY.to_string(),
            agent: "weather".to_string(),
            dry_run: false,
        },
        vec![],
        false,
        ApprovalCmd {
            list: true,
            ..ApprovalCmd::default()
        },
    )
    .await
    .expect("approvals --list should succeed against a well-formed mock")
}

fn pending_fields(out: &ApprovalsOutput) -> (usize, bool) {
    match out {
        ApprovalsOutput::Pending {
            records, truncated, ..
        } => (records.len(), *truncated),
        ApprovalsOutput::DryRun(_) => panic!("expected the pending list, not a dry-run plan"),
        ApprovalsOutput::Gates { .. } => panic!("expected the pending list, not the gate view"),
        ApprovalsOutput::Resolved { .. } => {
            panic!("expected the pending list, not a resolved record")
        }
        ApprovalsOutput::Routes { .. } => {
            panic!("expected the pending list, not the route bindings")
        }
    }
}

/// (a) The core regression guard: deleting the `limit` query param must fail
/// this. The mock asserts on the raw outgoing request path so it cannot be
/// satisfied by a handler that always fetches the server's default page size.
#[tokio::test]
async fn list_request_sends_the_server_max_limit() {
    let expected_limit = format!("limit={}", ApiClient::APPROVALS_LIST_LIMIT);
    let expected_limit_in_handler = expected_limit.clone();
    let server = serve(move |req| match req.path.split('?').next().unwrap() {
        "/agents" => agents_response(),
        "/approvals" => {
            assert!(
                req.path.contains(&expected_limit_in_handler),
                "expected the request to ask for the server's max page size, got path {:?}",
                req.path
            );
            assert!(
                req.path.contains("status_filter=pending"),
                "expected status_filter=pending in {:?}",
                req.path
            );
            assert!(
                req.path.contains(&format!("agent_id={AGENT_ID}")),
                "expected agent_id={AGENT_ID} in {:?}",
                req.path
            );
            Response::json(200, &pending_records_json(3))
        }
        other => panic!("unexpected request: {other}"),
    });

    let out = list_pending(&server).await;
    let (count, truncated) = pending_fields(&out);
    assert_eq!(count, 3);
    assert!(!truncated);

    let recorded = server.recorded();
    let approvals_req = recorded
        .iter()
        .find(|r| r.path.starts_with("/approvals"))
        .expect("the /approvals endpoint must have been called");
    assert!(approvals_req.path.contains(&expected_limit));
}

/// (b) Hitting the cap means more pending approvals may exist beyond what was
/// fetched: `truncated` must be `true` and both `to_json()` keys must reflect it.
#[tokio::test]
async fn exactly_the_cap_is_reported_as_truncated() {
    let server = serve(|req| match req.path.split('?').next().unwrap() {
        "/agents" => agents_response(),
        "/approvals" => Response::json(200, &pending_records_json(ApiClient::APPROVALS_LIST_LIMIT)),
        other => panic!("unexpected request: {other}"),
    });

    let out = list_pending(&server).await;
    let (count, truncated) = pending_fields(&out);
    assert_eq!(count, ApiClient::APPROVALS_LIST_LIMIT);
    assert!(
        truncated,
        "hitting the server cap must be reported as a possibly-truncated list"
    );

    let json = out.to_json();
    assert_eq!(json["truncated"], true);
    assert_eq!(json["count"], ApiClient::APPROVALS_LIST_LIMIT);
}

/// (c) The positive control: under the cap, nothing about the list is
/// ambiguous, so `truncated` stays `false` in both the struct and the JSON.
#[tokio::test]
async fn under_the_cap_is_reported_as_complete() {
    let server = serve(|req| match req.path.split('?').next().unwrap() {
        "/agents" => agents_response(),
        "/approvals" => Response::json(200, &pending_records_json(3)),
        other => panic!("unexpected request: {other}"),
    });

    let out = list_pending(&server).await;
    let (count, truncated) = pending_fields(&out);
    assert_eq!(count, 3);
    assert!(!truncated);

    let json = out.to_json();
    assert_eq!(json["truncated"], false);
    assert_eq!(json["count"], 3);
}

/// (d) The dry-run plan must report the same `limit` the real request sends
/// (#670 review finding): a hand-maintained dry-run string can drift from
/// `list_pending_approvals`'s actual query params without either the
/// truncation tests above or a dry-run consumer noticing. No mock server is
/// installed here because the dry-run path returns before any HTTP call.
#[tokio::test]
async fn dry_run_plan_reports_the_same_limit_as_the_real_request() {
    let out = approvals(
        AgentActionOpts {
            api_url: "http://localhost:28000".to_string(),
            api_key: TEST_API_KEY.to_string(),
            agent: "weather".to_string(),
            dry_run: true,
        },
        vec![],
        false,
        ApprovalCmd {
            list: true,
            ..ApprovalCmd::default()
        },
    )
    .await
    .expect("approvals --list --dry-run should succeed without any network call");

    match out {
        ApprovalsOutput::DryRun(plan) => {
            let expected_limit = format!("limit={}", ApiClient::APPROVALS_LIST_LIMIT);
            assert!(
                plan.lines[0].contains(&expected_limit),
                "expected the dry-run plan to report the same limit as the real request, got {:?}",
                plan.lines[0]
            );
        }
        ApprovalsOutput::Pending { .. } => panic!("expected a dry-run plan, not the pending list"),
        ApprovalsOutput::Routes { .. } => {
            panic!("expected a dry-run plan, not the route bindings")
        }
        ApprovalsOutput::Gates { .. } => panic!("expected a dry-run plan, not the gate view"),
        ApprovalsOutput::Resolved { .. } => {
            panic!("expected a dry-run plan, not a resolved record")
        }
    }
}
