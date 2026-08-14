//! Integration: `<tier> approvals <agent>` must never report an un-gated agent
//! when the deployed bundle manifest lookup did not complete (#607).
//!
//! Driven through the real `approvals` handler and a real `ApiClient` against a
//! wire-level stub, because the bug lives in the fetch path, not the renderer: a
//! failed lookup that collapses to an empty gate list reads as the affirmative
//! "nothing pauses for approval". A test that hands the renderer a
//! ready-made reason cannot see that, so every case here makes the HTTP call
//! actually fail (or succeed) and asserts what the command concluded from it.

mod support;

use curie::commands::{approvals, AgentActionOpts, ApprovalCmd, ApprovalsOutput};
use curie::ui::CliOutput;
use support::{serve, MockServer, Response};

const AGENT_ID: &str = "11111111-1111-1111-1111-111111111111";
const VERSION_ID: &str = "22222222-2222-2222-2222-222222222222";

/// The agent lookup always succeeds and gates nothing via the platform's mutable
/// `approval_required_tools` field, so the manifest half is the only thing under
/// test: whatever the report says about gates came from the deployment fetch.
fn agents_response() -> Response {
    Response::json(
        200,
        &format!(
            r##"[{{"id":"{AGENT_ID}","name":"weather","channels":[{{"kind":"slack","address":"#weather"}}],"approval_required_tools":[]}}]"##
        ),
    )
}

fn deployment_json(version_id: &str) -> String {
    format!(
        r#"[{{"id":"dep-1","agent_id":"{AGENT_ID}","environment":"dev","status":"active","version_id":{version_id},"deployed_at":"2026-07-05T00:00:00Z"}}]"#
    )
}

async fn read_gates(server: &MockServer) -> ApprovalsOutput {
    approvals(
        AgentActionOpts {
            api_url: server.base_url.clone(),
            api_key: "test-key".to_string(),
            agent: "weather".to_string(),
            dry_run: false,
        },
        vec![],
        false,
        ApprovalCmd::default(),
    )
    .await
    .expect("approvals should report the failure, not propagate it")
}

/// The gate list plus the unreadable reason, as the command concluded them.
fn gates(out: &ApprovalsOutput) -> (Vec<String>, Option<String>) {
    match out {
        ApprovalsOutput::Gates {
            gated_tools,
            manifest_unreadable,
            ..
        } => (gated_tools.clone(), manifest_unreadable.clone()),
        ApprovalsOutput::DryRun(_) => panic!("expected the gate view, not a dry-run plan"),
        ApprovalsOutput::Pending { .. } | ApprovalsOutput::Resolved { .. } => {
            panic!("expected the gate view, not a pending/resolved record")
        }
        ApprovalsOutput::Routes { .. } => {
            panic!("expected the gate view, not the route bindings")
        }
    }
}

#[tokio::test]
async fn a_failed_deployment_lookup_is_disclosed_rather_than_read_as_ungated() {
    let server = serve(|req| match req.path.split('?').next().unwrap() {
        "/agents" => agents_response(),
        "/deployments" => Response::json(503, r#"{"detail":"upstream unavailable"}"#),
        other => panic!("unexpected request: {other}"),
    });

    let out = read_gates(&server).await;
    let (gated, unreadable) = gates(&out);
    assert!(gated.is_empty(), "the platform field gates nothing here");
    let reason = unreadable.expect("a 503 on the deployment list leaves the manifest unread");
    assert!(
        reason.contains("deployments"),
        "the reason must name the lookup that failed, got {reason:?}"
    );
    assert_eq!(out.to_json()["manifest_unreadable"], reason.as_str());
}

#[tokio::test]
async fn a_failed_bundle_files_lookup_is_disclosed_rather_than_read_as_ungated() {
    let server = serve(|req| match req.path.split('?').next().unwrap() {
        "/agents" => agents_response(),
        "/deployments" => Response::json(200, &deployment_json(&format!(r#""{VERSION_ID}""#))),
        p if p == format!("/agents/{AGENT_ID}/versions/{VERSION_ID}/files") => {
            Response::json(404, r#"{"detail":"version not found"}"#)
        }
        other => panic!("unexpected request: {other}"),
    });

    let (gated, unreadable) = gates(&read_gates(&server).await);
    assert!(gated.is_empty());
    let reason = unreadable.expect("a 404 on the bundle files leaves the manifest unread");
    assert!(
        reason.contains("bundle"),
        "the reason must name the lookup that failed, got {reason:?}"
    );
}

#[tokio::test]
async fn an_in_force_deployment_with_no_version_id_is_disclosed() {
    // Response drift, not a stated absence: a bundle IS deployed and in force, we
    // simply cannot address it to read its manifest.
    let server = serve(|req| match req.path.split('?').next().unwrap() {
        "/agents" => agents_response(),
        "/deployments" => Response::json(200, &deployment_json("null")),
        other => panic!("unexpected request: {other}"),
    });

    let (gated, unreadable) = gates(&read_gates(&server).await);
    assert!(gated.is_empty());
    let reason = unreadable.expect("an unaddressable in-force bundle leaves the manifest unread");
    assert!(
        reason.contains("version id"),
        "the reason must name what was missing, got {reason:?}"
    );
}

/// The positive control. Without it the three cases above are satisfiable by
/// always claiming the manifest is unreadable, which would make the affirmative
/// "no tools are gated" answer unreachable and the disclosure meaningless.
#[tokio::test]
async fn a_readable_gate_free_manifest_still_answers_that_nothing_is_gated() {
    let server = serve(|req| match req.path.split('?').next().unwrap() {
        "/agents" => agents_response(),
        "/deployments" => Response::json(200, &deployment_json(&format!(r#""{VERSION_ID}""#))),
        p if p == format!("/agents/{AGENT_ID}/versions/{VERSION_ID}/files") => Response::json(
            200,
            r#"{"files":[{"path":"plugin.json","content":"{\"name\":\"weather\"}"}]}"#,
        ),
        other => panic!("unexpected request: {other}"),
    });

    let out = read_gates(&server).await;
    let (gated, unreadable) = gates(&out);
    assert!(gated.is_empty());
    assert_eq!(
        unreadable, None,
        "the manifest was read and declares no gate"
    );
    assert_eq!(
        out.to_json()["manifest_unreadable"],
        serde_json::Value::Null
    );
}
