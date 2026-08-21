//! Integration: `<tier> approvals <agent> --resolve <id> --as <actor>
//! --actor-channel <chan>` must forward `actor_channel` on the wire so a
//! channel-authorized approval gate can verify membership server-side (#704).
//! Today the CLI never sends this field, so channel-authorized gates 403 when
//! resolved from the CLI even though the API's `ApprovalResolve` schema
//! accepts it.
//!
//! Driven through the real `commands::approvals` handler and a real
//! `ApiClient` against a wire-level stub, mirroring
//! `approvals_list_truncation.rs`: the regression this guards is "the CLI
//! silently drops actor_channel," which only shows up by inspecting the
//! actual outgoing POST body, not by hand-constructing an `ApprovalRecord`.
//!
//! RED CONTRACT: this file references an `actor_channel: Option<String>`
//! field on `commands::ApprovalCmd` that does not exist yet. The intended
//! shape (mirroring the existing optional `note` field/param all the way
//! through): `ApprovalCmd.actor_channel: Option<String>`, threaded into
//! `ApiClient::resolve_approval(&self, approval_id, decision, resolved_by,
//! note: Option<&str>, actor_channel: Option<&str>)`, which conditionally
//! sets `body["actor_channel"] = json!(chan)` exactly like the existing
//! `if let Some(note) = note { body["note"] = json!(note); }` branch. Until
//! the implementer adds that field and threads it through, this file fails
//! to compile (not just fails to run) -- that is the intended RED signal.

mod support;

use curie::commands::{approvals, AgentActionOpts, ApprovalCmd, ApprovalsOutput};
use support::{serve, MockServer, Response};

const TEST_API_KEY: &str = "test-key";
const APPROVAL_ID: &str = "ap_1";

fn resolved_record_json(resolved_by: &str) -> String {
    format!(
        r#"{{"id":"{APPROVAL_ID}","author":"U1","route":null,"gate_kind":null,"granted_tool":"Bash","status":"approved","conversation_id":"C1-thread-0","summary":"do the thing","expires_at":null,"resolved_by":"{resolved_by}"}}"#
    )
}

async fn resolve(server: &MockServer, cmd: ApprovalCmd) -> ApprovalsOutput {
    approvals(
        AgentActionOpts {
            api_url: server.base_url.clone(),
            api_key: TEST_API_KEY.to_string(),
            agent: "weather".to_string(),
            dry_run: false,
        },
        vec![],
        false,
        cmd,
    )
    .await
    .expect("approvals --resolve should succeed against a well-formed mock")
}

/// (a) The core regression guard: when `--actor-channel` is supplied, the
/// outgoing POST body must include it alongside the existing `decision` and
/// `resolved_by` fields -- deleting the actor_channel wiring must fail this.
#[tokio::test]
async fn actor_channel_present_in_resolve_body_when_passed() {
    let server = serve(|req| match req.path.split('?').next().unwrap() {
        p if p == format!("/approvals/{APPROVAL_ID}/resolve") => {
            Response::json(200, &resolved_record_json("brian"))
        }
        other => panic!("unexpected request: {other}"),
    });

    let out = resolve(
        &server,
        ApprovalCmd {
            resolve: Some(APPROVAL_ID.to_string()),
            as_actor: Some("brian".to_string()),
            actor_channel: Some("C123456".to_string()),
            ..ApprovalCmd::default()
        },
    )
    .await;

    match out {
        ApprovalsOutput::Resolved { record } => {
            assert_eq!(record.resolved_by.as_deref(), Some("brian"));
        }
        _ => panic!("expected a resolved record"),
    }

    let recorded = server.recorded();
    let resolve_req = recorded
        .iter()
        .find(|r| {
            r.path
                .starts_with(&format!("/approvals/{APPROVAL_ID}/resolve"))
        })
        .expect("the resolve endpoint must have been called");
    let body: serde_json::Value =
        serde_json::from_slice(&resolve_req.body).expect("resolve body must be valid JSON");

    assert_eq!(
        body["actor_channel"], "C123456",
        "expected the POST body to carry actor_channel when --actor-channel is passed, got {body:?}"
    );
    assert_eq!(body["decision"], "approved");
    assert_eq!(body["resolved_by"], "brian");
}

/// (b) The negative/secondary path (mandatory): when no `--actor-channel` is
/// given, the POST body must have NO `actor_channel` key at all -- mirrors the
/// existing conditional `note` behavior and proves the field is optional, not
/// always-sent-as-empty-string/null.
#[tokio::test]
async fn actor_channel_absent_from_resolve_body_when_not_passed() {
    let server = serve(|req| match req.path.split('?').next().unwrap() {
        p if p == format!("/approvals/{APPROVAL_ID}/resolve") => {
            Response::json(200, &resolved_record_json("brian"))
        }
        other => panic!("unexpected request: {other}"),
    });

    let out = resolve(
        &server,
        ApprovalCmd {
            resolve: Some(APPROVAL_ID.to_string()),
            as_actor: Some("brian".to_string()),
            actor_channel: None,
            ..ApprovalCmd::default()
        },
    )
    .await;

    match out {
        ApprovalsOutput::Resolved { record } => {
            assert_eq!(record.resolved_by.as_deref(), Some("brian"));
        }
        _ => panic!("expected a resolved record"),
    }

    let recorded = server.recorded();
    let resolve_req = recorded
        .iter()
        .find(|r| {
            r.path
                .starts_with(&format!("/approvals/{APPROVAL_ID}/resolve"))
        })
        .expect("the resolve endpoint must have been called");
    let body: serde_json::Value =
        serde_json::from_slice(&resolve_req.body).expect("resolve body must be valid JSON");

    assert!(
        body.get("actor_channel").is_none(),
        "expected no actor_channel key when --actor-channel is not passed, got {body:?}"
    );
    assert_eq!(body["decision"], "approved");
    assert_eq!(body["resolved_by"], "brian");
}

/// (c) `--reject` with `--actor-channel` still forwards both the rejected
/// decision and the channel -- proves actor_channel wiring isn't accidentally
/// coupled to the approve-only branch.
#[tokio::test]
async fn actor_channel_present_alongside_reject_decision() {
    let server = serve(|req| match req.path.split('?').next().unwrap() {
        p if p == format!("/approvals/{APPROVAL_ID}/resolve") => {
            Response::json(200, &resolved_record_json("brian"))
        }
        other => panic!("unexpected request: {other}"),
    });

    let _out = resolve(
        &server,
        ApprovalCmd {
            resolve: Some(APPROVAL_ID.to_string()),
            as_actor: Some("brian".to_string()),
            actor_channel: Some("C999".to_string()),
            reject: true,
            ..ApprovalCmd::default()
        },
    )
    .await;

    let recorded = server.recorded();
    let resolve_req = recorded
        .iter()
        .find(|r| {
            r.path
                .starts_with(&format!("/approvals/{APPROVAL_ID}/resolve"))
        })
        .expect("the resolve endpoint must have been called");
    let body: serde_json::Value =
        serde_json::from_slice(&resolve_req.body).expect("resolve body must be valid JSON");

    assert_eq!(body["actor_channel"], "C999");
    assert_eq!(body["decision"], "rejected");
    assert_eq!(body["resolved_by"], "brian");
}

// ─── #1078: the recipe promises this field; --list must actually emit it ─────

/// The card channel must reach the operator through `--list --json`, because
/// `--resolve` cannot be driven without it.
///
/// #1056's recipe told an agent that `--list` reports the channel a route bound
/// the card to. It did not: `approval_record_json` projected ten fields and
/// dropped `card_channel`, while the API had carried it all along. With no
/// `approvers` block on the route, that channel's MEMBERS are the approver set
/// and `slack_approvers.py` compares `actor_channel` against it, so a guess is a
/// 403 and the value was underivable from the CLI.
///
/// Asserted end to end through the real handler rather than by constructing an
/// `ApprovalRecord`: the defect was in the PROJECTION, which a hand-built record
/// walks straight past.
#[tokio::test]
async fn list_json_reports_the_card_channel_the_recipe_promises() {
    let server = serve(|req| match (req.method.as_str(), req.path.as_str()) {
        ("GET", "/agents") => Response::json(
            200,
            r##"[{"id":"11111111-1111-1111-1111-111111111111","name":"weather","channels":[{"kind":"slack","address":"CREQUEST01"}],"created_at":"2026-07-05T00:00:00Z"}]"##,
        ),
        ("GET", p) if p.starts_with("/approvals") => Response::json(
            200,
            r##"[{"id":"22222222-2222-2222-2222-222222222222","agent_id":"11111111-1111-1111-1111-111111111111","author":"U-REQUESTER","route":"finance","gate_kind":"policy","granted_tool":null,"status":"pending","conversation_id":"thread-1","summary":"approve invoice","expires_at":null,"resolved_by":null,"card_channel":"CFINANCE01","reply_channel":"CREQUEST01"}]"##,
        ),
        other => panic!("unexpected request: {other:?}"),
    });

    let out = approvals(
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
    .expect("list succeeds");

    let json = curie::ui::CliOutput::to_json(&out);
    let record = &json["pending"][0];

    assert_eq!(
        record["card_channel"], "CFINANCE01",
        "--list --json must report the card channel; without it --actor-channel \
         is underivable and every resolve on a channel-authorized route 403s. \
         Payload: {json}"
    );
    // The requesting channel is a DIFFERENT channel, which is the whole reason
    // guessing fails: an agent that assumed the two were the same would send the
    // wrong one and read the refusal as an authorization problem.
    assert_ne!(
        record["card_channel"], "CREQUEST01",
        "the fixture must keep card and reply channels distinct, or this test \
         cannot tell a correct value from a lucky one"
    );
}
