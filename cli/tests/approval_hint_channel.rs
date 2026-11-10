//! Integration: the pre-wait resolve hint must name the channel the approval's
//! card was actually posted to, and only `GET /approvals/{approval_id}` can
//! answer that (#1531 finding 3).
//!
//! A route binding can put the card somewhere other than the channel this turn
//! used. The resolve hint therefore reads `ApprovalRecord.card_channel` through
//! the single-record endpoint instead of reusing the turn channel.
//!
//! Driven through a real `ApiClient` against the wire-level stub rather than by
//! hand-building an `ApprovalRecord`: the defect class here is the REQUEST (the
//! wrong path, a missing API key, a list scan instead of a single-record read),
//! which a constructed record walks straight past.
//!
//! The wire-level contract stays here while the private hint fallback and
//! timeout behavior are covered in `message.rs`:
//!
//! - `ApiClient::get_approval` authenticates one `GET /approvals/{id}` request
//!   and decodes the record.
//! - private `hint_channel` uses that value when present and falls back to the
//!   turn channel without failing the turn.
//! - `HINT_CHANNEL_LOOKUP_BUDGET` bounds the whole advisory lookup, including
//!   cluster port-forward startup.

use crate::support;

use curie::api::ApiClient;
use support::{serve, MockServer, Response};

const TEST_API_KEY: &str = "test-key";
/// A real UUID: both `parse_approval_id` (`cli/src/chat.rs:400`) and the API
/// route (`approvals.py`, path param typed `uuid.UUID`) parse this as one, so a
/// placeholder like `ap_1` could never reach the endpoint under test.
const APPROVAL_ID: &str = "22222222-2222-2222-2222-222222222222";

/// One pending approval whose card landed in a channel OTHER than the one the
/// requester spoke in. The two channels must stay distinct, per the sibling
/// test's own comment: with a single channel the assertion cannot tell a
/// correct value from a lucky one.
fn route_bound_record() -> String {
    format!(
        r#"{{"id":"{APPROVAL_ID}","agent_id":"11111111-1111-1111-1111-111111111111","author":"U-REQUESTER","route":"finance","gate_kind":"policy","granted_tool":null,"status":"pending","conversation_id":"thread-1","summary":"approve invoice","expires_at":null,"resolved_by":null,"card_channel":"CFINANCE01","reply_channel":"CREQUEST01"}}"#
    )
}

fn single_approval_server(status: u16, body: String) -> MockServer {
    serve(move |req| match (req.method.as_str(), req.path.as_str()) {
        ("GET", p) if p == format!("/approvals/{APPROVAL_ID}") => {
            Response::json(status, body.as_str())
        }
        other => panic!("unexpected request: {other:?}"),
    })
}

/// A-T1. The read the hint is built on: the single-record endpoint, carrying
/// the API key, returning the card channel.
///
/// The list endpoint is deliberately NOT an acceptable substitute and this test
/// is what pins that: `list_pending_approvals` caps at the server max and
/// signals truncation (#670), so the approval being waited on can simply not be
/// in the page. The hint must read the one record by id.
///
/// Mutation it catches: wiring the lookup to `/approvals?agent_id=...` instead
/// of `/approvals/{id}`, or dropping the `X-API-Key` header (which the API
/// answers 401 to, and an advisory caller would silently absorb into the wrong
/// channel).
#[tokio::test]
async fn get_approval_reads_the_card_channel_the_hint_needs() {
    let server = single_approval_server(200, route_bound_record());
    let api = ApiClient::new(&server.base_url, TEST_API_KEY).expect("build client");

    let record = api
        .get_approval(APPROVAL_ID)
        .await
        .expect("a 200 with a well-formed record must decode");

    assert_eq!(
        record.card_channel.as_deref(),
        Some("CFINANCE01"),
        "the hint must report the card's real route-bound location instead of \
         sending the operator back to the requesting channel"
    );
    assert_ne!(
        record.card_channel.as_deref(),
        Some("CREQUEST01"),
        "the fixture must keep the card and reply channels distinct, or this \
         test cannot tell a correct value from a lucky one"
    );

    let recorded = server.recorded();
    let request = recorded
        .iter()
        .find(|r| r.method == "GET")
        .expect("the client must have issued a GET");
    assert_eq!(
        request.path,
        format!("/approvals/{APPROVAL_ID}"),
        "the hint must read the single record by id, not scan the truncatable \
         pending list; recorded path: {}",
        request.path
    );
    assert_eq!(
        request.header("X-API-Key"),
        Some(TEST_API_KEY),
        "every platform API call authenticates; a missing key is a 401 the \
         advisory caller would absorb into a silently wrong channel"
    );
}

/// A-T2. A null `card_channel` must PARSE, not error.
///
/// It is a real wire value: a row predating route bindings, or a direct API
/// write that omitted the field. `#[serde(default)]` on the field already makes
/// this work; the test exists so a future tightening (removing the default, or
/// adding `deny_unknown_fields`-style strictness) turns an older row into a
/// visible failure here instead of a decode error inside an advisory lookup
/// that swallows every error it sees.
///
/// Mutation it catches: making `card_channel` non-optional on `ApprovalRecord`.
#[tokio::test]
async fn a_null_card_channel_yields_no_override() {
    let body = format!(
        r#"{{"id":"{APPROVAL_ID}","agent_id":"11111111-1111-1111-1111-111111111111","author":"U-REQUESTER","route":"finance","gate_kind":"policy","granted_tool":null,"status":"pending","conversation_id":"thread-1","summary":"approve invoice","expires_at":null,"resolved_by":null,"card_channel":null,"reply_channel":"CREQUEST01"}}"#
    );
    let server = single_approval_server(200, body);
    let api = ApiClient::new(&server.base_url, TEST_API_KEY).expect("build client");

    let record = api
        .get_approval(APPROVAL_ID)
        .await
        .expect("a null card_channel is a valid record, not a decode failure");

    assert!(
        record.card_channel.is_none(),
        "a null card_channel means 'an older row, so the requesting channel \
         applies' (#1431), which the caller expresses by falling back to the \
         turn channel; got {:?}",
        record.card_channel
    );
}

/// A-T3. The client method PROPAGATES a 404; absorbing it is the caller's job.
///
/// A 404 here is real rather than theoretical: another operator can resolve or
/// expire the approval between the pending notice and the hint. The advisory
/// wrapper in `message.rs` is where that becomes "print the turn channel"; if
/// `get_approval` absorbed it instead, a future non-advisory caller would read
/// a missing approval as a present one.
///
/// The second assertion pins the boundary `expect_ok`'s `is_unrouted` branch
/// draws: FastAPI answers an UNROUTED path with exactly `{"detail":"Not Found"}`
/// and the CLI turns that into "this platform release is older than this CLI".
/// A handler that ran and found nothing sets its own detail, and telling that
/// operator to upgrade their release would be a fabricated diagnosis.
///
/// Mutation it catches: implementing the absorb inside `get_approval` (returning
/// `Ok(None)` or a default record), or letting the not-found detail fall into
/// the unrouted branch.
#[tokio::test]
async fn a_404_is_absorbed_not_propagated() {
    let server = single_approval_server(404, r#"{"detail":"approval not found"}"#.to_string());
    let api = ApiClient::new(&server.base_url, TEST_API_KEY).expect("build client");

    let err = api
        .get_approval(APPROVAL_ID)
        .await
        .expect_err("a 404 must reach the caller as an Err, not be swallowed here")
        .to_string();

    assert!(
        !err.contains("older than this CLI"),
        "a genuine approval-not-found must not be misreported as an unrouted \
         endpoint; that sends the operator to upgrade a release that is fine. \
         Error: {err}"
    );
    assert!(
        !err.contains("Upgrade the release"),
        "same misdiagnosis, remedy half. Error: {err}"
    );
}
