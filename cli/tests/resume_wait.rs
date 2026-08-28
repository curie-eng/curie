//! Integration: the #766 keep-alive resume wait (`chat::await_resume`) against
//! real Valkey plus the embedded Slack stub over real HTTP.
//!
//! This is the behavioral coverage the mechanism pivot lost when the injectable
//! `await_resume_reply` seam was replaced by a concrete Valkey connection: it
//! drives the seam the way it actually runs. It needs a reachable Valkey (the
//! compose dev one on host port 26379 is fine) and the in-process stub. The test
//! uses a unique test-scoped stream it deletes afterward, so it never touches
//! the real `curie:runs` stream. Local runs skip when no Valkey is reachable, but
//! CI sets `CI_REQUIRE_VALKEY_TESTS` and starts Valkey, so these tests execute
//! there. For equivalent local coverage, run the compose Valkey or point
//! `TEST_VALKEY_URL` at another instance.

use std::time::Duration;

use curie::chat::{await_reply, await_resume, parse_approval_id, Outcome, SlackStub};
use curie::queue::{synthetic_turn, xadd, WORKER_GROUP};
use curie_aci_protocol::{QueuedTurn, ReplyHandle, TurnSource};

mod support;
use support::{unique_stream, valkey_or_skip};

const PLACEHOLDER_TS: &str = "1720000000.000200";

/// A resume turn under the deterministic `approval-<id>-resolved` event id, the
/// exact shape `resumequeue._build_turn` appends when a human resolves an
/// approval (replaying the original placeholder + this stub's endpoint).
fn resume_turn(resume_event_id: &str, endpoint: &str) -> QueuedTurn {
    QueuedTurn {
        event_id: resume_event_id.to_string(),
        conversation_id: "1720000000.000100".into(),
        author: "U-curie-message".into(),
        text: "(resumed after approval)".into(),
        reply_handle: ReplyHandle {
            kind: "slack".into(),
            channel: "C-SIM-x".into(),
            placeholder: Some(PLACEHOLDER_TS.into()),
            endpoint: Some(endpoint.to_string()),
            adapter: None,
        },
        received_at: "2026-07-21T00:00:00Z".into(),
        // A resume continues the turn a person started, matching what
        // `resumequeue._build_turn` mints on the Python side.
        source: TurnSource::Slack,
        delegation: None,
    }
}

/// Deliver + XACK `entry_id` under the worker group so `entry_acked` sees it
/// finalized, exactly as the worker does after finishing the turn.
async fn deliver_and_ack(conn: &mut redis::aio::MultiplexedConnection, stream: &str) {
    // BUSYGROUP on a second call is expected: the route-bound test acks in two
    // phases (original turn, then the resume entry) against the same group.
    let _: redis::RedisResult<()> = redis::cmd("XGROUP")
        .arg("CREATE")
        .arg(stream)
        .arg(WORKER_GROUP)
        .arg("0")
        .query_async(conn)
        .await;
    // Deliver every pending entry to the group (advances last-delivered-id).
    let _: redis::Value = redis::cmd("XREADGROUP")
        .arg("GROUP")
        .arg(WORKER_GROUP)
        .arg("worker-1")
        .arg("COUNT")
        .arg(100)
        .arg("STREAMS")
        .arg(stream)
        .arg(">")
        .query_async(conn)
        .await
        .unwrap();
    // XACK every entry in the stream under the worker group, exactly as the
    // worker does after finishing each turn (`_ack` = XACK in consumer.py), so
    // `entry_acked` sees the delivered entries as finalized rather than pending.
    let entries: Vec<(String, Vec<(String, String)>)> = redis::cmd("XRANGE")
        .arg(stream)
        .arg("-")
        .arg("+")
        .query_async(conn)
        .await
        .unwrap();
    if !entries.is_empty() {
        let mut xack = redis::cmd("XACK");
        xack.arg(stream).arg(WORKER_GROUP);
        for (id, _fields) in &entries {
            xack.arg(id);
        }
        let _: i64 = xack.query_async(conn).await.unwrap();
    }
}

#[tokio::test]
async fn await_resume_returns_the_finalized_reply_once_the_resume_entry_is_acked() {
    let Some(mut conn) =
        valkey_or_skip("await_resume_returns_the_finalized_reply_once_the_resume_entry_is_acked")
            .await
    else {
        return;
    };
    let stream = unique_stream("curie:test:resume:");

    // Stand up the reply stub (the surface the resumed turn's chat.update lands on).
    let mut stub = SlackStub::start("localhost", 0, "localhost").await.unwrap();
    let endpoint = stub.base_api_url().to_string();

    // The CLI's OWN original turn: its stream id is the exclusive scan cursor.
    let original = synthetic_turn(
        "slack",
        "C-SIM-x",
        "U-curie-message",
        "do the risky thing",
        "1720000000.000100",
        PLACEHOLDER_TS,
        Some(endpoint.clone()),
    );
    let original_id = xadd(&mut conn, &stream, &original).await.unwrap();

    // The API's resume turn, appended AFTER the original under the deterministic id.
    let resume_event_id = "approval-3f2504e0-4f89-41d3-9a0c-0305e82c3301-resolved";
    let resume = resume_turn(resume_event_id, &endpoint);
    xadd(&mut conn, &stream, &resume).await.unwrap();

    // Simulate the worker finalizing the resumed turn: deliver+ack the entry, then
    // post the final chat.update editing the tracked placeholder. Posting BEFORE
    // the wait proves a reply that landed while the scan was between iterations is
    // still captured (the stub's channel buffers it).
    deliver_and_ack(&mut conn, &stream).await;
    let http = reqwest::Client::new();
    let resp: serde_json::Value = http
        .post(format!("{endpoint}chat.update"))
        .form(&[
            ("channel", "C-SIM-x"),
            ("ts", PLACEHOLDER_TS),
            ("text", "the resolved answer"),
        ])
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(resp["ok"], true, "stub accepted the final edit");

    let observed = await_resume(
        &mut stub,
        &mut conn,
        &stream,
        resume_event_id,
        &original_id,
        PLACEHOLDER_TS,
        Duration::from_secs(3),
    )
    .await;

    let _: i64 = redis::cmd("DEL")
        .arg(&stream)
        .query_async(&mut conn)
        .await
        .unwrap();

    assert!(
        observed.resolved,
        "the resume entry was observed (resolved)"
    );
    match observed.outcome {
        Outcome::Replied(reply) => assert_eq!(
            reply, "the resolved answer",
            "await_resume returns the FINALIZED placeholder text, not a booting edit"
        ),
        other => panic!("expected Replied, got a different terminal: {other:?}"),
    }
}

#[tokio::test]
async fn await_resume_times_out_when_the_approval_is_never_resolved() {
    let Some(mut conn) =
        valkey_or_skip("await_resume_times_out_when_the_approval_is_never_resolved").await
    else {
        return;
    };
    let stream = unique_stream("curie:test:resume:");

    let mut stub = SlackStub::start("localhost", 0, "localhost").await.unwrap();
    let endpoint = stub.base_api_url().to_string();

    // Only the original turn exists; the approval is never resolved, so no resume
    // entry is ever appended.
    let original = synthetic_turn(
        "slack",
        "C-SIM-x",
        "U-curie-message",
        "do the risky thing",
        "1720000000.000100",
        PLACEHOLDER_TS,
        Some(endpoint),
    );
    let original_id = xadd(&mut conn, &stream, &original).await.unwrap();

    let resume_event_id = "approval-00000000-0000-4000-8000-000000000000-resolved";
    let observed = await_resume(
        &mut stub,
        &mut conn,
        &stream,
        resume_event_id,
        &original_id,
        PLACEHOLDER_TS,
        Duration::from_millis(400),
    )
    .await;

    let _: i64 = redis::cmd("DEL")
        .arg(&stream)
        .query_async(&mut conn)
        .await
        .unwrap();

    assert!(
        !observed.resolved,
        "no resume entry was ever appended, so the approval was NOT observed as resolved"
    );
    assert!(
        matches!(observed.outcome, Outcome::TimedOut),
        "the never-resolved wait must hit the deadline as TimedOut, never a false Replied"
    );
}

/// Post a `chat.update` editing the tracked placeholder, exactly as the worker
/// does. Deliberately carries NO approval card: this is the notice-only path.
async fn post_placeholder_edit(endpoint: &str, text: &str) {
    let resp: serde_json::Value = reqwest::Client::new()
        .post(format!("{endpoint}chat.update"))
        .form(&[
            ("channel", "C-SIM-x"),
            ("ts", PLACEHOLDER_TS),
            ("text", text),
        ])
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(resp["ok"], true, "stub accepted the placeholder edit");
}

/// Route-bound approval (#766): when the approval route is bound to a channel
/// other than the requesting one, the worker posts the Block Kit card over its
/// DEFAULT transport, so NO card ever reaches this stub -- but the authoritative
/// placeholder notice always uses the per-turn endpoint and does. Classifying on
/// the card alone reported that notice as the final answer and stranded the
/// resumed reply; the notice must park the turn too, and the keep-alive must
/// then deliver the resumed reply.
#[tokio::test]
async fn a_notice_without_a_card_parks_the_turn_and_the_keepalive_delivers_the_resumed_reply() {
    let Some(mut conn) = valkey_or_skip(
        "a_notice_without_a_card_parks_the_turn_and_the_keepalive_delivers_the_resumed_reply",
    )
    .await
    else {
        return;
    };
    let stream = unique_stream("curie:test:resume:");

    let mut stub = SlackStub::start("localhost", 0, "localhost").await.unwrap();
    let endpoint = stub.base_api_url().to_string();

    let original = synthetic_turn(
        "slack",
        "C-SIM-x",
        "U-curie-message",
        "do the risky thing",
        "1720000000.000100",
        PLACEHOLDER_TS,
        Some(endpoint.clone()),
    );
    let original_id = xadd(&mut conn, &stream, &original).await.unwrap();

    // The worker parks the turn: it edits the placeholder with the authoritative
    // notice and acks the entry. The card went elsewhere, so the stub never sees
    // one -- the notice is the ONLY signal available here.
    let approval_id = "3f2504e0-4f89-41d3-9a0c-0305e82c3302";
    let notice = format!("a partial answer\n\nAwaiting approval ({approval_id}): run the tool\n");
    post_placeholder_edit(&endpoint, &notice).await;
    deliver_and_ack(&mut conn, &stream).await;

    let outcome = await_reply(
        &mut stub,
        &mut conn,
        &stream,
        &original_id,
        PLACEHOLDER_TS,
        Duration::from_secs(3),
    )
    .await;

    let parked = match outcome {
        Outcome::AwaitingApproval(latest) => latest,
        other => {
            let _: i64 = redis::cmd("DEL")
                .arg(&stream)
                .query_async(&mut conn)
                .await
                .unwrap();
            panic!("a notice-only turn must park as AwaitingApproval, got {other:?}");
        }
    };
    assert_eq!(
        parse_approval_id(parked.as_deref().unwrap_or_default()).as_deref(),
        Some(approval_id),
        "the parked turn carries the notice the keep-alive parses its id from"
    );

    // The keep-alive path: the API appends the resume turn, the worker finalizes
    // it, and the resumed reply lands on this still-alive stub.
    let resume_event_id = format!("approval-{approval_id}-resolved");
    let resume = resume_turn(&resume_event_id, &endpoint);
    xadd(&mut conn, &stream, &resume).await.unwrap();
    deliver_and_ack(&mut conn, &stream).await;
    post_placeholder_edit(&endpoint, "the resolved answer").await;

    let observed = await_resume(
        &mut stub,
        &mut conn,
        &stream,
        &resume_event_id,
        &original_id,
        PLACEHOLDER_TS,
        Duration::from_secs(3),
    )
    .await;

    let _: i64 = redis::cmd("DEL")
        .arg(&stream)
        .query_async(&mut conn)
        .await
        .unwrap();

    assert!(
        observed.resolved,
        "the resume entry was observed (resolved)"
    );
    match observed.outcome {
        Outcome::Replied(reply) => assert_eq!(
            reply, "the resolved answer",
            "the route-bound turn's resumed reply is delivered, not stranded"
        ),
        other => panic!("expected the resumed Replied, got {other:?}"),
    }
}

/// #817: a model that emits a multi-paragraph approval summary produces a notice
/// whose blank line splits it across `\n\n` blocks. Before the fix the parse
/// returned `None`, so `await_reply` fell through to `Outcome::Replied(notice)`
/// (a FALSE SUCCESS -- the raw notice reported as the final answer, the exact
/// regression #766 closed) and never entered resume, stranding the resumed
/// reply. This drives the full path with that notice shape and asserts the turn
/// parks (never `Replied`), the id parses, and the keep-alive delivers the
/// resumed reply.
#[tokio::test]
async fn a_blank_line_summary_notice_parks_the_turn_and_the_keepalive_delivers_the_resumed_reply() {
    let Some(mut conn) = valkey_or_skip(
        "a_blank_line_summary_notice_parks_the_turn_and_the_keepalive_delivers_the_resumed_reply",
    )
    .await
    else {
        return;
    };
    let stream = unique_stream("curie:test:resume:");

    let mut stub = SlackStub::start("localhost", 0, "localhost").await.unwrap();
    let endpoint = stub.base_api_url().to_string();

    let original = synthetic_turn(
        "slack",
        "C-SIM-x",
        "U-curie-message",
        "do the risky thing",
        "1720000000.000100",
        PLACEHOLDER_TS,
        Some(endpoint.clone()),
    );
    let original_id = xadd(&mut conn, &stream, &original).await.unwrap();

    // The parked notice carries a multi-paragraph summary: the blank line inside
    // it splits the notice across `\n\n` blocks, so the trailing block is a
    // summary fragment rather than the marker-leading notice.
    let approval_id = "3f2504e0-4f89-41d3-9a0c-0305e82c3303";
    let notice = format!(
        "a partial answer\n\n\
         Awaiting approval ({approval_id}): first paragraph of the summary.\n\n\
         second paragraph of the summary.\n\
         The session is paused and will resume once an authorized member \
         resolves this request."
    );
    post_placeholder_edit(&endpoint, &notice).await;
    deliver_and_ack(&mut conn, &stream).await;

    let outcome = await_reply(
        &mut stub,
        &mut conn,
        &stream,
        &original_id,
        PLACEHOLDER_TS,
        Duration::from_secs(3),
    )
    .await;

    let parked = match outcome {
        Outcome::AwaitingApproval(latest) => latest,
        other => {
            let _: i64 = redis::cmd("DEL")
                .arg(&stream)
                .query_async(&mut conn)
                .await
                .unwrap();
            // A `Replied(notice)` here is the #817 false success being asserted
            // against: a blank-line summary must never report the notice as done.
            panic!("a blank-line summary notice must park as AwaitingApproval, got {other:?}");
        }
    };
    assert_eq!(
        parse_approval_id(parked.as_deref().unwrap_or_default()).as_deref(),
        Some(approval_id),
        "the id parses out of a multi-paragraph summary notice"
    );

    let resume_event_id = format!("approval-{approval_id}-resolved");
    let resume = resume_turn(&resume_event_id, &endpoint);
    xadd(&mut conn, &stream, &resume).await.unwrap();
    deliver_and_ack(&mut conn, &stream).await;
    post_placeholder_edit(&endpoint, "the resolved answer").await;

    let observed = await_resume(
        &mut stub,
        &mut conn,
        &stream,
        &resume_event_id,
        &original_id,
        PLACEHOLDER_TS,
        Duration::from_secs(3),
    )
    .await;

    let _: i64 = redis::cmd("DEL")
        .arg(&stream)
        .query_async(&mut conn)
        .await
        .unwrap();

    assert!(
        observed.resolved,
        "the resume entry was observed (resolved)"
    );
    match observed.outcome {
        Outcome::Replied(reply) => assert_eq!(
            reply, "the resolved answer",
            "the resumed reply is delivered, not stranded on the dead stub"
        ),
        other => panic!("expected the resumed Replied, got {other:?}"),
    }
}
