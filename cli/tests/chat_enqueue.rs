//! Integration: the chat enqueue seam against real Valkey, and the embedded
//! Slack stub exercised over real HTTP.
//!
//! The redis client is NOT mocked: the XADD runs against the Valkey selected by
//! `TEST_VALKEY_URL`, which is the compose dev Valkey locally (host port 26379,
//! password `valkeypass`) and a passwordless Valkey in CI. It uses a unique
//! test-scoped stream that the test deletes afterward and never touches the real
//! `curie:runs` stream.
//! The Slack stub is the real one; only its client (reqwest) stands in for the
//! worker.

use std::time::Duration;

use curie::chat::{resolve_targets, SlackStub};
use curie::message::{enqueue_over_connected_transport, MessageOpts};
use curie::queue::{diagnostics, entry_acked, synthetic_turn, xadd, WORKER_GROUP};
use curie::slack::{post_placeholder, SlackTransport};
use curie::state::{load_turn, TurnContext, TurnVerb};
use curie_aci_protocol::QueuedTurn;

mod support;
use support::{unique_stream, valkey_or_skip, Response};

/// `SLACK_API_BASE_URL` is process-global, so every test in this binary that
/// redirects it holds this lock for the whole redirect. Without it two such tests
/// running concurrently would each post at the other's stub (or, after the
/// other's `remove_var`, at real Slack). A tokio mutex, not a std one, because
/// the redirect is held across `await` points.
static SLACK_BASE_URL_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[tokio::test]
async fn xadd_lands_the_exact_seam_shape_on_real_valkey() {
    let Some(mut conn) = valkey_or_skip("xadd_lands_the_exact_seam_shape_on_real_valkey").await
    else {
        return;
    };
    let stream = unique_stream("curie:test:chat:");

    let event = synthetic_turn(
        "slack",
        "C-SIM-x",
        "U-curie-chat",
        "hello world",
        "111.100",
        "111.200",
        None,
    );
    let stream_id = xadd(&mut conn, &stream, &event).await.unwrap();
    assert!(!stream_id.is_empty(), "XADD returned an id");

    // Read the entry back with XRANGE: [ (id, [field, value, ...]) ].
    let entries: Vec<(String, Vec<(String, String)>)> = redis::cmd("XRANGE")
        .arg(&stream)
        .arg("-")
        .arg("+")
        .query_async(&mut conn)
        .await
        .unwrap();

    // Clean up before asserting so a failure never leaks a test stream.
    let _: i64 = redis::cmd("DEL")
        .arg(&stream)
        .query_async(&mut conn)
        .await
        .unwrap();

    assert_eq!(entries.len(), 1, "exactly one entry enqueued");
    let (entry_id, fields) = &entries[0];
    assert_eq!(entry_id, &stream_id, "read-back id matches the XADD id");

    // The single-`payload`-field encoding is the frozen seam.
    assert_eq!(fields.len(), 1, "exactly one field on the entry");
    assert_eq!(fields[0].0, "payload", "the field is named payload");

    // The payload JSON round-trips into the same event with the exact
    // dispatcher-side field names.
    let decoded: QueuedTurn = serde_json::from_str(&fields[0].1).unwrap();
    assert_eq!(decoded, event);

    let value: serde_json::Value = serde_json::from_str(&fields[0].1).unwrap();
    let mut keys: Vec<&str> = value
        .as_object()
        .unwrap()
        .keys()
        .map(String::as_str)
        .collect();
    keys.sort_unstable();
    assert_eq!(
        keys,
        vec![
            "author",
            "conversation_id",
            // ADR-0115: absent (null) for a turn the CLI mints -- the CLI
            // never delegates -- but still present in the serialized shape.
            "delegation",
            "event_id",
            "received_at",
            "reply_handle",
            "source",
            "text",
        ]
    );
    assert!(
        decoded.event_id.starts_with("EvSIM-"),
        "synthetic id keeps its collision-proof prefix on the wire"
    );
}

#[tokio::test]
async fn explicit_channel_and_thread_land_verbatim_on_the_wire() {
    // --channel must reach the worker byte-for-byte (its binding is exact
    // equality on agents.slack_channel) and --thread must become the event's
    // thread_ts so a follow-up continues the same thread.
    let Some(mut conn) =
        valkey_or_skip("explicit_channel_and_thread_land_verbatim_on_the_wire").await
    else {
        return;
    };
    let stream = unique_stream("curie:test:chat:");

    let (channel, thread_ts, placeholder_ts) =
        resolve_targets(Some("CSIM12345"), Some("1720000000.000100"));
    assert_eq!(channel, "CSIM12345", "explicit channel is carried verbatim");
    assert_eq!(
        thread_ts, "1720000000.000100",
        "explicit thread becomes thread_ts"
    );

    let event = synthetic_turn(
        "slack",
        &channel,
        "U-curie-chat",
        "hi",
        &thread_ts,
        &placeholder_ts,
        None,
    );
    let stream_id = xadd(&mut conn, &stream, &event).await.unwrap();

    let decoded = drain_one_turn(&mut conn, &stream).await;

    assert!(!stream_id.is_empty(), "XADD returned an id");
    assert_eq!(
        decoded.reply_handle.channel, "CSIM12345",
        "the XADD'd payload keeps the exact channel the worker binds on"
    );
    assert_eq!(
        decoded.conversation_id, "1720000000.000100",
        "the XADD'd payload keeps the exact thread_ts"
    );
}

#[tokio::test]
async fn absent_channel_and_thread_fall_back_to_synthetic() {
    // With neither flag, resolve_targets mints a synthetic channel and a fresh
    // slack-shaped thread ts, preserving the pre-flag behavior.
    let Some(mut conn) = valkey_or_skip("absent_channel_and_thread_fall_back_to_synthetic").await
    else {
        return;
    };
    let stream = unique_stream("curie:test:chat:");

    let (channel, thread_ts, placeholder_ts) = resolve_targets(None, None);
    assert!(
        channel.starts_with("C-SIM-"),
        "synthetic channel: {channel}"
    );
    assert_ne!(thread_ts, placeholder_ts, "thread and placeholder differ");
    let (secs, micros) = thread_ts.split_once('.').expect("slack-shaped thread ts");
    assert!(secs.parse::<u64>().is_ok(), "secs numeric: {thread_ts}");
    assert_eq!(micros.len(), 6, "micros width: {thread_ts}");

    let event = synthetic_turn(
        "slack",
        &channel,
        "U-curie-chat",
        "hi",
        &thread_ts,
        &placeholder_ts,
        None,
    );
    let stream_id = xadd(&mut conn, &stream, &event).await.unwrap();

    let decoded = drain_one_turn(&mut conn, &stream).await;

    assert!(!stream_id.is_empty(), "XADD returned an id");
    assert!(
        decoded.reply_handle.channel.starts_with("C-SIM-"),
        "synthetic channel on the wire: {}",
        decoded.reply_handle.channel
    );
    assert_eq!(
        decoded.conversation_id, thread_ts,
        "synthetic thread_ts round-trips onto the stream"
    );
}

#[tokio::test]
async fn diagnostics_reports_stream_length_and_consumer_group_state() {
    // The timeout path's value is showing whether a consumer group consumed the
    // entry. Prove it against real Valkey with a real group over a test stream.
    let Some(mut conn) =
        valkey_or_skip("diagnostics_reports_stream_length_and_consumer_group_state").await
    else {
        return;
    };
    let stream = unique_stream("curie:test:chat:");

    let event = synthetic_turn("slack", "C-SIM-x", "U-curie-chat", "hi", "1.1", "1.2", None);
    let stream_id = xadd(&mut conn, &stream, &event).await.unwrap();

    // The worker's group name; create it at 0 so it sees the existing entry.
    let _: () = redis::cmd("XGROUP")
        .arg("CREATE")
        .arg(&stream)
        .arg("curie-workers")
        .arg("0")
        .query_async(&mut conn)
        .await
        .unwrap();

    let report = diagnostics(&mut conn, &stream, &stream_id).await;

    let _: i64 = redis::cmd("DEL")
        .arg(&stream)
        .query_async(&mut conn)
        .await
        .unwrap();

    assert!(report.contains(&stream), "names the stream:\n{report}");
    assert!(report.contains(&stream_id), "names our entry:\n{report}");
    assert!(report.contains("XLEN 1"), "reports length:\n{report}");
    assert!(
        report.contains("curie-workers"),
        "surfaces the consumer group:\n{report}"
    );
    assert!(
        report.contains("XPENDING"),
        "includes pending state:\n{report}"
    );
}

#[tokio::test]
async fn entry_acked_tracks_the_worker_consuming_and_acking() {
    // chat's completion signal: our entry is "done" only once the worker group
    // has delivered AND acked it. Drive the group lifecycle by hand and assert
    // entry_acked flips exactly at the ack.
    let Some(mut conn) = valkey_or_skip("entry_acked_tracks_the_worker_consuming_and_acking").await
    else {
        return;
    };
    let stream = unique_stream("curie:test:chat:");
    let event = synthetic_turn("slack", "C-SIM-x", "U-curie-chat", "hi", "1.1", "1.2", None);
    let stream_id = xadd(&mut conn, &stream, &event).await.unwrap();

    // No group yet: not acked.
    assert!(!entry_acked(&mut conn, &stream, WORKER_GROUP, &stream_id).await);

    let _: () = redis::cmd("XGROUP")
        .arg("CREATE")
        .arg(&stream)
        .arg(WORKER_GROUP)
        .arg("0")
        .query_async(&mut conn)
        .await
        .unwrap();
    // Group exists but has not delivered the entry: not acked.
    assert!(!entry_acked(&mut conn, &stream, WORKER_GROUP, &stream_id).await);

    // The worker reads the entry (delivered, now pending-unacked): still not done.
    let _: redis::Value = redis::cmd("XREADGROUP")
        .arg("GROUP")
        .arg(WORKER_GROUP)
        .arg("worker-1")
        .arg("COUNT")
        .arg(10)
        .arg("STREAMS")
        .arg(&stream)
        .arg(">")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert!(!entry_acked(&mut conn, &stream, WORKER_GROUP, &stream_id).await);

    // The worker acks after finalizing the turn: now done.
    let _: i64 = redis::cmd("XACK")
        .arg(&stream)
        .arg(WORKER_GROUP)
        .arg(&stream_id)
        .query_async(&mut conn)
        .await
        .unwrap();
    let acked = entry_acked(&mut conn, &stream, WORKER_GROUP, &stream_id).await;

    let _: i64 = redis::cmd("DEL")
        .arg(&stream)
        .query_async(&mut conn)
        .await
        .unwrap();

    assert!(
        acked,
        "entry_acked must be true once the group has acked the entry"
    );
}

#[tokio::test]
async fn stub_captures_form_encoded_chat_update_over_real_http() {
    // slack_sdk posts chat.update form-encoded by default; the stub must parse
    // it, capture it, and answer ok:true so the worker's SDK never errors.
    let mut stub = SlackStub::start("localhost", 0, "localhost").await.unwrap();
    let base = stub.base_api_url().to_string();
    assert!(base.ends_with("/api/"), "base url: {base}");

    let http = reqwest::Client::new();
    let resp: serde_json::Value = http
        .post(format!("{base}chat.update"))
        .form(&[
            ("channel", "C1"),
            ("ts", "1720000000.000200"),
            ("text", "the answer"),
        ])
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(resp["ok"], true);
    assert_eq!(resp["ts"], "1720000000.000200");

    let call = tokio::time::timeout(Duration::from_secs(2), stub.recv())
        .await
        .expect("stub captured a call in time")
        .expect("stub is still up");
    assert_eq!(call.method, "chat.update");
    assert_eq!(call.channel.as_deref(), Some("C1"));
    assert_eq!(call.ts.as_deref(), Some("1720000000.000200"));
    assert_eq!(call.text.as_deref(), Some("the answer"));
}

#[tokio::test]
async fn stub_captures_json_body_and_never_404s_other_methods() {
    let mut stub = SlackStub::start("localhost", 0, "localhost").await.unwrap();
    let base = stub.base_api_url().to_string();
    let http = reqwest::Client::new();

    // A JSON-bodied call to an unexpected method (e.g. a future postMessage)
    // must still be captured and answered ok, not 404'd.
    let resp = http
        .post(format!("{base}chat.postMessage"))
        .json(&serde_json::json!({"channel": "C2", "text": "escalation"}))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["ok"], true);

    let call = tokio::time::timeout(Duration::from_secs(2), stub.recv())
        .await
        .expect("stub captured a call in time")
        .expect("stub is still up");
    assert_eq!(call.method, "chat.postMessage");
    assert_eq!(call.channel.as_deref(), Some("C2"));
    assert_eq!(call.text.as_deref(), Some("escalation"));
}

/// #954: `post_placeholder`'s `thread_ts` argument must actually reach the
/// outbound `chat.postMessage` body. Only the pure `post_body` was covered, so a
/// mutation dropping the argument inside `post_placeholder` (calling
/// `post_body(channel, text, None)`) passed the whole suite with no lint. This
/// drives the real client over real HTTP against the recording HTTP stub and pins
/// both branches from the request bodies that actually went out.
///
/// Valkey-free by design, this path uses only the recording HTTP stub. CI also
/// starts Valkey and executes the Valkey-backed tests in this file. Contributors
/// need a reachable Valkey, such as the compose Valkey, for equivalent local
/// coverage.
#[tokio::test]
async fn post_placeholder_threads_the_outbound_post_only_when_a_thread_is_named() {
    let _slack_base = SLACK_BASE_URL_LOCK.lock().await;
    const NAMED_THREAD: &str = "1717171717.000100";
    let slack =
        support::serve(|_req| Response::json(200, r#"{"ok": true, "ts": "1717171717.000900"}"#));
    // The destination is an argument now (#1030), not an ambient read. Exporting
    // the variable that used to decide it is left in place deliberately: if the
    // ambient read ever comes back, these posts land at real Slack instead of the
    // stub and the recorded-request assertions below fail.
    std::env::set_var("SLACK_API_BASE_URL", "http://127.0.0.1:1/never-used");
    let transport = SlackTransport::new(Some(slack.base_url.clone()), "xoxb-test".into());

    let ts = post_placeholder(&transport, "C-real", "\u{2026}", Some(NAMED_THREAD))
        .await
        .expect("the stub answers ok with a ts");
    assert!(!ts.is_empty(), "post_placeholder returned the created ts");

    post_placeholder(&transport, "C-real", "\u{2026}", None)
        .await
        .expect("the stub answers ok with a ts");

    std::env::remove_var("SLACK_API_BASE_URL");

    let posts = slack.recorded();
    assert_eq!(posts.len(), 2, "exactly two placeholders posted");

    assert_eq!(posts[0].path, "/chat.postMessage");
    let threaded: serde_json::Value = serde_json::from_slice(&posts[0].body).unwrap();
    assert_eq!(
        threaded.get("channel").and_then(|v| v.as_str()),
        Some("C-real")
    );
    assert_eq!(
        threaded.get("thread_ts").and_then(|v| v.as_str()),
        Some(NAMED_THREAD),
        "the named thread must reach Slack, or the placeholder lands outside the thread"
    );

    assert_eq!(posts[1].path, "/chat.postMessage");
    let top_level: serde_json::Value = serde_json::from_slice(&posts[1].body).unwrap();
    assert_eq!(
        top_level.get("thread_ts"),
        None,
        "a top-level placeholder must carry no thread_ts at all"
    );
}

/// A `MessageOpts` for the connected-transport enqueue. The connected path binds
/// no stub, forwards no port, and calls no platform API, so only `channel`,
/// `thread`, `stream`, `user`, and `text` are asserted here. The rest are not
/// unused: `persist_and_hint` reads `namespace`, `release`, `chart`,
/// `listen_host`, `timeout_secs`, `api_url`, and `api_key` into
/// `.curie/last-turn.json`, so they are consumed on this path, just not asserted.
fn connected_opts(stream: &str, thread: Option<&str>) -> MessageOpts {
    MessageOpts {
        text: "what is the deploy status".to_string(),
        channel: Some("C-REAL-CONNECTED".to_string()),
        thread: thread.map(str::to_string),
        user: "U-curie-message".to_string(),
        stream: stream.to_string(),
        ..Default::default()
    }
}

/// Read the single entry on `stream` back as a `QueuedTurn`, deleting the stream
/// first so a failed assertion never leaks it.
async fn drain_one_turn(conn: &mut redis::aio::MultiplexedConnection, stream: &str) -> QueuedTurn {
    let entries: Vec<(String, Vec<(String, String)>)> = redis::cmd("XRANGE")
        .arg(stream)
        .arg("-")
        .arg("+")
        .query_async(conn)
        .await
        .unwrap();
    let _: i64 = redis::cmd("DEL")
        .arg(stream)
        .query_async(conn)
        .await
        .unwrap();
    assert_eq!(entries.len(), 1, "exactly one entry enqueued");
    serde_json::from_str(&entries[0].1[0].1).unwrap()
}

/// Restores the process working directory when dropped, so a panicking
/// assertion inside the scope cannot strand the rest of the binary in a temp
/// directory.
struct CwdGuard(std::path::PathBuf);

impl Drop for CwdGuard {
    fn drop(&mut self) {
        let _ = std::env::set_current_dir(&self.0);
    }
}

/// Drive one real connected-transport enqueue against an HTTP Slack stub whose
/// canned `chat.postMessage` response returns `canned_ts`, and hand back the turn
/// that actually landed on the stream, the requests the stub recorded, and the
/// turn context the enqueue persisted for `--continue`. The two cases below
/// differ only in the thread they name and the ts Slack answers with.
async fn enqueue_connected(
    conn: &mut redis::aio::MultiplexedConnection,
    thread: Option<&str>,
    canned_ts: &'static str,
) -> (QueuedTurn, Vec<support::Request>, TurnContext) {
    let slack = support::serve(move |_req| {
        Response::json(200, &format!(r#"{{"ok": true, "ts": "{canned_ts}"}}"#))
    });
    std::env::set_var("SLACK_API_BASE_URL", &slack.base_url);

    // The real enqueue persists `.curie/last-turn.json` under the PROCESS
    // working directory, so run it inside a throwaway dir: a plain `cargo test`
    // must never clobber the developer's own saved turn and break their next
    // `message --continue`. Declared before the guard so the guard (dropped
    // first) restores the cwd before the temp dir is removed. The caller holds
    // SLACK_BASE_URL_LOCK across this whole call, which serializes the cwd
    // change against the only other test in this binary that touches
    // process-global state.
    let state_home = tempfile::tempdir().expect("temp dir for the persisted turn state");
    let _cwd = CwdGuard(std::env::current_dir().expect("resolving the current working directory"));
    std::env::set_current_dir(state_home.path()).expect("pointing the cwd at the temp dir");

    let stream = unique_stream("curie:test:connected:");
    let opts = connected_opts(&stream, thread);
    enqueue_over_connected_transport(
        &opts,
        conn,
        TurnVerb::Local,
        "C-REAL-CONNECTED",
        &SlackTransport::new(std::env::var("SLACK_API_BASE_URL").ok(), "xoxb-test".into()),
    )
    .await
    .expect("the connected enqueue completes against the stub and Valkey");

    let saved = load_turn(state_home.path())
        .expect("the persisted turn state parses")
        .expect("the enqueue persisted a turn context");

    let turn = drain_one_turn(conn, &stream).await;
    let posts = slack.recorded();
    std::env::remove_var("SLACK_API_BASE_URL");
    (turn, posts, saved)
}

/// #954: the connected-transport enqueue is a COMPOSITION -- it posts a real
/// Slack placeholder and then mints the turn that goes on the wire, and the bug
/// was that the two disagreed (a real placeholder ts on the reply handle, a
/// SYNTHETIC `conversation_id` beside it), so the worker's approval card could
/// not thread under the placeholder. Pinning the turn builder alone did not close
/// it: a mutation rebuilding the old `resolve_targets` wiring inside
/// `enqueue_over_connected_transport` still passed the whole suite, leaving only
/// an unused-function lint as the signal.
///
/// So drive the REAL `enqueue_over_connected_transport` against real Valkey with
/// an HTTP stub standing in for Slack (the only external seam), and assert on the
/// bytes that actually landed on the stream. The placeholder ts is the stub's
/// canned wire response, never a value this test computed, so a turn built from a
/// synthetic ts cannot match it.
///
/// Both cases live in one test because they share the process-global
/// `SLACK_API_BASE_URL` redirect.
#[tokio::test]
async fn connected_transport_enqueues_the_real_placeholder_as_the_conversation_id() {
    let _slack_base = SLACK_BASE_URL_LOCK.lock().await;
    let Some(mut conn) =
        valkey_or_skip("connected_transport_enqueues_the_real_placeholder_as_the_conversation_id")
            .await
    else {
        return;
    };

    // --- No --thread: the placeholder's own ts IS the thread root, so it must be
    // the conversation_id too.
    const TOP_LEVEL_TS: &str = "1717171717.000700";
    let (turn, posts, saved) = enqueue_connected(&mut conn, None, TOP_LEVEL_TS).await;

    assert_eq!(
        turn.conversation_id, TOP_LEVEL_TS,
        "with no --thread the conversation_id must be the real placeholder ts \
         Slack returned, not a synthetic one: the worker threads its approval card \
         on conversation_id, and Slack drops a card threaded under a ts that names \
         no message"
    );
    assert_eq!(
        turn.reply_handle.placeholder.as_deref(),
        Some(turn.conversation_id.as_str()),
        "the two coordinates on one turn must agree; #954 was exactly their disagreement"
    );
    assert_eq!(
        turn.reply_handle.channel, "C-REAL-CONNECTED",
        "the real channel rides the wire verbatim"
    );
    assert_eq!(
        turn.reply_handle.endpoint, None,
        "connected mode carries no per-turn endpoint, so the reply rides the \
         workspace transport"
    );
    assert_eq!(posts.len(), 1, "exactly one placeholder posted");
    let body: serde_json::Value = serde_json::from_slice(&posts[0].body).unwrap();
    assert_eq!(posts[0].path, "/chat.postMessage");
    assert_eq!(
        body.get("channel").and_then(|v| v.as_str()),
        Some("C-REAL-CONNECTED")
    );
    assert_eq!(
        body.get("thread_ts"),
        None,
        "a top-level placeholder names no parent thread"
    );
    assert_eq!(
        saved.thread_ts, TOP_LEVEL_TS,
        "the persisted `--continue` coordinates must name the same real \
         placeholder ts, or the next `message --continue` resumes a thread that \
         names no Slack message"
    );
    assert_eq!(
        saved.channel, "C-REAL-CONNECTED",
        "the persisted turn records the real channel"
    );

    // --- Explicit --thread: the named ts is the conversation_id AND the outbound
    // post must land inside that same thread. Both halves of the invariant, from
    // the same run.
    const NAMED_THREAD: &str = "1700000000.000123";
    const IN_THREAD_TS: &str = "1717171717.000900";
    let (turn, posts, saved) = enqueue_connected(&mut conn, Some(NAMED_THREAD), IN_THREAD_TS).await;

    assert_eq!(
        turn.conversation_id, NAMED_THREAD,
        "an explicit --thread is the conversation_id verbatim"
    );
    assert_eq!(
        turn.reply_handle.placeholder.as_deref(),
        Some(IN_THREAD_TS),
        "the reply handle still points at the real message Slack created inside \
         that thread, so the worker edits the right message"
    );
    assert_eq!(posts.len(), 1, "exactly one placeholder posted");
    let body: serde_json::Value = serde_json::from_slice(&posts[0].body).unwrap();
    assert_eq!(
        body.get("thread_ts").and_then(|v| v.as_str()),
        Some(NAMED_THREAD),
        "the outbound post must carry the named thread, or the placeholder lands \
         outside the thread the turn claims to run in"
    );
    assert_eq!(
        saved.thread_ts, NAMED_THREAD,
        "the persisted `--continue` coordinates must name the thread the turn \
         actually runs in, not the placeholder inside it"
    );
}
