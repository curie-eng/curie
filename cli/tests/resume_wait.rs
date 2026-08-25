//! Integration: the #766 keep-alive resume wait (`chat::await_resume`) against
//! real Valkey plus the embedded Slack stub over real HTTP.
//!
//! This is the behavioral coverage the mechanism pivot lost when the injectable
//! `await_resume_reply` seam was replaced by a concrete Valkey connection: it
//! drives the seam the way it actually runs. No compose stack is needed -- only a
//! reachable Valkey (the compose dev one on host port 26379 is fine) and the
//! in-process stub. The test uses a unique test-scoped stream it deletes
//! afterward, so it never touches the real `curie:runs` stream. When no Valkey
//! is reachable it SKIPS (like `chat_enqueue.rs`), so CI without the stack is
//! unaffected; run it locally with the compose Valkey up, or point
//! `TEST_VALKEY_URL` at another instance.

#[cfg(target_os = "linux")]
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::Duration;
#[cfg(target_os = "linux")]
use std::time::Instant;

use tokio::sync::Notify;

use curie::chat::{await_reply, await_resume, parse_approval_id, Outcome, SlackStub};
use curie::queue::{synthetic_turn, xadd, WORKER_GROUP};
use curie_aci_protocol::{QueuedTurn, ReplyHandle, TurnSource};

mod support;
use support::{unique_stream, valkey_or_skip};

const PLACEHOLDER_TS: &str = "1720000000.000200";

#[cfg(target_os = "linux")]
fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_curie")
}

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

    let mut observe_update = |_: &str| {};
    let observed = await_resume(
        &mut stub,
        &mut conn,
        &stream,
        resume_event_id,
        &original_id,
        PLACEHOLDER_TS,
        Duration::from_secs(3),
        &mut observe_update,
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
    let mut observe_update = |_: &str| {};
    let observed = await_resume(
        &mut stub,
        &mut conn,
        &stream,
        resume_event_id,
        &original_id,
        PLACEHOLDER_TS,
        Duration::from_millis(400),
        &mut observe_update,
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
    post_reply_edit(endpoint, PLACEHOLDER_TS, text).await;
}

async fn post_reply_edit(endpoint: &str, placeholder_ts: &str, text: &str) {
    let resp: serde_json::Value = reqwest::Client::new()
        .post(format!("{endpoint}chat.update"))
        .form(&[
            ("channel", "C-SIM-x"),
            ("ts", placeholder_ts),
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

#[tokio::test]
async fn await_reply_observes_matching_live_updates_but_returns_only_the_final_edit() {
    let Some(mut conn) = valkey_or_skip(
        "await_reply_observes_matching_live_updates_but_returns_only_the_final_edit",
    )
    .await
    else {
        return;
    };
    let stream = unique_stream("curie:test:resume:");

    let mut stub = SlackStub::start("localhost", 0, "localhost").await.unwrap();
    let endpoint = stub.base_api_url().to_string();
    let turn = synthetic_turn(
        "slack",
        "C-SIM-x",
        "U-curie-message",
        "show live progress",
        "1720000000.000100",
        PLACEHOLDER_TS,
        Some(endpoint.clone()),
    );
    let entry_id = xadd(&mut conn, &stream, &turn).await.unwrap();
    let mut producer_conn = conn.clone();

    let wrong_placeholder = "1720000000.999999";
    let wrong_text = "wrong placeholder update";
    let interim_text = "draft answer\n  -> [WebSearch] searching...";
    let final_text = "final answer";
    let observed_updates = Arc::new(Mutex::new(Vec::new()));
    let wait_started = Arc::new(Notify::new());
    let interim_observed = Arc::new(Notify::new());

    let wait_observed_updates = Arc::clone(&observed_updates);
    let wait_started_signal = Arc::clone(&wait_started);
    let interim_observed_signal = Arc::clone(&interim_observed);
    let wait = async {
        wait_started_signal.notify_one();
        let mut observe_update = move |text: &str| {
            wait_observed_updates.lock().unwrap().push(text.to_string());
            if text == interim_text {
                interim_observed_signal.notify_one();
            }
        };
        await_reply(
            &mut stub,
            &mut conn,
            &stream,
            &entry_id,
            PLACEHOLDER_TS,
            Duration::from_secs(3),
            &mut observe_update,
        )
        .await
    };

    let producer_observed_updates = Arc::clone(&observed_updates);
    let producer = async {
        wait_started.notified().await;
        post_reply_edit(&endpoint, wrong_placeholder, wrong_text).await;
        post_reply_edit(&endpoint, PLACEHOLDER_TS, interim_text).await;
        tokio::time::timeout(Duration::from_secs(2), interim_observed.notified())
            .await
            .expect(
                "the interim observer callback must run before the final edit and acknowledgement",
            );
        assert_eq!(
            producer_observed_updates.lock().unwrap().clone(),
            vec![interim_text.to_string()],
            "the interim callback runs immediately and filters another placeholder"
        );
        post_reply_edit(&endpoint, PLACEHOLDER_TS, final_text).await;
        deliver_and_ack(&mut producer_conn, &stream).await;
    };

    let (outcome, ()) = tokio::join!(wait, producer);

    let _: i64 = redis::cmd("DEL")
        .arg(&stream)
        .query_async(&mut conn)
        .await
        .unwrap();

    match outcome {
        Outcome::Replied(reply) => assert_eq!(
            reply, final_text,
            "the terminal outcome remains the final edit rather than the live preview"
        ),
        other => panic!("expected Replied, got a different terminal: {other:?}"),
    }
    let observed_updates = observed_updates.lock().unwrap().clone();
    assert_eq!(
        observed_updates,
        vec![interim_text.to_string(), final_text.to_string()],
        "the observer receives matching live updates in order and ignores another placeholder"
    );
    assert!(
        !observed_updates.iter().any(|text| text == wrong_text),
        "the observer never receives an update for another placeholder"
    );
}

#[cfg(target_os = "linux")]
async fn run_local_message_terminal(debug: bool) -> Option<String> {
    if support::valkey_url() != support::DEFAULT_VALKEY_URL {
        eprintln!(
            "skipping local_message_transport_narration_is_debug_only: the command uses compose Valkey defaults"
        );
        return None;
    }
    let mut conn = valkey_or_skip("local_message_transport_narration_is_debug_only").await?;
    let stream = unique_stream("curie:test:resume:");
    let workspace = tempfile::tempdir().expect("create isolated command directory");
    // `local message` now performs the producer write through a bounded
    // `docker compose run curie-dispatcher` process. Keep this terminal-focused
    // test hermetic by replacing only the Docker executable: ps/inspect expose
    // the same labels as a running local worker, while compose-run executes the
    // real one-shot dispatcher module against the real test Valkey. The module,
    // frozen payload parser, telemetry bootstrap, and dispatcher enqueue path
    // are therefore all real; no Valkey behavior is mocked.
    let fake_bin = workspace.path().join("bin");
    std::fs::create_dir(&fake_bin).expect("create fake docker bin directory");
    let fake_docker = fake_bin.join("docker");
    let python = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../.venv/bin/python");
    assert!(
        python.is_file(),
        "workspace Python is available for dispatcher one-shot test"
    );
    std::fs::write(
        &fake_docker,
        format!(
            "#!/bin/sh\ncase \"$1\" in\n  ps) echo curie-test-worker ;;\n  inspect) echo /tmp/curie-test-compose.yaml ;;\n  compose) exec '{}' -m curie_dispatcher.enqueue_once ;;\n  *) echo unsupported fake docker command >&2; exit 64 ;;\nesac\n",
            python.display()
        ),
    )
    .expect("write fake docker executable");
    {
        use std::os::unix::fs::PermissionsExt;
        let mut permissions = std::fs::metadata(&fake_docker)
            .expect("stat fake docker executable")
            .permissions();
        permissions.set_mode(0o700);
        std::fs::set_permissions(&fake_docker, permissions).expect("make fake docker executable");
    }
    let capture_path = workspace.path().join("terminal.log");
    let debug_flag = if debug { "--debug " } else { "" };
    let shell_command = format!(
        "exec {} {debug_flag}local message --color never --channel C-SIM-x --stream {} --timeout-secs 8 'show live progress'",
        bin(), stream
    );
    let mut child = Command::new("script")
        .args(["--quiet", "--return", "--flush", "--command"])
        .arg(shell_command)
        .arg(&capture_path)
        .current_dir(workspace.path())
        .env("TERM", "xterm-256color")
        .env("NO_COLOR", "1")
        .env(
            "PATH",
            format!(
                "{}:{}",
                fake_bin.display(),
                std::env::var("PATH").unwrap_or_default()
            ),
        )
        .env("VALKEY_HOST", "localhost")
        .env("VALKEY_PORT", "26379")
        .env("OTEL_EXPORTER_OTLP_ENDPOINT", "")
        .env_remove("CI")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("start local message in a terminal");

    let turn_deadline = Instant::now() + Duration::from_secs(5);
    let turn = loop {
        let entries: Vec<(String, Vec<(String, String)>)> = redis::cmd("XRANGE")
            .arg(&stream)
            .arg("-")
            .arg("+")
            .query_async(&mut conn)
            .await
            .unwrap();
        if let Some((_id, fields)) = entries.first() {
            let payload = fields
                .iter()
                .find(|(key, _)| key == "payload")
                .map(|(_, value)| value)
                .expect("queued turn carries payload");
            let turn: QueuedTurn = serde_json::from_str(payload).unwrap();
            break turn;
        }
        if child.try_wait().unwrap().is_some() || Instant::now() >= turn_deadline {
            let _ = child.kill();
            let _ = child.wait();
            let capture = std::fs::read_to_string(&capture_path).unwrap_or_default();
            panic!("local message did not enqueue a turn: {capture}");
        }
        tokio::time::sleep(Duration::from_millis(40)).await;
    };

    let endpoint = turn
        .reply_handle
        .endpoint
        .as_deref()
        .expect("local message turn carries the live reply endpoint");
    let placeholder = turn
        .reply_handle
        .placeholder
        .as_deref()
        .expect("local message turn carries its placeholder");
    let wrong_text = "wrong placeholder update";
    let interim_text = "draft answer\n  -> [WebSearch] searching...";
    let final_text = "final answer";
    post_reply_edit(endpoint, "1720000000.999999", wrong_text).await;
    post_reply_edit(endpoint, placeholder, interim_text).await;

    let render_deadline = Instant::now() + Duration::from_secs(3);
    loop {
        let capture = std::fs::read_to_string(&capture_path).unwrap_or_default();
        if capture.contains("[WebSearch] searching...") {
            assert!(
                !capture.contains(wrong_text),
                "another placeholder is never rendered as live progress"
            );
            break;
        }
        if child.try_wait().unwrap().is_some() || Instant::now() >= render_deadline {
            let _ = child.kill();
            let _ = child.wait();
            let _: redis::RedisResult<i64> =
                redis::cmd("DEL").arg(&stream).query_async(&mut conn).await;
            panic!(
                "the interim update was not visible before the final edit and acknowledgement: {capture}"
            );
        }
        tokio::time::sleep(Duration::from_millis(40)).await;
    }

    post_reply_edit(endpoint, placeholder, final_text).await;
    deliver_and_ack(&mut conn, &stream).await;

    let exit_deadline = Instant::now() + Duration::from_secs(5);
    let status = loop {
        if let Some(status) = child.try_wait().unwrap() {
            break status;
        }
        if Instant::now() >= exit_deadline {
            let _ = child.kill();
            let status = child.wait().unwrap();
            let capture = std::fs::read_to_string(&capture_path).unwrap_or_default();
            let _: redis::RedisResult<i64> =
                redis::cmd("DEL").arg(&stream).query_async(&mut conn).await;
            panic!("local message did not exit after XACK: {status:?}\n{capture}");
        }
        tokio::time::sleep(Duration::from_millis(40)).await;
    };
    let _: i64 = redis::cmd("DEL")
        .arg(&stream)
        .query_async(&mut conn)
        .await
        .unwrap();

    let capture = std::fs::read_to_string(&capture_path).unwrap();
    assert!(status.success(), "local message failed: {capture}");
    let interim_position = capture
        .find("[WebSearch] searching...")
        .expect("terminal capture includes the interim tool update");
    let final_position = capture
        .rfind(final_text)
        .expect("terminal capture includes the final reply");
    assert!(
        interim_position < final_position,
        "the live tool update is visible before the terminal reply"
    );
    assert!(
        !capture.contains(wrong_text),
        "the terminal never renders another placeholder"
    );
    Some(capture)
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn local_message_transport_narration_is_debug_only() {
    let Some(default_capture) = run_local_message_terminal(false).await else {
        return;
    };
    let debug_capture = run_local_message_terminal(true)
        .await
        .expect("the debug run uses the same reachable Valkey as the default run");

    let transport_lines = [
        "slack stub listening; the worker posts to",
        "routing to channel C-SIM-x",
        "enqueued EvSIM-",
        "waiting up to 8s for the worker to finalize the turn...",
    ];
    for line in transport_lines {
        assert!(
            !default_capture.contains(line),
            "default output must hide transport narration {line:?}: {default_capture}"
        );
        assert!(
            debug_capture.contains(line),
            "--debug output must include transport narration {line:?}: {debug_capture}"
        );
    }
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

    let mut observe_update = |_: &str| {};
    let outcome = await_reply(
        &mut stub,
        &mut conn,
        &stream,
        &original_id,
        PLACEHOLDER_TS,
        Duration::from_secs(3),
        &mut observe_update,
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
        &mut observe_update,
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

    let mut observe_update = |_: &str| {};
    let outcome = await_reply(
        &mut stub,
        &mut conn,
        &stream,
        &original_id,
        PLACEHOLDER_TS,
        Duration::from_secs(3),
        &mut observe_update,
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
        &mut observe_update,
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
