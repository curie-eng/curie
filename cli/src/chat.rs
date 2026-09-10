//! Shared Slack-stub machinery for driving the whole system end to end with no
//! Slack at all. Used by the `local message` and `cluster message` verbs.
//!
//! The CLI *is* the Slack service. It stands up a minimal Slack Web API stub on
//! a local port, XADDs the exact `QueuedTurn` the dispatcher would produce
//! onto the real Valkey stream (synthetic, internally-consistent ids since the
//! CLI itself is the endpoint that receives them back), then waits for the
//! worker to consume and finalize the turn. The caller prints the placeholder's
//! final text and exits 0, or on timeout prints stream diagnostics and exits
//! nonzero.
//!
//! Completion is the worker's XACK of our stream entry, not a timing guess: the
//! worker acks an entry only after the turn finalizes (its last `chat.update`
//! edit lands before the ack), so an acked entry means the turn is done and the
//! latest captured edit is the final reply. This avoids reporting a throttled
//! interim edit as the answer.
//!
//! The worker must run with `SLACK_API_BASE_URL` pointing at this stub's `/api/`
//! base: the worker reads that env var (`curie_worker.config`) and builds its
//! Slack sink's `AsyncWebClient(token=..., base_url=...)` against it, so its
//! `chat.update` edits land at this stub instead of real Slack. No Slack token,
//! channel, or real Slack HTTP on the CLI side.

use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use axum::extract::{Path, State};
use axum::http::header::CONTENT_TYPE;
use axum::http::HeaderMap;
use axum::routing::post;
use axum::{Json, Router};
use serde_json::json;
use tokio::net::TcpListener;
use tokio::sync::mpsc;

use crate::queue::{
    self, entry_acked, find_entry_by_event_id, synthetic_channel, synthetic_thread_and_placeholder,
    WORKER_GROUP,
};

pub const DEFAULT_STREAM: &str = queue::DEFAULT_STREAM;
pub const DEFAULT_VALKEY_URL: &str = queue::DEFAULT_VALKEY_URL;
pub const DEFAULT_USER: &str = "U-curie-chat";
pub const DEFAULT_TIMEOUT_SECS: u64 = 180;
pub const DEFAULT_LISTEN_HOST: &str = "localhost";
pub const DEFAULT_LISTEN_PORT: u16 = 0;

/// How often we check whether the worker has acked our entry.
const ACK_POLL_INTERVAL: Duration = Duration::from_millis(500);
/// Bounded drain after the ack to catch a final edit still in flight.
const FINAL_DRAIN: Duration = Duration::from_millis(100);

/// Hard per-call budget for the ack check inside [`await_reply`]. The check is
/// awaited in the poll arm's BODY, so while it is pending the deadline arm is not
/// polled and a half-open Valkey could hang the CLI (and its stub) well past
/// `--timeout-secs`. Bounding it makes an overrun read as "not acked yet" so the
/// loop re-checks the deadline, and the budget is further capped at the time LEFT
/// of that deadline. Four poll intervals is far beyond any healthy-latency
/// ack check on a multiplexed connection, so it cannot produce a false negative
/// under normal conditions.
const ACK_CALL_TIMEOUT: Duration = Duration::from_secs(2);

/// How often the keep-alive resume wait re-scans the runs stream for the
/// approval's resume entry (#766). Small enough to react promptly once the API
/// enqueues it; the scan is a cheap incremental XRANGE over the same connection
/// the enqueue already uses, and the whole wait stays bounded by the caller's
/// `--timeout-secs` (each sleep is capped at the time left of the deadline).
const RESUME_SCAN_INTERVAL: Duration = Duration::from_millis(200);

/// Hard per-scan budget for the resume XRANGE (#766, N3). A stalled or half-open
/// Valkey must not block past the caller's deadline the way the removed HTTP poll
/// could: a scan that overruns this is treated as "not found yet" and retried,
/// so `--timeout-secs` stays a hard bound by construction rather than deferring to the
/// OS TCP timeout. Two scan intervals is generous for a healthy multiplexed
/// connection while still bounding a dead peer.
const RESUME_SCAN_CALL_TIMEOUT: Duration = Duration::from_millis(400);

/// After this many consecutive failing/stalling scans, warn once so a persistently
/// unreachable Valkey is not silently reported as "approval still pending" (#766,
/// N4): the durable approval stays resolvable, but this CLI can no longer OBSERVE
/// its resolution, which is a different thing from "not resolved".
const SCAN_ERROR_WARN_THRESHOLD: u32 = 5;

/// Caps a per-call timeout budget at the time remaining before `deadline`, so
/// a fixed per-call timeout can never overrun the caller's overall deadline.
pub(crate) fn capped(budget: Duration, deadline: Instant) -> Duration {
    budget.min(deadline.saturating_duration_since(Instant::now()))
}

/// The shared prefix of every approval-card Approve action id
/// (`curie_dispatcher.approval_actions`). Its presence in a captured Slack call
/// body is the unambiguous signal that a turn parked awaiting approval and
/// posted a card (#529).
///
/// A PREFIX, deliberately, not one id: the card renders either
/// `curie-approval-approve` (resolve on click) or `curie-approval-approve-note`
/// (open a note dialog first, #1053), and this stub only needs to know a card
/// was posted, not which variant. Matching the prefix covers both by
/// construction -- the base Approve id IS this constant, and every note-or-later
/// variant extends it. The coupling to the dispatcher's ids is pinned by the
/// frozen vector `tests/vectors/approval-action-ids.json`, which this constant
/// is checked against in `tests::prefix_matches_the_frozen_action_id_vector`
/// below; the dispatcher's own constants are checked against the same file by
/// `test_action_ids_match_the_frozen_vector` in
/// `apps/dispatcher/tests/test_approval_actions.py`. A rename on either side is
/// therefore red rather than a silently blind detection (#1079).
pub const APPROVE_ACTION_ID_PREFIX: &str = "curie-approval-approve";

/// One captured Slack Web API call at the stub.
#[derive(Debug, Clone)]
pub struct SlackCall {
    pub method: String,
    pub channel: Option<String>,
    pub ts: Option<String>,
    pub text: Option<String>,
    /// True when the raw body carried the approval card's Approve action id, i.e.
    /// the worker parked this turn awaiting approval and posted a card (#529).
    pub approval_card: bool,
    /// The durable approval UUID carried by that card, when its structured
    /// payload contains a valid id. This is independent of placeholder edit
    /// ordering, so a card that arrives before its notice can still be resumed.
    pub approval_id: Option<String>,
}

/// If this call is a `chat.update` editing `placeholder_ts`, its new text.
pub fn placeholder_update_text<'a>(call: &'a SlackCall, placeholder_ts: &str) -> Option<&'a str> {
    if call.method == "chat.update" && call.ts.as_deref() == Some(placeholder_ts) {
        call.text.as_deref()
    } else {
        None
    }
}

/// Notify the caller immediately for each distinct edit to the tracked placeholder,
/// then retain that snapshot as the latest reply. Both the normal receive arm and
/// the final drain use this helper so their filtering and notification cannot drift.
fn observe_placeholder_update(
    call: &SlackCall,
    placeholder_ts: &str,
    latest: &mut Option<String>,
    observer: &mut impl FnMut(&str),
) {
    let Some(text) = placeholder_update_text(call, placeholder_ts) else {
        return;
    };
    if latest.as_deref() == Some(text) {
        return;
    }
    observer(text);
    *latest = Some(text.to_string());
}

/// Extract `channel`, `ts`, `text` from a Slack Web API request body, which
/// slack_sdk sends form-urlencoded by default and JSON when a param is complex.
pub fn extract_fields(
    content_type: &str,
    body: &str,
) -> (Option<String>, Option<String>, Option<String>) {
    if content_type.contains("application/json") {
        if let Ok(value) = serde_json::from_str::<serde_json::Value>(body) {
            let get = |k: &str| {
                value
                    .get(k)
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_string)
            };
            return (get("channel"), get("ts"), get("text"));
        }
    }
    let pairs: Vec<(String, String)> = serde_urlencoded::from_str(body).unwrap_or_default();
    let find = |k: &str| {
        pairs
            .iter()
            .find(|(key, _)| key == k)
            .map(|(_, v)| v.clone())
    };
    (find("channel"), find("ts"), find("text"))
}

/// Extract a validated durable approval id from a structured approval card.
///
/// The worker puts the same UUID in the card's `client_msg_id` and approval
/// action `value`. Neither copy is authoritative alone: both must be valid UUIDs
/// and equal, and the action id must start with [`APPROVE_ACTION_ID_PREFIX`].
/// This keeps incomplete, inconsistent, or ordinary Slack posts from being
/// mistaken for approval control data.
pub fn approval_card_id(content_type: &str, body: &str) -> Option<String> {
    fn valid_uuid(value: &str) -> Option<String> {
        uuid::Uuid::parse_str(value).ok().map(|_| value.to_string())
    }

    fn approval_action(value: &serde_json::Value) -> (bool, Option<String>) {
        match value {
            serde_json::Value::Array(values) => {
                let mut seen = false;
                for value in values {
                    let (nested_seen, id) = approval_action(value);
                    seen |= nested_seen;
                    if id.is_some() {
                        return (true, id);
                    }
                }
                (seen, None)
            }
            serde_json::Value::Object(fields) => {
                let is_approve = fields
                    .get("action_id")
                    .and_then(serde_json::Value::as_str)
                    .is_some_and(|id| id.starts_with(APPROVE_ACTION_ID_PREFIX));
                if is_approve {
                    let id = fields
                        .get("value")
                        .and_then(serde_json::Value::as_str)
                        .and_then(valid_uuid);
                    return (true, id);
                }
                let mut seen = false;
                for value in fields.values() {
                    let (nested_seen, id) = approval_action(value);
                    seen |= nested_seen;
                    if id.is_some() {
                        return (true, id);
                    }
                }
                (seen, None)
            }
            _ => (false, None),
        }
    }

    let (card_seen, action_id, client_msg_id) = if content_type.contains("application/json") {
        let root = serde_json::from_str::<serde_json::Value>(body).ok()?;
        let (seen, action_id) = approval_action(&root);
        let client_msg_id = root
            .get("client_msg_id")
            .and_then(serde_json::Value::as_str)
            .and_then(valid_uuid);
        (seen, action_id, client_msg_id)
    } else {
        let pairs: Vec<(String, String)> = serde_urlencoded::from_str(body).unwrap_or_default();
        let blocks = pairs
            .iter()
            .find(|(key, _)| key == "blocks")
            .and_then(|(_, value)| serde_json::from_str::<serde_json::Value>(value).ok());
        let (seen, action_id) = blocks
            .as_ref()
            .map(approval_action)
            .unwrap_or((false, None));
        let client_msg_id = pairs
            .iter()
            .find(|(key, _)| key == "client_msg_id")
            .and_then(|(_, value)| valid_uuid(value));
        (seen, action_id, client_msg_id)
    };

    match (card_seen, action_id, client_msg_id) {
        (true, Some(action_id), Some(client_msg_id)) if action_id == client_msg_id => {
            Some(action_id)
        }
        _ => None,
    }
}

#[derive(Clone)]
struct StubState {
    tx: mpsc::UnboundedSender<SlackCall>,
}

/// The embedded Slack Web API stub. Serves `POST /api/<method>` for any method,
/// captures the call, and answers `{"ok": true, ...}` so the worker's slack_sdk
/// never errors. A real Slack service does not 404 the worker, so the stub is
/// permissive by design.
pub struct SlackStub {
    base_api_url: String,
    calls: mpsc::UnboundedReceiver<SlackCall>,
    server: tokio::task::JoinHandle<()>,
}

impl Drop for SlackStub {
    fn drop(&mut self) {
        self.server.abort();
    }
}

impl SlackStub {
    /// Bind the stub on `bind_host:port` but advertise its base URL under
    /// `advertise_host`. `chat` passes the same value for both (it binds and is
    /// reached on the same local host); `message` binds `0.0.0.0` so an
    /// in-cluster worker can reach it, and advertises the routable host it
    /// detected. The advertised port is the actually-bound port, so an ephemeral
    /// `port: 0` still produces a correct URL.
    pub async fn start(bind_host: &str, port: u16, advertise_host: &str) -> Result<Self> {
        let listener = TcpListener::bind(format!("{bind_host}:{port}"))
            .await
            .with_context(|| {
                // A bound `port` (not 0) means the caller pinned a fixed listen
                // port, so a bind failure here is almost always "someone else is
                // already listening there" rather than an OS-level refusal. Two
                // things look identical from this error alone: another `local`/
                // `cluster message` that is legitimately running right now, or a
                // stale CLI process left over from a prior run whose stub never got
                // torn down (the #751 leak this fixes: a timed-out turn used to
                // exit via `std::process::exit`, which skips `Drop` and leaves the
                // listener bound). We cannot tell those apart from here, so name
                // both possibilities and point at how to check rather than
                // overclaiming which one it is.
                format!(
                    "binding the Slack stub on {bind_host}:{port}: the port is already in use. \
                     Either another `local message`/`cluster message` is genuinely running right \
                     now (safe to wait for it to finish, or rerun with a different \
                     --listen-port), or a previous invocation left a stale process still holding \
                     the port. Find the holder with `lsof -nP -iTCP:{port} -sTCP:LISTEN` (macOS) \
                     or `ss -ltnp 'sport = :{port}'` (Linux); if it is not a `message` invocation \
                     you expect to still be running, kill it and retry."
                )
            })?;
        let addr = listener
            .local_addr()
            .context("reading the stub's local addr")?;
        let (tx, calls) = mpsc::unbounded_channel();
        let app = Router::new()
            .route("/api/{method}", post(handle_call))
            .with_state(StubState { tx });
        let server = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        Ok(Self {
            base_api_url: format!("http://{advertise_host}:{}/api/", addr.port()),
            calls,
            server,
        })
    }

    /// The `SLACK_API_BASE_URL` the worker must point at.
    pub fn base_api_url(&self) -> &str {
        &self.base_api_url
    }

    /// Await the next captured call, or `None` if the stub has shut down.
    pub async fn recv(&mut self) -> Option<SlackCall> {
        self.calls.recv().await
    }
}

async fn handle_call(
    State(state): State<StubState>,
    Path(method): Path<String>,
    headers: HeaderMap,
    body: String,
) -> Json<serde_json::Value> {
    let content_type = headers
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("");
    let (channel, ts, text) = extract_fields(content_type, &body);
    // The approval card's Approve button carries this action id in the blocks,
    // which `extract_fields` does not parse -- match it on the raw body so an
    // awaiting-approval turn is detectable regardless of encoding (#529).
    let approval_card = body.contains(APPROVE_ACTION_ID_PREFIX);
    let approval_id = approval_card_id(content_type, &body);
    // chat.update echoes the existing ts; a hypothetical new-message call has no
    // ts, so synthesize one so the response still looks like Slack.
    let ts_out = ts
        .clone()
        .unwrap_or_else(|| synthetic_thread_and_placeholder().0);
    let _ = state.tx.send(SlackCall {
        method,
        channel: channel.clone(),
        ts: ts.clone(),
        text: text.clone(),
        approval_card,
        approval_id,
    });
    Json(json!({ "ok": true, "ts": ts_out, "channel": channel, "text": text }))
}

#[derive(Debug)]
pub enum Outcome {
    /// The worker finished the turn; the final placeholder text.
    Replied(String),
    /// The worker finished the turn but never edited the placeholder.
    CompletedNoEdit,
    /// The turn parked awaiting human approval: the worker posted an approval
    /// card (rather than finalizing) and persisted a durable `Approval` carrying
    /// THIS run's reply endpoint. For `local`/`cluster message` that endpoint is
    /// the CLI's throwaway stub, which dies when the command exits, so the resumed
    /// reply has nowhere to land (#529). Carries the latest placeholder text seen.
    AwaitingApproval {
        reply: Option<String>,
        /// Durable id captured from the approval card when it reaches this stub,
        /// or from the authoritative route-bound placeholder notice otherwise.
        approval_id: Option<String>,
    },
    /// The deadline passed with no completion.
    TimedOut,
}

/// Classify an acked turn after the final Slack-call drain.
///
/// Keeping the card-derived id separate from `latest` is load-bearing: Slack
/// calls can be observed in either order, and the card may be the only captured
/// call that carries the UUID needed to wait for the resume turn.
fn completed_turn_outcome(
    latest: Option<String>,
    approval_card_seen: bool,
    card_approval_id: Option<String>,
) -> Outcome {
    let approval_id = card_approval_id.or_else(|| latest.as_deref().and_then(parse_approval_id));
    if approval_card_seen || approval_id.is_some() {
        Outcome::AwaitingApproval {
            reply: latest,
            approval_id,
        }
    } else {
        latest.map_or(Outcome::CompletedNoEdit, Outcome::Replied)
    }
}

/// Wait for the worker to consume and finalize the turn. Terminal signal is the
/// XACK of our entry; the reply is the latest `chat.update` we captured. Shared
/// by `chat` and `message` (both stand up the same stub + enqueue + ack seam).
/// The observer sees each distinct matching edit immediately, but the returned
/// outcome still uses the latest edit only after XACK and the final drain.
///
/// The turn counts as awaiting approval on EITHER of two signals:
///
/// 1. An approval card was seen at this stub (the `APPROVE_ACTION_ID_PREFIX` in a
///    captured call). Fastest and unambiguous, but not always present.
/// 2. The latest placeholder text carries an authoritative trailing approval
///    notice ([`parse_approval_id`] validates the id as a UUID).
///
/// Signal 2 is load-bearing because the card does not always reach this stub:
/// the kernel posts it over the per-turn endpoint only when the approval route's
/// channel matches the requesting channel (`in_requesting_channel` /
/// `card_endpoint`, apps/worker/src/curie_worker/kernel.py). With a route bound
/// to a different channel the card rides the worker's DEFAULT transport, while
/// the placeholder notice always uses the per-turn endpoint -- so a route-bound
/// approval would otherwise be reported as a successful reply whose text is the
/// "Awaiting approval (...)" notice, stranding the resumed reply (#766).
pub async fn await_reply(
    stub: &mut SlackStub,
    conn: &mut redis::aio::MultiplexedConnection,
    stream: &str,
    entry_id: &str,
    placeholder_ts: &str,
    timeout: Duration,
    observer: &mut impl FnMut(&str),
) -> Outcome {
    let deadline = Instant::now() + timeout;
    let mut latest: Option<String> = None;
    // Whether the worker posted an approval card during this turn: the turn parked
    // awaiting approval rather than finalizing normally (#529).
    let mut awaiting_approval = false;
    let mut card_approval_id: Option<String> = None;
    let mut poll = tokio::time::interval(ACK_POLL_INTERVAL);
    loop {
        tokio::select! {
            call = stub.recv() => {
                if let Some(call) = call {
                    awaiting_approval |= call.approval_card;
                    if call.approval_id.is_some() {
                        card_approval_id = call.approval_id.clone();
                    }
                    observe_placeholder_update(&call, placeholder_ts, &mut latest, observer);
                }
            }
            _ = poll.tick() => {
                // An overrun (or a stalled connection) reads as "not acked yet",
                // so the loop returns to the deadline arm rather than hanging.
                // Cap the per-call budget at whatever is LEFT of the overall
                // deadline, so a slow ack check can never push past `--timeout-secs`
                // (a 2s fixed cap overran a short timeout by up to 2s).
                let ack_budget = capped(ACK_CALL_TIMEOUT, deadline);
                let acked = tokio::time::timeout(
                    ack_budget,
                    entry_acked(conn, stream, WORKER_GROUP, entry_id),
                )
                .await
                .unwrap_or(false);
                if acked {
                    // Drain any final edit still in flight before deciding.
                    while let Ok(Some(call)) = tokio::time::timeout(FINAL_DRAIN, stub.recv()).await {
                        awaiting_approval |= call.approval_card;
                        if call.approval_id.is_some() {
                            card_approval_id = call.approval_id.clone();
                        }
                        observe_placeholder_update(&call, placeholder_ts, &mut latest, observer);
                    }
                    // Either signal parks the turn: the card seen here, or an
                    // authoritative approval notice in the latest placeholder
                    // text (the route-bound case, where no card reaches us).
                    return completed_turn_outcome(
                        latest,
                        awaiting_approval,
                        card_approval_id,
                    );
                }
            }
            _ = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {
                return Outcome::TimedOut;
            }
        }
    }
}

/// Extract the approval UUID from the worker's placeholder notice.
///
/// Grounding (verified 2026-07-21 against this worktree,
/// `apps/worker/src/curie_worker/kernel.py:827-834`): the kernel builds
/// `notice = f"Awaiting approval ({created.id}): {summary}\n" + "The session is
/// paused and will resume once an authorized member resolves this request."` and
/// edits the placeholder to `f"{base}\n\n{notice}"` when a prior partial answer
/// exists, else to `notice` alone.
///
/// The parse is anchored to that STRUCTURE, not to a first/last occurrence of the
/// marker. `find` let base text shadow the real id; `rfind` let the model-supplied
/// `summary` (which FOLLOWS the id) shadow it just as easily. Instead:
///
/// 1. Split the text into `\n\n`-separated blocks. The authoritative notice
///    STARTS with the marker. The kernel appends it as the trailing logical
///    block, but rather than assuming it is the LAST block, anchor on the last
///    marker-leading block: a model-authored blank line inside the summary can
///    split the notice across `\n\n` boundaries, leaving a summary fragment as
///    the true last block (#817). The kernel now collapses the summary to one
///    line so this cannot happen in practice, but anchoring on the marker keeps
///    the parse robust to any future notice shape rather than to a fragile
///    "last block" assumption about its own control string.
/// 2. Require that block to be the only block starting with the marker: two
///    marker-leading blocks mean the text is ambiguous (a model that emitted a
///    whole notice-shaped block of its own), so no id is claimed.
/// 3. When the kernel's fixed trailing sentence appears in the text at all, it
///    must appear at or after that block (the sentence FOLLOWS the summary, so a
///    blank-line summary can push it into a later block) -- a tail that PRECEDES
///    the marker is not the kernel's structure and no id is claimed.
/// 4. Parse the UUID from that block's own marker.
///
/// Bias is deliberately toward a false negative: a WRONG id makes the CLI wait for
/// a resume event that never arrives and strands the reply, while `None` falls
/// back to the awaiting-approval terminal, which is truthful and retryable (#766).
pub fn parse_approval_id(placeholder_text: &str) -> Option<String> {
    const MARKER: &str = "Awaiting approval (";
    /// The kernel's fixed trailing sentence, appended verbatim after the summary.
    const NOTICE_TAIL: &str = "The session is paused and will resume once an authorized member \
                               resolves this request.";

    let blocks: Vec<&str> = placeholder_text.split("\n\n").collect();
    // The notice STARTS with the marker; anchor on the LAST marker-leading block
    // rather than the last block overall, so a blank-line summary that split the
    // notice across `\n\n` boundaries cannot let a trailing summary fragment
    // shadow it (#817).
    let (notice_idx, notice) = blocks
        .iter()
        .enumerate()
        .rev()
        .map(|(i, b)| (i, b.trim_start()))
        .find(|(_, b)| b.starts_with(MARKER))?;
    // ...and it is the ONLY marker-leading block; anything else is ambiguous.
    if blocks
        .iter()
        .filter(|b| b.trim_start().starts_with(MARKER))
        .count()
        != 1
    {
        return None;
    }
    // When the fixed sentence is present, it must appear at or after this block
    // (it FOLLOWS the summary, so a blank-line summary can push it into a later
    // block); a tail that precedes the marker is not the kernel's structure.
    if placeholder_text.contains(NOTICE_TAIL)
        && !blocks[notice_idx..].iter().any(|b| b.contains(NOTICE_TAIL))
    {
        return None;
    }
    let rest = &notice[MARKER.len()..];
    let end = rest.find(')')?;
    let candidate = &rest[..end];
    // The parenthesized middle counts only when it is a real UUID; a bare token
    // (or a notice-shaped false positive) is not an approval id.
    uuid::Uuid::parse_str(candidate)
        .ok()
        .map(|_| candidate.to_string())
}

/// Keep `stub` alive after [`Outcome::AwaitingApproval`] and wait for the turn
/// that resumes once a human resolves the approval (#766).
///
/// Mechanism: resolving an approval does not open a bespoke channel. The
/// platform API appends the resume turn onto the SAME runs stream this CLI
/// enqueued onto, under the deterministic event id `approval-<id>-resolved`
/// (`apps/api/src/curie_api/resumequeue.py`), replaying the original turn's
/// placeholder and this stub's reply endpoint. So the resume is just another
/// stream entry, and its completion is the ordinary ack-based signal:
///
/// 1. Scan the stream for `resume_event_id` every [`RESUME_SCAN_INTERVAL`],
///    bounded by `timeout`. Until a human resolves, nothing matches.
/// 2. Once the entry lands, delegate to [`await_reply`] on its stream id for the
///    time left. That is the identical path the original turn took, so the
///    resumed reply is reported only after the worker XACKs the entry -- i.e.
///    after the turn FINALIZES. A booting `chat.update` or a partial streaming
///    edit can never be printed as the final answer, and a reply that lands
///    while the scan is between iterations is still captured, because
///    `await_reply` tracks the latest placeholder edit continuously.
///
/// Carries [`await_reply`]'s [`Outcome`] verbatim (`Replied`, `CompletedNoEdit`,
/// `AwaitingApproval` if the resumed turn hit a NEW gate, or `TimedOut`) plus
/// `resolved`: whether the resume entry was ever observed on the stream. The flag
/// lets the caller distinguish "resolved, but the resumed turn did not finalize
/// before the deadline" (a plain timeout) from "never resolved" (the approval is
/// still pending), which must be reported to the operator as different terminals.
/// A timeout never mutates the durable `Approval` -- it stays pending and
/// resolvable later.
pub struct ResumeObservation {
    pub outcome: Outcome,
    pub resolved: bool,
}

/// The scan starts strictly AFTER `after_id` (the caller's own original turn entry
/// id), advancing an exclusive cursor to the last-seen entry each iteration, so an
/// ever-growing runs stream is never re-scanned from `-`. Each scan is wrapped in
/// [`RESUME_SCAN_CALL_TIMEOUT`] and the deadline is re-checked every iteration, so
/// a stalled Valkey cannot block past `timeout`. A failed or overrunning scan is
/// treated as "not found yet" and retried until the deadline, so a blip cannot be
/// misread as a resolution; a persistently failing scan warns once.
#[allow(clippy::too_many_arguments)]
pub async fn await_resume(
    stub: &mut SlackStub,
    conn: &mut redis::aio::MultiplexedConnection,
    stream: &str,
    resume_event_id: &str,
    after_id: &str,
    placeholder_ts: &str,
    timeout: Duration,
    observer: &mut impl FnMut(&str),
) -> ResumeObservation {
    let deadline = Instant::now() + timeout;
    let mut cursor = after_id.to_string();
    let mut scan_errors: u32 = 0;
    loop {
        // Every per-op budget is capped by what is LEFT of the overall deadline, so
        // the advertised `--timeout-secs` is a hard bound on this path too rather
        // than being overrun by up to one fixed scan budget.
        let scan_budget = capped(RESUME_SCAN_CALL_TIMEOUT, deadline);
        let scan = tokio::time::timeout(
            scan_budget,
            find_entry_by_event_id(conn, stream, resume_event_id, &cursor),
        )
        .await;
        let found = match scan {
            Ok(Ok(scan)) => {
                scan_errors = 0;
                if let Some(last) = scan.last_seen {
                    cursor = last;
                }
                scan.found
            }
            // A scan Err (Valkey blip) or an elapsed per-scan timeout (stalled
            // Valkey) both count as "not found yet"; the overall deadline stays
            // the hard bound.
            Ok(Err(_)) | Err(_) => {
                scan_errors += 1;
                None
            }
        };
        if let Some(resume_stream_id) = found {
            let remaining = deadline.saturating_duration_since(Instant::now());
            let outcome = await_reply(
                stub,
                conn,
                stream,
                &resume_stream_id,
                placeholder_ts,
                remaining,
                observer,
            )
            .await;
            return ResumeObservation {
                outcome,
                resolved: true,
            };
        }
        // Warn exactly once when observation has been failing for a while, so a
        // down Valkey is not silently reported as "still pending" (N4).
        if scan_errors == SCAN_ERROR_WARN_THRESHOLD {
            crate::ui::ui().warn(
                "the resume-stream scan keeps failing (Valkey slow or unreachable); the durable \
                 approval stays resolvable, but this CLI can no longer observe its resolution",
            );
        }
        if Instant::now() >= deadline {
            return ResumeObservation {
                outcome: Outcome::TimedOut,
                resolved: false,
            };
        }
        // Same cap on the idle sleep: never sleep past the deadline.
        tokio::time::sleep(capped(RESUME_SCAN_INTERVAL, deadline)).await;
    }
}

pub fn continue_hint_line(verb: &str) -> String {
    format!("continue this conversation: curie {verb} --continue \"...\"")
}

pub fn continue_hint_long_line(verb: &str, channel: &str, thread_ts: &str) -> String {
    format!(
        "continue this conversation: curie {verb} --channel '{channel}' --thread {thread_ts} \"...\""
    )
}

/// Resolve the channel and timestamps for a turn: an explicit `--channel` is
/// carried verbatim (so the worker's exact-equality binding can route to a
/// deployed agent) and an explicit `--thread` continues that thread; each falls
/// back to a fresh synthetic value when absent. The placeholder ts is always
/// synthetic (the CLI owns the placeholder message). Returns
/// `(channel, thread_ts, placeholder_ts)`.
pub fn resolve_targets(channel: Option<&str>, thread: Option<&str>) -> (String, String, String) {
    let channel = channel.map_or_else(synthetic_channel, str::to_string);
    let (synthetic_thread_ts, placeholder_ts) = synthetic_thread_and_placeholder();
    let thread_ts = thread.map_or(synthetic_thread_ts, str::to_string);
    (channel, thread_ts, placeholder_ts)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;

    /// The frozen action-id vector, parsed strictly. `deny_unknown_fields` is
    /// the whole point: a key this lane cannot see would pass vacuously, which
    /// is the exact drift the gate exists to catch. The Python lane rejects
    /// unknown keys the same way, via `_EXPECTED_VECTOR_KEYS` in
    /// `apps/dispatcher/tests/test_approval_actions.py`.
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct ActionIdVector {
        /// The file-level rationale; parsed so it is not an unknown field.
        /// Underscore-prefixed so rustc's dead_code lint skips it; the serde
        /// rename keeps the JSON key it matches on as `comment`.
        #[serde(rename = "comment")]
        _comment: String,
        approve_action_id_prefix: String,
    }

    /// The Rust half of the cross-language approval action-id gate (#1079).
    ///
    /// `APPROVE_ACTION_ID_PREFIX` is an independent Rust literal, so nothing but
    /// this pins it to the prefix the dispatcher's card actually renders. The
    /// dispatcher's own constants are checked against the same file by
    /// `test_action_ids_match_the_frozen_vector` in
    /// `apps/dispatcher/tests/test_approval_actions.py`, so a rename in one
    /// language without the vector fails that language's test. The rule is not
    /// restated here: it lives in the vector file.
    #[test]
    fn prefix_matches_the_frozen_action_id_vector() {
        let raw = std::fs::read_to_string(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../tests/vectors/approval-action-ids.json"
        ))
        .expect("read tests/vectors/approval-action-ids.json");
        let parsed: ActionIdVector = serde_json::from_str(&raw).unwrap_or_else(|err| {
            panic!(
                "parse tests/vectors/approval-action-ids.json: {err}\n\
                 An unknown field is rejected on purpose: a key this lane cannot see \
                 would pass vacuously. Teach the new key to ActionIdVector here, to \
                 _EXPECTED_VECTOR_KEYS in \
                 apps/dispatcher/tests/test_approval_actions.py, and to both lanes' \
                 assertions."
            )
        });
        let prefix = parsed.approve_action_id_prefix.as_str();

        assert_eq!(
            prefix, APPROVE_ACTION_ID_PREFIX,
            "the frozen approve_action_id_prefix ({prefix}) no longer matches this \
             CLI's prefix ({APPROVE_ACTION_ID_PREFIX}); `body.contains(...)` would \
             stop detecting a posted approval card and `local message` would stop \
             reporting an awaiting-approval turn"
        );
    }

    #[test]
    fn extract_fields_reads_form_and_json_bodies() {
        let (channel, ts, text) = extract_fields(
            "application/x-www-form-urlencoded",
            "token=xoxb-x&channel=C1&ts=1.2&text=hi+there",
        );
        assert_eq!(channel.as_deref(), Some("C1"));
        assert_eq!(ts.as_deref(), Some("1.2"));
        assert_eq!(text.as_deref(), Some("hi there"));

        let (channel, ts, text) = extract_fields(
            "application/json; charset=utf-8",
            r#"{"channel":"C2","ts":"3.4","text":"done"}"#,
        );
        assert_eq!(channel.as_deref(), Some("C2"));
        assert_eq!(ts.as_deref(), Some("3.4"));
        assert_eq!(text.as_deref(), Some("done"));
    }

    #[test]
    fn approval_card_id_requires_equal_structured_uuid_copies() {
        let id = "00000000-0000-4000-8000-000000000220";
        let body = format!(
            r#"{{"channel":"C0EXAMPLE1","client_msg_id":"{id}","blocks":[{{"type":"actions","elements":[{{"type":"button","action_id":"{APPROVE_ACTION_ID_PREFIX}","value":"{id}"}}]}}]}}"#
        );
        let captured = approval_card_id("application/json", &body);
        assert_eq!(captured.as_deref(), Some(id));

        // The card can win the Slack-call race while the latest placeholder is
        // still ordinary text. Preserve that text for output, but carry the
        // card's durable id independently so the caller enters its resume wait.
        match completed_turn_outcome(Some("working".into()), true, captured) {
            Outcome::AwaitingApproval { reply, approval_id } => {
                assert_eq!(reply.as_deref(), Some("working"));
                assert_eq!(approval_id.as_deref(), Some(id));
            }
            outcome => panic!("approval card was not classified as awaiting: {outcome:?}"),
        }
    }

    #[test]
    fn approval_card_id_rejects_malformed_and_non_card_payloads() {
        let id = "00000000-0000-4000-8000-000000000220";
        let non_card = format!(
            r#"{{"client_msg_id":"{id}","text":"mentions {APPROVE_ACTION_ID_PREFIX}","blocks":[{{"type":"section","text":{{"type":"plain_text","text":"not a card"}}}}]}}"#
        );
        assert_eq!(approval_card_id("application/json", &non_card), None);

        let malformed = format!(
            r#"{{"client_msg_id":"not-a-uuid","blocks":[{{"type":"actions","elements":[{{"type":"button","action_id":"{APPROVE_ACTION_ID_PREFIX}","value":"also-not-a-uuid"}}]}}]}}"#
        );
        assert_eq!(approval_card_id("application/json", &malformed), None);

        // The raw action-id signal still reports a parked turn, but malformed
        // external data is never promoted into an id used for resume lookup.
        match completed_turn_outcome(None, true, approval_card_id("application/json", &malformed)) {
            Outcome::AwaitingApproval { approval_id, .. } => assert_eq!(approval_id, None),
            outcome => panic!("malformed card lost its awaiting status: {outcome:?}"),
        }
    }

    #[test]
    fn approval_card_id_rejects_mismatched_or_missing_uuid_copies() {
        let client_id = "00000000-0000-4000-8000-000000000220";
        let action_id = "00000000-0000-4000-8000-000000000221";
        let mismatched = format!(
            r#"{{"client_msg_id":"{client_id}","blocks":[{{"type":"actions","elements":[{{"type":"button","action_id":"{APPROVE_ACTION_ID_PREFIX}","value":"{action_id}"}}]}}]}}"#
        );
        let missing_client = format!(
            r#"{{"blocks":[{{"type":"actions","elements":[{{"type":"button","action_id":"{APPROVE_ACTION_ID_PREFIX}","value":"{action_id}"}}]}}]}}"#
        );
        let missing_action_value = format!(
            r#"{{"client_msg_id":"{client_id}","blocks":[{{"type":"actions","elements":[{{"type":"button","action_id":"{APPROVE_ACTION_ID_PREFIX}"}}]}}]}}"#
        );

        for body in [&mismatched, &missing_client, &missing_action_value] {
            assert!(body.contains(APPROVE_ACTION_ID_PREFIX));
            assert_eq!(approval_card_id("application/json", body), None);
            match completed_turn_outcome(None, true, approval_card_id("application/json", body)) {
                Outcome::AwaitingApproval { approval_id, .. } => assert_eq!(approval_id, None),
                outcome => panic!("untrusted card lost its awaiting status: {outcome:?}"),
            }
        }
    }

    #[test]
    fn placeholder_update_text_matches_only_the_right_call() {
        let update = SlackCall {
            method: "chat.update".into(),
            channel: Some("C1".into()),
            ts: Some("1.2".into()),
            text: Some("the answer".into()),
            approval_card: false,
            approval_id: None,
        };
        assert_eq!(placeholder_update_text(&update, "1.2"), Some("the answer"));
        // Wrong ts (a different message).
        assert_eq!(placeholder_update_text(&update, "9.9"), None);
        // Wrong method (a new-message post, not an edit).
        let post = SlackCall {
            method: "chat.postMessage".into(),
            ..update.clone()
        };
        assert_eq!(placeholder_update_text(&post, "1.2"), None);
    }

    #[test]
    fn continue_hint_is_the_short_form() {
        let line = continue_hint_line("cluster message");

        assert!(line.contains("--continue"));
        assert!(line.contains("curie cluster message"));
        assert!(!line.contains("--channel"));
        assert!(!line.contains("--thread"));
    }

    #[test]
    fn long_fallback_hint_quotes_the_channel() {
        let line = continue_hint_long_line("local message", "#local-dev", "123.45");

        assert!(line.contains("'#local-dev'"));
        assert!(line.contains("--thread 123.45"));
    }

    // --- parse_approval_id (#766 AC-2) --------------------------------------
    // Grounding: apps/worker/src/curie_worker/kernel.py:922 edits the
    // placeholder to `f"Awaiting approval ({created.id}): {summary}\n..."`, and
    // kernel.py:929 wraps it as `f"{base}\n\n{notice}"` when a prior partial
    // answer exists. Verified 2026-07-21 against this worktree's kernel.py.

    #[test]
    fn parse_approval_id_reads_the_notice() {
        let id = "3f2504e0-4f89-41d3-9a0c-0305e82c3301";
        let text = format!(
            "Awaiting approval ({id}): do risky thing\n\
             The session is paused and will resume once an authorized member \
             resolves this request."
        );
        assert_eq!(parse_approval_id(&text).as_deref(), Some(id));
    }

    #[test]
    fn parse_approval_id_reads_a_human_sentence_tail() {
        // #2565: a bundle-authored sentence replaces the machine JSON in the
        // notice tail. The parser still only needs the UUID head.
        let id = "44c2cc21-7b0a-4f3e-9c1d-aaaaaaaaaaaa";
        let text = format!(
            "Awaiting approval ({id}): File FY26Q1: 11 workbooks into Approved, \
             25 cells going out blank. Approve?\n\
             The session is paused and will resume once an authorized member \
             resolves this request."
        );
        assert_eq!(parse_approval_id(&text).as_deref(), Some(id));
    }

    #[test]
    fn parse_approval_id_survives_the_base_prefix() {
        // The real shape when a prior answer exists: `f"{base}\n\n{notice}"`
        // (kernel.py:929). The id must still be recovered after the prefix.
        let id = "00000000-0000-4000-8000-000000000000";
        let text = format!(
            "Here is the partial answer so far.\n\n\
             Awaiting approval ({id}): run the deploy\n\
             The session is paused and will resume once an authorized member \
             resolves this request."
        );
        assert_eq!(parse_approval_id(&text).as_deref(), Some(id));
    }

    #[test]
    fn parse_approval_id_none_without_a_notice() {
        assert_eq!(parse_approval_id("just a normal reply, no gate here"), None);
    }

    #[test]
    fn parse_approval_id_rejects_a_non_uuid_middle() {
        // The parenthesized middle must validate as a UUID; a bare token is not
        // an approval id and must not be surfaced as one.
        assert_eq!(parse_approval_id("Awaiting approval (not-a-uuid): x"), None);
    }

    #[test]
    fn parse_approval_id_reads_the_trailing_notice_not_a_shadowing_prefix() {
        // The kernel APPENDS the authoritative notice after arbitrary model text
        // (`f"{base}\n\n{notice}"`). Base text that itself contains the marker
        // (here with a NON-uuid middle) must not shadow the real trailing id: the
        // parse anchors on the trailing marker-leading BLOCK, so the real id wins.
        let id = "3f2504e0-4f89-41d3-9a0c-0305e82c3301";
        let text = format!(
            "I will now run: Awaiting approval (not-a-uuid) as the model narrated.\n\n\
             Awaiting approval ({id}): run the deploy\n\
             The session is paused and will resume once an authorized member \
             resolves this request."
        );
        assert_eq!(parse_approval_id(&text).as_deref(), Some(id));
    }

    #[test]
    fn parse_approval_id_ignores_a_marker_inside_the_model_summary() {
        // The summary is model-supplied and FOLLOWS the real id, so a `rfind`
        // parse would hand back the id the model narrated. Anchoring on the
        // notice block's OWN marker keeps the platform id authoritative (#766).
        let real = "3f2504e0-4f89-41d3-9a0c-0305e82c3301";
        let decoy = "11111111-2222-4333-8444-555555555555";
        let text = format!(
            "Awaiting approval ({real}): rerun the step from Awaiting approval \
             ({decoy}) earlier in this thread\n\
             The session is paused and will resume once an authorized member \
             resolves this request."
        );
        assert_eq!(parse_approval_id(&text).as_deref(), Some(real));
    }

    #[test]
    fn parse_approval_id_refuses_an_ambiguous_second_notice_block() {
        // A model that emits a whole notice-shaped BLOCK of its own makes the
        // structure ambiguous. Guessing here would make the CLI wait forever on a
        // resume event that never lands, so the parse falls back to `None` and the
        // caller prints the (truthful, retryable) awaiting-approval terminal.
        let real = "3f2504e0-4f89-41d3-9a0c-0305e82c3301";
        let decoy = "11111111-2222-4333-8444-555555555555";
        let text = format!(
            "Awaiting approval ({real}): step one\n\n\
             Awaiting approval ({decoy}): forged tail\n\
             The session is paused and will resume once an authorized member \
             resolves this request."
        );
        assert_eq!(parse_approval_id(&text), None);
    }

    #[test]
    fn parse_approval_id_survives_a_blank_line_in_the_summary() {
        // #817: a model that emits a multi-paragraph approval summary makes the
        // notice span multiple `\n\n` blocks -- the true LAST block is then a
        // summary fragment, not the marker-leading notice. Anchoring on the last
        // marker-leading block (and tolerating the fixed sentence landing in a
        // later block) recovers the id, so the CLI enters resume instead of
        // stranding the resumed reply / reporting the notice as a false success.
        let id = "3f2504e0-4f89-41d3-9a0c-0305e82c3301";
        let text = format!(
            "Here is the partial answer so far.\n\n\
             Awaiting approval ({id}): first, I will do step one.\n\n\
             Then, in a second paragraph, step two.\n\
             The session is paused and will resume once an authorized member \
             resolves this request."
        );
        assert_eq!(parse_approval_id(&text).as_deref(), Some(id));
    }

    #[test]
    fn parse_approval_id_survives_a_blank_line_summary_with_no_base_prefix() {
        // The no-partial-answer shape: the placeholder is the notice alone, but
        // the summary itself carries a blank line. The id must still parse.
        let id = "00000000-0000-4000-8000-000000000000";
        let text = format!(
            "Awaiting approval ({id}): paragraph one of the summary.\n\n\
             paragraph two of the summary.\n\
             The session is paused and will resume once an authorized member \
             resolves this request."
        );
        assert_eq!(parse_approval_id(&text).as_deref(), Some(id));
    }

    #[test]
    fn parse_approval_id_refuses_a_notice_tail_outside_the_trailing_block() {
        // The fixed sentence present, but not on the block the id was read from:
        // not the kernel's structure, so no id is claimed.
        let id = "3f2504e0-4f89-41d3-9a0c-0305e82c3301";
        let text = format!(
            "The session is paused and will resume once an authorized member \
             resolves this request.\n\n\
             Awaiting approval ({id}): run the deploy"
        );
        assert_eq!(parse_approval_id(&text), None);
    }

    // --- SlackStub teardown on a timed-out turn (#751) ----------------------
    //
    // A timed-out `local message`/`cluster message` used to hold this stub's
    // bound port for as long as the timeout arm's post-timeout diagnostics
    // gather took -- and that gather read straight from the SAME Valkey the
    // worker never acked against, with no timeout of its own, so a stalled
    // Valkey (a likely cause of the original timeout) could hang it
    // indefinitely. The fix (`message::message_local` / `message::message`)
    // now drops the stub's listener FIRST, before that gather even starts, so
    // the port is released no matter how long (or whether) anything after it
    // takes. `SlackStub::drop` aborts the server task, which is cooperative
    // cancellation (the task actually unbinds the listener once the runtime
    // next schedules it, not necessarily synchronously) -- this test confirms
    // that happens promptly: start the stub, drop it exactly as the timeout
    // arm now does immediately on detecting `Outcome::TimedOut`, then confirm
    // the port becomes bindable again in this same process well within a
    // fraction of a second, rather than staying held for however long a
    // downstream diagnostics hang would otherwise last.

    #[tokio::test]
    async fn dropping_the_stub_after_a_timed_out_turn_frees_the_port_promptly() {
        let stub = SlackStub::start("127.0.0.1", 0, "127.0.0.1")
            .await
            .expect("binding an ephemeral port must succeed");
        // Recover the actually-bound port from the advertised URL (ephemeral
        // port 0 resolves to whatever the OS assigned), the same way a real
        // `local message` turn's stub would carry its port forward.
        let port: u16 = stub
            .base_api_url()
            .rsplit_once("127.0.0.1:")
            .and_then(|(_, rest)| rest.split('/').next())
            .and_then(|p| p.parse().ok())
            .expect("base_api_url carries the bound port");

        // A second bind while the stub is still alive must fail -- otherwise
        // this test would not be exercising the real OS-level port hold at all.
        assert!(
            TcpListener::bind(("127.0.0.1", port)).await.is_err(),
            "the port must be genuinely held while the stub is alive"
        );

        // This is the exact first step the timeout arms now take on detecting
        // `Outcome::TimedOut`, before the diagnostics gather that used to be
        // able to hang: drop the stub's listener.
        drop(stub);

        // Task abortion is cooperative -- the server task unbinds the listener
        // once the runtime schedules it, which is not guaranteed to have
        // happened by the very next instruction. Poll for a short, bounded
        // window rather than asserting on the first attempt; a real leak would
        // never clear across this whole window, while the fix clears it almost
        // immediately.
        let deadline = std::time::Instant::now() + Duration::from_millis(500);
        let rebound = loop {
            match TcpListener::bind(("127.0.0.1", port)).await {
                Ok(listener) => break Ok(listener),
                Err(_) if std::time::Instant::now() < deadline => {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                    continue;
                }
                Err(err) => break Err(err),
            }
        };
        assert!(
            rebound.is_ok(),
            "the port must become free within a fraction of a second of the stub being dropped, \
             not leaked past a timed-out turn (#751): {:?}",
            rebound.err()
        );
    }
}
