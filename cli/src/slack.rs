//! A minimal Slack Web API client for the connected-transport `message` path
//! (#770/ADR-0078).
//!
//! `local`/`cluster message` normally mint a throwaway in-process reply stub.
//! When a real workspace is connected, we instead post a real placeholder
//! message to the channel over the workspace bot token and enqueue the turn
//! against its real `ts`, so the worker edits that message in place and the
//! approval card threads under it -- the card and the resumed reply ride the
//! connected transport with no stub. This module owns just that one outbound
//! call.

use anyhow::{Context, Result};

/// Real Slack, used when the resolved transport names no base of its own.
pub const DEFAULT_API_BASE: &str = "https://slack.com/api";

/// The Slack transport to post a placeholder over: a bot token and the base URL
/// it belongs to, resolved TOGETHER from one source (#1030).
///
/// The two used to come from unrelated places. The token was read from the
/// release Secret (cluster) or from `docker inspect` of the worker (local), while
/// the base URL was read from `SLACK_API_BASE_URL` in the CLI's OWN process
/// environment. A developer with `export SLACK_API_BASE_URL=http://localhost:8155/api/`
/// left over from stub testing therefore sent a production workspace bot token to
/// their local stub, whose entire purpose is to log what it receives. No attacker
/// and no misconfiguration of the cluster was required.
///
/// Pairing them in one value is what removes that: a caller cannot construct a
/// transport without saying where both halves came from, and there is no ambient
/// read left for a stale shell variable to win.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SlackTransport {
    /// The API base the resolved token belongs to.
    pub api_base: String,
    /// The workspace bot token. Sent only in the Authorization header.
    pub bot_token: String,
}

impl SlackTransport {
    /// Build a transport, falling back to real Slack when the source named no base.
    ///
    /// An absent or empty base means the source did not configure one, which for
    /// both tiers means real Slack: the chart renders `SLACK_API_BASE_URL` only
    /// when `worker.slackApiBaseUrl` is non-empty, and a compose worker without it
    /// set talks to Slack directly. Falling back here rather than at each call site
    /// keeps one answer to "what did no value mean".
    pub fn new(api_base: Option<String>, bot_token: String) -> Self {
        let api_base = api_base
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| DEFAULT_API_BASE.to_string());
        Self {
            api_base,
            bot_token,
        }
    }
}

/// Post `text` to `channel` as the bot, returning the created message `ts` (the
/// placeholder the worker then edits in place). `thread_ts` posts the placeholder
/// INTO that thread; `None` posts it at the top level, where the created message's
/// own `ts` is the thread root. The token is sent only in the Authorization
/// header; it is never logged.
pub async fn post_placeholder(
    transport: &SlackTransport,
    channel: &str,
    text: &str,
    thread_ts: Option<&str>,
) -> Result<String> {
    let resp = reqwest::Client::new()
        .post(method_url(transport, "chat.postMessage"))
        .bearer_auth(&transport.bot_token)
        .json(&post_body(channel, text, thread_ts))
        .send()
        .await
        .context("posting the approval-turn placeholder to Slack")?
        .json::<serde_json::Value>()
        .await
        .context("decoding the Slack chat.postMessage response")?;
    parse_ts(&resp)
}

/// The absolute URL for a Slack Web API method on this transport.
///
/// Pure and separated from the request so the destination can be asserted without
/// a network round trip. That matters more than it looks: the defect in #1030 was
/// entirely about which base a request went to, and a guard that needs an HTTP
/// stub to run is a guard that does not run in the fast suite.
fn method_url(transport: &SlackTransport, method: &str) -> String {
    let base = transport.api_base.trim_end_matches('/');
    format!("{base}/{method}")
}

/// The `chat.postMessage` request body. `thread_ts` is an OPTIONAL Slack
/// parameter: sending it threads the new message under that parent, omitting it
/// posts at the top level. It must be OMITTED rather than sent as null when we
/// have no parent -- a null would be a malformed value, not "no thread". Pure, so
/// both branches are unit tested without a network round trip.
fn post_body(channel: &str, text: &str, thread_ts: Option<&str>) -> serde_json::Value {
    let mut body = serde_json::Map::new();
    body.insert("channel".into(), channel.into());
    body.insert("text".into(), text.into());
    if let Some(thread_ts) = thread_ts {
        body.insert("thread_ts".into(), thread_ts.into());
    }
    serde_json::Value::Object(body)
}

/// Extract the created message `ts` from a `chat.postMessage` response, turning
/// an `{"ok": false, "error": ...}` body into an actionable error. Pure, so it
/// is unit tested without a network round trip.
fn parse_ts(resp: &serde_json::Value) -> Result<String> {
    if resp.get("ok").and_then(serde_json::Value::as_bool) != Some(true) {
        let err = resp
            .get("error")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("unknown error");
        anyhow::bail!("Slack chat.postMessage failed: {err}");
    }
    resp.get("ts")
        .and_then(serde_json::Value::as_str)
        .map(str::to_string)
        .context("Slack chat.postMessage returned ok but no message ts")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parse_ts_returns_the_ts_on_ok() {
        assert_eq!(
            parse_ts(&json!({"ok": true, "ts": "1717171717.001900"})).unwrap(),
            "1717171717.001900"
        );
    }

    #[test]
    fn parse_ts_surfaces_the_slack_error_on_not_ok() {
        let err = parse_ts(&json!({"ok": false, "error": "channel_not_found"})).unwrap_err();
        assert!(err.to_string().contains("channel_not_found"), "{err}");
    }

    #[test]
    fn parse_ts_errors_when_ok_but_no_ts() {
        assert!(parse_ts(&json!({"ok": true})).is_err());
    }

    #[test]
    fn post_body_threads_the_placeholder_when_a_parent_is_given() {
        // Observed behavior of the real dispatcher against real Slack
        // (packages/channel-gateway/src/curie_channel_gateway/slack/handlers.py): it passes
        // `thread_ts=` to chat.postMessage so the placeholder lands IN the thread,
        // and enqueues that same value as the turn's conversation_id.
        let body = post_body("C-real", "\u{2026}", Some("1717171717.000100"));
        assert_eq!(body["channel"], "C-real");
        assert_eq!(body["thread_ts"], "1717171717.000100");
    }

    #[test]
    fn post_body_omits_the_thread_key_entirely_at_top_level() {
        // Same source: a root message carries no thread_ts at all, and its OWN ts
        // is the thread root. The key must be absent, not null -- Slack would
        // reject a null as a malformed thread_ts.
        let body = post_body("C-real", "\u{2026}", None);
        assert_eq!(body["channel"], "C-real");
        assert!(
            body.get("thread_ts").is_none(),
            "thread_ts must be omitted, not null: {body}"
        );
    }

    #[test]
    fn a_transport_without_a_base_falls_back_to_real_slack() {
        assert_eq!(
            SlackTransport::new(None, "xoxb-x".into()).api_base,
            DEFAULT_API_BASE
        );
        // Empty and whitespace-only mean "the source configured nothing", which is
        // real Slack for both tiers: the chart renders SLACK_API_BASE_URL only when
        // worker.slackApiBaseUrl is non-empty.
        assert_eq!(
            SlackTransport::new(Some(String::new()), "xoxb-x".into()).api_base,
            DEFAULT_API_BASE
        );
        assert_eq!(
            SlackTransport::new(Some("   ".into()), "xoxb-x".into()).api_base,
            DEFAULT_API_BASE
        );
    }

    #[test]
    fn the_request_url_comes_from_the_transport_and_never_from_the_environment() {
        // The #1030 regression, asserted on the destination itself rather than on
        // the value that feeds it. An earlier revision of this fix had no test at
        // this layer: restoring the ambient read inside the request builder left
        // the whole suite green, because every other test asserted on the
        // constructor. Mutating the rule is what found that.
        let transport =
            SlackTransport::new(Some("https://proxy.example/api/".into()), "xoxb-x".into());
        std::env::set_var("SLACK_API_BASE_URL", "http://127.0.0.1:18081/api");
        assert_eq!(
            method_url(&transport, "chat.postMessage"),
            "https://proxy.example/api/chat.postMessage"
        );
        std::env::remove_var("SLACK_API_BASE_URL");
        // A trailing slash on the resolved base must not produce a double slash:
        // some Slack-compatible stubs route on the exact path.
        assert_eq!(
            method_url(
                &SlackTransport::new(Some("https://x/api///".into()), "t".into()),
                "chat.postMessage"
            ),
            "https://x/api/chat.postMessage"
        );
    }

    #[test]
    fn a_resolved_base_is_used_verbatim_and_the_ambient_env_cannot_win() {
        // The regression this pins (#1030): a stale SLACK_API_BASE_URL in the
        // operator's own shell used to decide where a real workspace token was
        // sent. There is no ambient read left, so setting it changes nothing.
        std::env::set_var("SLACK_API_BASE_URL", "http://127.0.0.1:18081/api");
        let transport =
            SlackTransport::new(Some("https://proxy.example/api".into()), "xoxb-x".into());
        assert_eq!(transport.api_base, "https://proxy.example/api");
        std::env::remove_var("SLACK_API_BASE_URL");
    }
}
