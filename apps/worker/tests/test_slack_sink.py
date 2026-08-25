"""SlackReplyAdapter: the Slack adapter of the neutral reply wire.

Migrated to ``emit`` in ADR-0096 phase 2 (T-B9). Every case here asserts the same
Slack behavior it did before the seam swap -- mrkdwn conversion, Block Kit
rendering, the text-only fallbacks, the #530 transport fallback, #708's
best-effort swallow, the approval card and the settled card. What changed is the
CALL: the kernel now emits ``reply.update`` / ``reply.post`` / ``turn.status``
over a ``TargetRoute``, and this adapter turns each into the Slack call that
expresses it.

One narrowing is new and is asserted rather than assumed (D4.4): a per-turn
endpoint is honored only at the CONFIGURED Slack origin. The #19 / #530 cases
that used to name a second ORIGIN now name a second PATH within the trusted one,
which is the shape that still exercises them; the refusal itself is pinned in
test_reply_sink.py.
"""

from __future__ import annotations

import asyncio
import json

import aiohttp
import pytest
from channel_protocol import (
    MESSAGE_VERSION,
    Action,
    ConfirmIntent,
    OutboundMessage,
)
from channel_protocol.reply import (
    REPLY_WIRE_VERSION,
    ReplyPost,
    ReplyTarget,
    ReplyUpdate,
    SettledOutcome,
    TurnStatus,
)
from curie_worker import slack_sink as slack_sink_module
from curie_worker.config import WorkerConfig
from curie_worker.reply_sink import TargetRoute
from curie_worker.slack_sink import SlackReplyAdapter, UntrustedSlackEndpointError
from slack_sdk.errors import SlackApiError

# The worker default and a SECOND PATH under the same origin. Before D4.4 these
# cases used a second origin ("http://stub:2/api/"); an endpoint there is now
# refused before any request, so the same behaviors are exercised against a
# trusted origin whose path moved or died -- the surviving #530 shape.
_DEFAULT = "http://default:1/api/"
_STUB = "http://default:1/stub/"
# A second path inside the REAL-Slack origin, used as a no-wander recorder by the
# no-default-configured cases below: with no ``base_url`` the trusted origin is
# real Slack, so this is the only second client those cases can legally build.
_ELSEWHERE = "https://slack.com/elsewhere/"


def _target(
    *, channel: str = "C1", ts: str | None = "1.1", thread: str | None = None
) -> ReplyTarget:
    return ReplyTarget(
        kind="slack", address=channel, conversation_id=thread, reply_ref=ts
    )


def _update(
    sink: SlackReplyAdapter,
    *,
    channel: str = "C1",
    ts: str = "1.1",
    text: str,
    endpoint: str | None = None,
    best_effort_unreachable: bool = False,
) -> object:
    return sink.emit(
        ReplyUpdate(
            version=REPLY_WIRE_VERSION,
            event="reply.update",
            target=_target(channel=channel, ts=ts),
            text=text,
        ),
        route=TargetRoute(endpoint=endpoint),
        best_effort_unreachable=best_effort_unreachable,
    )


def test_base_url_override_passes_through() -> None:
    sink = SlackReplyAdapter("xoxb-test", base_url="http://localhost:9999")
    assert sink._client_for(None).base_url == "http://localhost:9999/"


def test_unset_base_url_uses_the_sdk_default() -> None:
    sink = SlackReplyAdapter("xoxb-test")
    assert sink._client_for(None).base_url == "https://slack.com/api/"


def test_per_turn_endpoint_routes_to_a_distinct_cached_client() -> None:
    # Issue #19: a turn carrying its own endpoint must post through a client bound
    # to that endpoint, not the worker default; the client is cached per base URL
    # (built once, reused) since the SDK binds the endpoint at construction.
    sink = SlackReplyAdapter("xoxb-test", base_url=_DEFAULT)
    default = sink._client_for(None)
    per_turn = sink._client_for(_STUB)

    assert per_turn.base_url == _STUB
    assert per_turn is not default  # a distinct endpoint gets a distinct client
    assert sink._client_for(_STUB) is per_turn  # cached, not rebuilt
    assert sink._client_for(None) is default  # the default is stable too
    # An explicit-empty endpoint collapses onto the worker default, not a third client.
    assert sink._client_for("") is default


def test_update_routes_to_the_per_turn_endpoint() -> None:
    # The endpoint on the route selects which client posts the edit.
    sink = SlackReplyAdapter("xoxb-test", base_url=_DEFAULT)
    seen: list[str] = []

    def _record(label: str):
        async def _fake_chat_update(*, channel: str, ts: str, text: str) -> None:
            seen.append(label)

        return _fake_chat_update

    sink._client_for(None).chat_update = _record("default")  # type: ignore[method-assign]
    sink._client_for(_STUB).chat_update = _record("stub")  # type: ignore[method-assign]

    asyncio.run(_update(sink, text="a"))  # no endpoint -> default
    asyncio.run(_update(sink, ts="1.2", text="b", endpoint=_STUB))

    assert seen == ["default", "stub"]


def test_config_reads_slack_api_base_url_from_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("SLACK_API_BASE_URL", raising=False)
    assert WorkerConfig().slack_api_base_url == ""
    monkeypatch.setenv("SLACK_API_BASE_URL", "http://localhost:9999")
    assert WorkerConfig().slack_api_base_url == "http://localhost:9999"


def test_update_converts_markdown_to_mrkdwn() -> None:
    sink = SlackReplyAdapter("xoxb-test")
    captured: dict[str, str] = {}

    async def _fake_chat_update(*, channel: str, ts: str, text: str) -> None:
        captured.update(channel=channel, ts=ts, text=text)

    sink._client_for(None).chat_update = _fake_chat_update  # type: ignore[method-assign]

    asyncio.run(_update(sink, text="**hi** [x](http://y)"))

    assert captured["text"] == "*hi* <http://y|x>"
    assert "blocks" not in captured  # plain text path passes no blocks


def test_update_renders_blocks_for_a_reply_convention() -> None:
    sink = SlackReplyAdapter("xoxb-test")
    captured: dict[str, object] = {}

    async def _fake_chat_update(**kwargs: object) -> None:
        captured.update(kwargs)

    sink._client_for(None).chat_update = _fake_chat_update  # type: ignore[method-assign]

    text = '```curie-reply\n{"header": "Hi", "text": "body"}\n```'
    asyncio.run(_update(sink, text=text))

    assert isinstance(captured.get("blocks"), list)
    assert captured["blocks"][0]["type"] == "header"  # type: ignore[index]
    assert captured["text"] == "body"  # accessibility fallback, not raw JSON


def test_an_empty_status_sets_the_empty_assistant_status() -> None:
    # An EMPTY ``turn.status`` IS the clear on the neutral wire (EB-B6(a)): the
    # kernel no longer has a separate clear verb, and Slack's own clear has
    # always been "set it to empty".
    sink = SlackReplyAdapter("xoxb-test")
    captured: dict[str, str] = {}

    async def _fake_set_status(*, channel_id: str, thread_ts: str, status: str) -> None:
        captured.update(channel_id=channel_id, thread_ts=thread_ts, status=status)

    sink._client_for(None).assistant_threads_setStatus = _fake_set_status  # type: ignore[method-assign]

    asyncio.run(
        sink.emit(
            TurnStatus(
                version=REPLY_WIRE_VERSION,
                event="turn.status",
                target=_target(ts=None, thread="1.1"),
                status="",
            ),
            route=TargetRoute(),
        )
    )

    assert captured == {"channel_id": "C1", "thread_ts": "1.1", "status": ""}


def test_the_status_call_is_best_effort_on_error() -> None:
    sink = SlackReplyAdapter("xoxb-test")

    async def _boom(**_kwargs: object) -> None:
        raise RuntimeError("workspace has no assistant feature")

    sink._client_for(None).assistant_threads_setStatus = _boom  # type: ignore[method-assign]

    # Must not raise -- clearing the shimmer can never fail a completed turn.
    asyncio.run(
        sink.emit(
            TurnStatus(
                version=REPLY_WIRE_VERSION,
                event="turn.status",
                target=_target(ts=None, thread="1.1"),
                status="",
            ),
            route=TargetRoute(),
        )
    )


def test_turn_completed_sends_nothing_on_slack() -> None:
    # On Slack the edited message IS the delivery, so a completion signal would
    # be a second, contentless message in the thread. The event exists for
    # channels (email) whose reply is only sent at the end.
    from channel_protocol.reply import TurnCompleted

    sink = SlackReplyAdapter("xoxb-test")
    calls: list[object] = []

    async def _fake(**kwargs: object) -> None:
        calls.append(kwargs)

    sink._client_for(None).chat_update = _fake  # type: ignore[method-assign]
    sink._client_for(None).chat_postMessage = _fake  # type: ignore[method-assign]
    sink._client_for(None).assistant_threads_setStatus = _fake  # type: ignore[method-assign]

    ack = asyncio.run(
        sink.emit(
            TurnCompleted(
                version=REPLY_WIRE_VERSION,
                event="turn.completed",
                target=_target(),
                event_id="ev-1",
                outcome="delivered",
            ),
            route=TargetRoute(),
        )
    )

    assert calls == []
    assert ack.ref is None


# --- #31: no-edit streaming config + status/links pass-through ----------------


def test_config_no_edit_streaming_defaults_false(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("CURIE_SLACK_NO_EDIT_STREAMING", raising=False)
    assert WorkerConfig().slack_no_edit_streaming is False


def test_config_reads_no_edit_streaming_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CURIE_SLACK_NO_EDIT_STREAMING", "true")
    assert WorkerConfig().slack_no_edit_streaming is True


def test_update_renders_status_and_link_blocks_for_a_reply() -> None:
    sink = SlackReplyAdapter("xoxb-test")
    captured: dict[str, object] = {}

    async def _fake_chat_update(**kwargs: object) -> None:
        captured.update(kwargs)

    sink._client_for(None).chat_update = _fake_chat_update  # type: ignore[method-assign]

    text = (
        '```curie-reply\n'
        '{"status": "Working", "text": "body", "links": [["Docs", "https://x/y"]]}\n'
        '```'
    )
    asyncio.run(_update(sink, text=text))

    blocks = captured.get("blocks")
    assert isinstance(blocks, list)
    assert blocks[0]["type"] == "context"  # status context leads  # type: ignore[index]
    link_actions = [
        b
        for b in blocks
        if b["type"] == "actions" and any("url" in e for e in b["elements"])
    ]
    assert link_actions, "expected an actions block of URL link buttons"
    assert link_actions[0]["elements"][0]["url"] == "https://x/y"


# --- #228: text-only fallback when a blocks update is rejected ----------------
# If Slack rejects the with-blocks chat_update (SlackApiError, e.g. invalid_blocks),
# the reply must still be delivered: retry text-only so the turn completes exactly
# once and never re-enqueues.


def test_update_falls_back_to_text_only_on_slack_api_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sink = SlackReplyAdapter("xoxb-test")
    calls: list[dict[str, object]] = []
    metrics: list[tuple[str, dict[str, str]]] = []

    monkeypatch.setattr(
        slack_sink_module,
        "record_metric",
        lambda name, *, attributes: metrics.append((name, attributes)),
    )

    async def _fake_chat_update(**kwargs: object) -> None:
        calls.append(kwargs)
        if "blocks" in kwargs:  # the first (with-blocks) call is rejected
            raise SlackApiError(
                "invalid_blocks", {"ok": False, "error": "invalid_blocks"}
            )

    sink._client_for(None).chat_update = _fake_chat_update  # type: ignore[method-assign]

    text = '```curie-reply\n{"header": "Hi", "text": "body"}\n```'
    # Must not raise: the rejected blocks update falls back to a text-only update.
    asyncio.run(_update(sink, text=text))

    assert len(calls) == 2
    second = calls[1]
    assert "blocks" not in second  # the retry is text-only
    assert second["text"] == "body"  # same accessibility fallback text
    assert metrics == [
        (
            "curie.reply.retry",
            {
                "service.name": "curie-worker",
                "operation": "update",
                "role": "client",
                "retry_class": "block-fallback",
            },
        )
    ]


def test_update_labels_a_429_retry_as_rate_limit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sink = SlackReplyAdapter("xoxb-test")
    metrics: list[dict[str, str]] = []
    monkeypatch.setattr(
        slack_sink_module,
        "record_metric",
        lambda _name, *, attributes: metrics.append(attributes),
    )
    calls = 0

    async def _fake_chat_update(**kwargs: object) -> None:
        nonlocal calls
        calls += 1
        if "blocks" in kwargs:
            raise SlackApiError(
                "ratelimited",
                {"ok": False, "error": "ratelimited", "status_code": 429},
            )

    sink._client_for(None).chat_update = _fake_chat_update  # type: ignore[method-assign]
    text = '```curie-reply\n{"header": "Hi", "text": "body"}\n```'

    asyncio.run(_update(sink, text=text))

    assert calls == 2
    assert metrics == [
        {
            "service.name": "curie-worker",
            "operation": "update",
            "role": "client",
            "retry_class": "rate-limit",
        }
    ]


def test_update_fallback_text_stays_within_slack_text_cap() -> None:
    # Loop-closure: model Slack's real failure mode where chat.update rejects
    # ANY call carrying blocks OR text longer than 40000 chars. A reply with a
    # ~60000-char body must still complete: the with-blocks call is rejected,
    # and the text-only retry must send bounded (<=40000) text so it succeeds
    # instead of re-raising and re-opening the unbounded paid-retry loop.
    sink = SlackReplyAdapter("xoxb-test")
    calls: list[dict[str, object]] = []

    async def _fake_chat_update(**kwargs: object) -> None:
        text = kwargs.get("text")
        if "blocks" in kwargs or (isinstance(text, str) and len(text) > 40000):
            raise SlackApiError("too_long", {"ok": False, "error": "msg_too_long"})
        calls.append(kwargs)  # only a within-cap text-only call is recorded

    sink._client_for(None).chat_update = _fake_chat_update  # type: ignore[method-assign]

    text = "```curie-reply\n" + json.dumps({"header": "H", "text": "x" * 60000}) + "\n```"
    # Must not raise: the fallback text is bounded so the retry succeeds.
    asyncio.run(_update(sink, text=text))

    assert calls, "expected a successful text-only update"
    final = calls[-1]
    assert "blocks" not in final
    assert isinstance(final["text"], str)
    assert len(final["text"]) <= 40000


def test_update_does_not_retry_when_blocks_update_succeeds(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sink = SlackReplyAdapter("xoxb-test")
    calls: list[dict[str, object]] = []
    metrics: list[str] = []
    monkeypatch.setattr(
        slack_sink_module,
        "record_metric",
        lambda name, *, attributes: metrics.append(name),
    )

    async def _fake_chat_update(**kwargs: object) -> None:
        calls.append(kwargs)

    sink._client_for(None).chat_update = _fake_chat_update  # type: ignore[method-assign]

    text = '```curie-reply\n{"header": "Hi", "text": "body"}\n```'
    asyncio.run(_update(sink, text=text))

    assert len(calls) == 1  # a spurious retry would make this 2
    assert isinstance(calls[0].get("blocks"), list)
    assert metrics == []


# --- #530: fall back to the default transport when a resumed reply endpoint is
# unreachable (the ephemeral CLI stub died; its URL is persisted on the Approval).


def _raise_connection_error():
    async def _fake(**_kwargs):
        raise aiohttp.ClientError("connection refused")

    return _fake


def _record_call(sink_calls, label):
    async def _fake(**kwargs):
        sink_calls.append(label)
        return {"ok": True, "ts": "9.9"}

    return _fake


def test_update_falls_back_to_default_when_endpoint_unreachable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sink = SlackReplyAdapter("xoxb-test", base_url=_DEFAULT)
    landed: list[str] = []
    metrics: list[dict[str, str]] = []
    monkeypatch.setattr(
        slack_sink_module,
        "record_metric",
        lambda _name, *, attributes: metrics.append(attributes),
    )
    # The per-turn client's PATH is dead (a stub whose route moved) -> connection
    # error on a trusted origin, which is the surviving #530 shape under D4.4.
    sink._client_for(_STUB).chat_update = _raise_connection_error()  # type: ignore[method-assign]
    sink._client_for(None).chat_update = _record_call(landed, "default")  # type: ignore[method-assign]

    asyncio.run(_update(sink, text="hi", endpoint=_STUB))

    assert landed == ["default"], "the resumed reply must land on the default transport"
    assert metrics == [
        {
            "service.name": "curie-worker",
            "operation": "update",
            "role": "client",
            "retry_class": "transport-fallback",
        }
    ]


def test_slack_api_error_is_not_treated_as_unreachable() -> None:
    # A SlackApiError means the endpoint IS reachable but rejected the call; it must
    # NOT trigger a transport fallback (that would misroute a live-workspace reply).
    sink = SlackReplyAdapter("xoxb-test", base_url=_DEFAULT)
    default_hits: list[str] = []

    async def _reject(**_kwargs):
        raise SlackApiError("nope", response={"error": "channel_not_found"})

    sink._client_for(_STUB).chat_update = _reject  # type: ignore[method-assign]
    sink._client_for(None).chat_update = _record_call(default_hits, "default")  # type: ignore[method-assign]

    with pytest.raises(SlackApiError):
        asyncio.run(_update(sink, text="hi", endpoint=_STUB))
    assert default_hits == [], "a SlackApiError must not fall back to the default"


def test_no_fallback_when_endpoint_equals_default() -> None:
    # A turn already on the default transport has no alternate to try; the
    # connection error propagates (there is nowhere else to send it).
    sink = SlackReplyAdapter("xoxb-test", base_url=_DEFAULT)
    sink._client_for(None).chat_update = _raise_connection_error()  # type: ignore[method-assign]

    with pytest.raises(aiohttp.ClientError):
        asyncio.run(_update(sink, text="hi"))  # no endpoint -> default


def test_a_foreign_origin_endpoint_never_reaches_the_default() -> None:
    # The invariant this file used to protect with "no fallback when no default
    # is configured": a CLI-stub endpoint must never have its reply re-sent to
    # the real-Slack default. D4.4 now protects it EARLIER and harder -- the
    # foreign origin is refused before any request exists to fall back from, so
    # the platform bot token never leaves for it either.
    sink = SlackReplyAdapter("xoxb-test")  # default is the real Slack sentinel
    default_hits: list[str] = []
    sink._client_for(None).chat_update = _record_call(default_hits, "default")  # type: ignore[method-assign]

    with pytest.raises(UntrustedSlackEndpointError):
        asyncio.run(_update(sink, text="hi", endpoint="http://stub:2/api/"))
    assert default_hits == [], "real-Slack default is not a safe fallback for a CLI-stub endpoint"


# --- #708: best-effort resume reply when the transport is unreachable and there
# is no default configured at all (the pure-offline local loop). The swallow
# POLICY lives in the adapter; the "is this a resume turn" DECISION lives in the
# kernel (_is_approval_resume) and reaches it as ``best_effort_unreachable``.


def test_best_effort_unreachable_swallows_when_no_default() -> None:
    # A resume turn's reply is best-effort: with NO default transport configured
    # at all (the offline loop) and the transport dead, best_effort_unreachable
    # makes the emit log-and-return instead of re-raising, so the resolved
    # approval's turn ACKs instead of dead-lettering.
    sink = SlackReplyAdapter("xoxb-test")  # no base_url -> no configured default
    sink._client_for(None).chat_update = _raise_connection_error()  # type: ignore[method-assign]
    # The no-wander half, restored in the shape the new model allows. Before
    # D4.4 this case named a dead per-turn CLI-stub endpoint and asserted the
    # real-Slack default took zero calls; a FOREIGN origin is now refused at
    # ``_client_for`` before a client can even be built, so the recorder sits on
    # a second path within the trusted origin instead. The property is the one
    # the old assertion was a special case of: a swallowed delivery must never be
    # quietly retried against some other transport.
    elsewhere: list[str] = []
    sink._client_for(_ELSEWHERE).chat_update = _record_call(  # type: ignore[method-assign]
        elsewhere, "elsewhere"
    )

    # Must NOT raise.
    asyncio.run(_update(sink, text="hi", best_effort_unreachable=True))
    assert elsewhere == [], "the best-effort swallow must not re-send anywhere else"


def test_best_effort_false_still_raises_when_no_default() -> None:
    # The discriminator guard: without the flag (the default False, i.e. every
    # non-resume turn and the loud _drop_with_message/_escalate paths), a dead
    # transport with no default still raises loudly.
    sink = SlackReplyAdapter("xoxb-test")
    sink._client_for(None).chat_update = _raise_connection_error()  # type: ignore[method-assign]
    elsewhere: list[str] = []
    sink._client_for(_ELSEWHERE).chat_update = _record_call(  # type: ignore[method-assign]
        elsewhere, "elsewhere"
    )

    with pytest.raises(aiohttp.ClientError):
        asyncio.run(_update(sink, text="hi", best_effort_unreachable=False))
    # Raising loudly and re-sending elsewhere are different failures, and only
    # the first is wanted: a loud failure that also delivered somewhere leaves a
    # reply in a place nothing will retract when the reclaim retries the turn.
    assert elsewhere == [], "a loud failure must not deliver anywhere else"


def test_slack_api_error_not_swallowed_even_with_best_effort() -> None:
    # A SlackApiError means the endpoint IS reachable but rejected the call; it is
    # NOT an unreachability, so best_effort_unreachable must NOT swallow it (that
    # would hide a real delivery rejection). Only transport-unreachable errors
    # (_UNREACHABLE_ERRORS) are in scope for the best-effort resume swallow.
    # Arranged on a PER-TURN target with a configured default, restoring the
    # dimension this case carried before the seam swap: a rejection on a second
    # target must be neither swallowed by the best-effort flag NOR re-sent to the
    # default. Those are two distinct misroutes and one arrangement catches both;
    # its non-best-effort twin is test_slack_api_error_is_not_treated_as_unreachable.
    sink = SlackReplyAdapter("xoxb-test", base_url=_DEFAULT)
    default_hits: list[str] = []

    async def _reject(**_kwargs):
        raise SlackApiError("nope", response={"error": "channel_not_found"})

    sink._client_for(_STUB).chat_update = _reject  # type: ignore[method-assign]
    sink._client_for(None).chat_update = _record_call(default_hits, "default")  # type: ignore[method-assign]

    with pytest.raises(SlackApiError):
        asyncio.run(
            _update(sink, text="hi", endpoint=_STUB, best_effort_unreachable=True)
        )
    assert default_hits == [], (
        "a SlackApiError must not fall back to the default, best-effort or not"
    )


def test_best_effort_stays_loud_when_default_transport_is_the_dead_target() -> None:
    # F1 (side-effects HIGH): the best-effort swallow must fire ONLY in the pure
    # no-default-configured case. Here a default IS configured and the reply is
    # going over that CONFIGURED default (endpoint=None), so an unreachable error
    # is a genuine transient OUTAGE of a real Slack transport -- it must stay LOUD
    # (raise -> reclaim -> retry per ADR-0039), NOT be swallowed and acked. Even
    # though has_distinct_default is False here, best_effort must not swallow.
    sink = SlackReplyAdapter("xoxb-test", base_url=_DEFAULT)
    sink._client_for(None).chat_update = _raise_connection_error()  # type: ignore[method-assign]

    with pytest.raises(aiohttp.ClientError):
        asyncio.run(_update(sink, text="hi", best_effort_unreachable=True))


# --- #454: the approval card renders BELOW the seam (ADR-0020) ----------------
# The kernel emits a channel-neutral Confirm intent; the Slack adapter's
# ``reply.post`` handling renders it into the same Block Kit approval card that
# used to be built in the kernel. These tests are where the byte-identical card
# contract lives now (the builders themselves are unit-tested in test_blocks.py).


def _approval_message(approval_id: str, summary: str) -> OutboundMessage:
    return OutboundMessage(
        version=MESSAGE_VERSION,
        text=summary,
        interaction=ConfirmIntent(
            kind="confirm",
            id=approval_id,
            prompt=summary,
            confirm=Action(label="Approve", value=approval_id),
            cancel=Action(label="Reject", value=approval_id),
        ),
    )


def _post(
    sink: SlackReplyAdapter,
    *,
    channel: str,
    message: OutboundMessage,
    requested_by: str,
    thread: str | None = None,
) -> object:
    return sink.emit(
        ReplyPost(
            version=REPLY_WIRE_VERSION,
            event="reply.post",
            target=_target(channel=channel, ts=None, thread=thread),
            message=message,
            requested_by=requested_by,
        ),
        route=TargetRoute(),
    )


def test_post_renders_the_approval_card_from_a_confirm_intent() -> None:
    # The adapter turns a Confirm intent into the Block Kit approval card: header,
    # the summary section, a "Requested by" context line, and Approve/Reject
    # buttons carrying the dispatcher's action ids + the record id as value.
    from curie_dispatcher.approval_actions import APPROVE_ACTION_ID, REJECT_ACTION_ID

    sink = SlackReplyAdapter("xoxb-test")
    captured: dict[str, object] = {}

    async def _fake_post(**kwargs: object):
        captured.update(kwargs)
        return {"ok": True, "ts": "9.9"}

    sink._client_for(None).chat_postMessage = _fake_post  # type: ignore[method-assign]

    ack = asyncio.run(
        _post(
            sink,
            channel="C1",
            message=_approval_message("appr-1", "Give ACME a 20% discount"),
            requested_by="U_AE",
            thread="th-card",
        )
    )

    assert ack.ref == "9.9"
    assert captured["channel"] == "C1"
    assert captured["thread_ts"] == "th-card"
    assert "Give ACME a 20% discount" in captured["text"]  # accessibility fallback
    blocks = captured["blocks"]
    assert isinstance(blocks, list)
    assert blocks[0]["type"] == "header"  # type: ignore[index]
    assert "Give ACME a 20% discount" in blocks[1]["text"]["text"]  # type: ignore[index]
    assert "<@U_AE>" in blocks[2]["elements"][0]["text"]  # type: ignore[index]
    actions = blocks[-1]  # type: ignore[index]
    assert actions["type"] == "actions"
    approve, reject = actions["elements"]
    assert approve["action_id"] == APPROVE_ACTION_ID
    assert approve["value"] == "appr-1"
    assert approve["style"] == "primary"
    assert reject["action_id"] == REJECT_ACTION_ID
    assert reject["value"] == "appr-1"
    assert reject["style"] == "danger"


def test_uuid_approval_post_uses_the_record_id_as_slack_idempotency_key() -> None:
    # Slack's chat.postMessage contract associates duplicate messages with
    # client_msg_id, including duplicate-specific errors. Keeping the durable
    # approval UUID in that field closes crash-after-post retries without a
    # second visible card. Provider contract:
    # https://docs.slack.dev/reference/methods/chat.postMessage/
    approval_id = "33333333-3333-4333-8333-333333333333"
    sink = SlackReplyAdapter("xoxb-test")
    calls: list[dict[str, object]] = []

    async def _fake_post(**kwargs: object):
        calls.append(kwargs)
        return {"ok": True, "ts": "9.9"}

    sink._client_for(None).chat_postMessage = _fake_post  # type: ignore[method-assign]

    asyncio.run(
        _post(
            sink,
            channel="C1",
            message=_approval_message(approval_id, "Publish repository changes"),
            requested_by="U_AE",
            thread="th-card",
        )
    )

    assert calls[0]["client_msg_id"] == approval_id


def test_post_falls_back_to_text_only_when_card_blocks_rejected() -> None:
    # Mirrors update(): a rejected Block Kit payload retries text-only so the
    # notice still lands rather than losing the message (the API resolve path
    # stands regardless).
    sink = SlackReplyAdapter("xoxb-test")
    calls: list[dict[str, object]] = []

    async def _fake_post(**kwargs: object):
        calls.append(kwargs)
        if "blocks" in kwargs:
            raise SlackApiError("invalid_blocks", {"ok": False, "error": "invalid_blocks"})
        return {"ok": True, "ts": "9.9"}

    sink._client_for(None).chat_postMessage = _fake_post  # type: ignore[method-assign]

    ack = asyncio.run(
        _post(
            sink,
            channel="C1",
            message=_approval_message("appr-1", "Discount ACME"),
            requested_by="U1",
        )
    )

    assert ack.ref == "9.9"
    assert len(calls) == 2
    assert "blocks" not in calls[1]  # the retry is text-only
    assert "Discount ACME" in calls[1]["text"]


def test_update_message_renders_the_expired_card() -> None:
    # The adapter rebuilds the settled expired card from the channel-neutral
    # summary: the summary stays, the Approve/Reject actions block is gone, and an
    # expiry line takes its place so the card can no longer be clicked. A
    # ``reply.update`` carrying a MESSAGE (rather than text) is the settle form.
    sink = SlackReplyAdapter("xoxb-test")
    captured: dict[str, object] = {}

    async def _fake_update(**kwargs: object) -> None:
        captured.update(kwargs)

    sink._client_for(None).chat_update = _fake_update  # type: ignore[method-assign]

    asyncio.run(
        sink.emit(
            ReplyUpdate(
                version=REPLY_WIRE_VERSION,
                event="reply.update",
                target=_target(ts="9.9"),
                message=OutboundMessage(
                    version=MESSAGE_VERSION, text="Give ACME a 20% discount"
                ),
            ),
            route=TargetRoute(),
        )
    )

    assert captured["channel"] == "C1"
    assert captured["ts"] == "9.9"
    assert "expired" in str(captured["text"]).lower()
    blocks = captured["blocks"]
    assert isinstance(blocks, list)
    assert "Give ACME a 20% discount" in blocks[1]["text"]["text"]  # type: ignore[index]
    assert all(b.get("type") != "actions" for b in blocks)  # type: ignore[union-attr]
    assert any("expired" in str(b).lower() for b in blocks)


def test_update_message_renders_the_resolved_card_from_the_outcome() -> None:
    # The other settle form (#1084): a decision means the RESOLVED card, and the
    # outcome is what carries the whole difference from an expiry.
    sink = SlackReplyAdapter("xoxb-test")
    captured: dict[str, object] = {}

    async def _fake_update(**kwargs: object) -> None:
        captured.update(kwargs)

    sink._client_for(None).chat_update = _fake_update  # type: ignore[method-assign]

    asyncio.run(
        sink.emit(
            ReplyUpdate(
                version=REPLY_WIRE_VERSION,
                event="reply.update",
                target=_target(ts="9.9"),
                message=OutboundMessage(version=MESSAGE_VERSION, text="Discount ACME"),
                settled=SettledOutcome(
                    requested_by="U9", decision="approved", resolver="U7", note="ship it"
                ),
            ),
            route=TargetRoute(),
        )
    )

    rendered = str(captured["blocks"])
    assert "expired" not in str(captured["text"]).lower()
    assert "approved" in rendered.lower()
    assert "U7" in rendered


def test_best_effort_still_falls_back_to_default_when_present() -> None:
    # #530 stays byte-for-byte: with a distinct default configured, a dead per-turn
    # endpoint on a resume turn STILL falls back to the default transport. The new
    # flag only governs the no-configured-default branch; it must not alter the
    # has-default path (the reply still LANDS on the default, it is not swallowed).
    sink = SlackReplyAdapter("xoxb-test", base_url=_DEFAULT)
    landed: list[str] = []
    sink._client_for(_STUB).chat_update = _raise_connection_error()  # type: ignore[method-assign]
    sink._client_for(None).chat_update = _record_call(landed, "default")  # type: ignore[method-assign]

    asyncio.run(_update(sink, text="hi", endpoint=_STUB, best_effort_unreachable=True))
    assert landed == ["default"], "the flag must not bypass the #530 default-transport fallback"
