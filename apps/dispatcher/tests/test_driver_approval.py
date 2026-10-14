"""Declared-driver approval replies traverse real ingress/Valkey, with external Slack/API.

Slack thread/message shapes follow conversations.replies and app_mention:
https://docs.slack.dev/reference/methods/conversations.replies/
https://docs.slack.dev/reference/events/app_mention/
"""

import base64
import copy
import json
from typing import Any
from unittest.mock import MagicMock

import httpx
import pytest
import redis
from curie_dispatcher.admission import build_admission
from curie_dispatcher.approval_actions import ApprovalResolveClient
from curie_dispatcher.handlers import process_event
from curie_dispatcher.marked_actions import reserve_turn

from .conftest import FakeAdmissionApi
from .test_test_actions import DRIVER, _configured

APPROVAL = "00000000-0000-4000-8000-000000000135"
CARD: dict[str, Any] = {
    "ts": "1600.1",
    "thread_ts": "1600.0",
    "user": "U0BOT",
    "bot_id": "B0BOT",
    "text": "Approval required",
    "blocks": [
        {"type": "header", "text": {"type": "plain_text", "text": "Approval required"}},
        {"type": "section", "text": {"type": "mrkdwn", "text": "Example action"}},
        {
            "type": "actions",
            "elements": [
                {"type": "button", "action_id": "curie-approval-approve", "value": APPROVAL},
                {"type": "button", "action_id": "curie-approval-reject", "value": APPROVAL},
            ],
        },
    ],
}


def _deliver(
    cfg: Any,
    redis_client: redis.Redis,
    *,
    text: str,
    cards: list[dict[str, Any]],
    event_id: str = "Ev1",
    thread: str | None = "1600.0",
    status: int = 200,
    bot: str = DRIVER.bot_id,
    more: bool = False,
    pages: list[dict[str, Any]] | None = None,
) -> tuple[MagicMock, list[httpx.Request]]:
    requests = []

    def api(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(
            status,
            json={"detail": "approval not found"}
            if status == 404
            else {"status": "approved", "resolved_by": DRIVER.bot_user_id},
        )

    resolver = ApprovalResolveClient(
        api_base_url="http://api.example.com",
        api_key="example-key",
        approval_chat_attester_secret=cfg.approval_chat_attester_secret,
        client=httpx.Client(transport=httpx.MockTransport(api)),
    )
    web = MagicMock()
    web.conversations_replies.return_value = {"ok": True, "messages": cards, "has_more": more}
    if pages is not None:
        web.conversations_replies.side_effect = pages
    event = {
        "channel": DRIVER.channel_id,
        "ts": "1700.0",
        "text": f"<@U0BOT> {text}",
        "bot_id": bot,
        "user": "U0FORGED",
    }
    if thread:
        event["thread_ts"] = thread
    process_event(
        body={"event_id": event_id},
        event=event,
        lane="mention",
        web_client=web,
        redis_client=redis_client,
        config=cfg,
        slack_identity="default",
        admission=build_admission(cfg, redis_client),
        bot_user_id="U0BOT",
        bot_id="B0BOT",
        approval_resolver=resolver,
    )
    return web, requests


@pytest.mark.parametrize("verb,decision", [("approve", "approved"), ("reject", "rejected")])
@pytest.mark.parametrize("root", [False, True])
def test_driver_card_reply_resolves_without_turn_or_budget(
    config: Any, redis_client: redis.Redis, verb: str, decision: str, root: bool
) -> None:
    cfg = _configured(config)
    card = copy.deepcopy(CARD)
    if root:
        card["ts"] = "1600.0"
        card.pop("thread_ts")
    web, requests = _deliver(
        cfg, redis_client, text=f"[test action] {verb} {APPROVAL}", cards=[card]
    )
    assert len(requests) == 1
    assert json.loads(requests[0].content) == {"decision": decision}
    claims = json.loads(
        base64.urlsafe_b64decode(
            requests[0].headers["X-Curie-Approval-Principal"].split(".")[1] + "=="
        )
    )
    assert (claims["kind"], claims["sub"], claims["actor_channel"], claims["approval_id"]) == (
        "test_driver",
        DRIVER.bot_user_id,
        DRIVER.channel_id,
        APPROVAL,
    )
    assert redis_client.xlen(cfg.stream) == 0
    web.chat_postMessage.assert_not_called()
    assert web.chat_update.call_args.kwargs["ts"] == card["ts"]
    _, duplicates = _deliver(
        cfg, redis_client, text=f"[test action] {verb} {APPROVAL}", cards=[card]
    )
    assert duplicates == []
    assert reserve_turn(
        redis_client, cfg, identity="default", channel=DRIVER.channel_id, thread="1600.0"
    )
    assert reserve_turn(
        redis_client, cfg, identity="default", channel=DRIVER.channel_id, thread="1600.0"
    )


@pytest.mark.parametrize(
    "violation",
    [
        "no_card",
        "foreign",
        "wrong_id",
        "wrong_thread",
        "no_thread",
        "malformed",
        "off",
        "spoof",
        "more",
        "duplicate_card",
        "foreign_user",
        "wrong_header",
        "root_wrong_thread",
    ],
)
def test_driver_approval_refuses_missing_or_untrusted_card(
    config: Any, redis_client: redis.Redis, violation: str
) -> None:
    cfg = _configured(config, enabled=violation != "off")
    card = copy.deepcopy(CARD)
    cards = [card]
    if violation == "foreign_user":
        card["user"] = "U0OTHER"
    if violation == "wrong_header":
        card["blocks"][0]["text"]["text"] = "Unrelated message"
    if violation == "root_wrong_thread":
        card["ts"] = "1600.0"
        card["thread_ts"] = "1500.0"
    if violation == "no_card":
        cards = []
    if violation == "foreign":
        card["bot_id"] = "B0OTHER"
    if violation == "wrong_id":
        card["blocks"][-1]["elements"][0]["value"] = "other"
    if violation == "wrong_thread":
        card["thread_ts"] = "1500.0"
    if violation == "duplicate_card":
        cards.append(copy.deepcopy(card))
    web, requests = _deliver(
        cfg,
        redis_client,
        text=f"[test action] approve {APPROVAL}" + (" extra" if violation == "malformed" else ""),
        cards=cards,
        thread=None if violation == "no_thread" else "1600.0",
        bot="B0OTHER" if violation == "spoof" else DRIVER.bot_id,
        more=violation == "more",
    )
    assert requests == []
    assert redis_client.xlen(cfg.stream) == 0
    web.chat_update.assert_not_called()
    assert (
        web.chat_postMessage.call_args.kwargs["text"]
        == "This installation does not accept test actions."
    )


def test_driver_approval_release_miss_does_not_stamp_card(
    config: Any, redis_client: redis.Redis
) -> None:
    web, requests = _deliver(
        _configured(config),
        redis_client,
        text=f"[test action] approve {APPROVAL}",
        cards=[CARD],
        status=404,
    )
    assert len(requests) == 1
    web.chat_update.assert_not_called()
    web.chat_postMessage.assert_not_called()
    web.chat_postEphemeral.assert_not_called()


def test_driver_approval_caller_refusal_does_not_claim_or_read(
    config: Any, redis_client: redis.Redis, admission_api: FakeAdmissionApi
) -> None:
    cfg = _configured(config)
    admission_api.lists[(DRIVER.channel_id, "default")] = {"U0FORGED"}
    web, requests = _deliver(
        cfg, redis_client, text=f"[test action] approve {APPROVAL}", cards=[CARD]
    )
    assert requests == []
    web.conversations_replies.assert_not_called()
    web.chat_postMessage.assert_not_called()
    assert not redis_client.exists(cfg.dedupe_key("Ev1"))


def test_driver_card_read_can_follow_one_bounded_cursor(
    config: Any, redis_client: redis.Redis
) -> None:
    web, requests = _deliver(
        _configured(config),
        redis_client,
        text=f"[test action] approve {APPROVAL}",
        cards=[],
        pages=[
            {
                "ok": True,
                "messages": [{"ts": "1600.0", "user": "U0PERSON"}],
                "has_more": True,
                "response_metadata": {"next_cursor": "example-cursor"},
            },
            {"ok": True, "messages": [CARD], "has_more": False},
        ],
    )
    assert len(requests) == 1
    assert web.conversations_replies.call_args.kwargs["cursor"] == "example-cursor"
    assert redis_client.xlen(config.stream) == 0


def test_driver_card_read_refuses_after_three_pages(config: Any, redis_client: redis.Redis) -> None:
    web, requests = _deliver(
        _configured(config),
        redis_client,
        text=f"[test action] approve {APPROVAL}",
        cards=[],
        pages=[
            {
                "ok": True,
                "messages": [CARD] if page == 0 else [],
                "has_more": True,
                "response_metadata": {"next_cursor": f"cursor-{page}"},
            }
            for page in range(3)
        ],
    )
    assert requests == []
    assert web.conversations_replies.call_count == 3
    assert redis_client.xlen(config.stream) == 0


def test_unavailable_slack_card_evidence_refuses(config: Any, redis_client: redis.Redis) -> None:
    web, requests = _deliver(
        _configured(config),
        redis_client,
        text=f"[test action] approve {APPROVAL}",
        cards=[],
        pages=[RuntimeError("example unavailable")],
    )
    assert requests == []
    assert redis_client.xlen(config.stream) == 0
    assert (
        web.chat_postMessage.call_args.kwargs["text"]
        == "This installation does not accept test actions."
    )


@pytest.mark.parametrize("owned", [False, True, None])
def test_socket_mode_driver_reply_declines_only_exact_release_miss(
    config: Any,
    redis_client: redis.Redis,
    owned: bool | None,
) -> None:
    from curie_dispatcher.app import build_app
    from slack_bolt.adapter.socket_mode import SocketModeHandler

    from .conftest import FakeSocketClient, _authorize, deliver_once
    from .test_inbound_relevance import _events_api_request

    cfg = _configured(config)
    requests = []

    def api(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.method == "GET":
            return httpx.Response(
                200 if owned else 404 if owned is False else 503,
                json={"detail": "approval not found"},
            )
        return httpx.Response(200, json={"status": "approved", "resolved_by": DRIVER.bot_user_id})

    resolver = ApprovalResolveClient(
        api_base_url="http://api.example.com",
        api_key="example-key",
        approval_chat_attester_secret=cfg.approval_chat_attester_secret,
        client=httpx.Client(transport=httpx.MockTransport(api)),
    )
    web = MagicMock()
    web.conversations_replies.return_value = {
        "ok": True,
        "messages": [{**CARD, "bot_id": "B1"}],
        "has_more": False,
    }
    app = build_app(
        cfg, web_client=web, redis_client=redis_client, authorize=_authorize, resolver=resolver
    )
    handler = SocketModeHandler(app, app_token="xapp-test")
    sock = FakeSocketClient()
    event = {
        "type": "app_mention",
        "channel": DRIVER.channel_id,
        "ts": "1700.0",
        "thread_ts": "1600.0",
        "text": f"<@U0BOT> [test action] approve {APPROVAL}",
        "bot_id": DRIVER.bot_id,
        "user": "U0FORGED",
    }
    deliver_once(handler, sock, app, _events_api_request("env-driver", "EvDriver", event))
    assert sock.acked_envelope_ids == (["env-driver"] if owned is True else [])
    assert [request.method for request in requests] == (
        ["GET", "POST"] if owned is True else ["GET"]
    )
    assert redis_client.xlen(cfg.stream) == 0
    if owned is not True:
        assert not redis_client.exists(cfg.dedupe_key("EvDriver"))
        web.conversations_replies.assert_not_called()
        web.chat_update.assert_not_called()


def test_approval_reply_ignores_an_exhausted_turn_budget(
    config: Any, redis_client: redis.Redis
) -> None:
    cfg = _configured(config)
    for _ in range(2):
        assert reserve_turn(
            redis_client, cfg, identity="default", channel=DRIVER.channel_id, thread="1600.0"
        )
    assert not reserve_turn(
        redis_client, cfg, identity="default", channel=DRIVER.channel_id, thread="1600.0"
    )
    _, requests = _deliver(
        cfg, redis_client, text=f"[test action] approve {APPROVAL}", cards=[CARD]
    )
    assert len(requests) == 1
    assert redis_client.xlen(cfg.stream) == 0


def test_owner_probe_deadline_saturation_and_late_completion() -> None:
    import threading
    import time

    from curie_dispatcher.handlers import _DriverApprovalProbe

    probe = _DriverApprovalProbe()
    started = threading.Event()
    release = threading.Event()
    exited = threading.Event()
    calls = []

    def delayed() -> None:
        calls.append("first")
        started.set()
        assert release.wait(2)
        exited.set()
        return None

    began = time.monotonic()
    try:
        assert probe.check(delayed, wait_s=0.02) == 503
        assert time.monotonic() - began < 0.5
        assert started.is_set()
        assert probe.check(lambda: calls.append("saturated"), wait_s=0.02) == 503
        assert calls == ["first"]
    finally:
        release.set()
    assert exited.wait(1)
    # A completed old envelope cannot authorize the next one. Wait until the
    # worker exits its finally block and proves the current, denied result.
    deadline = time.monotonic() + 1
    while probe.check(lambda: 200, wait_s=0.02) == 503:
        assert time.monotonic() < deadline
    assert probe.check(lambda: 404, wait_s=0.02) == 404


@pytest.mark.parametrize("status", [None, 200, 404, 503])
def test_owner_probe_keeps_completed_verdicts_distinct(status: int | None) -> None:
    from curie_dispatcher.handlers import _DriverApprovalProbe

    assert _DriverApprovalProbe().check(lambda: status) == status


def test_owner_probe_exception_is_retryable() -> None:
    from curie_dispatcher.handlers import _DriverApprovalProbe

    def unavailable() -> None:
        raise RuntimeError("example unavailable")

    assert _DriverApprovalProbe().check(unavailable) == 503


@pytest.mark.parametrize("allowed", [False, None])
def test_socket_mode_caller_denial_and_unknown_never_probe_or_claim(
    config: Any, redis_client: redis.Redis, allowed: bool | None
) -> None:
    from curie_dispatcher.admission import AdmissionClient, AdmissionGate
    from curie_dispatcher.app import build_app
    from slack_bolt.adapter.socket_mode import SocketModeHandler

    from .conftest import FakeSocketClient, _authorize, deliver_once
    from .test_inbound_relevance import _events_api_request

    cfg = _configured(config)
    calls = []

    def api(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return httpx.Response(
            503 if allowed is None else 200,
            json={"allowed": allowed, "restricted": True, "install_restricted": True},
        )

    gate = AdmissionGate(
        AdmissionClient(
            api_base_url="http://api.example.com",
            api_key="example-key",
            client=httpx.Client(transport=httpx.MockTransport(api)),
        ),
        ttl_s=30,
        stale_s=300,
        redis_client=redis_client,
        key_prefix=cfg.admission_cache_prefix,
    )
    resolver = ApprovalResolveClient(
        api_base_url="http://api.example.com",
        api_key="example-key",
        approval_chat_attester_secret=cfg.approval_chat_attester_secret,
        client=httpx.Client(transport=httpx.MockTransport(api)),
    )
    web = MagicMock()
    app = build_app(
        cfg,
        web_client=web,
        redis_client=redis_client,
        authorize=_authorize,
        resolver=resolver,
        admission=gate,
    )
    handler = SocketModeHandler(app, app_token="xapp-test")
    sock = FakeSocketClient()
    event = {
        "type": "app_mention",
        "channel": DRIVER.channel_id,
        "ts": "1700.0",
        "thread_ts": "1600.0",
        "text": f"<@U0BOT> [test action] approve {APPROVAL}",
        "bot_id": DRIVER.bot_id,
        "user": "U0FORGED",
    }
    deliver_once(handler, sock, app, _events_api_request("env-policy", "EvPolicy", event))
    assert sock.acked_envelope_ids == (["env-policy"] if allowed is False else [])
    assert len(calls) == 1 and calls[0].url.path == "/channels/admission"
    assert not redis_client.exists(cfg.dedupe_key("EvPolicy"))
    assert redis_client.xlen(cfg.stream) == 0
    web.conversations_replies.assert_not_called()
    web.chat_update.assert_not_called()


def test_socket_mode_deadline_saturation_and_late_read_cannot_ack_or_resolve(
    config: Any, redis_client: redis.Redis
) -> None:
    import threading
    import time

    from curie_dispatcher.admission import AdmissionClient, AdmissionGate
    from curie_dispatcher.app import build_app
    from slack_bolt.adapter.socket_mode import SocketModeHandler

    from .conftest import FakeSocketClient, _authorize
    from .test_inbound_relevance import _events_api_request

    cfg = _configured(config)
    release = threading.Event()
    late_read = threading.Event()
    calls = []

    def api(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        if request.url.path == "/channels/admission":
            assert release.wait(3)
            return httpx.Response(
                200, json={"allowed": True, "restricted": True, "install_restricted": True}
            )
        late_read.set()
        return httpx.Response(200, json={})

    client = httpx.Client(transport=httpx.MockTransport(api))
    gate = AdmissionGate(
        AdmissionClient(
            api_base_url="http://api.example.com", api_key="example-key", client=client
        ),
        ttl_s=30,
        stale_s=300,
        redis_client=redis_client,
        key_prefix=cfg.admission_cache_prefix,
    )
    resolver = ApprovalResolveClient(
        api_base_url="http://api.example.com",
        api_key="example-key",
        approval_chat_attester_secret=cfg.approval_chat_attester_secret,
        client=client,
    )
    web = MagicMock()
    app = build_app(
        cfg,
        web_client=web,
        redis_client=redis_client,
        authorize=_authorize,
        resolver=resolver,
        admission=gate,
    )
    handler = SocketModeHandler(app, app_token="xapp-test")
    sock = FakeSocketClient()
    event = {
        "type": "app_mention",
        "channel": DRIVER.channel_id,
        "ts": "1700.0",
        "thread_ts": "1600.0",
        "text": f"<@U0BOT> [test action] approve {APPROVAL}",
        "bot_id": DRIVER.bot_id,
        "user": "U0FORGED",
    }
    try:
        began = time.monotonic()
        handler.handle(sock, _events_api_request("env-delayed", "EvDelayed", event))
        assert 0.9 <= time.monotonic() - began < 1.5
        began = time.monotonic()
        handler.handle(sock, _events_api_request("env-saturated", "EvSaturated", event))
        assert time.monotonic() - began < 0.5
        assert len(calls) == 1
    finally:
        release.set()
        assert late_read.wait(1)
        app.listener_runner.listener_executor.shutdown(wait=True)
    assert sock.acked_envelope_ids == []
    assert [r.method for r in calls] == ["POST", "GET"]  # admission only; no resolve POST
    assert all(
        not redis_client.exists(cfg.dedupe_key(e))
        for e in ("EvDelayed", "EvSaturated")
    )
    assert redis_client.xlen(cfg.stream) == 0
    web.conversations_replies.assert_not_called()
    web.chat_update.assert_not_called()


def test_owner_probe_discards_completion_racing_deadline(monkeypatch: pytest.MonkeyPatch) -> None:
    from curie_dispatcher import handlers

    times = iter([0.0, 0.0, 2.0])
    monkeypatch.setattr(handlers.time, "monotonic", lambda: next(times))
    assert handlers._DriverApprovalProbe().check(lambda: None, wait_s=1.0) == 503
