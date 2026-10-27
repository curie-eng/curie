"""Actual Bolt Socket Mode ingress with real Valkey and synthetic Slack/API edges.

@spec HTTP-INVOCATION-EXECUTION-3 HTTP-INVOCATION-EXECUTION-19.
These tests do not qualify an actual Slack installation or protected key mounts.
"""

from __future__ import annotations

import asyncio
import json
import logging
import threading
from typing import Any
from unittest.mock import MagicMock

import httpx
import pytest
import redis
from curie_dispatcher.app import build_app
from curie_dispatcher.approval_actions import APPROVE_ACTION_ID, APPROVE_NOTE_ACTION_ID
from curie_dispatcher.config import DispatcherConfig
from curie_dispatcher.identities import SlackIdentityCredentials
from curie_dispatcher.preflight import PreflightedIdentity
from curie_dispatcher.queue import from_stream_fields
from slack_bolt.adapter.socket_mode import SocketModeHandler
from slack_sdk.socket_mode.request import SocketModeRequest

from .conftest import FakeSocketClient, _authorize
from .test_approval_note_dialog import _note_submit
from .test_dispatch import (
    _block_action_request,
    _build,
    _drain,
    _events_api_request,
    _mention_event,
)
from .test_http_receiver import (
    BOT,
    CHANNEL,
    NAME,
    SENDER,
    HttpRig,
    api,
    binding_body,
    native,
    receipt,
    scope_body,
)
from .test_test_actions import DRIVER

APPROVAL = "55555555-5555-4555-8555-555555555555"
# @spec HTTP-INVOCATION-EXECUTION-19: the two fixed person-facing notices.
REFUSED_TEXT = "Sorry, I can't help with this request."
UNAVAILABLE_TEXT = "I couldn't verify your access right now. Please try again in a few minutes."
DM_CHANNEL = "D0EXAMPLEDIRECT"
DM_BINDING = "66666666-6666-4666-8666-666666666666"


class ForbiddenBoundary:
    """@spec HTTP-INVOCATION-EXECUTION-19: record attempted external legacy API calls."""

    def __init__(self) -> None:
        self.calls: list[str] = []

    def __getattr__(self, name: str) -> Any:
        def forbidden(*_args: Any, **_kwargs: Any) -> Any:
            self.calls.append(name)
            raise AssertionError("selected receiver entered an unsupported legacy API boundary")

        return forbidden


def _identity() -> SlackIdentityCredentials:
    """@spec HTTP-INVOCATION-EXECUTION-19."""
    return SlackIdentityCredentials(name=NAME, app_token="xapp-test", bot_token="xoxb-test")


def _selected(
    config: DispatcherConfig, redis_client: redis.Redis, rig: HttpRig, *, bindings: Any = None
) -> tuple[Any, Any]:
    """@spec HTTP-INVOCATION-EXECUTION-19: actual constructor and listener registration."""
    module = api()
    runtime = rig.runtime(module, bindings=bindings)
    legacy = ForbiddenBoundary()
    for method in (
        "chat_postMessage",
        "chat_update",
        "chat_postEphemeral",
        "conversations_replies",
        "views_open",
        "assistant_threads_setStatus",
    ):
        setattr(rig.web, method, MagicMock(return_value={"ok": True, "ts": "555.000"}))
    app = build_app(
        config,
        web_client=rig.web,
        redis_client=redis_client,
        identity=_identity(),
        authorize=_authorize,
        http_receiver=runtime,
        admission=legacy,
        identity_client=legacy,
        resolver=legacy,
    )
    return app, legacy


def _request(envelope: str = "env-http") -> SocketModeRequest:
    """@spec HTTP-INVOCATION-EXECUTION-19: native envelope from the same event object."""
    body, _event = native()
    return SocketModeRequest(type="events_api", envelope_id=envelope, payload=body)


def _owned_keys(redis_client: redis.Redis, config: DispatcherConfig) -> set[str]:
    """@spec HTTP-INVOCATION-EXECUTION-19: read only this fixture's state, no datastore fake."""
    keys = set()
    for prefix in (
        config.dedupe_prefix,
        config.admission_cache_prefix,
        config.thread_context_cache_prefix,
    ):
        keys.update(redis_client.scan_iter(match=prefix + "*"))
    if redis_client.exists(config.stream):
        keys.add(config.stream)
    return keys


def _finish_owned_listeners(app: Any) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-19: join this process's optional observer work too."""
    from curie_dispatcher import identity
    _drain(app)
    pool = identity._pool
    identity.shutdown_identity_lookups()
    if pool is not None:
        pool.shutdown(wait=True)


def _no_legacy_effects(
    redis_client: redis.Redis, config: DispatcherConfig, rig: HttpRig, legacy: ForbiddenBoundary
) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-19."""
    assert legacy.calls == []
    assert _owned_keys(redis_client, config) == set()
    assert redis_client.xlen(config.stream) == 0
    for method in (
        "chat_postMessage",
        "chat_update",
        "chat_postEphemeral",
        "conversations_replies",
        "views_open",
        "assistant_threads_setStatus",
    ):
        getattr(rig.web, method).assert_not_called()


def test_selected_real_socket_ack_does_not_wait_for_api_and_has_zero_legacy_effects(
    config: DispatcherConfig,
    redis_client: redis.Redis,
) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-3/19: actual Bolt owner, not a direct submit unit."""
    api()
    release = threading.Event()
    arrived = threading.Event()

    async def handler(_request: httpx.Request) -> httpx.Response:
        arrived.set()
        await asyncio.to_thread(release.wait, 3)
        return httpx.Response(200, json=receipt())

    rig = HttpRig(handler)
    app, legacy = _selected(config, redis_client, rig)
    sock = FakeSocketClient()
    try:
        SocketModeHandler(app, app_token="xapp-test").handle(sock, _request())
        assert sock.acked_envelope_ids == ["env-http"]
        assert arrived.wait(2), "selected receiver never reached the synthetic API"
    finally:
        release.set()
        _finish_owned_listeners(app)
    assert len(rig.requests) == 1
    assert json.loads(rig.requests[0].content)["reply_ref"] == ""
    _no_legacy_effects(redis_client, config, rig, legacy)
    rig.closed()


@pytest.mark.parametrize("status", [403, 404, 503])
def test_selected_complete_denial_or_route_miss_never_enters_legacy_fallback(
    config: DispatcherConfig,
    redis_client: redis.Redis,
    status: int,
) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-3/19: even old-API404 has no compatibility grant."""
    api()

    async def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(status, json={"detail": "Not Found" if status == 404 else "refused"})

    rig = HttpRig(handler)
    app, legacy = _selected(config, redis_client, rig)
    sock = FakeSocketClient()
    try:
        SocketModeHandler(app, app_token="xapp-test").handle(sock, _request())
    finally:
        _finish_owned_listeners(app)
    assert sock.acked_envelope_ids == ["env-http"] and len(rig.requests) == 1
    # @spec HTTP-INVOCATION-EXECUTION-19: the one notice is the only Slack effect.
    rig.web.chat_postEphemeral.assert_called_once_with(
        channel=CHANNEL, user=SENDER, thread_ts="1700.0001",
        text=REFUSED_TEXT if status == 403 else UNAVAILABLE_TEXT,
    )
    rig.web.chat_postEphemeral.reset_mock()
    _no_legacy_effects(redis_client, config, rig, legacy)
    rig.closed()


def _deliver_selected(
    config: DispatcherConfig,
    redis_client: redis.Redis,
    rig: HttpRig,
    *events: dict[str, Any],
    bindings: Any = None,
) -> Any:
    """@spec HTTP-INVOCATION-EXECUTION-19: deliver native mentions through the actual Bolt owner."""
    app, legacy = _selected(config, redis_client, rig, bindings=bindings)
    sock = FakeSocketClient()
    try:
        for index, body in enumerate(events):
            request = SocketModeRequest(
                type="events_api", envelope_id=f"env-notice-{index}", payload=body,
            )
            SocketModeHandler(app, app_token="xapp-test").handle(sock, request)
    finally:
        _finish_owned_listeners(app)
    assert sock.acked_envelope_ids == [f"env-notice-{index}" for index in range(len(events))]
    assert legacy.calls == [] and redis_client.xlen(config.stream) == 0
    return rig.web


def _answering(status: int, detail: str = "caller_not_allowed") -> Any:
    """@spec HTTP-INVOCATION-EXECUTION-19: a complete API answer carrying revealing detail."""

    async def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(status, json={
            "detail": detail, "policy": "synthetic-finance-policy",
            "group": "synthetic-approvers-group", "provider": "okta",
        })

    return handler


_LEAKS = ("caller_not_allowed", "policy", "group", "okta", "provider", "403", "503", "allowed")


def _posted_texts(web: Any) -> list[str]:
    """@spec HTTP-INVOCATION-EXECUTION-19: every text this identity tried to show anyone."""
    return [
        str(call.kwargs.get("text"))
        for method in ("chat_postEphemeral", "chat_postMessage", "chat_update")
        for call in getattr(web, method).call_args_list
    ]


@pytest.mark.parametrize(
    ("status", "text"),
    [
        (401, REFUSED_TEXT),
        (403, REFUSED_TEXT),
        (409, REFUSED_TEXT),
        (413, REFUSED_TEXT),
        (422, REFUSED_TEXT),
        (404, UNAVAILABLE_TEXT),
        (429, UNAVAILABLE_TEXT),
        (500, UNAVAILABLE_TEXT),
        (503, UNAVAILABLE_TEXT),
    ],
)
def test_selected_refused_or_unavailable_mention_tells_only_the_sender_in_a_channel(
    config: DispatcherConfig, redis_client: redis.Redis, status: int, text: str
) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-19: one fixed ephemeral per outcome class, no reason."""
    api()
    rig = HttpRig(_answering(status))
    body, _event = native()
    web = _deliver_selected(config, redis_client, rig, body)
    web.chat_postEphemeral.assert_called_once_with(
        channel=CHANNEL, user=SENDER, thread_ts="1700.0001", text=text,
    )
    web.chat_postMessage.assert_not_called()
    web.chat_update.assert_not_called()
    for posted in _posted_texts(web):
        assert not any(leak in posted.lower() for leak in _LEAKS), posted
    rig.closed()


def test_selected_mention_whose_api_is_unreachable_gets_the_retry_notice(
    config: DispatcherConfig, redis_client: redis.Redis
) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-19: a transport failure is unavailable, not refused."""
    api()

    async def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("synthetic refused connection", request=request)

    rig = HttpRig(handler)
    body, _event = native()
    web = _deliver_selected(config, redis_client, rig, body)
    web.chat_postEphemeral.assert_called_once_with(
        channel=CHANNEL, user=SENDER, thread_ts="1700.0001", text=UNAVAILABLE_TEXT,
    )
    web.chat_postMessage.assert_not_called()


def test_selected_top_level_mention_notice_is_not_threaded(
    config: DispatcherConfig, redis_client: redis.Redis
) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-19: a channel-level mention gets a channel ephemeral."""
    api()
    rig = HttpRig(_answering(403))
    body, event = native()
    event.pop("thread_ts")
    web = _deliver_selected(config, redis_client, rig, body)
    web.chat_postEphemeral.assert_called_once_with(
        channel=CHANNEL, user=SENDER, text=REFUSED_TEXT,
    )


@pytest.mark.parametrize(("status", "text"), [(403, REFUSED_TEXT), (503, UNAVAILABLE_TEXT)])
def test_selected_direct_message_notice_is_an_ordinary_thread_reply(
    config: DispatcherConfig, redis_client: redis.Redis, status: int, text: str
) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-19: a DM has no audience to hide the notice from."""
    module = api()
    rig = HttpRig(_answering(status))
    bindings = (
        module.HttpReceiverBindingV1.model_validate(binding_body()),
        module.HttpReceiverBindingV1.model_validate(
            binding_body(native_channel_id=DM_CHANNEL, binding_id=DM_BINDING),
        ),
    )
    body, event = native()
    event["channel"] = DM_CHANNEL
    web = _deliver_selected(config, redis_client, rig, body, bindings=bindings)
    assert len(rig.requests) == 1
    web.chat_postMessage.assert_called_once_with(
        channel=DM_CHANNEL, thread_ts="1700.0001", text=text,
    )
    web.chat_postEphemeral.assert_not_called()


def test_selected_mention_in_an_unbound_channel_gets_the_same_refusal(
    config: DispatcherConfig, redis_client: redis.Redis
) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-19: a local refusal reads exactly like an API one."""
    api()
    rig = HttpRig()
    body, event = native()
    event["channel"] = "C0EXAMPLEUNBOUND"
    web = _deliver_selected(config, redis_client, rig, body)
    assert rig.requests == []
    web.chat_postEphemeral.assert_called_once_with(
        channel="C0EXAMPLEUNBOUND", user=SENDER, thread_ts="1700.0001", text=REFUSED_TEXT,
    )


@pytest.mark.parametrize("sender", ["bot", "self", "subtype", "no_user"])
def test_selected_bot_self_or_senderless_mention_never_gets_a_notice(
    config: DispatcherConfig, redis_client: redis.Redis, sender: str
) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-19: no notice can answer a bot or start a loop."""
    api()
    rig = HttpRig(_answering(403))
    body, event = native()
    if sender == "bot":
        event["bot_id"] = "B0EXAMPLEOTHER"
    elif sender == "self":
        event["user"] = BOT
    elif sender == "subtype":
        event["subtype"] = "message_changed"
    else:
        event.pop("user")
    web = _deliver_selected(config, redis_client, rig, body)
    assert rig.requests == []
    assert _posted_texts(web) == []
    web.chat_postEphemeral.assert_not_called()
    web.chat_postMessage.assert_not_called()


def test_selected_repeated_refused_delivery_posts_one_notice(
    config: DispatcherConfig, redis_client: redis.Redis
) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-19: a Slack redelivery of one event never re-notifies."""
    api()
    rig = HttpRig(_answering(403))
    first, _event = native()
    again, _event = native()
    web = _deliver_selected(config, redis_client, rig, first, again)
    assert len(rig.requests) == 2
    web.chat_postEphemeral.assert_called_once_with(
        channel=CHANNEL, user=SENDER, thread_ts="1700.0001", text=REFUSED_TEXT,
    )


def test_selected_pending_or_accepted_mention_posts_no_notice(
    config: DispatcherConfig, redis_client: redis.Redis
) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-19: an admitted turn is answered by the worker alone."""
    api()
    answers = iter([receipt(), receipt(pending=True)])

    async def handler(_request: httpx.Request) -> httpx.Response:
        value = next(answers)
        return httpx.Response(202 if value["duplicate"] else 200, json=value)

    rig = HttpRig(handler)
    first, _event = native()
    again, _event = native()
    web = _deliver_selected(config, redis_client, rig, first, again)
    assert len(rig.requests) == 2
    assert _posted_texts(web) == []


def test_selected_failed_notice_is_logged_by_class_and_never_raises(
    config: DispatcherConfig, redis_client: redis.Redis, caplog: pytest.LogCaptureFixture
) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-19: a lost notice changes no outcome and leaks nothing."""
    api()
    rig = HttpRig(_answering(403))
    body, _event = native()
    app, _legacy = _selected(config, redis_client, rig)
    rig.web.chat_postEphemeral.side_effect = RuntimeError("synthetic " + SENDER + " failure")
    errors: list[BaseException] = []

    @app.error
    def _collect(error: BaseException) -> None:
        errors.append(error)

    sock = FakeSocketClient()
    with caplog.at_level(logging.DEBUG):
        try:
            SocketModeHandler(app, app_token="xapp-test").handle(
                sock, SocketModeRequest(type="events_api", envelope_id="env-fail", payload=body),
            )
        finally:
            _finish_owned_listeners(app)
    assert errors == [] and sock.acked_envelope_ids == ["env-fail"]
    rig.web.chat_postEphemeral.assert_called_once()
    assert "RuntimeError" in caplog.text
    assert "synthetic " + SENDER + " failure" not in caplog.text


# Approval clicks, note dialogs and their submissions are the qualified answer
# lanes (HTTP-APPROVAL-CONTINUITY-7), covered in test_http_receiver_answers.py.
@pytest.mark.parametrize("lane", ["im", "action", "driver"])
def test_selected_unsupported_turn_and_approval_lanes_refuse_before_legacy_work(
    config: DispatcherConfig,
    redis_client: redis.Redis,
    lane: str,
) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-19: cover dedicated and generic Bolt listeners."""
    api()
    cfg = config.model_copy(
        update={"test_installation_enabled": True, "test_installation_drivers": (DRIVER,)}
    )
    rig = HttpRig()
    app, legacy = _selected(cfg, redis_client, rig)
    if lane == "im":
        event = {
            "type": "message",
            "channel_type": "im",
            "channel": "D0EXAMPLE1",
            "user": "U123",
            "text": "Synthetic unsupported IM",
            "ts": "1700.0001",
        }
        request = _events_api_request("env-unsupported", "EvUnsupportedIM", event)
    elif lane in {"action", "approve", "approve_note"}:
        action_id = {
            "action": "synthetic-turn-action",
            "approve": APPROVE_ACTION_ID,
            "approve_note": APPROVE_NOTE_ACTION_ID,
        }[lane]
        request = _block_action_request(
            "env-unsupported",
            action_id=action_id,
            value=APPROVAL if lane != "action" else "Synthetic command",
        )
    elif lane == "modal":
        request = _note_submit("env-unsupported", note="Synthetic note")
    else:
        body, event = native()
        event.update(bot_id=DRIVER.bot_id, text="<@U0BOT> [test action] approve " + APPROVAL)
        request = SocketModeRequest(type="events_api", envelope_id="env-unsupported", payload=body)
    sock = FakeSocketClient()
    try:
        SocketModeHandler(app, app_token="xapp-test").handle(sock, request)
    finally:
        _finish_owned_listeners(app)
    assert sock.acked_envelope_ids == ["env-unsupported"]
    assert rig.requests == [] and rig.clients == []
    _no_legacy_effects(redis_client, cfg, rig, legacy)


def test_selected_canary_retains_read_only_short_circuit(
    config: DispatcherConfig, redis_client: redis.Redis
) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-19: selected mode cannot turn a canary into work."""
    api()
    cfg = config.model_copy(update={"slack_delivery_canary_channels": (CHANNEL,)})
    rig = HttpRig()
    app, legacy = _selected(cfg, redis_client, rig)
    sock = FakeSocketClient()
    try:
        SocketModeHandler(app, app_token="xapp-test").handle(sock, _request())
    finally:
        _finish_owned_listeners(app)
    assert sock.acked_envelope_ids == ["env-http"] and rig.requests == []
    _no_legacy_effects(redis_client, cfg, rig, legacy)


def test_unselected_existing_socket_lane_keeps_claim_placeholder_and_xadd_order(
    config: DispatcherConfig,
    redis_client: redis.Redis,
) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-3/19: unchanged real-Valkey sibling control."""
    app, web = _build(config, redis_client)
    sock = FakeSocketClient()
    try:
        SocketModeHandler(app, app_token="xapp-test").handle(
            sock,
            _events_api_request("env-legacy", "EvLegacyHTTPControl", _mention_event()),
        )
    finally:
        _finish_owned_listeners(app)
    assert sock.acked_envelope_ids == ["env-legacy"]
    web.chat_postMessage.assert_called_once()
    entries = redis_client.xrange(config.stream)
    assert len(entries) == 1 and redis_client.exists(config.dedupe_key("EvLegacyHTTPControl"))
    turn = from_stream_fields(entries[0][1])
    assert turn.reply_handle.placeholder == "555.000"


def test_supervisor_constructor_passes_selected_runtime_to_its_actual_bolt_connection(
    config: DispatcherConfig,
    redis_client: redis.Redis,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-19: exercise the full existing constructor chain."""
    module = api()
    from curie_dispatcher import run

    rig = HttpRig()
    runtime = rig.runtime(module)
    real_build = run.build_app

    def authorized_build(*args: Any, **kwargs: Any) -> Any:
        return real_build(*args, authorize=_authorize, **kwargs)

    monkeypatch.setattr(run, "build_web_client", lambda *_args: rig.web)
    monkeypatch.setattr(run, "build_redis", lambda _config: redis_client)
    monkeypatch.setattr(run, "build_app", authorized_build)
    group = run.build_supervisor(
        config,
        logger=logging.getLogger("synthetic-http-receiver"),
        identities=(PreflightedIdentity(_identity(), None),),
        http_receivers=(runtime,),
    )
    (connection,) = group.identity_connections
    sock = FakeSocketClient()
    try:
        SocketModeHandler(connection.app, app_token="xapp-test").handle(sock, _request())
    finally:
        _finish_owned_listeners(connection.app)
    assert connection.name == NAME and sock.acked_envelope_ids == ["env-http"]
    assert len(rig.requests) == 1 and redis_client.xlen(config.stream) == 0
    rig.closed()


@pytest.mark.parametrize("selection", ["duplicate", "unknown"])
def test_startup_rejects_duplicate_or_unknown_receiver_identity_selection(
    config: DispatcherConfig,
    redis_client: redis.Redis,
    selection: str,
) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-19: unknown receiver selections refuse."""
    module = api()
    from curie_dispatcher import run

    rig = HttpRig()
    runtime = rig.runtime(
        module,
        scope=module.HttpReceiverScopeV1.model_validate(
            scope_body(
                channel_identity_name=NAME if selection == "duplicate" else "synthetic-unknown"
            ),
        ),
    )
    with pytest.raises(ValueError):
        run.build_identity_connections(
            config,
            (PreflightedIdentity(_identity(), None),),
            redis_client=redis_client,
            logger=logging.getLogger("synthetic-http-selection"),
            http_receivers=(runtime, runtime) if selection == "duplicate" else (runtime,),
        )
    assert rig.requests == []
