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
from .test_http_receiver import CHANNEL, NAME, HttpRig, api, native, receipt, scope_body
from .test_test_actions import DRIVER

APPROVAL = "55555555-5555-4555-8555-555555555555"


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


def _selected(config: DispatcherConfig, redis_client: redis.Redis, rig: HttpRig) -> tuple[Any, Any]:
    """@spec HTTP-INVOCATION-EXECUTION-19: actual constructor and listener registration."""
    module = api()
    runtime = rig.runtime(module)
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
    _no_legacy_effects(redis_client, config, rig, legacy)
    rig.closed()


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
