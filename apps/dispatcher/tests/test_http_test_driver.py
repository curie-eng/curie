"""Declared HTTP test drivers use ADR 0202 admission and real Valkey budgets.

Slack event shapes follow https://docs.slack.dev/reference/events/app_mention/.
The event user deliberately differs from the operator configured driver user.
"""

from __future__ import annotations

import json
import logging
from dataclasses import replace
from typing import Any
from unittest.mock import MagicMock

import httpx
import pytest
import redis
from curie_channel_gateway.slack.config import DispatcherConfig
from curie_channel_gateway.slack.handlers import submit_selected_mention
from curie_internal import identity_context as codec
from curie_internal.driver_declaration import DeclaredDriver

from .test_http_receiver import BOT, CHANNEL, KEY, NAME, RECEIVER, HttpRig, api, native

DRIVER = DeclaredDriver(CHANNEL, "B0EXAMPLEDRIVER", "U0EXAMPLEDRIVER")


def configured(config: DispatcherConfig, **changes: Any) -> DispatcherConfig:
    return config.model_copy(update={
        "test_installation_enabled": True,
        "test_installation_drivers": (DRIVER,),
        "test_installation_thread_turn_limit": 2,
        **changes,
    })


def runtime(rig: HttpRig, config: DispatcherConfig, store: redis.Redis) -> Any:
    module = api()
    from .test_http_receiver import binding_body, scope_body

    return module.build_http_receiver_runtime(
        module.HttpReceiverScopeV1.model_validate(scope_body()),
        (module.HttpReceiverBindingV1.model_validate(binding_body()),),
        receiver_key=KEY, web_client=rig.web, api_base_url="https://core.example.test",
        http_client_factory=rig.factory, config=config, redis_client=store,
    )


def marked(*, command: str = "act", root: bool = False, **changes: Any) -> tuple[Any, Any]:
    body, event = native()
    event.update(
        bot_id=DRIVER.bot_id, user="U0EXAMPLEOTHER", text=f"<@{BOT}> [test action] {command}",
    )
    event.update(changes)
    if root:
        event.pop("thread_ts", None)
    return body, event


async def answer(request: httpx.Request) -> httpx.Response:
    from .test_http_receiver import receipt

    return httpx.Response(204) if request.url.path.endswith("/admission") else httpx.Response(
        200, json=receipt(),
    )


def test_declared_http_driver_uses_configured_user_in_body_and_signed_evidence(
    config: DispatcherConfig, redis_client: redis.Redis,
) -> None:
    rig = HttpRig(answer)
    selected = runtime(rig, configured(config), redis_client)
    body, event = marked()
    result = rig.submit(selected, body=body, event=event)
    assert result.status == "accepted"
    assert [str(req.url.path) for req in rig.requests] == [
        "/channels/turns/admission", "/channels/turns",
    ]
    keys = codec.build_identity_keyring("receiver", ((RECEIVER, 2, KEY),))
    for request in rig.requests:
        assert json.loads(request.content)["author"] == DRIVER.bot_user_id
        proof = codec.verify_receiver_proof(request.headers["X-Curie-Receiver-Proof"], keys)
        assert proof is not None and proof.slack.user_id == DRIVER.bot_user_id
    assert event["user"] == "U0EXAMPLEOTHER"
    rig.closed()


@pytest.mark.parametrize("case", [
    "absent", "disabled", "unlisted", "wrong_channel", "own_bot", "own_user",
    "own_configured_user", "own_app",
    "unmarked", "direct_message", "subtype", "approve", "reject",
])
def test_http_driver_refusals_send_no_api_request(
    config: DispatcherConfig, redis_client: redis.Redis, case: str,
) -> None:
    cfg = configured(config)
    body, event = marked()
    rig = HttpRig(answer, auth={
        "ok": True, "team_id": "T1", "user_id": BOT, "bot_id": "B1", "app_id": "A0EXAMPLEOWN",
        "is_enterprise_install": False,
    })
    if case == "absent":
        cfg = config
    elif case == "disabled":
        cfg = configured(config, test_installation_enabled=False)
    elif case == "unlisted":
        event["bot_id"] = "B0EXAMPLEOTHER"
    elif case == "wrong_channel":
        cfg = configured(
            config, test_installation_drivers=(replace(DRIVER, channel_id="C0EXAMPLEOTHER"),),
        )
    elif case == "own_bot":
        event["bot_id"] = "B1"
        cfg = configured(config, test_installation_drivers=(replace(DRIVER, bot_id="B1"),))
    elif case == "own_user":
        event["user"] = BOT
    elif case == "own_configured_user":
        cfg = configured(config, test_installation_drivers=(replace(DRIVER, bot_user_id=BOT),))
    elif case == "own_app":
        event["app_id"] = "A0EXAMPLEOWN"
    elif case == "unmarked":
        event["text"] = "ordinary text"
    elif case == "direct_message":
        event.update(type="message", channel_type="im")
    elif case == "subtype":
        event["subtype"] = "message_changed"
    else:
        event["text"] = f"[test action] {case} approval-id"
    selected = runtime(rig, cfg, redis_client)
    assert rig.submit(selected, body=body, event=event).status == "refused"
    assert rig.requests == []


def test_http_driver_ping_and_turn_cap_share_legacy_budget(
    config: DispatcherConfig, redis_client: redis.Redis,
) -> None:
    cfg = configured(config)
    rig = HttpRig(answer)
    rig.web.chat_postMessage = MagicMock(return_value={"ok": True, "ts": "1700.0003"})
    selected = runtime(rig, cfg, redis_client)
    body, event = marked(command="ping", root=True)
    for _ in range(2):
        result = submit_selected_mention(
            selected, web_client=rig.web, config=cfg, log=logging.getLogger(__name__),
            body=body, event=event, slack_identity=NAME, bot_user_id=BOT,
        )
        assert result.result.status == "accepted" and result.placeholder is None
    rig.web.chat_postMessage.assert_called_once_with(
        channel=CHANNEL, thread_ts=event["ts"],
        text=f"This installation accepts test actions from <@{DRIVER.bot_user_id}>.",
    )
    for index in range(3):
        body, event = marked(thread_ts="1700.0002")
        body["event_id"] = f"EvDriver{index}"
        assert rig.submit(selected, body=body, event=event).status == (
            "accepted" if index < 2 else "refused"
        )
    assert sum(req.url.path == "/channels/turns" for req in rig.requests) == 2
    assert rig.web.chat_postMessage.call_args.kwargs["text"] == (
        "This installation does not accept test actions."
    )
    from curie_channel_gateway.slack.marked_actions import reserve_turn

    assert not reserve_turn(redis_client, cfg, identity=NAME, channel=CHANNEL, thread="1700.0002")


def test_http_driver_group_refusal_precedes_dedupe_budget_and_placeholder(
    config: DispatcherConfig, redis_client: redis.Redis,
) -> None:
    async def refuse(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(403, json={"detail": "caller_not_allowed"})

    cfg = configured(config)
    rig = HttpRig(refuse)
    rig.web.chat_postMessage = MagicMock()
    selected = runtime(rig, cfg, redis_client)
    body, event = marked()
    result = submit_selected_mention(
        selected, web_client=rig.web, config=cfg, log=logging.getLogger(__name__),
        body=body, event=event, slack_identity=NAME, bot_user_id=BOT,
    )
    assert result.result.status == "refused" and result.placeholder is None
    rig.web.chat_postMessage.assert_not_called()
    assert not list(redis_client.scan_iter(match=cfg.dedupe_prefix + "*"))


@pytest.mark.parametrize("command", ["ping", "approve", "reject"])
def test_threaded_ping_and_approval_verbs_never_start_http_turns(
    config: DispatcherConfig, redis_client: redis.Redis, command: str,
) -> None:
    rig = HttpRig(answer)
    rig.web.chat_postMessage = MagicMock()
    selected = runtime(rig, configured(config), redis_client)
    body, event = marked(command=command)
    assert rig.submit(selected, body=body, event=event).status == "refused"
    assert not any(req.url.path == "/channels/turns" for req in rig.requests)
    if command == "ping":
        assert rig.web.chat_postMessage.call_args.kwargs["text"] == (
            "This installation does not accept test actions."
        )
    else:
        rig.web.chat_postMessage.assert_not_called()


def test_receiver_initializer_passes_existing_environment_declaration(
    config: DispatcherConfig, redis_client: redis.Redis, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from curie_channel_gateway.slack.http_receiver_initializer import (
        HttpReceiverRegistration,
        build_selected_receivers,
    )
    from curie_channel_gateway.slack.identities import SlackIdentityCredentials
    from curie_channel_gateway.slack.preflight import PreflightedIdentity

    from .test_http_receiver import binding_body, scope_body

    module = api()
    monkeypatch.setenv("CURIE_TEST_INSTALLATION_ENABLED", "true")
    monkeypatch.setenv("CURIE_TEST_INSTALLATION_DRIVERS", json.dumps([{
        "channel_id": DRIVER.channel_id, "bot_id": DRIVER.bot_id,
        "bot_user_id": DRIVER.bot_user_id,
    }]))
    connection = redis_client.connection_pool.connection_kwargs
    cfg = DispatcherConfig(
        api_key="synthetic-private-api-key",
        approval_chat_attester_secret="synthetic-private-attester-key",
        dedupe_prefix=config.dedupe_prefix,
        valkey_host=connection["host"], valkey_port=connection["port"],
        valkey_password=connection.get("password") or "",
    )
    rig = HttpRig()
    result = build_selected_receivers(
        cfg,
        (PreflightedIdentity(SlackIdentityCredentials(NAME, "xapp-test", "xoxb-test"), None),),
        (HttpReceiverRegistration(
            module.HttpReceiverScopeV1.model_validate(scope_body()),
            (module.HttpReceiverBindingV1.model_validate(binding_body()),), KEY,
        ),),
        web_client_factory=lambda _config, _identity: rig.web,
        logger=logging.getLogger(__name__),
    )
    assert result.excluded == frozenset() and len(result.receivers) == 1
    body, event = marked()
    assert result.receivers[0].prepare(
        body=body, event=event, slack_identity=NAME, bot_user_id=BOT,
    ) is not None


@pytest.mark.parametrize("failure", ["unavailable", "redirect", "timeout"])
def test_unavailable_http_driver_admission_spends_no_budget(
    config: DispatcherConfig, redis_client: redis.Redis, failure: str,
) -> None:
    async def unavailable(request: httpx.Request) -> httpx.Response:
        if failure == "timeout":
            raise httpx.ReadTimeout("synthetic directory timeout", request=request)
        return httpx.Response(503 if failure == "unavailable" else 302)

    cfg = configured(config)
    rig = HttpRig(unavailable)
    rig.web.chat_postMessage = MagicMock()
    body, event = marked()
    outcome = rig.submit(runtime(rig, cfg, redis_client), body=body, event=event)
    assert outcome.status == "unavailable"
    rig.web.chat_postMessage.assert_not_called()
    assert len(rig.requests) == 1
    assert not list(redis_client.scan_iter(match=cfg.dedupe_prefix + "*"))
    rig.closed()


def test_marked_driver_through_real_bolt_http_listener(
    config: DispatcherConfig, redis_client: redis.Redis,
) -> None:
    from curie_channel_gateway.slack.app import build_app
    from curie_channel_gateway.slack.identities import SlackIdentityCredentials
    from slack_bolt.adapter.socket_mode import SocketModeHandler
    from slack_sdk.socket_mode.request import SocketModeRequest

    from .conftest import FakeSocketClient, _authorize
    from .test_http_receiver_dispatch import _finish_owned_listeners

    cfg = configured(config)
    rig = HttpRig(answer)
    rig.web.chat_postMessage = MagicMock(return_value={"ok": True, "ts": "1700.0003"})
    app = build_app(
        cfg, web_client=rig.web, redis_client=redis_client,
        identity=SlackIdentityCredentials(NAME, "xapp-test", "xoxb-test"),
        authorize=_authorize, http_receiver=runtime(rig, cfg, redis_client),
    )
    sock = FakeSocketClient()
    body, _event = marked()
    try:
        SocketModeHandler(app, "xapp-test").handle(
            sock, SocketModeRequest(type="events_api", envelope_id="env-driver", payload=body),
        )
        _finish_owned_listeners(app)
        assert sock.acked_envelope_ids == ["env-driver"]
        assert [req.url.path for req in rig.requests] == [
            "/channels/turns/admission", "/channels/turns",
        ]
        assert json.loads(rig.requests[-1].content)["author"] == DRIVER.bot_user_id
        assert rig.web.chat_postMessage.call_count == 1
        assert rig.web.chat_postMessage.call_args.kwargs["text"] == cfg.placeholder_text
    finally:
        _finish_owned_listeners(app)
