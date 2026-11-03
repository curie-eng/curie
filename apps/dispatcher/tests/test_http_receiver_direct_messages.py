"""Selected receiver intake of one-to-one direct messages to its bot.

@spec HTTP-INVOCATION-EXECUTION-26 HTTP-INVOCATION-EXECUTION-27.
Synthetic Slack/API edges, the actual Bolt Socket Mode owner and real Valkey.
No real direct message delivery is qualified: its team fields remain unmeasured.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any
from unittest.mock import call

import httpx
import pytest
import redis
from curie_dispatcher.config import DispatcherConfig
from curie_internal import identity_context as codec

from .test_http_receiver import (
    BINDING,
    BOT,
    CHANNEL,
    CREDENTIAL,
    IDENTITY,
    INSTALLATION,
    KEY,
    NAME,
    RECEIVER,
    SENDER,
    HttpRig,
    api,
    binding_body,
    native,
    receipt,
)
from .test_http_receiver_dispatch import (
    DM_BINDING,
    DM_CHANNEL,
    PLACEHOLDER_TS,
    REFUSED_TEXT,
    UNAVAILABLE_TEXT,
    _answering,
    _deliver_selected,
    _duplicate_of,
    _notice_texts,
    _posted_texts,
)

# Marks a field the synthetic event leaves out entirely.
_ABSENT = object()


def dm_native(**changes: Any) -> tuple[dict[str, Any], dict[str, Any]]:
    """@spec HTTP-INVOCATION-EXECUTION-26: a plain one-to-one direct message envelope."""
    event = {
        "type": "message", "channel": DM_CHANNEL, "channel_type": "im", "user": SENDER,
        "text": "Hello synthetic direct message", "ts": "1700.0004", "team": "T1",
        "user_team": "T1", **changes,
    }
    event = {key: value for key, value in event.items() if value is not _ABSENT}
    return {
        "type": "event_callback", "event_id": "EvNativeDirectSynthetic", "team_id": "T1",
        "is_ext_shared_channel": False, "event": event,
    }, event


def dm_bindings(module: Any) -> tuple[Any, ...]:
    """@spec HTTP-INVOCATION-EXECUTION-26: the operator registers the DM conversation."""
    return (
        module.HttpReceiverBindingV1.model_validate(binding_body()),
        module.HttpReceiverBindingV1.model_validate(
            binding_body(native_channel_id=DM_CHANNEL, binding_id=DM_BINDING),
        ),
    )


def test_plain_direct_message_signs_its_conversation_type_and_exact_turn() -> None:
    """@spec HTTP-INVOCATION-EXECUTION-2/19/26: the copier is reused, the receiver adds im."""
    module = api()
    rig = HttpRig()
    runtime = rig.runtime(module, bindings=dm_bindings(module))
    body, event = dm_native()
    result = runtime.submit(body=body, event=event, slack_identity=NAME, bot_user_id=BOT)
    assert result.status == "accepted"
    (request,) = rig.requests
    assert request.headers["X-API-Key"] == CREDENTIAL
    assert json.loads(request.content) == {
        "kind": "slack", "address": DM_CHANNEL, "adapter": NAME,
        "delivery_id": "EvNativeDirectSynthetic", "conversation_id": "1700.0004",
        "author": SENDER, "text": "Hello synthetic direct message", "reply_ref": "",
        "attachments": [],
    }
    ring = codec.build_identity_keyring("receiver", ((RECEIVER, 2, KEY),))
    proof = codec.verify_receiver_proof(request.headers["X-Curie-Receiver-Proof"], ring)
    assert isinstance(proof, codec.ReceiverProofV1)
    assert proof.model_dump(mode="json") == {
        "schema_version": 1, "receiver_id": RECEIVER, "receiver_generation": 2,
        "channel_identity_id": IDENTITY, "provider_installation_id": INSTALLATION,
        "binding_id": DM_BINDING, "binding_generation": 7,
        "delivery_id": "EvNativeDirectSynthetic", "native_channel_id": DM_CHANNEL,
        "conversation_id": "1700.0004", "body_sha256": hashlib.sha256(request.content).hexdigest(),
        "slack": {
            "delivery": "message", "user_id": SENDER, "event_user_team": "T1",
            "event_team": "T1", "interaction_user_team_id": None,
            "is_ext_shared_channel": False, "enterprise_ids": [], "channel_type": "im",
        },
    }
    rig.closed()


def test_threaded_direct_message_keeps_its_thread_as_the_conversation() -> None:
    """@spec HTTP-INVOCATION-EXECUTION-19/26."""
    module = api()
    rig = HttpRig()
    runtime = rig.runtime(module, bindings=dm_bindings(module))
    body, event = dm_native(thread_ts="1700.0003")
    assert runtime.submit(
        body=body, event=event, slack_identity=NAME, bot_user_id=BOT,
    ).status == "accepted"
    assert json.loads(rig.requests[0].content)["conversation_id"] == "1700.0003"


def test_mention_proof_carries_no_conversation_type() -> None:
    """@spec HTTP-INVOCATION-EXECUTION-26: every mention signs the same bytes as before."""
    module = api()
    rig = HttpRig()
    runtime = rig.runtime(module)
    body, event = native()
    assert runtime.submit(
        body=body, event=event, slack_identity=NAME, bot_user_id=BOT,
    ).status == "accepted"
    ring = codec.build_identity_keyring("receiver", ((RECEIVER, 2, KEY),))
    proof = codec.verify_receiver_proof(rig.requests[0].headers["X-Curie-Receiver-Proof"], ring)
    assert isinstance(proof, codec.ReceiverProofV1)
    assert proof.binding_id == BINDING and proof.native_channel_id == CHANNEL
    assert "channel_type" not in proof.model_dump(mode="json")["slack"]
    # A mention that happens to carry a channel_type still signs none.
    body, event = native()
    event["channel_type"] = "channel"
    assert runtime.submit(
        body=body, event=event, slack_identity=NAME, bot_user_id=BOT,
    ).status == "accepted"
    proof = codec.verify_receiver_proof(rig.requests[1].headers["X-Curie-Receiver-Proof"], ring)
    assert isinstance(proof, codec.ReceiverProofV1)
    assert "channel_type" not in proof.model_dump(mode="json")["slack"]


DROPPED = {
    "mpim": {"channel_type": "mpim", "channel": "G0EXAMPLE1"},
    "channel": {"channel_type": "channel", "channel": CHANNEL},
    "group": {"channel_type": "group", "channel": "G0EXAMPLE1"},
    "no_channel_type": {"channel_type": _ABSENT},
    "edit": {"subtype": "message_changed"},
    "delete": {"subtype": "message_deleted"},
    "file_share": {"subtype": "file_share"},
    "bot_message": {"subtype": "bot_message"},
    "thread_broadcast": {"subtype": "thread_broadcast"},
    "null_subtype": {"subtype": None},
    "bot_id": {"bot_id": "B0EXAMPLEOTHER"},
    "no_user": {"user": _ABSENT},
    "empty_user": {"user": ""},
}


@pytest.mark.parametrize("damage", sorted(DROPPED))
def test_every_other_message_event_refuses_before_http(damage: str) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-26: only a plain one-to-one DM is a new source."""
    module = api()
    rig = HttpRig()
    runtime = rig.runtime(module, bindings=dm_bindings(module))
    body, event = dm_native(**DROPPED[damage])
    result = runtime.submit(body=body, event=event, slack_identity=NAME, bot_user_id=BOT)
    assert result.status == "refused" and result.stream_id is None
    assert rig.requests == []


@pytest.mark.parametrize("damage", ["unbound", "envelope_team", "identity", "bot", "event_id"])
def test_direct_message_keeps_every_mention_check(damage: str) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-19/26."""
    module = api()
    rig = HttpRig()
    runtime = rig.runtime(module, bindings=dm_bindings(module))
    body, event = dm_native()
    kwargs = {"body": body, "event": event, "slack_identity": NAME, "bot_user_id": BOT}
    if damage == "unbound":
        event["channel"] = "D0EXAMPLEUNBOUND"
    elif damage == "envelope_team":
        body["team_id"] = "T0EXAMPLESTRANGER"
    elif damage == "identity":
        kwargs["slack_identity"] = "synthetic-other"
    elif damage == "bot":
        kwargs["bot_user_id"] = "synthetic-other"
    else:
        body.pop("event_id")
    result = runtime.submit(**kwargs)
    assert result.status == "refused"
    assert rig.requests == []


def _slot_sent(rig: HttpRig) -> str:
    """@spec HTTP-INVOCATION-EXECUTION-27: the reply slot the submitted TurnIn carried."""
    return str(json.loads(rig.requests[0].content)["reply_ref"])


def test_refused_direct_message_turns_its_placeholder_into_the_refusal(
    config: DispatcherConfig, redis_client: redis.Redis,
) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-26/27: the actual Bolt message listener submits it."""
    module = api()
    rig = HttpRig(_answering(403))
    body, _event = dm_native()
    web = _deliver_selected(config, redis_client, rig, body, bindings=dm_bindings(module))
    assert len(rig.requests) == 1 and _slot_sent(rig) == PLACEHOLDER_TS
    web.chat_postMessage.assert_called_once_with(
        channel=DM_CHANNEL, thread_ts="1700.0004", text=config.placeholder_text,
    )
    web.chat_update.assert_called_once_with(
        channel=DM_CHANNEL, ts=PLACEHOLDER_TS, text=REFUSED_TEXT,
    )
    web.chat_delete.assert_not_called()
    web.chat_postEphemeral.assert_not_called()
    rig.closed()


def test_unavailable_direct_message_keeps_its_placeholder_and_replies_the_notice(
    config: DispatcherConfig, redis_client: redis.Redis,
) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-19/26/27: unavailable never removes the placeholder."""
    module = api()
    rig = HttpRig(_answering(503))
    body, _event = dm_native()
    web = _deliver_selected(config, redis_client, rig, body, bindings=dm_bindings(module))
    assert len(rig.requests) == 1 and _slot_sent(rig) == PLACEHOLDER_TS
    assert web.chat_postMessage.call_args_list == [
        call(channel=DM_CHANNEL, thread_ts="1700.0004", text=config.placeholder_text),
        call(channel=DM_CHANNEL, thread_ts="1700.0004", text=UNAVAILABLE_TEXT),
    ]
    web.chat_update.assert_not_called()
    web.chat_delete.assert_not_called()
    web.chat_postEphemeral.assert_not_called()
    rig.closed()


def test_direct_message_in_an_unregistered_conversation_gets_the_refusal(
    config: DispatcherConfig, redis_client: redis.Redis,
) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-26: a local refusal reads exactly like an API one."""
    api()
    rig = HttpRig()
    body, _event = dm_native(channel="D0EXAMPLEUNBOUND", thread_ts="1700.0003")
    web = _deliver_selected(config, redis_client, rig, body)
    assert rig.requests == []
    web.chat_postMessage.assert_called_once_with(
        channel="D0EXAMPLEUNBOUND", thread_ts="1700.0003", text=REFUSED_TEXT,
    )


def test_accepted_direct_message_keeps_its_placeholder_and_a_redelivery_one_notice(
    config: DispatcherConfig, redis_client: redis.Redis,
) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-19/26/27: the answer edits the DM's placeholder."""
    module = api()
    rig = HttpRig()
    body, _event = dm_native()
    web = _deliver_selected(config, redis_client, rig, body, bindings=dm_bindings(module))
    assert len(rig.requests) == 1 and _slot_sent(rig) == PLACEHOLDER_TS
    web.chat_postMessage.assert_called_once_with(
        channel=DM_CHANNEL, thread_ts="1700.0004", text=config.placeholder_text,
    )
    assert _notice_texts(web, config) == []
    web.chat_delete.assert_not_called()
    refused = HttpRig(_answering(403))
    first, _event = dm_native()
    again, _event = dm_native()
    web = _deliver_selected(
        config, redis_client, refused, first, again, bindings=dm_bindings(module),
        placeholders=(PLACEHOLDER_TS, "556.000"),
    )
    assert len(refused.requests) == 2
    # Either delivery may run first: it turns its placeholder into the notice
    # and the repeated event_id deletes its own.
    web.chat_update.assert_called_once()
    web.chat_delete.assert_called_once()
    updated, deleted = web.chat_update.call_args.kwargs, web.chat_delete.call_args.kwargs
    assert updated["channel"] == deleted["channel"] == DM_CHANNEL
    assert updated["text"] == REFUSED_TEXT
    assert {updated["ts"], deleted["ts"]} == {PLACEHOLDER_TS, "556.000"}
    assert _notice_texts(web, config) == [REFUSED_TEXT]


@pytest.mark.parametrize(
    ("slot", "deleted"),
    [(PLACEHOLDER_TS, False), ("554.000", True), (None, False)],
    ids=["own_slot", "other_slot", "no_slot"],
)
def test_duplicate_direct_message_deletes_only_a_placeholder_another_slot_owns(
    config: DispatcherConfig, redis_client: redis.Redis, slot: str | None, deleted: bool,
) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-26/27: the DM lane follows the mention rules."""
    module = api()
    if slot is None:
        answer = receipt(duplicate=True)

        async def handler(_request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json=answer)

        rig = HttpRig(handler)
    else:
        rig = HttpRig(_duplicate_of(slot))
    body, _event = dm_native()
    web = _deliver_selected(config, redis_client, rig, body, bindings=dm_bindings(module))
    assert _slot_sent(rig) == PLACEHOLDER_TS
    if deleted:
        web.chat_delete.assert_called_once_with(channel=DM_CHANNEL, ts=PLACEHOLDER_TS)
    else:
        web.chat_delete.assert_not_called()
    web.chat_update.assert_not_called()
    assert _notice_texts(web, config) == []


@pytest.mark.parametrize("damage", sorted(DROPPED))
def test_every_other_dropped_message_event_stays_silent(
    config: DispatcherConfig, redis_client: redis.Redis, damage: str,
) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-26: no submission, no notice, no legacy work."""
    module = api()
    rig = HttpRig(_answering(403))
    body, _event = dm_native(**DROPPED[damage])
    web = _deliver_selected(config, redis_client, rig, body, bindings=dm_bindings(module))
    assert rig.requests == [] and rig.clients == []
    assert _posted_texts(web) == []
    web.chat_postMessage.assert_not_called()
    web.chat_postEphemeral.assert_not_called()


def test_api_refusal_of_a_direct_message_never_reaches_legacy_lanes(
    config: DispatcherConfig, redis_client: redis.Redis,
) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-19/26: a complete outcome terminates the lane."""
    module = api()

    async def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(401, json={"detail": "invalid_http_receiver"})

    rig = HttpRig(handler)
    body, _event = dm_native()
    web = _deliver_selected(config, redis_client, rig, body, bindings=dm_bindings(module))
    assert len(rig.requests) == 1
    web.chat_update.assert_called_once_with(
        channel=DM_CHANNEL, ts=PLACEHOLDER_TS, text=REFUSED_TEXT,
    )
    assert _notice_texts(web, config) == [REFUSED_TEXT]
