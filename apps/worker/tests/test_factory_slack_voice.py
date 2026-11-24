"""Voice turns at the worker's Slack ingress and status reply boundaries."""

from __future__ import annotations

import asyncio
import hashlib
import itertools
from dataclasses import replace
from datetime import UTC, datetime
from typing import Any

import pytest
from aci_protocol import QueuedTurn, ReplyHandle, TurnSource
from curie_api.factory_slack_voice import verified_request
from curie_api.schemas.workitems import FactorySlackSteerIn
from curie_channel_gateway.slack.identities import delivery_key, minted_adapter
from curie_internal.slack_mention_receipt import (
    FACTORY_VOICE_TURN,
    SlackMentionReceipt,
    receipt_key,
    voice_source_key,
)
from curie_internal.turn_queue import enqueue, from_stream_fields
from curie_worker.factory_slack_voice import VOICE_PATH, decide_voice_receipt, voice_status
from curie_worker.factory_steering import KICKOFF_PATH, STEER_PATH, SteeringUnavailable

from apps.api.tests.conftest import _disposable_db as _disposable_db
from apps.api.tests.conftest import clean_db as clean_db
from apps.api.tests.conftest import identity_fence_backend as identity_fence_backend
from apps.api.tests.conftest import identity_fence_key as identity_fence_key
from apps.api.tests.conftest import migrated as migrated
from apps.api.tests.test_factory_slack_runtime import (
    CHANNEL,
    FACTORY_BOT_USER,
    TEAM,
    SteeringStack,
    _assert_no_sandbox,
    _reached_sandbox,
)
from apps.api.tests.test_factory_slack_runtime import steering as steering
from apps.worker.tests.test_factory_steering import (
    DM_CHANNEL,
    _card_client,
    _card_turn,
    _deliver_envelope,
)


@pytest.mark.parametrize(
    ("result", "expected", "delivered"),
    [
        ({"outcome": "pending"}, "preparing", True),
        ({"outcome": "draft"}, "edited", True),
        ({"outcome": "confirmed", "disposition": "queued"}, "queued", True),
        ({"outcome": "confirmed", "disposition": "applied"}, "applied", True),
        ({"outcome": "rejected"}, "rejected", True),
        ({"outcome": "refused", "reason": "disabled"}, "disabled", False),
    ],
)
def test_voice_status_replies_only_with_fixed_text(
    result: dict[str, str], expected: str, delivered: bool
) -> None:
    reply = voice_status({**result, "text": "private transcript <!channel>"})
    assert expected in reply.text.lower()
    assert "private transcript" not in reply.text and "<!channel>" not in reply.text
    assert reply.admitted == delivered


@pytest.mark.parametrize(
    "result",
    [
        {"outcome": "refused", "reason": "unavailable"},
        {"outcome": "confirmed", "disposition": "private transcript"},
        {"outcome": "private transcript"},
    ],
)
def test_unavailable_or_malformed_voice_status_leaves_the_turn_pending(
    result: dict[str, str],
) -> None:
    with pytest.raises(SteeringUnavailable) as raised:
        voice_status(result)
    assert raised.value.pending_text
    assert "private transcript" not in str(raised.value)
    assert "private transcript" not in raised.value.pending_text


@pytest.mark.parametrize(
    ("verb", "status"),
    [
        ("clip", {"outcome": "pending"}),
        ("edit abcd1234ef01.1 Keep the timeout.", {"outcome": "draft"}),
        ("confirm abcd1234ef01.2", {"outcome": "confirmed", "disposition": "queued"}),
        ("reject abcd1234ef01.2", {"outcome": "rejected"}),
    ],
)
def test_worker_sends_only_its_bound_receipt_on_voice_replay(
    verb: str, status: dict[str, str]
) -> None:
    from curie_worker.factory_slack_voice import matches_voice_receipt

    turn = _card_turn(None)
    turn = replace(turn, qevent=turn.qevent.model_copy(update={"text": FACTORY_VOICE_TURN}))
    private_text = f"<@{FACTORY_BOT_USER}> {verb}"
    common = {
        "delivery_id": turn.qevent.event_id,
        "channel_identity": turn.identity,
        "channel": turn.handle.channel,
        "root_ts": turn.root_ts,
        "message_ts": "1800000900.000300",
        "user_id": turn.qevent.author,
        "evidence": {"delivery": "app_mention", "user_id": turn.qevent.author},
        "team_id": TEAM,
        "api_app_id": "A0EXAMPLE1",
    }
    fields = (
        {
            "version": 3,
            "body": "",
            "voice_file_id": "F0EXAMPLE1",
            "content_sha256": hashlib.sha256(private_text.encode()).hexdigest(),
        }
        if verb == "clip"
        else {"version": 1, "body": private_text, "bot_user_id": FACTORY_BOT_USER}
    )
    receipt = SlackMentionReceipt.model_validate({**common, **fields})
    sent: list[tuple[str, dict[str, Any]]] = []
    client = _card_client({**status, "text": "private <!channel>"}, sent)
    intent: list[int] = []

    async def record_intent() -> None:
        intent.append(len(sent))

    async def exercise() -> None:
        async with client._client:
            assert matches_voice_receipt(receipt, turn)
            for _ in range(2):
                response = await decide_voice_receipt(
                    client, turn, receipt, deadline=None, record_intent=record_intent
                )
                assert response.admitted
                assert private_text not in response.text and "<!channel>" not in response.text
            for invalid in (
                None,
                receipt.model_copy(update={"user_id": "U0EXAMPLE8"}),
                receipt.model_copy(update={"channel_identity": "other"}),
                receipt.model_copy(update={"channel": "C0EXAMPLE8"}),
                receipt.model_copy(update={"root_ts": receipt.message_ts}),
                receipt.model_copy(update={"delivery_id": "EvOTHER"}),
            ):
                assert not matches_voice_receipt(invalid, turn)
            if receipt.version == 1:
                invalid = receipt.model_copy(update={"body": f"<@{FACTORY_BOT_USER}> edit prose"})
                assert not matches_voice_receipt(invalid, turn)

    asyncio.run(exercise())
    assert intent == [0, 1]
    assert len(sent) == 2 and sent[0] == sent[1]
    path, body = sent[0]
    assert path == VOICE_PATH
    assert verified_request(receipt, FactorySlackSteerIn.model_validate(body)) is not None
    assert FACTORY_VOICE_TURN not in str(body)


@pytest.mark.parametrize("channel", [CHANNEL, DM_CHANNEL], ids=["unmapped", "dm"])
def test_literal_voice_marker_remains_ordinary_chat(steering: SteeringStack, channel: str) -> None:
    actor = steering.person().slack_user
    since = len(steering.k8s.requests)
    if channel == DM_CHANNEL:
        bound = steering.client.post(
            f"/agents/{steering.agent_id}/channels",
            headers=steering.platform,
            json={"kind": "slack", "address": channel, "adapter": steering.identity},
        )
        assert bound.status_code == 201, bound.text
    delivery_id = delivery_key("EvEXAMPLE1", steering.identity)
    turn = QueuedTurn(
        event_id=delivery_id,
        conversation_id="1800000901.000001",
        author=actor,
        text=FACTORY_VOICE_TURN,
        source=TurnSource.SLACK,
        reply_handle=ReplyHandle(
            kind="slack",
            channel=channel,
            placeholder=None,
            adapter=minted_adapter(steering.identity),
        ),
        received_at=datetime.now(UTC).isoformat(),
    )
    entry_id = enqueue(steering.valkey, steering.dispatcher_config, turn)
    ((_, fields),) = steering.valkey.xrange(steering.names["stream"], min=entry_id, max=entry_id)
    assert from_stream_fields(fields).text == FACTORY_VOICE_TURN

    assert steering.consume() == entry_id
    assert _reached_sandbox(steering, since)
    assert all(
        path not in {VOICE_PATH, STEER_PATH, KICKOFF_PATH}
        for path, _, _ in steering.api_hop.exchanges
    )
    assert steering.valkey_hop.touched(receipt_key(delivery_id)) == []


def test_literal_voice_marker_uses_ordinary_steering_in_a_mapped_thread(
    steering: SteeringStack,
) -> None:
    run = steering.open_steered_run()
    actor = steering.person().slack_user
    delivery = steering.queued_mention(actor, FACTORY_VOICE_TURN, thread_ts=run.root_ts)
    ((entry_id, fields),) = steering.valkey.xrevrange(steering.names["stream"], count=1)
    key = receipt_key(delivery.delivery_id)
    assert from_stream_fields(fields).text == FACTORY_VOICE_TURN
    receipt = SlackMentionReceipt.model_validate_json(steering.valkey.get(key))
    assert receipt.version == 1 and receipt.bot_user_id is None

    assert steering.consume() == entry_id
    ((status, answer),) = steering.api_hop.steer_answers()
    assert status == 200 and answer["outcome"] == "admitted"
    assert len(steering.requests()) == 2
    assert all(path != VOICE_PATH for path, _, _ in steering.api_hop.exchanges)
    assert steering.valkey_hop.touched(key) == [["GET", key]]
    _assert_no_sandbox(steering)


def test_only_a_mapped_clip_receipt_reaches_the_voice_route(steering: SteeringStack) -> None:
    """Local Socket Mode replay through the real worker and the default-disabled API."""
    steering.dispatcher_config = steering.dispatcher_config.model_copy(
        update={"factory_voice_enabled": True}
    )
    run = steering.open_steered_run()
    actor = steering.person().slack_user
    known_requests = {row["id"] for row in steering.requests()}
    ticks = itertools.count(1)

    # The Slack file fields are documented at https://docs.slack.dev/reference/objects/file-object/.
    # The app_mention and slack_audio combination models a native clip for local replay.
    file = {
        "id": "F0EXAMPLE1",
        "name": "example.m4a",
        "mode": "hosted",
        "mimetype": "audio/mp4",
        "subtype": "slack_audio",
        "user": actor,
        "user_team": TEAM,
        "is_external": False,
    }

    def deliver(
        text: str,
        *,
        files: list[dict[str, Any]] | None = None,
        expire: bool = False,
        mismatch: bool = False,
        queued: bool = True,
    ) -> None:
        event: dict[str, Any] = {
            "type": "app_mention",
            "channel": CHANNEL,
            "thread_ts": run.root_ts,
            "ts": f"1800000999.{next(ticks):06d}",
            "user": actor,
            "user_team": TEAM,
            "team": TEAM,
            "text": f"<@{FACTORY_BOT_USER}> {text}",
        }
        if files is not None:
            event.update(subtype="file_share", files=files)
        delivery = _deliver_envelope(steering, event)
        key = receipt_key(delivery.delivery_id)
        if not queued:
            assert delivery.placeholder is None
            assert steering.valkey.get(key) is None
            assert steering.consume() is None
            return
        ((_, fields),) = steering.valkey.xrevrange(steering.names["stream"], count=1)
        assert from_stream_fields(fields).text == FACTORY_VOICE_TURN
        receipt = SlackMentionReceipt.model_validate_json(steering.valkey.get(key))
        if files is not None:
            assert receipt.version == 3 and receipt.team_id and receipt.api_app_id
            steering.base.doomed_keys.append(
                voice_source_key(
                    receipt.channel_identity,
                    receipt.team_id,
                    receipt.api_app_id,
                    receipt.channel,
                    receipt.root_ts,
                    receipt.message_ts,
                )
            )
        else:
            assert receipt.version == 1 and receipt.bot_user_id == FACTORY_BOT_USER
        if expire:
            assert steering.valkey.delete(key)
        if mismatch:
            assert receipt.version == 3
            changed = receipt.model_copy(update={"delivery_id": "EvOTHER"})
            assert steering.valkey.set(key, changed.model_dump_json(), keepttl=True)
        assert steering.consume() is not None
        assert steering.is_terminal(delivery.delivery_id)
        assert steering.valkey_hop.touched(key) == [["GET", key]]
        assert delivery.placeholder is not None
        placeholder_texts = steering.placeholder_texts(delivery.placeholder)
        assert placeholder_texts and text not in placeholder_texts[-1]
        if not expire and not mismatch:
            assert placeholder_texts[-1] == "Voice feedback is disabled on this installation."

    start = len(steering.api_hop.exchanges)
    deliver("Expired clip.", files=[file], expire=True)
    deliver("Mismatched receipt.", files=[file], mismatch=True)
    deliver("Wrong uploader.", files=[{**file, "user": "U0EXAMPLE8"}], queued=False)
    assert all(
        path not in {VOICE_PATH, STEER_PATH, KICKOFF_PATH}
        for path, _, _ in steering.api_hop.exchanges[start:]
    )
    deliver("A recorded suggestion.", files=[file])
    deliver("edit abcd1234ef01.1 Keep the timeout.")
    deliver("confirm abcd1234ef01.2")
    deliver("reject abcd1234ef01.2")
    answers = [
        (path, status, body)
        for path, status, body in steering.api_hop.exchanges[start:]
        if path in {VOICE_PATH, STEER_PATH, KICKOFF_PATH}
    ]
    assert len(answers) == 4
    assert all(
        path == VOICE_PATH
        and status == 200
        and body["outcome"] == "refused"
        and body["reason"] == "disabled"
        for path, status, body in answers
    )
    assert {row["id"] for row in steering.requests()} == known_requests
    _assert_no_sandbox(steering)
