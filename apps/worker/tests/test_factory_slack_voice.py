"""Voice turns at the worker's Slack ingress and status reply boundaries."""

from __future__ import annotations

import asyncio
import hashlib
import itertools
from dataclasses import replace
from typing import Any

import pytest
from curie_api.factory_slack_voice import verified_request
from curie_api.schemas.workitems import FactorySlackSteerIn
from curie_internal.slack_mention_receipt import (
    FACTORY_VOICE_TURN,
    SlackMentionReceipt,
    receipt_key,
)
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
)
from apps.api.tests.test_factory_slack_runtime import steering as steering
from apps.worker.tests.test_factory_steering import _card_client, _card_turn, _deliver_envelope


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
                refusal = await decide_voice_receipt(
                    client, turn, invalid, deadline=None, record_intent=record_intent
                )
                assert not refusal.admitted

    asyncio.run(exercise())
    assert intent == [0, 1]
    assert len(sent) == 2 and sent[0] == sent[1]
    path, body = sent[0]
    assert path == VOICE_PATH
    assert verified_request(receipt, FactorySlackSteerIn.model_validate(body)) is not None
    assert FACTORY_VOICE_TURN not in str(body)


def test_only_a_mapped_clip_receipt_reaches_the_voice_route(steering: SteeringStack) -> None:
    """Local Socket Mode replay through the real worker and the default-disabled API."""
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
        root_ts: str,
        text: str,
        *,
        files: list[dict[str, Any]] | None = None,
        expire: bool = False,
    ) -> None:
        event: dict[str, Any] = {
            "type": "app_mention",
            "channel": CHANNEL,
            "thread_ts": root_ts,
            "ts": f"1800000999.{next(ticks):06d}",
            "user": actor,
            "user_team": TEAM,
            "team": TEAM,
            "text": f"<@{FACTORY_BOT_USER}> {text}",
        }
        if files is not None:
            event.update(subtype="file_share", files=files)
        delivery = _deliver_envelope(steering, event)
        if expire:
            assert steering.valkey.delete(receipt_key(delivery.delivery_id))
        assert steering.consume() is not None
        assert steering.is_terminal(delivery.delivery_id)
        assert delivery.placeholder is not None
        placeholder_texts = steering.placeholder_texts(delivery.placeholder)
        assert placeholder_texts and text not in placeholder_texts[-1]

    start = len(steering.api_hop.exchanges)
    deliver(run.root_ts, "Expired clip.", files=[file], expire=True)
    deliver(run.root_ts, "Wrong uploader.", files=[{**file, "user": "U0EXAMPLE8"}])
    deliver("1800000900.000333", "Unmapped thread.", files=[file])
    assert all(
        path not in {VOICE_PATH, STEER_PATH, KICKOFF_PATH}
        for path, _, _ in steering.api_hop.exchanges[start:]
    )
    deliver(run.root_ts, "A recorded suggestion.", files=[file])
    deliver(run.root_ts, "edit abcd1234ef01.1 Keep the timeout.")
    deliver(run.root_ts, "confirm abcd1234ef01.2")
    answers = [
        (path, status, body)
        for path, status, body in steering.api_hop.exchanges[start:]
        if path in {VOICE_PATH, STEER_PATH, KICKOFF_PATH}
    ]
    assert len(answers) == 3
    assert all(
        path == VOICE_PATH
        and status == 200
        and body["outcome"] == "refused"
        and body["reason"] == "disabled"
        for path, status, body in answers
    )
    assert {row["id"] for row in steering.requests()} == known_requests
    _assert_no_sandbox(steering)
