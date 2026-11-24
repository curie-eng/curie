"""A definite unavailable answer settles the selected placeholder.

@spec HTTP-INVOCATION-EXECUTION-32. Actual Bolt listeners with real Valkey,
recorded Slack doubles and a stand-in API; synthetic identifiers only.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import call

import httpx
import pytest
import redis
from curie_channel_gateway.slack.config import DispatcherConfig

from .test_http_receiver import CHANNEL, SENDER, HttpRig, api, receipt
from .test_http_receiver import native as channel_native
from .test_http_receiver_direct_messages import DM_CHANNEL, dm_bindings, dm_native
from .test_http_receiver_dispatch import (
    PLACEHOLDER_TS,
    UNAVAILABLE_TEXT,
    _answering,
    _deliver_selected,
)

DEFINITE = "identity_authority_unavailable"


def _ambiguous_then(status: int, detail: str) -> Any:
    """A lost first response, then a complete answer to the identical retry."""
    attempts = 0

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise httpx.ReadTimeout("SyntheticAmbiguousFailure", request=request)
        return httpx.Response(status, json={"detail": detail})

    return handler


def test_receiver_marks_only_a_first_attempt_definite_unavailable_as_nothing_queued() -> None:
    """@spec HTTP-INVOCATION-EXECUTION-32."""
    module = api()
    definite = HttpRig(_answering(503, DEFINITE))
    result = definite.submit(definite.runtime(module))
    assert result.status == "unavailable" and result.nothing_queued is True
    for handler in (
        _answering(503),
        _answering(500, DEFINITE),
        _answering(429, DEFINITE),
        _ambiguous_then(503, DEFINITE),
    ):
        rig = HttpRig(handler)
        other = rig.submit(rig.runtime(module))
        assert other.status == "unavailable" and other.nothing_queued is False
    accepted = HttpRig(lambda _request: _ok())
    assert accepted.submit(accepted.runtime(module)).nothing_queued is False


async def _ok() -> httpx.Response:
    return httpx.Response(200, json=receipt())


def test_channel_mention_placeholder_becomes_the_unavailable_sentence(
    config: DispatcherConfig, redis_client: redis.Redis
) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-32: no ephemeral, no second message, no delete."""
    api()
    rig = HttpRig(_answering(503, DEFINITE))
    body, _event = channel_native()
    web = _deliver_selected(config, redis_client, rig, body)
    web.chat_postMessage.assert_called_once_with(
        channel=CHANNEL, thread_ts="1700.0001", text=config.placeholder_text,
    )
    web.chat_update.assert_called_once_with(
        channel=CHANNEL, ts=PLACEHOLDER_TS, text=UNAVAILABLE_TEXT,
    )
    web.chat_postEphemeral.assert_not_called()
    web.chat_delete.assert_not_called()
    rig.closed()


def test_direct_message_placeholder_becomes_the_unavailable_sentence(
    config: DispatcherConfig, redis_client: redis.Redis
) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-32."""
    module = api()
    rig = HttpRig(_answering(503, DEFINITE))
    body, _event = dm_native()
    web = _deliver_selected(config, redis_client, rig, body, bindings=dm_bindings(module))
    assert web.chat_postMessage.call_args_list == [
        call(channel=DM_CHANNEL, thread_ts="1700.0004", text=config.placeholder_text),
    ]
    web.chat_update.assert_called_once_with(
        channel=DM_CHANNEL, ts=PLACEHOLDER_TS, text=UNAVAILABLE_TEXT,
    )
    web.chat_postEphemeral.assert_not_called()
    web.chat_delete.assert_not_called()
    rig.closed()


@pytest.mark.parametrize(
    "handler",
    [
        _answering(503),
        _answering(500, DEFINITE),
        _ambiguous_then(503, DEFINITE),
    ],
    ids=["other-detail", "server-error", "after-ambiguous-attempt"],
)
def test_any_other_unavailable_keeps_criterion_27(
    config: DispatcherConfig, redis_client: redis.Redis, handler: Any
) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-27/32: the placeholder stays and the sender is told."""
    api()
    rig = HttpRig(handler)
    body, _event = channel_native()
    web = _deliver_selected(config, redis_client, rig, body)
    web.chat_update.assert_not_called()
    web.chat_delete.assert_not_called()
    web.chat_postEphemeral.assert_called_once_with(
        channel=CHANNEL, user=SENDER, thread_ts="1700.0001", text=UNAVAILABLE_TEXT,
    )
    rig.closed()


def test_a_failed_edit_falls_back_to_criterion_27s_notice(
    config: DispatcherConfig, redis_client: redis.Redis
) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-32: the sender is still told when the edit fails."""
    api()
    answer = _answering(503, DEFINITE)

    async def handler(request: httpx.Request) -> httpx.Response:
        # The listener's Slack doubles exist by now; make its edit fail.
        rig.web.chat_update.side_effect = RuntimeError("synthetic edit failure")
        return await answer(request)

    rig = HttpRig(handler)
    body, _event = channel_native()
    web = _deliver_selected(config, redis_client, rig, body)
    web.chat_update.assert_called_once()
    web.chat_postEphemeral.assert_called_once_with(
        channel=CHANNEL, user=SENDER, thread_ts="1700.0001", text=UNAVAILABLE_TEXT,
    )
    web.chat_delete.assert_not_called()
    rig.closed()


def test_a_redelivered_event_deletes_its_own_placeholder_instead_of_a_second_notice(
    config: DispatcherConfig, redis_client: redis.Redis
) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-32: one notice per native event_id."""
    api()
    rig = HttpRig(_answering(503, DEFINITE))
    first, _event = channel_native()
    again, _event = channel_native()
    web = _deliver_selected(
        config, redis_client, rig, first, again, placeholders=(PLACEHOLDER_TS, "556.000"),
    )
    assert len(rig.requests) == 2
    # Bolt's listener pool may run the two deliveries in either order: one
    # placeholder becomes the notice and the other is deleted.
    edited = [(c.kwargs["channel"], c.kwargs["ts"]) for c in web.chat_update.call_args_list]
    deleted = [(c.kwargs["channel"], c.kwargs["ts"]) for c in web.chat_delete.call_args_list]
    assert len(edited) == len(deleted) == 1
    assert sorted(edited + deleted) == [(CHANNEL, PLACEHOLDER_TS), (CHANNEL, "556.000")]
    assert web.chat_update.call_args.kwargs["text"] == UNAVAILABLE_TEXT
    web.chat_postEphemeral.assert_not_called()
    rig.closed()
