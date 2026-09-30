"""GitHub reply sink: kind "github" must never reach the HTTP egress adapter.

The API's factory writer is the only GitHub writer. A GitHub-bound reply
target carries no endpoint and no adapter credential, so the router must
acknowledge every event locally (``ReplyAck(ref=None)``) and make no network
call at all -- not fall through to ``HttpReplyAdapter``, which raises
``MissingAdapterCredentialError`` when it finds no endpoint.

Negative case: another non-Slack, non-GitHub kind (``mail``) with no endpoint
must still raise ``MissingAdapterCredentialError``, so the fix cannot be a
blanket "no endpoint means succeed" bypass.
"""

from __future__ import annotations

import asyncio

import aiohttp
import pytest
from channel_protocol.reply import (
    REPLY_WIRE_VERSION,
    ReplyTarget,
    ReplyUpdate,
    TurnCompleted,
)
from curie_worker.config import WorkerConfig
from curie_worker.reply_sink import (
    MissingAdapterCredentialError,
    TargetRoute,
    build_reply_sink,
)

SLACK_TOKEN = "xoxb-test-bot-token"
GITHUB_ADDRESS = "acme-corp/acme-bot"


def _target(kind: str, address: str) -> ReplyTarget:
    return ReplyTarget(
        kind=kind,
        address=address,
        conversation_id="thread-1",
        reply_ref="ref-1",
    )


def _update(kind: str, address: str, text: str = "the answer") -> ReplyUpdate:
    return ReplyUpdate(
        version=REPLY_WIRE_VERSION,
        event="reply.update",
        target=_target(kind, address),
        text=text,
    )


def _config() -> WorkerConfig:
    return WorkerConfig(slack_bot_token=SLACK_TOKEN)  # type: ignore[arg-type]


class _NoNetworkSession:
    """A ClientSession stand-in that fails the test if it is ever used.

    Patched in as ``aiohttp.ClientSession`` for the duration of a test, so any
    adapter path that tries to make an HTTP request proves itself wrong by
    raising, rather than a mock silently recording a call nobody checks.
    """

    def __init__(self, *args: object, **kwargs: object) -> None:
        raise AssertionError(
            "no aiohttp.ClientSession should be constructed for a github reply"
        )


def test_a_github_reply_update_acks_locally_with_no_endpoint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(aiohttp, "ClientSession", _NoNetworkSession)

    async def go() -> None:
        sink = build_reply_sink(_config())
        ack = await sink.emit(
            _update("github", GITHUB_ADDRESS),
            route=TargetRoute(endpoint=None, adapter=None),
        )
        assert ack is not None
        assert ack.ref is None

    asyncio.run(go())


def test_a_github_turn_completed_acks_locally_with_no_endpoint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(aiohttp, "ClientSession", _NoNetworkSession)

    async def go() -> None:
        sink = build_reply_sink(_config())
        event = TurnCompleted(
            version=REPLY_WIRE_VERSION,
            event="turn.completed",
            target=_target("github", GITHUB_ADDRESS),
            event_id="ev-1",
            outcome="delivered",
        )
        ack = await sink.emit(
            event,
            route=TargetRoute(endpoint=None, adapter=None),
        )
        assert ack is not None
        assert ack.ref is None

    asyncio.run(go())


# --- Negative: a different non-Slack kind still fails closed with no endpoint -


def test_a_non_github_kind_with_no_endpoint_still_raises() -> None:
    # This is the guard against a blanket "no endpoint -> succeed" fix: only
    # kind "github" may skip the HTTP adapter. Every other kind with no
    # endpoint must still fail closed exactly as it does today.
    async def go() -> None:
        sink = build_reply_sink(_config())
        with pytest.raises(MissingAdapterCredentialError):
            await sink.emit(
                _update("mail", "agent@example.test"),
                route=TargetRoute(endpoint=None, adapter=None),
            )

    asyncio.run(go())
