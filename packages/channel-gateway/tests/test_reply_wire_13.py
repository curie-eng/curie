"""Slack adapter for reply wire 1.3.

Provider contract: https://docs.slack.dev/reference/methods/chat.update/
and https://docs.slack.dev/reference/methods/chat.postEphemeral/
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest
from channel_protocol import MESSAGE_VERSION, OutboundMessage
from channel_protocol.reply import ReplyPost, ReplyTarget, ReplyUpdate
from curie_channel_gateway.reply_route import ReplyDestination
from curie_channel_gateway.slack import slack_sink as slack_sink_module
from curie_channel_gateway.slack.slack_sink import SlackReplyAdapter
from slack_sdk.errors import SlackApiError

_TARGET = ReplyTarget(
    kind="slack",
    address="C0EXAMPLE1",
    conversation_id="1720000000.000100",
    reply_ref="1720000000.000400",
)


def _message(text: str, header: str) -> OutboundMessage:
    return OutboundMessage(version=MESSAGE_VERSION, text=text, header=header)


def _replace() -> ReplyUpdate:
    return ReplyUpdate(
        version="1.3",
        event="reply.update",
        target=_TARGET,
        message=_message("replacement body", "Replacement heading"),
        message_mode="replace",
    )


def _audience() -> ReplyPost:
    return ReplyPost(
        version="1.3",
        event="reply.post",
        target=_TARGET,
        message=_message("only you can see this", "Private heading"),
        requested_by="U0EXAMPLE1",
        audience_user="U0EXAMPLE1",
    )


def _install(sink: SlackReplyAdapter, **methods: Any) -> None:
    client = sink._client_for(None)
    for name, method in methods.items():
        setattr(client, name, method)


def test_wire_13_replace_edit_updates_generic_blocks_not_an_approval_card(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sink = SlackReplyAdapter("xoxb-test")
    calls: list[dict[str, Any]] = []

    async def chat_update(**kwargs: Any) -> None:
        calls.append(kwargs)

    _install(sink, chat_update=chat_update)

    def _forbid(called: str):
        def render(*_args: object, **_kwargs: object) -> None:
            raise AssertionError(called)

        return render

    for name in ("approval_card", "expired_approval_card", "resolved_approval_card"):
        monkeypatch.setattr(slack_sink_module, name, _forbid(name))

    ack = asyncio.run(sink.emit(_replace(), route=ReplyDestination()))

    assert ack.ref == "1720000000.000400"
    assert len(calls) == 1
    assert calls[0]["channel"] == "C0EXAMPLE1"
    assert calls[0]["ts"] == "1720000000.000400"
    assert calls[0]["text"] == "replacement body"
    blocks = calls[0]["blocks"]
    assert isinstance(blocks, list)
    assert any(
        block.get("type") == "header" and "Replacement heading" in str(block) for block in blocks
    )


def test_wire_13_audience_post_is_ephemeral_for_one_user() -> None:
    sink = SlackReplyAdapter("xoxb-test")
    updates: list[dict[str, Any]] = []
    posts: list[dict[str, Any]] = []
    ephemeral: list[dict[str, Any]] = []

    async def chat_update(**kwargs: Any) -> None:
        updates.append(kwargs)

    async def chat_postMessage(**kwargs: Any) -> None:
        posts.append(kwargs)

    async def chat_postEphemeral(**kwargs: Any) -> dict[str, str]:
        ephemeral.append(kwargs)
        return {"ok": "true", "message_ts": "1720000000.000900"}

    _install(
        sink,
        chat_update=chat_update,
        chat_postMessage=chat_postMessage,
        chat_postEphemeral=chat_postEphemeral,
    )

    ack = asyncio.run(sink.emit(_audience(), route=ReplyDestination()))

    assert ack.ref == "1720000000.000900"
    assert updates == []
    assert posts == []
    assert len(ephemeral) == 1
    assert ephemeral[0]["user"] == "U0EXAMPLE1"
    assert ephemeral[0]["thread_ts"] == "1720000000.000100"
    assert ephemeral[0]["text"] == "only you can see this"


@pytest.mark.parametrize("verb", ["chat_update", "chat_postEphemeral"])
def test_wire_13_block_rejection_retries_text_only(verb: str) -> None:
    sink = SlackReplyAdapter("xoxb-test")
    calls: list[dict[str, Any]] = []

    async def rejected(**kwargs: Any) -> dict[str, str]:
        calls.append(kwargs)
        if "blocks" in kwargs:
            raise SlackApiError("invalid_blocks", {"ok": False, "error": "invalid_blocks"})
        return {"ok": "true", "message_ts": "1720000000.000900"}

    _install(sink, **{verb: rejected})
    event = _replace() if verb == "chat_update" else _audience()
    ack = asyncio.run(sink.emit(event, route=ReplyDestination()))

    assert len(calls) == 2
    assert "blocks" in calls[0]
    assert "blocks" not in calls[1]
    assert calls[1]["text"] == event.message.text
    if verb == "chat_update":
        assert ack.ref == "1720000000.000400"
    else:
        assert ack.ref == "1720000000.000900"
