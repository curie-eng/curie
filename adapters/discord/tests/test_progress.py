"""Deliberate progress on Discord (reply wire 1.1, ADR-0130).

Discord renders progress as its plain-text fallback. A post that carries a
``delivery_id`` is remembered with the message it created, so a redelivery
adopts that message instead of posting again, and a card edit goes to the card
and never through the answer path. Bodies come from the committed reply-wire
corpus where it has one, re-addressed to this adapter's kind.
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any

import pytest
from channel_protocol import ReplyEvent, ReplyUpdate
from channel_protocol.progress import progress_text
from curie_discord_adapter.egress import DiscordReplyService
from curie_discord_adapter.http import create_reply_app
from curie_discord_adapter.state import DiscordState
from fastapi.testclient import TestClient
from pydantic import TypeAdapter

_CORPUS = (
    Path(__file__).resolve().parents[3]
    / "packages"
    / "channel-protocol"
    / "schema"
    / "reply-wire.corpus.json"
)
_EVENT: TypeAdapter[ReplyEvent] = TypeAdapter(ReplyEvent)


class FakeDiscord:
    def __init__(self) -> None:
        self.edits: list[tuple[str, str, str]] = []
        self.posts: list[tuple[str, str]] = []
        self.deletes: list[tuple[str, str]] = []

    async def edit_message(self, channel_id: str, message_id: str, text: str) -> None:
        self.edits.append((channel_id, message_id, text))

    async def post_message(self, channel_id: str, text: str) -> str:
        self.posts.append((channel_id, text))
        return f"posted-{len(self.posts)}"

    async def delete_message(self, channel_id: str, message_id: str) -> None:
        self.deletes.append((channel_id, message_id))


def _corpus_body(name: str, **target: Any) -> dict[str, Any]:
    corpus = json.loads(_CORPUS.read_text(encoding="utf-8"))
    (entry,) = [item for item in corpus["v1_1"] if item["name"] == name]
    body: dict[str, Any] = json.loads(json.dumps(entry["body"]))
    body["target"] = {
        "kind": "discord",
        "address": "111",
        "conversation_id": "222",
        "reply_ref": "9002",
        **target,
    }
    return body


def _event(name: str, **target: Any) -> ReplyEvent:
    return _EVENT.validate_python(_corpus_body(name, **target))


def _service(tmp_path: Path) -> tuple[FakeDiscord, DiscordState, DiscordReplyService]:
    port = FakeDiscord()
    state = DiscordState(tmp_path / "state.sqlite3")
    return port, state, DiscordReplyService(port, state)


def test_a_progress_card_post_is_its_text_fallback(tmp_path: Path) -> None:
    """@spec ADR-0130 d2: a channel with nothing richer shows the card as text."""

    port, _state, service = _service(tmp_path)
    event = _event("reply-post-progress-card-first-revision")

    ack = asyncio.run(service.deliver(event))

    assert ack.ref == "posted-1"
    assert event.progress is not None
    assert port.posts == [("222", progress_text(event.progress))]
    assert port.edits == [] and port.deletes == []


def test_a_redelivered_progress_post_adopts_the_first_message(tmp_path: Path) -> None:
    """@spec ADR-0130 d4: delivery_id is the post's idempotency key."""

    port, _state, service = _service(tmp_path)
    event = _event("reply-post-progress-milestone-evidence")

    first = asyncio.run(service.deliver(event))
    again = asyncio.run(service.deliver(event))

    assert first.ref == again.ref == "posted-1"
    assert len(port.posts) == 1


def test_the_first_message_is_adopted_after_a_restart(tmp_path: Path) -> None:
    port, state, service = _service(tmp_path)
    event = _event("reply-post-progress-milestone-scope")
    first = asyncio.run(service.deliver(event))
    state.close()

    reopened = DiscordState(tmp_path / "state.sqlite3")
    again = asyncio.run(DiscordReplyService(port, reopened).deliver(event))

    assert again.ref == first.ref
    assert len(port.posts) == 1
    reopened.close()


def test_a_fresh_delivery_id_is_a_new_post(tmp_path: Path) -> None:
    """The negative: remembering keys must not merge distinct milestones."""

    port, _state, service = _service(tmp_path)

    first = asyncio.run(service.deliver(_event("reply-post-progress-milestone-evidence")))
    second = asyncio.run(service.deliver(_event("reply-post-progress-milestone-scope")))

    assert first.ref != second.ref
    assert len(port.posts) == 2


def test_a_card_edit_goes_to_the_card_and_never_the_answer_path(tmp_path: Path) -> None:
    """@spec ADR-0130 d5: a card edit carries no answer text and must not be one.

    Through the answer path it would have been an empty edit, and it would have
    deleted the continuation messages of whatever it named.
    """

    port, state, service = _service(tmp_path)
    state.replace_continuations("222", "card-1", ["answer-continuation"])
    event = _event("reply-update-progress-card-revision", reply_ref="card-1")

    ack = asyncio.run(service.deliver(event))

    assert ack.ref == "card-1"
    assert isinstance(event, ReplyUpdate) and event.progress is not None
    assert port.edits == [("222", "card-1", progress_text(event.progress))]
    assert port.posts == [] and port.deletes == []
    assert state.continuations("222", "card-1") == ["answer-continuation"]


def test_a_terminal_card_edit_reads_closed(tmp_path: Path) -> None:
    port, _state, service = _service(tmp_path)

    asyncio.run(
        service.deliver(_event("reply-update-progress-card-terminal", reply_ref="card-1"))
    )

    ((_, _, text),) = port.edits
    assert text.startswith("Task complete.")


def test_a_card_edit_without_a_card_ref_posts_nothing(tmp_path: Path) -> None:
    port, _state, service = _service(tmp_path)

    with pytest.raises(ValueError, match="card"):
        asyncio.run(
            service.deliver(_event("reply-update-progress-card-revision", reply_ref=None))
        )

    assert port.posts == [] and port.edits == []


def test_a_redelivered_placeholderless_answer_edits_its_first_message(tmp_path: Path) -> None:
    """A 1.1 answer post is a create too, so its redelivery edits instead of posting."""

    port, _state, service = _service(tmp_path)
    event = _event("reply-update-text-with-delivery-id", reply_ref=None)

    first = asyncio.run(service.deliver(event))
    again = asyncio.run(service.deliver(event))

    assert first.ref == again.ref == "posted-1"
    assert port.posts == [("222", "The answer is 42.")]
    assert port.edits == [("222", "posted-1", "The answer is 42.")]


class _ServiceInAppThread:
    """Opens the SQLite state on the thread that serves requests.

    Production opens it inside the event loop that also serves the reply app
    (``main.run``); the test client serves from its own portal thread, and
    sqlite3 refuses a connection used across threads.
    """

    def __init__(self, port: FakeDiscord, path: Path) -> None:
        self._port = port
        self._path = path
        self.state: DiscordState | None = None
        self._service: DiscordReplyService | None = None

    async def deliver(self, event: ReplyEvent) -> Any:
        if self._service is None:
            self.state = DiscordState(self._path)
            self._service = DiscordReplyService(self._port, self.state)
        return await self._service.deliver(event)


def test_the_reply_endpoint_accepts_a_progress_body_and_its_redelivery(tmp_path: Path) -> None:
    """The HTTP layer decodes 1.1 and acks the ref the post created, twice."""

    port = FakeDiscord()
    service = _ServiceInAppThread(port, tmp_path / "state.sqlite3")
    body = _corpus_body("reply-post-progress-card-first-revision")

    with TestClient(create_reply_app(service, "reply-secret")) as client:
        answers = [
            client.post(
                "/replies", json=body, headers={"X-Curie-Adapter-Secret": "reply-secret"}
            )
            for _ in range(2)
        ]

    assert [answer.status_code for answer in answers] == [200, 200], answers[0].text
    assert [answer.json() for answer in answers] == [{"ref": "posted-1"}] * 2
    assert len(port.posts) == 1
