import asyncio
from pathlib import Path

from channel_protocol import (
    OutboundMessage,
    ReplyPost,
    ReplyTarget,
    ReplyUpdate,
    SettledOutcome,
    TurnCompleted,
    TurnStatus,
)
from curie_discord_adapter.egress import DiscordReplyService, split_discord_text
from curie_discord_adapter.state import DiscordState


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


def target() -> ReplyTarget:
    return ReplyTarget(
        kind="discord",
        address="111",
        conversation_id="222",
        reply_ref="9002",
    )


def test_text_chunks_are_unicode_safe_and_within_discord_limit() -> None:
    text = "x" * 1999 + "🙂" + "y" * 2001
    chunks = split_discord_text(text)
    assert "".join(chunks) == text
    assert all(0 < len(chunk) <= 2000 for chunk in chunks)


def test_reply_update_edits_placeholder_and_posts_continuations(tmp_path: Path) -> None:
    port = FakeDiscord()
    state = DiscordState(tmp_path / "state.sqlite3")
    service = DiscordReplyService(port, state)
    event = ReplyUpdate(version="1.0", event="reply.update", target=target(), text="a" * 2001)

    ack = asyncio.run(service.deliver(event))

    assert ack.ref == "9002"
    assert port.edits == [("222", "9002", "a" * 2000)]
    assert port.posts == [("222", "a")]
    assert state.continuations("222", "9002") == ["posted-1"]

    shorter = ReplyUpdate(version="1.0", event="reply.update", target=target(), text="done")
    asyncio.run(service.deliver(shorter))
    assert port.deletes == [("222", "posted-1")]
    assert state.continuations("222", "9002") == []


def test_settled_approval_uses_text_fallback_and_terminal_outcome(tmp_path: Path) -> None:
    port = FakeDiscord()
    state = DiscordState(tmp_path / "state.sqlite3")
    service = DiscordReplyService(port, state)
    event = ReplyUpdate(
        version="1.0",
        event="reply.update",
        target=target(),
        message=OutboundMessage(version="1.0", text="Deploy production?"),
        settled=SettledOutcome(
            requested_by="Ada",
            decision="approved",
            resolver="Grace",
        ),
    )

    asyncio.run(service.deliver(event))

    assert port.edits == [("222", "9002", "Deploy production?\n\nApproved by Grace.")]


def test_status_is_noop_and_post_uses_text_fallback(tmp_path: Path) -> None:
    port = FakeDiscord()
    state = DiscordState(tmp_path / "state.sqlite3")
    service = DiscordReplyService(port, state)

    status = asyncio.run(service.deliver(
        TurnStatus(version="1.0", event="turn.status", target=target(), status="working")
    ))
    posted = asyncio.run(service.deliver(
        ReplyPost(
            version="1.0",
            event="reply.post",
            target=target(),
            message=OutboundMessage(version="1.0", text="approve in the Curie CLI"),
            requested_by="Ada",
        )
    ))

    assert status.ref is None
    assert posted.ref == "posted-1"
    assert port.posts == [("222", "approve in the Curie CLI")]


def test_completion_is_deduped_durably(tmp_path: Path) -> None:
    port = FakeDiscord()
    state = DiscordState(tmp_path / "state.sqlite3")
    service = DiscordReplyService(port, state)
    event = TurnCompleted(
        version="1.0",
        event="turn.completed",
        target=target(),
        event_id="done-1",
        outcome="delivered",
    )

    assert asyncio.run(service.deliver(event)).ref is None
    assert asyncio.run(service.deliver(event)).ref is None
    assert state.completed_count() == 1
