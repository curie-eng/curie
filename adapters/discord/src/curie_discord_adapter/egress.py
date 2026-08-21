"""Neutral Curie reply events rendered onto Discord messages."""

from typing import Protocol

from channel_protocol import ReplyAck, ReplyEvent, ReplyPost, ReplyUpdate, TurnCompleted, TurnStatus

from .state import DiscordState

DISCORD_TEXT_LIMIT = 2000


def split_discord_text(text: str) -> list[str]:
    """Split on Python Unicode code points without dropping content."""

    if not text:
        return ["\u200b"]
    return [
        text[offset : offset + DISCORD_TEXT_LIMIT]
        for offset in range(0, len(text), DISCORD_TEXT_LIMIT)
    ]


class DiscordPort(Protocol):
    async def edit_message(self, channel_id: str, message_id: str, text: str) -> None: ...

    async def post_message(self, channel_id: str, text: str) -> str: ...

    async def delete_message(self, channel_id: str, message_id: str) -> None: ...


class DiscordReplyService:
    def __init__(self, discord: DiscordPort, state: DiscordState) -> None:
        self._discord = discord
        self._state = state

    async def deliver(self, event: ReplyEvent) -> ReplyAck:
        if event.target.kind != "discord":
            raise ValueError(f"Discord adapter cannot render kind {event.target.kind!r}")
        if isinstance(event, TurnStatus):
            return ReplyAck(ref=None)
        if isinstance(event, TurnCompleted):
            self._state.mark_completed(event.event_id)
            return ReplyAck(ref=None)
        channel_id = event.target.conversation_id or event.target.address
        if isinstance(event, ReplyPost):
            ref = await self._discord.post_message(channel_id, event.message.text)
            return ReplyAck(ref=ref)
        if not isinstance(event, ReplyUpdate):
            raise TypeError(f"unsupported reply event {type(event).__name__}")
        text = (
            event.text
            if event.text is not None
            else (event.message.text if event.message else "")
        )
        if event.settled is not None:
            if event.settled.decision is None:
                text = f"{text}\n\nApproval expired."
            else:
                resolver = event.settled.resolver or "an authorized operator"
                text = f"{text}\n\n{event.settled.decision.title()} by {resolver}."
        chunks = split_discord_text(text)
        reply_ref = event.target.reply_ref
        if reply_ref is None:
            ref = await self._discord.post_message(channel_id, chunks[0])
            reply_ref = ref
        else:
            await self._discord.edit_message(channel_id, reply_ref, chunks[0])
        existing = self._state.continuations(channel_id, reply_ref)
        continuation_ids: list[str] = []
        for index, chunk in enumerate(chunks[1:]):
            if index < len(existing):
                message_id = existing[index]
                await self._discord.edit_message(channel_id, message_id, chunk)
            else:
                message_id = await self._discord.post_message(channel_id, chunk)
            continuation_ids.append(message_id)
        for stale_id in existing[len(chunks) - 1 :]:
            await self._discord.delete_message(channel_id, stale_id)
        self._state.replace_continuations(channel_id, reply_ref, continuation_ids)
        return ReplyAck(ref=reply_ref)
