"""Neutral Curie reply events rendered onto Discord messages."""

from typing import Protocol

from channel_protocol import (
    ReplyAck,
    ReplyEvent,
    ReplyPost,
    ReplyUpdate,
    TurnCompleted,
    TurnStatus,
    progress_text,
)

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
            # A 1.1 post is keyed by its delivery_id (ADR-0130 d4): a redelivery
            # answers with the message the first attempt created.
            if event.delivery_id is not None:
                known = self._state.posted_message(event.delivery_id)
                if known is not None:
                    return ReplyAck(ref=known)
            text = (
                progress_text(event.progress)
                if event.progress is not None
                else event.message.text
            )
            ref = await self._discord.post_message(channel_id, text)
            if event.delivery_id is not None:
                self._state.remember_post(event.delivery_id, ref)
            return ReplyAck(ref=ref)
        if not isinstance(event, ReplyUpdate):
            raise TypeError(f"unsupported reply event {type(event).__name__}")
        if event.progress is not None:
            # A card edit (ADR-0130 d5) carries no answer text. It edits the card
            # and nothing else, so it never reaches the continuation logic below.
            if event.target.reply_ref is None:
                raise ValueError(
                    "reply.update carrying progress needs the progress card's reply_ref"
                )
            await self._discord.edit_message(
                channel_id, event.target.reply_ref, progress_text(event.progress)
            )
            return ReplyAck(ref=event.target.reply_ref)
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
        if reply_ref is None and event.delivery_id is not None:
            # A placeholderless answer redelivered under its delivery_id edits
            # the message its first attempt posted instead of posting another.
            reply_ref = self._state.posted_message(event.delivery_id)
        if reply_ref is None:
            ref = await self._discord.post_message(channel_id, chunks[0])
            if event.delivery_id is not None:
                self._state.remember_post(event.delivery_id, ref)
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
