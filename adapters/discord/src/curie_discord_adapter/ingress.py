"""Pure Discord message to Curie turn translation."""

import re
from dataclasses import dataclass


@dataclass(frozen=True)
class DiscordBinding:
    parent_channel_id: str
    address: str
    token: str


@dataclass(frozen=True)
class DiscordMessage:
    id: str
    channel_id: str
    thread_id: str
    author_id: str
    author_name: str
    content: str
    mentioned_user_ids: frozenset[str]


def build_turn(
    message: DiscordMessage,
    *,
    bot_user_id: str,
    binding: DiscordBinding,
    reply_ref: str,
    require_mention: bool = True,
) -> dict[str, str] | None:
    """Build the exact `/channels/turns` body for one Discord delivery."""

    if require_mention and bot_user_id not in message.mentioned_user_ids:
        return None
    text = re.sub(rf"<@!?{re.escape(bot_user_id)}>", "", message.content).strip()
    if not text:
        return None
    return {
        "kind": "discord",
        "address": binding.address,
        "delivery_id": message.id,
        "conversation_id": message.thread_id,
        "author": f"{message.author_name} ({message.author_id})",
        "text": text,
        "reply_ref": reply_ref,
    }
