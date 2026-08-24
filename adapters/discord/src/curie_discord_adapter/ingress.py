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
    """Build the exact `/channels/turns` body for one Discord delivery.

    A mention with nothing else -- ``text`` empty after stripping it -- still
    becomes a turn, carrying empty text. Whether "nothing to say" is itself a
    meaningful instruction is the AGENT's call, not this adapter's: a
    stack-shaped agent (squawk) treats it as "pop," and Slack's dispatcher
    already forwards the same empty text unconditionally (`handlers.py`'s
    `_strip_self_mention`) for exactly this reason. Refusing to build a turn
    here would silently drop the delivery before it ever reaches Curie -- no
    thread, no turn, nothing an operator could see -- which is a stronger,
    and inconsistent, claim than Slack's ingress makes about the same
    mention-only message.
    """

    if require_mention and bot_user_id not in message.mentioned_user_ids:
        return None
    text = re.sub(rf"<@!?{re.escape(bot_user_id)}>", "", message.content).strip()
    return {
        "kind": "discord",
        "address": binding.address,
        "delivery_id": message.id,
        "conversation_id": message.thread_id,
        "author": f"{message.author_name} ({message.author_id})",
        "text": text,
        "reply_ref": reply_ref,
    }
