"""The worker's thread key, as the API reads it back (ADR-0168 decision 4).

A named non-Slack route's key gained an identity segment. Rows it wrote before
that stay under its pre-identity key, and a reader may look there on a miss,
but only for the agent's one binding on that pair: before the route triple a
pair held one binding (migration 0023), so the old key can only be that
binding's. A named Slack identity did not exist before the ADR and has no old
form.
"""

from __future__ import annotations

import uuid

from aci_protocol.turn import SLACK_KIND
from channel_protocol import parse_scoped_conversation_id, scoped_conversation_id
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from .models import AgentChannel


def pre_identity_key_of(thread_key: str) -> str | None:
    """The key ``thread_key``'s route had before decision 4, or None if it did not change."""

    parsed = parse_scoped_conversation_id(thread_key)
    if parsed is None or parsed.identity is None or parsed.kind == SLACK_KIND:
        return None
    return scoped_conversation_id(parsed.kind, parsed.address, parsed.conversation_id)


async def pre_identity_thread_key_for(
    session: AsyncSession, agent_id: uuid.UUID, thread_key: str
) -> str | None:
    """``pre_identity_key_of``, when that key can only be this agent's route's."""

    old = pre_identity_key_of(thread_key)
    parsed = parse_scoped_conversation_id(thread_key)
    if old is None or parsed is None:
        return None
    adapters = list(
        await session.scalars(
            select(AgentChannel.adapter).where(
                AgentChannel.agent_id == agent_id,
                AgentChannel.kind == parsed.kind,
                AgentChannel.address == parsed.address,
            )
        )
    )
    return old if adapters == [parsed.identity] else None
