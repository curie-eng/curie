"""The worker's thread key, as the API reads it back (ADR-0168 decision 4).

A named non-Slack route's key gained an identity segment. Rows it wrote before
that stay under its pre-identity key, and a reader may look there on a miss,
but only for the agent's one binding on that pair: before the route triple a
pair held one binding (migration 0023), so the old key can only be that
binding's.

Every Slack route is excluded from that lookup here, including the pre-ADR
custom-transport binding (an endpoint plus a credential in ``adapter``),
which decision 3 already keys by that same adapter, the same as any other
named identity. That binding's key also gains a segment under decision 4, so
it does have an old, pre-identity key -- this module simply never looks for
it, and that binding starts a fresh thread on the next turn.
"""

from __future__ import annotations

import uuid

from aci_protocol.turn import SLACK_KIND, route_identity
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


def route_thread_key(
    kind: str, adapter: str | None, address: str, conversation_id: str
) -> str:
    """The worker's thread key for this route and conversation (``kernel._thread_key_for``)."""

    return scoped_conversation_id(
        kind, address, conversation_id, identity=route_identity(kind, adapter)
    )


def pre_identity_thread_key(
    kind: str, adapter: str | None, address: str, conversation_id: str
) -> str | None:
    """The key this route had before decision 4, or None when it did not change."""

    if kind == SLACK_KIND:
        return None
    old = scoped_conversation_id(kind, address, conversation_id)
    return None if old == route_thread_key(kind, adapter, address, conversation_id) else old


def route_thread_key_matches(
    kind: str, adapter: str | None, address: str, conversation_id: str, stored: str
) -> bool:
    """Whether ``stored`` is this route's thread key, in its current or pre-identity form."""

    return stored in (
        route_thread_key(kind, adapter, address, conversation_id),
        pre_identity_thread_key(kind, adapter, address, conversation_id),
    )
