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


async def thread_key_forms(
    session: AsyncSession, agent_id: uuid.UUID, thread_key: str
) -> tuple[str, ...]:
    """``thread_key``, then its pre-identity form when that can only be this agent's route's."""

    old = await pre_identity_thread_key_for(session, agent_id, thread_key)
    return (thread_key,) if old is None else (thread_key, old)


def fence_key_forms(thread_key: str) -> tuple[str, ...]:
    """``thread_key``, then its pre-identity form -- unguarded.

    Only safe for a REFUSAL already scoped to one agent: `pre_identity_key_of`
    can only be this key's own kind/address pair, so a false match would have
    to also land inside that one agent's rows, and over-matching there is the
    safe direction for a refusal (`publication_cancellation_conflict`,
    `_refuse_fenced_work_item`). `thread_key_forms`'s single-binding guard
    exists for a WRITE (`_bind_running_work_item_lineage`) or an agent-blind
    reader (`running_for_conversation`), where over-matching would not be
    safe, and it fails open exactly where this must not: once the binding
    that proved the old key is gone, a cancelled legacy work item would stop
    fencing credential redemption.
    """

    old = pre_identity_key_of(thread_key)
    return (thread_key,) if old is None else (thread_key, old)


def route_adapter_of(thread_key: str) -> str | None:
    """The ``adapter`` a wake's ``ReplyHandle`` needs to reproduce this key's route.

    ``route_thread_key`` folds ``adapter`` into the key through
    ``route_identity``; this is its inverse for a key already minted. A
    default Slack route's key carries no identity segment, so this returns
    None for it -- the same value an unnamed route already used, and
    ``route_identity`` maps both back to the same default app.

    None also comes back for a pre-identity key: one minted before decision 4
    never carries a segment at all, so this cannot tell "default" from "no
    segment was ever added". A legacy non-Slack work item's CURRENT execute
    wake still resolves its live binding at dispatch time (`_admission_refusal`,
    `load_execute_wake`), so a caller keying that same thread for a legacy
    row -- the terminate wake -- cannot stop here; see
    `legacy_route_adapter_of`.
    """

    parsed = parse_scoped_conversation_id(thread_key)
    return None if parsed is None else parsed.identity


async def legacy_route_adapter_of(
    session: AsyncSession, agent_id: uuid.UUID, thread_key: str
) -> str | None:
    """``route_adapter_of``, falling back to the agent's one binding for a
    pre-identity non-Slack key.

    `WorkItem.conversation_id` is immutable once written (migration 0046), so
    a work item admitted before decision 4 is stuck under its old, unnamed
    key forever, even once its route gains a named adapter under decision 3
    and every wake keyed from a LIVE binding lookup starts carrying it
    (`_admission_refusal`'s resolved binding, threaded through admission and
    replay). The single-binding condition is `pre_identity_thread_key_for`'s
    own guard: before the route triple a pair held one binding, so this is
    the only adapter the pair's binding could be.
    """

    decoded = route_adapter_of(thread_key)
    if decoded is not None:
        return decoded
    parsed = parse_scoped_conversation_id(thread_key)
    if parsed is None or parsed.kind == SLACK_KIND:
        return decoded
    adapters = list(
        await session.scalars(
            select(AgentChannel.adapter).where(
                AgentChannel.agent_id == agent_id,
                AgentChannel.kind == parsed.kind,
                AgentChannel.address == parsed.address,
            )
        )
    )
    return adapters[0] if len(adapters) == 1 else None


def legacy_producer_thread_key(kind: str, address: str, conversation_id: str) -> str:
    """The workspace key a pre-#2274 producer always wrote.

    A publication request that omits ``reply_conversation_id`` predates that
    field (added 2026-09-04): its ``conversation_id`` is the bare, unscoped id
    the worker minted before decision 4 gave the worker's own key an identity
    segment, so every route -- named or not -- wrote this same unidentified
    form. Building the CURRENT identity key here instead would hand such a
    request's publication a key it never wrote, and the lookup would miss.
    """

    return scoped_conversation_id(kind, address, conversation_id)
