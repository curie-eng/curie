"""@spec PROTECTED-HOOK-SOURCE-2/4/10."""

from __future__ import annotations

import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass

from curie_protected_hooks.source_policy_sql import (
    SourceAgentNotFound,
    SourceGate,
    SourceGateContext,
    SourceGateInvalid,
    SourceSnapshotUnavailable,
    ensure_source_gate_live,
    read_source_snapshot,
)
from fastapi import HTTPException, Request
from sqlalchemy import and_, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from . import hook_signing, hook_source_signing
from .config import get_settings
from .models import Agent, HookSourcePolicy

AUTH_DETAIL = "missing or invalid signature"
_UNAVAILABLE = "authority_unavailable"


@dataclass(frozen=True)
class AuthenticatedHookSource:
    """@spec PROTECTED-HOOK-SOURCE-2."""

    agent: Agent
    _gate: SourceGateContext

    async def ensure_live(self) -> None:
        """@spec PROTECTED-HOOK-SOURCE-2."""
        try:
            await ensure_source_gate_live(self._gate)
        except (SourceGateInvalid, SourceSnapshotUnavailable):
            raise HTTPException(503, _UNAVAILABLE) from None


async def _current_key(session: AsyncSession, agent_id: uuid.UUID, hook: str) -> str:
    """@spec PROTECTED-HOOK-SOURCE-2/4."""
    row = (
        await session.execute(
            select(Agent.hook_generation, HookSourcePolicy.mode, HookSourcePolicy.generation)
            .outerjoin(
                HookSourcePolicy,
                and_(HookSourcePolicy.agent_id == Agent.id, HookSourcePolicy.hook == hook),
            )
            .where(Agent.id == agent_id)
        )
    ).one_or_none()
    if row is None:
        raise HTTPException(401, AUTH_DETAIL)
    legacy, mode, generation = row
    if type(legacy) is not int or not 0 <= legacy <= 2**31 - 1:
        raise SourceSnapshotUnavailable("invalid_source_state")
    if mode is None:
        if generation is not None:
            raise SourceSnapshotUnavailable("invalid_source_state")
    elif mode not in ("ordinary", "protected") or (
        type(generation) is not int or not 1 <= generation <= 2**63 - 1
    ):
        raise SourceSnapshotUnavailable("invalid_source_state")
    api_key = get_settings().api_key
    if mode == "protected":
        return hook_source_signing.derive(
            api_key, agent_id=str(agent_id), hook=hook, generation=generation
        )
    return hook_signing.derive(api_key, agent_id=str(agent_id), generation=legacy)


def _authenticate(
    key: str,
    *,
    hook: str,
    raw: bytes,
    tool_access: str | None,
    timestamp: str | None,
    delivery_id: str,
    signature: str | None,
) -> None:
    """@spec PROTECTED-HOOK-SOURCE-2/4."""
    if signature is not None and not signature.isascii():
        raise HTTPException(401, AUTH_DETAIL)
    if not hook_signing.verify(
        key,
        timestamp=timestamp,
        delivery_id=delivery_id,
        hook=hook,
        tool_access=tool_access,
        body=raw,
        header=signature,
    ):
        raise HTTPException(401, AUTH_DETAIL)


@asynccontextmanager
async def authenticated_source(
    request: Request,
    session: AsyncSession,
    *,
    agent_id: uuid.UUID,
    hook: str,
    raw: bytes,
    tool_access: str | None,
    timestamp: str | None,
    delivery_id: str,
    signature: str | None,
) -> AsyncIterator[AuthenticatedHookSource]:
    """@spec PROTECTED-HOOK-SOURCE-2/4/10."""
    try:
        preliminary = await _current_key(session, agent_id, hook)
        _authenticate(
            preliminary,
            hook=hook,
            raw=raw,
            tool_access=tool_access,
            timestamp=timestamp,
            delivery_id=delivery_id,
            signature=signature,
        )
        del preliminary
        await session.rollback()
        gate = getattr(request.app.state, "source_gate", None)
        if not isinstance(gate, SourceGate):
            raise SourceSnapshotUnavailable("source_gate_unavailable")
        async with gate.hold(agent_id) as held:
            current = await _current_key(session, agent_id, hook)
            _authenticate(
                current,
                hook=hook,
                raw=raw,
                tool_access=tool_access,
                timestamp=timestamp,
                delivery_id=delivery_id,
                signature=signature,
            )
            del current
            snapshot = await read_source_snapshot(held, await session.connection(), hook)
            if not snapshot.never_configured:
                raise HTTPException(503, snapshot.refusal_reason or _UNAVAILABLE)
            agent: Agent | None = await session.scalar(
                select(Agent)
                .where(Agent.id == agent_id)
                .options(selectinload(Agent.channels))
                .execution_options(populate_existing=True)
            )
            if agent is None:
                raise HTTPException(401, AUTH_DETAIL)
            yield AuthenticatedHookSource(agent, held)
    except (SourceAgentNotFound, SourceGateInvalid, SourceSnapshotUnavailable, SQLAlchemyError):
        raise HTTPException(503, _UNAVAILABLE) from None
