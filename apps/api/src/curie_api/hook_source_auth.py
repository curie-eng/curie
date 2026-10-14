"""Signed hook authentication under the agent source gate.

@spec PROTECTED-HOOK-SOURCE-2/4/9/10.

Ingress yields the gate-held snapshot for never configured, tombstoned and
protected rows; pending history stays closed here. When the ungated
authentication found a protected row, signed delivery ingress waits for the
gate at most ``PROTECTED_GATE_WAIT_SECONDS`` and answers 503
``authority_unavailable`` past it; every other signed delivery keeps the
unbounded wait. The gate-held reload still decides the path.
"""

from __future__ import annotations

import uuid
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass

from curie_protected_hooks.source_policy_sql import (
    SourceAgentNotFound,
    SourceGate,
    SourceGateContext,
    SourceGateInvalid,
    SourceSnapshot,
    SourceSnapshotUnavailable,
    ensure_source_gate_live,
    read_source_snapshot,
)
from fastapi import HTTPException, Request, status
from sqlalchemy import and_, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from . import hook_signing, hook_source_signing
from .config import get_settings
from .models import Agent, HookSourcePolicy

AUTH_DETAIL = "missing or invalid signature"
MISSING_DELIVERY_DETAIL = (
    f"{hook_signing.DELIVERY_HEADER} is required: this ingress is at-least-once, so a "
    "stable upstream id is what keeps a retried delivery from running the "
    "agent twice"
)
_UNAVAILABLE = "authority_unavailable"
PROTECTED_GATE_WAIT_SECONDS = 5.0
"""Protected ingress gives up on the agent gate after this long, @spec PROTECTED-HOOK-SOURCE-2."""


@dataclass(frozen=True)
class AuthenticatedHookSource:
    """@spec PROTECTED-HOOK-SOURCE-2."""

    agent: Agent
    snapshot: SourceSnapshot
    _gate: SourceGateContext

    async def ensure_live(self) -> None:
        """@spec PROTECTED-HOOK-SOURCE-2."""
        try:
            await ensure_source_gate_live(self._gate)
        except (SourceGateInvalid, SourceSnapshotUnavailable):
            raise HTTPException(503, _UNAVAILABLE) from None


async def _current_key(
    session: AsyncSession, agent_id: uuid.UUID, hook: str
) -> tuple[str, str | None]:
    """The current key and the row mode it was selected by, @spec PROTECTED-HOOK-SOURCE-2/4."""
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
        return (
            hook_source_signing.derive(
                api_key, agent_id=str(agent_id), hook=hook, generation=generation
            ),
            mode,
        )
    return hook_signing.derive(api_key, agent_id=str(agent_id), generation=legacy), mode


@dataclass(frozen=True)
class _SignedRequest:
    """One request's signed context and its purpose verifier.

    @spec PROTECTED-HOOK-SOURCE-2/4/9.
    """

    verify: Callable[..., bool]
    hook: str
    raw: bytes
    tool_access: str | None
    timestamp: str | None
    delivery_id: str
    signature: str | None


def _authenticate(key: str, signed: _SignedRequest) -> None:
    """@spec PROTECTED-HOOK-SOURCE-2/4/9."""
    if signed.signature is not None and not signed.signature.isascii():
        raise HTTPException(401, AUTH_DETAIL)
    if not signed.verify(
        key,
        timestamp=signed.timestamp,
        delivery_id=signed.delivery_id,
        hook=signed.hook,
        tool_access=signed.tool_access,
        body=signed.raw,
        header=signed.signature,
    ):
        raise HTTPException(401, AUTH_DETAIL)


async def _preauthenticate(
    session: AsyncSession, agent_id: uuid.UUID, signed: _SignedRequest
) -> str | None:
    """Ungated check, never admission authority; returns the row mode it saw.

    @spec PROTECTED-HOOK-SOURCE-2/9.
    """
    preliminary, mode = await _current_key(session, agent_id, signed.hook)
    _authenticate(preliminary, signed)
    del preliminary
    return mode


@asynccontextmanager
async def _gated_snapshot(
    request: Request,
    session: AsyncSession,
    agent_id: uuid.UUID,
    signed: _SignedRequest,
    wait_seconds: float | None = None,
) -> AsyncIterator[tuple[SourceGateContext, SourceSnapshot]]:
    """Gate-held reload and reauthentication before the snapshot read.

    @spec PROTECTED-HOOK-SOURCE-2/4/9.
    """
    await session.rollback()
    gate = getattr(request.app.state, "source_gate", None)
    if not isinstance(gate, SourceGate):
        raise SourceSnapshotUnavailable("source_gate_unavailable")
    async with gate.hold(agent_id, wait_seconds=wait_seconds) as held:
        current, _mode = await _current_key(session, agent_id, signed.hook)
        _authenticate(current, signed)
        del current
        yield held, await read_source_snapshot(held, await session.connection(), signed.hook)


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
    """Yield the agent and the gate-held snapshot of a deliverable source state.

    Pending history is refused with its reason; never configured, tombstoned
    and protected rows are yielded with the gate held, and the caller decides
    their path. @spec PROTECTED-HOOK-SOURCE-2/4/10.
    """
    signed = _SignedRequest(
        hook_signing.verify, hook, raw, tool_access, timestamp, delivery_id, signature
    )
    try:
        mode = await _preauthenticate(session, agent_id, signed)
        wait = PROTECTED_GATE_WAIT_SECONDS if mode == "protected" else None
        async with _gated_snapshot(request, session, agent_id, signed, wait) as (
            held,
            snapshot,
        ):
            if snapshot.policy is None and not snapshot.never_configured:
                raise HTTPException(503, snapshot.refusal_reason or _UNAVAILABLE)
            if snapshot.policy is not None and snapshot.policy.mode not in (
                "ordinary",
                "protected",
            ):
                raise HTTPException(503, _UNAVAILABLE)
            agent: Agent | None = await session.scalar(
                select(Agent)
                .where(Agent.id == agent_id)
                .options(selectinload(Agent.channels))
                .execution_options(populate_existing=True)
            )
            if agent is None:
                raise HTTPException(401, AUTH_DETAIL)
            yield AuthenticatedHookSource(agent, snapshot, held)
    except (SourceAgentNotFound, SourceGateInvalid, SourceSnapshotUnavailable, SQLAlchemyError):
        raise HTTPException(503, _UNAVAILABLE) from None


@dataclass(frozen=True)
class SupportSnapshot:
    """The gate-held snapshot and the agent's source bindings, read in one gate hold.

    ``source_bound`` is whether the agent declares any source binding (step 0).
    @spec PROTECTED-HOOK-SOURCE-9 @spec PROTECTED-HOOK-LANE-4.
    """

    snapshot: SourceSnapshot
    source_bound: bool


@asynccontextmanager
async def authenticated_support(
    request: Request,
    session: AsyncSession,
    *,
    agent_id: uuid.UUID,
    hook: str,
    raw: bytes,
    tool_access: str | None,
    timestamp: str | None,
    delivery_id: str | None,
    signature: str | None,
) -> AsyncIterator[SupportSnapshot]:
    """Support-purpose authentication yielding the gate-held snapshot unrefused.

    Verifies only ``hook_source_signing.verify_support``; the caller resolves
    every source state. @spec PROTECTED-HOOK-SOURCE-9 @spec PROTECTED-HOOK-SOURCE-2/4.
    """
    signed = _SignedRequest(
        hook_source_signing.verify_support,
        hook,
        raw,
        tool_access,
        timestamp,
        delivery_id or "",
        signature,
    )
    try:
        await _preauthenticate(session, agent_id, signed)
        if not delivery_id:
            raise HTTPException(status.HTTP_400_BAD_REQUEST, MISSING_DELIVERY_DETAIL)
        async with _gated_snapshot(request, session, agent_id, signed) as (_held, snapshot):
            bindings = await session.scalar(
                select(Agent.source_bindings).where(Agent.id == agent_id)
            )
            yield SupportSnapshot(snapshot, bool(bindings))
    except (SourceAgentNotFound, SourceGateInvalid, SourceSnapshotUnavailable, SQLAlchemyError):
        raise HTTPException(503, _UNAVAILABLE) from None
