"""Mint and read for the agent's bounded channel read (ADR 0100, #2877).

The read repeats every authorization on every request, in a fixed order, and
refuses with a named ``channel_read.*`` code before any provider call. The one
refusal the provider decides is membership. Nothing here is specific to a
surface: each readable kind is a ``readers.ChannelReader`` registered in
``channel_readers``. Nothing here logs or stores a message body or the
capability.
"""

from __future__ import annotations

import tempfile
import time
import uuid
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import cast

import httpx
from aci_protocol.turn import SLACK_KIND
from curie_internal.channel_read_ledger import turn_key
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from starlette.concurrency import run_in_threadpool

from .. import bundles
from ..config import Settings
from ..identities import slack_bot_tokens
from ..models import AgentChannel, AgentVersion, Approval, Deployment
from ..resumequeue import parse_resume_event_id
from ..schemas.channel_read import (
    ChannelReadContext,
    ChannelReadContextMint,
    ChannelReadMessage,
    ChannelReadPage,
    ChannelReadRequest,
    ChannelSelector,
)
from ..storage import ObjectStore
from . import token as capability
from .errors import ChannelReadRefused
from .ledger import ChannelReadLedger
from .provider_guard import ProviderGuard
from .readers import BindingRoute, ChannelReader, ProviderPage, reader_for
from .slack_reads import SlackChannelReader
from .token import ChannelPair, ChannelReadClaims, GrantName
from .window import (
    CursorState,
    Operation,
    ResolvedWindow,
    mint_cursor,
    reconcile,
    resolve_limit,
    resolve_window,
    verify_cursor,
)

MAX_RESUME_HOPS = 8
_NO_GRANT = "the bundle grants no channel read or canvas operation"


def channel_readers(settings: Settings, http: httpx.AsyncClient) -> Mapping[str, ChannelReader]:
    """Every readable surface, keyed by binding kind."""

    return {SLACK_KIND: SlackChannelReader(http, slack_bot_tokens(settings))}


# The platform Slack grants a stored bundle declares, keyed by its digest. The
# stored object is write once, so the answer for a digest never changes.
_GRANTS: dict[str, frozenset[str]] = {}


@dataclass(frozen=True)
class LogicalTurn:
    turn: str
    hops: int


async def resolve_logical_turn(
    session: AsyncSession, agent_id: uuid.UUID, event_id: str
) -> LogicalTurn:
    """Walk an approval resume back to the event that opened the logical turn."""

    turn = event_id
    hops = 0
    while (approval_id := parse_resume_event_id(turn)) is not None:
        if hops >= MAX_RESUME_HOPS:
            raise ChannelReadRefused(
                409, "turn_unresolvable", "the resume chain is too long to resolve"
            )
        approval = await session.get(Approval, approval_id)
        if approval is None or approval.agent_id != agent_id:
            raise ChannelReadRefused(
                409, "turn_unresolvable", "the resumed turn does not resolve for this agent"
            )
        turn = approval.dedupe_key
        hops += 1
    return LogicalTurn(turn=turn, hops=hops)


def _declares_grant(data: bytes, settings: Settings) -> frozenset[str]:
    with tempfile.TemporaryDirectory() as tmp:
        dest = Path(tmp)
        bundles.extract_stored_bundle(
            data,
            dest,
            max_uncompressed_bytes=settings.bundle_max_uncompressed_bytes,
            max_compression_ratio=settings.bundle_max_compression_ratio,
            max_members=settings.bundle_max_members,
        )
        return bundles.declared_platform_slack_grants(dest)


async def _active_version(
    session: AsyncSession, *, agent_id: uuid.UUID, deployment_id: uuid.UUID
) -> AgentVersion | None:
    deployment = await session.get(Deployment, deployment_id, populate_existing=True)
    if deployment is None or deployment.status != "active" or deployment.agent_id != agent_id:
        return None
    return await session.get(AgentVersion, deployment.version_id)


async def read_grant(
    session: AsyncSession,
    store: ObjectStore,
    settings: Settings,
    *,
    agent_id: uuid.UUID,
    deployment_id: uuid.UUID,
) -> tuple[str, frozenset[str]]:
    """The granted bundle's digest for the agent's active deployment, and the
    platform Slack grants it declares (ADR 0100, ADR 0200). A bundle that
    declares none of them gets no capability."""

    version = await _active_version(session, agent_id=agent_id, deployment_id=deployment_id)
    if version is None:
        raise ChannelReadRefused(
            409, "deployment_inactive", "the deployment is not active for this agent"
        )
    digest = version.bundle_sha256
    if digest is None or version.bundle_ref is None:
        raise ChannelReadRefused(409, "grant_absent", _NO_GRANT)
    granted = _GRANTS.get(digest)
    if granted is None:
        data = await store.get(version.bundle_ref)
        granted = await run_in_threadpool(_declares_grant, data, settings)
        _GRANTS[digest] = granted
    if not granted:
        raise ChannelReadRefused(409, "grant_absent", _NO_GRANT)
    return digest, granted


async def mint_context(
    data: ChannelReadContextMint,
    *,
    session: AsyncSession,
    ledger: ChannelReadLedger,
    store: ObjectStore,
    settings: Settings,
) -> ChannelReadContext:
    grant, grants = await read_grant(
        session, store, settings, agent_id=data.agent_id, deployment_id=data.deployment_id
    )
    logical = await resolve_logical_turn(session, data.agent_id, data.event_id)
    now = int(time.time())
    ttl_s = min(data.ttl_s, capability.MAX_TTL_SECONDS)
    if data.mode == "open":
        assert data.owner is not None  # the schema requires it for open
        opened = await ledger.open(
            data.agent_id, logical.turn, data.owner, ttl_s, resume=logical.hops > 0
        )
        if opened == "expired":
            raise ChannelReadRefused(
                409, "turn_expired", "the resumed turn's read ledger has expired"
            )
        gen, exp = opened, now + ttl_s
    else:
        steered = await ledger.steer(data.agent_id, logical.turn, ttl_s)
        if steered is None:
            raise ChannelReadRefused(409, "turn_inactive", "the turn holds no live capability")
        # The lease, not the token, carries liveness: a steer token keeps the
        # finite lifetime the worker asked for, already capped at the turn deadline.
        gen, _remaining_ms = steered
        exp = now + ttl_s
    default = (
        ChannelPair(kind=data.default_channel.kind, address=data.default_channel.address)
        if data.default_channel is not None
        else None
    )
    claims = ChannelReadClaims(
        aud="channel.read",
        agent=data.agent_id,
        deployment=data.deployment_id,
        grant=grant,
        grants=tuple(cast("list[GrantName]", sorted(grants))),
        turn=logical.turn,
        gen=gen,
        default=default,
        iat=now,
        exp=exp,
    )
    return ChannelReadContext(
        token=capability.mint(settings.api_key, claims),
        generation=gen,
        expires_at=exp,
        turn_key=turn_key(logical.turn),
    )


@dataclass(frozen=True)
class AuthorizedBinding:
    kind: str
    address: str
    routes: list[BindingRoute]


async def authorized_binding(
    session: AsyncSession, claims: ChannelReadClaims, channel: ChannelSelector | None
) -> AuthorizedBinding:
    """The named channel, else the turn's default, as one of the agent's bindings."""

    if channel is not None:
        if channel.kind is None:
            raise ChannelReadRefused(400, "channel_required", "name a channel by kind and address")
        kind, address = channel.kind, channel.address
    elif claims.default is not None:
        kind, address = claims.default.kind, claims.default.address
    else:
        raise ChannelReadRefused(
            400, "channel_required", "this turn has no default channel; name one"
        )
    rows = (
        await session.scalars(
            select(AgentChannel).where(
                AgentChannel.agent_id == claims.agent,
                AgentChannel.kind == kind,
                AgentChannel.address == address,
            )
        )
    ).all()
    if not rows:
        raise ChannelReadRefused(403, "not_bound", "the agent is not bound to that channel")
    routes = [BindingRoute(adapter=r.adapter, endpoint=r.endpoint) for r in rows]
    return AuthorizedBinding(kind=kind, address=address, routes=routes)


def _invalid_identifier(message: str) -> ChannelReadRefused:
    return ChannelReadRefused(422, "invalid_identifier", message)


def _check_identifiers(
    reader: ChannelReader, op: Operation, thread_id: str | None, message_id: str | None
) -> None:
    if op == "thread":
        if thread_id is None or not reader.valid_thread_id(thread_id):
            raise _invalid_identifier("a thread read needs a thread_id")
    elif thread_id is not None:
        raise _invalid_identifier("thread_id belongs to a thread read only")
    if op == "message":
        if message_id is None or not reader.valid_message_id(message_id):
            raise _invalid_identifier("a message read needs a message id")
    elif message_id is not None:
        raise _invalid_identifier("message_id belongs to a message read only")


@dataclass(frozen=True)
class _Plan:
    op: Operation
    window: ResolvedWindow | None
    limit: int
    thread_id: str | None
    message_id: str | None
    provider_cursor: str | None
    boundary: int | None = None


def _plan(
    reader: ChannelReader,
    body: ChannelReadRequest,
    claims: ChannelReadClaims,
    binding: AuthorizedBinding,
    *,
    api_key: str,
    now: datetime,
) -> _Plan:
    if body.cursor is not None:
        state = verify_cursor(
            api_key,
            body.cursor,
            agent=claims.agent,
            turn=claims.turn,
            kind=binding.kind,
            address=binding.address,
        )
        reconcile(
            state,
            op=body.operation,
            oldest=body.oldest,
            latest=body.latest,
            thread_id=body.thread_id,
            message_id=body.message_id,
        )
        limit = resolve_limit(state.op, body.limit) if body.limit is not None else state.limit
        _check_identifiers(reader, state.op, state.thread_id, None)
        return _Plan(
            state.op,
            state.window,
            limit,
            state.thread_id,
            None,
            state.provider_cursor,
            state.boundary,
        )
    if body.operation is None:
        raise ChannelReadRefused(
            422, "operation_required", "name an operation or continue with a cursor"
        )
    op = body.operation
    window = resolve_window(op, body.oldest, body.latest, now)
    limit = resolve_limit(op, body.limit)
    _check_identifiers(reader, op, body.thread_id, body.message_id)
    return _Plan(op, window, limit, body.thread_id, body.message_id, None)


async def _provider_read(
    reader: ChannelReader, identity: str, binding: AuthorizedBinding, plan: _Plan
) -> ProviderPage:
    if plan.op == "message":
        assert plan.message_id is not None
        found = await reader.message(
            identity=identity, channel=binding.address, message_id=plan.message_id
        )
        if found is None:
            raise ChannelReadRefused(404, "message_not_found", "no such message in this channel")
        return ProviderPage(messages=[found])
    assert plan.window is not None
    if plan.op == "thread":
        assert plan.thread_id is not None
        return await reader.thread(
            identity=identity,
            channel=binding.address,
            thread_id=plan.thread_id,
            window=plan.window,
            limit=plan.limit,
            cursor=plan.provider_cursor,
            boundary=plan.boundary,
        )
    return await reader.history(
        identity=identity,
        channel=binding.address,
        window=plan.window,
        limit=plan.limit,
        cursor=plan.provider_cursor,
        boundary=plan.boundary,
    )


async def _guarded_provider_read(
    reader: ChannelReader,
    guard: ProviderGuard,
    identity: str,
    binding: AuthorizedBinding,
    plan: _Plan,
    claims: ChannelReadClaims,
) -> ProviderPage:
    """Refuse a cooled down method or a spent attempt budget, then call the provider once."""

    identity_key = reader.identity_key(identity)
    method = reader.rate_limit_key(plan.op, plan.message_id)
    remaining = await guard.cooldown_remaining(identity_key, method)
    if remaining is not None:
        raise ChannelReadRefused(
            429,
            "provider_rate_limited",
            "the provider is rate limiting reads; retry later",
            retry_after=remaining,
        )
    if not await guard.charge_attempt(claims.agent, claims.turn):
        raise ChannelReadRefused(
            429, "attempt_budget_exhausted", "this turn has used its provider read attempts"
        )
    try:
        return await _provider_read(reader, identity, binding, plan)
    except ChannelReadRefused as refused:
        if refused.code == "channel_read.provider_rate_limited":
            await guard.cool_down(identity_key, method, refused.retry_after)
        raise


_RESERVE_REFUSALS: Mapping[str, tuple[int, str, str]] = {
    "exhausted": (429, "page_budget_exhausted", "this turn has read its eight pages"),
    "inactive": (409, "turn_inactive", "the turn's capability is no longer current"),
    "expired": (409, "turn_expired", "the turn's read ledger has expired"),
}


async def check_authority(
    session: AsyncSession, ledger: ChannelReadLedger, claims: ChannelReadClaims
) -> None:
    """The turn's generation is current and the active deployment still grants
    the digest the capability names."""

    if not await ledger.is_current(claims.agent, claims.turn, claims.gen):
        raise ChannelReadRefused(409, "turn_inactive", "the turn's capability is no longer current")
    version = await _active_version(session, agent_id=claims.agent, deployment_id=claims.deployment)
    if version is None or version.bundle_sha256 != claims.grant:
        raise ChannelReadRefused(409, "grant_revoked", "the deployment no longer grants this read")


async def reserve_page(ledger: ChannelReadLedger, claims: ChannelReadClaims) -> None:
    """Reserve one of the turn's pages, or refuse by the ledger's reason."""

    reservation = await ledger.reserve(claims.agent, claims.turn, claims.gen)
    if reservation != "reserved":
        status, code, message = _RESERVE_REFUSALS[reservation]
        raise ChannelReadRefused(status, code, message)


async def authorize_and_read(
    *,
    claims: ChannelReadClaims,
    body: ChannelReadRequest,
    session: AsyncSession,
    ledger: ChannelReadLedger,
    guard: ProviderGuard,
    settings: Settings,
    readers: Mapping[str, ChannelReader],
    now: datetime,
) -> ChannelReadPage:
    await check_authority(session, ledger, claims)
    if "channelRead" not in claims.grants:
        raise ChannelReadRefused(
            403, "history_not_granted", "the bundle does not grant channel history reads"
        )
    binding = await authorized_binding(session, claims, body.channel)
    reader = reader_for(readers, binding.kind)
    if reader is None:
        raise ChannelReadRefused(
            409, "capability_unsupported", f"{binding.kind} channels cannot be read"
        )
    plan = _plan(reader, body, claims, binding, api_key=settings.api_key, now=now)
    identity = reader.identity(binding.routes)
    if not reader.has_identity(identity):
        raise ChannelReadRefused(
            503, "provider_unconfigured", "no provider credential serves this binding"
        )
    await reserve_page(ledger, claims)
    try:
        page = await _guarded_provider_read(reader, guard, identity, binding, plan, claims)
    except BaseException:
        await ledger.release(claims.agent, claims.turn)
        raise
    next_cursor = None
    if page.has_more and plan.window is not None and plan.op != "message":
        next_cursor = mint_cursor(
            settings.api_key,
            agent=claims.agent,
            turn=claims.turn,
            kind=binding.kind,
            address=binding.address,
            state=CursorState(
                op=plan.op,
                thread_id=plan.thread_id,
                window=plan.window,
                limit=plan.limit,
                provider_cursor=page.next_cursor,
                boundary=None if page.next_cursor is not None else page.boundary,
            ),
        )
    return ChannelReadPage(
        messages=[
            ChannelReadMessage(
                id=m.id,
                thread_id=m.thread_id,
                timestamp=m.timestamp,
                author=m.author,
                text=m.text,
                truncated=m.truncated,
                provenance=m.provenance,
                reply_count=m.reply_count,
            )
            for m in page.messages
        ],
        has_more=page.has_more,
        next_cursor=next_cursor,
    )
