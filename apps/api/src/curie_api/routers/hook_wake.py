"""Declared webhook wake admission and wake-key administration (ADR 0274).

``POST /hooks/{agent_id}/{hook}/wake`` lets a hosted connector that owns a work
queue wake one declared hook. The wire contract is
``docs/interfaces/triggers/INTERFACE.md``, section "Declared webhook wake
contract (ADR 0274)"; ADR 0274 decisions 2 to 4 give the reasons. This module
is the API half of the targetless shape: it admits at most one open
``hook_runs`` row per hook and enqueues the standing prompt as a targetless
``TurnSource.CRON`` turn, the shape the worker already runs for cron.

**Why its own route.** Ordinary ingress (``hooks.ingest_hook``) queues payload
text to a reply binding, signs with the shared agent key and branches into
protected admission. A wake carries no payload, target, prompt or tool policy:
the body is exactly ``{}`` and the declaration supplies the whole turn. A
separate route keeps declaration resolution out of that route's
pre-authentication and protected branches, and the wake key out of every
other route. Nothing here calls ``ingest_hook``, ``_ingest_protected`` or the
ordinary claim keys.

**Why authentication comes first and touches only generation metadata.** The
key is derived (``hook_wake_signing``), so verification needs the stored
generation and nothing else. Every malformed identity, header, unknown agent,
wrong key and stale timestamp gets the same 401 before any bundle is read, so
an unsigned caller learns nothing about which agents or hooks exist.

**Why the source gate is held from declaration to enqueue.** Source policy
writes, remediation writes and rotation take the agent's source gate. Holding
it across the declaration read, every guard, the reservation, the admission
transaction and the enqueue means none of them can change underneath an
admission. The per hook advisory lock is the one cron and pause take, so the
rechecks inside it also see a pause that committed first.

**Why the delivery reservation precedes any SQL write.** Decision 4 bounds
captured signature replay with a Valkey ``SET NX EX`` reservation that
outlives the signed timestamp's window. Every guard runs before it, so a
paused, killed or over limit hook refuses even a duplicate. A failed or
uncertain reservation refuses with no run mutation and no stream entry. There
is no follow-up flag, successor, outbox, reconciler or failure counter: a wake
that meets a live open run coalesces, and the connector's next poll recovers
any work that run missed.

**Secrecy.** The wake key, the signature and the delivery id never reach a
log line, exception message, span attribute or response body other than the
authorized key read. Every refusal is a fixed reason, and the one log line is
a fixed message.
"""

from __future__ import annotations

import logging
import time
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import Any

from aci_protocol import HookRunRef, QueuedTurn, TurnSource
from channel_protocol import hook_conversation_id
from curie_protected_hooks.source_policy_sql import (
    SourceAgentNotFound,
    SourceGate,
    SourceGateContext,
    SourceGateInvalid,
    SourceSnapshotUnavailable,
    ensure_source_gate_live,
    read_source_snapshot,
)
from fastapi import APIRouter, Depends, HTTPException, Request, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from redis.exceptions import ResponseError
from sqlalchemy import TextClause, select, text
from sqlalchemy.exc import DBAPIError, SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession
from starlette.concurrency import run_in_threadpool

from .. import hook_signing, hook_wake_signing
from ..auth import require_api_key
from ..config import get_settings
from ..db import SCHEMA
from ..deps import SessionDep, StoreDep
from ..hook_partition import HOOK_NAME
from ..models import Agent, RemediationPolicy
from ..schemas.hook_wake import (
    HookWakeCoalescedOut,
    HookWakeErrorOut,
    HookWakeQueuedOut,
    HookWakeRotationOut,
    HookWakeSecretOut,
)
from ..storage import ObjectStore
from ..wirebody import read_bounded_body
from .schedules import read_wake_triggers

router = APIRouter(prefix="/hooks", tags=["hooks"])
admin_router = APIRouter(prefix="/agents", tags=["hooks"], dependencies=[Depends(require_api_key)])
logger = logging.getLogger(__name__)

_INVALID = "invalid_wake_signature"
_QUERY = "wake_query_forbidden"
_BODY = "wake_body_invalid"
_NOT_DECLARED = "wake_not_declared"
_UNREADABLE = "wake_declaration_unavailable"
_NOT_ORDINARY = "wake_source_not_ordinary"
_AUTHORITY = "authority_unavailable"
_PAUSED = "hook_paused"
_KILLED = "agent_killed"
_BUDGET = "budget_exhausted"
_DEDUPE = "wake_dedupe_unavailable"
_ENQUEUE = "wake_enqueue_unavailable"
_EXHAUSTED = "wake_generation_exhausted"

_NO_STORE = {"Cache-Control": "no-store"}

# Postgres numeric_value_out_of_range.
_OUT_OF_RANGE = "22003"

# The statements below repeat ``hook_fire``'s, which this module may not import
# (``scripts/check-private-imports.py``). The in-force rule is the platform's
# ranking: prod outranks dev, then the newest active deployment.
_IN_FORCE_SQL = """
SELECT DISTINCT ON (a.id)
       a.id AS agent_id,
       v.id AS version_id,
       v.bundle_ref AS bundle_ref
FROM {schema}.agents a
JOIN {schema}.deployments d ON d.agent_id = a.id AND d.status = 'active'
JOIN {schema}.agent_versions v ON v.id = d.version_id AND v.agent_id = a.id
WHERE a.id = :agent_id
ORDER BY a.id, (d.environment = 'prod') DESC, d.deployed_at DESC, d.id DESC
"""

# The per agent and hook admission lock cron, manual fire and pause share.
_LOCK_SQL = """
SELECT pg_advisory_xact_lock(hashtextextended(CAST(:agent_id AS text) || ':' || :name, 0))
"""

_IN_FLIGHT_SQL = """
SELECT 1 FROM {schema}.hook_runs
WHERE agent_id = :agent_id AND name = :name AND outcome IS NULL
LIMIT 1
"""

_FAIL_SQL = """
UPDATE {schema}.hook_runs
SET outcome = 'failed', reason = 'enqueue_failed', ended_at = now()
WHERE id = :id AND outcome IS NULL
"""

_GENERATION_SQL = """
SELECT generation FROM {schema}.hook_wake_keys
WHERE agent_id = :agent_id AND hook = :hook
"""

_PAUSED_SQL = """
SELECT paused_at FROM {schema}.schedule_controls
WHERE agent_id = :agent_id AND name = :name
"""

# The cron loop's reclaim (ADR 0274 decision 4), on the database clock that
# set the lease. A claim with no lease is held for one lease from its start.
_RECLAIM_SQL = """
UPDATE {schema}.hook_runs
SET outcome = 'reclaimed', reason = 'claim_expired', ended_at = now()
WHERE agent_id = :agent_id AND name = :name AND outcome IS NULL
  AND COALESCE(lease_expires_at, started_at + make_interval(secs => :lease_s)) < now()
"""

# The slot is a server-allocated unique run instant, not a schedule: strictly
# after every slot this hook has used, so the (agent, name, slot) key never
# collides with a cron or manual row. The casts fix each parameter's type in
# the SELECT list, where Postgres would otherwise deduce the name parameter as
# text from the WHERE clause and varchar from the target column.
_ADMIT_SQL = """
INSERT INTO {schema}.hook_runs
       (id, agent_id, name, slot_utc, version_id, source, outcome, reason, started_at, ended_at,
        lease_expires_at)
SELECT CAST(:id AS uuid), CAST(:agent_id AS uuid), CAST(:name AS text),
       GREATEST(clock_timestamp(),
                COALESCE(max(slot_utc) + interval '1 microsecond', '-infinity'::timestamptz)),
       CAST(:version_id AS uuid), 'webhook', NULL, NULL, now(), NULL,
       now() + make_interval(secs => :lease_s)
FROM {schema}.hook_runs
WHERE agent_id = :agent_id AND name = :name
RETURNING id, slot_utc
"""

# Absent metadata is generation 1, so the first rotation writes 2. The bound is
# cast because an INTEGER column would otherwise type the parameter as int4,
# which cannot carry the contract's 2^63-1.
_ROTATE_SQL = """
INSERT INTO {schema}.hook_wake_keys (agent_id, hook, generation)
VALUES (:agent_id, :hook, 2)
ON CONFLICT (agent_id, hook) DO UPDATE
SET generation = hook_wake_keys.generation + 1, updated_at = now()
WHERE hook_wake_keys.generation < CAST(:max AS bigint)
RETURNING generation
"""


def _sql(statement: str) -> TextClause:
    return text(statement.format(schema=SCHEMA))


def _limit_spent(usd: Any, tokens: Any) -> bool:
    """``hook_fire``'s configured-limit rule: only a set, nonpositive limit refuses.

    An unset limit takes a positive default there, so it never refuses.
    """

    return (usd is not None and float(usd) <= 0) or (tokens is not None and int(tokens) <= 0)


class _Refusal(Exception):
    """One contract refusal: a status and a fixed reason, never request data."""

    def __init__(self, status_code: int, detail: str) -> None:
        super().__init__(detail)
        self.status_code = status_code
        self.detail = detail


def _error(description: str) -> dict[str, Any]:
    return {"model": HookWakeErrorOut, "description": description}


_WAKE_RESPONSES: dict[int | str, dict[str, Any]] = {
    202: {"model": HookWakeQueuedOut, "description": "One webhook run admitted and enqueued"},
    200: {
        "model": HookWakeCoalescedOut,
        "description": "Joined a live open run, or the delivery id is already reserved",
    },
    400: _error("wake_query_forbidden"),
    401: _error("invalid_wake_signature"),
    404: _error("wake_not_declared"),
    409: _error("wake_source_not_ordinary, hook_paused or agent_killed"),
    413: _error("hook body exceeds the maximum size"),
    422: _error("wake_body_invalid"),
    429: _error("budget_exhausted"),
    503: _error(
        "authority_unavailable, wake_declaration_unavailable, wake_dedupe_unavailable "
        "or wake_enqueue_unavailable"
    ),
}

_ADMIN_RESPONSES: dict[int | str, dict[str, Any]] = {
    400: _error("wake_query_forbidden"),
    404: _error("wake_not_declared"),
    409: _error("wake_source_not_ordinary"),
    422: _error("wake_body_invalid"),
    503: _error("authority_unavailable or wake_declaration_unavailable"),
}

_ROTATE_RESPONSES: dict[int | str, dict[str, Any]] = {
    **_ADMIN_RESPONSES,
    503: _error("authority_unavailable, wake_declaration_unavailable or wake_generation_exhausted"),
}


def _answer(status_code: int, body: BaseModel, *, no_store: bool = False) -> JSONResponse:
    return JSONResponse(
        status_code=status_code,
        content=body.model_dump(mode="json"),
        headers=_NO_STORE if no_store else None,
    )


def _refused(refusal: _Refusal, *, no_store: bool = False) -> JSONResponse:
    return _answer(refusal.status_code, HookWakeErrorOut(detail=refusal.detail), no_store=no_store)


def _coalesced() -> JSONResponse:
    return _answer(status.HTTP_200_OK, HookWakeCoalescedOut(status="coalesced"))


async def _generation(session: AsyncSession, agent_id: uuid.UUID, hook: str) -> int:
    """The stored wake generation, 1 when absent; unreadable metadata fails closed."""

    try:
        stored = await session.scalar(_sql(_GENERATION_SQL), {"agent_id": agent_id, "hook": hook})
    except SQLAlchemyError:
        raise _Refusal(status.HTTP_503_SERVICE_UNAVAILABLE, _AUTHORITY) from None
    if stored is None:
        return 1
    if type(stored) is not int or not 1 <= stored <= hook_wake_signing.MAX_GENERATION:
        raise _Refusal(status.HTTP_503_SERVICE_UNAVAILABLE, _AUTHORITY)
    return stored


@asynccontextmanager
async def _gated_agent(
    request: Request, session: AsyncSession, agent_id: uuid.UUID
) -> AsyncIterator[tuple[Agent, SourceGateContext]]:
    """Hold the agent's source gate, as ``hook_fire._source_locked_agent`` does.

    A missing, invalid or unavailable gate, and any database failure inside it
    that no step mapped itself, is 503 ``authority_unavailable``: authority that
    cannot be read refuses closed. A missing agent is ``wake_not_declared``.
    """

    try:
        await session.rollback()
        gate = getattr(request.app.state, "source_gate", None)
        if not isinstance(gate, SourceGate):
            raise SourceSnapshotUnavailable("source_gate_unavailable")
        async with gate.hold(agent_id) as held:
            agent: Agent | None = await session.scalar(
                select(Agent).where(Agent.id == agent_id).execution_options(populate_existing=True)
            )
            if agent is None:
                raise _Refusal(status.HTTP_404_NOT_FOUND, _NOT_DECLARED)
            yield agent, held
    except (SourceAgentNotFound, SourceGateInvalid, SourceSnapshotUnavailable, SQLAlchemyError):
        raise _Refusal(status.HTTP_503_SERVICE_UNAVAILABLE, _AUTHORITY) from None


async def _declaration(
    session: AsyncSession, store: ObjectStore, agent_id: uuid.UUID, hook: str
) -> tuple[uuid.UUID, str]:
    """The ranked active version and the hook's standing prompt (contract step 3)."""

    deployment = (
        (await session.execute(_sql(_IN_FORCE_SQL), {"agent_id": agent_id})).mappings().first()
    )
    if deployment is None or deployment["bundle_ref"] is None:
        raise _Refusal(status.HTTP_404_NOT_FOUND, _NOT_DECLARED)
    try:
        data = await store.get(str(deployment["bundle_ref"]))
        wakes = await run_in_threadpool(read_wake_triggers, data)
    except Exception:  # noqa: BLE001 - any unreadable bundle is the one 503; diagnostics stay out
        raise _Refusal(status.HTTP_503_SERVICE_UNAVAILABLE, _UNREADABLE) from None
    trigger = wakes.get(hook)
    if trigger is None:
        raise _Refusal(status.HTTP_404_NOT_FOUND, _NOT_DECLARED)
    prompt = trigger.prompt
    if not isinstance(prompt, str) or not prompt.strip():
        # Unreachable through the shared validator; a blank prompt is malformed.
        raise _Refusal(status.HTTP_503_SERVICE_UNAVAILABLE, _UNREADABLE)
    return deployment["version_id"], prompt.strip()


async def _require_ordinary(
    session: AsyncSession, held: SourceGateContext, agent_id: uuid.UUID, hook: str
) -> Any:
    """Ordinary source authority and no hook-scoped configuration (contract step 4).

    The source half is ``hook_fire._require_ordinary_source`` for a name that
    matches ``HOOK_NAME``, which every wake name must: the gate-held snapshot
    of policy and history. A source policy or unresolved history is the 409,
    because the hook is not ordinary, where that helper answers 503; only an
    unreadable snapshot is 503. Returns the agent's limit columns for the
    budget guard.
    """

    try:
        snapshot = await read_source_snapshot(held, await session.connection(), hook)
    except SourceAgentNotFound:
        raise _Refusal(status.HTTP_404_NOT_FOUND, _NOT_DECLARED) from None
    except (SourceGateInvalid, SourceSnapshotUnavailable, SQLAlchemyError):
        raise _Refusal(status.HTTP_503_SERVICE_UNAVAILABLE, _AUTHORITY) from None
    if not snapshot.never_configured:
        raise _Refusal(status.HTTP_409_CONFLICT, _NOT_ORDINARY)
    controls = (
        await session.execute(
            select(
                Agent.hook_partitions,
                Agent.source_bindings,
                Agent.max_usd_per_day,
                Agent.max_output_tokens_per_run,
            ).where(Agent.id == agent_id)
        )
    ).one_or_none()
    if controls is None:
        raise _Refusal(status.HTTP_404_NOT_FOUND, _NOT_DECLARED)
    # Any remediation row counts, removed or not: its generation survives removal.
    remediation = await session.scalar(
        select(RemediationPolicy.generation).where(
            RemediationPolicy.agent_id == agent_id, RemediationPolicy.hook == hook
        )
    )
    if (
        remediation is not None
        or hook in (controls.hook_partitions or {})
        or hook in (controls.source_bindings or {})
    ):
        raise _Refusal(status.HTTP_409_CONFLICT, _NOT_ORDINARY)
    return controls


async def _require_admissible(
    request: Request, session: AsyncSession, agent_id: uuid.UUID, hook: str, controls: Any
) -> None:
    """Pause, then kill, then configured limits (contract step 5)."""

    paused = await session.scalar(_sql(_PAUSED_SQL), {"agent_id": agent_id, "name": hook})
    if paused is not None:
        raise _Refusal(status.HTTP_409_CONFLICT, _PAUSED)
    try:
        killed = await request.app.state.kill_switch.is_killed(agent_id)
    except Exception:  # noqa: BLE001 - unreadable kill authority refuses before any reservation
        raise _Refusal(status.HTTP_503_SERVICE_UNAVAILABLE, _AUTHORITY) from None
    if killed:
        raise _Refusal(status.HTTP_409_CONFLICT, _KILLED)
    if _limit_spent(controls.max_usd_per_day, controls.max_output_tokens_per_run):
        raise _Refusal(status.HTTP_429_TOO_MANY_REQUESTS, _BUDGET)


async def _close_failed(session: AsyncSession, held: SourceGateContext, run_id: Any) -> None:
    """Best effort ``failed`` / ``enqueue_failed`` close after a known failed append."""

    try:
        async with session.begin():
            await session.connection()
            await ensure_source_gate_live(held)
            await session.execute(_sql(_FAIL_SQL), {"id": run_id})
    except Exception:  # noqa: BLE001 - cleanup diagnostics stay out of logs
        # The committed claim stays recoverable through lease reclaim.
        logger.warning("hook_wake_cleanup_unavailable")


@router.post(
    "/{agent_id}/{hook}/wake",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=None,
    responses=_WAKE_RESPONSES,
)
async def wake_hook(
    request: Request, session: SessionDep, store: StoreDep, agent_id: str, hook: str
) -> JSONResponse:
    """Admit one targetless run of a declared webhook wake hook.

    The body is exactly ``{}``, signed with the hook's wake key; there are no
    query parameters. 202 ``queued`` names the admitted run; 200 ``coalesced``
    means a run was already open or this delivery id was already reserved, so
    poll again later with a fresh delivery id if work remains.
    \f
    Refusal order is the contract's "Admission responses and ordering"
    (ADR 0274 decisions 2 and 4). Path parameters are plain strings and the
    headers are read from the request, so FastAPI never answers with its own
    validation 422.
    """

    try:
        return await _wake(request, session, store, agent_id, hook)
    except _Refusal as refusal:
        return _refused(refusal)


async def _wake(
    request: Request, session: AsyncSession, store: ObjectStore, raw_agent: str, hook: str
) -> JSONResponse:
    settings = get_settings()

    # Step 1: bound the body, then authenticate against generation metadata only.
    try:
        raw = await read_bounded_body(request, settings.hook_max_body_bytes, subject="hook body")
    except HTTPException as exc:
        raise _Refusal(exc.status_code, str(exc.detail)) from None
    agent_id = hook_wake_signing.canonical_agent_id(raw_agent)
    timestamp = request.headers.get(hook_signing.TIMESTAMP_HEADER)
    delivery_id = request.headers.get(hook_signing.DELIVERY_HEADER)
    presented = request.headers.get(hook_signing.SIGNATURE_HEADER)
    if (
        agent_id is None
        or HOOK_NAME.fullmatch(hook) is None
        or timestamp is None
        or delivery_id is None
        or presented is None
        or not hook_wake_signing.well_formed(timestamp, delivery_id, presented)
    ):
        raise _Refusal(status.HTTP_401_UNAUTHORIZED, _INVALID)
    agent_uuid = uuid.UUID(agent_id)
    generation = await _generation(session, agent_uuid, hook)
    authentic = hook_wake_signing.verify(
        hook_wake_signing.derive_key(
            settings.api_key, agent_id=agent_id, hook=hook, generation=generation
        ),
        timestamp=timestamp,
        delivery_id=delivery_id,
        agent_id=agent_id,
        hook=hook,
        body=raw,
        header=presented,
        now=time.time(),
    )
    if not authentic:
        raise _Refusal(status.HTTP_401_UNAUTHORIZED, _INVALID)

    # Step 2: request shape, only after authentication.
    if request.scope.get("query_string"):
        raise _Refusal(status.HTTP_400_BAD_REQUEST, _QUERY)
    if raw != hook_wake_signing.MAX_BODY:
        raise _Refusal(status.HTTP_422_UNPROCESSABLE_CONTENT, _BODY)

    async with _gated_agent(request, session, agent_uuid) as (agent, held):
        # Steps 3 to 5: declaration, ordinary source, pause, kill, limits.
        version_id, prompt = await _declaration(session, store, agent_uuid, hook)
        controls = await _require_ordinary(session, held, agent_uuid, hook)
        await _require_admissible(request, session, agent_uuid, hook, controls)

        # Step 6: reserve the signed delivery id before any run mutation.
        reservation = hook_wake_signing.dedupe_key(
            settings.api_key,
            agent_id=agent_id,
            hook=hook,
            generation=generation,
            delivery_id=delivery_id,
        )
        try:
            reserved = await request.app.state.valkey.set(
                reservation,
                "1",
                nx=True,
                ex=hook_wake_signing.dedupe_ttl_s(timestamp, time.time()),
            )
        except Exception:  # noqa: BLE001 - failed or uncertain reservation; key stays out of logs
            raise _Refusal(status.HTTP_503_SERVICE_UNAVAILABLE, _DEDUPE) from None
        if not reserved:
            return _coalesced()

        # Step 7: recheck under the per hook lock, reclaim, then coalesce or admit.
        await session.commit()
        admitted: Any = None
        async with session.begin():
            await session.execute(_sql(_LOCK_SQL), {"agent_id": agent_id, "name": hook})
            if await _generation(session, agent_uuid, hook) != generation:
                raise _Refusal(status.HTTP_401_UNAUTHORIZED, _INVALID)
            controls = await _require_ordinary(session, held, agent_uuid, hook)
            await _require_admissible(request, session, agent_uuid, hook, controls)
            hook_key = {"agent_id": agent_uuid, "name": hook}
            leased = {**hook_key, "lease_s": settings.effective_hook_claim_lease_s}
            await session.execute(_sql(_RECLAIM_SQL), leased)
            busy = (await session.execute(_sql(_IN_FLIGHT_SQL), hook_key)).first()
            if busy is None:
                await ensure_source_gate_live(held)
                admitted = (
                    (
                        await session.execute(
                            _sql(_ADMIT_SQL),
                            {**leased, "id": uuid.uuid4(), "version_id": version_id},
                        )
                    )
                    .mappings()
                    .one()
                )
        if admitted is None:
            # A live open run, queued or started: only the reservation was written.
            return _coalesced()

        # Step 8: enqueue after commit. A server error reply means the XADD was
        # not applied (known); anything else may have landed (uncertain).
        run_id = admitted["id"]
        try:
            slot_iso = admitted["slot_utc"].isoformat()
            turn = QueuedTurn(
                event_id=f"cron:{agent.id}:{hook}:{slot_iso}",
                conversation_id=hook_conversation_id(agent.id, hook),
                author=f"cron:{hook}",
                text=prompt,
                source=TurnSource.CRON,
                reply_handle=None,
                received_at=datetime.now(UTC).isoformat(),
                hook_run=HookRunRef(agent_id=str(agent.id), name=hook, slot_utc=slot_iso),
            )
            await ensure_source_gate_live(held)
        except (ValueError, SourceSnapshotUnavailable):
            known = True
        else:
            try:
                await request.app.state.resume_queue.enqueue(turn)
            except ResponseError:
                known = True
            except Exception:  # noqa: BLE001 - uncertain append keeps the claim for lease recovery
                known = False
            else:
                return _answer(
                    status.HTTP_202_ACCEPTED, HookWakeQueuedOut(status="queued", run_id=run_id)
                )
        if known:
            await _close_failed(session, held, run_id)
        raise _Refusal(status.HTTP_503_SERVICE_UNAVAILABLE, _ENQUEUE)


async def _admin_target(request: Request, raw_agent: str, hook: str) -> tuple[str, uuid.UUID]:
    """Request shape and path identity of a wake-key operation, after admin auth."""

    if request.scope.get("query_string"):
        raise _Refusal(status.HTTP_400_BAD_REQUEST, _QUERY)
    try:
        body = await read_bounded_body(
            request, get_settings().hook_max_body_bytes, subject="hook body"
        )
    except HTTPException:
        # An oversized body is still a nonempty one.
        raise _Refusal(status.HTTP_422_UNPROCESSABLE_CONTENT, _BODY) from None
    if body:
        raise _Refusal(status.HTTP_422_UNPROCESSABLE_CONTENT, _BODY)
    agent_id = hook_wake_signing.canonical_agent_id(raw_agent)
    if agent_id is None or HOOK_NAME.fullmatch(hook) is None:
        raise _Refusal(status.HTTP_404_NOT_FOUND, _NOT_DECLARED)
    return agent_id, uuid.UUID(agent_id)


def _out_of_range(exc: DBAPIError) -> bool:
    """Whether the driver reported numeric_value_out_of_range (SQLSTATE 22003)."""

    orig = exc.orig
    return any(
        getattr(candidate, "sqlstate", None) == _OUT_OF_RANGE
        for candidate in (orig, getattr(orig, "__cause__", None))
    )


@admin_router.get(
    "/{agent_id}/hooks/{hook}/wake-secret",
    response_model=HookWakeSecretOut,
    responses=_ADMIN_RESPONSES,
)
async def get_wake_secret(
    request: Request, session: SessionDep, store: StoreDep, agent_id: str, hook: str
) -> JSONResponse:
    """Read the current wake key of one declared webhook wake hook.

    Repeated reads return the same key until rotation. Provision it into the
    connector's operator-managed secret as ``CURIE_HOOK_WAKE_KEY``.
    \f
    Same authority as ``agents.get_hook_secret``; ``Cache-Control: no-store``
    on every answer. Reading creates no generation row (ADR 0274 decision 2).
    """

    try:
        canonical, agent_uuid = await _admin_target(request, agent_id, hook)
        async with _gated_agent(request, session, agent_uuid) as (_agent, held):
            await _declaration(session, store, agent_uuid, hook)
            await _require_ordinary(session, held, agent_uuid, hook)
            generation = await _generation(session, agent_uuid, hook)
            secret = hook_wake_signing.derive_key(
                get_settings().api_key, agent_id=canonical, hook=hook, generation=generation
            )
            await ensure_source_gate_live(held)
        out = HookWakeSecretOut(agent_id=canonical, hook=hook, generation=generation, secret=secret)
        return _answer(status.HTTP_200_OK, out, no_store=True)
    except _Refusal as refusal:
        return _refused(refusal, no_store=True)


@admin_router.post(
    "/{agent_id}/hooks/{hook}/wake-secret/rotate",
    response_model=HookWakeRotationOut,
    responses=_ROTATE_RESPONSES,
)
async def rotate_wake_secret(
    request: Request, session: SessionDep, store: StoreDep, agent_id: str, hook: str
) -> JSONResponse:
    """Rotate one hook's wake key; the previous key stops working at once.

    The answer carries the new generation and no key; read the key back with
    the GET. Rotation does not cancel an admitted run.
    \f
    The source gate and per hook lock are the ones admission holds, so a
    rotation serializes with admission and the old key is refused right after
    (ADR 0274 decision 2). ``Cache-Control: no-store`` on every answer.
    """

    try:
        canonical, agent_uuid = await _admin_target(request, agent_id, hook)
        async with _gated_agent(request, session, agent_uuid) as (_agent, held):
            await _declaration(session, store, agent_uuid, hook)
            await _require_ordinary(session, held, agent_uuid, hook)
            await session.commit()
            try:
                async with session.begin():
                    await session.execute(_sql(_LOCK_SQL), {"agent_id": canonical, "name": hook})
                    await _require_ordinary(session, held, agent_uuid, hook)
                    await ensure_source_gate_live(held)
                    generation = await session.scalar(
                        _sql(_ROTATE_SQL),
                        {
                            "agent_id": agent_uuid,
                            "hook": hook,
                            "max": hook_wake_signing.MAX_GENERATION,
                        },
                    )
                    if generation is None:
                        raise _Refusal(status.HTTP_503_SERVICE_UNAVAILABLE, _EXHAUSTED)
            except DBAPIError as exc:
                # The INTEGER column overflows before the contract's 2^63-1 bound;
                # that is the same exhaustion, and the rollback mutated nothing.
                if _out_of_range(exc):
                    raise _Refusal(status.HTTP_503_SERVICE_UNAVAILABLE, _EXHAUSTED) from None
                raise
        out = HookWakeRotationOut(agent_id=canonical, hook=hook, generation=generation)
        return _answer(status.HTTP_200_OK, out, no_store=True)
    except _Refusal as refusal:
        return _refused(refusal, no_store=True)
