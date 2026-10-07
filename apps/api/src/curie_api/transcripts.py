"""Per-thread conversation transcripts (ADR-0170, #3070).

A transcript used to be one key in the agent's reserved ``transcript``
namespace of the durable state store, so every thread an agent ever ran shared
that namespace's byte cap. A busy factory agent filled it and then failed every
new run. Transcripts now live in ``thread_transcripts``: one row per thread,
capped per thread by ``transcript_max_thread_bytes``, with no agent-wide cap.

The state router still serves ``/state/transcript/<thread_key>`` from here, so
the runner's ``CURIE_HISTORY_REF`` and the worker's publication outcome append
are unchanged on the wire. Revision 0059 reconciles and removes legacy rows;
this table is the only runtime transcript store.

Lifetime: a WorkItem's transcript is deleted in the transaction that makes the
WorkItem terminal (``expire_for_work_item``). Every write also moves
``expires_at`` forward by ``transcript_idle_ttl_seconds``; an expired row reads
as absent and is deleted by the next write for the same agent. That covers
threads with no WorkItem, and is a backstop for a WorkItem thread.

The thread's attachment ledger (``thread_attachments``, ADR 0205) has the same
lifetime: every path here that ends a thread's history deletes its file
references in the same transaction, and the sweep also removes references past
their own expiry that no live transcript holds.
"""

from __future__ import annotations

import json
import uuid
from datetime import timedelta
from typing import Any

from curie_telemetry import record_metric
from fastapi import HTTPException, status
from sqlalchemy import delete, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from . import thread_attachments
from .config import get_settings
from .models import ThreadTranscript, WorkItem
from .threadkeys import pre_identity_thread_key_for

TRANSCRIPT_NAMESPACE = "transcript"


def json_size(value: Any) -> int:
    """Serialized-JSON byte length, the unit every state and transcript cap uses."""
    return len(json.dumps(value, separators=(",", ":")).encode("utf-8"))


def _live() -> Any:
    return or_(ThreadTranscript.expires_at.is_(None), ThreadTranscript.expires_at > func.now())


def _where(agent_id: uuid.UUID, scope: str | None, key: str) -> tuple[Any, ...]:
    return (
        ThreadTranscript.agent_id == agent_id,
        ThreadTranscript.binding_scope == scope,
        ThreadTranscript.thread_key == key,
    )


def _expiry() -> Any:
    return func.now() + timedelta(seconds=get_settings().transcript_idle_ttl_seconds)


def enforce_thread_cap(key: str, value: Any, *, reserve_bytes: int | None = None) -> None:
    """Refuse a transcript larger than the per-thread cap.

    ``reserve_bytes`` (#2927) also refuses a value that leaves less than that
    much room under the cap. That refusal is the runner's compaction trigger, so
    it records no persistence failure.
    """
    cap = get_settings().transcript_max_thread_bytes
    size = json_size(value)
    if size > cap:
        record_metric(
            "curie.history.persistence.failure",
            attributes={
                "service.name": "curie-api",
                "source": "state-api",
                "outcome": "capacity",
                "limit": "value",
            },
        )
        raise HTTPException(
            413,
            f"value for key {key!r} is {size} bytes, over the {cap}-byte per-thread transcript cap",
        )
    if reserve_bytes is not None and cap - size < reserve_bytes:
        raise HTTPException(
            413,
            f"value for key {key!r} is {size} bytes, leaving under the "
            f"{reserve_bytes}-byte reserve of the {cap}-byte per-thread transcript cap",
        )


async def _delete_pre_identity(
    session: AsyncSession, agent_id: uuid.UUID, scope: str | None, keys: list[str]
) -> None:
    """Delete the pre-identity row a route in ``keys`` may still have.

    Called wherever a thread's history ends (``remove``,
    ``expire_for_work_item``, and a key ``_sweep_expired`` just swept): the
    rule that adoption never touches the old row belongs to adoption, not to
    the end of history, and without this the old row outlives the delete and
    the next read on ``key`` copies it straight back.
    SKIP LOCKED: a worker that has not rolled and is mid-write to the old key
    is left alone, and a later end of history catches it.
    """
    old_keys = [
        old_key
        for key in keys
        if (old_key := await pre_identity_thread_key_for(session, agent_id, key)) is not None
    ]
    if not old_keys:
        return
    locked = (
        select(ThreadTranscript.id)
        .where(
            ThreadTranscript.agent_id == agent_id,
            ThreadTranscript.binding_scope == scope,
            ThreadTranscript.thread_key.in_(old_keys),
        )
        .with_for_update(skip_locked=True)
    )
    await session.execute(delete(ThreadTranscript).where(ThreadTranscript.id.in_(locked)))


async def _sweep_expired(session: AsyncSession, agent_id: uuid.UUID) -> None:
    """Delete this agent's expired transcripts.

    SKIP LOCKED, so the sweep never waits on a row another transaction holds and
    cannot form a lock cycle with a concurrent append; a skipped row is swept by
    a later write.
    """
    expired = (
        select(ThreadTranscript.id)
        .where(ThreadTranscript.agent_id == agent_id, ThreadTranscript.expires_at <= func.now())
        .with_for_update(skip_locked=True)
    )
    swept = await session.execute(
        delete(ThreadTranscript)
        .where(ThreadTranscript.id.in_(expired))
        .returning(ThreadTranscript.binding_scope, ThreadTranscript.thread_key)
    )
    by_scope: dict[str | None, list[str]] = {}
    for scope, key in swept:
        by_scope.setdefault(scope, []).append(key)
    for scope, keys in by_scope.items():
        await _delete_pre_identity(session, agent_id, scope, keys)
        await thread_attachments.delete_for(session, agent_id, scope, keys)
    await thread_attachments.sweep_orphans(session, agent_id)


async def _adopt_pre_identity(
    session: AsyncSession, agent_id: uuid.UUID, scope: str | None, key: str
) -> bool:
    """Copy the transcript a named non-Slack route kept under its pre-identity key.

    ADR-0168 decision 4 gave that route's key an identity segment. Copy when
    this key has no row or the old row is newer. The read of the old row
    never locks or deletes it, so a worker that has not rolled keeps writing
    it. Does not commit. Returns whether anything was copied.
    """
    old_key = await pre_identity_thread_key_for(session, agent_id, key)
    if old_key is None:
        return False
    old: ThreadTranscript | None = await session.scalar(
        select(ThreadTranscript).where(*_where(agent_id, scope, old_key), _live())
    )
    if old is None:
        return False
    current: ThreadTranscript | None = await session.scalar(
        select(ThreadTranscript).where(*_where(agent_id, scope, key)).with_for_update()
    )
    if current is None:
        session.add(
            ThreadTranscript(
                agent_id=agent_id,
                binding_scope=scope,
                thread_key=key,
                value=old.value,
                version=old.version,
                expires_at=_expiry(),
            )
        )
    elif old.updated_at > current.updated_at:
        current.value = old.value
        current.version = max(current.version, old.version) + 1
        current.expires_at = _expiry()
    else:
        return False
    await session.flush()
    return True


async def get(
    session: AsyncSession, agent_id: uuid.UUID, scope: str | None, key: str
) -> ThreadTranscript | None:
    if await _adopt_pre_identity(session, agent_id, scope, key):
        await session.commit()
    row: ThreadTranscript | None = await session.scalar(
        select(ThreadTranscript).where(*_where(agent_id, scope, key), _live())
    )
    return row


async def _get_locked(
    session: AsyncSession, agent_id: uuid.UUID, scope: str | None, key: str
) -> ThreadTranscript | None:
    await _adopt_pre_identity(session, agent_id, scope, key)
    await _sweep_expired(session, agent_id)
    row: ThreadTranscript | None = await session.scalar(
        select(ThreadTranscript).where(*_where(agent_id, scope, key)).with_for_update()
    )
    return row


async def put(
    session: AsyncSession,
    agent_id: uuid.UUID,
    scope: str | None,
    key: str,
    value: Any,
    expected_version: int | None,
) -> ThreadTranscript:
    """Replace a thread's transcript, optionally compare-and-set. Commits."""
    enforce_thread_cap(key, value)
    row = await _get_locked(session, agent_id, scope, key)
    if row is None:
        if expected_version is not None:
            raise HTTPException(
                status.HTTP_409_CONFLICT, "version mismatch: entry does not exist yet"
            )
        row = ThreadTranscript(
            agent_id=agent_id,
            binding_scope=scope,
            thread_key=key,
            value=value,
            expires_at=_expiry(),
        )
        session.add(row)
    else:
        if expected_version is not None and expected_version != row.version:
            raise HTTPException(
                status.HTTP_409_CONFLICT,
                f"version mismatch: expected {expected_version}, stored {row.version}",
            )
        row.value = value
        row.version += 1
        row.expires_at = _expiry()
    await session.commit()
    await session.refresh(row)
    return row


async def append(
    session: AsyncSession,
    agent_id: uuid.UUID,
    scope: str | None,
    key: str,
    item: Any,
    reserve_bytes: int | None,
) -> ThreadTranscript:
    """Append one record to a thread's transcript, creating it if absent. Commits."""
    row = await _get_locked(session, agent_id, scope, key)
    if row is None:
        value = [item]
        enforce_thread_cap(key, value, reserve_bytes=reserve_bytes)
        row = ThreadTranscript(
            agent_id=agent_id,
            binding_scope=scope,
            thread_key=key,
            value=value,
            expires_at=_expiry(),
        )
        session.add(row)
    else:
        if not isinstance(row.value, list):
            raise HTTPException(
                status.HTTP_409_CONFLICT, "cannot append: stored value is not a JSON array"
            )
        value = [*row.value, item]
        enforce_thread_cap(key, value, reserve_bytes=reserve_bytes)
        row.value = value
        row.version += 1
        row.expires_at = _expiry()
    await session.commit()
    await session.refresh(row)
    return row


async def remove(
    session: AsyncSession,
    agent_id: uuid.UUID,
    scope: str | None,
    key: str,
    expected_version: int | None,
) -> bool:
    """Delete a thread's transcript. With ``expected_version`` a moved or
    missing row is a 409 (#2820). Commits. Returns whether a transcript row
    existed to delete."""
    row = await get(session, agent_id, scope, key)
    if expected_version is None:
        if row is not None:
            await session.delete(row)
        await _delete_pre_identity(session, agent_id, scope, [key])
        # The thread ends even when only its files exist yet (ADR 0205).
        await thread_attachments.delete_for(session, agent_id, scope, [key])
        await session.commit()
        return row is not None
    stored = row.version if row is not None else None
    deleted = None
    if row is not None and stored == expected_version:
        deleted = await session.scalar(
            delete(ThreadTranscript)
            .where(ThreadTranscript.id == row.id, ThreadTranscript.version == expected_version)
            .returning(ThreadTranscript.id)
        )
        if deleted is not None:
            await _delete_pre_identity(session, agent_id, scope, [key])
            await thread_attachments.delete_for(session, agent_id, scope, [key])
        await session.commit()
    if deleted is None:
        if stored is None:
            found = "entry does not exist"
        elif stored != expected_version:
            found = f"stored {stored}"
        else:
            found = "entry changed during the delete"
        raise HTTPException(
            status.HTTP_409_CONFLICT, f"version mismatch: expected {expected_version}, {found}"
        )
    return True


async def list_threads(
    session: AsyncSession, agent_id: uuid.UUID, scope: str | None
) -> list[ThreadTranscript]:
    rows = await session.scalars(
        select(ThreadTranscript)
        .where(
            ThreadTranscript.agent_id == agent_id,
            ThreadTranscript.binding_scope == scope,
            _live(),
        )
        .order_by(ThreadTranscript.thread_key)
    )
    return list(rows)


async def summary(
    session: AsyncSession, agent_id: uuid.UUID, scope: str | None
) -> tuple[int, Any] | None:
    """The thread count and latest write, for the namespace listing."""
    count, last = (
        await session.execute(
            select(func.count(), func.max(ThreadTranscript.updated_at)).where(
                ThreadTranscript.agent_id == agent_id,
                ThreadTranscript.binding_scope == scope,
                _live(),
            )
        )
    ).one()
    if not count:
        return None
    return int(count), last


async def expire_for_work_item(session: AsyncSession, work_item: WorkItem) -> None:
    """Delete the transcript of a WorkItem that just became terminal.

    The caller owns the transaction, so the delete commits or rolls back with
    the terminal transition itself. ``WorkItem.conversation_id`` is the worker's
    scoped thread key, the same key the runner's history ref names.
    """
    await session.execute(
        delete(ThreadTranscript).where(
            ThreadTranscript.agent_id == work_item.agent_id,
            ThreadTranscript.binding_scope.is_(None),
            ThreadTranscript.thread_key == work_item.conversation_id,
        )
    )
    await _delete_pre_identity(session, work_item.agent_id, None, [work_item.conversation_id])
    await thread_attachments.delete_for(
        session, work_item.agent_id, None, [work_item.conversation_id]
    )
