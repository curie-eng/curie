"""The thread attachment ledger (ADR 0205 decisions 1 and 2, #4079).

One row per file a thread's agent was given, keyed exactly like the transcript
(agent, binding scope, thread key). Only the worker writes it, through the
internal routes in ``routers/thread_attachments.py``; no state route exposes
it. A row records the channel's file id, names, digest, arrival order and the
route it came from, never an endpoint, a URL or bytes.

Lifetime is the transcript's, applied lazily like the transcript's own:

* a read of a thread whose transcript has expired returns nothing;
* a read of a thread with a live transcript returns every row and moves their
  ``expires_at`` to the transcript's;
* a read of a thread with no transcript yet (the append lands right after
  install, before the runner's first transcript write) returns the rows whose
  own ``expires_at`` is still ahead. An append sets it to now plus
  ``transcript_idle_ttl_seconds``.

Removal rides on ``transcripts``: ``remove``, ``expire_for_work_item`` and the
idle sweep call ``delete_for`` / ``sweep_orphans`` in their own transaction,
and the agent foreign key cascades. A named non-Slack route's pre-identity key
(ADR-0168 decision 4) is copied forward on read and append and deleted with the
new key, the same way the transcript is.
"""

from __future__ import annotations

import uuid
from collections.abc import Mapping, Sequence
from datetime import timedelta
from typing import Any

from sqlalchemy import Text, and_, delete, exists, func, insert, literal, or_, select, text, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from .config import get_settings
from .models import ThreadAttachmentRef, ThreadTranscript
from .threadkeys import pre_identity_thread_key_for

REF_FIELDS: tuple[str, ...] = (
    "file_id",
    "ordinal",
    "name",
    "disk_name",
    "mime_type",
    "size_bytes",
    "sha256",
    "route_kind",
    "route_adapter",
    "route_identity",
)

_LOCK_SQL = text(
    "SELECT pg_advisory_xact_lock(hashtextextended("
    "'thread_attachment:' || CAST(:agent_id AS text) || ':' || coalesce(:scope, '') "
    "|| ':' || :key, 0))"
)


async def _lock(session: AsyncSession, agent_id: uuid.UUID, scope: str | None, key: str) -> None:
    """Serialize ledger writes to one thread for the rest of the transaction."""

    await session.execute(_LOCK_SQL, {"agent_id": str(agent_id), "scope": scope, "key": key})


class NameMismatch(Exception):
    """A redelivered (event, file) names a different ``disk_name`` than recorded.

    A disk name is fixed when it is recorded (ADR 0205 decision 4), so the
    redelivery is refused rather than swallowed: the worker would otherwise
    tell the agent a path the ledger never holds.
    """

    def __init__(self, file_id: str) -> None:
        super().__init__(file_id)
        self.file_id = file_id


class NameConflict(Exception):
    """A ref's ``disk_name`` is held in the thread by a different (event, file)."""

    def __init__(self, disk_name: str) -> None:
        super().__init__(disk_name)
        self.disk_name = disk_name


class ThreadFull(Exception):
    """The append would take the thread past ``thread_attachment_max_refs``."""

    def __init__(self, limit: int) -> None:
        super().__init__(limit)
        self.limit = limit


def _expiry() -> Any:
    return func.now() + timedelta(seconds=get_settings().transcript_idle_ttl_seconds)


def _thread(agent_id: uuid.UUID, scope: str | None, key: str) -> tuple[Any, ...]:
    return (
        ThreadAttachmentRef.agent_id == agent_id,
        ThreadAttachmentRef.binding_scope == scope,
        ThreadAttachmentRef.thread_key == key,
    )


def _transcript(agent_id: uuid.UUID, scope: str | None, key: str) -> tuple[Any, ...]:
    return (
        ThreadTranscript.agent_id == agent_id,
        ThreadTranscript.binding_scope == scope,
        ThreadTranscript.thread_key == key,
    )


async def _transcript_state(
    session: AsyncSession, agent_id: uuid.UUID, scope: str | None, key: str
) -> tuple[str, Any]:
    """``("absent", None)``, ``("expired", at)`` or ``("live", at)`` for the thread."""

    row = (
        await session.execute(
            select(
                ThreadTranscript.expires_at,
                or_(
                    ThreadTranscript.expires_at.is_(None),
                    ThreadTranscript.expires_at > func.now(),
                ),
            ).where(*_transcript(agent_id, scope, key))
        )
    ).first()
    if row is None:
        return "absent", None
    expires_at, live = row
    return ("live" if live else "expired"), expires_at


def _live_rows(agent_id: uuid.UUID, scope: str | None, key: str, state: str) -> Any:
    """The rows of a thread that a read would return in ``state``."""

    query = select(ThreadAttachmentRef).where(*_thread(agent_id, scope, key))
    if state == "absent":
        query = query.where(ThreadAttachmentRef.expires_at > func.now())
    return query


async def _adopt_pre_identity(
    session: AsyncSession, agent_id: uuid.UUID, scope: str | None, key: str
) -> None:
    """Copy the files a named route recorded under its pre-identity key.

    Mirrors ``transcripts._adopt_pre_identity``: the old rows are read, never
    locked or deleted, so a worker that has not rolled keeps appending there,
    and the old key's own lifetime decides what is live. A row already present
    under ``key`` (same event and file) or whose disk name ``key`` already
    holds is skipped. Does not commit.
    """

    old_key = await pre_identity_thread_key_for(session, agent_id, key)
    if old_key is None:
        return
    state, _ = await _transcript_state(session, agent_id, scope, old_key)
    if state == "expired":
        return
    old = _live_rows(agent_id, scope, old_key, state).subquery()
    columns = ["id", "agent_id", "binding_scope", "thread_key", "event_id", *REF_FIELDS]
    copied = select(
        func.gen_random_uuid(),
        old.c.agent_id,
        old.c.binding_scope,
        literal(key, Text()),
        old.c.event_id,
        *(old.c[field] for field in REF_FIELDS),
        _expiry(),
    ).order_by(old.c.seq)
    await session.execute(
        pg_insert(ThreadAttachmentRef)
        .from_select([*columns, "expires_at"], copied)
        .on_conflict_do_nothing()
    )


async def list_refs(
    session: AsyncSession, agent_id: uuid.UUID, scope: str | None, key: str
) -> list[ThreadAttachmentRef]:
    """The thread's live references in arrival order. Commits.

    Takes the per-thread lock before copying pre-identity rows forward, so the
    copy cannot land between a concurrent append's checks and its insert.
    """

    await _lock(session, agent_id, scope, key)
    await _adopt_pre_identity(session, agent_id, scope, key)
    state, transcript_expires_at = await _transcript_state(session, agent_id, scope, key)
    if state == "expired":
        await session.commit()
        return []
    if state == "live" and transcript_expires_at is not None:
        await session.execute(
            update(ThreadAttachmentRef)
            .where(*_thread(agent_id, scope, key))
            .values(expires_at=transcript_expires_at)
        )
    rows = list(
        await session.scalars(
            _live_rows(agent_id, scope, key, state).order_by(ThreadAttachmentRef.seq)
        )
    )
    await session.commit()
    return rows


async def _settle(session: AsyncSession, agent_id: uuid.UUID, scope: str | None, key: str) -> None:
    """End a dead thread before writing to it.

    An expired transcript reads as absent and the next write deletes it; an
    append is a write, so it deletes the expired transcript and its files here.
    Otherwise the idle sweep would later delete the files this append records.
    With no live transcript, files past their own expiry go too, so a later
    transcript does not bring them back. Does not commit.
    """

    ended = await session.scalar(
        delete(ThreadTranscript)
        .where(*_transcript(agent_id, scope, key), ThreadTranscript.expires_at <= func.now())
        .returning(ThreadTranscript.id)
    )
    if ended is not None:
        await session.execute(delete(ThreadAttachmentRef).where(*_thread(agent_id, scope, key)))
        return
    state, _ = await _transcript_state(session, agent_id, scope, key)
    if state == "absent":
        await session.execute(
            delete(ThreadAttachmentRef).where(
                *_thread(agent_id, scope, key), ThreadAttachmentRef.expires_at <= func.now()
            )
        )


async def append_refs(
    session: AsyncSession,
    agent_id: uuid.UUID,
    scope: str | None,
    key: str,
    event_id: str,
    refs: Sequence[Mapping[str, Any]],
) -> int:
    """Record one turn's files in one transaction. Commits.

    Idempotent per (event_id, file_id): a ref already recorded is not inserted
    again and keeps its place. Raises ``NameMismatch`` when a recorded (or
    repeated) (event, file) arrives with a different ``disk_name``,
    ``NameConflict`` when a ref's
    ``disk_name`` is held by a different (event, file) in the thread or in the
    request, and ``ThreadFull`` when the new rows would pass the cap; in every
    case nothing is stored. Returns the number of rows inserted.
    """

    await _lock(session, agent_id, scope, key)
    await _adopt_pre_identity(session, agent_id, scope, key)
    await _settle(session, agent_id, scope, key)

    existing = (
        await session.execute(
            select(
                ThreadAttachmentRef.event_id,
                ThreadAttachmentRef.file_id,
                ThreadAttachmentRef.disk_name,
            ).where(*_thread(agent_id, scope, key))
        )
    ).all()
    recorded: dict[tuple[str, str], str] = {
        (row.event_id, row.file_id): row.disk_name for row in existing
    }
    holders: dict[str, tuple[str, str]] = {
        row.disk_name: (row.event_id, row.file_id) for row in existing
    }
    fresh: list[Mapping[str, Any]] = []
    for ref in refs:
        owner = (event_id, str(ref["file_id"]))
        disk_name = str(ref["disk_name"])
        stored_name = recorded.get(owner)
        if stored_name is not None and stored_name != disk_name:
            await session.rollback()
            raise NameMismatch(owner[1])
        held = holders.get(disk_name)
        if held is not None and held != owner:
            await session.rollback()
            raise NameConflict(disk_name)
        if owner in recorded:
            continue
        holders[disk_name] = owner
        recorded[owner] = disk_name
        fresh.append(ref)

    cap = get_settings().thread_attachment_max_refs
    if fresh and len(existing) + len(fresh) > cap:
        await session.rollback()
        raise ThreadFull(cap)

    appended = 0
    if fresh:
        expires_at = _expiry()
        inserted = await session.scalars(
            insert(ThreadAttachmentRef)
            .values(
                [
                    {
                        "id": uuid.uuid4(),
                        "agent_id": agent_id,
                        "binding_scope": scope,
                        "thread_key": key,
                        "event_id": event_id,
                        "expires_at": expires_at,
                        **{field: ref.get(field) for field in REF_FIELDS},
                    }
                    for ref in fresh
                ]
            )
            .returning(ThreadAttachmentRef.id)
        )
        appended = len(list(inserted))
        # An append is thread activity: the thread's earlier files live as
        # long as the newest one.
        await session.execute(
            update(ThreadAttachmentRef)
            .where(*_thread(agent_id, scope, key), ThreadAttachmentRef.expires_at < expires_at)
            .values(expires_at=expires_at)
        )
    await session.commit()
    return appended


async def _with_pre_identity(
    session: AsyncSession, agent_id: uuid.UUID, keys: Sequence[str]
) -> list[str]:
    every = list(keys)
    for key in keys:
        old_key = await pre_identity_thread_key_for(session, agent_id, key)
        if old_key is not None:
            every.append(old_key)
    return every


async def delete_for(
    session: AsyncSession, agent_id: uuid.UUID, scope: str | None, keys: Sequence[str]
) -> None:
    """Delete the files of threads whose history just ended, and of their
    pre-identity keys, so the next read cannot copy them back. Does not commit."""

    if not keys:
        return
    every = await _with_pre_identity(session, agent_id, keys)
    await session.execute(
        delete(ThreadAttachmentRef).where(
            ThreadAttachmentRef.agent_id == agent_id,
            ThreadAttachmentRef.binding_scope == scope,
            ThreadAttachmentRef.thread_key.in_(every),
        )
    )


async def sweep_orphans(session: AsyncSession, agent_id: uuid.UUID) -> None:
    """Delete this agent's files past their own expiry with no live transcript.

    A thread whose runner never wrote a transcript has nothing for the
    transcript sweep to find, so its files expire on their own clock. SKIP
    LOCKED, like the transcript sweep. Does not commit.
    """

    live_transcript = exists().where(
        ThreadTranscript.agent_id == ThreadAttachmentRef.agent_id,
        ThreadTranscript.binding_scope.is_not_distinct_from(ThreadAttachmentRef.binding_scope),
        ThreadTranscript.thread_key == ThreadAttachmentRef.thread_key,
        or_(ThreadTranscript.expires_at.is_(None), ThreadTranscript.expires_at > func.now()),
    )
    orphans = (
        select(ThreadAttachmentRef.id)
        .where(
            and_(
                ThreadAttachmentRef.agent_id == agent_id,
                ThreadAttachmentRef.expires_at <= func.now(),
                ~live_transcript,
            )
        )
        .with_for_update(skip_locked=True)
    )
    await session.execute(delete(ThreadAttachmentRef).where(ThreadAttachmentRef.id.in_(orphans)))
