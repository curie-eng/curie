"""API-owned metadata and compare-and-set integrity binding (ADR 0244)."""

from __future__ import annotations

import os
import uuid
from datetime import timedelta
from typing import Any

from fastapi import HTTPException
from sqlalchemy import delete, exists, func, or_, select, text, update
from sqlalchemy.ext.asyncio import AsyncSession

from ..config import get_settings
from ..models import AttachmentCatalog, AttachmentReference, ThreadAttachmentRef, ThreadTranscript
from ..schemas.attachment_access import FileDescriptor, Registration
from ..thread_attachments import list_refs


def refused(code: str, status: int = 409) -> HTTPException:
    return HTTPException(
        status, {"code": f"attachment_access.{code}"}, headers={"Cache-Control": "no-store"}
    )


def scope(agent: uuid.UUID, thread: str) -> tuple[Any, ...]:
    return (
        AttachmentReference.agent_id == agent,
        AttachmentReference.binding_scope.is_(None),
        AttachmentReference.thread_key == thread,
    )


async def live_rows(db: AsyncSession, agent: uuid.UUID, thread: str) -> list[AttachmentReference]:
    transcript = await db.scalar(
        select(ThreadTranscript)
        .where(
            ThreadTranscript.agent_id == agent,
            ThreadTranscript.binding_scope.is_(None),
            ThreadTranscript.thread_key == thread,
        )
        .execution_options(populate_existing=True)
    )
    now = (await db.execute(select(func.now()))).scalar_one()
    if (
        transcript is not None
        and transcript.expires_at is not None
        and transcript.expires_at <= now
    ):
        return []
    query = select(AttachmentReference).where(*scope(agent, thread))
    if transcript is None:
        query = query.where(AttachmentReference.expires_at > func.now())
    return list(
        await db.scalars(
            query.order_by(AttachmentReference.seq).execution_options(populate_existing=True)
        )
    )


async def live_catalog(db: AsyncSession, agent: uuid.UUID, thread: str) -> AttachmentCatalog | None:
    transcript = (
        ThreadTranscript.agent_id == agent,
        ThreadTranscript.binding_scope.is_(None),
        ThreadTranscript.thread_key == thread,
    )
    live = exists().where(
        *transcript,
        or_(ThreadTranscript.expires_at.is_(None), ThreadTranscript.expires_at > func.now()),
    )
    expired = exists().where(*transcript, ThreadTranscript.expires_at <= func.now())
    result = await db.scalar(
        select(AttachmentCatalog)
        .where(
            AttachmentCatalog.agent_id == agent,
            AttachmentCatalog.thread_key == thread,
            ~expired,
            or_(AttachmentCatalog.expires_at > func.now(), live),
        )
        .execution_options(populate_existing=True)
    )

    return result if isinstance(result, AttachmentCatalog) else None


def safe_leaf(name: str, taken: set[str]) -> str:
    leaf = name.replace("\\", "/").rsplit("/", 1)[-1].strip()
    leaf = "".join(ch for ch in leaf if ord(ch) >= 32 and not 127 <= ord(ch) <= 159)
    if not leaf or leaf in {".", ".."}:
        leaf = "attachment"
    if leaf.startswith("."):
        leaf = "_" + leaf[1:]
    # Reserve suffix space in bytes, preserving a short extension when possible.
    stem, extension = os.path.splitext(leaf)
    extension = extension.encode()[:32].decode("utf-8", errors="ignore")
    stem = stem.encode()[: 220 - len(extension.encode())].decode("utf-8", errors="ignore")
    candidate, counter = stem + extension, 2
    while candidate in taken:
        candidate = f"{stem}-{counter}{extension}"
        counter += 1
    taken.add(candidate)
    return candidate


async def register(db: AsyncSession, data: Registration) -> int:
    # Reuse the legacy importer before taking our lock; it owns a separate
    # transaction. Only verified live legacy rows are copied, never changed.
    legacy = await list_refs(db, data.agent_id, None, data.thread_key)
    await db.execute(
        text("SELECT pg_advisory_xact_lock(hashtextextended(:scope, 0))"),
        {"scope": f"attachment_catalog:{data.agent_id}:{data.thread_key}"},
    )
    ended = await db.scalar(
        delete(ThreadTranscript)
        .where(
            ThreadTranscript.agent_id == data.agent_id,
            ThreadTranscript.binding_scope.is_(None),
            ThreadTranscript.thread_key == data.thread_key,
            ThreadTranscript.expires_at <= func.now(),
        )
        .returning(ThreadTranscript.id)
    )
    if ended is not None:
        await db.execute(delete(AttachmentReference).where(*scope(data.agent_id, data.thread_key)))
        await db.execute(
            delete(ThreadAttachmentRef).where(
                ThreadAttachmentRef.agent_id == data.agent_id,
                ThreadAttachmentRef.binding_scope.is_(None),
                ThreadAttachmentRef.thread_key == data.thread_key,
            )
        )
    if ended is not None:
        await db.execute(
            delete(AttachmentCatalog).where(
                AttachmentCatalog.agent_id == data.agent_id,
                AttachmentCatalog.thread_key == data.thread_key,
            )
        )
    incarnation = await live_catalog(db, data.agent_id, data.thread_key)
    expiry = func.now() + timedelta(seconds=get_settings().transcript_idle_ttl_seconds)
    if incarnation is None:
        await db.execute(
            delete(AttachmentCatalog).where(
                AttachmentCatalog.agent_id == data.agent_id,
                AttachmentCatalog.thread_key == data.thread_key,
            )
        )
        await db.execute(delete(AttachmentReference).where(*scope(data.agent_id, data.thread_key)))
        db.add(
            AttachmentCatalog(agent_id=data.agent_id, thread_key=data.thread_key, expires_at=expiry)
        )
    else:
        await db.execute(
            update(AttachmentCatalog)
            .where(AttachmentCatalog.id == incarnation.id)
            .values(expires_at=expiry)
        )
    current = await live_rows(db, data.agent_id, data.thread_key)
    live_ids = [row.id for row in current]
    await db.execute(
        delete(AttachmentReference).where(
            *scope(data.agent_id, data.thread_key), AttachmentReference.id.not_in(live_ids)
        )
    )
    existing = {(r.event_id, r.file_id): r for r in current}
    taken = {r.disk_name for r in current}
    expires = func.now() + timedelta(seconds=get_settings().transcript_idle_ttl_seconds)
    appended = 0
    for old in legacy:
        identity = (old.event_id, old.file_id)
        if identity in existing:
            continue
        if (
            not old.disk_name
            or "/" in old.disk_name
            or "\\" in old.disk_name
            or len(old.disk_name.encode()) > 255
            or old.disk_name.startswith(".")
            or any(ord(c) < 32 or 127 <= ord(c) <= 159 for c in old.disk_name)
        ):
            raise refused("legacy_name_invalid")
        if old.disk_name in taken:
            raise refused("legacy_name_conflict")
        copied = AttachmentReference(
            agent_id=data.agent_id,
            binding_scope=None,
            thread_key=data.thread_key,
            event_id=old.event_id,
            file_id=old.file_id,
            ordinal=old.ordinal,
            name=old.name,
            disk_name=old.disk_name,
            mime_type=old.mime_type,
            size_bytes=old.size_bytes,
            sha256=old.sha256.lower(),
            route_kind=old.route_kind,
            route_adapter=old.route_adapter,
            route_identity=old.route_identity,
            expires_at=expires,
        )
        db.add(copied)
        existing[identity] = copied
        taken.add(old.disk_name)
    for ref in data.refs:
        identity = (data.event_id, ref.file_id)
        prior = existing.get(identity)
        if prior is not None:
            if (
                prior.ordinal,
                prior.name,
                prior.route_kind,
                prior.route_adapter,
                prior.route_identity,
            ) != (
                ref.ordinal,
                ref.name,
                ref.route_kind,
                ref.route_adapter,
                ref.route_identity,
            ):
                raise refused("reference_changed")
            continue
        row = AttachmentReference(
            agent_id=data.agent_id,
            binding_scope=None,
            thread_key=data.thread_key,
            event_id=data.event_id,
            disk_name=safe_leaf(ref.name, taken),
            expires_at=expires,
            **ref.model_dump(),
        )
        db.add(row)
        existing[identity] = row
        appended += 1
    if len(existing) > get_settings().thread_attachment_max_refs:
        raise refused("catalog_full", 413)
    await db.flush()
    await db.commit()
    return appended


async def selected(
    db: AsyncSession, agent: uuid.UUID, thread: str, ref_id: uuid.UUID
) -> AttachmentReference:
    for row in await live_rows(db, agent, thread):
        if row.id == ref_id:
            return row
    raise refused("reference_unavailable", 404)


def descriptor(row: AttachmentReference, event: str) -> FileDescriptor:
    return FileDescriptor(
        id=str(row.id),
        name=row.disk_name,
        mime_type=(
            row.mime_type
            if row.mime_type
            and row.mime_type.strip()
            and not any(ord(c) < 32 or 127 <= ord(c) <= 159 for c in row.mime_type)
            else None
        ),
        size_bytes=row.size_bytes,
        sha256=row.sha256,
        current=row.event_id == event,
    )


async def bind_integrity(
    db: AsyncSession, row: AttachmentReference, *, version: int, digest: str, size: int
) -> None:
    if row.sha256 is None:
        bound = await db.scalar(
            update(AttachmentReference)
            .where(
                AttachmentReference.id == row.id,
                AttachmentReference.version == version,
                AttachmentReference.sha256.is_(None),
            )
            .values(sha256=digest, size_bytes=size, version=version + 1)
            .returning(AttachmentReference.id)
        )
        if bound is not None:
            await db.flush()
            return
    await db.refresh(row)
    if row.sha256 != digest or row.size_bytes != size:
        raise refused("integrity_conflict")
