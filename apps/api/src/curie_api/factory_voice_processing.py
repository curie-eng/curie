"""Claim voice recognition or prompt delivery, then run its callbacks outside the transaction."""

from __future__ import annotations

import asyncio
import hashlib
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Literal, Protocol, cast
from uuid import UUID, uuid4

from sqlalchemy import func, or_, select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from .factory_voice_bindings import SOURCE
from .factory_voice_contract import (
    Draft,
    Refusal,
    Refused,
    VoiceDraftCallbacks,
    VoiceMedia,
    VoiceSource,
)
from .factory_voice_decoder import MAX_ENCODED_BYTES, supports_media_type
from .models import FactoryVoiceDraft

type Surface = Literal["slack", "github"]
_PROVIDER_SECONDS = 30
_MAX_TEXT = 65536


class VoiceRecognizer(Protocol):
    async def available(self) -> bool: ...

    async def recognize(self, declared_type: str, data: bytes) -> str | Refused: ...


async def storage_available(session: AsyncSession) -> bool:
    return bool(
        await session.scalar(text("SELECT to_regclass('curie.factory_voice_drafts') IS NOT NULL"))
    )


async def database_now(session: AsyncSession) -> datetime:
    return cast(datetime, (await session.execute(select(func.clock_timestamp()))).scalar_one())


def terminal(row: FactoryVoiceDraft, status: str) -> None:
    row.status = status
    row.source = None
    row.media_sha256 = None
    row.transcript = None
    row.refusal = None
    row.lease_id = None
    row.lease_until = None
    row.retry_at = None


def deadline_passed(row: FactoryVoiceDraft, now: datetime) -> bool:
    if row.source is not None and row.expires_at <= now:
        terminal(row, "expired")
    return row.status == "expired"


async def expire_voice_drafts(session: AsyncSession) -> int:
    if not await storage_available(session):
        return 0
    expired = (
        await session.scalars(
            select(FactoryVoiceDraft)
            .where(
                FactoryVoiceDraft.source.is_not(None), FactoryVoiceDraft.expires_at <= func.now()
            )
            .order_by(FactoryVoiceDraft.expires_at, FactoryVoiceDraft.id)
            .limit(100)
            .with_for_update(skip_locked=True)
        )
    ).all()
    for row in expired:
        terminal(row, "expired")
    return len(expired)


def valid_text(value: str) -> bool:
    return (
        isinstance(value, str)
        and 0 < len(value) <= _MAX_TEXT
        and bool(value.strip())
        and "\0" not in value
        and not any(0xD800 <= ord(ch) <= 0xDFFF for ch in value)
    )


async def read_media(media: VoiceMedia) -> bytes | Refused:
    data = bytearray()
    async for chunk in media.chunks:
        if not isinstance(chunk, bytes):
            return Refused(reason="invalid_media")
        if len(chunk) > MAX_ENCODED_BYTES - len(data):
            return Refused(reason="media_limit")
        data.extend(chunk)
    return bytes(data)


def displayed(row: FactoryVoiceDraft, source: VoiceSource) -> Draft:
    assert row.transcript is not None
    return Draft(
        draft_id=row.id,
        audience=source.audience,
        transcript=row.transcript,
        version=row.version,
        expires_at=row.expires_at,
    )


@dataclass(frozen=True)
class _Claim:
    id: UUID
    lease_id: UUID
    source: VoiceSource
    digest: str | None
    delivery: Draft | Refused | None
    seconds_left: float


async def _claim(sessions: async_sessionmaker[AsyncSession], surface: Surface) -> _Claim | None:
    async with sessions.begin() as session:
        if not await storage_available(session):
            return None
        # Serialize the short check-and-reserve across source-specific pumps and API replicas.
        locked = await session.scalar(text("SELECT pg_try_advisory_xact_lock(4292, 196)"))
        if not locked:
            return None
        now = await database_now(session)
        active = await session.scalar(
            select(FactoryVoiceDraft.id)
            .where(
                FactoryVoiceDraft.status == "recognizing",
                FactoryVoiceDraft.expires_at > now,
                FactoryVoiceDraft.lease_until > now,
            )
            .limit(1)
        )
        stages = ("delivering",) if active else ("delivering", "recognizing")
        row = await session.scalar(
            select(FactoryVoiceDraft)
            .where(
                FactoryVoiceDraft.surface == surface,
                FactoryVoiceDraft.status.in_(stages),
                FactoryVoiceDraft.expires_at > now,
                or_(FactoryVoiceDraft.retry_at.is_(None), FactoryVoiceDraft.retry_at <= now),
                or_(FactoryVoiceDraft.lease_until.is_(None), FactoryVoiceDraft.lease_until <= now),
            )
            .order_by(FactoryVoiceDraft.created_at, FactoryVoiceDraft.id)
            .limit(1)
            .with_for_update(skip_locked=True)
        )
        if row is None:
            return None
        source = SOURCE.validate_python(row.source)
        delivery: Draft | Refused | None = None
        if row.status == "delivering":
            delivery = (
                Refused(reason=cast(Refusal, row.refusal))
                if row.refusal
                else displayed(row, source)
            )
        row.lease_id = uuid4()
        row.lease_until = now + timedelta(seconds=240 if delivery is None else 75)
        return _Claim(
            row.id,
            row.lease_id,
            source,
            row.media_sha256,
            delivery,
            (row.expires_at - now).total_seconds(),
        )


async def _current_media(
    claim: _Claim,
    callbacks: VoiceDraftCallbacks,
) -> tuple[str, bytes] | Refused:
    async with asyncio.timeout(_PROVIDER_SECONDS):
        media = await callbacks.media(claim.source)
        if isinstance(media, Refused):
            return media
        data = await read_media(media)
    if isinstance(data, Refused):
        if data.reason == "media_limit":
            return Refused(reason="source_changed")
        return data
    if hashlib.sha256(data).hexdigest() != claim.digest:
        return Refused(reason="source_changed")
    if not data or not supports_media_type(media.declared_type):
        return Refused(reason="invalid_media")
    return media.declared_type, data


async def _execute(
    sessions: async_sessionmaker[AsyncSession],
    claim: _Claim,
    deadline: float,
    callbacks: VoiceDraftCallbacks,
    recognizer: VoiceRecognizer,
) -> str | Refused | None:
    if not isinstance(claim.delivery, Refused):
        media = await _current_media(claim, callbacks)
        if isinstance(media, Refused):
            return media
        if claim.delivery is None:
            transcript = await recognizer.recognize(*media)
            if isinstance(transcript, str) and not valid_text(transcript):
                return Refused(reason="no_speech" if not transcript.strip() else "invalid_text")
            return transcript
    async with sessions() as session:
        live = await session.scalar(
            select(FactoryVoiceDraft.id).where(
                FactoryVoiceDraft.id == claim.id,
                FactoryVoiceDraft.lease_id == claim.lease_id,
                FactoryVoiceDraft.expires_at > func.clock_timestamp(),
                FactoryVoiceDraft.lease_until > func.clock_timestamp(),
            )
        )
    if live is None or asyncio.get_running_loop().time() >= deadline:
        return Refused(reason="unavailable")
    assert claim.delivery is not None
    async with asyncio.timeout(_PROVIDER_SECONDS):
        await callbacks.deliver(claim.source, claim.id, claim.delivery)
    return None


async def process_one(
    sessions: async_sessionmaker[AsyncSession],
    surface: Surface,
    callbacks: VoiceDraftCallbacks,
    recognizer: VoiceRecognizer,
) -> bool:
    loop = asyncio.get_running_loop()
    started = loop.time()
    claim = await _claim(sessions, surface)
    if claim is None:
        return False
    outcome: str | Refused | None = None
    try:
        # Include claim time so an adapter can never start a delivery with an expired deadline.
        deadline = started + claim.seconds_left
        async with asyncio.timeout_at(deadline):
            outcome = await _execute(sessions, claim, deadline, callbacks, recognizer)
    except Exception:  # noqa: BLE001 - provider errors can contain private source information
        outcome = Refused(reason="unavailable")

    async with sessions.begin() as session:
        row = await session.scalar(
            select(FactoryVoiceDraft)
            .where(FactoryVoiceDraft.id == claim.id, FactoryVoiceDraft.lease_id == claim.lease_id)
            .with_for_update()
        )
        if row is None:
            return True
        now = await database_now(session)
        if deadline_passed(row, now) or row.lease_until is None or row.lease_until <= now:
            return True
        row.lease_id = None
        row.lease_until = None
        if isinstance(outcome, Refused) and outcome.reason == "unavailable":
            row.retry_at = now + timedelta(seconds=5)
        elif isinstance(outcome, Refused):
            row.status = "delivering"
            row.transcript = None
            row.media_sha256 = None
            row.refusal = outcome.reason
            row.retry_at = None
        elif claim.delivery is not None:
            if isinstance(claim.delivery, Refused):
                terminal(row, "changed" if claim.delivery.reason == "source_changed" else "refused")
            else:
                row.status = "ready"
                row.retry_at = None
        else:
            assert outcome is not None
            row.status = "delivering"
            row.transcript = outcome
            row.retry_at = None
    return True
