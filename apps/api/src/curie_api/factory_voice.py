"""Persistent factory voice drafts and the transactional handoff to confirmed steering."""

from __future__ import annotations

import asyncio
import hashlib
from datetime import timedelta
from typing import Literal, cast
from uuid import UUID, uuid4

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from .config import Settings
from .factory_confirmed_steering import AppliedSteering, ConfirmedText, RefusedSteering
from .factory_voice_bindings import (
    SOURCE,
    matches_steering,
    matches_target,
    scope_hashes,
    source_key,
    valid_scope,
    valid_source,
)
from .factory_voice_contract import (
    Action,
    Actor,
    Audience,
    Confirmed,
    CurrentSource,
    Edit,
    EditAccepted,
    Pending,
    Refused,
    Rejected,
    SlackThread,
    VoiceDraftCallbacks,
    VoiceDraftService,
    VoiceFeedbackPort,
    VoiceMedia,
    VoiceReply,
    VoiceSource,
    parse_voice_reply,
)
from .factory_voice_decoder import supports_media_type
from .factory_voice_model import LocalVoiceRecognizer
from .factory_voice_processing import (
    Surface,
    VoiceRecognizer,
    database_now,
    deadline_passed,
    process_one,
    read_media,
    storage_available,
    terminal,
    valid_text,
)
from .factory_voice_processing import (
    expire_voice_drafts as expire_voice_drafts,
)
from .models import FactoryVoiceDraft


def _terminal_refusal(row: FactoryVoiceDraft) -> Refused:
    if row.status == "changed" or row.refusal == "source_changed":
        return Refused(reason="source_changed")
    if row.status == "expired":
        return Refused(reason="expired")
    return Refused(reason="closed")


def _scope_and_version(
    row: FactoryVoiceDraft | None, scope: tuple[str, str, str], version: int, action: Action
) -> Confirmed | Refused | None:
    if row is None:
        return Refused(reason="not_found")
    if scope != (row.tenant_hash, row.actor_hash, row.audience_hash):
        return Refused(reason="forbidden")
    if row.status == "confirmed" and action == "confirm":
        if row.version != version:
            return Refused(reason="stale_version")
        assert row.admission_id is not None and row.disposition in ("queued", "applied")
        return Confirmed(
            admission_id=row.admission_id,
            disposition=cast(Literal["queued", "applied"], row.disposition),
            replayed=True,
        )
    if row.source is None or row.refusal is not None:
        return _terminal_refusal(row)
    if row.version != version:
        return Refused(reason="stale_version")
    if row.status != "ready":
        return Refused(reason="unavailable")
    if isinstance(action, Edit) and (not valid_text(action.text) or version == 999999999):
        return Refused(reason="invalid_text")
    return None


async def _admit(
    session: AsyncSession,
    row: FactoryVoiceDraft,
    current: CurrentSource,
    feedback: VoiceFeedbackPort,
) -> Confirmed | Refused:
    assert row.transcript is not None
    if not await matches_target(session, current.source, row.transcript):
        return Refused(reason="target_mismatch")
    if deadline_passed(row, await database_now(session)):
        return Refused(reason="expired")
    command = ConfirmedText(
        draft_id=row.id,
        work_item_id=current.source.work_item_id,
        source=current.steering_source,
        source_content_sha256=current.source.content_sha256,
        text=row.transcript,
    )
    savepoint = await session.begin_nested()
    try:
        async with asyncio.timeout(30):
            decision = await feedback.admit(session, command)
            await session.flush()
    except Exception:  # noqa: BLE001 - the adapter may raise an infrastructure error with private data
        await savepoint.rollback()
        return Refused(reason="unavailable")
    if row.expires_at <= await database_now(session):
        await savepoint.rollback()
        terminal(row, "expired")
        return Refused(reason="expired")
    if isinstance(decision, RefusedSteering) or decision.steering_id != command.steering_id:
        await savepoint.rollback()
        if isinstance(decision, RefusedSteering):
            if decision.reason == "source_changed":
                terminal(row, "changed")
                return Refused(reason="source_changed")
            if decision.reason == "target_mismatch":
                return Refused(reason="target_mismatch")
            if decision.reason == "unavailable":
                return Refused(reason="unavailable")
        return Refused(reason="admission_refused")
    await savepoint.commit()
    disposition: Literal["queued", "applied"] = (
        "applied" if isinstance(decision, AppliedSteering) else "queued"
    )
    row.admission_id = decision.steering_id
    row.disposition = disposition
    terminal(row, "confirmed")
    return Confirmed(
        admission_id=decision.steering_id, disposition=disposition, replayed=decision.replayed
    )


class _VoiceDraftService:
    def __init__(
        self,
        settings: Settings,
        sessions: async_sessionmaker[AsyncSession],
        recognizer: VoiceRecognizer,
    ) -> None:
        self.settings = settings
        self.sessions = sessions
        self.recognizer = recognizer

    async def submit(self, source: VoiceSource, media: VoiceMedia) -> Pending | Refused:
        if not self.settings.factory_voice_enabled:
            return Refused(reason="disabled")
        if not valid_source(source):
            return Refused(reason="invalid_source")
        try:
            async with self.sessions() as session:
                if not await storage_available(session):
                    return Refused(reason="unavailable")
            if not await self.recognizer.available():
                return Refused(reason="disabled")
            try:
                async with asyncio.timeout(30):
                    data = await read_media(media)
            except Exception:  # noqa: BLE001 - partial provider reads do not establish changed bytes
                data = Refused(reason="unavailable")
            async with self.sessions.begin() as session:
                return await self._reserve(session, source, media.declared_type, data)
        except SQLAlchemyError:
            return Refused(reason="unavailable")

    async def _reserve(
        self, session: AsyncSession, source: VoiceSource, declared_type: str, data: bytes | Refused
    ) -> Pending | Refused:
        key = source_key(source)
        facts = SOURCE.dump_python(source, mode="json")
        digest = hashlib.sha256(data).hexdigest() if isinstance(data, bytes) else None
        invalid = data if isinstance(data, Refused) else None
        if invalid is None and (not data or not supports_media_type(declared_type)):
            invalid = Refused(reason="invalid_media")
        scope = scope_hashes(source.tenant_id, source.actor, source.audience)
        # A collision on the display prefix needs a fresh ID. A source collision reuses its row.
        while True:
            if invalid is None:
                draft_id = uuid4()
                now = await database_now(session)
                expires_at = now + timedelta(seconds=self.settings.factory_voice_draft_ttl_seconds)
                inserted = await session.scalar(
                    insert(FactoryVoiceDraft)
                    .values(
                        id=draft_id,
                        code_prefix=draft_id.hex[:12],
                        source_key=key,
                        source=facts,
                        tenant_hash=scope[0],
                        actor_hash=scope[1],
                        audience_hash=scope[2],
                        surface="slack" if isinstance(source.audience, SlackThread) else "github",
                        media_sha256=digest,
                        version=1,
                        status="recognizing",
                        created_at=now,
                        expires_at=expires_at,
                    )
                    .on_conflict_do_nothing()
                    .returning(FactoryVoiceDraft.id)
                )
                if inserted is not None:
                    return Pending(draft_id=inserted, expires_at=expires_at)
            row = await session.scalar(
                select(FactoryVoiceDraft)
                .where(FactoryVoiceDraft.source_key == key)
                .with_for_update()
            )
            if row is not None:
                break
            if invalid is not None:
                return invalid
        if deadline_passed(row, await database_now(session)) or row.source is None or row.refusal:
            return _terminal_refusal(row)
        if (
            facts != row.source
            or (digest is not None and digest != row.media_sha256)
            or (invalid is not None and invalid.reason == "media_limit")
        ):
            terminal(row, "changed")
            return Refused(reason="source_changed")
        return invalid or Pending(draft_id=row.id, expires_at=row.expires_at)

    async def process_pending(self, surface: Surface, callbacks: VoiceDraftCallbacks) -> bool:
        if not self.settings.factory_voice_enabled:
            return False
        return await process_one(self.sessions, surface, callbacks, self.recognizer)

    async def decide(
        self,
        *,
        code: str,
        tenant_id: UUID,
        actor: Actor,
        audience: Audience,
        action: Action,
        feedback: VoiceFeedbackPort,
    ) -> EditAccepted | Confirmed | Rejected | Refused:
        if not self.settings.factory_voice_enabled:
            return Refused(reason="disabled")
        if not isinstance(code, str) or len(code) > 22:
            return Refused(reason="not_found")
        if not isinstance(parse_voice_reply("confirm " + code), VoiceReply):
            return Refused(reason="not_found")
        if not valid_scope(tenant_id, actor, audience):
            return Refused(reason="forbidden")
        if not isinstance(action, Edit) and action not in ("confirm", "reject"):
            return Refused(reason="invalid_text")
        prefix, version_text = code.split(".")
        version = int(version_text)
        scope = scope_hashes(tenant_id, actor, audience)
        query = select(FactoryVoiceDraft).where(FactoryVoiceDraft.code_prefix == prefix)
        try:
            async with self.sessions() as session:
                if not await storage_available(session):
                    return Refused(reason="unavailable")
                row = await session.scalar(query)
                guarded = _scope_and_version(row, scope, version, action)
                if row is None:
                    return Refused(reason="not_found")
                if scope != (row.tenant_hash, row.actor_hash, row.audience_hash):
                    return Refused(reason="forbidden")
                expired = row.source is not None and row.expires_at <= await database_now(session)
                if guarded is not None and not expired:
                    return guarded
                source = SOURCE.validate_python(row.source)
            if expired:
                current: CurrentSource | Refused = Refused(reason="expired")
            else:
                try:
                    async with asyncio.timeout(30):
                        current = await feedback.reread(source)
                except Exception:  # noqa: BLE001 - no provider body or credential in the response
                    current = Refused(reason="unavailable")
            async with self.sessions.begin() as session:
                row = await session.scalar(query.with_for_update())
                if row is not None:
                    deadline_passed(row, await database_now(session))
                guarded = _scope_and_version(row, scope, version, action)
                if guarded is not None:
                    return guarded
                assert row is not None
                if isinstance(current, Refused):
                    if current.reason == "source_changed":
                        terminal(row, "changed")
                    return current
                if (
                    not valid_source(current.source)
                    or current.source != source
                    or row.source != SOURCE.dump_python(source, mode="json")
                    or current.media_sha256 != row.media_sha256
                ):
                    terminal(row, "changed")
                    return Refused(reason="source_changed")
                if not matches_steering(source, current.steering_source):
                    return Refused(reason="forbidden")
                if isinstance(action, Edit):
                    row.transcript = action.text
                    row.version += 1
                    row.status = "delivering"
                    row.retry_at = None
                    return EditAccepted(draft_id=row.id)
                if action == "reject":
                    terminal(row, "rejected")
                    return Rejected(draft_id=row.id)
                return await _admit(session, row, current, feedback)
        except SQLAlchemyError:
            return Refused(reason="unavailable")


def build_voice_draft_service(
    settings: Settings,
    sessions: async_sessionmaker[AsyncSession],
    *,
    recognizer: VoiceRecognizer | None = None,
) -> VoiceDraftService:
    return _VoiceDraftService(
        settings, sessions, recognizer or LocalVoiceRecognizer(settings.factory_voice_model_path)
    )
