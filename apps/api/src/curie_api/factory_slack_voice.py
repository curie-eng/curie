"""Slack source authentication, draft delivery and confirmed factory feedback."""

from __future__ import annotations

import asyncio
import hashlib
import logging
from collections.abc import AsyncIterator
from contextlib import suppress
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TypedDict
from uuid import UUID, uuid5

import httpx
from curie_channel_gateway.read_transport import (
    GatewayClient,
    SlackVoiceReadError,
    post_voice_draft,
    read_voice_clip,
)
from curie_internal.slack_mention_receipt import (
    MalformedMentionReceiptError,
    SlackMentionReceipt,
    read_receipt,
    read_voice_source_receipt,
    voice_reply_text,
)
from pydantic import ValidationError
from redis.asyncio import Redis
from redis.exceptions import RedisError
from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from .config import Settings
from .factory_confirmed_steering import (
    ConfirmedText,
    RefusedSteering,
    SlackSteeringSource,
    SteeringDecision,
)
from .factory_slack import (
    CodeHostUnavailable,
    SlackVoiceAuthorization,
    admit_confirmed_text,
    authorize_voice_source,
)
from .factory_voice import build_voice_draft_service
from .factory_voice_contract import (
    Actor,
    Confirmed,
    CurrentSource,
    Draft,
    EditAccepted,
    Pending,
    Refused,
    SlackThread,
    VoiceDraftService,
    VoiceMedia,
    VoiceSource,
    parse_voice_reply,
)
from .identities import gateway_client
from .identity.service import resolve_principal
from .identity.slack import SlackEvidence
from .models import DEFAULT_TENANT_ID, ChannelIdentity, ProviderInstallation
from .schemas.workitems import FactorySlackSteerIn, FactorySlackVoiceOut

logger = logging.getLogger(__name__)


class VoiceUnavailable(Exception):
    """A source dependency could not answer. Retry with the same receipt."""


def _request(receipt: SlackMentionReceipt) -> FactorySlackSteerIn:
    return FactorySlackSteerIn(
        delivery_id=receipt.delivery_id,
        channel_identity=receipt.channel_identity,
        channel=receipt.channel,
        root_ts=receipt.root_ts,
        author=receipt.user_id,
        receipt=receipt.model_dump(mode="json"),
    )


def verified_request(
    receipt: SlackMentionReceipt | None, data: FactorySlackSteerIn
) -> SlackEvidence | None:
    if (
        receipt is None
        or receipt.version not in (1, 3)
        or receipt.message_ts == receipt.root_ts
        or data != _request(receipt)
    ):
        return None
    try:
        evidence = SlackEvidence.model_validate(receipt.evidence)
    except ValidationError:
        return None
    if evidence.delivery != "app_mention" or evidence.user_id != receipt.user_id:
        return None
    return evidence


def _audience(receipt: SlackMentionReceipt) -> SlackThread | None:
    if receipt.team_id is None or receipt.api_app_id is None:
        return None
    return SlackThread(
        channel_identity=receipt.channel_identity,
        team_id=receipt.team_id,
        app_id=receipt.api_app_id,
        channel_id=receipt.channel,
        root_ts=receipt.root_ts,
    )


def delivery_message(
    source: VoiceSource, draft_id: UUID, outcome: Draft | Refused, now: datetime
) -> str | Refused:
    if isinstance(outcome, Refused):
        return f"Voice feedback refused ({outcome.reason})."
    if draft_id != outcome.draft_id:
        return Refused(reason="invalid_source")
    if source.audience != outcome.audience:
        return Refused(reason="forbidden")
    if outcome.expires_at <= now:
        return Refused(reason="expired")
    code = outcome.code
    return (
        f"Voice transcript\n{outcome.transcript}\n\n"
        f"Mention this app in this thread with one of these replies\n"
        f"confirm {code}\nreject {code}\nedit {code} <replacement text>"
    )


class _ClipFields(TypedDict):
    identity: str
    team_id: str
    app_id: str
    channel: str
    root_ts: str
    message_ts: str
    author: str
    file_id: str
    content_sha256: str


@dataclass(frozen=True)
class _BoundClip:
    source: VoiceSource
    authority: SlackVoiceAuthorization = field(repr=False)
    fields: _ClipFields


async def _chunks(data: bytes) -> AsyncIterator[bytes]:
    yield data


def _refusal(outcome: Refused) -> FactorySlackVoiceOut:
    if outcome.reason == "unavailable":
        raise VoiceUnavailable
    return FactorySlackVoiceOut(outcome="refused", reason=outcome.reason)


@dataclass
class SlackVoiceCallbacks:
    settings: Settings
    sessions: async_sessionmaker[AsyncSession]
    valkey: Redis
    http: httpx.AsyncClient
    gateway: GatewayClient

    async def _authorize(
        self, receipt: SlackMentionReceipt, evidence: SlackEvidence
    ) -> _BoundClip | Refused:
        audience = _audience(receipt)
        if audience is None or receipt.voice_file_id is None or receipt.content_sha256 is None:
            return Refused(reason="invalid_source")
        async with self.sessions() as session:
            authority = await authorize_voice_source(
                session, _request(receipt), evidence, settings=self.settings, client=self.http
            )
        if isinstance(authority, RefusedSteering):
            return Refused(reason="forbidden")
        source = VoiceSource(
            tenant_id=DEFAULT_TENANT_ID,
            work_item_id=authority.work_item_id,
            audience=audience,
            message_id=receipt.message_ts,
            actor=Actor(provider_user_id=receipt.user_id, principal_id=authority.principal_id),
            attachment_id=receipt.voice_file_id,
            content_sha256=receipt.content_sha256,
        )
        fields = _ClipFields(
            identity=audience.channel_identity,
            team_id=audience.team_id,
            app_id=audience.app_id,
            channel=audience.channel_id,
            root_ts=audience.root_ts,
            message_ts=source.message_id,
            author=source.actor.provider_user_id,
            file_id=source.attachment_id,
            content_sha256=source.content_sha256,
        )
        return _BoundClip(source, authority, fields)

    async def _current(self, source: VoiceSource) -> _BoundClip | Refused:
        audience = source.audience
        if not isinstance(audience, SlackThread):
            return Refused(reason="invalid_source")
        try:
            stored = await read_voice_source_receipt(
                self.valkey,
                channel_identity=audience.channel_identity,
                team_id=audience.team_id,
                app_id=audience.app_id,
                channel=audience.channel_id,
                root_ts=audience.root_ts,
                message_ts=source.message_id,
            )
        except MalformedMentionReceiptError:
            return Refused(reason="invalid_source")
        if stored is None or stored.version != 3:
            return Refused(reason="invalid_source")
        evidence = verified_request(stored, _request(stored))
        if evidence is None:
            return Refused(reason="invalid_source")
        bound = await self._authorize(stored, evidence)
        if isinstance(bound, _BoundClip) and bound.source != source:
            return Refused(reason="source_changed")
        return bound

    async def _read(self, bound: _BoundClip) -> tuple[str, bytes] | Refused:
        try:
            return await read_voice_clip(self.gateway, **bound.fields)
        except SlackVoiceReadError as exc:
            return Refused(reason=exc.reason)

    async def media(self, source: VoiceSource) -> VoiceMedia | Refused:
        bound = await self._current(source)
        if isinstance(bound, Refused):
            return bound
        return await self._media(bound)

    async def _media(self, bound: _BoundClip) -> VoiceMedia | Refused:
        clip = await self._read(bound)
        if isinstance(clip, Refused):
            return clip
        media_type, data = clip
        return VoiceMedia(declared_type=media_type, chunks=_chunks(data))

    async def deliver(self, source: VoiceSource, draft_id: UUID, outcome: Draft | Refused) -> None:
        bound = await self._current(source)
        if isinstance(bound, Refused):
            raise VoiceUnavailable
        message = delivery_message(source, draft_id, outcome, datetime.now(UTC))
        if isinstance(message, Refused):
            raise VoiceUnavailable
        if isinstance(outcome, Draft):
            key, deadline = f"draft:{outcome.version}", outcome.expires_at
        else:
            key, deadline = f"refused:{outcome.reason}", None
        await post_voice_draft(
            self.gateway,
            **bound.fields,
            text=message,
            delivery_id=uuid5(draft_id, key),
            expires_at=deadline,
        )


@dataclass
class SlackVoice(SlackVoiceCallbacks):
    service: VoiceDraftService

    async def handle(self, data: FactorySlackSteerIn) -> FactorySlackVoiceOut:
        try:
            receipt = await read_receipt(self.valkey, data.delivery_id)
        except MalformedMentionReceiptError:
            receipt = None
        evidence = verified_request(receipt, data)
        if receipt is None or evidence is None:
            return _refusal(Refused(reason="invalid_source"))
        if receipt.version == 3:
            bound = await self._authorize(receipt, evidence)
            if isinstance(bound, Refused):
                return _refusal(bound)
            media = await self._media(bound)
            if isinstance(media, Refused):
                return _refusal(media)
            result = await self.service.submit(bound.source, media)
            if isinstance(result, Pending):
                return FactorySlackVoiceOut(outcome="pending")
            return _refusal(result)
        audience = _audience(receipt)
        if audience is None or receipt.bot_user_id is None:
            return _refusal(Refused(reason="invalid_source"))
        body = voice_reply_text(receipt.body, receipt.bot_user_id)
        parsed = parse_voice_reply(body) if body is not None else None
        if parsed is None:
            return _refusal(Refused(reason="invalid_source"))
        if isinstance(parsed, Refused):
            return _refusal(parsed)
        actor = await self._decision_actor(receipt, evidence)
        if actor is None:
            return _refusal(Refused(reason="forbidden"))
        feedback = _Feedback(self, receipt, audience, actor)
        decision = await self.service.decide(
            code=parsed.code,
            tenant_id=DEFAULT_TENANT_ID,
            actor=actor,
            audience=audience,
            action=parsed.action,
            feedback=feedback,
        )
        if isinstance(decision, Refused):
            return _refusal(decision)
        if isinstance(decision, EditAccepted):
            return FactorySlackVoiceOut(outcome="draft")
        if isinstance(decision, Confirmed):
            return FactorySlackVoiceOut(
                outcome="confirmed", disposition=decision.disposition, replayed=decision.replayed
            )
        return FactorySlackVoiceOut(outcome="rejected")

    async def _decision_actor(
        self, receipt: SlackMentionReceipt, evidence: SlackEvidence
    ) -> Actor | None:
        async with self.sessions() as session:
            resolution = await resolve_principal(
                session,
                tenant_id=DEFAULT_TENANT_ID,
                provider="slack",
                channel_identity=receipt.channel_identity,
                slack=evidence,
            )
            if resolution.status != "resolved" or resolution.principal_id is None:
                return None
            team = await session.scalar(
                select(ProviderInstallation.external_account_id)
                .join(
                    ChannelIdentity,
                    ChannelIdentity.provider_installation_id == ProviderInstallation.id,
                )
                .where(ChannelIdentity.id == resolution.channel_identity_id)
            )
        if team != receipt.team_id:
            return None
        return Actor(provider_user_id=receipt.user_id, principal_id=resolution.principal_id)

    async def pump(self) -> None:
        while True:
            try:
                if await self.service.process_pending("slack", self):
                    continue
            except (SQLAlchemyError, RedisError, OSError, CodeHostUnavailable):
                logger.warning("factory Slack voice processing temporarily unavailable")
            await asyncio.sleep(1)


@dataclass
class _Feedback:
    adapter: SlackVoice
    confirmation: SlackMentionReceipt
    audience: SlackThread
    actor: Actor
    bound: _BoundClip | None = None

    async def reread(self, source: VoiceSource) -> CurrentSource | Refused:
        self.bound = None
        if (
            source.actor != self.actor
            or source.audience != self.audience
            or source.message_id == self.confirmation.message_ts
            or self.actor.principal_id is None
        ):
            return Refused(reason="forbidden")
        confirmation = await read_receipt(self.adapter.valkey, self.confirmation.delivery_id)
        if confirmation != self.confirmation:
            return Refused(reason="invalid_source")
        bound = await self.adapter._current(source)
        if isinstance(bound, Refused):
            return bound
        clip = await self.adapter._read(bound)
        if isinstance(clip, Refused):
            return clip
        self.bound = bound
        return CurrentSource(
            source=bound.source,
            media_sha256=hashlib.sha256(clip[1]).hexdigest(),
            steering_source=SlackSteeringSource(
                channel_identity=self.audience.channel_identity,
                channel=self.audience.channel_id,
                root_ts=self.audience.root_ts,
                message_ts=source.message_id,
                file_id=source.attachment_id,
                author=self.actor.provider_user_id,
                principal_id=self.actor.principal_id,
                confirmation_delivery_id=self.confirmation.delivery_id,
            ),
        )

    async def admit(self, session: AsyncSession, feedback: ConfirmedText) -> SteeringDecision:
        if self.bound is None:
            return RefusedSteering(reason="authority_changed")
        return await admit_confirmed_text(
            session,
            feedback,
            settings=self.adapter.settings,
            authority=self.bound.authority,
            confirmation=_request(self.confirmation),
        )


def build_slack_voice(
    settings: Settings,
    sessions: async_sessionmaker[AsyncSession],
    valkey: Redis,
    http: httpx.AsyncClient,
) -> SlackVoice | None:
    if not settings.factory_voice_enabled:
        return None
    gateway = gateway_client(settings, http)
    if gateway is None:
        return None
    return SlackVoice(
        settings,
        sessions,
        valkey,
        http,
        gateway,
        build_voice_draft_service(settings, sessions),
    )


async def stop_pump(task: asyncio.Task[None] | None) -> None:
    if task is not None:
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task
