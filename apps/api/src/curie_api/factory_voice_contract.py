"""API-side contract between factory voice sources and confirmed text admission."""

from __future__ import annotations

from collections.abc import AsyncIterable
from dataclasses import dataclass, field
from datetime import datetime
from typing import Literal, Protocol
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession


@dataclass(frozen=True, kw_only=True)
class SlackThread:
    channel_identity: str
    team_id: str
    app_id: str
    channel_id: str
    root_ts: str


@dataclass(frozen=True, kw_only=True)
class GitHubConversation:
    host: str
    installation_id: int
    repository_id: int
    kind: Literal["issue", "pull_request"]
    number: int


type Audience = SlackThread | GitHubConversation


@dataclass(frozen=True, kw_only=True)
class Actor:
    provider_user_id: str
    principal_id: UUID | None


@dataclass(frozen=True, kw_only=True)
class VoiceSource:
    tenant_id: UUID
    work_item_id: UUID
    audience: Audience
    message_id: str
    actor: Actor
    attachment_id: str
    content_sha256: str


@dataclass(frozen=True, kw_only=True)
class VoiceMedia:
    declared_type: str
    chunks: AsyncIterable[bytes] = field(repr=False)


@dataclass(frozen=True, kw_only=True)
class Draft:
    draft_id: UUID
    audience: Audience
    transcript: str = field(repr=False)
    version: int
    expires_at: datetime


@dataclass(frozen=True, kw_only=True)
class Edit:
    text: str = field(repr=False)


type Action = Literal["confirm", "reject"] | Edit
type Refusal = Literal[
    "disabled",
    "unavailable",
    "invalid_source",
    "source_changed",
    "forbidden",
    "invalid_media",
    "media_limit",
    "no_speech",
    "not_found",
    "expired",
    "closed",
    "stale_version",
    "invalid_text",
    "target_mismatch",
    "admission_refused",
]


@dataclass(frozen=True, kw_only=True)
class Refused:
    reason: Refusal


@dataclass(frozen=True, kw_only=True)
class Confirmed:
    admission_id: UUID
    disposition: Literal["queued", "applied"]
    replayed: bool


@dataclass(frozen=True, kw_only=True)
class Rejected:
    draft_id: UUID


@dataclass(frozen=True, kw_only=True)
class CurrentSource:
    source: VoiceSource
    media_sha256: str


@dataclass(frozen=True, kw_only=True)
class ConfirmedText:
    source: VoiceSource
    draft_id: UUID
    idempotency_key: UUID
    text: str = field(repr=False)


class VoiceFeedbackPort(Protocol):
    async def reread(self, source: VoiceSource) -> CurrentSource | Refused:
        """Read the canonical source, hash its current bytes and check authority."""
        ...

    async def admit(self, session: AsyncSession, feedback: ConfirmedText) -> Confirmed | Refused:
        """Apply current text steering policy inside the supplied transaction."""
        ...


class VoiceDraftService(Protocol):
    async def submit(self, source: VoiceSource, media: VoiceMedia) -> Draft | Refused: ...

    async def decide(
        self,
        *,
        draft_id: UUID,
        version: int,
        tenant_id: UUID,
        actor: Actor,
        audience: Audience,
        action: Action,
        feedback: VoiceFeedbackPort,
    ) -> Draft | Confirmed | Rejected | Refused: ...
