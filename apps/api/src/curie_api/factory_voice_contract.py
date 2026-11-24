"""API-side contract between factory voice sources and confirmed text admission."""

from __future__ import annotations

from collections.abc import AsyncIterable
from dataclasses import dataclass, field
from datetime import datetime
from typing import Literal, Protocol
from uuid import UUID

from curie_internal.factory_voice_reply import VOICE_REPLY
from sqlalchemy.ext.asyncio import AsyncSession

from .factory_confirmed_steering import (
    ConfirmedText,
    GitHubSteeringSource,
    SlackSteeringSource,
    SteeringDecision,
)


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
class Pending:
    draft_id: UUID
    expires_at: datetime


@dataclass(frozen=True, kw_only=True)
class Draft:
    draft_id: UUID
    audience: Audience
    transcript: str = field(repr=False)
    version: int
    expires_at: datetime

    @property
    def code(self) -> str:
        """The unique draft prefix and the version shown in this prompt."""
        return f"{self.draft_id.hex[:12]}.{self.version}"


@dataclass(frozen=True, kw_only=True)
class Edit:
    text: str = field(repr=False)


type Action = Literal["confirm", "reject"] | Edit


@dataclass(frozen=True, kw_only=True)
class VoiceReply:
    code: str
    action: Action


def parse_voice_reply(text: str) -> VoiceReply | Refused | None:
    """Parse the complete reply after the adapter removes its verified mention."""
    match = VOICE_REPLY.fullmatch(text)
    if match is None:
        return None
    verb, code, body = match.groups()
    if verb == "edit":
        return VoiceReply(code=code, action=Edit(text=body or ""))
    if body is not None:
        return Refused(reason="invalid_text")
    if verb == "confirm":
        return VoiceReply(code=code, action="confirm")
    return VoiceReply(code=code, action="reject")


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
class EditAccepted:
    """The updated transcript and code are queued for delivery at the source."""

    draft_id: UUID


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
    steering_source: SlackSteeringSource | GitHubSteeringSource


class VoiceDraftCallbacks(Protocol):
    async def media(self, source: VoiceSource) -> VoiceMedia | Refused:
        """Reopen the exact source under current provider authority."""
        ...

    async def deliver(self, source: VoiceSource, draft_id: UUID, outcome: Draft | Refused) -> None:
        """Show the result only at its verified origin; tolerate repeated delivery."""
        ...


class VoiceFeedbackPort(Protocol):
    async def reread(self, source: VoiceSource) -> CurrentSource | Refused:
        """Read the canonical source, hash its current bytes and check authority."""
        ...

    async def admit(self, session: AsyncSession, feedback: ConfirmedText) -> SteeringDecision:
        """Apply current text steering policy inside the supplied transaction."""
        ...


class VoiceDraftService(Protocol):
    async def submit(self, source: VoiceSource, media: VoiceMedia) -> Pending | Refused: ...

    async def process_pending(
        self, surface: Literal["slack", "github"], callbacks: VoiceDraftCallbacks
    ) -> bool:
        """Claim and process at most one pending recognition or delivery for this surface."""
        ...

    async def decide(
        self,
        *,
        code: str,
        tenant_id: UUID,
        actor: Actor,
        audience: Audience,
        action: Action,
        feedback: VoiceFeedbackPort,
    ) -> EditAccepted | Confirmed | Rejected | Refused: ...
