"""Internal admission port for text confirmed by the author of a voice draft."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal, Protocol
from uuid import NAMESPACE_URL, UUID, uuid5

from sqlalchemy.ext.asyncio import AsyncSession


@dataclass(frozen=True, kw_only=True)
class SlackSteeringSource:
    channel_identity: str
    channel: str
    root_ts: str
    message_ts: str
    file_id: str
    author: str
    principal_id: UUID
    confirmation_delivery_id: str


@dataclass(frozen=True, kw_only=True)
class GitHubSteeringSource:
    host: str
    installation_id: int
    repository_id: int
    conversation_kind: Literal["issue", "pull_request"]
    number: int
    source_comment_id: int
    confirmation_comment_id: int
    actor_id: int


@dataclass(frozen=True, kw_only=True)
class ConfirmedText:
    draft_id: UUID
    work_item_id: UUID
    source: SlackSteeringSource | GitHubSteeringSource
    source_content_sha256: str
    text: str = field(repr=False)

    @property
    def steering_id(self) -> UUID:
        """One logical command for the lifetime of this draft, across confirmations."""
        return uuid5(NAMESPACE_URL, f"curie:factory:confirmed-text:{self.draft_id}")


@dataclass(frozen=True, kw_only=True)
class QueuedSteering:
    steering_id: UUID
    execution_request_id: UUID | None
    replayed: bool


@dataclass(frozen=True, kw_only=True)
class AppliedSteering:
    steering_id: UUID
    execution_request_id: UUID
    replayed: bool


@dataclass(frozen=True, kw_only=True)
class RefusedSteering:
    reason: str


type SteeringDecision = QueuedSteering | AppliedSteering | RefusedSteering


class ConfirmedTextIngress(Protocol):
    """Called with the draft locked. The caller owns the transaction and its commit."""

    async def __call__(
        self, session: AsyncSession, command: ConfirmedText, /
    ) -> SteeringDecision: ...
