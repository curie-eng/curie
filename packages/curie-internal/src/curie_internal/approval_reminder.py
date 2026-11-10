"""Notification metadata outside the frozen turn payload. ADR 0273."""

from datetime import UTC, datetime
from uuid import NAMESPACE_URL, uuid5

from pydantic import AwareDatetime, BaseModel, ConfigDict, field_validator

from .identity_context import CanonicalUUID

APPROVAL_REMINDER_FIELD = "curie_approval_reminder_v1"


class ApprovalReminderV1(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)

    version: int
    approval_id: CanonicalUUID
    claimed_due_at: AwareDatetime

    @field_validator("version")
    @classmethod
    def _version(cls, value: int) -> int:
        if value != 1:
            raise ValueError("unsupported approval reminder version")
        return value

    @field_validator("claimed_due_at")
    @classmethod
    def _utc(cls, value: datetime) -> datetime:
        return value.astimezone(UTC)

    @property
    def delivery_id(self) -> str:
        return str(
            uuid5(
                NAMESPACE_URL,
                f"approval-reminder-v1:{self.approval_id}:{self.claimed_due_at.isoformat()}",
            )
        )
