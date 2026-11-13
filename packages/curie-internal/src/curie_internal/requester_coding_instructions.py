"""Owner coding text and frozen selection contracts (ADR 0278, #5590)."""

from __future__ import annotations

import hashlib
from typing import Annotated, Literal, Self
from uuid import UUID

from pydantic import BaseModel, BeforeValidator, ConfigDict, Field, model_validator
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

MAX_BYTES = 16_384
MAX_REVISION = 9_223_372_036_854_775_807
FORMAT_VERSION: Literal[1] = 1
ENVELOPE_VERSION: Literal[1] = 1


class InvalidCodingInstructions(ValueError):
    """A redacted validation refusal; the supplied text is never retained."""

    def __init__(self, code: Literal["invalid_text", "too_large"]) -> None:
        self.code = code
        super().__init__(code)


def normalize_text(value: str | bytes) -> str:
    """Normalize line endings, then validate UTF-8, NUL and the byte bound."""

    try:
        if isinstance(value, bytes):
            value = value.decode("utf-8", errors="strict")
        if type(value) is not str:
            raise InvalidCodingInstructions("invalid_text")
        normalized = value.replace("\r\n", "\n").replace("\r", "\n")
        encoded = normalized.encode("utf-8", errors="strict")
    except UnicodeError:
        raise InvalidCodingInstructions("invalid_text") from None
    if b"\x00" in encoded:
        raise InvalidCodingInstructions("invalid_text")
    if len(encoded) > MAX_BYTES:
        raise InvalidCodingInstructions("too_large")
    return normalized


def text_sha256(normalized_text: str) -> str:
    """Digest the exact normalized UTF-8 document."""

    normalized = normalize_text(normalized_text)
    if normalized != normalized_text:
        raise InvalidCodingInstructions("invalid_text")
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def validate_expected_revision(value: int) -> int:
    if type(value) is not int or not 0 <= value <= MAX_REVISION:
        raise InvalidCodingInstructions("invalid_text")
    return value


def _version(value: object) -> int:
    if type(value) is not int or value != 1:
        raise ValueError("invalid_version")
    return value


def _uuid(value: object) -> str:
    if type(value) is not str:
        raise ValueError("invalid_identifier")
    try:
        if str(UUID(value)) != value:
            raise ValueError("invalid_identifier")
    except (ValueError, AttributeError):
        raise ValueError("invalid_identifier") from None
    return value


Version = Annotated[Literal[1], BeforeValidator(_version)]
Identifier = Annotated[str, BeforeValidator(_uuid)]
Revision = Annotated[int, Field(strict=True, ge=1, le=MAX_REVISION)]
Digest = Annotated[str, Field(strict=True, min_length=64, max_length=64, pattern=r"^[0-9a-f]{64}$")]
AbsenceReason = Literal[
    "no_profile",
    "no_verified_requester",
    "tenant_disabled",
    "application_unavailable",
    "historical",
]
EvidenceState = Literal["pending", "none", "applied", "failed"]


class _Contract(BaseModel):
    model_config = ConfigDict(
        strict=True,
        frozen=True,
        extra="forbid",
        hide_input_in_errors=True,
        revalidate_instances="always",
    )


class SelectedCodingInstructions(_Contract):
    kind: Literal["selected"] = "selected"
    principal_id: Identifier
    revision: Revision
    sha256: Digest
    format_version: Version = FORMAT_VERSION
    text: str = Field(repr=False)

    @model_validator(mode="after")
    def _valid_text(self) -> Self:
        normalized = normalize_text(self.text)
        if not normalized or normalized != self.text or text_sha256(normalized) != self.sha256:
            raise ValueError("invalid_selected_text")
        return self


class AbsentCodingInstructions(_Contract):
    kind: Literal["absent"] = "absent"
    reason: AbsenceReason


CodingInstructionsSelection = Annotated[
    SelectedCodingInstructions | AbsentCodingInstructions, Field(discriminator="kind")
]


class CodingInstructionsContextV1(_Contract):
    envelope_version: Version = ENVELOPE_VERSION
    tenant_id: Identifier
    work_item_id: Identifier
    execution_request_id: Identifier
    execution_fence: Revision
    selection: CodingInstructionsSelection


class CodingInstructionsAcknowledgmentV1(_Contract):
    envelope_version: Version = ENVELOPE_VERSION
    work_item_id: Identifier
    execution_request_id: Identifier
    execution_fence: Revision
    outcome: Literal["none", "applied", "failed"]
    sha256: Digest | None = Field(default=None, exclude_if=lambda value: value is None)

    @model_validator(mode="after")
    def _digest_when_applied(self) -> Self:
        if (self.outcome == "applied") != (self.sha256 is not None):
            raise ValueError("invalid_acknowledgment_digest")
        return self


class CodingInstructionsEvidence(_Contract):
    state: EvidenceState


async def require_read_committed(session: AsyncSession) -> None:
    isolation = await session.scalar(text("SHOW transaction_isolation"))
    if isolation != "read committed":
        raise ValueError("coding_instructions_requires_read_committed")


async def lock_principal(
    session: AsyncSession, *, tenant_id: UUID, principal_id: UUID, writing: bool = False
) -> bool:
    """Serialize on the identity row even before a profile exists."""

    await require_read_committed(session)
    lock = "FOR NO KEY UPDATE" if writing else "FOR SHARE"
    found = await session.scalar(
        text(
            "SELECT status FROM curie.principals WHERE tenant_id = :tenant AND id = :principal "
            + lock
        ),
        {"tenant": tenant_id, "principal": principal_id},
    )
    return isinstance(found, str) and found == "active"


async def select_coding_instructions(
    session: AsyncSession,
    *,
    tenant_id: UUID,
    principal_id: UUID | None,
    application_available: bool,
    historical: bool = False,
) -> SelectedCodingInstructions | AbsentCodingInstructions:
    """Select inside the caller's transaction, keeping the principal lock until commit."""

    if historical:
        return AbsentCodingInstructions(reason="historical")
    if not application_available:
        return AbsentCodingInstructions(reason="application_unavailable")
    if principal_id is None:
        return AbsentCodingInstructions(reason="no_verified_requester")
    if not await lock_principal(session, tenant_id=tenant_id, principal_id=principal_id):
        return AbsentCodingInstructions(reason="no_verified_requester")
    enabled = await session.scalar(
        text("SELECT enabled FROM curie.requester_coding_policies WHERE tenant_id = :tenant"),
        {"tenant": tenant_id},
    )
    if enabled is False:
        return AbsentCodingInstructions(reason="tenant_disabled")
    row = (
        (
            await session.execute(
                text(
                    "SELECT revision, sha256, format_version, text "
                    "FROM curie.requester_coding_profiles "
                    "WHERE tenant_id = :tenant AND principal_id = :principal"
                ),
                {"tenant": tenant_id, "principal": principal_id},
            )
        )
        .mappings()
        .one_or_none()
    )
    if row is None or row["text"] is None:
        return AbsentCodingInstructions(reason="no_profile")
    return SelectedCodingInstructions(principal_id=str(principal_id), **dict(row))
