"""Wire bodies of the worker-only thread attachment ledger routes (ADR 0205, #4079)."""

from __future__ import annotations

import uuid

from pydantic import BaseModel, ConfigDict, Field, field_validator


class ThreadAttachmentRefBody(BaseModel):
    """One recorded file. Exactly these fields: never an endpoint, URL or bytes."""

    model_config = ConfigDict(extra="forbid", from_attributes=True)

    file_id: str = Field(min_length=1, max_length=512)
    ordinal: int = Field(ge=0)
    name: str = Field(min_length=1, max_length=1024)
    disk_name: str = Field(min_length=1, max_length=255)
    mime_type: str | None = Field(default=None, max_length=255)
    size_bytes: int | None = Field(default=None, ge=0)
    sha256: str = Field(pattern=r"^[0-9A-Fa-f]{64}$")
    route_kind: str = Field(min_length=1, max_length=64)
    route_adapter: str | None = Field(default=None, max_length=255)
    route_identity: str = Field(min_length=1, max_length=255)

    @field_validator("disk_name")
    @classmethod
    def _one_path_segment(cls, value: str) -> str:
        if value in {".", ".."} or any(ch in value for ch in "/\\\x00"):
            raise ValueError("disk_name must be a single file name")
        return value


class ThreadAttachmentQuery(BaseModel):
    model_config = ConfigDict(extra="forbid")

    agent_id: uuid.UUID
    thread_key: str = Field(min_length=1, max_length=1024)


class ThreadAttachmentAppend(ThreadAttachmentQuery):
    event_id: str = Field(min_length=1, max_length=512)
    refs: list[ThreadAttachmentRefBody] = Field(max_length=1000)


class ThreadAttachmentRefRow(ThreadAttachmentRefBody):
    """A query row: the appended ref plus the event it was recorded under, so a
    redelivered turn's worker recognises its own rows by (event_id, file_id)."""

    event_id: str


class ThreadAttachmentRefsOut(BaseModel):
    refs: list[ThreadAttachmentRefRow]


class ThreadAttachmentAppendOut(BaseModel):
    appended: int
