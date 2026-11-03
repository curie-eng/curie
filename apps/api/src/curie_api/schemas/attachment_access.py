"""Bounded control messages; no file-byte limits or provider URLs."""

from __future__ import annotations

import uuid
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


class AccessBody(BaseModel):
    model_config = ConfigDict(extra="forbid")


class Reference(AccessBody):
    file_id: str = Field(min_length=1, max_length=512)
    ordinal: int = Field(ge=0, le=1000)
    name: str = Field(min_length=1, max_length=1024)
    mime_type: str | None = Field(default=None, max_length=255)
    size_bytes: int | None = Field(default=None, ge=0, le=2**63 - 1)
    route_kind: str = Field(min_length=1, max_length=64)
    route_adapter: str | None = Field(default=None, max_length=255)
    route_identity: str = Field(min_length=1, max_length=255)


class Registration(AccessBody):
    agent_id: uuid.UUID
    thread_key: str = Field(min_length=1, max_length=1024)
    event_id: str = Field(min_length=1, max_length=512)
    refs: list[Reference] = Field(max_length=1000)


class Registered(AccessBody):
    appended: int


class Owner(AccessBody):
    agent_id: uuid.UUID
    thread_key: str = Field(min_length=1, max_length=1024)
    event_id: str = Field(min_length=1, max_length=512)
    owner: str = Field(min_length=1, max_length=255)


class Renewal(Owner):
    current_event_id: str | None = Field(default=None, min_length=1, max_length=512)


class Context(Owner):
    deployment_id: uuid.UUID
    session_id: str = Field(min_length=1, max_length=255)
    sandbox_name: str = Field(min_length=1, max_length=255)
    route_generation: int = Field(ge=0)
    ttl_s: int = Field(default=3600, ge=1, le=3600)


class Capability(AccessBody):
    token: str = Field(repr=False)
    expires_at: int
    lease_seconds: int


class Active(AccessBody):
    active: bool


class Listing(AccessBody):
    cursor: str | None = Field(default=None, max_length=36)
    limit: int = Field(default=50, ge=1, le=100)


class Selection(AccessBody):
    id: uuid.UUID


class FileDescriptor(AccessBody):
    id: str
    name: str
    mime_type: str | None
    size_bytes: int | None
    sha256: str | None
    current: bool


class Page(AccessBody):
    files: list[FileDescriptor]
    next_cursor: str | None


class Transfer(AccessBody):
    transfer_id: str = Field(min_length=32, max_length=128)


class Ticket(AccessBody):
    ticket: str = Field(min_length=32, max_length=128, repr=False)


class Completed(Transfer):
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    size_bytes: int = Field(ge=0, le=2**63 - 1)


class Completion(AccessBody):
    id: str
    sha256: str
    size_bytes: int
    status: Literal["complete"] = "complete"
