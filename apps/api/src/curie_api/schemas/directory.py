"""Generic IdP delivery contracts. No manual principal/link mapping surface."""

import re
import uuid
from datetime import datetime
from typing import Literal
from urllib.parse import urlsplit

from pydantic import BaseModel, ConfigDict, Field, field_validator


class DirectoryModel(BaseModel):
    model_config = ConfigDict(extra="forbid", from_attributes=True)


class DirectoryConfigIn(DirectoryModel):
    issuer: str = Field(strict=True, max_length=2048)
    governed_domains: list[str] = Field(max_length=100)
    managed_by: str = Field(strict=True, pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
    expected_version: int = Field(strict=True, ge=0)

    @field_validator("issuer")
    @classmethod
    def issuer_valid(cls, value: str) -> str:
        parts = urlsplit(value)
        if (
            parts.scheme != "https"
            or not parts.hostname
            or parts.username
            or parts.password
            or parts.query
            or parts.fragment
            or value != value.strip()
        ):
            raise ValueError("invalid_issuer")
        return value

    @field_validator("governed_domains")
    @classmethod
    def domains_valid(cls, values: list[str]) -> list[str]:
        pattern = r"(?=.{1,253}$)[a-z0-9](?:[a-z0-9.-]*[a-z0-9])?"
        if len(set(values)) != len(values) or any(
            not re.fullmatch(pattern, value)
            or "." not in value
            or ".." in value
            or any(
                label.startswith("-") or label.endswith("-") or len(label) > 63
                for label in value.split(".")
            )
            for value in values
        ):
            raise ValueError("invalid_governed_domain")
        return sorted(values)


class DirectoryConfigOut(DirectoryModel):
    tenant_id: uuid.UUID
    issuer: str
    governed_domains: list[str]
    managed_by: str
    version: int


class DirectoryCredentialIn(DirectoryModel):
    tenant_id: uuid.UUID
    expected_config_version: int = Field(strict=True, ge=1)
    team_ids: list[uuid.UUID] = Field(min_length=1, max_length=1000)
    namespace_ids: list[uuid.UUID] = Field(max_length=1000)
    expires_in_seconds: int = Field(default=3600, strict=True, ge=60, le=86400)


class DirectoryCredentialOut(DirectoryModel):
    credential: str
    expires_at: datetime


class DirectoryTeamIn(DirectoryModel):
    tenant_id: uuid.UUID
    external_id: str = Field(strict=True, min_length=1, max_length=512)
    name: str = Field(strict=True, min_length=1, max_length=256)


class DirectoryTeamOut(DirectoryModel):
    id: uuid.UUID
    tenant_id: uuid.UUID
    external_id: str | None
    name: str
    managed_by: str | None
    epoch: int
    member_count: int
    digest: str | None
    last_synced_at: datetime | None
    last_attempt_at: datetime | None
    last_error: str | None


class DirectoryAccountIn(DirectoryModel):
    identity_namespace_id: uuid.UUID
    provider_native_id: str = Field(strict=True, min_length=1, max_length=320)


class DirectoryPersonIn(DirectoryModel):
    subject: str = Field(strict=True, min_length=1, max_length=512)
    display_name: str | None = Field(default=None, strict=True, max_length=256)
    email: str | None = Field(default=None, strict=True, max_length=320)
    status: Literal["active", "disabled", "revoked"] = "active"
    accounts: list[DirectoryAccountIn] = Field(default_factory=list, max_length=1000)


class DirectorySnapshotIn(DirectoryModel):
    expected_epoch: int = Field(strict=True, ge=0)
    complete: bool = Field(strict=True)
    people: list[DirectoryPersonIn] = Field(max_length=10000)


class DirectorySnapshotOut(DirectoryModel):
    epoch: int
    member_count: int
    digest: str


class DirectoryAttemptIn(DirectoryModel):
    expected_epoch: int = Field(strict=True, ge=0)
    error: Literal["provider_unavailable", "provider_rejected", "invalid_snapshot"]


class DirectoryStatusIn(DirectoryModel):
    subject: str = Field(strict=True, min_length=1, max_length=512)
    expected_version: int = Field(strict=True, ge=1)
    status: Literal["active", "disabled", "revoked"]


class DirectoryStatusOut(DirectoryModel):
    authorization_version: int


class DirectoryPersonOut(DirectoryModel):
    subject: str
    status: str
    authorization_version: int


class DirectoryPeopleOut(DirectoryModel):
    people: list[DirectoryPersonOut]
    next_after_subject: str | None


class DirectoryNamespaceIn(DirectoryModel):
    tenant_id: uuid.UUID
    provider: Literal["slack", "email"]
    authority: str = Field(default="", strict=True, max_length=256)
    kind: Literal["slack_workspace", "email_domain"]
    key: str = Field(strict=True, min_length=1, max_length=253)


class DirectoryNamespaceOut(DirectoryModel):
    id: uuid.UUID
    tenant_id: uuid.UUID
    provider: str
    authority: str
    kind: str
    key: str
    status: str
