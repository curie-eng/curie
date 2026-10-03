"""Provider installations: one row per channel identity (#2909, ADR 0166 step 4).

A row is one channel identity -- one bot speaking through one connected
account -- not one connected account (ADR 0168 decision 1). ``name`` is
unique within the provider and tenant and is what a binding's ``adapter``
names (decision 3); two bots in one Slack workspace are two rows sharing one
``external_account_id`` but never a ``name``. "default" is the one identity
an install need not name explicitly.
"""

import uuid
from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

ProviderName = Literal[
    "slack", "m365", "github", "jira", "linear", "confluence", "quickbooks", "other"
]
ProviderInstallationStatus = Literal["connected", "disconnected", "degraded"]


def _reject_explicit_null(value: Any) -> Any:
    if value is None:
        raise ValueError("may be omitted but not null")
    return value


class ProviderInstallationCreate(BaseModel):
    """One channel identity, created by an administrator.

    ``credential_ref`` and ``webhook_verification_ref`` are plain strings
    here, with no Field constraints, on purpose: FastAPI's 422 echoes the
    rejected input, and a value pasted where a reference belongs is exactly
    the credential that must not come back. The router checks both against
    the reference grammar and answers with a message that omits the value.
    """

    provider: ProviderName
    # Omitted means "default", the one identity an install need not name.
    name: str = Field(default="default", min_length=1)
    external_account_id: str = Field(min_length=1)
    # Omitted means the default tenant; nothing branches on how many exist.
    tenant_id: uuid.UUID | None = None
    display_name: str | None = None
    credential_ref: str | None = None
    scopes: list[str] = Field(default_factory=list)
    webhook_verification_ref: str | None = None
    # Provider-specific identity details that don't fit a fixed column.
    attributes: dict[str, Any] = Field(default_factory=dict)
    status: ProviderInstallationStatus = "connected"
    installed_by_principal_id: uuid.UUID | None = None


class ProviderInstallationUpdate(BaseModel):
    """Partial update: an omitted field is unchanged, and null clears a nullable one.

    The reference fields follow ``ProviderInstallationCreate``'s rule.
    """

    name: str | None = Field(default=None, min_length=1)
    display_name: str | None = None
    external_account_id: str | None = Field(default=None, min_length=1)
    credential_ref: str | None = None
    scopes: list[str] | None = None
    webhook_verification_ref: str | None = None
    attributes: dict[str, Any] | None = None
    status: ProviderInstallationStatus | None = None

    _not_null = field_validator("name", "external_account_id", "scopes", "attributes", "status")(
        _reject_explicit_null
    )


class ProviderInstallationOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    tenant_id: uuid.UUID
    provider: ProviderName
    name: str
    external_account_id: str
    display_name: str | None
    # A pointer into the secret store, held to the reference grammar.
    credential_ref: str | None
    scopes: list[str]
    webhook_verification_ref: str | None
    attributes: dict[str, Any]
    status: ProviderInstallationStatus
    installed_by_principal_id: uuid.UUID | None
    installed_at: datetime
    disconnected_at: datetime | None
