"""Channel identities: who Curie speaks as (#2909, ADR 0168 decision 1).

``name`` is unique within the provider and tenant and is what a binding's
``adapter`` names (decision 3); two bots in one Slack workspace are two rows
sharing nothing but the tenant and provider, never a ``name``. ``attributes``
holds whatever that provider's identity needs beyond the fixed columns -- for
Slack, the app-token reference alongside ``credential_ref``'s bot-token
reference. "default" is the one identity an install need not name explicitly.

``provider_installation_id`` is nullable: a declared identity is created at
boot unattached (ADR 0193 decision 4), and an operator attaches it through
this resource's PATCH until #3039 lands.
"""

import uuid
from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

from .common import ProviderName

ChannelIdentityStatus = Literal["active", "disabled", "revoked"]


def _reject_explicit_null(value: Any) -> Any:
    if value is None:
        raise ValueError("may be omitted but not null")
    return value


class ChannelIdentityCreate(BaseModel):
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
    # Omitted means the default tenant; nothing branches on how many exist.
    tenant_id: uuid.UUID | None = None
    credential_ref: str | None = None
    scopes: list[str] = Field(default_factory=list)
    webhook_verification_ref: str | None = None
    # Provider-specific identity details that don't fit a fixed column.
    attributes: dict[str, Any] = Field(default_factory=dict)
    status: ChannelIdentityStatus = "active"
    # Omitted means unattached, same as a declared identity created at boot.
    provider_installation_id: uuid.UUID | None = None


class ChannelIdentityUpdate(BaseModel):
    """Partial update: an omitted field is unchanged, and null clears a nullable one.

    ``provider_installation_id`` is nullable in the database, so explicit
    null is a real operation here -- it detaches the identity -- unlike the
    other fields below, which reject it.
    """

    name: str | None = Field(default=None, min_length=1)
    credential_ref: str | None = None
    scopes: list[str] | None = None
    webhook_verification_ref: str | None = None
    attributes: dict[str, Any] | None = None
    status: ChannelIdentityStatus | None = None
    provider_installation_id: uuid.UUID | None = None

    _not_null = field_validator("name", "scopes", "attributes", "status")(_reject_explicit_null)


class ChannelIdentityOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    tenant_id: uuid.UUID
    provider: ProviderName
    name: str
    # A pointer into the secret store, held to the reference grammar.
    credential_ref: str | None
    scopes: list[str]
    webhook_verification_ref: str | None
    attributes: dict[str, Any]
    status: ChannelIdentityStatus
    installation_mismatch: bool
    provider_installation_id: uuid.UUID | None
    created_at: datetime
