"""Provider installations: a connected external account (#2909, ADR 0166 step 4).

A row is the connection a tenant authorised -- ADR 0166's original meaning,
restored by ADR 0193 decision 3 after ADR 0168 decision 1 had made it one row
per channel identity (now :mod:`.channel_identities`). ``authority`` is the
provider endpoint ``external_account_id`` belongs to: empty for a provider
with one global service, the canonical hostname for a self-hosted or
per-host one.
"""

import uuid
from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

from .common import ProviderName

ProviderInstallationStatus = Literal["connected", "disconnected"]


def _reject_explicit_null(value: Any) -> Any:
    if value is None:
        raise ValueError("may be omitted but not null")
    return value


class ProviderInstallationCreate(BaseModel):
    """A connected external account, created by an administrator."""

    provider: ProviderName
    # Omitted means a provider with one global service; empty is the
    # equivalent explicit spelling of the same thing.
    authority: str = ""
    external_account_id: str = Field(min_length=1)
    # Omitted means the default tenant; nothing branches on how many exist.
    tenant_id: uuid.UUID | None = None
    display_name: str | None = None
    status: ProviderInstallationStatus = "connected"
    installed_by_principal_id: uuid.UUID | None = None


class ProviderInstallationUpdate(BaseModel):
    """Partial update: an omitted field is unchanged, and null clears a nullable one."""

    authority: str | None = None
    display_name: str | None = None
    external_account_id: str | None = Field(default=None, min_length=1)
    status: ProviderInstallationStatus | None = None

    _not_null = field_validator("authority", "external_account_id", "status")(
        _reject_explicit_null
    )


class ProviderInstallationOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    tenant_id: uuid.UUID
    provider: ProviderName
    authority: str
    external_account_id: str
    display_name: str | None
    status: ProviderInstallationStatus
    installed_by_principal_id: uuid.UUID | None
    installed_at: datetime
    disconnected_at: datetime | None
