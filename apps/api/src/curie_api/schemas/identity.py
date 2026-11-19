"""Principal resolution and Slack identity reports (#2910, #3039, ADR 0198, ADR 0201).

Both bodies refuse extra fields, so a field the resolver does not use cannot
be sent as though it were evidence. The report's Slack answers are strict
booleans and REQUIRED keys (null allowed where Slack can say null): a missing
answer is not evidence, and a string ``"false"`` is not ``false``.
"""

import uuid
from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from ..identity.slack import SlackEvidence
from .common import ProviderName

# A Slack team id (never an enterprise id); the same grammar as the
# identity_namespaces_slack_workspace_key_ck CHECK.
SLACK_TEAM_PATTERN = r"^T[A-Z0-9]{2,}$"
SLACK_ID_MAX_LENGTH = 64


class PrincipalResolveIn(BaseModel):
    """Which principal sent this event, as seen by one receiving channel identity."""

    model_config = ConfigDict(extra="forbid")

    # Omitted means the default tenant; nothing branches on how many exist.
    tenant_id: uuid.UUID | None = None
    provider: ProviderName
    channel_identity: str = Field(min_length=1)
    # Per provider: Slack's sender fields. Another provider answers
    # provider_unsupported after the identity checks.
    slack: SlackEvidence | None = None


class PrincipalResolutionOut(BaseModel):
    status: Literal["resolved", "unresolved"]
    reason: str
    principal_id: uuid.UUID | None
    channel_identity_id: uuid.UUID | None
    namespace_id: uuid.UUID | None


class SlackReportIn(BaseModel):
    """What ``auth.test`` returned for one Slack identity's own token."""

    model_config = ConfigDict(extra="forbid")

    tenant_id: uuid.UUID | None = None
    name: str = Field(min_length=1)
    team_id: str = Field(pattern=SLACK_TEAM_PATTERN, max_length=SLACK_ID_MAX_LENGTH)
    # Strict: a number, bool, list or object is malformed, never coerced.
    enterprise_id: str | None = Field(strict=True, max_length=SLACK_ID_MAX_LENGTH)
    enterprise_id_present: bool = Field(strict=True)
    is_enterprise_install: bool | None = Field(strict=True)


class SlackReportOut(BaseModel):
    identity_id: uuid.UUID
    provider_installation_id: uuid.UUID
    namespace_id: uuid.UUID
    installation_mismatch: bool


# A GitHub numeric user id (ADR 0198: GitHub's native id), never a login.
GITHUB_ACCOUNT_ID_PATTERN = r"^[1-9][0-9]{0,19}$"
# A canonical lowercase hostname, optionally with a port: github.com or a GHES host.
CODE_HOST_HOST_PATTERN = (
    r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?(?:\.[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?)*"
    r"(?::[0-9]{1,5})?$"
)


class CodeHostLinkIn(BaseModel):
    """An operator's assertion that a principal holds one code host account (#5414).

    Recorded as an ``admin_mapped`` link in the host's ``github_host``
    namespace (ADR 0198 decision 8). The account is named by its immutable id.
    """

    model_config = ConfigDict(extra="forbid")

    principal_id: uuid.UUID
    code_host_kind: Literal["github"]
    host: str = Field(max_length=255, pattern=CODE_HOST_HOST_PATTERN)
    account_id: str = Field(pattern=GITHUB_ACCOUNT_ID_PATTERN)
    created_by_principal_id: uuid.UUID


class CodeHostLinkOut(BaseModel):
    link_id: uuid.UUID
    identity_namespace_id: uuid.UUID


class IdentityLinkRevokedOut(BaseModel):
    link_id: uuid.UUID
    revoked_at: datetime
