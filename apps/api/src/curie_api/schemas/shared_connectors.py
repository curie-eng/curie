"""Wire bodies of the shared connector store routes (ADR 0239).

Every request object refuses extra fields, so a private key, a plaintext
credential or any other field the store does not model is refused rather than
ignored. No validator message carries the value it refused, so a redacted 422
names what failed without echoing what was sent.

Credentials cross this API only as sealed boxes; no response model has a
ciphertext field, and the sealed DSN appears only in the provisioning and
rotation answers, never in an application record.
"""

from __future__ import annotations

import re
import unicodedata
import uuid
from datetime import datetime
from typing import Annotated, Any, Literal, Self

from pydantic import AfterValidator, BaseModel, ConfigDict, Field, field_validator, model_validator

from ..shared_connectors import sealing

HEX64_PATTERN = r"^[0-9a-f]{64}$"
DNS_LABEL_PATTERN = r"^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$"
TOOL_PATTERN = r"^[A-Za-z0-9_.:-]{1,128}$"
GRANT_ID_PATTERN = r"^[A-Za-z0-9._:-]{1,128}$"
CLIENT_FINGERPRINT_PATTERN = r"^sha256:[0-9a-f]{64}$"
SOURCE_FINGERPRINT_PATTERN = r"^hmac-sha256:[0-9a-f]{64}$"
MANAGED_ROLE_PATTERN = r"^[a-z_][a-z0-9_]{0,62}$"
STATUS_REASON_PATTERN = r"^[a-z][a-z0-9_]{0,63}$"

Custody = Literal["platform", "connector"]
DatabaseMode = Literal["bundled", "managed"]
DatabaseStatus = Literal["provisioned", "pending", "ready", "failed"]
RefreshState = Literal["ready", "refreshing", "recovery_required"]

_DNS_LABEL = re.compile(DNS_LABEL_PATTERN)

_Tool = Annotated[str, Field(pattern=TOOL_PATTERN)]
_Tools = Annotated[list[_Tool], Field(min_length=1, max_length=256)]
# A policy tool subset; may be empty.
_ToolSubset = Annotated[list[_Tool], Field(max_length=256)]


def _has_control(value: str) -> bool:
    return any(unicodedata.category(char) in {"Cc", "Cf", "Cs"} for char in value)


def _opaque(value: str, limit: int) -> str:
    """A source identifier: 1..``limit`` UTF-8 bytes, no whitespace or controls."""

    if (
        not 1 <= len(value.encode("utf-8")) <= limit
        or any(char.isspace() for char in value)
        or _has_control(value)
    ):
        raise ValueError(f"expected 1 to {limit} bytes without whitespace or control characters")
    return value


def _dns_subdomain(value: str) -> str:
    if len(value) > 253 or not all(_DNS_LABEL.fullmatch(part) for part in value.split(".")):
        raise ValueError("expected a DNS subdomain of at most 253 characters")
    return value


def _unique(values: list[str], what: str) -> list[str]:
    if len(set(values)) != len(values):
        raise ValueError(f"{what} must be unique")
    return values


_DnsSubdomain = Annotated[str, AfterValidator(_dns_subdomain)]
_SourceUid = Annotated[str, AfterValidator(lambda value: _opaque(value, 128))]
_ResourceVersion = Annotated[str, AfterValidator(lambda value: _opaque(value, 64))]


# --- requests -----------------------------------------------------------------


class SharedPolicy(BaseModel):
    """The operator-approved tool set and who holds the credential."""

    model_config = ConfigDict(extra="forbid")

    tools: Annotated[_Tools, AfterValidator(lambda value: _unique(value, "policy tools"))]
    # "platform": the store holds sealed credentials, so the application must
    # declare at least one slot. "connector": identity-only admission for a
    # connector holding its own binding-indexed store, which declares none.
    custody: Custody = "platform"
    # Tools the shared proxy admits only for a caller token carrying a run pair,
    # and tools it admits only with a shared approval grant (``csg``). Each is
    # a subset of ``tools``. Stored only when nonempty, so a policy without
    # them keeps the JSON and pin digest it had before they existed.
    run_tools: Annotated[
        _ToolSubset, AfterValidator(lambda value: _unique(value, "run tools"))
    ] = Field(default_factory=list)
    gated_tools: Annotated[
        _ToolSubset, AfterValidator(lambda value: _unique(value, "gated tools"))
    ] = Field(default_factory=list)

    @model_validator(mode="after")
    def _subsets_of_tools(self) -> Self:
        allowed = set(self.tools)
        if not set(self.run_tools) <= allowed:
            raise ValueError("run tools must be policy tools")
        if not set(self.gated_tools) <= allowed:
            raise ValueError("gated tools must be policy tools")
        return self


class SharedApplicationRegistration(BaseModel):
    model_config = ConfigDict(extra="forbid")

    # The frozen ``SharedApplication`` in its JSON form. Validated by the route
    # in JSON mode (the model is strict and has tuple fields), so a refusal is
    # one code rather than an error list that could echo the submitted spec.
    application: dict[str, Any]
    policy: SharedPolicy


class SharedAliasIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    alias: str = Field(pattern=DNS_LABEL_PATTERN)
    # Null admits every policy tool; a list may only narrow the policy.
    tools: Annotated[_Tools, AfterValidator(lambda value: _unique(value, "alias tools"))] | None


class SharedMembershipReplace(BaseModel):
    model_config = ConfigDict(extra="forbid")

    aliases: list[SharedAliasIn] = Field(min_length=1, max_length=16)

    @model_validator(mode="after")
    def _distinct_aliases(self) -> Self:
        _unique([item.alias for item in self.aliases], "aliases")
        return self


class SharedCredentialEnvelope(BaseModel):
    """A credential the CLI sealed to one of the application's recipients."""

    model_config = ConfigDict(extra="forbid")

    recipient_id: str = Field(pattern=HEX64_PATTERN)
    ciphertext: str = Field(min_length=1, max_length=sealing.MAX_CIPHERTEXT_BASE64)

    @field_validator("ciphertext")
    @classmethod
    def _sealed_box(cls, value: str) -> str:
        sealing.ciphertext_bytes(value)
        return value

    def sealed(self) -> bytes:
        return sealing.ciphertext_bytes(self.ciphertext)


class SharedSourceMetadata(BaseModel):
    """Where an imported credential came from; never its material."""

    model_config = ConfigDict(extra="forbid")

    kind: Literal["kubernetes-secret"]
    namespace: str = Field(pattern=DNS_LABEL_PATTERN)
    name: _DnsSubdomain
    uid: _SourceUid
    resource_version: _ResourceVersion
    fingerprint: str = Field(pattern=SOURCE_FINGERPRINT_PATTERN)


class SharedSourceVersion(BaseModel):
    model_config = ConfigDict(extra="forbid")

    uid: _SourceUid
    resource_version: _ResourceVersion


class SharedRefreshGrantIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    grant_id: str = Field(pattern=GRANT_ID_PATTERN)
    client_config_fingerprint: str = Field(pattern=CLIENT_FINGERPRINT_PATTERN)


class SharedBindingImport(BaseModel):
    model_config = ConfigDict(extra="forbid")

    agent_id: uuid.UUID
    provider_account: str = Field(min_length=1, max_length=256)
    # Platform custody requires a credential and its source; connector custody
    # requires all three absent.
    credential: SharedCredentialEnvelope | None = None
    source: SharedSourceMetadata | None = None
    refresh_grant: SharedRefreshGrantIn | None = None

    @field_validator("provider_account")
    @classmethod
    def _account(cls, value: str) -> str:
        if len(value.encode("utf-8")) > 256 or _has_control(value):
            raise ValueError("expected 1 to 256 bytes without control characters")
        return value


class SharedBindingReimport(BaseModel):
    model_config = ConfigDict(extra="forbid")

    expected_revision: int = Field(ge=1)
    credential: SharedCredentialEnvelope
    source: SharedSourceMetadata


class SharedBindingCheck(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source: SharedSourceVersion


class SharedDatabaseProvision(BaseModel):
    """``bundled``: the API creates the role. ``managed``: an operator's role."""

    model_config = ConfigDict(extra="forbid")

    mode: DatabaseMode
    role_name: Annotated[str, Field(pattern=MANAGED_ROLE_PATTERN)] | None = None

    @model_validator(mode="after")
    def _role_for_mode(self) -> Self:
        if (self.mode == "managed") != (self.role_name is not None):
            raise ValueError("managed mode requires role_name and bundled mode forbids it")
        return self


class SharedDatabaseRotate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    expected_dsn_revision: int = Field(ge=1)


class SharedRecipientPublicKey(BaseModel):
    model_config = ConfigDict(extra="forbid")

    algorithm: Literal["x25519-sealed-box"]
    key: str = Field(max_length=64)

    @field_validator("key")
    @classmethod
    def _public_key(cls, value: str) -> str:
        sealing.public_key_bytes(value)
        return value

    def raw(self) -> bytes:
        return sealing.public_key_bytes(self.key)


class SharedRecipientReadback(BaseModel):
    model_config = ConfigDict(extra="forbid")

    public_key_sha256: str = Field(pattern=HEX64_PATTERN)


class SharedRecipientRegistration(BaseModel):
    """Public key material only; any other field (a private key) is refused."""

    model_config = ConfigDict(extra="forbid")

    application_id: uuid.UUID
    recipient_id: str = Field(pattern=HEX64_PATTERN)
    public_key: SharedRecipientPublicKey
    readback: SharedRecipientReadback


class SharedDatabaseStatusReport(BaseModel):
    model_config = ConfigDict(extra="forbid")

    status: Literal["ready", "failed"]
    reason: Annotated[str, Field(pattern=STATUS_REASON_PATTERN)] | None = None
    # The DSN revision the worker activated; a report for a rotated DSN is stale.
    dsn_revision: int = Field(ge=1)


# --- responses ----------------------------------------------------------------


class SharedPolicyOut(BaseModel):
    tools: list[str]
    custody: Custody
    run_tools: list[str]
    gated_tools: list[str]


class SharedSecretRefOut(BaseModel):
    name: str
    key: str


class SharedRecipientOut(BaseModel):
    recipient_id: str
    algorithm: str
    current: bool
    created_at: datetime
    retired_at: datetime | None


class SharedDatabaseOut(BaseModel):
    mode: DatabaseMode
    role_name: str
    status: DatabaseStatus
    status_reason: str | None
    dsn_recipient_id: str | None
    dsn_revision: int
    dsn_secret: SharedSecretRefOut | None


class SharedApplicationOut(BaseModel):
    application_id: uuid.UUID
    installation_id: str
    server_image: str
    credential_slots: list[str]
    provider_origins: list[str]
    snapshot_seal: SharedSecretRefOut | None
    policy: SharedPolicyOut
    custody: Custody
    policy_generation: int
    enabled: bool
    recipients: list[SharedRecipientOut]
    database: SharedDatabaseOut | None
    created_at: datetime


class SharedApplicationDisabled(BaseModel):
    application_id: uuid.UUID
    enabled: bool
    acknowledged: bool


class SharedAliasOut(BaseModel):
    alias: str
    tools: list[str] | None


class SharedMembershipOut(BaseModel):
    application_id: uuid.UUID
    agent_id: uuid.UUID
    aliases: list[SharedAliasOut]
    policy_generation: int


class SharedRefreshOut(BaseModel):
    grant_id: str
    client_config_fingerprint: str
    state: RefreshState
    epoch: int


class SharedBindingOut(BaseModel):
    binding_id: uuid.UUID
    application_id: uuid.UUID
    agent_id: uuid.UUID
    provider_account: str
    custody: Custody
    enabled: bool
    credential_revision: int
    recipient_id: str | None
    source: SharedSourceMetadata | None
    refresh: SharedRefreshOut | None
    created_at: datetime
    updated_at: datetime


class SharedBindingDisabled(BaseModel):
    binding_id: uuid.UUID
    enabled: bool
    acknowledged: bool


class SharedBindingCheckOut(BaseModel):
    binding_id: uuid.UUID
    status: Literal["current", "source_stale"]
    credential_revision: int
    recorded_source: SharedSourceVersion


class SharedBundledDatabaseOut(BaseModel):
    application_id: uuid.UUID
    mode: Literal["bundled"]
    role_name: str
    status: Literal["provisioned"]
    recipient_id: str
    dsn_revision: int
    # Standard base64 of the sealed box; only the resolver's private key opens it.
    sealed_dsn: str


class SharedManagedDatabaseOut(BaseModel):
    application_id: uuid.UUID
    mode: Literal["managed"]
    role_name: str
    status: Literal["pending"]
    dsn_secret: SharedSecretRefOut


class SharedDatabaseStatusOut(BaseModel):
    application_id: uuid.UUID
    mode: DatabaseMode
    status: DatabaseStatus
    status_reason: str | None
    dsn_revision: int


class SharedRecipientRegistered(BaseModel):
    application_id: uuid.UUID
    recipient_id: str
    current: bool


class SharedRecipientRetired(BaseModel):
    application_id: uuid.UUID
    recipient_id: str
    retired_at: datetime
