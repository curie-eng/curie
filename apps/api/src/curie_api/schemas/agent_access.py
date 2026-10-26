"""An agent's access policy: its invocation and approver teams (#4585, ADR 0220 s.8)."""

import uuid
from datetime import datetime
from typing import Annotated, Any, Literal, Self

from pydantic import BaseModel, ConfigDict, Field, ModelWrapValidatorHandler, model_validator

from curie_api.schemas.http_identity import (
    CanonicalUUID,
    FactsModel,
    GroupV1,
    Issuer,
    NamespaceV1,
    Opaque,
    SchemaVersion,
)


class AgentAccessPolicyWrite(BaseModel):
    """Replace an agent's policy.

    Both team keys are required so a write states the whole policy; ``null``
    clears that role back to today's behavior. ``expected_version`` is the
    version the writer last read: 0 when the agent has no policy yet.
    """

    model_config = ConfigDict(extra="forbid")

    invocation_team_id: uuid.UUID | None
    approver_team_id: uuid.UUID | None
    expected_version: int = Field(ge=0)


class AgentAccessPolicyOut(BaseModel):
    """An agent's policy; version 0 and no ``updated_at`` mean none was set."""

    agent_id: uuid.UUID
    tenant_id: uuid.UUID
    invocation_team_id: uuid.UUID | None
    approver_team_id: uuid.UUID | None
    version: int
    updated_at: datetime | None


class SurfaceAccountSelectorV1(FactsModel):
    """@spec AGENT-ROLE-FILTER-V1-2."""

    kind: Literal["surface_account"]
    provider: Opaque
    namespace: NamespaceV1
    native_id: Opaque


class IssuerSubjectSelectorV1(FactsModel):
    """@spec AGENT-ROLE-FILTER-V1-2."""

    kind: Literal["issuer_subject"]
    issuer: Issuer
    subject: Opaque


class IssuerGroupSelectorV1(FactsModel):
    """@spec AGENT-ROLE-FILTER-V1-2."""

    kind: Literal["issuer_group"]
    group: GroupV1


SelectorV1 = Annotated[
    SurfaceAccountSelectorV1 | IssuerSubjectSelectorV1 | IssuerGroupSelectorV1,
    Field(discriminator="kind"),
]


def _consistent_namespaces(entries: list[SelectorV1]) -> None:
    """@spec AGENT-ROLE-FILTER-V1-6."""
    descriptors: dict[str, tuple[str, str, str, str]] = {}
    for entry in entries:
        if isinstance(entry, SurfaceAccountSelectorV1):
            namespace = entry.namespace
            descriptor = (entry.provider, namespace.authority, namespace.kind, namespace.key)
            if namespace.id in descriptors and descriptors[namespace.id] != descriptor:
                raise ValueError("contradictory namespace")
            descriptors[namespace.id] = descriptor


class RoleFilterV1(FactsModel):
    """@spec AGENT-ROLE-FILTER-V1-2, AGENT-ROLE-FILTER-V1-6."""

    entries: list[SelectorV1] = Field(max_length=128, json_schema_extra={"uniqueItems": True})

    @model_validator(mode="after")
    def coherent_entries(self) -> Self:
        """@spec AGENT-ROLE-FILTER-V1-6."""
        keys: set[tuple[str, ...]] = set()
        for entry in self.entries:
            key: tuple[str, ...]
            if isinstance(entry, SurfaceAccountSelectorV1):
                namespace = entry.namespace
                key = (
                    entry.kind, entry.provider, namespace.id, namespace.authority,
                    namespace.kind, namespace.key, entry.native_id,
                )
            elif isinstance(entry, IssuerSubjectSelectorV1):
                key = (entry.kind, entry.issuer, entry.subject)
            else:
                key = (entry.kind, entry.group.issuer, entry.group.id)
            if key in keys:
                raise ValueError("duplicate selector")
            keys.add(key)
        _consistent_namespaces(self.entries)
        return self


class RetainedRoleConfigurationV1(FactsModel):
    """@spec AGENT-ROLE-FILTER-V1-3."""

    schema_version: SchemaVersion
    legacy_state: Literal["retained"]
    filter: RoleFilterV1 | None


class MigratedRoleConfigurationV1(FactsModel):
    """@spec AGENT-ROLE-FILTER-V1-3."""

    schema_version: SchemaVersion
    legacy_state: Literal["migrated"]
    filter: RoleFilterV1 | None
    raw_account_revocation: Literal["separate_removal_required"]


RoleConfigurationV1 = Annotated[
    RetainedRoleConfigurationV1 | MigratedRoleConfigurationV1,
    Field(discriminator="legacy_state"),
]


class AgentRolesV1(FactsModel):
    """@spec AGENT-ROLE-FILTER-V1-3, AGENT-ROLE-FILTER-V1-6."""

    # None is an internal omission default, never a valid supplied role. The
    # nonnullable annotations also keep the exported shape faithful to that rule.
    invocation: RoleConfigurationV1 = Field(  # type: ignore[assignment]
        default=None, exclude_if=lambda value: value is None,
    )
    approver: RoleConfigurationV1 = Field(  # type: ignore[assignment]
        default=None, exclude_if=lambda value: value is None,
    )

    @model_validator(mode="wrap")
    @classmethod
    def preserve_presence(cls, value: Any, handler: ModelWrapValidatorHandler[Self]) -> Self:
        """@spec AGENT-ROLE-FILTER-V1-3, AGENT-ROLE-FILTER-V1-6."""
        if isinstance(value, cls):
            # Revalidation otherwise turns omitted, unvalidated defaults into
            # explicit nulls. Keep raw nested values so their validators run.
            fields = dict(value.__dict__)
            for name in ("invocation", "approver"):
                if name not in value.model_fields_set and fields.get(name) is None:
                    fields.pop(name, None)
            value = fields
        return handler(value)


class AgentRolePolicyV1(FactsModel):
    """@spec AGENT-ROLE-FILTER-V1-3, AGENT-ROLE-FILTER-V1-4."""

    schema_version: SchemaVersion
    tenant_id: CanonicalUUID
    agent_id: CanonicalUUID
    version: int = Field(ge=1, le=2_147_483_647)
    roles: AgentRolesV1

    @model_validator(mode="after")
    def coherent_namespaces(self) -> Self:
        """@spec AGENT-ROLE-FILTER-V1-6."""
        entries = []
        for role in (self.roles.invocation, self.roles.approver):
            if role is not None and role.filter is not None:
                entries.extend(role.filter.entries)
        _consistent_namespaces(entries)
        return self


class SetRoleFilterV1(FactsModel):
    """@spec AGENT-ROLE-FILTER-V1-4."""

    schema_version: SchemaVersion
    tenant_id: CanonicalUUID
    agent_id: CanonicalUUID
    expected_version: int = Field(ge=0, le=2_147_483_647)
    role: Literal["invocation", "approver"]
    operation: Literal["set_filter"]
    filter: RoleFilterV1 | None


class MigrateRoleV1(FactsModel):
    """@spec AGENT-ROLE-FILTER-V1-4."""

    schema_version: SchemaVersion
    tenant_id: CanonicalUUID
    agent_id: CanonicalUUID
    expected_version: int = Field(ge=0, le=2_147_483_647)
    role: Literal["invocation", "approver"]
    operation: Literal["migrate_role"]
    filter: RoleFilterV1 | None
    raw_account_revocation: Literal["separate_removal_required"]


AgentRoleChangeV1 = Annotated[SetRoleFilterV1 | MigrateRoleV1, Field(discriminator="operation")]


class RoleFilterRefusal(FactsModel):
    """@spec AGENT-ROLE-FILTER-V1-6."""

    code: Literal[
        "invalid_agent_role_configuration", "unsupported_agent_role_configuration_version"
    ]
