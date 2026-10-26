"""Pure filter calculation. @spec AGENT-ROLE-LAYER-V1-1 to AGENT-ROLE-LAYER-V1-6."""

from datetime import datetime
from typing import Literal

from curie_api.agent_role_filter import parse_agent_role_policy_v1, render_agent_role_policy_v1
from curie_api.schemas.agent_access import (
    AgentRolePolicyV1,
    IssuerGroupSelectorV1,
    IssuerSubjectSelectorV1,
    RoleConfigurationV1,
    RoleFilterRefusal,
    SurfaceAccountSelectorV1,
)
from curie_api.schemas.http_identity import (
    CanonicalUUID,
    FactsModel,
    FactsRefusal,
    LookupRequestV1,
    SenderV1,
    ValidatedFacts,
    utc_clock_instant,
)

Reason = Literal[
    "unrestricted", "exact_selector", "exact_raw", "not_configured", "empty_filter",
    "no_selector_match", "invalid_response", "unsupported_version", "scope_mismatch",
    "identity_stale", "membership_stale", "unavailable", "invalid_agent_role_configuration",
    "unsupported_agent_role_configuration_version", "invalid_context", "invalid_facts",
    "forbidden_kind", "inactive_requires_migrated_raw", "unresolved_requires_raw",
    "independent_human_required",
]


class RoleFilterContext(FactsModel):
    """@spec AGENT-ROLE-LAYER-V1-1."""

    tenant_id: CanonicalUUID
    agent_id: CanonicalUUID
    role: Literal["invocation", "approver"]
    request: LookupRequestV1


class RoleFilterLayerResult(FactsModel):
    """@spec AGENT-ROLE-LAYER-V1-2."""

    status: Literal["match", "no_match", "refusal"]
    reason: Reason
    permission: Literal["not_evaluated"] = "not_evaluated"

    def __bool__(self) -> bool:
        """@spec AGENT-ROLE-LAYER-V1-2."""
        raise TypeError("role_filter_layer_is_not_permission")


def _refuse(reason: Reason) -> RoleFilterLayerResult:
    """@spec AGENT-ROLE-LAYER-V1-2, AGENT-ROLE-LAYER-V1-3."""
    return RoleFilterLayerResult(status="refusal", reason=reason)


def _coherent(facts: ValidatedFacts) -> bool:
    """@spec AGENT-ROLE-LAYER-V1-1, AGENT-ROLE-LAYER-V1-3."""
    identity = facts.identity
    no_membership = (
        facts.groups is None and facts.membership_observation is None
        and facts.membership_expires_at is None
    )
    if facts.outcome == "unresolved":
        return identity is None and no_membership
    if (
        identity is None or identity.issuer != facts.request.scope.expected_issuer
        or identity.kind != facts.sender_kind
    ):
        return False
    if facts.outcome == "inactive":
        return identity.account_status in ("disabled", "revoked") and no_membership
    return (
        identity.account_status == "active" and facts.groups is not None
        and facts.membership_observation is not None and facts.membership_expires_at is not None
        and all(group.issuer == facts.request.scope.expected_issuer for group in facts.groups)
    )


def _raw_matches(entry: SurfaceAccountSelectorV1, sender: SenderV1) -> bool:
    """@spec AGENT-ROLE-LAYER-V1-4."""
    return (
        entry.provider == sender.provider and entry.namespace == sender.namespace
        and entry.native_id == sender.native_id
    )


def _has_raw(role: RoleConfigurationV1 | None, sender: SenderV1) -> bool:
    """@spec AGENT-ROLE-LAYER-V1-4, AGENT-ROLE-LAYER-V1-5."""
    return role is not None and role.filter is not None and any(
        isinstance(entry, SurfaceAccountSelectorV1) and _raw_matches(entry, sender)
        for entry in tuple(role.filter.entries)
    )


def _active_match(role: RoleConfigurationV1 | None, facts: ValidatedFacts) -> RoleFilterLayerResult:
    """@spec AGENT-ROLE-LAYER-V1-4, AGENT-ROLE-LAYER-V1-5."""
    if role is None:
        return RoleFilterLayerResult(status="no_match", reason="not_configured")
    if role.filter is None:
        return RoleFilterLayerResult(status="match", reason="unrestricted")
    entries = tuple(role.filter.entries)
    if not entries:
        return RoleFilterLayerResult(status="no_match", reason="empty_filter")
    assert facts.identity is not None and facts.groups is not None
    identity = facts.identity
    groups = {(group.issuer, group.id) for group in facts.groups}
    for entry in entries:
        if isinstance(entry, SurfaceAccountSelectorV1):
            matches = _raw_matches(entry, facts.request.scope.sender)
        elif isinstance(entry, IssuerSubjectSelectorV1):
            matches = (entry.issuer, entry.subject) == (identity.issuer, identity.subject)
        elif isinstance(entry, IssuerGroupSelectorV1):
            matches = (entry.group.issuer, entry.group.id) in groups
        else:
            return _refuse("invalid_agent_role_configuration")
        if matches:
            return RoleFilterLayerResult(status="match", reason="exact_selector")
    return RoleFilterLayerResult(status="no_match", reason="no_selector_match")


def evaluate_role_filter_layer(
    policy: AgentRolePolicyV1 | RoleFilterRefusal | None,
    facts: ValidatedFacts | FactsRefusal,
    *,
    context: RoleFilterContext,
    now: datetime,
) -> RoleFilterLayerResult:
    """@spec AGENT-ROLE-LAYER-V1-1 to AGENT-ROLE-LAYER-V1-6."""
    if (
        not isinstance(context, RoleFilterContext)
        or not isinstance(facts, (ValidatedFacts, FactsRefusal))
        or (policy is not None and not isinstance(policy, (AgentRolePolicyV1, RoleFilterRefusal)))
    ):
        return _refuse("invalid_context")
    try:
        context = RoleFilterContext.model_validate(context)
        current = utc_clock_instant(now)
    except (ValueError, TypeError, RecursionError):
        return _refuse("invalid_context")

    if isinstance(policy, RoleFilterRefusal):
        try:
            return _refuse(RoleFilterRefusal.model_validate(policy).code)
        except (ValueError, TypeError, RecursionError):
            return _refuse("invalid_agent_role_configuration")
    if policy is not None:
        rendered = render_agent_role_policy_v1(policy)
        if isinstance(rendered, RoleFilterRefusal):
            return _refuse(rendered.code)
        detached = parse_agent_role_policy_v1(rendered)
        if isinstance(detached, RoleFilterRefusal):
            return _refuse(detached.code)
        policy = detached

    try:
        if isinstance(facts, FactsRefusal):
            return _refuse(FactsRefusal.model_validate(facts).code)
        facts = ValidatedFacts.model_validate(facts)
        if not _coherent(facts):
            return _refuse("invalid_facts")
    except (ValueError, TypeError, RecursionError):
        return _refuse("invalid_facts")

    purpose = "approval" if context.role == "approver" else "invocation"
    if (
        context.tenant_id != context.request.scope.tenant_id
        or context.request.purpose != purpose or facts.request != context.request
        or (policy is not None and (
            policy.tenant_id != context.tenant_id or policy.agent_id != context.agent_id
        ))
    ):
        return _refuse("scope_mismatch")
    if current > facts.identity_expires_at:
        return _refuse("identity_stale")
    if facts.membership_expires_at is not None and current > facts.membership_expires_at:
        return _refuse("membership_stale")
    if facts.sender_kind in ("service", "agent"):
        return _refuse("forbidden_kind")

    role = None if policy is None else getattr(policy.roles, context.role)
    if facts.outcome == "resolved":
        return _active_match(role, facts)
    raw_match = _has_raw(role, facts.request.scope.sender)
    if facts.outcome == "inactive":
        if role is not None and role.legacy_state == "migrated" and raw_match:
            return RoleFilterLayerResult(status="match", reason="exact_raw")
        return _refuse("inactive_requires_migrated_raw")
    return _refuse("independent_human_required" if raw_match else "unresolved_requires_raw")
