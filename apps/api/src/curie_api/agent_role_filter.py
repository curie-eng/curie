"""Pure role configuration boundaries. @spec AGENT-ROLE-FILTER-V1-1 to AGENT-ROLE-FILTER-V1-6."""

import json
from typing import Any

from pydantic import TypeAdapter

from curie_api.schemas.agent_access import (
    AgentRoleChangeV1,
    AgentRolePolicyV1,
    RoleConfigurationV1,
    RoleFilterRefusal,
)
from curie_api.schemas.http_identity import decode_json_bytes

MAX_CONFIGURATION_BYTES = 65_536
_ROLE: TypeAdapter[RoleConfigurationV1] = TypeAdapter(RoleConfigurationV1)
_CHANGE: TypeAdapter[AgentRoleChangeV1] = TypeAdapter(AgentRoleChangeV1)
_INVALID = RoleFilterRefusal(code="invalid_agent_role_configuration")
_UNSUPPORTED = RoleFilterRefusal(code="unsupported_agent_role_configuration_version")


def _version(value: Any) -> RoleFilterRefusal | None:
    """@spec AGENT-ROLE-FILTER-V1-6."""
    if type(value) is not dict or type(value.get("schema_version")) is not int:
        return _INVALID
    return None if value["schema_version"] == 1 else _UNSUPPORTED


def parse_agent_role_policy_v1(data: bytes) -> AgentRolePolicyV1 | RoleFilterRefusal:
    """@spec AGENT-ROLE-FILTER-V1-1 to AGENT-ROLE-FILTER-V1-6."""
    try:
        value = decode_json_bytes(data, max_bytes=MAX_CONFIGURATION_BYTES)
        refusal = _version(value)
        if refusal is not None:
            return refusal
        # Validate the envelope with opaque role content. This uses the same
        # field constraints as the final model without scanning nested versions.
        AgentRolePolicyV1.model_validate(dict(value, roles={}))
        roles = value.get("roles")
        if type(roles) is not dict or roles.keys() - {"invocation", "approver"}:
            return _INVALID
        for name in ("invocation", "approver"):
            if name in roles:
                refusal = _version(roles[name])
                if refusal is not None:
                    return refusal
                _ROLE.validate_python(roles[name])
        return AgentRolePolicyV1.model_validate(value)
    except (ValueError, TypeError, RecursionError):
        return _INVALID


def parse_agent_role_change_v1(data: bytes) -> AgentRoleChangeV1 | RoleFilterRefusal:
    """@spec AGENT-ROLE-FILTER-V1-1, AGENT-ROLE-FILTER-V1-4 to AGENT-ROLE-FILTER-V1-6."""
    try:
        value = decode_json_bytes(data, max_bytes=MAX_CONFIGURATION_BYTES)
        refusal = _version(value)
        if refusal is not None:
            return refusal
        if "filter" not in value:
            return _INVALID
        _CHANGE.validate_python(dict(value, filter=None))
        return _CHANGE.validate_python(value)
    except (ValueError, TypeError, RecursionError):
        return _INVALID


def render_agent_role_policy_v1(policy: AgentRolePolicyV1) -> bytes | RoleFilterRefusal:
    """@spec AGENT-ROLE-FILTER-V1-3, AGENT-ROLE-FILTER-V1-4, AGENT-ROLE-FILTER-V1-6."""
    try:
        if not isinstance(policy, AgentRolePolicyV1):
            return _INVALID
        validated = AgentRolePolicyV1.model_validate(policy)
        rendered = json.dumps(
            validated.model_dump(mode="json"), ensure_ascii=False, separators=(",", ":"),
        ).encode("utf-8")
        return rendered if len(rendered) <= MAX_CONFIGURATION_BYTES else _INVALID
    except (ValueError, TypeError, RecursionError):
        return _INVALID
