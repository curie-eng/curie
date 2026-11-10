"""Standalone role filter artifact. @spec AGENT-ROLE-FILTER-V1-1, AGENT-ROLE-FILTER-V1-7."""

import json
from pathlib import Path

from pydantic import TypeAdapter

from curie_api.schemas.agent_access import (
    AgentRoleChangeV1,
    AgentRolePolicyV1,
    RoleConfigurationV1,
    SelectorV1,
)


def render_json_schema() -> bytes:
    """@spec AGENT-ROLE-FILTER-V1-7."""
    roots, schema = TypeAdapter.json_schemas(
        [
            ("AgentRolePolicyV1", "validation", TypeAdapter(AgentRolePolicyV1)),
            ("AgentRoleChangeV1", "validation", TypeAdapter(AgentRoleChangeV1)),
            ("RoleConfigurationV1", "validation", TypeAdapter(RoleConfigurationV1)),
            ("SelectorV1", "validation", TypeAdapter(SelectorV1)),
        ],
        ref_template="#/$defs/{model}",
    )
    # Pydantic names model definitions itself; give the operation and selector
    # aliases stable local definitions as well, without replacing model refs.
    for (name, _mode), root in roots.items():
        schema["$defs"].setdefault(name, root)
    schema.update({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$id": "https://curietech.ai/schemas/agent-role-filter-v1.schema.json",
        "title": "Agent role filter representation v1",
        "description": (
            "Pure policy and one-role change representations; no runtime route or "
            "authorization result. Select a root through its local $defs reference. "
            "Byte limits, duplicate JSON object keys, integer-token spelling, "
            "refusal-code precedence, namespace descriptor consistency, inherited "
            "Issuer Unicode-NFKC validity and unpaired-surrogate rejection require "
            "the pure byte parser. JSON Schema integer semantics admit mathematically "
            "integral decimal and exponent values. Structural schema acceptance "
            "does not establish authenticated scope, persistence or migration execution."
        ),
    })
    return (json.dumps(schema, indent=2, sort_keys=True, ensure_ascii=True) + "\n").encode("utf-8")


def write_artifact() -> Path:
    """@spec AGENT-ROLE-FILTER-V1-7."""
    root = Path(__file__).resolve().parents[4]
    destination = root / "docs" / "interfaces" / "agent-access" / "agent-role-filter-v1.schema.json"
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_bytes(render_json_schema())
    return destination


if __name__ == "__main__":
    print(f"wrote {write_artifact()}")
