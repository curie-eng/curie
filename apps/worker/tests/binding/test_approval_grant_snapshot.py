"""BindingResolver.approval_grant_snapshot against the real compose Postgres.

The snapshot is one coherent read of an approved row's exact effect fields,
detached into an immutable scalar model before the read is released. It is a
value to submit to the current core check, never a grant by itself.
@spec HTTP-APPROVAL-CONTINUITY-12.
"""

from __future__ import annotations

import asyncio
import uuid
from typing import Any

import pytest
from curie_api.resumequeue import resume_event_id
from curie_internal.identity_approval import ApprovalGrantExpectationV1
from curie_test_support.postgres import pg_connect_or_skip
from sqlalchemy.ext.asyncio import create_async_engine

from .test_approval_grant import _DB_URL, _cleanup_agents, _resolver, _seed_agent, _seed_approval

_UNSET = object()


def _snapshot(
    *, status: str = "approved", gate_kind: Any = "permission",
    granted_tool: Any = "synthetic_read", granted_arguments: Any = _UNSET,
    owner: str = "same",
) -> Any:
    """Seed one row and read it through the real resolver. @spec HTTP-APPROVAL-CONTINUITY-12."""
    result: dict[str, Any] = {}

    async def go() -> None:
        """@spec HTTP-APPROVAL-CONTINUITY-12."""
        engine = create_async_engine(_DB_URL)
        try:
            await pg_connect_or_skip(engine)
            agent_id, other = uuid.uuid4(), uuid.uuid4()
            approval_id = uuid.uuid4()
            await _seed_agent(engine, agent_id)
            await _seed_agent(engine, other)
            seed: dict[str, Any] = {"gate_kind": gate_kind, "granted_tool": granted_tool}
            if granted_arguments is not _UNSET:
                seed["granted_arguments"] = granted_arguments
            await _seed_approval(
                engine, approval_id=approval_id, agent_id=agent_id, status=status,
                summary="Approve the synthetic bounded operation", **seed,
            )
            try:
                resolver = _resolver(engine)
                asking = other if owner == "other" else agent_id
                try:
                    result["value"] = await resolver.approval_grant_snapshot(
                        resume_event_id(approval_id), asking,
                    )
                except ValueError as error:
                    result["error"] = str(error)
                result["approval_id"] = str(approval_id)
                result["agent_id"] = str(agent_id)
                result["conversation_id"] = f"th-{approval_id.hex[:8]}"
            finally:
                await _cleanup_agents(engine, [agent_id, other])
        finally:
            await engine.dispose()

    asyncio.run(go())
    return result


@pytest.mark.parametrize(
    ("arguments", "encoded"),
    [
        ({}, "{}"),
        ({"z": [1, 2.5, None], "a": {"é": True}}, '{"a":{"é":true},"z":[1,2.5,null]}'),
    ],
    ids=["empty-object", "canonical-nested"],
)
def test_approved_permission_row_is_one_detached_canonical_snapshot(
    arguments: dict[str, Any], encoded: str,
) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-12: exact tool and canonical non-null arguments."""
    result = _snapshot(granted_arguments=arguments)
    value = result["value"]
    assert type(value) is ApprovalGrantExpectationV1
    assert value.model_dump() == {
        "approval_id": result["approval_id"], "agent_id": result["agent_id"],
        "conversation_id": result["conversation_id"], "gate_kind": "permission",
        "granted_tool": "synthetic_read", "granted_arguments_json": encoded,
    }
    assert all(
        type(item) in {str, type(None)} for item in value.model_dump().values()
    ), "snapshot retains a mutable reference"
    with pytest.raises(Exception):  # noqa: B017 - frozen models refuse assignment
        value.granted_tool = "widened"  # type: ignore[misc]


def test_policy_row_keeps_its_manifest_scope_without_arguments() -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-12: policy resumes keep manifest-defined scope."""
    value = _snapshot(gate_kind="policy", granted_tool=None)["value"]
    assert type(value) is ApprovalGrantExpectationV1
    assert (value.gate_kind, value.granted_tool, value.granted_arguments_json) == (
        "policy", None, None,
    )


@pytest.mark.parametrize("status", ["pending", "rejected", "expired"])
def test_only_an_approved_row_has_a_grant_snapshot(status: str) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-12: rejected/expired carry no tool or arguments."""
    result = _snapshot(status=status, granted_arguments={"path": "synthetic"})
    assert "error" not in result and result["value"] is None


def test_other_agent_never_receives_the_snapshot() -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-12: the matching agent is required in the same read."""
    result = _snapshot(granted_arguments={}, owner="other")
    assert "error" not in result and result["value"] is None


@pytest.mark.parametrize(
    "seed",
    [
        {"gate_kind": None, "granted_arguments": {}},
        {"granted_arguments": ["not", "an", "object"]},
        {"granted_arguments": {"pad": "x" * 16_384}},
    ],
    ids=["missing-gate-kind", "non-object-arguments", "oversized-arguments"],
)
def test_mixed_or_malformed_effect_fields_refuse_instead_of_degrading(
    seed: dict[str, Any],
) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-12: no summary fallback or silent no-grant."""
    result = _snapshot(**seed)
    assert result.get("error") == "invalid_approval_grant"
    assert "value" not in result


def test_non_approval_event_reads_nothing() -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-12: the existing parser selects the approval ID."""

    async def go() -> None:
        """@spec HTTP-APPROVAL-CONTINUITY-12."""
        engine = create_async_engine("postgresql+asyncpg://invalid:invalid@127.0.0.1:1/none")
        try:
            resolver = _resolver(engine)
            assert await resolver.approval_grant_snapshot("evt-plain", uuid.uuid4()) is None
        finally:
            await engine.dispose()

    asyncio.run(go())
