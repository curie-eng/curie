"""Exact grant checks for HTTP-selected resumes through the actual kernel.

Real Valkey, substrate and in-process runner; the core API and the SQL snapshot
are synthetic boundaries with their own real-PG and API proofs. These cases
prove worker ordering and that only the checked snapshot reaches boot.
@spec HTTP-APPROVAL-CONTINUITY-12.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import uuid
from typing import Any

import pytest
from curie_internal.identity_approval import (
    ApprovalGrantAuthorityRequestV1,
    ApprovalGrantAuthorityResponseV1,
    ApprovalGrantExpectationV1,
)
from curie_internal.identity_authority import TurnAuthorityRequestV1, TurnAuthorityResponseV1
from curie_worker.binding import GRANT_ARGUMENTS_ENV, GRANT_TOOL_ENV, RESUMED_KIND_ENV
from curie_worker.identity_authority import TurnAuthorityUnavailable

from ..queue_fixtures import qevent
from .test_http_identity_authority import CAPSULE, ORIGINAL_HASH, Binding, Core

ARGUMENTS = '{"a":{"é":true},"z":[1,2.5,null]}'


def snapshot(binding: Binding, approval: str, **changes: Any) -> ApprovalGrantExpectationV1:
    """@spec HTTP-APPROVAL-CONTINUITY-12: what the binding owner reads from the row."""
    values: dict[str, Any] = {
        "approval_id": approval, "agent_id": str(binding.identity.agent_id),
        "conversation_id": "1760000000.000001", "gate_kind": "permission",
        "granted_tool": "synthetic_read", "granted_arguments_json": ARGUMENTS,
    }
    values.update(changes)
    return ApprovalGrantExpectationV1.model_validate(values)


class GrantBinding(Binding):
    """Synthetic SQL snapshot owner; legacy readers record any use.

    @spec HTTP-APPROVAL-CONTINUITY-12.
    """

    def __init__(self, value: Any = None, *, fault: BaseException | None = None) -> None:
        """@spec HTTP-APPROVAL-CONTINUITY-12."""
        super().__init__()
        self.value = value
        self.snapshot_fault = fault
        self.snapshots: list[tuple[str, Any]] = []
        self.legacy: list[str] = []

    async def approval_grant_snapshot(self, event_id: str, agent_id: Any) -> Any:
        """@spec HTTP-APPROVAL-CONTINUITY-12."""
        self.snapshots.append((event_id, agent_id))
        if self.snapshot_fault is not None:
            raise self.snapshot_fault
        return self.value

    async def approval_grant_tool(self, *_args: Any) -> str:
        """@spec HTTP-APPROVAL-CONTINUITY-12: a widened legacy value must never be used."""
        self.legacy.append("tool")
        return "legacy_widened"

    async def approval_grant_arguments(self, *_args: Any) -> dict[str, Any]:
        """@spec HTTP-APPROVAL-CONTINUITY-12."""
        self.legacy.append("arguments")
        return {"path": "legacy"}

    async def approval_resumed_kind(self, *_args: Any) -> str:
        """@spec HTTP-APPROVAL-CONTINUITY-12."""
        self.legacy.append("kind")
        return "legacy"


class GrantCore(Core):
    """Synthetic external core: resume checks continue; grants follow the test's decision.

    @spec HTTP-APPROVAL-CONTINUITY-12.
    """

    def __init__(self, binding: Binding, *, grant: str = "continue") -> None:
        """@spec HTTP-APPROVAL-CONTINUITY-12."""
        super().__init__()
        self.binding = binding
        self.grant = grant
        self.grants: list[ApprovalGrantAuthorityRequestV1] = []
        self.order: list[tuple[str, int]] = []

    async def check(self, request: TurnAuthorityRequestV1) -> TurnAuthorityResponseV1:
        """@spec HTTP-APPROVAL-CONTINUITY-12."""
        self.order.append((request.stage, len(self.binding.boots)))
        result = await super().check(request)
        return result.model_copy(update={"decision": "continue", "reason": "current_authority"})

    async def check_grant(
        self, request: ApprovalGrantAuthorityRequestV1,
    ) -> ApprovalGrantAuthorityResponseV1:
        """@spec HTTP-APPROVAL-CONTINUITY-12: external boundary, not an offline permit."""
        assert type(request) is ApprovalGrantAuthorityRequestV1
        self.grants.append(request)
        self.order.append(("grant", len(self.binding.boots)))
        if self.grant == "unavailable":
            raise TurnAuthorityUnavailable()
        check = request.check
        decision, reason = {
            "continue": ("continue", "current_authority"),
            "refuse": ("refuse", "caller_not_allowed"),
            "wrong-digest": ("continue", "current_authority"),
        }[self.grant]
        encoded = json.dumps(
            request.grant.model_dump(mode="json"), sort_keys=True, separators=(",", ":"),
            ensure_ascii=False, allow_nan=False,
        )
        return ApprovalGrantAuthorityResponseV1(
            schema_version=1,
            check=TurnAuthorityResponseV1(
                schema_version=1, request_id=check.request_id, stage=check.stage,
                event_id=check.event_id,
                context_sha256=hashlib.sha256(check.context.encode("ascii")).hexdigest(),
                scope=check.scope, decision=decision, reason=reason,
            ),
            grant_sha256=(
                "0" * 64 if self.grant == "wrong-digest"
                else hashlib.sha256(encoded.encode("utf-8")).hexdigest()
            ),
        )


async def resume(h: Any, approval: str) -> Any:
    """@spec HTTP-APPROVAL-CONTINUITY-12: the existing parser derives the approval ID."""
    event = qevent("approved system message", event_id=f"approval-{approval}-resolved")
    await h.kernel.process_event(
        event, http_identity_context=CAPSULE, original_payload_sha256=ORIGINAL_HASH,
    )
    return event


def grant_envs(h: Any) -> list[tuple[Any, Any, Any]]:
    """@spec HTTP-APPROVAL-CONTINUITY-12: the actual boot environments handed to claims."""
    return [
        (env.get(GRANT_TOOL_ENV), env.get(GRANT_ARGUMENTS_ENV), env.get(RESUMED_KIND_ENV))
        for env in h.fake_k8s.claim_envs if env is not None
    ]


def test_checked_snapshot_is_the_only_grant_that_reaches_boot(make_harness: Any) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-12: after prepare, before boot_env, exact string."""
    async def run() -> None:
        """@spec HTTP-APPROVAL-CONTINUITY-12."""
        approval = str(uuid.uuid4())
        binding = GrantBinding()
        binding.value = snapshot(binding, approval)
        core = GrantCore(binding)
        async with make_harness(binding=binding, identity_authority=core) as h:
            await resume(h, approval)
            expected = (f"approval-{approval}-resolved", binding.resolved.agent_id)
            assert binding.snapshots == [expected]
            assert len(core.grants) == 1
            request = core.grants[0]
            assert request.grant == binding.value
            assert request.check.stage == "prepare" and request.check.approval_id == approval
            assert request.check.context == CAPSULE
            assert request.check.request_id != core.requests[0].request_id
            stages = [stage for stage, _ in core.order]
            assert stages[0] == "prepare" and stages.index("grant") > 0
            assert dict(core.order)["grant"] == 0, "grant checked after boot_env consumed it"
            assert "claim_or_resume" in stages[stages.index("grant"):]
            assert grant_envs(h) == [("synthetic_read", ARGUMENTS, "permission")]
            assert binding.legacy == []
    asyncio.run(run())


def test_policy_snapshot_keeps_manifest_scope_without_arguments(make_harness: Any) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-12: a policy grant is the stored manifest tool only."""
    async def run() -> None:
        """@spec HTTP-APPROVAL-CONTINUITY-12."""
        approval = str(uuid.uuid4())
        binding = GrantBinding()
        binding.value = snapshot(
            binding, approval, gate_kind="policy", granted_tool="close_issue",
            granted_arguments_json=None,
        )
        core = GrantCore(binding)
        async with make_harness(binding=binding, identity_authority=core) as h:
            await resume(h, approval)
            assert len(core.grants) == 1
            assert grant_envs(h) == [("close_issue", None, "policy")]
            assert binding.legacy == []
    asyncio.run(run())


def test_non_grant_resume_reads_no_legacy_grant_and_asks_for_none(make_harness: Any) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-12: rejected/expired carry no tool or arguments."""
    async def run() -> None:
        """@spec HTTP-APPROVAL-CONTINUITY-12."""
        approval = str(uuid.uuid4())
        binding = GrantBinding(None)
        core = GrantCore(binding)
        async with make_harness(binding=binding, identity_authority=core) as h:
            await resume(h, approval)
            assert len(binding.snapshots) == 1 and core.grants == []
            assert grant_envs(h) == [(None, None, None)]
            assert binding.legacy == []
    asyncio.run(run())


@pytest.mark.parametrize("outcome", ["refuse", "unavailable", "wrong-digest"])
def test_grant_without_a_continue_for_that_snapshot_boots_nothing(
    make_harness: Any, outcome: str,
) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-12: refusal is terminal; unknown stays pending."""
    async def run() -> None:
        """@spec HTTP-APPROVAL-CONTINUITY-12."""
        approval = str(uuid.uuid4())
        binding = GrantBinding()
        binding.value = snapshot(binding, approval)
        core = GrantCore(binding, grant=outcome)
        async with make_harness(binding=binding, identity_authority=core) as h:
            event = await resume(h, approval)
            assert len(core.grants) == 1
            assert binding.boots == [] and h.fake_k8s.claim_envs == []
            assert h.runner.opened == [] and binding.legacy == []
            done = await h.async_redis.exists(h.config.done_key(event.event_id))
            assert bool(done) is (outcome == "refuse")
    asyncio.run(run())


@pytest.mark.parametrize(
    "defect",
    ["permission-without-arguments", "permission-without-tool", "other-approval", "other-agent",
     "not-a-snapshot"],
)
def test_malformed_snapshot_refuses_before_asking_or_booting(
    make_harness: Any, defect: str,
) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-12: explicit non-null scope; no retagged snapshot."""
    async def run() -> None:
        """@spec HTTP-APPROVAL-CONTINUITY-12."""
        approval = str(uuid.uuid4())
        binding = GrantBinding()
        value: Any = {
            "permission-without-arguments": lambda: snapshot(
                binding, approval, granted_arguments_json=None,
            ),
            "permission-without-tool": lambda: snapshot(binding, approval, granted_tool=None),
            "other-approval": lambda: snapshot(binding, str(uuid.uuid4())),
            "other-agent": lambda: snapshot(binding, approval, agent_id=str(uuid.uuid4())),
            "not-a-snapshot": lambda: snapshot(binding, approval).model_dump(),
        }[defect]()
        binding.value = value
        core = GrantCore(binding)
        async with make_harness(binding=binding, identity_authority=core) as h:
            event = await resume(h, approval)
            assert core.grants == []
            assert binding.boots == [] and h.fake_k8s.claim_envs == []
            assert await h.async_redis.exists(h.config.done_key(event.event_id))
    asyncio.run(run())


@pytest.mark.parametrize("fault", ["malformed-row", "read-failure"])
def test_snapshot_read_faults_never_fall_back_to_legacy_grants(
    make_harness: Any, fault: str,
) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-12: malformed refuses; an unknown read stays pending."""
    async def run() -> None:
        """@spec HTTP-APPROVAL-CONTINUITY-12."""
        approval = str(uuid.uuid4())
        error: BaseException = (
            ValueError("invalid_approval_grant") if fault == "malformed-row"
            else RuntimeError("synthetic snapshot read failure")
        )
        binding = GrantBinding(fault=error)
        core = GrantCore(binding)
        async with make_harness(binding=binding, identity_authority=core) as h:
            event = await resume(h, approval)
            assert core.grants == [] and binding.legacy == []
            assert binding.boots == [] and h.fake_k8s.claim_envs == []
            done = await h.async_redis.exists(h.config.done_key(event.event_id))
            assert bool(done) is (fault == "malformed-row")
    asyncio.run(run())
