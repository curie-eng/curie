"""Runtime compatibility refuses before boot and preserves approval ownership.

These use the existing real-Valkey kernel harness. Pure HTTP/adoption proofs live
in test_workitem_dispatch_client.py and cannot replace this runtime rung.
"""

import asyncio
import uuid
from dataclasses import replace

from aci_protocol import Final, SessionStatus

from apps.worker.tests.kernel.test_work_item_early_stop import (
    AGENT_ID,
    DEPLOYMENT_ID,
    ISSUE_PROMPT,
    _Approvals,
    _Binding,
    _done,
    _PublicationApi,
    _tool,
    _turn,
    _WorkItems,
    _Workspace,
)

VERSION = uuid.UUID("33333333-3333-4333-8333-333333333333")
RECEIPT = {
    "interface": "curie/input-contracts@1",
    "contract": "curie.factory-specification/v1",
    "deployment_id": str(DEPLOYMENT_ID),
    "version_id": str(VERSION),
    "bundle_sha256": "a" * 64,
    "body_sha256": "b" * 64,
}


class VersionedItems(_WorkItems):
    def __init__(self) -> None:
        super().__init__()
        self.deferrals: list[dict[str, object]] = []

    async def defer(self, request_id, **kwargs):
        self.calls.append("defer")
        self.deferrals.append(kwargs)
        return None

    async def acquire(self, request_id, *, owner, generation, wake, context):
        grant = await super().acquire(
            request_id, owner=owner, generation=generation, wake=wake, context=context
        )
        grant = replace(grant, input_contract_receipt=dict(RECEIPT))
        self.acquisitions[request_id] = grant
        return grant

    async def running_for_conversation(self, conversation_id, *, wake):
        running = await super().running_for_conversation(conversation_id, wake=wake)
        if running is None:
            return None
        return replace(running, input_contract_receipt=dict(RECEIPT))


class VersionedBinding(_Binding):
    def __init__(self) -> None:
        self.rolled_out = False
        self.boots: list[dict[str, object]] = []
        self.pinned_reads: list[uuid.UUID] = []

    async def resolve(self, kind, adapter, channel):
        resolved = await super().resolve(kind, adapter, channel)
        resolved.version_id = VERSION
        resolved.bundle_sha256 = "a" * 64
        if self.rolled_out:
            resolved.deployment_id = uuid.UUID("44444444-4444-4444-8444-444444444444")
            resolved.version_id = uuid.UUID("55555555-5555-4555-8555-555555555555")
            resolved.bundle_sha256 = "c" * 64
            resolved.endpoint = "https://current-adapter.example.test/replies"
            resolved.adapter = "fixture-current-adapter"
        return resolved

    async def resolve_factory(self, context):
        return await self.resolve(context.kind, context.adapter, context.address)

    async def resolve_deployment(self, agent_id, deployment_id):
        assert agent_id == AGENT_ID
        self.pinned_reads.append(deployment_id)
        assert deployment_id == DEPLOYMENT_ID
        resolved = await super().resolve("slack", None, "fixture")
        resolved.version_id = VERSION
        resolved.bundle_sha256 = "a" * 64
        return resolved

    def boot_env(self, resolved, thread_key, **kwargs):
        self.boots.append(
            {"deployment_id": resolved.deployment_id, "version_id": resolved.version_id, **kwargs}
        )
        return super().boot_env(resolved, thread_key, **kwargs)


def test_new_versioned_execution_mismatch_defers_before_boot_or_runner(make_harness) -> None:
    async def exercise() -> None:
        binding = VersionedBinding()
        binding.rolled_out = True
        async with make_harness(binding=binding, workspace_factory=_Workspace) as harness:
            items = VersionedItems()
            harness.kernel._work_items = items
            event = _turn(f"work-item-{uuid.uuid4()}-execute-1", ISSUE_PROMPT)
            await harness.kernel.process_event(event)
            assert "defer" in items.calls
            assert len(items.deferrals) == 1
            assert items.deferrals[0]["reason"] == "input_contract_mismatch"
            assert items.deferrals[0]["capacity"] is False
            assert "start" not in items.calls
            assert harness.runner.opened == []
            assert binding.boots == []

    asyncio.run(exercise())


def test_held_execution_keeps_its_original_deployment_after_rollout(make_harness) -> None:
    async def exercise() -> None:
        binding = VersionedBinding()
        async with make_harness(
            binding=binding,
            workspace_factory=_Workspace,
            approvals=_Approvals(),
            publication_creator=_PublicationApi(),
        ) as harness:
            items = VersionedItems()
            harness.kernel._work_items = items
            harness.runner.turn_scripts = [
                [
                    _tool("Bash"),
                    Final(
                        text="Request approval",
                        status=SessionStatus.AWAITING_APPROVAL,
                        approval_summary="Run requested command",
                        approval_gate_kind="permission",
                        approval_granted_tool="Bash",
                    ),
                ],
                [_done("Resumed. Nothing to publish.")],
            ]
            await harness.kernel.process_event(
                _turn(f"work-item-{uuid.uuid4()}-execute-1", ISSUE_PROMPT)
            )
            binding.rolled_out = True
            resume_reply_start = len(harness.sink.events)
            await harness.kernel.process_event(
                _turn(
                    f"approval-{uuid.uuid4()}-resolved",
                    "[approval resolved] approved",
                    placeholder="fixture-approval",
                )
            )
            assert len(harness.runner.opened) == 2
            assert items.calls.count("start") == 1
            assert binding.pinned_reads == [DEPLOYMENT_ID]
            assert binding.boots[-1]["deployment_id"] == DEPLOYMENT_ID
            assert binding.boots[-1]["version_id"] == VERSION
            assert binding.boots[-1]["input_contract_enforcement"] == "curie/input-contracts@1"
            reply_routes = [
                route for _, route, _ in harness.sink.events[resume_reply_start:]
            ]
            assert reply_routes, "approval continuation must emit its resumed reply"
            assert all(
                route.endpoint == "https://current-adapter.example.test/replies"
                and route.adapter == "fixture-current-adapter"
                for route in reply_routes
            )

    asyncio.run(exercise())
