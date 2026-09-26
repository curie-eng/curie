"""A factory execution waits for node capacity instead of failing (#3169).

A sandbox the ResourceQuota admits but no node can place sits Pending with
``PodScheduled=False`` and reason ``Unschedulable``. For a work-item execution
that is the same condition as a quota refusal: the claim is released and the
request defers as capacity, keeping its place in the queue. A chat delivery
keeps today's claim-timeout behavior.
"""

from __future__ import annotations

import asyncio
import sys
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from test_work_item_workspace import (  # noqa: E402
    ISSUE_URL,
    _Binding,
    _turn,
    _WorkItems,
    _Workspace,
)

UNSCHEDULABLE = "0/1 nodes are available: 1 Insufficient cpu."


class _DeferringWorkItems(_WorkItems):
    def __init__(self) -> None:
        super().__init__()
        self.defers: list[tuple[uuid.UUID, dict[str, object]]] = []

    async def defer(self, request_id: uuid.UUID, **kwargs: object) -> None:
        self.calls.append("defer")
        self.defers.append((request_id, kwargs))


def test_an_unschedulable_execution_defers_as_capacity(make_harness) -> None:
    async def exercise() -> None:
        async with make_harness(
            binding=_Binding(),
            workspace_factory=_Workspace,
            claim_timeout_seconds=0.05,
        ) as h:
            work_items = _DeferringWorkItems()
            h.kernel._work_items = work_items
            h.fake_k8s.bind_ready = False
            h.fake_k8s.unschedulable_message = UNSCHEDULABLE
            request_id = uuid.uuid4()

            await h.kernel.process_event(
                _turn(f"work-item-{request_id}-execute-1", f"Resolve {ISSUE_URL}")
            )

            assert len(work_items.defers) == 1, work_items.calls
            deferred_id, defer = work_items.defers[0]
            assert deferred_id == request_id
            assert defer["reason"] == "capacity"
            assert defer["capacity"] is True
            assert "start" not in work_items.calls
            assert work_items.finishes == []
            assert h.runner.opened == []
            # The claim is released, never left Pending on the node.
            assert h.fake_k8s.claims == {}
            assert h.fake_k8s.deleted_claims

    asyncio.run(exercise())


def test_a_slow_but_scheduled_execution_keeps_the_claim_timeout(make_harness) -> None:
    async def exercise() -> None:
        async with make_harness(
            binding=_Binding(),
            workspace_factory=_Workspace,
            claim_timeout_seconds=0.05,
        ) as h:
            work_items = _DeferringWorkItems()
            h.kernel._work_items = work_items
            h.fake_k8s.bind_ready = False
            request_id = uuid.uuid4()

            await h.kernel.process_event(
                _turn(f"work-item-{request_id}-execute-1", f"Resolve {ISSUE_URL}")
            )

            # Today's unstarted, non-capacity defer: no capacity backoff.
            assert len(work_items.defers) == 1, work_items.calls
            _deferred_id, defer = work_items.defers[0]
            assert str(defer["reason"]).startswith("not_started:")
            assert defer["capacity"] is False
            assert h.runner.opened == []

    asyncio.run(exercise())


def test_an_unschedulable_chat_turn_is_unchanged(make_harness) -> None:
    async def observe(unschedulable: str | None) -> tuple[list[str], list[object]]:
        async with make_harness(
            binding=_Binding(),
            workspace_factory=_Workspace,
            claim_timeout_seconds=0.05,
        ) as h:
            work_items = _DeferringWorkItems()
            h.kernel._work_items = work_items
            h.fake_k8s.bind_ready = False
            h.fake_k8s.unschedulable_message = unschedulable

            await h.kernel.process_event(
                _turn(f"slack-{uuid.uuid4().hex}", f"What is {ISSUE_URL} about?")
            )

            assert h.runner.opened == []
            return work_items.calls, [type(event).__name__ for event in h.sink.events]

    async def exercise() -> None:
        baseline = await observe(None)
        unschedulable = await observe(UNSCHEDULABLE)
        assert "defer" not in unschedulable[0]
        assert unschedulable == baseline

    asyncio.run(exercise())
