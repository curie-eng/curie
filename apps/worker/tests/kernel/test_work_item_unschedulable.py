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
from typing import Any

import pytest
from curie_worker.capacity_wait import CapacityWaitRequested

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
    def __init__(self, terminal_cause: str | None = None) -> None:
        super().__init__()
        self.defers: list[tuple[uuid.UUID, dict[str, object]]] = []
        self.terminal_cause = terminal_cause

    async def defer(self, request_id: uuid.UUID, **kwargs: object) -> str | None:
        self.calls.append("defer")
        self.defers.append((request_id, kwargs))
        return self.terminal_cause


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


def test_unschedulable_chat_requests_capacity_wait_without_factory_defer(
    make_harness,
) -> None:
    async def observe(
        unschedulable: str | None,
    ) -> tuple[list[str], list[str], list[str]]:
        async with make_harness(
            binding=_Binding(),
            workspace_factory=_Workspace,
            claim_timeout_seconds=0.05,
            slack_no_edit_streaming=True,
        ) as h:
            work_items = _DeferringWorkItems()
            h.kernel._work_items = work_items
            h.fake_k8s.bind_ready = False
            h.fake_k8s.unschedulable_message = unschedulable

            event = _turn(f"slack-{uuid.uuid4().hex}", f"What is {ISSUE_URL} about?")
            if unschedulable is None:
                await h.kernel.process_event(event)
            else:
                with pytest.raises(CapacityWaitRequested):
                    await h.kernel.process_event(event)
                assert not await h.async_redis.exists(h.config.done_key(event.event_id))

            assert h.runner.opened == []
            return (
                work_items.calls,
                [text for _, _, text in h.sink.updates],
                [completion.event_id for completion in h.sink.completions],
            )

    async def exercise() -> None:
        baseline = await observe(None)
        unschedulable = await observe(UNSCHEDULABLE)
        assert "defer" not in unschedulable[0]
        assert "defer" not in baseline[0]
        assert unschedulable[1] == []
        assert unschedulable[2] == []
        assert baseline[1]
        assert baseline[2]

    asyncio.run(exercise())


@pytest.mark.parametrize("terminal_cause", ["start_failed", None])
def test_a_terminal_unstarted_defer_releases_the_thread_sandbox(
    make_harness, monkeypatch, terminal_cause: str | None
) -> None:
    """#4170: the fifth non-capacity start deferral ends the request
    ``start_failed``. An unstarted request gets no terminate wake, so, as in
    #3208, this delivery must hand its thread to the settled-run release. A
    deferral that leaves the request waiting must not: the next acquire adopts
    this thread's route."""

    async def exercise() -> None:
        async with make_harness(
            binding=_Binding(),
            workspace_factory=_Workspace,
            claim_timeout_seconds=0.05,
        ) as h:
            work_items = _DeferringWorkItems(terminal_cause)
            h.kernel._work_items = work_items
            h.fake_k8s.bind_ready = False
            releases: list[str] = []
            release = h.kernel._release_work_item_sandbox

            async def observed_release(run: Any) -> None:
                releases.append(run.thread_key)
                await release(run)

            monkeypatch.setattr(h.kernel, "_release_work_item_sandbox", observed_release)
            request_id = uuid.uuid4()

            await h.kernel.process_event(
                _turn(f"work-item-{request_id}-execute-1", f"Resolve {ISSUE_URL}")
            )

            assert len(work_items.defers) == 1, work_items.calls
            assert str(work_items.defers[0][1]["reason"]).startswith("not_started:")
            assert "start" not in work_items.calls
            assert h.runner.opened == []
            assert not h.kernel.owns_work_item(request_id)
            if terminal_cause is None:
                assert releases == []
            else:
                assert len(releases) == 1
                assert h.fake_k8s.claims == {}

    asyncio.run(exercise())
