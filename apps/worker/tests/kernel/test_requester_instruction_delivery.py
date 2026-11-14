"""The kernel fails a request whose requester context is not acknowledged (#5592).

ADR 0278 section 3. Through the real kernel (real Valkey lock and markers, the
fake Kubernetes control plane, the in-process fake runner), a WorkItem execute
delivery reads its context envelope and posts it to the runner after the
request starts and before its first turn. Only an acknowledgment the API
accepts lets the turn open; every other outcome fails the started request with
a visible reason, opens no turn (so no model request) and still releases the
claim. Each refusal is paired with the acknowledged control.
"""

from __future__ import annotations

import asyncio
import sys
import uuid
from pathlib import Path
from typing import Any

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))

from test_work_item_workspace import (  # noqa: E402
    ISSUE_URL,
    _Binding,
    _NoExistingPublication,
    _turn,
    _WorkItems,
    _Workspace,
)

DIGEST = "a" * 64


class _RecordingWorkItems(_WorkItems):
    """The dispatch double, recording what the worker read and relayed."""

    def __init__(self, *, selected: bool = False, read_refusal: str | None = None) -> None:
        super().__init__()
        self.selected = selected
        self.read_refusal = read_refusal
        self.reads: list[tuple[uuid.UUID, int]] = []
        self.relayed: list[tuple[dict[str, Any] | None, str | None]] = []

    async def requester_context(self, request_id: uuid.UUID, *, runtime_epoch: int) -> Any:
        from curie_worker.workitem_dispatch import WorkItemConflict

        self.calls.append("requester_context")
        self.reads.append((request_id, runtime_epoch))
        if self.read_refusal is not None:
            raise WorkItemConflict(self.read_refusal)
        envelope = await super().requester_context(request_id, runtime_epoch=runtime_epoch)
        if self.selected:
            envelope["selection"] = {
                "kind": "selected",
                "principal_id": str(uuid.uuid4()),
                "revision": 1,
                "sha256": DIGEST,
                "format_version": 1,
                "text": "Prefer early returns.",
            }
        return envelope

    async def acknowledge_requester_context(
        self,
        request_id: uuid.UUID,
        *,
        runtime_epoch: int,
        acknowledgment: dict[str, Any] | None,
        reason: str | None,
    ) -> str:
        self.calls.append("acknowledge_requester_context")
        self.relayed.append((acknowledgment, reason))
        return await super().acknowledge_requester_context(
            request_id, runtime_epoch=runtime_epoch, acknowledgment=acknowledgment, reason=reason
        )


async def _execute(h: Any, work_items: _RecordingWorkItems) -> uuid.UUID:
    h.kernel._work_items = work_items
    request_id = uuid.uuid4()
    await h.kernel.process_event(_turn(f"work-item-{request_id}-execute-1", f"Resolve {ISSUE_URL}"))
    return request_id


@pytest.mark.parametrize("selected", [False, True])
def test_an_acknowledged_context_opens_the_first_turn(make_harness, selected: bool) -> None:
    async def exercise() -> None:
        async with make_harness(
            binding=_Binding(),
            workspace_factory=_Workspace,
            publication_creator=_NoExistingPublication(),
        ) as h:
            work_items = _RecordingWorkItems(selected=selected)

            request_id = await _execute(h, work_items)

            # Delivered once, after start and before the turn, for the started fence.
            assert work_items.reads == [(request_id, 1)]
            assert work_items.calls.index("start") < work_items.calls.index("requester_context")
            assert len(h.runner.requester_contexts) == 1
            assert h.runner.requester_contexts[0]["execution_fence"] == 1
            ((ack, reason),) = work_items.relayed
            assert ack is not None and reason is None
            assert ack["outcome"] == ("applied" if selected else "none")
            assert h.runner.opened, "the acknowledged request never opened its turn"
            # It ends on its own merits (here: no pull request), not on the context.
            ((finish),) = work_items.finishes
            assert finish["cause"] != "runner_escalated"

    asyncio.run(exercise())


@pytest.mark.parametrize(
    ("answer", "relayed_reason"),
    [
        # A runner without the route, as an older image answers.
        ((404, {"error": "not found"}), "runner_unacknowledged"),
        # A runner that expected no envelope.
        ((409, {"error": "not_expected"}), "runner_unacknowledged"),
        # A 200 without an acknowledgment.
        ((200, {}), "runner_unacknowledged"),
    ],
)
def test_a_runner_that_never_acknowledges_fails_the_request_before_model_work(
    make_harness, answer: tuple[int, dict[str, Any]], relayed_reason: str
) -> None:
    async def exercise() -> None:
        async with make_harness(
            binding=_Binding(),
            workspace_factory=_Workspace,
            publication_creator=_NoExistingPublication(),
        ) as h:
            h.runner.requester_context_answer = answer
            work_items = _RecordingWorkItems(selected=True)

            await _execute(h, work_items)

            assert work_items.relayed == [(None, relayed_reason)]
            _assert_failed_before_model_work(h, work_items, relayed_reason)

    asyncio.run(exercise())


def test_a_runner_refusal_is_relayed_with_its_reason(make_harness) -> None:
    async def exercise() -> None:
        async with make_harness(
            binding=_Binding(),
            workspace_factory=_Workspace,
            publication_creator=_NoExistingPublication(),
        ) as h:
            work_items = _RecordingWorkItems(selected=True)
            h.kernel._work_items = work_items
            request_id = uuid.uuid4()
            work_item_id = str(uuid.uuid5(uuid.UUID(int=0), str(request_id)))
            h.runner.requester_context_answer = (
                200,
                {
                    "acknowledgment": {
                        "envelope_version": 1,
                        "work_item_id": work_item_id,
                        "execution_request_id": str(request_id),
                        "execution_fence": 1,
                        "outcome": "failed",
                    },
                    "reason": "digest_mismatch",
                },
            )

            await h.kernel.process_event(
                _turn(f"work-item-{request_id}-execute-1", f"Resolve {ISSUE_URL}")
            )

            ((ack, reason),) = work_items.relayed
            assert ack is not None and ack["outcome"] == "failed"
            assert reason == "digest_mismatch"
            _assert_failed_before_model_work(h, work_items, "digest_mismatch")

    asyncio.run(exercise())


@pytest.mark.parametrize("refusal", ["tenant_disabled", "coding_instructions_unavailable"])
def test_an_envelope_the_api_refuses_fails_the_request_without_posting(
    make_harness, refusal: str
) -> None:
    async def exercise() -> None:
        async with make_harness(
            binding=_Binding(),
            workspace_factory=_Workspace,
            publication_creator=_NoExistingPublication(),
        ) as h:
            work_items = _RecordingWorkItems(selected=True, read_refusal=refusal)

            await _execute(h, work_items)

            # Nothing reached the runner, and the failure was relayed for evidence.
            assert h.runner.requester_contexts == []
            assert work_items.relayed == [(None, refusal)]
            _assert_failed_before_model_work(h, work_items, refusal)

    asyncio.run(exercise())


def _assert_failed_before_model_work(h: Any, work_items: _WorkItems, reason: str) -> None:
    assert h.runner.opened == [], "a turn opened without an acknowledged context"
    assert h.runner.queried == []
    ((finish),) = work_items.finishes
    assert finish["outcome"] == "failed"
    assert finish["cause"] == "runner_escalated"
    assert reason in str(finish["detail"])
    # The claim is released, as for any other settled request.
    assert h.fake_k8s.claims == {}
