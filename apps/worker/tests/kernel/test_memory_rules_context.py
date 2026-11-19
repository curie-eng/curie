"""Learned rules, worker to runner (#5798 decision 4, acceptance criterion 3).

Drives the real kernel (real Valkey lock and markers, the fake Kubernetes
control plane, the in-process fake runner) through a WorkItem execute delivery.
After the request starts, the worker delivers the requester context and then,
beside it, the learned rules envelope, and relays each acknowledgment before
the first turn. Only an acknowledgment the API accepts lets the turn open;
every other outcome fails the started request with a visible reason, opens no
turn (so no model request) and still releases the claim.

The dispatch double replays the API's envelope and acknowledgment shapes, and
the fake runner's route answers the shapes the runner's route answers.
"""

from __future__ import annotations

import asyncio
import sys
import uuid
from pathlib import Path
from typing import Any

import pytest
from curie_worker.memory_rules_context import PREFLIGHT_BLOCKED, MemoryRulesContextRefused
from curie_worker.requester_context import PREFLIGHT_BLOCKED as REQUESTER_PREFLIGHT_BLOCKED
from curie_worker.requester_context import RequesterContextRefused
from curie_worker.workitem_dispatch import WorkItemConflict

sys.path.insert(0, str(Path(__file__).resolve().parent))

from test_work_item_workspace import (  # noqa: E402
    ISSUE_URL,
    _Binding,
    _NoExistingPublication,
    _turn,
    _WorkItems,
    _Workspace,
)

RULES_TEXT = "R1: Name tests after the behavior they prove.\nR4: Keep migrations reversible."
RULES_DIGEST = "b" * 64
REQUESTER_DIGEST = "a" * 64


class _RecordingWorkItems(_WorkItems):
    """The dispatch double, recording what the worker read and relayed."""

    def __init__(
        self,
        *,
        selected: bool = False,
        requester_selected: bool = False,
        read_refusal: str | None = None,
        acknowledgment_refusal: str | None = None,
    ) -> None:
        super().__init__()
        self.selected = selected
        self.requester_selected = requester_selected
        self.read_refusal = read_refusal
        self.acknowledgment_refusal = acknowledgment_refusal
        self.reads: list[tuple[uuid.UUID, int]] = []
        self.relayed: list[tuple[dict[str, Any] | None, str | None]] = []
        self.requester_relayed: list[tuple[dict[str, Any] | None, str | None]] = []

    async def requester_context(self, request_id: uuid.UUID, *, runtime_epoch: int) -> Any:
        self.calls.append("requester_context")
        envelope = await super().requester_context(request_id, runtime_epoch=runtime_epoch)
        if self.requester_selected:
            envelope["selection"] = {
                "kind": "selected",
                "principal_id": str(uuid.uuid4()),
                "revision": 1,
                "sha256": REQUESTER_DIGEST,
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
        self.requester_relayed.append((acknowledgment, reason))
        return await super().acknowledge_requester_context(
            request_id, runtime_epoch=runtime_epoch, acknowledgment=acknowledgment, reason=reason
        )

    async def memory_rules_context(self, request_id: uuid.UUID, *, runtime_epoch: int) -> Any:
        self.calls.append("memory_rules_context")
        self.reads.append((request_id, runtime_epoch))
        if self.read_refusal is not None:
            raise WorkItemConflict(self.read_refusal)
        envelope = await super().memory_rules_context(request_id, runtime_epoch=runtime_epoch)
        if self.selected:
            envelope["selection"] = {
                "kind": "selected",
                "rule_numbers": [1, 4],
                "sha256": RULES_DIGEST,
                "format_version": 1,
                "text": RULES_TEXT,
            }
        return envelope

    async def acknowledge_memory_rules_context(
        self,
        request_id: uuid.UUID,
        *,
        runtime_epoch: int,
        acknowledgment: dict[str, Any] | None,
        reason: str | None,
    ) -> str:
        self.calls.append("acknowledge_memory_rules_context")
        self.relayed.append((acknowledgment, reason))
        if self.acknowledgment_refusal is not None and acknowledgment is not None:
            # The API refuses an acknowledgment for another execution, fence
            # or digest and keeps the evidence pending.
            raise WorkItemConflict(self.acknowledgment_refusal)
        return await super().acknowledge_memory_rules_context(
            request_id, runtime_epoch=runtime_epoch, acknowledgment=acknowledgment, reason=reason
        )


def _harness(make_harness: Any) -> Any:
    return make_harness(
        binding=_Binding(),
        workspace_factory=_Workspace,
        publication_creator=_NoExistingPublication(),
    )


async def _execute(h: Any, work_items: _RecordingWorkItems) -> uuid.UUID:
    h.kernel._work_items = work_items
    request_id = uuid.uuid4()
    await h.kernel.process_event(_turn(f"work-item-{request_id}-execute-1", f"Resolve {ISSUE_URL}"))
    return request_id


def _failed_ack(request_id: uuid.UUID) -> dict[str, Any]:
    return {
        "envelope_version": 1,
        "work_item_id": str(uuid.uuid5(uuid.UUID(int=0), str(request_id))),
        "execution_request_id": str(request_id),
        "execution_fence": 1,
        "outcome": "failed",
    }


def _assert_failed_before_model_work(h: Any, work_items: _RecordingWorkItems, detail: str) -> None:
    assert h.runner.opened == [], "a turn opened without acknowledged learned rules"
    assert h.runner.queried == []
    ((finish),) = work_items.finishes
    assert finish["outcome"] == "failed"
    assert finish["cause"] == "runner_escalated"
    assert finish["detail"] == detail
    # A refusal before any turn proves nothing about effects (ADR 0269).
    assert finish["retry_safety"] == "uncertain"
    assert finish["retry_condition"] is None
    # The claim is released, as for any other settled request.
    assert h.fake_k8s.claims == {}


def _rules_detail(reason: str) -> str:
    return MemoryRulesContextRefused(reason).detail


@pytest.mark.parametrize(
    ("selected", "requester_selected"), [(False, False), (True, False), (True, True)]
)
def test_learned_rules_are_acknowledged_beside_the_requester_context_before_the_first_turn(
    make_harness: Any, selected: bool, requester_selected: bool
) -> None:
    async def exercise() -> None:
        async with _harness(make_harness) as h:
            work_items = _RecordingWorkItems(
                selected=selected, requester_selected=requester_selected
            )

            request_id = await _execute(h, work_items)

            # Read once for the started fence, after the requester context was
            # acknowledged and before the first turn opened.
            assert work_items.reads == [(request_id, 1)]
            calls = work_items.calls
            assert (
                calls.index("start")
                < calls.index("acknowledge_requester_context")
                < calls.index("memory_rules_context")
                < calls.index("acknowledge_memory_rules_context")
            )
            assert h.runner.context_posts == [("requester", 0), ("memory_rules", 0)]
            (posted,) = h.runner.memory_rules_contexts
            assert posted["execution_fence"] == 1
            assert posted["execution_request_id"] == str(request_id)
            ((ack, reason),) = work_items.relayed
            assert ack is not None and reason is None
            assert ack["outcome"] == ("applied" if selected else "none")
            assert ack.get("sha256") == (RULES_DIGEST if selected else None)
            # The requester path is untouched by the learned rules beside it.
            ((requester_ack, _),) = work_items.requester_relayed
            assert requester_ack is not None
            assert requester_ack["outcome"] == ("applied" if requester_selected else "none")
            assert h.runner.opened, "the acknowledged request never opened its turn"
            # It ends on its own merits (here: no pull request), not on the rules.
            ((finish),) = work_items.finishes
            assert finish["cause"] != "runner_escalated"

    asyncio.run(exercise())


@pytest.mark.parametrize(
    "answer",
    [
        # An older runner without the route.
        (404, {"error": "not found"}),
        # A runner that does not serve the learned rules.
        (404, {"error": "memory_rules_context_not_served"}),
        # A 409 that is not the preflight blocked answer.
        (409, {"error": "requester_context_preflight_blocked"}),
        # A 200 without an acknowledgment.
        (200, {}),
    ],
)
def test_a_runner_that_never_acknowledges_the_rules_fails_the_request_before_model_work(
    make_harness: Any, answer: tuple[int, dict[str, Any]]
) -> None:
    async def exercise() -> None:
        async with _harness(make_harness) as h:
            h.runner.memory_rules_context_answer = answer
            work_items = _RecordingWorkItems(selected=True)

            await _execute(h, work_items)

            assert len(h.runner.memory_rules_contexts) == 1
            assert work_items.relayed == [(None, "runner_unacknowledged")]
            _assert_failed_before_model_work(h, work_items, _rules_detail("runner_unacknowledged"))

    asyncio.run(exercise())


def test_a_runner_refusal_of_the_rules_is_relayed_with_its_reason(make_harness: Any) -> None:
    async def exercise() -> None:
        async with _harness(make_harness) as h:
            work_items = _RecordingWorkItems(selected=True)
            h.kernel._work_items = work_items
            request_id = uuid.uuid4()
            h.runner.memory_rules_context_answer = (
                200,
                {"acknowledgment": _failed_ack(request_id), "reason": "too_large"},
            )

            await h.kernel.process_event(
                _turn(f"work-item-{request_id}-execute-1", f"Resolve {ISSUE_URL}")
            )

            ((ack, reason),) = work_items.relayed
            assert ack is not None and ack["outcome"] == "failed"
            assert reason == "too_large"
            detail = _rules_detail("too_large")
            assert (
                detail == "The learned rules could not be applied (too_large); no model work ran."
            )
            _assert_failed_before_model_work(h, work_items, detail)
            # The pinned text never reaches the terminal detail.
            assert RULES_TEXT not in detail

    asyncio.run(exercise())


def test_a_failed_acknowledgment_without_a_reason_fails_as_runner_refused(
    make_harness: Any,
) -> None:
    async def exercise() -> None:
        async with _harness(make_harness) as h:
            work_items = _RecordingWorkItems(selected=True)
            h.kernel._work_items = work_items
            request_id = uuid.uuid4()
            h.runner.memory_rules_context_answer = (
                200,
                {"acknowledgment": _failed_ack(request_id)},
            )

            await h.kernel.process_event(
                _turn(f"work-item-{request_id}-execute-1", f"Resolve {ISSUE_URL}")
            )

            ((ack, reason),) = work_items.relayed
            assert ack is not None and reason == "runner_refused"
            _assert_failed_before_model_work(h, work_items, _rules_detail("runner_refused"))

    asyncio.run(exercise())


def test_an_envelope_the_api_cannot_serve_fails_the_request_without_posting(
    make_harness: Any,
) -> None:
    async def exercise() -> None:
        async with _harness(make_harness) as h:
            work_items = _RecordingWorkItems(selected=True, read_refusal="memory_rules_unavailable")

            await _execute(h, work_items)

            # Nothing reached the runner, and the failure was relayed for evidence.
            assert h.runner.memory_rules_contexts == []
            assert work_items.relayed == [(None, "memory_rules_unavailable")]
            _assert_failed_before_model_work(
                h, work_items, _rules_detail("memory_rules_unavailable")
            )

    asyncio.run(exercise())


def test_a_mismatched_acknowledgment_records_the_failure_without_it(make_harness: Any) -> None:
    async def exercise() -> None:
        async with _harness(make_harness) as h:
            work_items = _RecordingWorkItems(
                selected=True, acknowledgment_refusal="acknowledgment_mismatch"
            )

            await _execute(h, work_items)

            # The runner's acknowledgment was refused, then the failure was
            # recorded with no acknowledgment attached.
            ((first, first_reason), (second, second_reason)) = work_items.relayed
            assert first is not None and first["outcome"] == "applied" and first_reason is None
            assert (second, second_reason) == (None, "acknowledgment_mismatch")
            _assert_failed_before_model_work(
                h, work_items, _rules_detail("acknowledgment_mismatch")
            )

    asyncio.run(exercise())


@pytest.mark.parametrize("selected", [False, True])
def test_a_preflight_blocked_runner_leaves_the_rules_evidence_and_answers_its_turn(
    make_harness: Any, selected: bool
) -> None:
    async def exercise() -> None:
        async with _harness(make_harness) as h:
            # A blocked start answers both routes the same way.
            h.runner.requester_context_answer = (409, {"error": REQUESTER_PREFLIGHT_BLOCKED})
            h.runner.memory_rules_context_answer = (409, {"error": PREFLIGHT_BLOCKED})
            work_items = _RecordingWorkItems(selected=selected)

            await _execute(h, work_items)

            # Posted once, nothing relayed: the evidence stays as admission left it.
            assert len(h.runner.memory_rules_contexts) == 1
            assert work_items.relayed == []
            assert h.runner.opened, "the blocked request never reached its explanation turn"
            ((finish),) = work_items.finishes
            assert finish["cause"] != "runner_escalated"

    asyncio.run(exercise())


def test_a_refused_requester_context_wins_and_the_rules_are_never_posted(
    make_harness: Any,
) -> None:
    async def exercise() -> None:
        async with _harness(make_harness) as h:
            # Both would refuse; the requester context is delivered first.
            h.runner.requester_context_answer = (404, {"error": "not found"})
            h.runner.memory_rules_context_answer = (404, {"error": "not found"})
            work_items = _RecordingWorkItems(selected=True)

            await _execute(h, work_items)

            assert h.runner.memory_rules_contexts == []
            assert work_items.relayed == []
            _assert_failed_before_model_work(
                h, work_items, RequesterContextRefused("runner_unacknowledged").detail
            )

    asyncio.run(exercise())
