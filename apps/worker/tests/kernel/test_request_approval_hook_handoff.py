from __future__ import annotations

import asyncio
import uuid

import pytest
from curie_runner.approval import APPROVAL_TOOL_NAME

from .test_approval_lifecycle import RecordingApprovals
from .test_publish_hook_deny_handoff import _support
from .test_work_item_early_stop import (
    ISSUE_PROMPT,
    _Binding,
    _done,
    _PublicationApi,
    _turn,
    _WorkItems,
    _Workspace,
)


@pytest.mark.parametrize("allow_second", [False, True], ids=["deny", "deny-then-allow"])
def test_policy_approval_handoff_uses_the_executed_request(
    make_harness, tmp_path, monkeypatch, allow_second: bool
) -> None:
    support = _support(monkeypatch)
    turn = support.run_approval_turn(tmp_path, monkeypatch, allow_second=allow_second)
    assert support.APPROVAL_REFUSAL in turn.result_text(support.call_id(0))
    assert [event.tool for event in turn.events if event.type == "tool_note"] == [
        APPROVAL_TOOL_NAME,
        APPROVAL_TOOL_NAME,
    ]

    async def exercise() -> None:
        approvals = RecordingApprovals()
        publications = _PublicationApi()
        async with make_harness(
            binding=_Binding(),
            workspace_factory=_Workspace,
            approvals=approvals,
            publication_creator=publications,
        ) as h:
            items = _WorkItems()
            h.kernel._work_items = items
            h.runner.turn_scripts = [list(turn.events), [_done("Finished the review.")]]

            await h.kernel.process_event(_turn(f"work-item-{uuid.uuid4()}-execute-1", ISSUE_PROMPT))

            assert h.runner.opened
            assert not publications.creates
            if allow_second:
                assert approvals.create_calls == 1
                [request] = approvals.requests
                assert request.gate_kind == "policy"
                assert request.summary == support.APPROVED_SUMMARY
                assert items.calls.count("hold_for_approval") == 1
                assert not items.finishes
                assert len(h.runner.opened) == 1
            else:
                assert approvals.create_calls == 0
                assert "hold_for_approval" not in items.calls
                assert len(items.finishes) == 1

    asyncio.run(exercise())
