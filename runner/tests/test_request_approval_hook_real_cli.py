from pathlib import Path

import pytest
from aci_protocol import SessionStatus
from curie_runner.approval import APPROVAL_TOOL_NAME
from runner_publish_hook_deny_support import (
    APPROVAL_REFUSAL,
    APPROVED_SUMMARY,
    REFUSED_REQUEST,
    SECOND_SUMMARY,
    call_id,
    run_approval_turn,
)


def test_bundle_deny_creates_no_policy_approval(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    turn = run_approval_turn(tmp_path, monkeypatch, allow_second=False)

    assert turn.emitted == [(call_id(i), REFUSED_REQUEST) for i in range(2)]
    for i in range(2):
        assert APPROVAL_REFUSAL in turn.result_text(call_id(i))
    assert [event.tool for event in turn.events if event.type == "tool_note"] == [
        APPROVAL_TOOL_NAME,
        APPROVAL_TOOL_NAME,
    ]
    assert not any(event.type == "side_effect_flag" for event in turn.events)
    assert turn.final.status is SessionStatus.DONE
    assert turn.final.approval_summary is None
    assert turn.final.approval_route is None
    assert turn.final.approval_gate_kind is None


def test_bundle_deny_then_allow_records_the_executed_policy_request(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    turn = run_approval_turn(tmp_path, monkeypatch, allow_second=True)

    assert turn.emitted == [
        (call_id(0), REFUSED_REQUEST),
        (call_id(1), {"summary": SECOND_SUMMARY}),
    ]
    assert APPROVAL_REFUSAL in turn.result_text(call_id(0))
    assert "Approval requested" in turn.result_text(call_id(1))
    assert turn.final.status is SessionStatus.AWAITING_APPROVAL
    assert turn.final.approval_gate_kind == "policy"
    assert turn.final.approval_route is None
    # The allow hook edits the second call's input before the tool runs.
    assert turn.final.approval_summary == APPROVED_SUMMARY
