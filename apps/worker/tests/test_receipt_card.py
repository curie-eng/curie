"""The receipt card: what a turn did, and what can be taken back."""

import json

from curie_worker.blocks import UNDO_ACTION_ID, receipt_card


def _undoable(**over):
    row = {
        "id": "11111111-1111-1111-1111-111111111111",
        "tool": "mcp__k8s-scale__scale_deployment",
        "summary": "scaled public/api from 3 to 10",
        "undoable": True,
        "irreversible_reason": None,
    }
    row.update(over)
    return row


def _final(**over):
    row = {
        "id": "22222222-2222-2222-2222-222222222222",
        "tool": "mcp__k8s-write__restart_deployment",
        "summary": "restarted public/api",
        "undoable": False,
        "irreversible_reason": "restarting pods cannot be undone",
    }
    row.update(over)
    return row


def test_both_kinds_of_line_are_on_the_same_card() -> None:
    """Hiding the irreversible ones would hide the lines that matter most."""

    fallback, blocks = receipt_card([_undoable(), _final()])
    rendered = json.dumps(blocks)
    assert "scaled public/api from 3 to 10" in rendered
    assert "restarting pods cannot be undone" in rendered
    assert "2 things" in fallback


def test_only_the_undoable_row_carries_a_button() -> None:
    _, blocks = receipt_card([_undoable(), _final()])
    buttons = [b for b in blocks if "accessory" in b]
    assert len(buttons) == 1
    assert buttons[0]["accessory"]["action_id"] == UNDO_ACTION_ID


def test_the_button_carries_the_durable_action_id() -> None:
    """A click has to resolve exactly this action, not the newest one."""

    _, blocks = receipt_card([_undoable(id="abc-123")])
    accessory = next(b["accessory"] for b in blocks if "accessory" in b)
    assert accessory["value"] == "abc-123"


def test_an_undeclared_tool_says_so_rather_than_claiming_a_reason() -> None:
    """A tool that explained itself and one that did not are different cases."""

    _, blocks = receipt_card([_final(irreversible_reason=None)])
    rendered = json.dumps(blocks)
    assert "nothing reported a prior state" in rendered


def test_an_action_with_no_summary_still_renders_naming_its_tool() -> None:
    """An action nobody can describe is still an action that happened."""

    _, blocks = receipt_card([_undoable(summary=None)])
    assert "mcp__k8s-scale__scale_deployment" in json.dumps(blocks)


def test_one_action_reads_as_one_thing() -> None:
    fallback, _ = receipt_card([_undoable()])
    assert "1 thing to your systems" in fallback
    assert "1 things" not in fallback
