"""What a turn tells you it did to the world (ADR-0117 decision 7).

A turn that changed anything ends with a receipt listing each action, each
carrying either an undo control or the stated reason it has none.

The control is not here, and its absence is deliberate rather than unfinished.
Nothing in the platform can reach a connector yet, so a button would authorize a
restore that never runs -- the platform telling a user an action was put back
when it was not, which is the failure ADR-0117 exists to prevent. The line still
says which actions COULD be put back, because that is the thing an operator is
actually buying: not a bot that cannot make mistakes, but a platform that knows
which mistakes it can take back.

Channel-neutral on purpose. A Slack card carrying a working undo control needs an
interaction type on the channel protocol, and that belongs in the change that has
something for the control to do.
"""

from __future__ import annotations

from typing import Any

from curie_worker.receipt import render_receipt


def _action(**overrides: Any) -> dict[str, Any]:
    row: dict[str, Any] = {
        "tool": "scale_deployment",
        "result": {"summary": "scaled public/api from 3 to 10"},
        "undoable": True,
        "detail": "non-idempotent tool completed",
        "status": "succeeded",
    }
    row.update(overrides)
    return row


def test_a_turn_that_changed_nothing_has_no_receipt() -> None:
    """Most turns are reads. A receipt on every one of them is noise."""

    assert render_receipt([]) is None


def test_an_undoable_action_says_it_can_be_put_back() -> None:
    receipt = render_receipt([_action()])

    assert receipt is not None
    assert "scaled public/api from 3 to 10" in receipt
    assert "can be undone" in receipt


def test_an_irreversible_action_states_its_own_reason() -> None:
    """The connector's sentence, not a generic one.

    "restarting pods cannot be undone" is the connector explaining itself; a
    platform-authored "not undoable" would be the platform guessing on its
    behalf.
    """

    receipt = render_receipt(
        [_action(undoable=False, detail="restarting pods cannot be undone", result=None)]
    )

    assert receipt is not None
    assert "restarting pods cannot be undone" in receipt


def test_an_action_that_explained_nothing_still_appears() -> None:
    """An undeclared third-party tool is not the same as one that explained itself.

    Both are not-undoable, and flattening them to one sentence would hide which
    happened. An action nobody can describe is still an action that happened.
    """

    receipt = render_receipt([_action(undoable=False, detail=None, result=None)])

    assert receipt is not None
    assert "scale_deployment" in receipt
    assert "nothing reported a prior state" in receipt


def test_both_kinds_appear_together() -> None:
    """The honest half sells as well as the undoable half.

    A receipt listing only the reversible actions would hide the ones that matter
    most.
    """

    receipt = render_receipt(
        [
            _action(),
            _action(
                tool="restart_deployment",
                undoable=False,
                detail="restarting pods cannot be undone",
                result=None,
            ),
        ]
    )

    assert receipt is not None
    assert "can be undone" in receipt
    assert "restarting pods cannot be undone" in receipt


def test_a_failed_call_is_reported_as_maybe_rather_than_done() -> None:
    """"It may have happened" is the state a human most needs told."""

    receipt = render_receipt([_action(status="failed", undoable=False, result=None)])

    assert receipt is not None
    assert "failed" in receipt.lower()


def test_a_long_summary_is_clamped() -> None:
    """A connector's summary is not a size the platform controls."""

    receipt = render_receipt([_action(result={"summary": "x" * 5000})])

    assert receipt is not None
    assert len(receipt) < 1000


def test_a_long_turn_lists_a_bounded_receipt_and_counts_the_rest() -> None:
    """A receipt is read beneath an answer, and must not crowd it out (#3064).

    A turn that made a hundred calls used to end with a hundred lines, and the
    reply plus receipt passed the channel's size limit, so the answer was lost.
    The failed call sits last here: it is the line a person most needs, so it
    must survive the cut.
    """

    actions = [_action(result={"summary": f"read thread {i}"}) for i in range(40)]
    actions.append(_action(status="failed", undoable=False, result={"summary": "posted probe"}))

    receipt = render_receipt(actions)

    assert receipt is not None
    lines = receipt.splitlines()
    assert len(lines) <= 12
    assert "posted probe" in receipt and "failed" in receipt
    assert lines[-1].endswith("31 more actions not listed")
