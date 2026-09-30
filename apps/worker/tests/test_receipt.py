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

import pytest
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


def test_read_only_bash_calls_do_not_add_a_receipt() -> None:
    actions = [
        _action(tool="Bash", arguments={"command": command}, undoable=False, result=None)
        for command in (
            "pwd",
            "rg -n receipt apps/worker",
            "sed -n '1,40p' apps/worker/src/curie_worker/receipt.py",
            "git status --short",
            "git diff --stat",
        )
    ]

    assert render_receipt(actions) is None


def test_uncertain_bash_calls_are_counted_without_guessing_their_effects() -> None:
    actions = [
        _action(tool="Bash", arguments=arguments, undoable=False, result=None)
        for arguments in (
            None,
            {"command": "cat file; rm file"},
            {"command": "sed -i 's/a/b/' file"},
            {"command": "git diff --output=report.txt"},
            {"command": "cat $(touch marker)"},
            {"command": "rg --pre rm x ."},
        )
    ]

    receipt = render_receipt(actions)

    assert receipt is not None
    assert receipt.count("Bash") == 1
    assert "6 Bash calls" in receipt
    assert "changes not described" in receipt


def test_large_bash_result_joins_the_generic_call_count() -> None:
    receipt = render_receipt(
        [
            _action(
                tool="Bash",
                undoable=False,
                result=None,
                detail="tool result too large to record",
            ),
            _action(tool="Bash", undoable=False, result=None),
        ]
    )

    assert receipt is not None
    assert "2 Bash calls" in receipt
    assert receipt.count("Bash") == 1


def test_repeated_bash_noise_keeps_meaningful_summary_and_failure() -> None:
    generic = _action(tool="Bash", arguments={"command": "make build"}, undoable=False, result=None)
    actions = [generic.copy() for _ in range(25)]
    actions.extend(
        [
            _action(
                tool="Bash",
                arguments={"command": "make deploy"},
                undoable=False,
                result={"summary": "deployed acme service"},
            ),
            _action(
                tool="Bash",
                arguments={"command": "make deploy"},
                status="failed",
                undoable=False,
                result=None,
            ),
        ]
    )

    receipt = render_receipt(actions)

    assert receipt is not None
    assert len(receipt.splitlines()) == 4
    assert "25 Bash calls" in receipt
    assert "deployed acme service" in receipt
    assert "failed" in receipt


def test_repeated_named_actions_keep_the_count_and_distinct_verdicts() -> None:
    actions = [
        _action(),
        _action(),
        _action(status="failed", undoable=False),
        _action(result={"summary": "scaled public/web from 2 to 4"}),
    ]

    receipt = render_receipt(actions)

    assert receipt is not None
    assert receipt.count("scaled public/api from 3 to 10") == 2
    assert "2 calls" in receipt
    assert "failed" in receipt
    assert "scaled public/web from 2 to 4" in receipt


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


def test_late_failure_survives_the_slack_receipt_budget() -> None:
    actions = [
        _action(
            tool=f"tool_{i}",
            undoable=False,
            result={"summary": "é" * 160 + str(i)},
            detail="é" * 160,
        )
        for i in range(9)
    ]
    actions.append(
        _action(
            tool="late_failure",
            status="failed",
            undoable=False,
            result={"summary": "late failed action " + "é" * 160},
        )
    )

    receipt = render_receipt(actions)

    assert receipt is not None
    assert len(("\n\n" + receipt).encode("utf-8")) <= 2500
    assert "late failed action" in receipt
    assert "failed" in receipt


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


# --- The install's receipt mode (ADR-0180) ------------------------------------
#
# The mode changes only what is rendered to the person. Each fixture below is a
# turn's completed ledger rows; the goldens are the bytes ADR-0117's receipt
# rendered before ADR-0180, and `all` must keep producing exactly those.


def _mixed_turn() -> list[dict[str, Any]]:
    """Every kind of line at once: grouped, irreversible, undeclared, Bash, failed."""

    return [
        _action(),
        _action(),
        _action(
            tool="restart_deployment",
            undoable=False,
            detail="restarting pods cannot be undone",
            result=None,
        ),
        _action(tool="stage_file", undoable=False, detail=None, result=None),
        _action(tool="Bash", arguments={"command": "make build"}, undoable=False, result=None),
        _action(tool="Bash", arguments={"command": "make test"}, undoable=False, result=None),
        _action(
            tool="Bash", arguments={"command": "git status --short"}, undoable=False, result=None
        ),
        _action(
            tool="file_document",
            status="failed",
            undoable=False,
            result={"summary": "filed acme-invoice.pdf"},
        ),
        _action(
            tool="Bash",
            arguments={"command": "make deploy"},
            status="failed",
            undoable=False,
            result=None,
        ),
    ]


_MIXED_TURN_GOLDEN = (
    "_What I changed:_\n"
    "• scaled public/api from 3 to 10 — can be undone (2 calls)\n"
    "• called `restart_deployment` — restarting pods cannot be undone\n"
    "• called `stage_file` — cannot be undone: nothing reported a prior state\n"
    "• 2 Bash calls; changes not described\n"
    "• filed acme-invoice.pdf — failed — check before retrying\n"
    "• called `Bash` — failed — check before retrying"
)


def _long_turn() -> list[dict[str, Any]]:
    """Past the line cap, with the one failure last."""

    actions = [_action(result={"summary": f"read thread {i}"}) for i in range(40)]
    actions.append(_action(status="failed", undoable=False, result={"summary": "posted probe"}))
    return actions


_LONG_TURN_GOLDEN = "\n".join(
    [
        "_What I changed:_",
        *(f"• read thread {i} — can be undone" for i in range(9)),
        "• posted probe — failed — check before retrying",
        "• …and 31 more actions not listed",
    ]
)


def _failed_only(actions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [action for action in actions if action.get("status") == "failed"]


def test_the_default_receipt_bytes_are_pinned() -> None:
    """The oracle for `all`: today's receipt, written out rather than derived."""

    assert render_receipt(_mixed_turn()) == _MIXED_TURN_GOLDEN
    assert render_receipt(_long_turn()) == _LONG_TURN_GOLDEN


def test_all_mode_renders_byte_for_byte_what_the_receipt_rendered_before() -> None:
    """`all` is ADR-0117 as built, so an install that sets nothing sees no change."""

    assert render_receipt(_mixed_turn(), mode="all") == _MIXED_TURN_GOLDEN
    assert render_receipt(_long_turn(), mode="all") == _LONG_TURN_GOLDEN
    for actions in (
        [],
        [_action()],
        [_action(status="failed", undoable=False, result=None)],
        [_action(tool="Bash", arguments={"command": "pwd"}, undoable=False, result=None)],
        [_action(result={"summary": "x" * 5000})],
    ):
        assert render_receipt(actions, mode="all") == render_receipt(actions)


def test_failures_mode_without_a_failed_action_has_no_receipt() -> None:
    """A turn that staged a file and filed nothing ends with the answer alone."""

    actions = [
        _action(),
        _action(tool="stage_file", undoable=False, detail=None, result=None),
        _action(tool="Bash", arguments={"command": "make build"}, undoable=False, result=None),
    ]

    assert render_receipt(actions, mode="all") is not None
    assert render_receipt(actions, mode="failures") is None
    assert render_receipt([], mode="failures") is None


def test_failures_mode_keeps_only_the_failed_lines_under_the_same_header() -> None:
    receipt = render_receipt(_mixed_turn(), mode="failures")

    assert receipt == (
        "_What I changed:_\n"
        "• filed acme-invoice.pdf — failed — check before retrying\n"
        "• called `Bash` — failed — check before retrying"
    )


def test_failures_mode_is_the_full_receipt_of_the_failed_actions_alone() -> None:
    """Header, clamp, grouping and the line cap apply to what remains unchanged.

    Stated as an equivalence over fixtures that exercise each of them, so a
    failures-only rendering path that drifted from the full one fails here.
    """

    repeated = _action(tool="file_document", status="failed", undoable=False, result=None)
    fixtures = [
        _mixed_turn(),
        _long_turn(),
        # Identical failures: whatever `all` does with them, `failures` does too.
        [repeated.copy(), repeated.copy(), _action()],
        # A connector summary past the clamp.
        [_action(status="failed", undoable=False, result={"summary": "y" * 5000})],
    ]
    for actions in fixtures:
        expected = render_receipt(_failed_only(actions), mode="all")
        assert expected is not None
        assert render_receipt(actions, mode="failures") == expected


def test_failures_mode_still_bounds_a_long_run_of_failures() -> None:
    """Twelve failed calls keep the cap: ten lines, then the count of the rest."""

    actions = [
        _action(tool=f"tool_{i}", status="failed", undoable=False, result={"summary": f"f{i}"})
        for i in range(12)
    ]
    actions.extend(_action(result={"summary": f"ok {i}"}) for i in range(5))

    receipt = render_receipt(actions, mode="failures")

    assert receipt is not None
    lines = receipt.splitlines()
    assert lines[0] == "_What I changed:_"
    assert lines[1:11] == [f"• f{i} — failed — check before retrying" for i in range(10)]
    assert lines[11] == "• …and 2 more actions not listed"
    assert len(lines) == 12
    assert "ok " not in receipt


def test_a_clamped_failure_is_clamped_the_same_way_in_failures_mode() -> None:
    receipt = render_receipt(
        [_action(status="failed", undoable=False, result={"summary": "z" * 5000})],
        mode="failures",
    )

    assert receipt is not None
    assert len(receipt) < 1000
    assert receipt.endswith("… — failed — check before retrying")


def test_off_mode_never_renders_a_receipt() -> None:
    """Not on success, and not on failure."""

    for actions in ([], [_action()], _mixed_turn(), _long_turn(), _failed_only(_mixed_turn())):
        assert render_receipt(actions, mode="off") is None


@pytest.mark.parametrize(
    "detail",
    [
        "non-idempotent tool completed",
        "non-idempotent tool executed",
        "tool result too large to record",
    ],
)
def test_generic_irreversible_tool_detail_explains_missing_prior_state(detail: str) -> None:
    # WORKER-RECEIPT-1: an unknown tool remains visible and irreversible; a
    # bookkeeping detail neither explains its effect nor supplies prior state.
    receipt = render_receipt(
        [_action(tool="mcp__acme__stage_file", result=None, undoable=False, detail=detail)]
    )

    assert receipt == (
        "_What I changed:_\n"
        "• called `mcp__acme__stage_file` — cannot be undone: nothing reported a prior state"
    )
    assert detail not in receipt


@pytest.mark.parametrize(
    "detail",
    [
        "non-idempotent tool completed",
        "non-idempotent tool executed",
        "tool result too large to record",
    ],
)
@pytest.mark.parametrize(
    "overrides,verdict",
    [
        ({"status": "failed", "undoable": False}, "failed — check before retrying"),
        ({"undoable": True}, "can be undone"),
    ],
)
def test_generic_detail_does_not_replace_failure_or_undoability(
    detail: str, overrides: dict[str, Any], verdict: str
) -> None:
    receipt = render_receipt(
        [_action(tool="mcp__acme__stage_file", result=None, detail=detail, **overrides)]
    )

    assert receipt == f"_What I changed:_\n• called `mcp__acme__stage_file` — {verdict}"
