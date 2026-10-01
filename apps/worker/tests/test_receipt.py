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

from copy import deepcopy
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
    assert receipt.count("Shell request completed") == 1
    assert "(6 calls)" in receipt
    assert "changes were not summarized and undo information is incomplete" in receipt


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
    assert "(2 calls)" in receipt
    assert receipt.count("Shell request completed") == 1


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
    assert "Shell request completed" in receipt
    assert "(25 calls)" in receipt
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


def test_an_undoable_action_says_its_restore_was_recorded_not_that_it_can_be_undone() -> None:
    """Nothing performs a restore yet (#1867), so the line cannot promise one.

    ``undoable`` means the ledger holds what a restore needs. Saying "can be
    undone" to the person who asked would claim a capability no part of the
    platform has, which is the overclaim ADR-0117 exists to prevent (#1861).
    """

    receipt = render_receipt([_action()])

    assert receipt is not None
    assert "scaled public/api from 3 to 10" in receipt
    assert "restore information recorded" in receipt
    assert "can be undone" not in receipt


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
    assert "scale deployment" in receipt
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
    assert "restore information recorded" in receipt
    assert "restarting pods cannot be undone" in receipt


def test_a_failed_call_is_reported_as_maybe_rather_than_done() -> None:
    """ "It may have happened" is the state a human most needs told."""

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
# turn's completed ledger rows; the goldens pin the current wording under
# WORKER-RECEIPT-1 while `all` and the default retain the same behavior.


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
    "• scaled public/api from 3 to 10 — restore information recorded (2 calls)\n"
    "• restart deployment — restarting pods cannot be undone\n"
    "• stage file — cannot be undone: nothing reported a prior state\n"
    "• Shell request completed; changes were not summarized and undo information "
    "is incomplete (2 calls)\n"
    "• filed acme-invoice.pdf — failed — check before retrying\n"
    "• shell request — failed — check before retrying"
)


def _long_turn() -> list[dict[str, Any]]:
    """Past the line cap, with the one failure last."""

    actions = [_action(result={"summary": f"read thread {i}"}) for i in range(40)]
    actions.append(_action(status="failed", undoable=False, result={"summary": "posted probe"}))
    return actions


_LONG_TURN_GOLDEN = "\n".join(
    [
        "_What I changed:_",
        *(f"• read thread {i} — restore information recorded" for i in range(9)),
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


def test_all_mode_matches_the_default_receipt() -> None:
    """The install mode preserves the same content, grouping and budgets."""

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
        "• shell request — failed — check before retrying"
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
        "• stage file — cannot be undone: nothing reported a prior state"
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
        ({"undoable": True}, "restore information recorded"),
    ],
)
def test_generic_detail_does_not_replace_failure_or_undoability(
    detail: str, overrides: dict[str, Any], verdict: str
) -> None:
    receipt = render_receipt(
        [_action(tool="mcp__acme__stage_file", result=None, detail=detail, **overrides)]
    )

    assert receipt == f"_What I changed:_\n• stage file — {verdict}"


@pytest.mark.parametrize("tool,noun", [("Skill", "Instruction"), ("Bash", "Shell")])
@pytest.mark.parametrize(
    "detail",
    [
        None,
        "non-idempotent tool completed",
        "non-idempotent tool executed",
        "tool result too large to record",
    ],
)
def test_generic_native_request_reports_completion_without_guessing_changes(
    tool: str, noun: str, detail: str | None
) -> None:
    # WORKER-RECEIPT-1: success proves completion, not harmless instruction loading
    # or the absence of a prior snapshot. The call must still be visible.
    receipt = render_receipt([_action(tool=tool, result=None, undoable=False, detail=detail)])

    assert receipt == (
        f"_What I changed:_\n• {noun} request completed; "
        "changes were not summarized and undo information is incomplete"
    )


@pytest.mark.parametrize("tool,noun", [("Skill", "Instruction"), ("Bash", "Shell")])
def test_native_request_with_prior_snapshot_and_no_target_does_not_deny_prior_state(
    tool: str, noun: str
) -> None:
    action = _action(
        tool=tool,
        result={"prior": {"replicas": 2}},
        prior_state={"replicas": 2},
        target=None,
        undoable=False,
    )

    receipt = render_receipt([action])

    assert receipt == (
        f"_What I changed:_\n• {noun} request completed; "
        "changes were not summarized and undo information is incomplete"
    )
    assert "nothing reported a prior state" not in receipt


@pytest.mark.parametrize("tool", ["Skill", "Bash"])
@pytest.mark.parametrize(
    "overrides,line",
    [
        ({"status": "failed"}, "{label} — failed — check before retrying"),
        ({"undoable": True}, "{label} — restore information recorded"),
        (
            {"detail": "external changes require manual restoration"},
            "{label} — external changes require manual restoration",
        ),
        (
            {"result": {"summary": "updated acme configuration"}},
            "updated acme configuration — cannot be undone: nothing reported a prior state",
        ),
    ],
)
def test_native_request_keeps_failure_undoability_custom_detail_and_summary(
    tool: str, overrides: dict[str, Any], line: str
) -> None:
    action = _action(tool=tool, result=None, undoable=False)
    action.update(overrides)

    assert render_receipt([action]) == "_What I changed:_\n• " + line.format(label="shell request" if tool == "Bash" else "instruction request")


def test_native_request_groups_keep_counts_rows_and_distinct_request_kinds() -> None:
    actions = [
        *[_action(tool="Skill", result=None, undoable=False) for _ in range(3)],
        *[_action(tool="Bash", result=None, undoable=False) for _ in range(2)],
        _action(),
        _action(tool="Skill", result=None, undoable=False, status="failed"),
        _action(tool="Skill", result=None, undoable=False, status="failed"),
    ]
    stored_rows = deepcopy(actions)

    receipt = render_receipt(actions)

    assert receipt == (
        "_What I changed:_\n"
        "• Instruction request completed; changes were not summarized and undo information "
        "is incomplete (3 calls)\n"
        "• Shell request completed; changes were not summarized and undo information "
        "is incomplete (2 calls)\n"
        "• scaled public/api from 3 to 10 — restore information recorded\n"
        "• instruction request — failed — check before retrying\n"
        "• instruction request — failed — check before retrying"
    )
    assert actions == stored_rows


@pytest.mark.parametrize("tool", ["mcp__acme__Skill", "mcp__acme__Bash"])
def test_third_party_native_like_name_is_plain_but_not_hidden_or_native(tool: str) -> None:
    assert render_receipt([_action(tool=tool, result=None, undoable=False)]) == (
        f"_What I changed:_\n• {tool.rsplit('__', 1)[-1].lower()} — cannot be undone: nothing reported a prior state"
    )


def test_native_instruction_request_still_obeys_receipt_modes() -> None:
    success = _action(tool="Skill", result=None, undoable=False)
    failure = _action(tool="Skill", result=None, undoable=False, status="failed")

    assert render_receipt([success], mode="all") is not None
    assert render_receipt([success], mode="failures") is None
    assert render_receipt([success, failure], mode="failures") == (
        "_What I changed:_\n• instruction request — failed — check before retrying"
    )
    assert render_receipt([success, failure], mode="off") is None


@pytest.mark.parametrize(
    "snapshot_fields",
    [{"prior_state": {"replicas": 3}, "result": None}, {"result": {"prior": {"replicas": 3}}}],
)
def test_unknown_action_with_prior_snapshot_does_not_claim_it_was_absent(
    snapshot_fields: dict[str, Any],
) -> None:
    receipt = render_receipt(
        [_action(tool="third_party_action", undoable=False, **snapshot_fields)]
    )
    assert receipt is not None
    assert "third party action" in receipt
    assert "undo information is incomplete" in receipt
    assert "nothing reported a prior state" not in receipt


@pytest.mark.parametrize("tool,label", [
    ("mcp__acme__file_attachment", "file attachment"),
    ("mcp__acme__nested__createInvoice", "create invoice"),
    ("mcp__acme__", "action"),
    ("mcp__only", "action"),
    (None, "action"),
    ("bad\nname", "action"),
    ("Read", "read file"),
    ("Edit", "edit file"),
])
@pytest.mark.parametrize("status,verdict", [("succeeded", "restore information recorded"), ("failed", "failed — check before retrying")])
def test_every_receipt_fallback_is_plain_and_status_is_independent(tool: str | None, label: str, status: str, verdict: str) -> None:
    action = _action(tool=tool, result=None, status=status)
    original = deepcopy(action)
    assert render_receipt([action]) == f"_What I changed:_\n• {label} — {verdict}"
    assert action == original


def test_connector_metadata_replaces_identifier_references_and_keeps_content() -> None:
    tool = "mcp__acme__file_attachment"
    action = _action(tool=tool, result={"summary": f"{tool} attached acme_invoice.pdf to public/api"}, undoable=False, detail=f"{tool}: manual restoration required")
    original = deepcopy(action)
    assert render_receipt([action]) == (
        "_What I changed:_\n• file attachment attached acme_invoice.pdf to public/api — "
        "file attachment: manual restoration required"
    )
    assert action == original


def test_connector_metadata_does_not_rewrite_identifier_substrings_in_content() -> None:
    action = _action(tool="stage_file", result={"summary": "saved acme_stage_file.txt"}, undoable=False, detail="restore acme_stage_file.txt manually")
    assert render_receipt([action]) == (
        "_What I changed:_\n• saved acme_stage_file.txt — restore acme_stage_file.txt manually"
    )


def test_connector_metadata_normalizes_secondary_mcp_references() -> None:
    action = _action(result={"summary": "mcp__acme__file_attachment followed mcp__acme__readFile"})
    assert render_receipt([action]) == "_What I changed:_\n• file attachment followed read file — restore information recorded"
