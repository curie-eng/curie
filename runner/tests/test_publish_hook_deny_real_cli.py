"""A bundle hook's deny of publish_changes holds, on the real bundled CLI.

A bundle's PreToolUse hooks (its ``hooks/hooks.json``, which the CLI loads with
the plugin) run beside the runner's own approval hook, concurrently. Before
this fix the runner recorded a publication on sight of the ``publish_changes``
call, from the stream and from its own PreToolUse hook, and asked the CLI to
stop the turn there. A bundle hook that denied the same call could not take
that back: the run parked awaiting approval on a body the bundle had refused.
The dark factory's Decisions grammar and its diff approval publish gate were
both bypassed this way.

Now the in-process tool body records the publication, and the CLI runs it only
after every PreToolUse hook allowed the call; the approval hook's PostToolUse
callback ends the turn after it ran. The turn itself is
``runner_publish_hook_deny_support.run_publish_turn``; the worker's handoff
test runs the deny-then-correct case through its completion path.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

import pytest
from aci_protocol import SessionStatus
from curie_runner.session import PUBLICATION_UNRECORDED_CLASSIFICATION
from plugin_format import PLATFORM_PUBLISH_TOOL_NAME
from runner_publish_hook_deny_support import (
    CORRECTED_BODY,
    GIVE_UP_TEXT,
    REFUSED_BODY,
    REVIEW_GATE,
    SCRIPTED_REFUSAL,
    TITLE,
    PublishTurn,
    call_id,
    run_publish_turn,
    scripted_hook,
    seed_review_gate,
    tool_results,
    workspace_with_change,
)


def _pinned(events: list[Any]) -> list[dict[str, Any]]:
    """The frames as data: no version or token counts."""

    pinned: list[dict[str, Any]] = []
    for event in events:
        data = event.model_dump(mode="json", exclude_none=True)
        for key in ("version", "input_tokens", "output_tokens"):
            data.pop(key, None)
        pinned.append(data)
    return pinned


def _assert_published_once(turn: PublishTurn, call_id: str, body: str) -> None:
    final = turn.final
    assert final.status is SessionStatus.AWAITING_APPROVAL, final
    assert final.approval_granted_tool == PLATFORM_PUBLISH_TOOL_NAME
    assert final.approval_granted_arguments == {"title": TITLE, "body": body}
    assert turn.gate.publication_title == TITLE
    assert turn.gate.publication_body == body
    assert turn.gate.pending_halt is True
    # The halt lands right after the recorded call: the model is never asked
    # again, so no request carries that call's result.
    assert not any(call_id in tool_results(request) for request in turn.main_requests)


def _assert_nothing_published(turn: PublishTurn) -> None:
    final = turn.final
    assert final.status is SessionStatus.DONE, final
    assert final.approval_summary is None
    assert final.approval_granted_tool is None
    assert final.approval_granted_arguments is None
    assert turn.gate.pending_summary is None
    assert turn.gate.publication_title is None
    assert turn.gate.publication_body is None
    assert turn.gate.pending_halt is False
    assert not any(
        getattr(event, "classification", None) == PUBLICATION_UNRECORDED_CLASSIFICATION
        for event in turn.events
    ), turn.events
    assert final.text == GIVE_UP_TEXT


def test_a_bundle_deny_reaches_the_model_and_the_corrected_call_publishes_once(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """(a) Deny once, then allow: only the corrected body is recorded, once.

    Red before the fix: the runner recorded the refused body on sight of the
    first call and stopped the turn, so the run parked awaiting approval on
    the body the bundle refused and the model never saw the deny.
    """

    workspace = workspace_with_change(tmp_path)
    turn = run_publish_turn(
        tmp_path,
        monkeypatch,
        workspace=workspace,
        hook_command=scripted_hook(tmp_path),
        script=[REFUSED_BODY, CORRECTED_BODY],
    )

    assert turn.emitted == [
        (call_id(0), {"title": TITLE, "body": REFUSED_BODY}),
        (call_id(1), {"title": TITLE, "body": CORRECTED_BODY}),
    ]
    # The bundle hook really ran on both calls, and its reason reached the model.
    logged = (tmp_path / "scripted-gate.log").read_text(encoding="utf-8").splitlines()
    assert [json.loads(line)["body"] for line in logged] == [REFUSED_BODY, CORRECTED_BODY]
    assert SCRIPTED_REFUSAL in turn.result_text(call_id(0))
    _assert_published_once(turn, call_id(1), CORRECTED_BODY)
    # The refused body reaches neither the final nor the publication record the
    # worker reads. Only the stream's side effect frames, which describe each
    # call made, carry it.
    assert '"output." (plan)' not in json.dumps(_pinned([turn.final]))


def test_a_bundle_that_always_denies_publishes_nothing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """(b) Deny every time: nothing is recorded and the turn ends on its own.

    Red before the fix: the first call was recorded and the run parked
    awaiting approval on a body the bundle refused.
    """

    workspace = workspace_with_change(tmp_path)
    turn = run_publish_turn(
        tmp_path,
        monkeypatch,
        workspace=workspace,
        hook_command=scripted_hook(tmp_path),
        script=[REFUSED_BODY, REFUSED_BODY],
    )

    assert [call_id for call_id, _ in turn.emitted] == [call_id(0), call_id(1)]
    for index in range(2):
        assert SCRIPTED_REFUSAL in turn.result_text(call_id(index))
    _assert_nothing_published(turn)


def test_the_review_gate_decisions_deny_holds_and_the_corrected_body_publishes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The dark factory's real review gate refuses the live Decisions shape.

    With an approved diff of this checkout, the gate denies the refused body
    on its Decisions grammar, the model corrects it, and the corrected body is
    the one recorded. Red before the fix: the refused body was recorded.
    """

    workspace = workspace_with_change(tmp_path)
    state_dir = tmp_path / "gate-state"
    seed_review_gate(workspace, state_dir, monkeypatch, diff_approved=True)
    turn = run_publish_turn(
        tmp_path,
        monkeypatch,
        workspace=workspace,
        hook_command=f"{sys.executable} {REVIEW_GATE}",
        script=[REFUSED_BODY, CORRECTED_BODY],
        extra_env={
            "DARK_FACTORY_STATE_DIR": str(state_dir),
            "DARK_FACTORY_PROGRESS_LOG": str(tmp_path / "gate-progress.log"),
        },
    )

    assert len(turn.emitted) == 2
    assert "Invalid Decisions section" in turn.result_text(call_id(0))
    _assert_published_once(turn, call_id(1), CORRECTED_BODY)


def test_the_review_gate_diff_approval_deny_holds(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The diff approval publish gate: no approving diff review, nothing published.

    Red before the fix: the runner recorded the call the gate denied and the
    run parked awaiting approval without any diff review.
    """

    workspace = workspace_with_change(tmp_path)
    state_dir = tmp_path / "gate-state"
    seed_review_gate(workspace, state_dir, monkeypatch, diff_approved=False)
    turn = run_publish_turn(
        tmp_path,
        monkeypatch,
        workspace=workspace,
        hook_command=f"{sys.executable} {REVIEW_GATE}",
        script=[CORRECTED_BODY],
        extra_env={
            "DARK_FACTORY_STATE_DIR": str(state_dir),
            "DARK_FACTORY_PROGRESS_LOG": str(tmp_path / "gate-progress.log"),
        },
    )

    assert len(turn.emitted) == 1
    assert "Nothing is published without a diff reviewer VERDICT: APPROVE." in turn.result_text(
        call_id(0)
    )
    _assert_nothing_published(turn)


_ROUTE = "publish-approvers"


def test_an_approval_routed_publish_a_bundle_denies_creates_no_approval(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A route-bound, approval-gated publish that a bundle hook always denies.

    The runner's approval hook and the bundle's deny run on the same call. No
    approval is parked, so the worker has nothing to create one from, and the
    model sees the deny each time. Red before the fix: the approval hook parked
    the first call on its route.
    """

    workspace = workspace_with_change(tmp_path)
    turn = run_publish_turn(
        tmp_path,
        monkeypatch,
        workspace=workspace,
        hook_command=scripted_hook(tmp_path),
        script=[REFUSED_BODY, REFUSED_BODY],
        publish_route=_ROUTE,
    )

    assert turn.gate.route_by_tool[PLATFORM_PUBLISH_TOOL_NAME] == _ROUTE
    assert len(turn.emitted) == 2
    for index in range(2):
        assert SCRIPTED_REFUSAL in turn.result_text(call_id(index))
    assert turn.gate.pending_route is None
    assert turn.final.approval_route is None
    _assert_nothing_published(turn)


def test_an_approval_routed_publish_parks_only_the_corrected_call(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The same route-bound gate, denied once then allowed: one approval, on
    the route, for the corrected body. Red before the fix: the refused body was
    parked on the route."""

    workspace = workspace_with_change(tmp_path)
    turn = run_publish_turn(
        tmp_path,
        monkeypatch,
        workspace=workspace,
        hook_command=scripted_hook(tmp_path),
        script=[REFUSED_BODY, CORRECTED_BODY],
        publish_route=_ROUTE,
    )

    assert len(turn.emitted) == 2
    assert SCRIPTED_REFUSAL in turn.result_text(call_id(0))
    assert turn.final.approval_route == _ROUTE
    _assert_published_once(turn, call_id(1), CORRECTED_BODY)


def test_an_allowing_bundle_publishes_once_and_halts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Liveness: a bundle hook that allows the first call publishes it once.

    Passes both ways: the halt after an allowed call is unchanged.
    """

    workspace = workspace_with_change(tmp_path)
    turn = run_publish_turn(
        tmp_path,
        monkeypatch,
        workspace=workspace,
        hook_command=scripted_hook(tmp_path),
        script=[CORRECTED_BODY, CORRECTED_BODY],
    )

    assert turn.emitted == [(call_id(0), {"title": TITLE, "body": CORRECTED_BODY})]
    _assert_published_once(turn, call_id(0), CORRECTED_BODY)
