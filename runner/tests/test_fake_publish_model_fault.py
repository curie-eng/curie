"""``[fake:publish-changes]`` and ``[fake:model-fault ...]`` (#5429).

Publish rides a parsed write-file turn: the write, then one
``mcp__curie__publish_changes`` call through the permission gate. A model fault
emits the SDK shape the real provider uses, and the production session
translator yields the cause, the ``no_effects`` safety fact and the retry
condition the factory policy yields. A bad fault argument is the default turn.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import anyio
import pytest
from aci_protocol import (
    ErrorEvent,
    Event,
    Final,
    RetryCondition,
    RetryReason,
    SessionStatus,
    SideEffectFlag,
    ToolNote,
    parse_ndjson,
)
from curie_api.factory_retry_policy import (
    RESET_GRACE,
    Disposition,
    DispositionKind,
    RetryFacts,
    decide,
)
from curie_runner.approval import ApprovalGate
from curie_runner.fake import STANDING_DIRECTIVE_ENV, FakeModelSession
from curie_runner.harness.claude.approval import build_can_use_tool
from curie_runner.otel import RunTracer
from curie_runner.session import SessionRunner
from curie_runner.side_effects import SideEffectClassifier
from curie_worker.kernel.failures import TurnOutcome, _escalation_cause, retry_safety
from plugin_format import PLATFORM_PUBLISH_TOOL_NAME

# The same instant the daily-budget fixture in test_provider_retry_condition.py uses.
_RESET = datetime(2026, 10, 30, tzinfo=UTC)
_RESET_S = int(_RESET.timestamp())
# The directive takes no proposal arguments. This is the one call it emits.
_PUBLISH_INPUT = {
    "title": "Publish the reviewed change",
    "body": "Publish the file written in this turn.",
}


def _drive(
    prompt: str,
    *,
    cwd: Path | None = None,
    gate: ApprovalGate | None = None,
) -> tuple[list[Any], SessionRunner]:
    session = FakeModelSession(
        cwd=None if cwd is None else str(cwd),
        can_use_tool=None if gate is None else build_can_use_tool(gate),
        approval_gate=gate,
    )
    runner = SessionRunner(
        max_usd_per_day=None,
        held_secrets=frozenset(),
        session_factory=lambda: session,
        ceiling=10_000,
        tracer=RunTracer(None),
        classifier=SideEffectClassifier(),
        trace_name="fake-directive",
        session_id="s-1",
        approval_gate=gate,
    )

    async def run() -> list[Any]:
        await runner.start()
        lines = [
            line
            async for line in runner.run_turn(
                Event(type="message", text=prompt, user="U1", ts="1.1")
            )
        ]
        return parse_ndjson("".join(lines))

    return anyio.run(run), runner


def _tools(events: Sequence[Any]) -> list[str]:
    return [event.tool for event in events if isinstance(event, ToolNote) and event.tool]


def _errors(events: Sequence[Any]) -> list[ErrorEvent]:
    return [event for event in events if isinstance(event, ErrorEvent)]


def _safety(events: Sequence[Any], error: ErrorEvent) -> str:
    """The worker safety fact for a turn that ended on ``error`` with no marker."""

    final = events[-1]
    outcome = TurnOutcome(
        terminal_ok=False,
        saw_side_effect=any(isinstance(event, SideEffectFlag) for event in events),
        runner_ended=isinstance(final, Final) and final.status is SessionStatus.CLASSIFIED_FAILURE,
        classification=error.classification,
        retry_condition=error.retry_condition,
    )
    return retry_safety(outcome, marker_flagged=False, own_execute_turn=True)


def _disposition(cause: str, error: ErrorEvent, safety: str) -> Disposition:
    condition = error.retry_condition
    return decide(
        RetryFacts(
            cause=cause,
            safety=safety,  # type: ignore[arg-type]
            condition_reason=None if condition is None else condition.reason.value,
            condition_retry_at=None if condition is None else condition.retry_at,
            condition_scope_key=None if condition is None else condition.scope_key,
        )
    )


@pytest.fixture(autouse=True)
def no_standing_directive(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(STANDING_DIRECTIVE_ENV, raising=False)


@pytest.mark.parametrize(
    "prompt",
    [
        "[fake:write-file note.txt hello] [fake:publish-changes]",
        "[fake:publish-changes] [fake:write-file note.txt hello]",
    ],
)
def test_publish_changes_writes_then_one_publish_through_the_gate(
    tmp_path: Path, prompt: str
) -> None:
    gate = ApprovalGate(required=frozenset({PLATFORM_PUBLISH_TOOL_NAME}))
    events, _runner = _drive(prompt, cwd=tmp_path, gate=gate)

    assert (tmp_path / "note.txt").read_text() == "hello"
    assert _tools(events) == ["Write", PLATFORM_PUBLISH_TOOL_NAME]
    assert gate.pending_gate_kind == "permission"
    assert gate.pending_granted_tool == PLATFORM_PUBLISH_TOOL_NAME
    assert gate.pending_granted_arguments == _PUBLISH_INPUT
    final = events[-1]
    assert isinstance(final, Final)
    assert final.status is SessionStatus.AWAITING_APPROVAL
    assert final.approval_granted_tool == PLATFORM_PUBLISH_TOOL_NAME
    assert final.approval_granted_arguments == _PUBLISH_INPUT


@pytest.mark.parametrize(
    ("directive", "classification", "condition", "disposition"),
    [
        pytest.param(
            f"[fake:model-fault rate-limit {_RESET_S}]",
            "rate-limit",
            RetryCondition(reason=RetryReason.RATE_LIMITED, retry_at=_RESET, scope_key=None),
            Disposition(
                DispositionKind.RETRY_BACKOFF,
                condition="provider_rate_limit",
                not_before=_RESET + RESET_GRACE,
                successor=True,
            ),
            id="rate-limit",
        ),
        pytest.param(
            "[fake:model-fault rate-limit-untimed]",
            "rate-limit",
            RetryCondition(reason=RetryReason.RATE_LIMITED, retry_at=None, scope_key=None),
            Disposition(DispositionKind.RETRY_BACKOFF, successor=True),
            id="rate-limit-untimed",
        ),
        pytest.param(
            f"[fake:model-fault daily-budget {_RESET_S}]",
            "model-credit-exhausted",
            RetryCondition(reason=RetryReason.USAGE_RESET, retry_at=_RESET, scope_key=None),
            Disposition(
                DispositionKind.CONDITION_WAIT,
                condition="provider_usage_reset",
                not_before=_RESET + RESET_GRACE,
                successor=True,
            ),
            id="daily-budget",
        ),
        pytest.param(
            "[fake:model-fault generic-403]",
            "model-credential-rejected",
            None,
            Disposition(DispositionKind.TERMINAL),
            id="generic-403",
        ),
        pytest.param(
            "[fake:model-fault unreachable]",
            "model-unreachable",
            None,
            Disposition(DispositionKind.RETRY_BACKOFF, successor=True),
            id="unreachable",
        ),
    ],
)
def test_model_fault_directives_yield_the_translator_cause_safety_and_retry_condition(
    directive: str,
    classification: str,
    condition: RetryCondition | None,
    disposition: Disposition,
) -> None:
    # Fake-mode boot leaves provider_scope unset, so the translator stamps no
    # scope key. A timed reset still schedules; a generic 403 carries no condition.
    events, runner = _drive(directive)

    errors = _errors(events)
    assert errors, "the directive produced no error"
    assert errors[-1].classification == classification
    assert errors[-1].retry_condition == condition
    assert _tools(events) == []
    assert not runner.side_effect_emitted
    final = events[-1]
    assert isinstance(final, Final)
    assert final.status is SessionStatus.CLASSIFIED_FAILURE
    safety = _safety(events, errors[-1])
    assert safety == "no_effects"
    cause = _escalation_cause(
        TurnOutcome(terminal_ok=False, classification=errors[-1].classification)
    )
    assert _disposition(cause, errors[-1], safety) == disposition


def test_a_standing_model_fault_applies_to_a_link_only_turn(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(STANDING_DIRECTIVE_ENV, "[fake:model-fault unreachable]")
    events, runner = _drive("https://forge.example/acme/widgets/issues/7")

    errors = _errors(events)
    assert errors, "the directive produced no error"
    assert errors[-1].classification == "model-unreachable"
    assert _tools(events) == []
    assert not runner.side_effect_emitted


@pytest.mark.parametrize(
    "directive",
    [
        "[fake:model-fault]",
        "[fake:model-fault nope]",
        "[fake:model-fault rate-limit]",
        "[fake:model-fault rate-limit -1]",
        "[fake:model-fault rate-limit 1.5]",
        "[fake:model-fault daily-budget]",
        "[fake:model-fault daily-budget soon]",
        "[fake:model-fault rate-limit-untimed 5]",
        "[fake:model-fault generic-403 extra]",
        "[fake:model-fault unreachable extra]",
        "[fake:model-fault rate-limit 999999999999999999]",
    ],
)
def test_a_bad_model_fault_argument_falls_back_to_the_default_turn(
    tmp_path: Path, directive: str
) -> None:
    # The fault marker is present, so a bad argument must not fall through to
    # the write-file directive that follows it.
    prompt = f"{directive} [fake:write-file note.txt hello]"
    events, _runner = _drive(prompt, cwd=tmp_path)

    assert _tools(events) == ["Bash"]
    assert not (tmp_path / "note.txt").exists()
    final = events[-1]
    assert isinstance(final, Final)
    assert final.status is SessionStatus.DONE
