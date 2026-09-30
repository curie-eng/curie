"""Budget accounting and the per-run output-token halt."""

import anyio
from aci_protocol import Event, SessionStatus, parse_ndjson
from claude_agent_sdk import AssistantMessage, ResultMessage, TextBlock
from curie_runner import BudgetTracker, RunTracer, SideEffectClassifier
from curie_runner.budget import BUDGET_CLASSIFICATION
from curie_runner.fake import FakeModelSession
from curie_runner.session import SessionRunner


def test_tracker_sums_per_message_output() -> None:
    tracker = BudgetTracker(ceiling=100)
    tracker.add_increment({"output_tokens": 40})
    assert not tracker.exceeded
    tracker.add_increment({"output_tokens": 90})  # per-message output accumulates
    assert tracker.used == 130
    assert tracker.exceeded


def test_tracker_does_not_double_count_terminal_total() -> None:
    # An 80-token reply reported on both the assistant message and the terminal
    # result must count once, not 160, so it stays under a 100 ceiling.
    tracker = BudgetTracker(ceiling=100)
    tracker.add_increment({"output_tokens": 80})
    tracker.set_total({"output_tokens": 80})
    assert tracker.used == 80
    assert not tracker.exceeded


def test_tracker_uses_terminal_total_when_no_increments() -> None:
    tracker = BudgetTracker(ceiling=100)
    tracker.set_total({"output_tokens": 150})  # usage only on the result
    assert tracker.used == 150
    assert tracker.exceeded


def test_tracker_ignores_missing_usage() -> None:
    tracker = BudgetTracker(ceiling=100)
    tracker.add_increment(None)
    tracker.add_increment({"input_tokens": 5})  # no output_tokens field
    assert tracker.used == 0


def test_zero_ceiling_disables_enforcement() -> None:
    tracker = BudgetTracker(ceiling=0)
    tracker.add_increment({"output_tokens": 10_000})
    assert not tracker.exceeded


def _event() -> Event:
    return Event(type="message", text="hi", user="U1", ts="1.0")


def _run(runner: SessionRunner, event: Event) -> list:
    lines: list[str] = []

    async def go() -> None:
        await runner.start()
        async for line in runner.run_turn(event):
            lines.append(line)

    anyio.run(go)
    return parse_ndjson("".join(lines))


def _runner(script, ceiling: int) -> tuple[SessionRunner, FakeModelSession]:
    fake = FakeModelSession(lambda: script)
    runner = SessionRunner(
        held_secrets=frozenset(),
        session_factory=lambda: fake,
        ceiling=ceiling,
        tracer=RunTracer(None),
        classifier=SideEffectClassifier(),
        trace_name="t",
    )
    return runner, fake


def test_budget_halt_on_assistant_usage() -> None:
    script = [
        AssistantMessage(
            content=[TextBlock(text="thinking hard")],
            model="fake",
            usage={"output_tokens": 500},
        ),
        ResultMessage(
            subtype="success", duration_ms=1, duration_api_ms=1, is_error=False,
            num_turns=1, session_id="s", result="done", usage={"output_tokens": 500},
        ),
    ]
    runner, fake = _runner(script, ceiling=10)
    events = _run(runner, _event())

    final = events[-1]
    assert final.type == "final"
    assert final.status == SessionStatus.CLASSIFIED_FAILURE
    assert any(e.type == "error" and e.classification == BUDGET_CLASSIFICATION for e in events)
    # The run was actually halted, not just relabelled.
    assert fake.interrupts >= 1
    assert runner.status == SessionStatus.CLASSIFIED_FAILURE


def test_budget_halt_when_usage_only_on_result() -> None:
    script = [
        AssistantMessage(content=[TextBlock(text="quick")], model="fake"),
        ResultMessage(
            subtype="success", duration_ms=1, duration_api_ms=1, is_error=False,
            num_turns=1, session_id="s", result="done", usage={"output_tokens": 999},
        ),
    ]
    runner, _ = _runner(script, ceiling=10)
    events = _run(runner, _event())
    final = events[-1]
    assert final.type == "final"
    assert final.status == SessionStatus.CLASSIFIED_FAILURE
    # The budget error must be present even when the ceiling is only crossed at
    # the terminal result, so consumers can distinguish it from a model failure.
    assert any(e.type == "error" and e.classification == BUDGET_CLASSIFICATION for e in events)


def test_no_false_halt_from_terminal_double_report() -> None:
    # Same 80 tokens reported on the assistant message and the terminal result
    # must not sum to 160 and trip a 100 ceiling.
    script = [
        AssistantMessage(
            content=[TextBlock(text="hi")], model="fake", usage={"output_tokens": 80}
        ),
        ResultMessage(
            subtype="success", duration_ms=1, duration_api_ms=1, is_error=False,
            num_turns=1, session_id="s", result="hi", usage={"output_tokens": 80},
        ),
    ]
    runner, _ = _runner(script, ceiling=100)
    events = _run(runner, _event())
    assert events[-1].type == "final"
    assert events[-1].status == SessionStatus.DONE


def test_under_budget_completes_done() -> None:
    script = [
        AssistantMessage(content=[TextBlock(text="hello")], model="fake"),
        ResultMessage(
            subtype="success", duration_ms=1, duration_api_ms=1, is_error=False,
            num_turns=1, session_id="s", result="hello", usage={"output_tokens": 5},
        ),
    ]
    runner, _ = _runner(script, ceiling=1000)
    events = _run(runner, _event())
    assert events[-1].type == "final"
    assert events[-1].status == SessionStatus.DONE
