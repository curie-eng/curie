"""Budget accounting and the per-run output-token halt."""

import anyio
import pytest
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


def test_deduplicates_message_ids() -> None:
    # The SDK can repeat one response's id and usage across content blocks:
    # https://code.claude.com/docs/en/agent-sdk/cost-tracking
    tracker = BudgetTracker(ceiling=150)
    for _ in range(3):
        tracker.add_increment({"output_tokens": 100}, message_id="msg_same")
        assert tracker.used == 100
        assert not tracker.exceeded

    tracker.add_increment({"output_tokens": 75}, message_id="msg_new")
    assert tracker.used == 175
    assert tracker.exceeded


@pytest.mark.parametrize("message_id", [None, ""])
def test_counts_usage_without_message_id(message_id: str | None) -> None:
    tracker = BudgetTracker(ceiling=150)
    for _ in range(3):
        tracker.add_increment({"output_tokens": 100}, message_id=message_id)
    assert tracker.used == 300
    assert tracker.exceeded


def test_terminal_total_can_raise_usage() -> None:
    tracker = BudgetTracker(ceiling=150)
    for _ in range(3):
        tracker.add_increment({"output_tokens": 100}, message_id="msg_same")
    tracker.set_total({"output_tokens": 175})
    assert tracker.used == 175
    assert tracker.exceeded


def test_terminal_total_is_not_added() -> None:
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


@pytest.mark.parametrize(
    "usage", [None, {}, {"input_tokens": 5}, {"output_tokens": "100"}, {"output_tokens": None}]
)
def test_ignores_invalid_usage(usage: dict[str, object] | None) -> None:
    tracker = BudgetTracker(ceiling=100)
    tracker.add_increment(usage)
    assert tracker.used == 0


def test_zero_ceiling_is_unlimited() -> None:
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
        max_usd_per_day=None,
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
            subtype="success",
            duration_ms=1,
            duration_api_ms=1,
            is_error=False,
            num_turns=1,
            session_id="s",
            result="done",
            usage={"output_tokens": 500},
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
            subtype="success",
            duration_ms=1,
            duration_api_ms=1,
            is_error=False,
            num_turns=1,
            session_id="s",
            result="done",
            usage={"output_tokens": 999},
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
        AssistantMessage(content=[TextBlock(text="hi")], model="fake", usage={"output_tokens": 80}),
        ResultMessage(
            subtype="success",
            duration_ms=1,
            duration_api_ms=1,
            is_error=False,
            num_turns=1,
            session_id="s",
            result="hi",
            usage={"output_tokens": 80},
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
            subtype="success",
            duration_ms=1,
            duration_api_ms=1,
            is_error=False,
            num_turns=1,
            session_id="s",
            result="hello",
            usage={"output_tokens": 5},
        ),
    ]
    runner, _ = _runner(script, ceiling=1000)
    events = _run(runner, _event())
    assert events[-1].type == "final"
    assert events[-1].status == SessionStatus.DONE


@pytest.mark.parametrize("new_response", [False, True])
def test_duplicate_usage_does_not_halt_early(new_response: bool) -> None:
    # Repeated SDK response blocks, as documented in the cost tracking guide:
    # https://code.claude.com/docs/en/agent-sdk/cost-tracking
    script = [
        AssistantMessage(
            content=[TextBlock(text=f"block {i}")],
            model="fake",
            message_id="msg_same",
            usage={"output_tokens": 100},
        )
        for i in range(3)
    ]
    if new_response:
        script.append(
            AssistantMessage(
                content=[TextBlock(text="new response")],
                model="fake",
                message_id="msg_new",
                usage={"output_tokens": 75},
            )
        )
    script.append(
        ResultMessage(
            subtype="success",
            duration_ms=1,
            duration_api_ms=1,
            is_error=False,
            num_turns=1,
            session_id="s",
            result="done",
            usage={"output_tokens": 175 if new_response else 100},
        )
    )
    runner, fake = _runner(script, ceiling=150)
    events = _run(runner, _event())
    # All repeated blocks must be delivered before any budget halt. With a new
    # response, the halt must land on that response, before the terminal total.
    texts = [e.text.strip() for e in events if e.type == "text_delta"]
    assert texts == ["block 0", "block 1", "block 2"] + (["new response"] if new_response else [])
    assert fake.interrupts == int(new_response)
    assert events[-1].type == "final"
    expected = SessionStatus.CLASSIFIED_FAILURE if new_response else SessionStatus.DONE
    assert events[-1].status == expected
    assert (
        any(e.type == "error" and e.classification == BUDGET_CLASSIFICATION for e in events)
        is new_response
    )


def test_deduplication_resets_each_turn() -> None:
    script = []
    runner, fake = _runner(script, ceiling=150)

    async def go() -> None:
        await runner.start()
        try:
            for count, status in [
                (100, SessionStatus.DONE),
                (175, SessionStatus.CLASSIFIED_FAILURE),
            ]:
                script[:] = [
                    AssistantMessage(
                        content=[TextBlock(text="reply")],
                        model="fake",
                        message_id="msg_same",
                        usage={"output_tokens": count},
                    ),
                    ResultMessage(
                        subtype="success",
                        duration_ms=1,
                        duration_api_ms=1,
                        is_error=False,
                        num_turns=1,
                        session_id="s",
                        result="reply",
                    ),
                ]
                lines = [line async for line in runner.run_turn(_event())]
                assert parse_ndjson("".join(lines))[-1].status == status
        finally:
            await runner.close()

    anyio.run(go)
    assert fake.interrupts == 1


def test_a_tracker_result_session_at_its_dollar_allowance_ends_budget_exceeded_once(
    tmp_path, monkeypatch
) -> None:
    # #5586: a tracker result session runs under the API's dollar allowance. When
    # the SDK reports that allowance reached, the turn ends budget-exceeded, and
    # the runner opens no second model session for the request: a reset (or any
    # later session the runner asks for) answers offline.
    import json

    from aiohttp import web
    from aiohttp.test_utils import TestServer
    from claude_agent_sdk import ResultMessage as Result
    from curie_runner import RunnerConfig
    from curie_runner import __main__ as boot
    from curie_runner.__main__ import build_runner
    from curie_runner.issue_read import (
        CAPABILITY_HEADER,
        ISSUE_READ_TOKEN_ENV,
        ISSUE_READ_URL_ENV,
    )

    started: list[FakeModelSession] = []

    class AllowanceReached(FakeModelSession):
        def __init__(self, *args, **kwargs) -> None:
            super().__init__(*args, **kwargs)
            started.append(self)

        def _default_script(self) -> list:
            return [
                Result(
                    subtype="error_max_budget_usd",
                    duration_ms=1,
                    duration_api_ms=1,
                    is_error=True,
                    num_turns=3,
                    session_id="s",
                    result=None,
                )
            ]

    contract_calls: list[str | None] = []

    async def contract(request: web.Request) -> web.Response:
        contract_calls.append(request.headers.get(CAPABILITY_HEADER))
        return web.json_response(
            {
                "completion_contract": "tracker_result",
                "result_kinds": ["findings"],
                "session_usd_allowance": 0.5,
            }
        )

    app = web.Application()
    app.router.add_post("/work-items/completion-contract", contract)
    monkeypatch.setattr(boot, "FakeModelSession", AllowanceReached)
    plugin = tmp_path / ".claude-plugin"
    plugin.mkdir()
    (plugin / "plugin.json").write_text(json.dumps({"name": "triage"}))
    config = RunnerConfig.from_env(
        {
            "CURIE_PLUGIN_DIR": str(tmp_path),
            "CURIE_SESSION_ID": "s-triage",
            "CURIE_SANDBOX_ID": "b-triage",
            "CURIE_BUDGET": '{"max_output_tokens_per_run": 10000, "max_usd_per_day": 1.0}',
        }
    )
    first: list = []
    after_reset: list = []

    async def go() -> None:
        async with TestServer(app) as server:
            monkeypatch.setenv(ISSUE_READ_URL_ENV, str(server.make_url("/work-items/issue-read")))
            monkeypatch.setenv(ISSUE_READ_TOKEN_ENV, "wir.example-capability.signature")
            runner = build_runner(config, fake_model=True)
            try:
                await runner.start()
                first.extend([line async for line in runner.run_turn(_event())])
                await runner.reset()
                after_reset.extend([line async for line in runner.run_turn(_event())])
            finally:
                await runner.close()

    anyio.run(go)
    events = parse_ndjson("".join(first))
    assert any(e.type == "error" and e.classification == BUDGET_CLASSIFICATION for e in events)
    assert events[-1].type == "final"
    assert events[-1].status == SessionStatus.CLASSIFIED_FAILURE
    # One model session for the request, one contract call (the reservation).
    assert len(started) == 1
    assert contract_calls == ["wir.example-capability.signature"]
    later = parse_ndjson("".join(after_reset))
    assert not any(e.type == "error" and e.classification == BUDGET_CLASSIFICATION for e in later)
    assert len(started) == 1
