"""Per-model token usage reported at the turn's ResultMessage boundary (#3223).

Pinned interface (the implementation must provide):

- module ``curie_runner.usage_report``
- ``build_usage_body(message: ResultMessage, primary_model: str | None) -> dict | None``
  pure; ``None`` when the message carries no usage at all.
- ``UsageReporter(url: str, token: str)``; ``url`` is the progress URL plus
  ``"/usage"``. ``async report(message, primary_model) -> None`` POSTs the body
  with the token in ``X-API-Key``; one retry on transport error or 5xx; never
  raises.
- ``SessionRunner(..., usage_reporter=<obj with async report(message,
  primary_model)>, primary_model=<str | None>)``: the report is awaited at the
  ResultMessage boundary, before the Final event is yielded.

HTTP cases run against a real local aiohttp server that records what it got.
"""

from __future__ import annotations

from typing import Any

import anyio
import pytest
from aci_protocol import Event, parse_ndjson
from aiohttp import web
from aiohttp.test_utils import TestServer
from claude_agent_sdk import ResultMessage
from curie_runner import RunTracer, SideEffectClassifier
from curie_runner.fake import FakeModelSession
from curie_runner.session import SessionRunner
from curie_runner.usage_report import UsageReporter, build_usage_body

TOKEN = "sbx.example-usage-token.signature"
PRIMARY = "example-org/implementer-model-PLACEHOLDER"
REVIEWER = "example-org/reviewer-model-PLACEHOLDER"


def _result(**overrides: Any) -> ResultMessage:
    fields: dict[str, Any] = {
        "subtype": "success",
        "duration_ms": 1,
        "duration_api_ms": 1,
        "is_error": False,
        "num_turns": 3,
        "session_id": "sdk-session-PLACEHOLDER",
        "result": "done",
    }
    fields.update(overrides)
    return ResultMessage(**fields)


TWO_MODELS = {
    PRIMARY: {
        "inputTokens": 1200,
        "cacheReadInputTokens": 3400,
        "cacheCreationInputTokens": 560,
        "outputTokens": 780,
    },
    REVIEWER: {
        "inputTokens": 90,
        "cacheReadInputTokens": 0,
        "cacheCreationInputTokens": 0,
        "outputTokens": 45,
    },
}


def _by_model(body: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {entry["model"]: entry for entry in body["models"]}


# --- the body -----------------------------------------------------------------


def test_body_carries_every_model_from_model_usage() -> None:
    body = build_usage_body(_result(model_usage=TWO_MODELS, uuid="turn-uuid-1"), PRIMARY)
    assert body is not None
    assert body["turn_id"] == "turn-uuid-1"
    assert body["primary_model"] == PRIMARY
    models = _by_model(body)
    assert set(models) == {PRIMARY, REVIEWER}
    assert models[PRIMARY] == {
        "model": PRIMARY,
        "input_tokens": 1200,
        "cached_input_tokens": 3400,
        "cache_write_tokens": 560,
        "output_tokens": 780,
    }
    assert models[REVIEWER]["input_tokens"] == 90
    assert models[REVIEWER]["output_tokens"] == 45


def test_body_falls_back_to_the_usage_block_for_the_primary_model() -> None:
    usage = {
        "input_tokens": 100,
        "cache_read_input_tokens": 20,
        "cache_creation_input_tokens": 5,
        "output_tokens": 40,
    }
    body = build_usage_body(_result(usage=usage, uuid="turn-uuid-2"), PRIMARY)
    assert body is not None
    assert body["models"] == [
        {
            "model": PRIMARY,
            "input_tokens": 100,
            "cached_input_tokens": 20,
            "cache_write_tokens": 5,
            "output_tokens": 40,
        }
    ]


def test_no_usage_at_all_builds_nothing() -> None:
    assert build_usage_body(_result(), PRIMARY) is None


def test_turn_id_without_uuid_is_stable_for_the_same_message() -> None:
    message = _result(model_usage=TWO_MODELS)
    first = build_usage_body(message, PRIMARY)
    second = build_usage_body(message, PRIMARY)
    assert first is not None and second is not None
    assert first["turn_id"]
    assert first["turn_id"] == second["turn_id"]


# --- the POST -----------------------------------------------------------------


class _Recorder:
    def __init__(self, statuses: list[int] | None = None) -> None:
        self.statuses = list(statuses or [])
        self.received: list[tuple[dict[str, Any], str | None]] = []

    def app(self) -> web.Application:
        app = web.Application()

        async def usage(request: web.Request) -> web.Response:
            self.received.append((await request.json(), request.headers.get("X-API-Key")))
            status = self.statuses.pop(0) if self.statuses else 201
            return web.json_response({"recorded": status == 201}, status=status)

        app.router.add_post("/v1/work-item-progress/{request_id}/usage", usage)
        return app


def _run_with_server(recorder: _Recorder, message: ResultMessage) -> None:
    async def go() -> None:
        server = TestServer(recorder.app())
        await server.start_server()
        try:
            url = str(server.make_url("/v1/work-item-progress/example-request/usage"))
            await UsageReporter(url, TOKEN).report(message, PRIMARY)
        finally:
            await server.close()

    anyio.run(go)


def test_report_posts_the_body_with_the_token() -> None:
    recorder = _Recorder()
    _run_with_server(recorder, _result(model_usage=TWO_MODELS, uuid="turn-uuid-3"))
    assert len(recorder.received) == 1
    body, key = recorder.received[0]
    assert key == TOKEN
    assert body["turn_id"] == "turn-uuid-3"
    assert set(_by_model(body)) == {PRIMARY, REVIEWER}


def test_nothing_to_report_posts_nothing() -> None:
    recorder = _Recorder()
    _run_with_server(recorder, _result())
    assert recorder.received == []


def test_a_5xx_is_retried_once_with_the_same_turn_id() -> None:
    recorder = _Recorder(statuses=[503, 201])
    _run_with_server(recorder, _result(model_usage=TWO_MODELS))
    assert len(recorder.received) == 2
    assert recorder.received[0][0]["turn_id"] == recorder.received[1][0]["turn_id"]


def test_a_failed_post_never_raises() -> None:
    recorder = _Recorder(statuses=[500, 500, 500])
    _run_with_server(recorder, _result(model_usage=TWO_MODELS))
    assert len(recorder.received) == 2  # one retry, then give up quietly


def test_an_unreachable_api_never_raises() -> None:
    async def go() -> None:
        reporter = UsageReporter("http://127.0.0.1:9/v1/work-item-progress/x/usage", TOKEN)
        await reporter.report(_result(model_usage=TWO_MODELS), PRIMARY)

    anyio.run(go)


# --- the session boundary -----------------------------------------------------


def test_the_report_is_awaited_before_the_final_event_is_yielded() -> None:
    lines: list[str] = []
    calls: list[tuple[Any, str | None, int]] = []
    message = _result(model_usage=TWO_MODELS, uuid="turn-uuid-4")

    class _Session:
        async def connect(self) -> None: ...

        async def query(self, _text: str) -> None: ...

        async def receive_turn(self):
            yield message

        async def interrupt(self) -> None: ...

        async def close(self) -> None: ...

    class _Reporter:
        async def report(self, got: Any, primary_model: str | None) -> None:
            calls.append((got, primary_model, len(lines)))

    runner = SessionRunner(
        max_usd_per_day=None,
        held_secrets=frozenset(),
        session_factory=_Session,
        ceiling=0,
        tracer=RunTracer(None),
        classifier=SideEffectClassifier(),
        trace_name="t",
        usage_reporter=_Reporter(),
        primary_model=PRIMARY,
    )

    async def go() -> None:
        await runner.start()
        async for line in runner.run_turn(
            Event(type="message", text="go", user="U0EXAMPLE1", ts="1")
        ):
            lines.append(line)
        await runner.close()

    anyio.run(go)
    assert len(calls) == 1
    got, primary, seen_before = calls[0]
    assert got is message
    assert primary == PRIMARY
    events = parse_ndjson("".join(lines))
    assert events[-1].type == "final"
    already = parse_ndjson("".join(lines[:seen_before]))
    assert all(event.type != "final" for event in already)


# --- role from the SDK: per-message observation (#3223 review) -----------------
#
# Pinned interface:
#
# - ``UsageReporter.observe(message: AssistantMessage) -> None``: accumulates the
#   message's ``usage`` keyed by ``(role, model)`` where role is ``"reviewer"``
#   when ``message.parent_tool_use_id is not None`` else ``"implementer"``.
#   A non-empty ``message_id`` counts once per ``(role, model)`` for the turn;
#   a message with no id still adds. ``report(result, primary_model)`` builds
#   the body from the accumulated observations and then resets them, whether
#   or not anything was posted.
# - ``build_usage_body(message, primary_model, observed=None)``: ``observed`` is a
#   ``Mapping[tuple[str, str], Mapping[str, int]]`` of ``(role, model)`` to wire
#   token counts (``input_tokens``, ``cached_input_tokens``,
#   ``cache_write_tokens``, ``output_tokens``). Every ``models`` entry carries a
#   ``role``. Per model in ``model_usage``: the reviewer entry is the observed
#   reviewer usage capped at the model's totals, the implementer entry is the
#   remainder floored at 0; all-zero entries are omitted.
# - ``SessionRunner`` calls ``usage_reporter.observe(message)`` for every
#   ``AssistantMessage`` of the turn, before the ResultMessage report.

from claude_agent_sdk import AssistantMessage, TextBlock  # noqa: E402


def _assistant(
    model: str,
    usage: dict[str, int],
    *,
    parent: str | None = None,
    message_id: str | None = None,
) -> AssistantMessage:
    return AssistantMessage(
        content=[TextBlock(text="x")],
        model=model,
        parent_tool_use_id=parent,
        usage=usage,
        message_id=message_id,
    )


def _sdk_usage(inp: int, out: int, cached: int = 0, write: int = 0) -> dict[str, int]:
    return {
        "input_tokens": inp,
        "output_tokens": out,
        "cache_read_input_tokens": cached,
        "cache_creation_input_tokens": write,
    }


def _wire(inp: int, out: int, cached: int = 0, write: int = 0) -> dict[str, int]:
    return {
        "input_tokens": inp,
        "cached_input_tokens": cached,
        "cache_write_tokens": write,
        "output_tokens": out,
    }


def _by_role_model(body: dict[str, Any]) -> dict[tuple[str, str], dict[str, Any]]:
    return {(entry["role"], entry["model"]): entry for entry in body["models"]}


SHARED = "example-org/shared-model-PLACEHOLDER"


def _model_usage(inp: int, out: int, cached: int = 0, write: int = 0) -> dict[str, int]:
    return {
        "inputTokens": inp,
        "outputTokens": out,
        "cacheReadInputTokens": cached,
        "cacheCreationInputTokens": write,
    }


def test_one_model_used_by_main_thread_and_subagent_splits_into_two_roles() -> None:
    message = _result(model_usage={SHARED: _model_usage(1000, 100, cached=50)}, uuid="t-split")
    observed = {("reviewer", SHARED): _wire(300, 40, cached=10)}
    body = build_usage_body(message, SHARED, observed=observed)
    assert body is not None
    got = _by_role_model(body)
    assert set(got) == {("implementer", SHARED), ("reviewer", SHARED)}
    assert {k: got[("reviewer", SHARED)][k] for k in _wire(0, 0)} == _wire(300, 40, cached=10)
    assert {k: got[("implementer", SHARED)][k] for k in _wire(0, 0)} == _wire(700, 60, cached=40)


def test_reviewer_usage_is_capped_at_the_model_total_and_implementer_is_never_negative() -> None:
    message = _result(model_usage={SHARED: _model_usage(100, 10)}, uuid="t-cap")
    observed = {("reviewer", SHARED): _wire(500, 50)}
    body = build_usage_body(message, SHARED, observed=observed)
    assert body is not None
    got = _by_role_model(body)
    # The implementer remainder is all zero, so it is omitted.
    assert set(got) == {("reviewer", SHARED)}
    assert got[("reviewer", SHARED)]["input_tokens"] == 100
    assert got[("reviewer", SHARED)]["output_tokens"] == 10


def test_a_model_seen_only_on_subagent_messages_is_wholly_reviewer() -> None:
    message = _result(model_usage=TWO_MODELS, uuid="t-rev-only")
    # Observed counts differ from model_usage: the whole model still goes reviewer.
    observed = {
        ("implementer", PRIMARY): _wire(1200, 780, cached=3400, write=560),
        ("reviewer", REVIEWER): _wire(10, 5),
    }
    body = build_usage_body(message, PRIMARY, observed=observed)
    assert body is not None
    got = _by_role_model(body)
    assert set(got) == {("implementer", PRIMARY), ("reviewer", REVIEWER)}
    assert got[("reviewer", REVIEWER)]["input_tokens"] == 90
    assert got[("reviewer", REVIEWER)]["output_tokens"] == 45


def test_a_zero_model_usage_entry_beside_a_nonzero_one_posts_no_zero_row() -> None:
    """A cumulative entry at zero means the model gained nothing this turn."""
    message = _result(
        model_usage={PRIMARY: _model_usage(100, 10), REVIEWER: _model_usage(0, 0)},
        uuid="t-zero-beside",
    )
    observed = {("implementer", PRIMARY): _wire(100, 10)}
    body = build_usage_body(message, PRIMARY, observed=observed)
    assert body is not None
    assert [(e["role"], e["model"]) for e in body["models"]] == [("implementer", PRIMARY)]


def test_model_usage_with_only_zero_entries_builds_nothing() -> None:
    """All-zero cumulative entries are no observation, so there is no body."""
    message = _result(
        model_usage={PRIMARY: _model_usage(0, 0), REVIEWER: _model_usage(0, 0)},
        uuid="t-all-zero",
    )
    assert build_usage_body(message, PRIMARY, observed={}) is None


def test_an_explicit_zero_per_turn_usage_posts_one_zero_implementer_row() -> None:
    """A blocked-preflight turn ran and spent nothing; that zero is a real row (#3977)."""
    message = _result(
        usage={"input_tokens": 0, "output_tokens": 0}, model_usage={}, uuid="t-zero-usage"
    )
    observed = {("implementer", PRIMARY): _wire(0, 0)}
    body = build_usage_body(message, PRIMARY, observed=observed)
    assert body is not None
    assert body["models"] == [{"model": PRIMARY, "role": "implementer", **_wire(0, 0)}]


def test_a_model_never_observed_is_implementer_whatever_the_primary() -> None:
    message = _result(model_usage={REVIEWER: _model_usage(9, 9)}, uuid="t-unseen")
    body = build_usage_body(message, PRIMARY, observed={})
    assert body is not None
    assert [(e["role"], e["model"]) for e in body["models"]] == [("implementer", REVIEWER)]


@pytest.mark.parametrize("result_shape", ["model_usage", "usage"])
def test_session_posts_observed_reviewer_missing_from_result_totals_once(
    result_shape: str,
) -> None:
    """Text only nested messages carry usage even without terminal model totals.

    Provider shapes are defined by installed claude_agent_sdk/types.py,
    AssistantMessage and ClaudeAgentOptions.forward_subagent_text.
    Repeated message IDs carry the same API response usage:
    https://code.claude.com/docs/en/agent-sdk/cost-tracking
    """
    recorder = _Recorder()
    terminal = (
        {"model_usage": {PRIMARY: _model_usage(100, 40, cached=20, write=5)}}
        if result_shape == "model_usage"
        else {"usage": _sdk_usage(100, 40, cached=20, write=5)}
    )
    reviewer = _assistant(
        REVIEWER,
        _sdk_usage(30, 12, cached=7, write=3),
        parent="toolu_example",
        message_id="msg_reviewer_example",
    )
    messages = [
        _assistant(PRIMARY, _sdk_usage(100, 40, cached=20, write=5)),
        reviewer,
        reviewer,
        _result(**terminal, uuid="turn_missing_reviewer"),
    ]

    async def go() -> None:
        async with TestServer(recorder.app()) as server:
            fake = FakeModelSession(lambda: messages)
            runner = SessionRunner(
                max_usd_per_day=None,
                held_secrets=frozenset(),
                session_factory=lambda: fake,
                ceiling=0,
                tracer=RunTracer(None),
                classifier=SideEffectClassifier(),
                trace_name="usage",
                usage_reporter=UsageReporter(
                    str(server.make_url("/v1/work-item-progress/example-request/usage")), TOKEN
                ),
                primary_model=PRIMARY,
            )
            await runner.start()
            try:
                lines = [
                    line
                    async for line in runner.run_turn(
                        Event(type="message", text="go", user="U0EXAMPLE1", ts="1")
                    )
                ]
                assert parse_ndjson("".join(lines))[-1].type == "final"
                assert len(recorder.received) == 1
            finally:
                await runner.close()

    anyio.run(go)
    body, token = recorder.received[0]
    assert token == TOKEN
    assert body["turn_id"] == "turn_missing_reviewer"
    models = _by_role_model(body)
    assert set(models) == {("implementer", PRIMARY), ("reviewer", REVIEWER)}
    assert {key: models[("implementer", PRIMARY)][key] for key in _wire(0, 0)} == _wire(
        100, 40, cached=20, write=5
    )
    assert {key: models[("reviewer", REVIEWER)][key] for key in _wire(0, 0)} == _wire(
        30, 12, cached=7, write=3
    )


def test_zero_cumulative_delta_keeps_fresh_reviewer_usage_without_recounting() -> None:
    """A repeated cumulative snapshot must still retain fresh observed usage.

    Cumulative snapshots and response ID deduplication follow the SDK guide:
    https://code.claude.com/docs/en/agent-sdk/cost-tracking
    """
    recorder = _Recorder()
    totals = {PRIMARY: _model_usage(100, 10), REVIEWER: _model_usage(40, 4)}

    async def go() -> None:
        async with TestServer(recorder.app()) as server:
            reporter = UsageReporter(
                str(server.make_url("/v1/work-item-progress/example-request/usage")), TOKEN
            )
            reporter.observe(_assistant(PRIMARY, _sdk_usage(100, 10)))
            reporter.observe(_assistant(REVIEWER, _sdk_usage(40, 4), parent="toolu_example"))
            await reporter.report(_result(model_usage=totals, uuid="turn_initial"), PRIMARY)
            fresh = _assistant(
                REVIEWER,
                _sdk_usage(20, 2, cached=5, write=1),
                parent="toolu_example",
                message_id="msg_fresh_reviewer",
            )
            reporter.observe(fresh)
            reporter.observe(fresh)
            await reporter.report(_result(model_usage=totals, uuid="turn_fresh"), PRIMARY)
            await reporter.report(_result(model_usage=totals, uuid="turn_empty"), PRIMARY)
            reporter.observe(_assistant(PRIMARY, _sdk_usage(25, 3)))
            await reporter.report(
                _result(
                    model_usage={**totals, PRIMARY: _model_usage(125, 13)},
                    uuid="turn_followup",
                ),
                PRIMARY,
            )

    anyio.run(go)
    assert [body["turn_id"] for body, _ in recorder.received] == [
        "turn_initial",
        "turn_fresh",
        "turn_followup",
    ]
    fresh_body = _by_role_model(recorder.received[1][0])
    assert set(fresh_body) == {("reviewer", REVIEWER)}
    assert {key: fresh_body[("reviewer", REVIEWER)][key] for key in _wire(0, 0)} == _wire(
        20, 2, cached=5, write=1
    )
    followup = _by_role_model(recorder.received[2][0])
    assert set(followup) == {("implementer", PRIMARY)}
    assert {key: followup[("implementer", PRIMARY)][key] for key in _wire(0, 0)} == _wire(25, 3)


@pytest.mark.parametrize(
    ("initial_report_fails", "fresh_usage"), [(False, True), (True, True), (True, False)]
)
def test_delayed_reviewer_totals_count_previously_observed_usage_once(
    initial_report_fails: bool,
    fresh_usage: bool,
) -> None:
    """A later cumulative snapshot includes reviewer usage already reported.

    The SDK cost guide defines model_usage as cumulative within a session:
    https://code.claude.com/docs/en/agent-sdk/cost-tracking
    """
    recorder = _Recorder(statuses=[503, 503, 201, 201] if initial_report_fails else None)

    async def go() -> None:
        async with TestServer(recorder.app()) as server:
            reporter = UsageReporter(
                str(server.make_url("/v1/work-item-progress/example-request/usage")), TOKEN
            )
            reporter.observe(_assistant(PRIMARY, _sdk_usage(100, 10)))
            reporter.observe(
                _assistant(
                    REVIEWER,
                    _sdk_usage(30, 12, cached=9, write=6),
                    parent="toolu_example",
                    message_id="msg_reviewer_initial",
                )
            )
            await reporter.report(
                _result(model_usage={PRIMARY: _model_usage(100, 10)}, uuid="turn_observed"),
                PRIMARY,
            )
            if fresh_usage:
                reporter.observe(_assistant(PRIMARY, _sdk_usage(25, 3)))
                reporter.observe(
                    _assistant(
                        REVIEWER,
                        _sdk_usage(20, 8, cached=3, write=2),
                        parent="toolu_example",
                        message_id="msg_reviewer_fresh",
                    )
                )
            totals = {
                PRIMARY: _model_usage(125, 13) if fresh_usage else _model_usage(100, 10),
                REVIEWER: (
                    _model_usage(50, 20, cached=12, write=8)
                    if fresh_usage
                    else _model_usage(30, 12, cached=9, write=6)
                ),
            }
            await reporter.report(_result(model_usage=totals, uuid="turn_catchup"), PRIMARY)
            await reporter.report(_result(model_usage=totals, uuid="turn_empty"), PRIMARY)

    anyio.run(go)
    expected_turns = ["turn_observed"] * (3 if initial_report_fails else 1)
    if fresh_usage:
        expected_turns.append("turn_catchup")
    assert [body["turn_id"] for body, _ in recorder.received] == expected_turns
    if initial_report_fails:
        assert recorder.received[0] == recorder.received[1] == recorder.received[2]
    first = _by_role_model(recorder.received[0][0])
    assert set(first) == {("implementer", PRIMARY), ("reviewer", REVIEWER)}
    assert {key: first[("reviewer", REVIEWER)][key] for key in _wire(0, 0)} == _wire(
        30, 12, cached=9, write=6
    )
    if not fresh_usage:
        return
    catchup = _by_role_model(recorder.received[-1][0])
    assert set(catchup) == {("implementer", PRIMARY), ("reviewer", REVIEWER)}
    assert {key: catchup[("reviewer", REVIEWER)][key] for key in _wire(0, 0)} == _wire(
        20, 8, cached=3, write=2
    )
    assert {
        key: sum(body[("reviewer", REVIEWER)][key] for body in (first, catchup))
        for key in _wire(0, 0)
    } == _wire(50, 20, cached=12, write=8)


def test_observe_keys_role_on_parent_tool_use_id_and_report_resets() -> None:
    recorder = _Recorder()

    async def go() -> None:
        server = TestServer(recorder.app())
        await server.start_server()
        try:
            url = str(server.make_url("/v1/work-item-progress/example-request/usage"))
            reporter = UsageReporter(url, TOKEN)
            reporter.observe(_assistant(SHARED, _sdk_usage(600, 60)))
            reporter.observe(_assistant(SHARED, _sdk_usage(200, 20), parent="toolu_1"))
            reporter.observe(_assistant(SHARED, _sdk_usage(100, 10), parent="toolu_1"))
            await reporter.report(
                _result(model_usage={SHARED: _model_usage(900, 90)}, uuid="turn-a"), SHARED
            )
            # Next turn: no subagent messages, so nothing carries over.
            # model_usage is the session running total, so this result includes turn A.
            reporter.observe(_assistant(SHARED, _sdk_usage(50, 5)))
            await reporter.report(
                _result(model_usage={SHARED: _model_usage(950, 95)}, uuid="turn-b"), SHARED
            )
        finally:
            await server.close()

    anyio.run(go)
    assert len(recorder.received) == 2
    first = _by_role_model(recorder.received[0][0])
    assert first[("reviewer", SHARED)]["input_tokens"] == 300
    assert first[("reviewer", SHARED)]["output_tokens"] == 30
    assert first[("implementer", SHARED)]["input_tokens"] == 600
    second = _by_role_model(recorder.received[1][0])
    assert set(second) == {("implementer", SHARED)}
    assert second[("implementer", SHARED)]["input_tokens"] == 50


def test_the_session_runner_observes_every_assistant_message_before_the_report() -> None:
    calls: list[tuple[str, Any]] = []
    main = _assistant(SHARED, _sdk_usage(10, 1))
    sub = _assistant(SHARED, _sdk_usage(5, 1), parent="toolu_2")
    result = _result(model_usage={SHARED: _model_usage(15, 2)}, uuid="turn-obs")

    class _Session:
        async def connect(self) -> None: ...

        async def query(self, _text: str) -> None: ...

        async def receive_turn(self):
            yield main
            yield sub
            yield result

        async def interrupt(self) -> None: ...

        async def close(self) -> None: ...

    class _Reporter:
        def observe(self, message: Any) -> None:
            calls.append(("observe", message))

        async def report(self, got: Any, primary_model: str | None) -> None:
            calls.append(("report", got))

    runner = SessionRunner(
        max_usd_per_day=None,
        held_secrets=frozenset(),
        session_factory=_Session,
        ceiling=0,
        tracer=RunTracer(None),
        classifier=SideEffectClassifier(),
        trace_name="t",
        usage_reporter=_Reporter(),
        primary_model=SHARED,
    )

    async def go() -> None:
        await runner.start()
        async for _line in runner.run_turn(
            Event(type="message", text="go", user="U0EXAMPLE1", ts="1")
        ):
            pass
        await runner.close()

    anyio.run(go)
    assert [kind for kind, _ in calls] == ["observe", "observe", "report"]
    assert calls[0][1] is main and calls[1][1] is sub


def test_observe_counts_each_message_id_once() -> None:
    """Several AssistantMessages can repeat one API response.

    The Agent SDK cost guide says that when Claude uses multiple tools in one
    turn, all messages in that turn share the same ID and the same usage, so
    callers deduplicate by ID. The posted reviewer share is that once-count,
    capped by the result total. A missing id is not a shared response, so
    those messages still add. A distinct id adds too.
    https://code.claude.com/docs/en/agent-sdk/cost-tracking
    """

    recorder = _Recorder()

    async def go() -> None:
        server = TestServer(recorder.app())
        await server.start_server()
        try:
            url = str(server.make_url("/v1/work-item-progress/example-request/usage"))
            reporter = UsageReporter(url, TOKEN)
            repeated = _sdk_usage(100, 10, cached=4, write=2)
            for _ in range(3):
                reporter.observe(
                    _assistant(
                        PRIMARY,
                        repeated,
                        parent="toolu_example",
                        message_id="msg_example_repeat",
                    )
                )
            reporter.observe(
                _assistant(
                    PRIMARY,
                    _sdk_usage(7, 1),
                    parent="toolu_example",
                    message_id="msg_example_other",
                )
            )
            for _ in range(2):
                reporter.observe(_assistant(PRIMARY, _sdk_usage(1, 0), parent="toolu_example"))
            await reporter.report(
                _result(
                    model_usage={PRIMARY: _model_usage(200, 20, cached=10, write=4)},
                    uuid="t-dedupe",
                ),
                PRIMARY,
            )
            # The seen set is per turn. The same id on the next turn counts again.
            reporter.observe(
                _assistant(
                    PRIMARY,
                    _sdk_usage(5, 1),
                    parent="toolu_example",
                    message_id="msg_example_repeat",
                )
            )
            await reporter.report(
                _result(
                    model_usage={PRIMARY: _model_usage(205, 21, cached=10, write=4)},
                    uuid="t-dedupe-next",
                ),
                PRIMARY,
            )
        finally:
            await server.close()

    anyio.run(go)
    assert len(recorder.received) == 2
    got = _by_role_model(recorder.received[0][0])
    assert set(got) == {("implementer", PRIMARY), ("reviewer", PRIMARY)}
    assert {k: got[("reviewer", PRIMARY)][k] for k in _wire(0, 0)} == _wire(
        109, 11, cached=4, write=2
    )
    assert {k: got[("implementer", PRIMARY)][k] for k in _wire(0, 0)} == _wire(
        91, 9, cached=6, write=2
    )
    nxt = _by_role_model(recorder.received[1][0])
    assert set(nxt) == {("reviewer", PRIMARY)}
    assert nxt[("reviewer", PRIMARY)]["input_tokens"] == 5


def test_shared_model_split_uses_deduplicated_message_counts() -> None:
    """Implementer and reviewer on one model split from once-per-message counts.

    Three reviewer blocks that repeat one message_id must not consume the whole
    model total and leave the implementer the remainder of a triple count.
    """

    recorder = _Recorder()

    async def go() -> None:
        server = TestServer(recorder.app())
        await server.start_server()
        try:
            url = str(server.make_url("/v1/work-item-progress/example-request/usage"))
            reporter = UsageReporter(url, TOKEN)
            reporter.observe(_assistant(SHARED, _sdk_usage(600, 60), message_id="msg_example_impl"))
            reviewer = _sdk_usage(200, 20)
            for _ in range(3):
                reporter.observe(
                    _assistant(
                        SHARED,
                        reviewer,
                        parent="toolu_example",
                        message_id="msg_example_rev",
                    )
                )
            await reporter.report(
                _result(model_usage={SHARED: _model_usage(800, 80)}, uuid="t-shared-dedupe"),
                SHARED,
            )
        finally:
            await server.close()

    anyio.run(go)
    assert len(recorder.received) == 1
    got = _by_role_model(recorder.received[0][0])
    assert set(got) == {("implementer", SHARED), ("reviewer", SHARED)}
    assert {k: got[("reviewer", SHARED)][k] for k in _wire(0, 0)} == _wire(200, 20)
    assert {k: got[("implementer", SHARED)][k] for k in _wire(0, 0)} == _wire(600, 60)


def test_a_subagent_message_without_usage_still_marks_its_model_reviewer() -> None:
    """A reviewer message may carry no per-message usage; its role must survive."""

    reporter = UsageReporter("http://127.0.0.1:9/usage", TOKEN)
    message = _assistant(REVIEWER, {}, parent="toolu_example")
    message.usage = None
    reporter.observe(message)
    reporter.observe(_assistant(PRIMARY, _sdk_usage(1, 1)))
    body = build_usage_body(
        _result(model_usage=TWO_MODELS, uuid="t-no-usage"),
        PRIMARY,
        observed=reporter._observed,
    )
    assert body is not None
    roles = {(e["role"], e["model"]) for e in body["models"]}
    assert ("reviewer", REVIEWER) in roles
    assert ("implementer", REVIEWER) not in roles


def test_a_follow_up_turn_posts_only_its_increment_and_keeps_the_reviewer_role() -> None:
    """A second result repeats the call's running model_usage.

    The Agent SDK cost guide says that in streaming input mode each turn's
    ``model_usage`` is the running total for the whole call, and that summing
    those results double-counts. A follow-up that does not see the reviewer
    must not book the reviewer's earlier tokens to the implementer.
    https://code.claude.com/docs/en/agent-sdk/cost-tracking
    """

    recorder = _Recorder()

    async def go() -> None:
        server = TestServer(recorder.app())
        await server.start_server()
        try:
            url = str(server.make_url("/v1/work-item-progress/example-request/usage"))
            reporter = UsageReporter(url, TOKEN)
            reporter.observe(_assistant(PRIMARY, _sdk_usage(1_000_000, 500_000)))
            reporter.observe(
                _assistant(REVIEWER, _sdk_usage(200_000, 100_000), parent="toolu_example")
            )
            await reporter.report(
                _result(
                    model_usage={
                        PRIMARY: _model_usage(1_000_000, 500_000),
                        REVIEWER: _model_usage(200_000, 100_000),
                    },
                    uuid="turn-a",
                ),
                PRIMARY,
            )
            reporter.observe(_assistant(PRIMARY, _sdk_usage(500_000, 200_000)))
            await reporter.report(
                _result(
                    model_usage={
                        PRIMARY: _model_usage(1_500_000, 700_000),
                        REVIEWER: _model_usage(200_000, 100_000),
                    },
                    uuid="turn-b",
                ),
                PRIMARY,
            )
        finally:
            await server.close()

    anyio.run(go)
    assert len(recorder.received) == 2
    first = _by_role_model(recorder.received[0][0])
    second = _by_role_model(recorder.received[1][0])
    assert set(first) == {("implementer", PRIMARY), ("reviewer", REVIEWER)}
    assert first[("reviewer", REVIEWER)]["output_tokens"] == 100_000
    assert set(second) == {("implementer", PRIMARY)}
    assert second[("implementer", PRIMARY)]["input_tokens"] == 500_000
    assert second[("implementer", PRIMARY)]["output_tokens"] == 200_000
    posted_output = sum(entry["output_tokens"] for _, entry in (*first.items(), *second.items()))
    assert posted_output == 800_000


def test_a_restarted_session_posts_its_new_total_when_the_running_total_drops() -> None:
    """``/clear`` and a new session id restart model_usage. The new total is the turn."""

    recorder = _Recorder()

    async def go() -> None:
        server = TestServer(recorder.app())
        await server.start_server()
        try:
            url = str(server.make_url("/v1/work-item-progress/example-request/usage"))
            reporter = UsageReporter(url, TOKEN)
            reporter.observe(_assistant(PRIMARY, _sdk_usage(900, 90)))
            await reporter.report(
                _result(model_usage={PRIMARY: _model_usage(900, 90)}, uuid="turn-old"),
                PRIMARY,
            )
            reporter.observe(_assistant(PRIMARY, _sdk_usage(40, 4)))
            await reporter.report(
                _result(
                    model_usage={PRIMARY: _model_usage(40, 4)},
                    uuid="turn-new",
                    session_id="sdk-session-RESTARTED",
                ),
                PRIMARY,
            )
        finally:
            await server.close()

    anyio.run(go)
    assert len(recorder.received) == 2
    second = _by_role_model(recorder.received[1][0])
    assert second[("implementer", PRIMARY)]["input_tokens"] == 40
    assert second[("implementer", PRIMARY)]["output_tokens"] == 4


def test_a_failed_reviewer_report_is_replayed_with_its_role_before_the_next_delta() -> None:
    """A lost response must not fold that turn into the next one.

    The replay keeps the original turn id and reviewer role. The follow-up
    then posts only its own increment, so a server that already stored the
    first body can no-op the replay without a second copy of those tokens.
    """

    recorder = _Recorder(statuses=[500, 500])

    async def go() -> None:
        server = TestServer(recorder.app())
        await server.start_server()
        try:
            url = str(server.make_url("/v1/work-item-progress/example-request/usage"))
            reporter = UsageReporter(url, TOKEN)
            reporter.observe(_assistant(PRIMARY, _sdk_usage(100, 10)))
            reporter.observe(_assistant(REVIEWER, _sdk_usage(40, 4), parent="toolu_example"))
            await reporter.report(
                _result(
                    model_usage={
                        PRIMARY: _model_usage(100, 10),
                        REVIEWER: _model_usage(40, 4),
                    },
                    uuid="turn-lost",
                ),
                PRIMARY,
            )
            reporter.observe(_assistant(PRIMARY, _sdk_usage(50, 5)))
            await reporter.report(
                _result(
                    model_usage={
                        PRIMARY: _model_usage(150, 15),
                        REVIEWER: _model_usage(40, 4),
                    },
                    uuid="turn-kept",
                ),
                PRIMARY,
            )
        finally:
            await server.close()

    anyio.run(go)
    assert [status_body["turn_id"] for status_body, _key in recorder.received] == [
        "turn-lost",
        "turn-lost",
        "turn-lost",
        "turn-kept",
    ]
    replayed = _by_role_model(recorder.received[2][0])
    assert set(replayed) == {("implementer", PRIMARY), ("reviewer", REVIEWER)}
    assert replayed[("reviewer", REVIEWER)]["output_tokens"] == 4
    kept = _by_role_model(recorder.received[3][0])
    assert set(kept) == {("implementer", PRIMARY)}
    assert kept[("implementer", PRIMARY)]["input_tokens"] == 50
    assert kept[("implementer", PRIMARY)]["output_tokens"] == 5


# --- a context-window suffix on model_usage keys (#3992) ---------------------------
#
# The SDK keys ``model_usage`` with the configured id, which can carry a trailing
# context-window token such as ``[1m]``; ``AssistantMessage.model`` carries the
# base id. The counts below are the ones a factory run stored for one request.

FAST = "z-ai/glm-5.3-flash"
OPUS = "anthropic/claude-opus-5.5"
OPUS_1M = f"{OPUS}[1m]"


@pytest.mark.parametrize("primary", [FAST, OPUS_1M])
def test_a_suffixed_model_usage_key_pairs_with_its_base_model_observations(
    primary: str,
) -> None:
    observed = {
        ("implementer", FAST): _wire(35696, 9279, cached=750720),
        ("reviewer", OPUS): _wire(16, 64, cached=45517, write=29094),
    }
    if primary == FAST:
        observed[("implementer", OPUS)] = _wire(0, 4434)
    message = _result(
        model_usage={
            FAST: _model_usage(35696, 9279, cached=750720),
            OPUS_1M: _model_usage(16, 4498, cached=45517, write=29094),
        },
        uuid="turn-suffixed",
    )

    body = build_usage_body(message, primary, observed=observed)

    assert body is not None
    models = _by_role_model(body)
    assert set(models) == {
        ("implementer", FAST),
        ("implementer", OPUS_1M),
        ("reviewer", OPUS_1M),
    }
    assert {key: models[("reviewer", OPUS_1M)][key] for key in _wire(0, 0)} == _wire(
        16, 64, cached=45517, write=29094
    )
    assert {key: models[("implementer", OPUS_1M)][key] for key in _wire(0, 0)} == _wire(0, 4434)
    assert {
        key: sum(entry[key] for entry in body["models"] if entry["model"] == OPUS_1M)
        for key in _wire(0, 0)
    } == _wire(16, 4498, cached=45517, write=29094)


def test_a_different_bracketed_model_does_not_take_another_models_observations() -> None:
    other = "anthropic/claude-opus-5[1m]"
    observed = {("reviewer", OPUS): _wire(16, 64)}
    message = _result(model_usage={other: _model_usage(100, 50)}, uuid="turn-other")

    body = build_usage_body(message, other, observed=observed)

    assert body is not None
    models = _by_role_model(body)
    assert set(models) == {("implementer", other), ("reviewer", OPUS)}
    assert {key: models[("implementer", other)][key] for key in _wire(0, 0)} == _wire(100, 50)


def test_reviewer_usage_reported_before_its_suffixed_totals_is_not_counted_again() -> None:
    """The SDK cost guide defines model_usage as cumulative within a session:
    https://code.claude.com/docs/en/agent-sdk/cost-tracking
    """
    recorder = _Recorder()

    async def go() -> None:
        async with TestServer(recorder.app()) as server:
            reporter = UsageReporter(
                str(server.make_url("/v1/work-item-progress/example-request/usage")), TOKEN
            )
            reporter.observe(_assistant(FAST, _sdk_usage(100, 10)))
            reporter.observe(
                _assistant(
                    OPUS,
                    _sdk_usage(16, 64, cached=45517, write=29094),
                    parent="toolu_example",
                    message_id="msg_reviewer_early",
                )
            )
            await reporter.report(
                _result(model_usage={FAST: _model_usage(100, 10)}, uuid="turn_early"), FAST
            )
            await reporter.report(
                _result(
                    model_usage={
                        FAST: _model_usage(100, 10),
                        OPUS_1M: _model_usage(16, 64, cached=45517, write=29094),
                    },
                    uuid="turn_caught_up",
                ),
                FAST,
            )

    anyio.run(go)
    assert [body["turn_id"] for body, _ in recorder.received] == ["turn_early"]
    early = _by_role_model(recorder.received[0][0])
    assert set(early) == {("implementer", FAST), ("reviewer", OPUS)}
