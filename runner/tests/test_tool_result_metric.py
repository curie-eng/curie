"""Every tool result the runner sees is counted by origin and outcome.

A connector tool that answers with an error changes no turn status: the turn
still ends done and the model tells the person it could not read the system.
``curie.tool.result`` is the signal an alert can read, and the WARNING it pairs
with a failed connector call is what names the connector, because the metric
may not carry any identifier.

The fake model is the mock at the adapter seam, so translation and the session
run unmodified. Metric points are read back from a real OpenTelemetry
MeterProvider installed through ``curie_telemetry.configure_meter_provider``,
so the real ``record_metric`` validates every name, key and value.

The fake stops replaying at an interrupting deny, before the denied call's
result the real CLI delivers, so its own ``can_use_tool`` path cannot reach
awaiting_approval. The gate case here runs the runner's real PreToolUse approval
hook itself and scripts the result the real CLI was measured to deliver; the
same case on the real CLI is in ``test_tool_result_real_cli.py``. The stop
cases call the runner's own ``interrupt`` and ``timeout`` from inside the fake
between a call and its result, where the real CLI's cut-off result lands.
"""

from __future__ import annotations

import logging
import re
from collections.abc import Awaitable, Callable, Iterator
from functools import partial
from typing import Any, Literal

import anyio
import pytest
from aci_protocol import Event, Final, SessionStatus, parse_ndjson
from claude_agent_sdk import (
    AssistantMessage,
    ResultMessage,
    TextBlock,
    ToolResultBlock,
    ToolUseBlock,
    UserMessage,
)
from curie_runner import RunTracer, SideEffectClassifier
from curie_runner.approval import ApprovalGate, build_approval_gate, build_approval_hook
from curie_runner.fake import FakeModelSession
from curie_runner.session import SessionRunner
from curie_telemetry import configure_meter_provider
from curie_telemetry import metrics as curie_metrics
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader

_METRIC = "curie.tool.result"
# Distinctive strings a failed call carries in its arguments and its result. The
# connector WARNING must name the server and tool and nothing the call said.
_ARGUMENT = "acme-account-PLACEHOLDER"
_RESULT_TEXT = "upstream answered 401 for acme-ledger-PLACEHOLDER"
# The opaque turn epoch the server hands a turn, which a timeout names.
_EPOCH = "acme-epoch-PLACEHOLDER"
# What the real CLI answers a call it cut off on an SDK interrupt with, as an
# is_error result, followed by an error_during_execution result whose
# terminal_reason is aborted_tools (measured on claude-agent-sdk 0.2.159,
# bundled CLI 2.1.281, 2026-09-29). An operator stop and a turn deadline both
# reach the CLI as that interrupt.
_CUT_OFF_TEXT = "The user doesn't want to proceed with this tool use. The tool use was rejected."


@pytest.fixture
def reader(monkeypatch: pytest.MonkeyPatch) -> Iterator[InMemoryMetricReader]:
    """A real provider for this test only; the module globals are restored after."""

    monkeypatch.setattr(curie_metrics, "_provider", curie_metrics._provider)
    monkeypatch.setattr(curie_metrics, "_instruments", curie_metrics._instruments)
    metric_reader = InMemoryMetricReader()
    provider = MeterProvider(metric_readers=[metric_reader], shutdown_on_exit=False)
    configure_meter_provider(provider)
    yield metric_reader
    provider.shutdown()


def _points(reader: InMemoryMetricReader) -> dict[tuple[str, str], float]:
    """Summed ``curie.tool.result`` values keyed by (origin, outcome)."""

    data = reader.get_metrics_data()
    points: dict[tuple[str, str], float] = {}
    if data is None:
        return points
    for resource_metrics in data.resource_metrics:
        for scope_metrics in resource_metrics.scope_metrics:
            for metric in scope_metrics.metrics:
                if metric.name != _METRIC:
                    continue
                for point in getattr(metric.data, "data_points", ()):
                    attributes = dict(point.attributes)
                    assert set(attributes) == {"service.name", "source", "origin", "outcome"}
                    assert attributes["service.name"] == "curie-runner"
                    assert attributes["source"] == "runner"
                    key = (str(attributes["origin"]), str(attributes["outcome"]))
                    points[key] = points.get(key, 0) + point.value
    return {key: value for key, value in points.items() if value}


def _connector_warnings(caplog: pytest.LogCaptureFixture) -> list[tuple[str, str, str]]:
    """(server, tool, message) for each WARNING that names a server and a tool."""

    found: list[tuple[str, str, str]] = []
    for record in caplog.records:
        if record.levelno < logging.WARNING:
            continue
        message = record.getMessage()
        server = re.search(r"(?:^|\s)server=(\S+)", message)
        tool = re.search(r"(?:^|\s)tool=(\S+)", message)
        if server and tool:
            found.append((server.group(1), tool.group(1), message))
    return found


def _use(*calls: tuple[str, str, dict[str, Any]]) -> AssistantMessage:
    return AssistantMessage(
        content=[ToolUseBlock(id=call_id, name=name, input=args) for call_id, name, args in calls],
        model="fake-model",
    )


def _answer(*results: tuple[str, Any, bool | None]) -> UserMessage:
    """Tool results on the UserMessage the SDK delivers them on.

    ``is_error`` is passed through as given: the real CLI reports a successful
    MCP call with ``is_error=None`` and a failed one with ``True`` (measured on
    claude-agent-sdk 0.2.159, bundled CLI 2.1.281, 2026-09-29).
    """

    return UserMessage(
        content=[
            ToolResultBlock(tool_use_id=call_id, content=content, is_error=is_error)
            for call_id, content, is_error in results
        ]
    )


def _done() -> list[Any]:
    return [
        AssistantMessage(content=[TextBlock(text="done")], model="fake-model"),
        ResultMessage(
            subtype="success",
            duration_ms=1,
            duration_api_ms=1,
            is_error=False,
            num_turns=1,
            session_id="fake-session",
            result="done",
        ),
    ]


class _HookedFake(FakeModelSession):
    """The fake, with the runner's real PreToolUse approval hook run on every call.

    Ordered as the real CLI orders it: the tool_use reaches the session first,
    then the hook decides, then the call's result arrives. The script supplies
    the result; this only runs the hook, so the gate records a hold exactly as
    production's hook does.
    """

    def __init__(self, script_factory: Any, gate: ApprovalGate) -> None:
        super().__init__(script_factory)
        # Typed Any: the SDK types the callback against its TypedDict union,
        # and the CLI's hook input is a plain dict at runtime.
        self._hook: Any = build_approval_hook(gate)["PreToolUse"][0].hooks[0]

    async def receive_turn(self) -> Any:
        async for message in super().receive_turn():
            yield message
            if isinstance(message, AssistantMessage):
                for block in message.content:
                    if isinstance(block, ToolUseBlock):
                        await self._hook(
                            {"tool_name": block.name, "tool_input": block.input}, block.id, None
                        )


class _StoppingFake(FakeModelSession):
    """The fake, stopping the turn once the first tool call has reached the session.

    ``stop`` is the runner's own stop entry point (``interrupt`` or ``timeout``),
    bound after the runner is built. The replay is not truncated by the stop:
    the script carries what the real CLI delivers after an interrupt, the cut-off
    call's synthetic result and the aborted terminal result.
    """

    def __init__(self, script_factory: Any) -> None:
        super().__init__(script_factory, truncate_on_interrupt=False)
        self.stop: Callable[[], Awaitable[object]] | None = None

    async def receive_turn(self) -> Any:
        stopped = False
        async for message in super().receive_turn():
            yield message
            if (
                not stopped
                and self.stop is not None
                and isinstance(message, AssistantMessage)
                and any(isinstance(block, ToolUseBlock) for block in message.content)
            ):
                stopped = True
                await self.stop()


def _run(
    *scripts: list[Any],
    gate: ApprovalGate | None = None,
    stop: Literal["interrupt", "timeout"] | None = None,
) -> list[Final]:
    """Run one turn per script through a real SessionRunner; return each final."""

    remaining = list(scripts)

    def next_script() -> list[Any]:
        return remaining.pop(0)

    fake: FakeModelSession
    if gate is not None:
        fake = _HookedFake(next_script, gate)
    elif stop is not None:
        fake = _StoppingFake(next_script)
    else:
        fake = FakeModelSession(next_script)
    runner = SessionRunner(
        session_factory=lambda: fake,
        ceiling=0,
        tracer=RunTracer(None),
        classifier=SideEffectClassifier(),
        trace_name="curie-run:acme-tool-result",
        session_id="session-PLACEHOLDER",
        model="fake-model",
        approval_gate=gate,
    )
    if isinstance(fake, _StoppingFake):
        fake.stop = runner.interrupt if stop == "interrupt" else partial(runner.timeout, _EPOCH)
    finals: list[Final] = []

    async def go() -> None:
        await runner.start()
        try:
            for index in range(len(scripts)):
                lines = [
                    line
                    async for line in runner.run_turn(
                        Event(type="message", text="go", user="U0EXAMPLE1", ts=str(index)),
                        turn_epoch=_EPOCH,
                    )
                ]
                finals.extend(
                    event for event in parse_ndjson("".join(lines)) if isinstance(event, Final)
                )
        finally:
            await runner.close()

    anyio.run(go)
    return finals


def test_a_failed_connector_call_counts_as_a_connector_error_and_logs_one_warning(
    reader: InMemoryMetricReader, caplog: pytest.LogCaptureFixture
) -> None:
    """AC3, AC5, AC6: the case the whole change exists for.

    Red until the session records ``curie.tool.result`` for a closed call (no
    point at all today) and logs the connector WARNING. Red again if origin is
    taken from anything but the ``mcp__`` prefix, if the WARNING parses the
    server and tool at a different ``__``, or if it carries the call's arguments
    or the connector's reply.
    """

    caplog.set_level(logging.WARNING, logger="curie_runner")
    finals = _run(
        [
            _use(("toolu_ledger", "mcp__acme__read_ledger", {"account": _ARGUMENT})),
            _answer(("toolu_ledger", _RESULT_TEXT, True)),
            *_done(),
        ]
    )

    # The turn itself is untouched: an errored tool result is still a done turn.
    assert [final.status for final in finals] == [SessionStatus.DONE]
    assert _points(reader) == {("connector", "error"): 1}
    warnings = _connector_warnings(caplog)
    assert [(server, tool) for server, tool, _ in warnings] == [("acme", "read_ledger")]
    # Fragments, so a truncated copy of the reply is caught too.
    for record in caplog.records:
        if record.levelno >= logging.WARNING:
            for fragment in (_ARGUMENT, "acme-ledger-PLACEHOLDER", "upstream answered"):
                assert fragment not in record.getMessage()


def test_a_successful_connector_call_counts_as_success_and_logs_nothing(
    reader: InMemoryMetricReader, caplog: pytest.LogCaptureFixture
) -> None:
    """AC3, AC6: ``is_error`` None (the CLI's success shape) and False are both success.

    Red until the point is recorded. Red again if success is decided by
    ``not is_error`` on a value other than True, or if a successful call logs.
    """

    caplog.set_level(logging.WARNING, logger="curie_runner")
    _run(
        [
            _use(
                ("toolu_list", "mcp__acme__list_files", {"folder": _ARGUMENT}),
                ("toolu_read", "mcp__acme__read_ledger", {"account": _ARGUMENT}),
            ),
            _answer(("toolu_list", "[]", None), ("toolu_read", "{}", False)),
            *_done(),
        ]
    )

    assert _points(reader) == {("connector", "success"): 2}
    assert _connector_warnings(caplog) == []


def test_a_failed_builtin_call_counts_as_builtin_and_logs_nothing(
    reader: InMemoryMetricReader, caplog: pytest.LogCaptureFixture
) -> None:
    """AC5, AC6: a CLI tool is not a connector, so it is not the alert's concern.

    Red until the point is recorded. Red again if a non-``mcp__`` name is
    counted as a connector, or if a builtin error logs the connector WARNING.
    """

    caplog.set_level(logging.WARNING, logger="curie_runner")
    _run(
        [
            _use(
                ("toolu_read", "Read", {"file_path": "/workspace/missing-PLACEHOLDER"}),
                ("toolu_bash", "Bash", {"command": "echo hi"}),
            ),
            _answer(
                ("toolu_read", "File does not exist.", True),
                ("toolu_bash", "hi\n", False),
            ),
            *_done(),
        ]
    )

    assert _points(reader) == {("builtin", "error"): 1, ("builtin", "success"): 1}
    assert _connector_warnings(caplog) == []


def test_platform_tools_count_as_platform_and_log_nothing(
    reader: InMemoryMetricReader, caplog: pytest.LogCaptureFixture
) -> None:
    """AC5, AC6: Curie's own in-process tools wear ``mcp__`` names but are not connectors.

    ``mcp__curie-state__get`` is classified as platform even though this runner
    mounted no state server: origin grants nothing, so it classifies against
    ``platform_tool_names(state_server_mounted=True)``. A policy approval
    request is a successful platform call, never awaiting_approval, which is
    reserved for a call the runner's permission gate held.

    Red until the points are recorded. Red again if platform names are
    classified by an ``mcp__curie`` prefix instead of exact membership (see the
    impostor below), or if a platform error logs the connector WARNING.
    """

    caplog.set_level(logging.WARNING, logger="curie_runner")
    _run(
        [
            _use(
                ("toolu_state", "mcp__curie-state__get", {"key": _ARGUMENT}),
                (
                    "toolu_approval",
                    "mcp__curie__request_approval",
                    {"summary": "scale acme-api to 10"},
                ),
                # A different server whose key merely begins ``curie__``: exact
                # membership makes it a connector, as #2286 made it for policy.
                ("toolu_impostor", "mcp__curie__extra__lookup", {}),
            ),
            _answer(
                ("toolu_state", "state API unavailable", True),
                ("toolu_approval", "approval requested", None),
                ("toolu_impostor", "{}", None),
            ),
            *_done(),
        ]
    )

    assert _points(reader) == {
        ("platform", "error"): 1,
        ("platform", "success"): 1,
        ("connector", "success"): 1,
    }
    assert _connector_warnings(caplog) == []


def test_a_plugin_mounted_connector_is_a_connector_named_by_its_first_separator(
    reader: InMemoryMetricReader, caplog: pytest.LogCaptureFixture
) -> None:
    """AC5, AC6: a bundle's own MCP server has a ``plugin_<bundle>_<server>`` key.

    The WARNING splits the live name after ``mcp__`` at its first ``__``, so the
    server token is the whole plugin key and the tool is the rest, including a
    tool name that itself contains ``__``.

    Red until the points and the WARNING exist. Red again if the parse strips
    the plugin infix, or splits at the last ``__`` instead of the first.
    """

    caplog.set_level(logging.WARNING, logger="curie_runner")
    _run(
        [
            _use(
                ("toolu_issue", "mcp__plugin_acme_github__create_issue", {"title": _ARGUMENT}),
                ("toolu_repo", "mcp__plugin_acme_github__get_repo", {"name": _ARGUMENT}),
                ("toolu_export", "mcp__acme__ledger__export", {"account": _ARGUMENT}),
            ),
            _answer(
                ("toolu_issue", _RESULT_TEXT, True),
                ("toolu_repo", "{}", None),
                ("toolu_export", _RESULT_TEXT, True),
            ),
            *_done(),
        ]
    )

    assert _points(reader) == {("connector", "error"): 2, ("connector", "success"): 1}
    warnings = _connector_warnings(caplog)
    assert [(server, tool) for server, tool, _ in warnings] == [
        ("plugin_acme_github", "create_issue"),
        ("acme", "ledger__export"),
    ]
    for _, _, message in warnings:
        assert _ARGUMENT not in message
        assert _RESULT_TEXT not in message


def test_several_failed_connector_calls_log_one_warning_each(
    reader: InMemoryMetricReader, caplog: pytest.LogCaptureFixture
) -> None:
    """AC2, AC6: one point and one WARNING per result, across messages of one turn.

    Red until recorded. Red again if the session observes only the first
    result of a message, or re-observes an earlier message's results on every
    later message (the values would sum past three).
    """

    caplog.set_level(logging.WARNING, logger="curie_runner")
    _run(
        [
            _use(
                ("toolu_1", "mcp__acme__read_ledger", {"account": _ARGUMENT}),
                ("toolu_2", "mcp__acme__read_ledger", {"account": _ARGUMENT}),
            ),
            _answer(("toolu_1", _RESULT_TEXT, True), ("toolu_2", _RESULT_TEXT, True)),
            _use(("toolu_3", "mcp__acme-billing__read_invoice", {"id": _ARGUMENT})),
            _answer(("toolu_3", _RESULT_TEXT, True)),
            _use(("toolu_4", "mcp__acme__list_files", {})),
            _answer(("toolu_4", "[]", None)),
            *_done(),
        ]
    )

    assert _points(reader) == {("connector", "error"): 3, ("connector", "success"): 1}
    warnings = _connector_warnings(caplog)
    assert sorted((server, tool) for server, tool, _ in warnings) == [
        ("acme", "read_ledger"),
        ("acme", "read_ledger"),
        ("acme-billing", "read_invoice"),
    ]


def test_only_a_result_closing_a_call_seen_this_turn_is_counted(
    reader: InMemoryMetricReader, caplog: pytest.LogCaptureFixture
) -> None:
    """AC2: an unseen id, a second result for one id, and last turn's call count nothing.

    The first turn makes a call whose result never arrives. The second turn
    receives a result for that id, a result for an id nobody called, then one
    real call answered twice. Only the first answer to the real call counts.

    Red until the closing result is counted. Red again if the session counts
    every ToolResultBlock rather than popping the call it closes, or if the
    call map outlives its turn.
    """

    caplog.set_level(logging.WARNING, logger="curie_runner")
    finals = _run(
        [
            _use(("toolu_stale", "mcp__acme__read_ledger", {"account": _ARGUMENT})),
            *_done(),
        ],
        [
            _answer(("toolu_stale", _RESULT_TEXT, True)),
            _answer(("toolu_unseen", _RESULT_TEXT, True)),
            _use(("toolu_real", "mcp__acme__list_files", {})),
            _answer(("toolu_real", "[]", None)),
            _answer(("toolu_real", _RESULT_TEXT, True)),
            *_done(),
        ],
    )

    assert len(finals) == 2
    assert _points(reader) == {("connector", "success"): 1}
    assert _connector_warnings(caplog) == []


def test_a_held_call_is_awaiting_approval_and_another_tools_error_is_still_an_error(
    reader: InMemoryMetricReader, caplog: pytest.LogCaptureFixture
) -> None:
    """AC3, AC4, AC6: the hold covers the one call the gate holds, and nothing else.

    The model calls a gated and an ungated connector tool in one message. The
    gate denies the first and the second fails on its own. The denied result is
    scripted as the real CLI delivers it: is_error True with the hook's text,
    then a success result whose terminal_reason is hook_stopped (measured on
    claude-agent-sdk 0.2.159, bundled CLI 2.1.281, 2026-09-29).

    Red until the points exist. Red again if a hold is read from
    ``pending_halt`` alone, which would count the ungated failure as
    awaiting_approval and hide a real connector error; or if the held call logs
    the connector WARNING.
    """

    caplog.set_level(logging.WARNING, logger="curie_runner")
    gate = build_approval_gate(
        operator_tools=["mcp__acme__delete_files"],
        policy_routes={},
        mcp_servers=set(),
        connector_servers={"acme"},
    )
    assert gate is not None
    finals = _run(
        [
            _use(
                ("toolu_delete", "mcp__acme__delete_files", {"account": _ARGUMENT}),
                ("toolu_ledger", "mcp__acme__read_ledger", {"account": _ARGUMENT}),
            ),
            _answer(
                (
                    "toolu_delete",
                    "PreToolUse:mcp__acme__delete_files hook error: approval required",
                    True,
                ),
                ("toolu_ledger", _RESULT_TEXT, True),
            ),
            ResultMessage(
                subtype="success",
                duration_ms=1,
                duration_api_ms=1,
                is_error=False,
                num_turns=1,
                session_id="fake-session",
                result="",
                terminal_reason="hook_stopped",
            ),
        ],
        gate=gate,
    )

    assert [final.status for final in finals] == [SessionStatus.AWAITING_APPROVAL]
    assert finals[0].approval_granted_tool == "mcp__acme__delete_files"
    assert _points(reader) == {("connector", "awaiting_approval"): 1, ("connector", "error"): 1}
    warnings = _connector_warnings(caplog)
    assert [(server, tool) for server, tool, _ in warnings] == [("acme", "read_ledger")]


def _cut_off_turn() -> list[Any]:
    """A connector call the stop cuts off, shaped as the real CLI delivers it."""

    return [
        _use(("toolu_slow", "mcp__acme__slow_read", {"account": _ARGUMENT})),
        _answer(("toolu_slow", _CUT_OFF_TEXT, True)),
        ResultMessage(
            subtype="error_during_execution",
            duration_ms=1,
            duration_api_ms=1,
            is_error=True,
            num_turns=1,
            session_id="fake-session",
            terminal_reason="aborted_tools",
        ),
    ]


def test_a_connector_call_an_operator_stopped_counts_as_cancelled(
    reader: InMemoryMetricReader, caplog: pytest.LogCaptureFixture
) -> None:
    """The fake-tier twin of the real CLI's operator stop case.

    ``SessionRunner.interrupt`` lands after the call reached the session and
    before its result, where the real CLI's cut-off result lands. The turn
    ending idle holds today. Red until the manifest declares ``cancelled`` and
    the session records it for an errored result after an operator interrupt;
    red again if that result counts as a connector error or logs the WARNING.
    """

    caplog.set_level(logging.WARNING, logger="curie_runner")
    finals = _run(_cut_off_turn(), stop="interrupt")

    assert [final.status for final in finals] == [SessionStatus.IDLE_AWAITING_INPUT]
    assert _points(reader) == {("connector", "cancelled"): 1}
    assert _connector_warnings(caplog) == []


def test_a_connector_call_the_turn_deadline_cut_off_is_still_a_connector_error(
    reader: InMemoryMetricReader, caplog: pytest.LogCaptureFixture
) -> None:
    """The fake-tier twin of the real CLI's deadline case.

    ``SessionRunner.timeout`` is not an operator stop: a connector holding a
    call to the turn's deadline is failing, so the cut-off result is a connector
    error with its WARNING. Green today; red if the cancelled branch reads the
    timeout's interrupt as an operator stop.
    """

    caplog.set_level(logging.WARNING, logger="curie_runner")
    finals = _run(_cut_off_turn(), stop="timeout")

    assert [final.status for final in finals] == [SessionStatus.CLASSIFIED_FAILURE]
    assert _points(reader) == {("connector", "error"): 1}
    warnings = _connector_warnings(caplog)
    assert [(server, tool) for server, tool, _ in warnings] == [("acme", "slow_read")]
