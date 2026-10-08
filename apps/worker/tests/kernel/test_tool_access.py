"""The worker's half of per-turn tool access (WORKER-TOOL-ACCESS-1..4).

A queued turn may ask to run ``read-only``. These tests drive the real
``Kernel.process_event`` against real Valkey and the scriptable HTTP runner in
``conftest.py``: what the runner advertises, what it is sent, whether a live
turn is steered, and whether an approval is ever created.
"""

from __future__ import annotations

import asyncio
import functools
import json
import sys
from collections.abc import Iterator
from pathlib import Path

import pytest
from aci_protocol import (
    ApprovalRequest,
    Final,
    QueuedTurn,
    SessionStatus,
    TextDelta,
    ToolAccess,
)
from curie_runner.__main__ import build_runner
from curie_runner.config import RunnerConfig
from curie_runner.fake import FakeModelSession
from curie_runner.otel import RunTracer
from curie_runner.server import create_app as create_runner_app
from curie_runner.session import SessionRunner
from curie_runner.side_effects import SideEffectClassifier
from curie_telemetry import configure_meter_provider
from curie_telemetry import metrics as curie_metrics
from curie_worker.approvals import CreatedApproval
from curie_worker.kernel.failures import ThreadBusyError
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader

# importlib import mode does not add the test root to sys.path.
sys.path.insert(0, str(Path(__file__).parent.parent))

from queue_fixtures import qevent  # noqa: E402
from queue_fixtures import wait_until as _wait_until  # noqa: E402

_REFUSAL = (
    "This agent cannot start: its runner cannot enforce read-only tool access for "
    "this turn, so the turn was not run."
)
_APPROVAL_REFUSAL = (
    "This read-only turn asked for an approval, which it may not do. "
    "No approval was created."
)
_qevent = functools.partial(qevent, received_at="2026-09-30T00:00:00+00:00")


def _assert_refused(h: object) -> None:
    """WORKER-TOOL-ACCESS-2: a failed turn, marked and completed escalated."""

    sink = h.sink  # type: ignore[attr-defined]
    assert sink.last_text is not None
    assert sink.last_text.startswith("curie-turn-failure: tool-access-unenforced\n\n")
    assert _REFUSAL in sink.last_text
    assert [c.outcome for c in sink.completions][-1] == "escalated"


def _restricted(text: str = "Reply with exactly: nonce-0001", **kwargs: object) -> QueuedTurn:
    turn = _qevent(text, **kwargs)  # type: ignore[arg-type]
    return turn.model_copy(update={"tool_access": ToolAccess.READ_ONLY})


class RecordingApprovals:
    """An ApprovalCreator fake that records requests."""

    def __init__(self) -> None:
        self.requests: list[ApprovalRequest] = []

    async def create(self, request: ApprovalRequest, *, budget_s: float = 120) -> CreatedApproval:
        self.requests.append(request)
        return CreatedApproval(id=f"appr-{len(self.requests)}", status="pending")


def _awaiting() -> list:
    return [
        TextDelta(text="Requesting sign-off"),
        Final(
            text="Requesting sign-off",
            status=SessionStatus.AWAITING_APPROVAL,
            approval_summary="Tool call awaiting approval: Bash",
            approval_gate_kind="permission",
            approval_granted_tool="Bash",
        ),
    ]


def test_a_restricted_turn_reaches_a_runner_that_enforces_it(make_harness) -> None:
    # @spec WORKER-TOOL-ACCESS-1 WORKER-TOOL-ACCESS-2
    async def go() -> None:
        async with make_harness() as h:
            h.runner.tool_access_enforced = ["read-only"]
            turn = _restricted(thread="th-ro-1")

            await h.kernel.process_event(turn)

            assert h.runner.opened == [turn.text]
            assert h.runner.event_bodies[0]["tool_access"] == "read-only"
            # The advertisement was read from this runner before the event.
            assert h.runner.status_headers, "the runner's status was never read"
            assert h.sink.last_text == "ok"

    asyncio.run(go())


def test_an_unrestricted_turn_opens_exactly_as_before(make_harness) -> None:
    # @spec WORKER-TOOL-ACCESS-1: no advertisement needed, no extra status read.
    async def go() -> None:
        async with make_harness() as h:
            turn = _qevent("hello", thread="th-ro-2")

            await h.kernel.process_event(turn)

            assert h.runner.opened == ["hello"]
            assert h.runner.event_bodies[0]["tool_access"] is None
            assert h.runner.status_headers == []
            assert h.sink.last_text == "ok"

    asyncio.run(go())


def test_a_runner_that_does_not_advertise_read_only_never_runs_the_turn(
    make_harness,
) -> None:
    # @spec WORKER-TOOL-ACCESS-2: the older runner would ignore the field and
    # run the probe unrestricted, so it is refused once, before the model.
    async def go() -> None:
        async with make_harness() as h:
            assert h.runner.tool_access_enforced is None
            turn = _restricted(thread="th-ro-3")

            await h.kernel.process_event(turn)

            assert h.runner.opened == []
            assert h.runner.queried == []
            _assert_refused(h)
            assert await h.async_redis.exists(h.config.done_key(turn.event_id))

    asyncio.run(go())


def test_a_runner_advertising_other_values_never_runs_the_turn(make_harness) -> None:
    # @spec WORKER-TOOL-ACCESS-2: only the exact value counts.
    async def go() -> None:
        async with make_harness() as h:
            h.runner.tool_access_enforced = []
            turn = _restricted(thread="th-ro-4")

            await h.kernel.process_event(turn)

            assert h.runner.opened == []
            _assert_refused(h)

    asyncio.run(go())


def test_an_unreadable_runner_status_opens_nothing_and_is_retried(make_harness) -> None:
    # @spec WORKER-TOOL-ACCESS-2 WORKER-TOOL-ACCESS-3: nothing is sent, and the
    # delivery is left for redelivery rather than refused.
    async def go() -> None:
        async with make_harness() as h:
            h.runner.tool_access_enforced = ["read-only"]
            h.runner.status_fails = True
            turn = _restricted(thread="th-ro-5")

            with pytest.raises(ThreadBusyError):
                await h.kernel.process_event(turn)

            assert h.runner.opened == []
            assert h.sink.last_text is None or _REFUSAL not in h.sink.last_text
            assert not await h.async_redis.exists(h.config.done_key(turn.event_id))

    asyncio.run(go())


def test_a_restricted_turn_never_steers_a_live_turn(make_harness) -> None:
    # @spec WORKER-TOOL-ACCESS-3
    async def go() -> None:
        async with make_harness() as h:
            h.runner.tool_access_enforced = ["read-only"]
            hold = asyncio.Event()
            h.runner.hold = hold
            first = asyncio.create_task(
                h.kernel.process_event(_qevent("a person's question", thread="th-ro-6"))
            )
            try:
                await _wait_until(lambda: h.runner.turn_active, "the first turn to be live")
                with pytest.raises(ThreadBusyError):
                    await h.kernel.process_event(_restricted(thread="th-ro-6"))
                steers = list(h.runner.steers)
                opened = list(h.runner.opened)
            finally:
                hold.set()
                await asyncio.gather(first, return_exceptions=True)

            assert steers == [], "a restricted turn was steered into a live turn"
            assert opened == ["a person's question"]

    asyncio.run(go())


def test_a_read_only_turn_ending_awaiting_approval_creates_no_approval(
    make_harness,
) -> None:
    # @spec WORKER-TOOL-ACCESS-4
    async def go() -> None:
        approvals = RecordingApprovals()
        async with make_harness(approvals=approvals) as h:
            h.runner.tool_access_enforced = ["read-only"]
            h.runner.default_script = _awaiting()
            turn = _restricted(thread="th-ro-7")

            await h.kernel.process_event(turn)

            assert approvals.requests == []
            assert h.sink.posts == [], "an approval card was posted"
            assert h.sink.last_text is not None
            assert h.sink.last_text.endswith(_APPROVAL_REFUSAL)
            assert await h.async_redis.exists(h.config.done_key(turn.event_id))

    asyncio.run(go())


def test_the_same_final_on_an_ordinary_turn_still_creates_the_approval(
    make_harness,
) -> None:
    # @spec WORKER-TOOL-ACCESS-4: the control for the test above.
    async def go() -> None:
        approvals = RecordingApprovals()
        async with make_harness(approvals=approvals) as h:
            h.runner.default_script = _awaiting()

            await h.kernel.process_event(_qevent("please run it", thread="th-ro-8"))

            assert len(approvals.requests) == 1
            assert len(h.sink.posts) == 1

    asyncio.run(go())


# --- the real runner behind the worker --------------------------------------------
#
# The two halves meet over HTTP: the production runner app (its model seam faked,
# nothing else) serves the kernel harness. The fake model's default turn calls
# Bash, so what is proven is the whole path a canary relies on: the worker reads
# the real advertisement, forwards the access, and the runner refuses the write.


@pytest.fixture
def tool_results(monkeypatch: pytest.MonkeyPatch) -> Iterator[InMemoryMetricReader]:
    """A real meter provider for this test only; the module globals are restored."""

    monkeypatch.setattr(curie_metrics, "_provider", curie_metrics._provider)
    monkeypatch.setattr(curie_metrics, "_instruments", curie_metrics._instruments)
    reader = InMemoryMetricReader()
    provider = MeterProvider(metric_readers=[reader], shutdown_on_exit=False)
    configure_meter_provider(provider)
    yield reader
    provider.shutdown()


def _refused_builtin_calls(reader: InMemoryMetricReader) -> float:
    data = reader.get_metrics_data()
    total = 0.0
    for resource_metrics in data.resource_metrics if data is not None else ():
        for scope_metrics in resource_metrics.scope_metrics:
            for metric in scope_metrics.metrics:
                if metric.name != "curie.tool.result":
                    continue
                for point in getattr(metric.data, "data_points", ()):
                    attributes = dict(point.attributes)
                    if attributes.get("outcome") == "refused" and attributes.get(
                        "origin"
                    ) == "builtin":
                        total += point.value
    return total


def _booted_runner(tmp_path: Path) -> SessionRunner:
    """The fake-model runner exactly as ``python -m curie_runner`` boots it."""

    plugin_dir = tmp_path / "bundle"
    (plugin_dir / ".claude-plugin").mkdir(parents=True)
    (plugin_dir / ".claude-plugin" / "plugin.json").write_text(
        json.dumps({"name": "acme-bot"}), encoding="utf-8"
    )
    config = RunnerConfig.from_env(
        {
            "CURIE_PLUGIN_DIR": str(plugin_dir),
            "CURIE_SESSION_ID": "session-acme-read-only",
            "CURIE_SANDBOX_ID": "sandbox-acme-read-only",
            "CURIE_BUDGET": '{"max_output_tokens_per_run": 10000, "max_usd_per_day": 1.0}',
        }
    )
    return build_runner(config, fake_model=True)


def test_the_real_runner_refuses_the_write_a_read_only_turn_attempts(
    make_harness, tmp_path: Path, tool_results: InMemoryMetricReader
) -> None:
    # @spec WORKER-TOOL-ACCESS-1 WORKER-TOOL-ACCESS-2
    async def go() -> None:
        runner = _booted_runner(tmp_path)
        await runner.start()
        async with make_harness(runner_app=create_runner_app(runner)) as h:
            turn = _restricted(thread="th-ro-9")

            await h.kernel.process_event(turn)

            assert h.sink.last_text is not None
            assert h.sink.last_text.endswith("all done")
            assert await h.async_redis.exists(h.config.done_key(turn.event_id))
        assert _refused_builtin_calls(tool_results) == 1, "the runner never refused Bash"

    asyncio.run(go())


def test_a_runner_that_cannot_enforce_is_never_sent_the_turn(
    make_harness, tool_results: InMemoryMetricReader
) -> None:
    # @spec WORKER-TOOL-ACCESS-2: the older-runner shape, a session with no
    # enforcement, lists nothing, so the model is never asked.
    async def go() -> None:
        session = FakeModelSession()
        runner = SessionRunner(
            max_usd_per_day=None,
            held_secrets=frozenset(),
            session_factory=lambda: session,
            ceiling=10_000,
            tracer=RunTracer(None),
            classifier=SideEffectClassifier(),
            trace_name="t",
        )
        await runner.start()
        async with make_harness(runner_app=create_runner_app(runner)) as h:
            turn = _restricted(thread="th-ro-10")

            await h.kernel.process_event(turn)

            assert session.queries == []
            _assert_refused(h)
        assert _refused_builtin_calls(tool_results) == 0

    asyncio.run(go())


def test_a_runner_session_that_ran_an_ordinary_turn_is_not_sent_a_read_only_one(
    make_harness, tmp_path: Path, tool_results: InMemoryMetricReader
) -> None:
    # @spec WORKER-TOOL-ACCESS-2: the real runner stops advertising read-only
    # once its session has sent an unrestricted prompt (RUNNER-TOOL-ACCESS-4),
    # and the worker reads that from the same runner before sending.
    async def go() -> None:
        runner = _booted_runner(tmp_path)
        await runner.start()
        async with make_harness(runner_app=create_runner_app(runner)) as h:
            await h.kernel.process_event(_qevent("an ordinary question", thread="th-ro-11"))
            assert h.sink.last_text is not None and h.sink.last_text.endswith("all done")

            turn = _restricted(thread="th-ro-12")
            await h.kernel.process_event(turn)

            _assert_refused(h)
        assert _refused_builtin_calls(tool_results) == 0

    asyncio.run(go())


# --- the advertisement read itself -----------------------------------------------


def _fail_the_advertisement_read(
    h: object, monkeypatch: pytest.MonkeyPatch, answer: object
) -> list[int]:
    """Make only the tool access status read misbehave; every other read is real.

    ``answer`` is raised when it is an exception, returned otherwise. The busy
    read on the job branch must keep answering, or the turn would defer on it
    and never reach the check under test.
    """

    kernel = h.kernel  # type: ignore[attr-defined]
    real = kernel._runner.status
    hits: list[int] = []

    async def status(*args: object, **kwargs: object) -> object:
        frame = sys._getframe(1)
        while frame is not None:
            if frame.f_code.co_name == "_require_tool_access":
                hits.append(1)
                if isinstance(answer, BaseException):
                    raise answer
                return answer
            frame = frame.f_back  # type: ignore[assignment]
        return await real(*args, **kwargs)

    monkeypatch.setattr(kernel._runner, "status", status)
    monkeypatch.setattr(kernel, "_backoff", lambda _attempt: 0.0)
    return hits


@pytest.mark.parametrize(
    "answer",
    [OSError("status blip"), ["read-only"], "read-only"],
    ids=["unreadable", "a-json-list", "a-json-string"],
)
def test_a_bad_advertisement_read_never_opens_the_turn(
    make_harness, monkeypatch: pytest.MonkeyPatch, answer: object
) -> None:
    # @spec WORKER-TOOL-ACCESS-2: an unreadable or non-object status opens
    # nothing, is retried like any turn the runner did not accept, and is not
    # the refusal (which would claim the runner was read).
    async def go() -> None:
        async with make_harness(max_attempts=2) as h:
            h.runner.tool_access_enforced = ["read-only"]
            hits = _fail_the_advertisement_read(h, monkeypatch, answer)
            turn = _restricted(thread="th-ro-13")

            await h.kernel.process_event(turn)

            assert len(hits) == 2, "each attempt reads the advertisement once"
            assert h.runner.opened == []
            assert h.sink.last_text is not None
            assert h.sink.last_text.startswith("curie-turn-failure: runner-error")
            assert _REFUSAL not in h.sink.last_text

    asyncio.run(go())


def test_the_runners_refusal_classes_are_platform_classes() -> None:
    # @spec WORKER-TOOL-ACCESS-5: a turn the runner refuses escalates under its
    # own class; the runner's constants are the source, read here directly.
    from curie_runner.tool_access import (
        TOOL_ACCESS_REFUSED_CLASSIFICATION,
        TOOL_ACCESS_UNENFORCED_CLASSIFICATION,
    )
    from curie_worker.kernel.constants import PLATFORM_ERROR_CLASSIFICATIONS
    from curie_worker.kernel.failures import _display_error_classification

    for classification in (
        TOOL_ACCESS_UNENFORCED_CLASSIFICATION,
        TOOL_ACCESS_REFUSED_CLASSIFICATION,
    ):
        assert classification in PLATFORM_ERROR_CLASSIFICATIONS
        assert _display_error_classification(classification) == classification


def test_a_turn_the_runner_itself_refuses_escalates_under_its_class(
    make_harness, tmp_path: Path, tool_results: InMemoryMetricReader
) -> None:
    # @spec WORKER-TOOL-ACCESS-5: a read-only slash command passes the worker's
    # check and is refused by the real runner (RUNNER-TOOL-ACCESS-9).
    async def go() -> None:
        runner = _booted_runner(tmp_path)
        await runner.start()
        async with make_harness(runner_app=create_runner_app(runner)) as h:
            await h.kernel.process_event(_restricted("/acme-bot:probe", thread="th-ro-14"))

            assert h.sink.last_text is not None
            assert h.sink.last_text.startswith("curie-turn-failure: tool-access-refused")
            assert [c.outcome for c in h.sink.completions][-1] == "escalated"

    asyncio.run(go())
