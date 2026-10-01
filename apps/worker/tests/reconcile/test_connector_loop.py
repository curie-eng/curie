"""The reconcile loop's failure containment (ADR-0090, #1184).

The loop shares a process with the kernel, whose four correctness rules are not
negotiable. So most of what matters here is what happens when something goes
wrong: one bad agent, a raising client, a database that will not answer.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

import httpx
import pytest
from curie_worker.connector_agent import AgentOutcome, RenderedConnectors
from curie_worker.connector_apply import ApplyReport
from curie_worker.connector_loop import AgentTarget, ConnectorReconcileLoop, PassSummary

pytestmark = pytest.mark.anyio


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


def target(name: str) -> AgentTarget:
    import uuid

    return AgentTarget(agent_id=uuid.uuid4(), agent_name=name, version_id=uuid.uuid4())


class Loop(ConnectorReconcileLoop):
    """The loop with the database and the reconcile step swapped out.

    Subclassed rather than mocked: `one_pass`'s containment logic is the thing
    under test, and it should be exercised exactly as written.
    """

    def __init__(self, targets: list[AgentTarget], outcomes: dict[str, Any], **kw: Any) -> None:
        super().__init__(
            engine=None,  # type: ignore[arg-type]
            source=None,  # type: ignore[arg-type]
            client=None,  # type: ignore[arg-type]
            namespace="curie",
            db_schema="curie",
            interval_seconds=kw.get("interval_seconds", 0.01),
        )
        self._targets = targets
        self._outcomes = outcomes
        self.seen: list[str] = []

    async def targets(self) -> list[AgentTarget]:  # type: ignore[override]
        return list(self._targets)

    def _reconcile_one(self, t: AgentTarget) -> AgentOutcome:  # type: ignore[override]
        self.seen.append(t.agent_name)
        result = self._outcomes[t.agent_name]
        if isinstance(result, Exception):
            raise result
        return result


def ok(agent: str, applied: int = 0, deleted: int = 0) -> AgentOutcome:
    return AgentOutcome(
        agent=agent,
        report=ApplyReport(
            applied=[("Service", f"s{i}") for i in range(applied)],
            deleted=[("Service", f"d{i}") for i in range(deleted)],
        ),
    )


def platform_server_error(status_code: int = 503) -> httpx.HTTPStatusError:
    request = httpx.Request("GET", "http://api:8000/agents/a/connectors")
    response = httpx.Response(status_code, request=request)
    return httpx.HTTPStatusError(
        f"platform API returned {status_code}",
        request=request,
        response=response,
    )


# --------------------------------------------------------------------------- #
# One agent's failure ends with that agent
# --------------------------------------------------------------------------- #
async def test_a_raising_agent_does_not_strand_the_rest() -> None:
    # Aborting the sweep would leave every later agent unreconciled, and the
    # order is arbitrary -- so which agents those are changes between passes.
    loop = Loop(
        [target("a"), target("boom"), target("c")],
        {"a": ok("a", applied=1), "boom": RuntimeError("apiserver down"), "c": ok("c", applied=1)},
    )
    summary = await loop.one_pass()

    assert loop.seen == ["a", "boom", "c"], "the sweep stopped early"
    assert summary.reconciled == 3
    assert summary.applied == 2
    assert summary.failed == 1


async def test_a_failed_report_counts_without_raising() -> None:
    failing = AgentOutcome(
        agent="a", report=ApplyReport(failures=[("Service", "svc", "forbidden")])
    )
    summary = await Loop([target("a")], {"a": failing}).one_pass()
    assert summary.failed == 1


async def test_a_skipped_agent_is_counted_separately_from_a_failure() -> None:
    # Skipping is a correct, expected outcome (an operator-supplied credential
    # that is not provisioned). Counting it as a failure would make the normal
    # state of a partially-migrated install look broken.
    skipped = AgentOutcome(agent="a", skipped="credentials not provisioned")
    summary = await Loop([target("a")], {"a": skipped}).one_pass()
    assert summary.skipped == 1
    assert summary.failed == 0


async def test_a_skipped_agents_deletes_are_counted(caplog) -> None:
    # #1214: an unprovisioned-Secret agent no longer early-returns -- it still
    # runs a delete-only plan, so a skipped outcome can carry a report with
    # real deletes. The `continue` on `outcome.skipped` drops that report on
    # the floor: a pass that pruned three objects would claim zero deletes, in
    # the summary and in the pass log operators actually read.
    skipped = AgentOutcome(
        agent="a",
        skipped="credentials not provisioned",
        report=ApplyReport(deleted=[("Service", "d0"), ("Service", "d1"), ("Service", "d2")]),
    )
    with caplog.at_level("INFO", logger="curie_worker.connector_loop"):
        summary = await Loop([target("a")], {"a": skipped}).one_pass()
    assert summary.skipped == 1
    assert summary.deleted == 3
    assert any("3 deleted" in r.getMessage() for r in caplog.records)


async def test_a_failed_delete_on_a_skipped_agent_still_counts_as_a_failure() -> None:
    # Same gap, the failure side: a delete that errors on a skipped agent must
    # not vanish from `summary.failed` along with the rest of its report.
    skipped = AgentOutcome(
        agent="a",
        skipped="credentials not provisioned",
        report=ApplyReport(failures=[("Service", "d0", "forbidden")]),
    )
    summary = await Loop([target("a")], {"a": skipped}).one_pass()
    assert summary.skipped == 1
    assert summary.failed == 1


async def test_no_agents_is_a_clean_pass() -> None:
    summary = await Loop([], {}).one_pass()
    assert summary == PassSummary()
    assert not summary.did_work


# --------------------------------------------------------------------------- #
# Logging: quiet when converged
# --------------------------------------------------------------------------- #
async def test_a_converged_pass_does_not_log_at_info(caplog) -> None:
    # The steady state is "nothing changed", forever. A loop that narrates every
    # pass trains people to ignore it, and the one pass that mattered scrolls by.
    with caplog.at_level("INFO", logger="curie_worker.connector_loop"):
        await Loop([target("a")], {"a": ok("a")}).one_pass()
    assert caplog.records == []


async def test_a_pass_that_did_work_says_so(caplog) -> None:
    with caplog.at_level("INFO", logger="curie_worker.connector_loop"):
        await Loop([target("a")], {"a": ok("a", applied=2, deleted=1)}).one_pass()
    assert any("2 applied, 1 deleted" in r.getMessage() for r in caplog.records)


async def test_platform_5xx_escalates_only_after_three_consecutive_passes(caplog) -> None:
    loop = Loop([target("a")], {"a": platform_server_error()})

    with caplog.at_level(logging.DEBUG, logger="curie_worker.connector_loop"):
        for pass_number in range(1, 6):
            caplog.clear()
            summary = await loop.one_pass()
            agent_records = [r for r in caplog.records if "agent=a" in r.getMessage()]

            assert agent_records, f"pass {pass_number} did not log the retry"
            if pass_number <= 3:
                assert summary.failed == 0
                assert all(r.levelno < logging.ERROR for r in agent_records)
                assert all(r.exc_info is None for r in agent_records)
                assert all("returned 503" in r.getMessage() for r in agent_records)
                assert all(f"({pass_number}/3)" in r.getMessage() for r in agent_records)
            else:
                assert summary.failed == 1
                assert any(
                    r.levelno >= logging.ERROR and r.exc_info is not None for r in agent_records
                )


async def test_platform_5xx_streak_is_per_agent(caplog) -> None:
    loop = Loop([target("a")], {"a": platform_server_error()})

    with caplog.at_level(logging.DEBUG, logger="curie_worker.connector_loop"):
        for _ in range(3):
            await loop.one_pass()

        loop._targets = [target("b")]
        loop._outcomes = {"b": platform_server_error()}
        caplog.clear()
        summary = await loop.one_pass()

    agent_records = [r for r in caplog.records if "agent=b" in r.getMessage()]
    assert summary.failed == 0
    assert agent_records
    assert all(r.levelno < logging.ERROR for r in agent_records)
    assert all(r.exc_info is None for r in agent_records)
    assert all("returned 503" in r.getMessage() for r in agent_records)
    assert all("(1/3)" in r.getMessage() for r in agent_records)


async def test_non_5xx_failure_resets_platform_5xx_streak_for_the_same_agent(caplog) -> None:
    loop = Loop([target("a")], {"a": platform_server_error()})

    with caplog.at_level(logging.DEBUG, logger="curie_worker.connector_loop"):
        for _ in range(3):
            assert (await loop.one_pass()).failed == 0

        loop._outcomes["a"] = platform_server_error(404)
        caplog.clear()
        loud_summary = await loop.one_pass()
        loud_records = [r for r in caplog.records if "agent=a" in r.getMessage()]

        loop._outcomes["a"] = platform_server_error()
        caplog.clear()
        quiet_summary = await loop.one_pass()

    quiet_records = [r for r in caplog.records if "agent=a" in r.getMessage()]
    assert loud_summary.failed == 1
    assert len(loud_records) == 1
    assert loud_records[0].levelno >= logging.ERROR
    assert loud_records[0].exc_info is not None
    assert quiet_summary.failed == 0
    assert quiet_records
    assert all(r.levelno < logging.ERROR for r in quiet_records)
    assert all(r.exc_info is None for r in quiet_records)
    assert all("returned 503" in r.getMessage() for r in quiet_records)
    assert all("(1/3)" in r.getMessage() for r in quiet_records)


async def test_platform_5xx_streaks_are_isolated_within_one_pass(caplog) -> None:
    agent_a = target("a")
    agent_b = target("b")
    loop = Loop([agent_a], {"a": platform_server_error()})

    with caplog.at_level(logging.DEBUG, logger="curie_worker.connector_loop"):
        for _ in range(3):
            await loop.one_pass()

        loop._targets = [agent_a, agent_b]
        loop._outcomes = {
            "a": platform_server_error(),
            "b": platform_server_error(),
        }
        loop.seen.clear()
        caplog.clear()
        summary = await loop.one_pass()

    agent_a_records = [r for r in caplog.records if "agent=a" in r.getMessage()]
    agent_b_records = [r for r in caplog.records if "agent=b" in r.getMessage()]
    error_records = [r for r in caplog.records if r.levelno >= logging.ERROR]

    assert loop.seen == ["a", "b"]
    assert summary.reconciled == 2
    assert summary.failed == 1
    assert agent_a_records
    assert any(
        r.levelno >= logging.ERROR and r.exc_info is not None for r in agent_a_records
    )
    assert agent_b_records
    assert all(r.levelno < logging.ERROR for r in agent_b_records)
    assert all(r.exc_info is None for r in agent_b_records)
    assert all("returned 503" in r.getMessage() for r in agent_b_records)
    assert all("(1/3)" in r.getMessage() for r in agent_b_records)
    assert len(error_records) == 1
    assert "agent=a" in error_records[0].getMessage()


async def test_success_resets_platform_5xx_streak_for_the_same_agent(caplog) -> None:
    loop = Loop([target("a")], {"a": platform_server_error()})

    with caplog.at_level(logging.DEBUG, logger="curie_worker.connector_loop"):
        for _ in range(3):
            await loop.one_pass()

        loop._outcomes["a"] = ok("a")
        assert (await loop.one_pass()).failed == 0

        loop._outcomes["a"] = platform_server_error()
        caplog.clear()
        summary = await loop.one_pass()

    agent_records = [r for r in caplog.records if "agent=a" in r.getMessage()]
    assert summary.failed == 0
    assert agent_records
    assert all(r.levelno < logging.ERROR for r in agent_records)
    assert all(r.exc_info is None for r in agent_records)
    assert all("returned 503" in r.getMessage() for r in agent_records)
    assert all("(1/3)" in r.getMessage() for r in agent_records)


async def test_non_5xx_exception_fails_immediately_with_traceback(caplog) -> None:
    loop = Loop([target("a")], {"a": RuntimeError("render is invalid")})

    with caplog.at_level(logging.DEBUG, logger="curie_worker.connector_loop"):
        summary = await loop.one_pass()

    agent_records = [r for r in caplog.records if "agent=a" in r.getMessage()]
    assert summary.failed == 1
    assert any(r.levelno >= logging.ERROR and r.exc_info is not None for r in agent_records)


async def test_platform_404_fails_immediately_with_traceback(caplog) -> None:
    loop = Loop([target("a")], {"a": platform_server_error(404)})

    with caplog.at_level(logging.DEBUG, logger="curie_worker.connector_loop"):
        summary = await loop.one_pass()

    agent_records = [r for r in caplog.records if "agent=a" in r.getMessage()]
    assert summary.reconciled == 1
    assert summary.failed == 1
    assert len(agent_records) == 1
    assert agent_records[0].levelno >= logging.ERROR
    assert agent_records[0].exc_info is not None


# --------------------------------------------------------------------------- #
# The loop never takes the worker down
# --------------------------------------------------------------------------- #
async def test_a_pass_that_raises_outright_does_not_end_the_loop() -> None:
    # `targets()` hitting a dead database is the realistic version of this.
    passes = 0

    class Broken(Loop):
        async def one_pass(self):  # type: ignore[override]
            nonlocal passes
            passes += 1
            if passes < 3:
                raise RuntimeError("database is gone")
            return PassSummary()

    loop = Broken([], {}, interval_seconds=0.01)
    stop = asyncio.Event()

    async def run() -> None:
        await loop.run_forever(stop)

    task = asyncio.create_task(run())
    while passes < 3:
        await asyncio.sleep(0.01)
    stop.set()
    await asyncio.wait_for(task, timeout=2)

    assert passes >= 3, "the loop gave up after a failing pass"


async def test_stop_is_honoured_promptly_rather_than_after_the_interval() -> None:
    # A worker shutdown must not wait out a 60s sleep.
    loop = Loop([], {}, interval_seconds=300)
    stop = asyncio.Event()
    task = asyncio.create_task(loop.run_forever(stop))
    await asyncio.sleep(0.05)
    stop.set()
    # If stop did not interrupt the interval wait, this times out rather than
    # sitting for the full 300s.
    await asyncio.wait_for(task, timeout=2)


# --------------------------------------------------------------------------- #
# The manifest source
# --------------------------------------------------------------------------- #
def test_the_http_source_reads_the_fields_the_agent_step_needs(monkeypatch) -> None:
    import functools

    import httpx
    from curie_worker.connector_loop import HttpManifestSource

    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["url"] = str(request.url)
        captured["key"] = request.headers.get("X-API-Key")
        return httpx.Response(
            200,
            json={
                "manifests": [{"kind": "Service", "metadata": {"name": "svc"}}],
                "owned_secret_name": "curie-a-connector-secrets",
                "owned_secret_keys": ["TOKEN"],
                "mcp_entries": {},
            },
        )

    source = HttpManifestSource(
        api_base_url="http://api:8000/",
        api_key="k",
        release="curie",
        namespace="curie",
        app_name="curie",
    )
    monkeypatch.setattr(
        httpx, "Client", functools.partial(httpx.Client, transport=httpx.MockTransport(handler))
    )
    rendered = source.rendered(agent_id="a-1", version_id="v-1")

    assert rendered.manifests[0]["metadata"]["name"] == "svc"
    assert rendered.needs_operator_credentials
    assert rendered.owned_secret_name == "curie-a-connector-secrets"
    assert "/agents/a-1/versions/v-1/connectors" in captured["url"]
    assert "release=curie" in captured["url"], "the caller must supply install-time facts"
    assert captured["key"] == "k"


def test_the_worker_builds_no_loop_when_the_flag_is_off() -> None:
    # The default path must not touch the Kubernetes client at all -- a worker
    # with no kubeconfig and no interest in connectors still has to boot.
    from curie_worker.config import WorkerConfig
    from curie_worker.run import _build_connector_loop

    assert _build_connector_loop(WorkerConfig(), engine=None) is None  # type: ignore[arg-type]


def test_enabling_the_reconciler_without_addressing_it_is_refused() -> None:
    # Names are built from release + app_name. With either missing, every name
    # the reconciler renders differs from what exists: it finds nothing of its
    # own, creates a parallel set under wrong names, and leaves the real
    # connectors unmanaged -- silently, looking like it works.
    import pydantic
    from curie_worker.config import WorkerConfig

    with pytest.raises(pydantic.ValidationError, match="CURIE_CONNECTOR_APP_NAME"):
        WorkerConfig(
            connector_reconcile_enabled=True,
            connector_namespace="curie",
            connector_release="curie",
            connector_app_name="",
        )


def test_the_http_source_defaults_are_safe_when_the_api_omits_fields() -> None:
    # An older API that predates owned_secret_keys must not read as "no
    # credentials needed", which would let the loop prune one.
    rendered = RenderedConnectors()
    assert rendered.manifests == []
    assert not rendered.needs_operator_credentials


# --------------------------------------------------------------------------- #
# The in-force version has no bundle (#1216)
# --------------------------------------------------------------------------- #
# These exercise the REAL `_reconcile_one`, so only `targets()` is replaced.
# The fake client and its manifest helper are the ones
# `tests/reconcile/test_connector_agent.py` already established -- imported
# rather than re-declared, so a second, subtly different notion of "what the
# cluster returns" cannot drift away from the first.
from .test_connector_agent import FakeClient, live_copy, manifest  # noqa: E402


class ExplodingSource:
    """A ManifestSource that must never be asked for anything.

    The #1216 bug is invisible to a source that answers: the loop happily
    rendered SOMETHING, just from the wrong version. Making the render itself
    the failure is what turns "did not render" into an assertion.
    """

    def rendered(self, *, agent_id: str, version_id: str) -> Any:
        raise AssertionError(
            f"rendered a bundleless version: agent={agent_id} version={version_id}"
        )


class StubTargetsLoop(ConnectorReconcileLoop):
    """The loop with only the database swapped out.

    Unlike `Loop` above, `_reconcile_one` is the real one -- the branch under
    test lives there, so faking it would test nothing.
    """

    def __init__(self, targets: list[AgentTarget], **kw: Any) -> None:
        super().__init__(namespace="curie", db_schema="curie", **kw)
        self._targets = targets

    async def targets(self) -> list[AgentTarget]:  # type: ignore[override]
        return list(self._targets)


def bundleless(name: str) -> AgentTarget:
    import uuid

    return AgentTarget(
        agent_id=uuid.uuid4(), agent_name=name, version_id=uuid.uuid4(), has_bundle=False
    )


async def test_a_bundleless_target_never_renders_and_prunes_what_it_owns() -> None:
    # The render endpoint 404s for a version with no stored bundle, so asking
    # would fail this agent every pass forever. The objects it owns belong to a
    # declaration nothing can produce, so they are pruned instead.
    agent = bundleless("a")
    client = FakeClient(
        [
            live_copy(manifest("Service", "grafana"), agent="a"),
            live_copy(manifest("Deployment", "grafana"), agent="a"),
        ]
    )
    loop = StubTargetsLoop([agent], engine=None, source=ExplodingSource(), client=client)

    summary = await loop.one_pass()

    assert client.applied == [], "a bundleless version must not apply anything"
    assert sorted(client.deleted) == [("Deployment", "grafana"), ("Service", "grafana")]
    assert summary.skipped == 1
    assert summary.deleted == 2
    assert summary.failed == 0


async def test_a_bundleless_target_never_deletes_an_owned_secret() -> None:
    # Without a render we do not know which Secret name is ours, and the one
    # live here is the operator's credential (ADR-0086) -- unrecoverable, and
    # its removal breaks every connector pod on its next restart. So no Secret
    # of any name is manageable on this path, exactly as on the unprovisioned
    # branch.
    agent = bundleless("a")
    client = FakeClient(
        [
            live_copy(manifest("Service", "grafana"), agent="a"),
            live_copy(manifest("Secret", "curie-a-connector-secrets"), agent="a"),
        ]
    )
    loop = StubTargetsLoop([agent], engine=None, source=ExplodingSource(), client=client)

    await loop.one_pass()

    assert client.deleted == [("Service", "grafana")]
    assert all(kind != "Secret" for kind, _ in client.deleted)


async def test_a_converged_bundleless_target_touches_the_cluster_only_to_look() -> None:
    # Nothing owned means nothing to remove. The steady state of an agent parked
    # on a bundleless version must not re-issue deletes every minute.
    agent = bundleless("a")
    client = FakeClient([])
    loop = StubTargetsLoop([agent], engine=None, source=ExplodingSource(), client=client)

    summary = await loop.one_pass()

    assert client.applied == []
    assert client.deleted == []
    assert summary.skipped == 1
    assert summary.deleted == 0


# --------------------------------------------------------------------------- #
# A persistent skip is an edge, not a heartbeat (#1215)
# --------------------------------------------------------------------------- #
# A skip is the expected steady state of a partially-migrated install, so it
# recurs every pass for as long as nobody acts. Logging it every pass is the
# narration the module docstring warns about. The skip stays visible as a log
# line on each transition and as a gauge on every pass.
import curie_worker.connector_loop as connector_loop_module  # noqa: E402
from curie_telemetry import record_metric  # noqa: E402

_SKIPPED_AGENTS_GAUGE = "curie.connector.reconcile.skipped_agents"


def unprovisioned_source() -> Any:
    """A render whose operator-supplied Secret is absent from the cluster."""

    class _Source:
        def rendered(self, *, agent_id: str, version_id: str) -> RenderedConnectors:
            return RenderedConnectors(
                manifests=[manifest("Deployment", "dep")],
                owned_secret_name="curie-a-connector-secrets",
                owned_secret_keys=["TOKEN"],
            )

    return _Source()


def skipped(agent: str, reason: str) -> AgentOutcome:
    return AgentOutcome(agent=agent, skipped=reason, report=ApplyReport())


def _records_at(caplog: pytest.LogCaptureFixture, level: int) -> list[logging.LogRecord]:
    return [r for r in caplog.records if r.levelno == level]


def _capture_skipped_gauge(monkeypatch: pytest.MonkeyPatch) -> list[float]:
    # Wrapped, not replaced: the real `record_metric` refuses an undeclared
    # instrument or label, which is the declaration half of this contract.
    recorded: list[float] = []

    def capture(name: str, value: float = 1, *, attributes: dict[str, str] | None = None) -> None:
        record_metric(name, value, attributes=attributes)
        if name == _SKIPPED_AGENTS_GAUGE:
            recorded.append(value)

    monkeypatch.setattr(connector_loop_module, "record_metric", capture)
    return recorded


async def test_an_unchanging_skip_logs_one_warning_and_no_info_across_ten_passes(
    caplog: pytest.LogCaptureFixture,
) -> None:
    # The real reconcile step, so a WARNING from `connector_agent` counts too:
    # the issue reproduced ten from each module for ten passes.
    agent = target("a")
    loop = StubTargetsLoop(
        [agent], engine=None, source=unprovisioned_source(), client=FakeClient([])
    )

    with caplog.at_level(logging.DEBUG, logger="curie_worker"):
        for _ in range(10):
            summary = await loop.one_pass()
            assert summary.skipped == 1, "the skip is still counted every pass"

    warnings = _records_at(caplog, logging.WARNING)
    assert len(warnings) == 1, [r.getMessage() for r in warnings]
    assert "agent=a" in warnings[0].getMessage()
    assert "curie cluster deploy" in warnings[0].getMessage(), "the line says what to do"
    assert _records_at(caplog, logging.INFO) == [], [
        r.getMessage() for r in _records_at(caplog, logging.INFO)
    ]


async def test_a_skip_is_not_work() -> None:
    # Pinned on its own so reverting `did_work` to count `skipped` is red here
    # by name, not only through a log count. The skip's own WARNING already
    # reports its entry, so the pass line need not rise to INFO for it.
    loop = Loop([target("a")], {"a": skipped("a", "credentials not provisioned")})
    for _ in range(3):
        summary = await loop.one_pass()
        assert summary.skipped == 1
        assert not summary.did_work, "a skip is not work"


async def test_a_changed_skip_reason_logs_again(caplog: pytest.LogCaptureFixture) -> None:
    outcomes: dict[str, Any] = {"a": skipped("a", "reason one")}
    loop = Loop([target("a")], outcomes)

    with caplog.at_level(logging.DEBUG, logger="curie_worker"):
        await loop.one_pass()
        await loop.one_pass()
        outcomes["a"] = skipped("a", "reason two")
        caplog.clear()
        await loop.one_pass()

    warnings = _records_at(caplog, logging.WARNING)
    assert len(warnings) == 1
    assert "agent=a" in warnings[0].getMessage()
    assert "reason two" in warnings[0].getMessage()


async def test_leaving_the_skip_logs_once(caplog: pytest.LogCaptureFixture) -> None:
    outcomes: dict[str, Any] = {"a": skipped("a", "credentials not provisioned")}
    loop = Loop([target("a")], outcomes)

    with caplog.at_level(logging.DEBUG, logger="curie_worker"):
        await loop.one_pass()
        outcomes["a"] = ok("a")
        caplog.clear()
        await loop.one_pass()
        leaving = [r for r in caplog.records if r.levelno >= logging.INFO]
        caplog.clear()
        await loop.one_pass()
        steady = [r for r in caplog.records if r.levelno >= logging.INFO]

    assert len(leaving) == 1, [r.getMessage() for r in leaving]
    assert leaving[0].levelno == logging.INFO
    assert "agent=a" in leaving[0].getMessage()
    assert "no longer skipped" in leaving[0].getMessage()
    assert steady == [], [r.getMessage() for r in steady]


async def test_the_skipped_agents_gauge_reports_every_pass(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # With the repeating line gone, the gauge is what keeps a persistent skip
    # visible: one point per pass, the number of agents skipped right now.
    recorded = _capture_skipped_gauge(monkeypatch)
    outcomes: dict[str, Any] = {
        "a": skipped("a", "credentials not provisioned"),
        "b": skipped("b", "no stored bundle"),
        "c": ok("c"),
    }
    loop = Loop([target("a"), target("b"), target("c")], outcomes)

    await loop.one_pass()
    await loop.one_pass()
    outcomes["a"] = ok("a")
    await loop.one_pass()

    assert recorded == [2, 2, 1]


async def test_an_agent_no_longer_deployed_leaves_the_skipped_gauge(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    recorded = _capture_skipped_gauge(monkeypatch)
    a, b = target("a"), target("b")
    loop = Loop([a, b], {"a": skipped("a", "r"), "b": skipped("b", "r")})

    await loop.one_pass()
    loop._targets = [b]
    await loop.one_pass()

    assert recorded == [2, 1]
