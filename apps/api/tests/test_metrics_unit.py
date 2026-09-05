"""Unit tests for the Langfuse metrics query builders (no I/O)."""

import asyncio
import time
import uuid
from collections.abc import Callable, Coroutine
from typing import Any

import pytest
from curie_api import metrics
from curie_api.metrics import (
    _cost_known,
    _error_rate,
    _filters,
    _scalar_query,
    agent_trace_filter,
    resolve_window,
)


def test_filters_exclude_eval_traces_from_the_aggregate() -> None:
    # #547: eval traces (`eval:<suite>:<case>`) are billed but not product
    # traffic; the summary must exclude them so runs/tokens/cost aren't inflated.
    for view, name_col in (("traces", "name"), ("observations", "traceName")):
        filters = _filters(view, None, None)
        assert {
            "column": name_col,
            "operator": "does not contain",
            "value": "eval:",
            "type": "string",
        } in filters, view


def test_cost_known_flags_priced_to_zero_as_unknown() -> None:
    # #547: tokens spent but cost summed to exactly 0 => a missing Langfuse price
    # row, not a free run. A genuinely zero-work window stays cost-known.
    assert _cost_known(tokens=2576, cost_usd=0.0) is False
    assert _cost_known(tokens=2576, cost_usd=0.0506) is True
    assert _cost_known(tokens=0, cost_usd=0.0) is True


def test_agent_trace_filter_is_the_agent_id_token() -> None:
    # The runner names traces curie-run:agent-<id>-thread-<ts>, so the per-agent
    # filter must be the `agent-<id>` substring, not the agent's display name.
    agent_id = uuid.UUID("00000000-0000-0000-0000-000000000042")
    token = agent_trace_filter(agent_id)
    assert token == "agent-00000000-0000-0000-0000-000000000042"
    # The token is a substring of a real runner trace name.
    assert token in f"curie-run:{token}-thread-1720200000"


def test_agent_filter_matches_the_runner_trace_name() -> None:
    # A filter built from the id must select against the trace name via `contains`.
    agent_id = uuid.uuid4()
    token = agent_trace_filter(agent_id)
    filters = _filters("observations", None, token)
    assert {
        "column": "traceName",
        "operator": "contains",
        "value": token,
        "type": "string",
    } in filters


def test_resolve_window_uses_explicit_bounds() -> None:
    start, end = resolve_window("2026-01-01T00:00:00+00:00", "2026-01-02T00:00:00+00:00", 24)
    assert start == "2026-01-01T00:00:00+00:00"
    assert end == "2026-01-02T00:00:00+00:00"


def test_resolve_window_defaults_to_window_hours() -> None:
    start, end = resolve_window(None, "2026-01-08T00:00:00+00:00", 168)
    assert start == "2026-01-01T00:00:00+00:00"


def test_filters_use_the_right_name_column_per_view() -> None:
    traces = _filters("traces", "prod", "billing")
    assert {"column": "environment", "operator": "=", "value": "prod", "type": "string"} in traces
    assert any(f["column"] == "name" and f["value"] == "billing" for f in traces)

    observations = _filters("observations", None, "billing")
    assert any(f["column"] == "traceName" for f in observations)
    assert all(f["column"] != "environment" for f in observations)


def test_scalar_query_maps_metric_to_view_and_measure() -> None:
    q = _scalar_query("cost_usd", "s", "e", None, None)
    assert q["view"] == "observations"
    assert q["metrics"] == [{"measure": "totalCost", "aggregation": "sum"}]
    assert "timeDimension" not in q

    series_q = _scalar_query("runs", "s", "e", None, None, granularity="day")
    assert series_q["view"] == "traces"
    assert series_q["timeDimension"] == {"granularity": "day"}


def test_latency_is_measured_per_run_on_the_traces_view() -> None:
    # p95 latency must be a per-run aggregate, not span-weighted (observations).
    q = _scalar_query("latency_p95_ms", "s", "e", None, None)
    assert q["view"] == "traces"
    assert q["metrics"] == [{"measure": "latency", "aggregation": "p95"}]


def test_error_rate_from_level_rows() -> None:
    rows = [
        {"level": "DEFAULT", "count_count": "8"},
        {"level": "ERROR", "count_count": "2"},
    ]
    assert _error_rate(rows) == 0.2
    assert _error_rate([]) == 0.0


# --- concurrent query dispatch (no I/O; the Langfuse client is faked) --------
#
# summary() issues five independent Langfuse queries and cost_known() issues
# two. They share no state and the real client is an httpx.AsyncClient, so they
# are dispatched together: the wall time is the slowest round trip rather than
# the sum. The tests below pin both halves of that change, the speedup AND the
# fact that the caller still sees the same values and the same failure.


_Handler = Callable[[dict[str, Any]], Coroutine[Any, Any, list[dict[str, Any]]]]


class _FakeLangfuse:
    """A no-I/O stand-in for LangfuseClient with a per-query delay.

    Records every query it is handed, sleeps `delay` seconds, then awaits
    `handler(query)` for the rows -- or lets it raise. The delay is what lets a
    test tell concurrent dispatch from sequential dispatch by wall time alone;
    a handler may add its own extra sleep where a test needs one.
    """

    def __init__(
        self,
        delay: float = 0.0,
        handler: _Handler | None = None,
    ) -> None:
        self.delay = delay
        self.handler = handler
        self.queries: list[dict[str, Any]] = []

    async def query_metrics(self, query: dict[str, Any]) -> list[dict[str, Any]]:
        self.queries.append(query)
        if self.delay:
            await asyncio.sleep(self.delay)
        return await self.handler(query) if self.handler else []


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


# 0.2s per query: sequential summary() would floor at 5 * 0.2 = 1.0s, and the
# assertion allows 2 * 0.2 = 400ms. That leaves ~200ms of scheduling headroom
# above the concurrent time (one delay) and a 600ms margin below the sequential
# floor, so a loaded CI box cannot flip the verdict either way. 0.05s left only
# ~50ms of headroom, which a busy runner can eat. cost_known() keeps the same
# ratio: two queries, a 400ms sequential floor against the same 400ms bound, and
# the same one-delay concurrent time.
_DELAY = 0.2


@pytest.mark.anyio
async def test_summary_runs_its_queries_concurrently() -> None:
    lf = _FakeLangfuse(delay=_DELAY)

    started = time.perf_counter()
    await metrics.summary(lf, "s", "e", None, None)
    elapsed = time.perf_counter() - started

    assert len(lf.queries) == 5, "four scalars plus the level query"
    assert elapsed < 2 * _DELAY, (
        f"summary() took {elapsed:.3f}s; the old sequential shape takes about "
        f"{5 * _DELAY:.3f}s, so this is the query dispatch going serial again"
    )


@pytest.mark.anyio
async def test_cost_known_runs_its_two_queries_concurrently() -> None:
    lf = _FakeLangfuse(delay=_DELAY)

    started = time.perf_counter()
    await metrics.cost_known(lf, "s", "e", None, None)
    elapsed = time.perf_counter() - started

    assert len(lf.queries) == 2, "the cost query plus the tokens query"
    assert elapsed < 2 * _DELAY, (
        f"cost_known() took {elapsed:.3f}s; sequentially it takes about "
        f"{2 * _DELAY:.3f}s"
    )


async def _distinct_rows(query: dict[str, Any]) -> list[dict[str, Any]]:
    """A different canned row per query, so a mis-assignment cannot pass.

    Dispatches on the query shape the builders produce: the level query is the
    only one carrying `dimensions`, and the four scalars are told apart by their
    measure.
    """

    if "dimensions" in query:
        return [
            {"level": "DEFAULT", "count_count": 8},
            {"level": "ERROR", "count_count": 2},
        ]
    measure = query["metrics"][0]["measure"]
    return {
        "count": [{"count_count": 7}],
        "latency": [{"p95_latency": 123.5}],
        "totalTokens": [{"sum_totalTokens": 4242}],
        "totalCost": [{"sum_totalCost": 0.0506}],
    }[measure]


@pytest.mark.anyio
async def test_summary_returns_the_same_values_as_the_sequential_shape() -> None:
    # Concurrency must not scramble which row lands in which field. Every scalar
    # is a distinct value, so any cross-wiring changes the assertion below.
    lf = _FakeLangfuse(handler=_distinct_rows)

    result = await metrics.summary(
        lf, "2026-01-01T00:00:00+00:00", "2026-01-02T00:00:00+00:00", None, None
    )

    assert result.runs == 7
    assert result.latency_p95_ms == 123.5
    assert result.tokens == 4242
    assert result.cost_usd == 0.0506
    assert result.cost_known is True
    assert result.error_rate == 0.2
    assert result.start == "2026-01-01T00:00:00+00:00"
    assert result.end == "2026-01-02T00:00:00+00:00"


class _RunsQueryFailed(RuntimeError):
    pass


class _CostQueryFailed(ValueError):
    pass


class _LevelQueryFailed(RuntimeError):
    pass


@pytest.mark.anyio
async def test_summary_raises_the_first_failing_query_in_order() -> None:
    # Sequentially, the caller saw the failure of the earliest query in
    # SCALAR_METRICS order; a later query's failure was invisible behind it.
    # Here `runs` (first) fails slowly and `cost_usd` (fourth) fails instantly,
    # so completion order and list order disagree. A bare asyncio.gather would
    # surface _CostQueryFailed, which is the race this pins against.
    finished: list[str] = []

    async def handler(query: dict[str, Any]) -> list[dict[str, Any]]:
        measure = query["metrics"][0]["measure"]
        if "dimensions" not in query:
            if measure == "count":
                await asyncio.sleep(_DELAY)
                raise _RunsQueryFailed("runs query failed")
            if measure == "totalCost":
                raise _CostQueryFailed("cost query failed")
        # The survivors outlive the runs failure, so a cancellation of the
        # in-flight siblings would drop them off `finished`.
        await asyncio.sleep(_DELAY * 2)
        finished.append(measure)
        return []

    lf = _FakeLangfuse(handler=handler)

    with pytest.raises(_RunsQueryFailed):
        await metrics.summary(lf, "s", "e", None, None)

    assert len(lf.queries) == 5, "four scalars plus the level query are all dispatched"
    assert sorted(finished) == ["count", "latency", "totalTokens"], (
        "the queries that did not fail must run to completion, not be cancelled "
        f"mid-flight; finished={finished}"
    )


@pytest.mark.anyio
async def test_summary_still_fails_when_a_query_fails() -> None:
    # return_exceptions=True collects failures instead of raising them, so the
    # ordered re-raise is the only thing keeping a single broken query from
    # being silently swallowed into a zero-valued summary.
    async def handler(query: dict[str, Any]) -> list[dict[str, Any]]:
        if "dimensions" in query:
            raise _LevelQueryFailed("level query failed")
        return []

    with pytest.raises(_LevelQueryFailed):
        await metrics.summary(_FakeLangfuse(handler=handler), "s", "e", None, None)


class _LatencyQueryFailed(RuntimeError):
    pass


class _TokensQueryFailed(RuntimeError):
    pass


@pytest.mark.anyio
async def test_summary_prefers_an_earlier_conversion_failure_over_a_later_query_failure() -> None:
    # The old sequential code converted each scalar before issuing the next
    # query: runs-query, runs-convert, latency-query, latency-convert, and so
    # on. A nonnumeric value in the `runs` response therefore raised ValueError
    # before the latency query was ever sent, and the latency failure could not
    # be seen. Gathering the five raw queries first and converting afterwards
    # inverts that and surfaces the latency failure instead, so the unit of work
    # has to be the query plus its own conversion.
    async def handler(query: dict[str, Any]) -> list[dict[str, Any]]:
        if "dimensions" in query:
            return []
        measure = query["metrics"][0]["measure"]
        if measure == "count":
            return [{"count_count": "not-a-number"}]
        if measure == "latency":
            raise _LatencyQueryFailed("latency query failed")
        return []

    with pytest.raises(ValueError) as excinfo:
        await metrics.summary(_FakeLangfuse(handler=handler), "s", "e", None, None)

    assert not isinstance(excinfo.value, _LatencyQueryFailed), (
        "the later latency query's failure must not preempt the earlier runs "
        f"conversion failure; got {excinfo.value!r}"
    )
    assert "not-a-number" in str(excinfo.value)


@pytest.mark.anyio
async def test_cost_known_prefers_a_later_query_failure_over_a_conversion_failure() -> None:
    # Deliberate asymmetry with summary(), not an oversight. The old
    # cost_known() awaited BOTH queries before calling _num on either, so a
    # failure of the LATER tokens query beat a conversion failure in the EARLIER
    # cost response. Query-plus-convert units here would flip that precedence,
    # so this pins the equivalent direction against a "consistency fix".
    async def handler(query: dict[str, Any]) -> list[dict[str, Any]]:
        measure = query["metrics"][0]["measure"]
        if measure == "totalCost":
            return [{"sum_totalCost": "not-a-number"}]
        if measure == "totalTokens":
            raise _TokensQueryFailed("tokens query failed")
        return []

    with pytest.raises(_TokensQueryFailed):
        await metrics.cost_known(_FakeLangfuse(handler=handler), "s", "e", None, None)


@pytest.mark.anyio
async def test_summary_prefers_an_int_conversion_failure_over_a_bad_level_row() -> None:
    # The original built MetricsSummary(..., runs=int(...), ..., tokens=int(...),
    # ..., error_rate=_error_rate(level_rows)). Constructor arguments are
    # evaluated left to right, so int(runs) ran BEFORE the error-rate reduction,
    # and a level row that _num could not convert was invisible behind an
    # infinite runs count. Folding the reduction into the level gather unit
    # inverts that, so this pins the level unit staying query-only.
    #
    # "Infinity" is what a Langfuse response can carry through json and float()
    # alike, and it is int() that then refuses it with OverflowError.
    async def handler(query: dict[str, Any]) -> list[dict[str, Any]]:
        if "dimensions" in query:
            return [{"level": "ERROR", "count_count": "not-a-number"}]
        if query["metrics"][0]["measure"] == "count":
            return [{"count_count": "Infinity"}]
        return []

    with pytest.raises(OverflowError) as excinfo:
        await metrics.summary(_FakeLangfuse(handler=handler), "s", "e", None, None)

    assert not isinstance(excinfo.value, ValueError), (
        "the level row's conversion failure must not preempt int(runs); "
        f"got {excinfo.value!r}"
    )
