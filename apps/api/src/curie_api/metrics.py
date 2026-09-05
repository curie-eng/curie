"""Langfuse-backed metrics for the Metrics tab (OB1).

Builds Langfuse Metrics API queries for the five series the design shows (runs,
latency, tokens, cost, error rate) and assembles them into a summary or a time
series, filterable by environment and by agent (a trace-name match; see the note
below). Every number is a faithful proxy of a Langfuse aggregate.

Agent filtering matches the Langfuse trace name (`name` on traces, `traceName`
on observations) with a `contains` operator. The runner names traces
`curie-run:agent-<agent_id>-thread-<ts>`, so an agent's runs are exactly the
traces whose name contains `agent-<agent_id>`. `agent_trace_filter` builds that
token; callers pass it as the `agent` argument below.
"""

import asyncio
import uuid
from collections.abc import Coroutine
from datetime import UTC, datetime, timedelta
from typing import Any

from .langfuse import LangfuseClient
from .schemas import MetricPoint, MetricSeries, MetricsSummary


def agent_trace_filter(agent_id: uuid.UUID | str) -> str:
    """The trace-name substring that selects one agent's runs.

    The runner names every trace `curie-run:agent-<agent_id>-thread-<ts>`, so
    the agent's traces are those whose name contains `agent-<agent_id>`. This is
    the value passed as the `agent` filter (a `contains` match on trace name);
    matching on `agent.name` never matched a real trace name, which is why
    per-agent cost/traces always read zero.
    """

    return f"agent-{agent_id}"

SCALAR_METRICS = ("runs", "latency_p95_ms", "tokens", "cost_usd")
ALL_METRICS = (*SCALAR_METRICS, "error_rate")

# metric -> (view, measure, aggregation, result-key, is_integer)
# Latency is queried on the traces view so the p95 is per run, not per span
# (a run with many tool/generation spans would otherwise skew a span-weighted p95).
_SPEC: dict[str, tuple[str, str, str, str, bool]] = {
    "runs": ("traces", "count", "count", "count_count", True),
    "latency_p95_ms": ("traces", "latency", "p95", "p95_latency", False),
    "tokens": ("observations", "totalTokens", "sum", "sum_totalTokens", True),
    "cost_usd": ("observations", "totalCost", "sum", "sum_totalCost", False),
}


def resolve_window(
    start: str | None, end: str | None, window_hours: int
) -> tuple[str, str]:
    """Resolve the [start, end] ISO window, defaulting to the last window_hours."""

    end_dt = datetime.fromisoformat(end) if end else datetime.now(UTC)
    start_dt = (
        datetime.fromisoformat(start)
        if start
        else end_dt - timedelta(hours=window_hours)
    )
    return start_dt.isoformat(), end_dt.isoformat()


# Eval traces are named `eval:<suite>:<case_id>` by the worker's eval recorder.
# They are real billed runs but they are NOT product traffic, so counting them in
# the OB1 metrics/cost summary inflates runs/tokens/cost with eval activity (#547).
# The eval matrix has its own surface (EvalModelSummary); exclude eval traces here.
# Excluded via the same name column the agent filter matches on, so no untested
# filter shape is introduced.
_EVAL_TRACE_PREFIX = "eval:"


def _filters(view: str, environment: str | None, agent: str | None) -> list[dict[str, Any]]:
    name_col = "name" if view == "traces" else "traceName"
    filters: list[dict[str, Any]] = []
    if environment:
        filters.append(
            {"column": "environment", "operator": "=", "value": environment, "type": "string"}
        )
    if agent:
        filters.append(
            {"column": name_col, "operator": "contains", "value": agent, "type": "string"}
        )
    # Drop eval traces from the aggregate (#547). Harmless when `agent` is set (an
    # agent's `curie-run:` traces never carry the eval name), load-bearing on the
    # summary tab where `agent` is None and every trace in the window is counted.
    filters.append(
        {
            "column": name_col,
            "operator": "does not contain",
            "value": _EVAL_TRACE_PREFIX,
            "type": "string",
        }
    )
    return filters


def _num(row: dict[str, Any], key: str) -> float:
    value = row.get(key)
    return float(value) if value is not None else 0.0


def _scalar_query(
    metric: str,
    start: str,
    end: str,
    environment: str | None,
    agent: str | None,
    granularity: str | None = None,
) -> dict[str, Any]:
    view, measure, aggregation, _key, _is_int = _SPEC[metric]
    query: dict[str, Any] = {
        "view": view,
        "metrics": [{"measure": measure, "aggregation": aggregation}],
        "filters": _filters(view, environment, agent),
        "fromTimestamp": start,
        "toTimestamp": end,
    }
    if granularity:
        query["timeDimension"] = {"granularity": granularity}
    return query


def _level_query(
    start: str,
    end: str,
    environment: str | None,
    agent: str | None,
    granularity: str | None = None,
) -> dict[str, Any]:
    query: dict[str, Any] = {
        "view": "observations",
        "metrics": [{"measure": "count", "aggregation": "count"}],
        "dimensions": [{"field": "level"}],
        "filters": _filters("observations", environment, agent),
        "fromTimestamp": start,
        "toTimestamp": end,
    }
    if granularity:
        query["timeDimension"] = {"granularity": granularity}
    return query


def _error_rate(rows: list[dict[str, Any]]) -> float:
    total = sum(_num(r, "count_count") for r in rows)
    if total == 0:
        return 0.0
    errors = sum(_num(r, "count_count") for r in rows if r.get("level") == "ERROR")
    return errors / total


async def _gather_ordered(*coros: Coroutine[Any, Any, Any]) -> list[Any]:
    """Run independent units of work concurrently, failing in argument order.

    The summary and cost_known queries share no state and the Langfuse client is
    an httpx.AsyncClient, so running them together makes the wall time the
    slowest round trip instead of the sum of all of them.

    The re-raise is deliberately ordered rather than first-to-fail. These calls
    used to be a sequence of awaits, so the caller saw the failure of the
    EARLIEST unit in the list, and a later unit's failure was invisible
    whenever an earlier one had already raised. A bare ``asyncio.gather`` would
    instead surface whichever unit lost the race, which turns one Langfuse
    outage into a nondeterministic error surface. ``return_exceptions=True``
    lets every unit finish (the queries are read-only GETs, so completing them
    is harmless and leaves no cancelled httpx connections), and the scan below
    then raises the first exception in argument order, reproducing the old
    behavior exactly.

    What counts as one unit differs per caller, and inside ``summary`` it
    differs per argument, because the sequential code each one replaces
    differed. ``summary`` ran query-then-convert per scalar metric, so a bad
    value in an earlier response raised before the next query was even issued;
    its four scalar units are therefore "query plus its own conversion"
    (``_scalar_value``), each returning a float. Its fifth unit, the level
    query, is the query ALONE: the original reduced those rows to an error
    rate inside the ``MetricsSummary(...)`` call, and constructor arguments
    evaluate left to right, so the ``int()`` conversions of runs and tokens ran
    before that reduction. Folding the reduction into this unit would let a bad
    level row preempt them. ``cost_known`` awaited BOTH of its queries before
    either conversion, so there a later query's failure legitimately beat an
    earlier response's conversion failure; both of its units are "query only"
    and it converts afterwards.

    There is no type parameter, because the units are heterogeneous even within
    one ``summary`` call: four floats and one list of rows. The elements are
    typed as ``Any``; every caller destructures the result positionally and
    knows what each of its own units returns.
    """

    results = await asyncio.gather(*coros, return_exceptions=True)
    ordered: list[Any] = []
    for result in results:
        if isinstance(result, BaseException):
            raise result
        ordered.append(result)
    return ordered


async def _scalar_value(
    lf: LangfuseClient,
    metric: str,
    start: str,
    end: str,
    environment: str | None,
    agent: str | None,
) -> float:
    """One scalar metric: issue its query, then convert its own response.

    Query and conversion stay in the same unit so that _gather_ordered's ordered
    re-raise covers a conversion failure at the position the metric occupies,
    exactly as the sequential awaits did.
    """

    rows = await lf.query_metrics(_scalar_query(metric, start, end, environment, agent))
    return _num(rows[0], _SPEC[metric][3]) if rows else 0.0


async def summary(
    lf: LangfuseClient,
    start: str,
    end: str,
    environment: str | None,
    agent: str | None,
) -> MetricsSummary:
    # Scalars in SCALAR_METRICS order, each unit converting its own response,
    # then the level query -- the same sequential order as before. The level
    # rows come back unreduced on purpose: _error_rate runs where it originally
    # did, as the last MetricsSummary argument, after the int() conversions
    # above it. See _gather_ordered for why.
    *scalar_values, level_rows = await _gather_ordered(
        *(
            _scalar_value(lf, metric, start, end, environment, agent)
            for metric in SCALAR_METRICS
        ),
        lf.query_metrics(_level_query(start, end, environment, agent)),
    )
    scalars: dict[str, float] = dict(zip(SCALAR_METRICS, scalar_values, strict=True))

    return MetricsSummary(
        start=start,
        end=end,
        runs=int(scalars["runs"]),
        latency_p95_ms=scalars["latency_p95_ms"],
        tokens=int(scalars["tokens"]),
        cost_usd=scalars["cost_usd"],
        cost_known=_cost_known(scalars["tokens"], scalars["cost_usd"]),
        error_rate=_error_rate(level_rows),
    )


def _cost_known(tokens: float, cost_usd: float) -> bool:
    """Whether a summed cost of `cost_usd` is a real total or a priced-to-zero gap.

    Langfuse returns a generation's cost by matching its model to a stored price
    row; with no matching row it returns 0 even when tokens were spent. So a
    ``cost_usd == 0`` with ``tokens > 0`` is "cost unknown" (a missing price row),
    not "free" -- the exact $0.00-for-a-billed-run confusion in #547. A genuinely
    zero-token window (no work) stays cost-known.
    """

    return not (tokens > 0 and cost_usd == 0.0)


async def cost_known(
    lf: LangfuseClient,
    start: str,
    end: str,
    environment: str | None,
    agent: str | None,
) -> bool:
    """The `cost_known` flag for a window, for callers (get_cost) that fetch cost
    without the token total. Runs the one extra tokens query summary already has."""

    # Cost first, tokens second. Unlike summary(), the unit here is the query
    # ALONE, and both conversions run after both queries -- deliberately, so a
    # later tokens-query failure beats an earlier cost-conversion failure. Do
    # not "consistency-fix" this to match summary(); see _gather_ordered.
    cost_rows, token_rows = await _gather_ordered(
        lf.query_metrics(_scalar_query("cost_usd", start, end, environment, agent)),
        lf.query_metrics(_scalar_query("tokens", start, end, environment, agent)),
    )
    cost = _num(cost_rows[0], _SPEC["cost_usd"][3]) if cost_rows else 0.0
    tokens = _num(token_rows[0], _SPEC["tokens"][3]) if token_rows else 0.0
    return _cost_known(tokens, cost)


async def series(
    lf: LangfuseClient,
    metric: str,
    start: str,
    end: str,
    granularity: str,
    environment: str | None,
    agent: str | None,
) -> MetricSeries:
    if metric == "error_rate":
        points = await _error_rate_series(lf, start, end, granularity, environment, agent)
    else:
        rows = await lf.query_metrics(
            _scalar_query(metric, start, end, environment, agent, granularity)
        )
        key = _SPEC[metric][3]
        points = [
            MetricPoint(ts=str(r.get("time_dimension")), value=_num(r, key))
            for r in rows
            if r.get("time_dimension")
        ]
    return MetricSeries(
        metric=metric, granularity=granularity, start=start, end=end, points=points
    )


async def _error_rate_series(
    lf: LangfuseClient,
    start: str,
    end: str,
    granularity: str,
    environment: str | None,
    agent: str | None,
) -> list[MetricPoint]:
    rows = await lf.query_metrics(
        _level_query(start, end, environment, agent, granularity)
    )
    buckets: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        ts = row.get("time_dimension")
        if ts:
            buckets.setdefault(str(ts), []).append(row)
    return [
        MetricPoint(ts=ts, value=_error_rate(bucket_rows))
        for ts, bucket_rows in sorted(buckets.items())
    ]
