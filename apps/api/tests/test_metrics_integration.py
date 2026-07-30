"""Metrics endpoints against the REAL Langfuse dev instance.

Faithfulness is checked by comparing each endpoint number to the same aggregate
queried directly from Langfuse (the done-when: verified against Langfuse's own
aggregates). Skips when the dev stack is not reachable so the unit suite stays
runnable standalone.
"""

import datetime
import json
import time
import urllib.parse
import uuid
from typing import Any

import httpx
import pytest
from curie_api.config import get_settings


def _stack_up() -> bool:
    try:
        httpx.get(
            f"{get_settings().langfuse_host}/api/public/health", timeout=2.0
        ).raise_for_status()
    except Exception:
        return False
    return True


pytestmark = pytest.mark.skipif(not _stack_up(), reason="dev stack not reachable")


def _lf_metric(query: dict[str, Any]) -> list[dict[str, Any]]:
    settings = get_settings()
    url = f"{settings.langfuse_host}/api/public/metrics?query=" + urllib.parse.quote(
        json.dumps(query)
    )
    resp = httpx.get(
        url, auth=(settings.langfuse_public_key, settings.langfuse_secret_key), timeout=10
    )
    resp.raise_for_status()
    data: list[dict[str, Any]] = resp.json()["data"]
    return data


def _window() -> tuple[str, str]:
    now = datetime.datetime.now(datetime.UTC)
    return (now - datetime.timedelta(days=90)).isoformat(), now.isoformat()


def _seed_cost_bearing_trace() -> tuple[str, float]:
    """Ingest one trace + generation carrying an explicit cost into Langfuse.

    Keeps the cost assertion hermetic: it verifies against a workload this test
    created rather than depending on ambient data left by other lanes, which is
    absent on a fresh instance. Langfuse honours an explicit ``costDetails`` and
    surfaces it via the totalCost measure, so no model-price table is required.
    """

    settings = get_settings()
    ts = datetime.datetime.now(datetime.UTC).isoformat()
    name = f"metrics-cost-seed-{uuid.uuid4().hex}"
    trace_id = str(uuid.uuid4())
    total_cost = 0.0424242
    batch = {
        "batch": [
            {
                "id": str(uuid.uuid4()),
                "type": "trace-create",
                "timestamp": ts,
                "body": {
                    "id": trace_id,
                    "name": name,
                    "timestamp": ts,
                    "environment": "default",
                },
            },
            {
                "id": str(uuid.uuid4()),
                "type": "generation-create",
                "timestamp": ts,
                "body": {
                    "id": str(uuid.uuid4()),
                    "traceId": trace_id,
                    "type": "GENERATION",
                    "name": "llm.generation",
                    "startTime": ts,
                    "endTime": ts,
                    "model": "claude-opus-4-8",
                    "environment": "default",
                    "usageDetails": {"input": 1200, "output": 88, "total": 1288},
                    "costDetails": {"input": 0.03, "output": 0.0124242, "total": total_cost},
                },
            },
        ]
    }
    resp = httpx.post(
        f"{settings.langfuse_host}/api/public/ingestion",
        auth=(settings.langfuse_public_key, settings.langfuse_secret_key),
        json=batch,
        timeout=15,
    )
    resp.raise_for_status()
    assert not resp.json()["errors"], resp.text
    return name, total_cost


def _await_seeded_cost(name: str, start: str, end: str, floor: float) -> None:
    """Block until the seeded cost is queryable; Langfuse ingests to ClickHouse
    asynchronously. Fails (never skips) if it does not land within the budget."""

    for _ in range(45):
        rows = _lf_metric(
            {
                "view": "observations",
                "metrics": [{"measure": "totalCost", "aggregation": "sum"}],
                "filters": [
                    {"column": "traceName", "operator": "=", "value": name, "type": "string"}
                ],
                "fromTimestamp": start,
                "toTimestamp": end,
            }
        )
        got = rows[0].get("sum_totalCost") if rows else None
        if got is not None and float(got) >= floor - 1e-9:
            return
        time.sleep(2)
    pytest.fail(f"seeded cost for {name!r} never became queryable in Langfuse")


def test_summary_matches_langfuse_aggregates(client: Any, auth_headers: dict[str, str]) -> None:
    # Seed our own cost-bearing workload so the assertions do not depend on
    # ambient data, then wait for Langfuse's async ingestion to reflect it.
    seed_name, seeded_cost = _seed_cost_bearing_trace()
    start, end = _window()
    _await_seeded_cost(seed_name, start, end, seeded_cost)

    resp = client.get(
        "/observability/metrics/summary",
        params={"start": start, "end": end},
        headers=auth_headers,
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["runs"] >= 1  # at least the trace we just seeded

    runs = _lf_metric(
        {
            "view": "traces",
            "metrics": [{"measure": "count", "aggregation": "count"}],
            # The summary excludes eval traces (#547), so the reference aggregate
            # must exclude them too or the counts diverge when the shared Langfuse
            # holds eval traces in the window.
            "filters": [
                {
                    "column": "name",
                    "operator": "does not contain",
                    "value": "eval:",
                    "type": "string",
                }
            ],
            "fromTimestamp": start,
            "toTimestamp": end,
        }
    )
    assert body["runs"] == int(float(runs[0]["count_count"]))

    cost = _lf_metric(
        {
            "view": "observations",
            "metrics": [{"measure": "totalCost", "aggregation": "sum"}],
            # Same eval exclusion as the summary (#547); observations key on
            # traceName.
            "filters": [
                {
                    "column": "traceName",
                    "operator": "does not contain",
                    "value": "eval:",
                    "type": "string",
                }
            ],
            "fromTimestamp": start,
            "toTimestamp": end,
        }
    )
    # The endpoint must equal Langfuse's own aggregate, and both must include at
    # least the cost we seeded (proves a real, non-null cost is reported).
    assert body["cost_usd"] >= seeded_cost - 1e-9
    assert abs(body["cost_usd"] - float(cost[0]["sum_totalCost"])) < 1e-9


def test_runs_series_sums_to_the_summary(client: Any, auth_headers: dict[str, str]) -> None:
    start, end = _window()
    summary = client.get(
        "/observability/metrics/summary",
        params={"start": start, "end": end},
        headers=auth_headers,
    ).json()
    series = client.get(
        "/observability/metrics/series",
        params={"metric": "runs", "start": start, "end": end, "granularity": "day"},
        headers=auth_headers,
    )
    assert series.status_code == 200, series.text
    points = series.json()["points"]
    assert points, "expected at least one time bucket"
    assert sum(p["value"] for p in points) == summary["runs"]


def test_environment_filter_passes_through(client: Any, auth_headers: dict[str, str]) -> None:
    start, end = _window()
    resp = client.get(
        "/observability/metrics/summary",
        params={"start": start, "end": end, "environment": "default"},
        headers=auth_headers,
    )
    assert resp.status_code == 200
    filtered = _lf_metric(
        {
            "view": "traces",
            "metrics": [{"measure": "count", "aggregation": "count"}],
            "filters": [
                {"column": "environment", "operator": "=", "value": "default", "type": "string"},
                # The summary excludes eval traces (#547); match it here too.
                {
                    "column": "name",
                    "operator": "does not contain",
                    "value": "eval:",
                    "type": "string",
                },
            ],
            "fromTimestamp": start,
            "toTimestamp": end,
        }
    )
    assert resp.json()["runs"] == int(float(filtered[0]["count_count"]))


def test_metrics_require_api_key(client: Any) -> None:
    assert client.get("/observability/metrics/summary").status_code == 401


def test_unknown_metric_is_422(client: Any, auth_headers: dict[str, str]) -> None:
    resp = client.get(
        "/observability/metrics/series",
        params={"metric": "bogus"},
        headers=auth_headers,
    )
    assert resp.status_code == 422


def test_malformed_date_is_422_not_500(client: Any, auth_headers: dict[str, str]) -> None:
    resp = client.get(
        "/observability/metrics/summary",
        params={"start": "not-a-date"},
        headers=auth_headers,
    )
    assert resp.status_code == 422
