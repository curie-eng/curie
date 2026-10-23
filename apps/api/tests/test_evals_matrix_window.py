"""The matrix scopes its real Langfuse HTTP reads before paging suite traces."""

import asyncio
from datetime import datetime
from typing import Any

import httpx
import pytest
from curie_api.config import Settings
from curie_api.langfuse import LangfuseClient
from curie_api.routers.evals import router
from fastapi import FastAPI


def _trace(
    trace_id: str, timestamp: str, stream_id: str, *, suite: str = "acme-suite"
) -> dict[str, Any]:
    return {
        "id": trace_id,
        "timestamp": timestamp,
        "tags": ["eval", f"suite:{suite}", "version:acme-sha"],
        "metadata": {
            "version": "acme-sha",
            "case_id": trace_id,
            "stream_id": stream_id,
            "outcome": "pass",
            "model": "acme-model",
            "scorer": "grader",
            "cost_usd": 1.25,
        },
        "observations": ["acme-observation"],
        "scores": ["acme-score"],
        "totalCost": 999,
    }


async def _matrix(
    params: dict[str, str],
    headers: dict[str, str],
    traces: list[dict[str, Any]] | None = None,
    *,
    page_size: int = 1,
    unstable_timestamp_ties: bool = False,
) -> tuple[httpx.Response, list[httpx.Request]]:
    requests: list[httpx.Request] = []

    def handle(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        assert request.url.path == "/api/public/traces"
        tags = request.url.params.get_list("tags")
        selected = [trace for trace in traces or [] if set(tags) <= set(trace["tags"])]
        # Langfuse's official v3 API defines fromTimestamp as an inclusive
        # ISO-8601 datetime and tags as requiring every supplied tag. Its fields
        # selector retains metadata in io and timestamp/tags in core, while
        # omitting observations, scores and aggregated metrics when unrequested:
        # https://github.com/langfuse/langfuse/blob/v3.225.5/fern/apis/server/definition/trace.yml
        lower = request.url.params.get("fromTimestamp")
        if lower is not None:
            selected = [
                trace
                for trace in selected
                if datetime.fromisoformat(trace["timestamp"]) >= datetime.fromisoformat(lower)
            ]
        # One trace per page exercises propagation on subsequent HTTP requests.
        page = int(request.url.params["page"])
        if request.url.params.get("orderBy") == "id.asc":
            selected = sorted(selected, key=lambda trace: trace["id"])
        elif unstable_timestamp_ties and page % 2 == 0:
            # Timestamp-only ordering has no tie-breaker: different page queries
            # may enumerate equal-time records in a different physical order.
            selected = list(reversed(selected))
        fields = set(
            request.url.params.get("fields", "core,io,scores,observations,metrics").split(",")
        )
        data = []
        for trace in selected[(page - 1) * page_size : page * page_size]:
            projected = {key: trace[key] for key in ("id", "timestamp", "tags")}
            projected.update(observations=[], scores=[], totalCost=-1, latency=-1)
            if "io" in fields:
                projected["metadata"] = trace["metadata"]
            if "observations" in fields:
                projected["observations"] = trace["observations"]
            if "scores" in fields:
                projected["scores"] = trace["scores"]
            if "metrics" in fields:
                projected["totalCost"] = trace["totalCost"]
            data.append(projected)
        return httpx.Response(
            200,
            json={
                "data": data,
                "meta": {"totalPages": (len(selected) + page_size - 1) // page_size},
            },
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as outbound:
        app = FastAPI()
        app.include_router(router)
        app.state.langfuse = LangfuseClient(
            Settings(langfuse_host="https://langfuse.example.com"), outbound
        )
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as api:
            response = await api.get("/evals/matrix", params=params, headers=headers)
    return response, requests


def test_stream_window_reaches_http_and_keeps_exact_run_and_suite(
    auth_headers: dict[str, str],
) -> None:
    # 1,735,689,600,123 ms is 2025-01-01T00:00:00.123Z. The five-second
    # allowance crosses the UTC date boundary without dropping milliseconds.
    stream_id = "1735689600123-7"
    traces = [
        _trace("historic", "2024-01-01T00:00:00Z", "1704067200000-0"),
        _trace("edge", "2024-12-31T23:59:55.123Z", stream_id),
        _trace("requested", "2025-01-01T00:00:01Z", stream_id),
        _trace("other-run", "2025-01-01T00:00:02Z", "1735689600123-8"),
        _trace("other-suite", "2025-01-01T00:00:03Z", stream_id, suite="acme-other"),
    ]
    response, requests = asyncio.run(
        _matrix({"suite": "acme-suite", "stream_id": stream_id}, auth_headers, traces)
    )

    assert response.status_code == 200, response.text
    assert response.json()["cases"] == ["edge", "requested"]
    assert response.json()["model_summaries"] == [
        {
            "model": "acme-model",
            "passed": 2,
            "total": 2,
            "cost_usd": 2.5,
            "plumbing": 0,
            "completed": 2,
        }
    ]
    for row in response.json()["rows"]:
        assert row["cells"][0]["status"] == "pass"
        assert row["cells"][0]["stream_id"] == stream_id
        assert row["cells"][0]["model"] == "acme-model"
        assert row["cells"][0]["scorer"] == "grader"
    assert len(requests) == 3
    for request in requests:
        assert request.url.params.get_list("tags") == ["eval", "suite:acme-suite"]
        assert request.url.params["fromTimestamp"] == "2024-12-31T23:59:55.123+00:00"
        assert request.url.params.get("fields") == "core,io"


def test_no_stream_preserves_suite_history(auth_headers: dict[str, str]) -> None:
    response, requests = asyncio.run(
        _matrix(
            {"suite": "acme-suite"},
            auth_headers,
            [
                _trace("historic", "2024-01-01T00:00:00Z", "1704067200000-0"),
                _trace("recent", "2025-01-01T00:00:01Z", "1735689600123-0"),
                _trace(
                    "other-suite", "2025-01-01T00:00:02Z", "1735689600123-0", suite="acme-other"
                ),
            ],
        )
    )

    assert response.status_code == 200, response.text
    assert response.json()["cases"] == ["historic", "recent"]
    assert response.json()["model_summaries"] == [
        {
            "model": "acme-model",
            "passed": 2,
            "total": 2,
            "cost_usd": 2.5,
            "plumbing": 0,
            "completed": 2,
        }
    ]
    cells = [row["cells"][0] for row in response.json()["rows"]]
    assert [cell["stream_id"] for cell in cells] == ["1704067200000-0", "1735689600123-0"]
    assert all(cell["status"] == "pass" and cell["scorer"] == "grader" for cell in cells)
    assert len(requests) == 2
    assert all("fromTimestamp" not in request.url.params for request in requests)
    assert all(request.url.params.get("fields") == "core,io" for request in requests)


@pytest.mark.parametrize(
    "stream_id",
    [
        "",
        "invalid",
        "1735689600123",
        "1735689600123-",
        "1735689600123-x",
        "-1-0",
        "1--1",
        "1-0-0",
        "1.5-0",
        "1e3-0",
        " 1-0",
        "1-0\n",
        "١-0",
        "0-0",
        "18446744073709551616-0",
        "1-18446744073709551616",
        "18446744073709551615-0",
    ],
)
def test_malformed_stream_id_is_rejected_before_langfuse(
    stream_id: str,
    auth_headers: dict[str, str],
) -> None:
    response, requests = asyncio.run(
        _matrix({"suite": "acme-suite", "stream_id": stream_id}, auth_headers)
    )

    assert response.status_code == 422, response.text
    assert "stream_id" in response.text
    assert requests == []


def test_early_stream_window_stays_at_epoch(auth_headers: dict[str, str]) -> None:
    response, requests = asyncio.run(
        _matrix({"suite": "acme-suite", "stream_id": "1-18446744073709551615"}, auth_headers)
    )

    assert response.status_code == 200, response.text
    assert requests[0].url.params["fromTimestamp"] == "1970-01-01T00:00:00.000+00:00"


def test_equal_timestamp_pages_preserve_every_case(auth_headers: dict[str, str]) -> None:
    # The pinned API supports a unique id order, unlike timestamp-only ordering:
    # https://github.com/langfuse/langfuse/blob/v3.225.5/fern/apis/server/definition/trace.yml
    # Case outcomes must survive a page boundary even when timestamps tie.
    stream_id = "1735689600123-7"
    traces = [
        _trace(f"case-{index:03d}", "2025-01-01T00:00:01Z", stream_id) for index in range(101)
    ]
    response, requests = asyncio.run(
        _matrix(
            {"suite": "acme-suite", "stream_id": stream_id},
            auth_headers,
            traces,
            page_size=100,
            unstable_timestamp_ties=True,
        )
    )
    assert response.status_code == 200, response.text
    assert response.json()["cases"] == [trace["id"] for trace in traces]
    assert response.json()["model_summaries"][0]["total"] == 101
    assert response.json()["model_summaries"][0]["cost_usd"] == 126.25
    assert len(requests) == 2


def test_matrix_refuses_history_beyond_complete_read_bound(auth_headers: dict[str, str]) -> None:
    # A stable ID order must never turn the 50-page safety cap into an arbitrary
    # partial history that can hide the newest version.
    traces = [
        _trace(f"case-{index:05d}", "2025-01-01T00:00:01Z", "1735689600123-7")
        for index in range(5001)
    ]
    response, requests = asyncio.run(
        _matrix({"suite": "acme-suite"}, auth_headers, traces, page_size=100)
    )
    assert response.status_code == 503, response.text[:200]
    assert "stream_id" in response.json()["detail"]
    assert len(requests) == 1
