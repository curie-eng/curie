"""LangfuseClient against the Langfuse v4 read API (``langfuse_api_version = "v4"``).

Langfuse Cloud is v4-only from 2026-11-16, when Langfuse removes the legacy
trace, observation and metrics reads there. The upstream here is an httpx.MockTransport that
serves ONLY the v2 endpoints and 404s everything else, so a call that still
reaches a legacy path fails the test. Request shapes follow Langfuse's published
OpenAPI spec (cursor pagination via ``meta.cursor``, ``fields`` groups, the
structured ``filter`` parameter). No live Langfuse v4 is involved.
Contract: https://cloud.langfuse.com/generated/api/openapi.yml
"""

import asyncio
import json
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any, Literal

import httpx
import pytest
from curie_api.config import Settings, get_settings
from curie_api.deps import get_langfuse
from curie_api.evalcase import trace_to_eval_case
from curie_api.evals import build_matrix
from curie_api.langfuse import (
    LangfuseClient,
    LangfuseReadLimitExceeded,
    LangfuseTraceNotFound,
    build_tree,
    to_v2_metrics_query,
)
from curie_api.routers.runs import router
from fastapi import FastAPI
from fastapi.testclient import TestClient

_OBS = "/api/public/v2/observations"
_METRICS = "/api/public/v2/metrics"


class _V2Upstream:
    """Serves Observations/Metrics API v2 from a fixed observation list.

    Observations are returned newest-first like the real endpoint, in cursor
    pages of the requested ``limit``. Every request is recorded.
    """

    def __init__(
        self, observations: list[dict[str, Any]], metrics_rows: list[dict[str, Any]] | None = None
    ) -> None:
        self.observations = sorted(
            observations, key=lambda o: o.get("startTime") or "", reverse=True
        )
        self.metrics_rows = metrics_rows or []
        self.requests: list[httpx.Request] = []

    def handle(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        params = request.url.params
        if request.url.path == _OBS:
            return self._observations(params)
        if request.url.path == _METRICS:
            return httpx.Response(200, json={"data": self.metrics_rows})
        return httpx.Response(404, json={"message": "removed in v4"})

    def _observations(self, params: httpx.QueryParams) -> httpx.Response:
        rows = self.observations
        if params.get("traceId"):
            rows = [o for o in rows if o["traceId"] == params["traceId"]]
        if params.get("isRootObservation") == "true":
            rows = [o for o in rows if o.get("isRootObservation")]
        if params.get("filter"):
            rows = [o for o in rows if _matches(o, json.loads(params["filter"]))]
        limit = int(params.get("limit", "50"))
        start = int(params.get("cursor", "0"))
        page = rows[start : start + limit]
        meta: dict[str, Any] = {}
        if start + limit < len(rows):
            meta["cursor"] = str(start + limit)
        return httpx.Response(200, json={"data": page, "meta": meta})


def _matches(obs: dict[str, Any], conditions: list[dict[str, Any]]) -> bool:
    for cond in conditions:
        column, operator, value = cond["column"], cond["operator"], cond["value"]
        if column == "isRootObservation" and obs.get("isRootObservation") != value:
            return False
        name = obs.get("traceName") or ""
        if column == "traceName" and operator == "contains" and value not in name:
            return False
        tags = set(obs.get("tags") or [])
        if column == "tags" and operator == "all of" and not set(value) <= tags:
            return False
        if column == "startTime" and operator == ">=" and (obs.get("startTime") or "") < value:
            return False
    return True


def _root(trace_id: str, start: str, **extra: Any) -> dict[str, Any]:
    return {
        "id": f"root-{trace_id}-{start}",
        "traceId": trace_id,
        "type": "SPAN",
        "name": "agent.run",
        "traceName": f"curie-run:agent-A-thread-{trace_id}",
        "startTime": start,
        "parentObservationId": None,
        "isRootObservation": True,
        "tags": [],
        **extra,
    }


def _client(upstream: _V2Upstream, version: Literal["v3", "v4"] = "v4") -> LangfuseClient:
    http = httpx.AsyncClient(transport=httpx.MockTransport(upstream.handle))
    return LangfuseClient(Settings(langfuse_api_version=version), http)


def _filters(request: httpx.Request) -> list[dict[str, Any]]:
    filters: list[dict[str, Any]] = json.loads(request.url.params["filter"])
    return filters


def test_default_api_version_is_v3() -> None:
    assert Settings().langfuse_api_version == "v3"


def test_list_traces_reads_root_observations_as_trace_dicts() -> None:
    upstream = _V2Upstream(
        [
            _root(
                "t1",
                "2026-11-17T10:00:00.000Z",
                input='[{"role": "user", "content": "hi"}]',
                output="plain text",
                metadata={"case_id": "c1"},
                latency=1.5,
                environment="prod",
            ),
            {**_root("t1", "2026-11-17T10:00:01.000Z"), "id": "child", "isRootObservation": False},
        ]
    )

    traces = asyncio.run(_client(upstream).list_traces(limit=20))

    assert [request.url.path for request in upstream.requests] == [_OBS]
    assert _filters(upstream.requests[0]) == [
        {"type": "boolean", "column": "isRootObservation", "operator": "=", "value": True}
    ]
    assert len(traces) == 1
    trace = traces[0]
    assert trace["id"] == "t1"
    assert trace["name"] == "curie-run:agent-A-thread-t1"
    assert trace["timestamp"] == "2026-11-17T10:00:00.000Z"
    assert trace["input"] == [{"role": "user", "content": "hi"}]
    assert trace["output"] == "plain text"
    assert trace["metadata"] == {"case_id": "c1"}
    assert trace["latency"] == 1.5
    assert trace["environment"] == "prod"


def test_list_traces_filters_by_agent_token_on_the_server() -> None:
    upstream = _V2Upstream(
        [
            _root("a", "2026-11-17T10:00:00.000Z", traceName="curie-run:agent-A-thread-1"),
            _root("b", "2026-11-17T10:00:01.000Z", traceName="curie-run:agent-B-thread-1"),
        ]
    )

    traces = asyncio.run(_client(upstream).list_traces(limit=5, name_contains="agent-A"))

    assert [t["id"] for t in traces] == ["a"]
    assert {
        "type": "string",
        "column": "traceName",
        "operator": "contains",
        "value": "agent-A",
    } in _filters(upstream.requests[0])


def test_list_traces_pages_by_cursor_never_over_100_and_stops_at_limit() -> None:
    upstream = _V2Upstream(
        [_root(f"t{i:04d}", f"2026-11-17T10:{i // 60:02d}:{i % 60:02d}.000Z") for i in range(450)]
    )

    traces = asyncio.run(_client(upstream).list_traces(limit=250))

    assert len(traces) == 250
    limits = [int(r.url.params["limit"]) for r in upstream.requests]
    assert max(limits) == 100
    # 250 distinct traces need three 100-row pages, not the whole 450.
    assert len(upstream.requests) == 3
    # Newest first: the 250 newest start times.
    assert traces[0]["id"] == "t0449"


def test_trace_with_several_roots_is_one_row_with_the_earliest_root() -> None:
    upstream = _V2Upstream(
        [
            _root("t1", "2026-11-17T10:00:05.000Z", output="late"),
            _root("t1", "2026-11-17T10:00:00.000Z", output="first"),
        ]
    )

    traces = asyncio.run(_client(upstream).list_traces(limit=10))

    assert len(traces) == 1
    assert traces[0]["output"] == "first"


def test_get_trace_returns_the_trace_dict_or_raises_not_found() -> None:
    upstream = _V2Upstream([_root("t1", "2026-11-17T10:00:00.000Z", sessionId="s1")])
    client = _client(upstream)

    trace = asyncio.run(client.get_trace("t1"))
    assert trace["id"] == "t1"
    assert trace["sessionId"] == "s1"
    request = upstream.requests[0]
    assert request.url.path == _OBS
    assert request.url.params["traceId"] == "t1"
    assert request.url.params["isRootObservation"] == "true"
    assert "trace_context" in request.url.params["fields"]

    with pytest.raises(LangfuseTraceNotFound):
        asyncio.run(client.get_trace("missing"))


def test_get_observations_requests_field_groups_and_decodes_io() -> None:
    children = [
        {
            "id": f"o{i}",
            "traceId": "t1",
            "type": "GENERATION",
            "name": "llm",
            "startTime": f"2026-11-17T10:00:{i % 60:02d}.{i // 60:03d}Z",
            "parentObservationId": "root",
            "isRootObservation": False,
            "model": "m",
            "usageDetails": {"input": 3},
            "input": json.dumps({"q": i}),
            "output": "done",
        }
        for i in range(150)
    ]
    root = {**_root("t1", "2026-11-17T09:59:00.000Z"), "id": "root"}
    upstream = _V2Upstream([root, *children])

    observations = asyncio.run(_client(upstream).get_observations("t1"))

    # More than one page of 100: the v3 read silently stopped at the first.
    assert len(observations) == 151
    assert len(upstream.requests) == 2
    first = upstream.requests[0].url.params
    assert first["traceId"] == "t1"
    for group in ("io", "metadata", "model", "usage"):
        assert group in first["fields"].split(",")
    assert "curie.sandbox_id" in first["expandMetadata"]
    by_id = {o["id"]: o for o in observations}
    assert by_id["o7"]["input"] == {"q": 7}
    assert by_id["o7"]["output"] == "done"
    tree = build_tree(observations)
    assert [node.id for node in tree] == ["root"]
    assert len(tree[0].children) == 150


def test_list_traces_by_tags_reads_every_page_with_server_side_tags() -> None:
    upstream = _V2Upstream(
        [
            _root(
                f"e{i}",
                f"2026-11-17T10:{i // 60:02d}:{i % 60:02d}.000Z",
                tags=["eval", "suite:s1", "version:v1"],
                metadata={"case_id": f"case-{i}", "outcome": "pass"},
            )
            for i in range(230)
        ]
        + [_root("other", "2026-11-17T09:00:00.000Z", tags=["eval", "suite:s2"])]
    )

    traces = asyncio.run(
        _client(upstream).list_traces_by_tags(
            ["eval", "suite:s1"], fields="core,io", order_by="id.asc", require_complete=True
        )
    )

    assert len(traces) == 230
    assert {t["id"] for t in traces} == {f"e{i}" for i in range(230)}
    # v3-only parameters are not sent to v2.
    first = upstream.requests[0].url.params
    assert "orderBy" not in first
    tag_filter = {
        "type": "arrayOptions",
        "column": "tags",
        "operator": "all of",
        "value": ["eval", "suite:s1"],
    }
    assert tag_filter in _filters(upstream.requests[0])
    # What the matrix reads (tags, metadata, timestamp) survives the round trip.
    matrix = build_matrix(traces, "s1", limit_versions=5)
    assert matrix.versions == ["v1"]
    assert len(matrix.cases) == 230
    assert all(cell.status == "pass" for row in matrix.rows for cell in row.cells)


def test_list_traces_by_tags_applies_from_timestamp() -> None:
    from datetime import UTC, datetime

    upstream = _V2Upstream(
        [
            _root("old", "2026-11-17T09:00:00.000Z", tags=["eval"]),
            _root("new", "2026-11-17T11:00:00.000Z", tags=["eval"]),
        ]
    )

    traces = asyncio.run(
        _client(upstream).list_traces_by_tags(
            ["eval"], from_timestamp=datetime(2026, 11, 17, 10, 0, tzinfo=UTC)
        )
    )

    assert [t["id"] for t in traces] == ["new"]


def test_complete_read_raises_when_the_page_bound_is_exceeded() -> None:
    class _Endless(_V2Upstream):
        def _observations(self, params: httpx.QueryParams) -> httpx.Response:
            row = _root(f"t{len(self.requests)}", "2026-11-17T10:00:00.000Z", tags=["eval"])
            return httpx.Response(200, json={"data": [row], "meta": {"cursor": "more"}})

    with pytest.raises(LangfuseReadLimitExceeded):
        asyncio.run(
            _client(_Endless([])).list_traces_by_tags(["eval"], require_complete=True)
        )


def test_trace_view_metrics_queries_become_root_observation_queries() -> None:
    legacy: dict[str, Any] = {
        "view": "traces",
        "metrics": [{"measure": "latency", "aggregation": "p95"}],
        "filters": [
            {"column": "environment", "operator": "=", "value": "prod", "type": "string"},
            {"column": "name", "operator": "contains", "value": "curie-run:", "type": "string"},
        ],
        "fromTimestamp": "2026-11-17T00:00:00+00:00",
        "toTimestamp": "2026-11-18T00:00:00+00:00",
    }

    rewritten = to_v2_metrics_query(legacy)

    assert rewritten["view"] == "observations"
    assert rewritten["metrics"] == legacy["metrics"]
    assert [f["column"] for f in rewritten["filters"]] == [
        "environment",
        "traceName",
        "isRootObservation",
    ]
    assert rewritten["filters"][-1] == {
        "type": "boolean",
        "column": "isRootObservation",
        "operator": "=",
        "value": True,
    }
    # The caller's query object is not mutated.
    assert legacy["view"] == "traces"
    assert legacy["filters"][1]["column"] == "name"


def test_observation_view_metrics_queries_pass_through() -> None:
    query = {"view": "observations", "metrics": [{"measure": "totalCost", "aggregation": "sum"}]}
    assert to_v2_metrics_query(query) == query


def test_query_metrics_calls_metrics_v2() -> None:
    upstream = _V2Upstream([], metrics_rows=[{"count_count": "7"}])
    query = {
        "view": "traces",
        "metrics": [{"measure": "count", "aggregation": "count"}],
        "filters": [],
    }

    rows = asyncio.run(_client(upstream).query_metrics(query))

    assert rows == [{"count_count": "7"}]
    assert [r.url.path for r in upstream.requests] == [_METRICS]
    sent = json.loads(upstream.requests[0].url.params["query"])
    assert sent["view"] == "observations"


def test_v3_mode_still_calls_legacy_endpoints() -> None:
    seen: list[str] = []

    def handle(request: httpx.Request) -> httpx.Response:
        seen.append(request.url.path)
        return httpx.Response(200, json={"data": [], "meta": {"totalPages": 1}})

    http = httpx.AsyncClient(transport=httpx.MockTransport(handle))
    client = LangfuseClient(Settings(), http)

    asyncio.run(client.list_traces(limit=5))
    asyncio.run(client.get_observations("t1"))
    asyncio.run(client.query_metrics({"view": "traces"}))

    assert seen == ["/api/public/traces", "/api/public/observations", "/api/public/metrics"]


def test_trace_to_eval_case_accepts_a_v4_trace_and_observations() -> None:
    upstream = _V2Upstream(
        [
            _root(
                "t1",
                "2026-11-17T10:00:00.000Z",
                input=json.dumps([{"role": "user", "content": "refund order 1"}]),
                output="Refunded.",
            )
        ]
    )
    client = _client(upstream)

    trace = asyncio.run(client.get_trace("t1"))
    observations = asyncio.run(client.get_observations("t1"))
    case = trace_to_eval_case("t1", trace, observations)

    assert "refund order 1" in json.dumps(case.model_dump())


@contextmanager
def _runs_client(upstream: _V2Upstream) -> Iterator[TestClient]:
    http = httpx.AsyncClient(transport=httpx.MockTransport(upstream.handle))
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_langfuse] = lambda: LangfuseClient(
        Settings(langfuse_api_version="v4"), http
    )
    with TestClient(app, raise_server_exceptions=False) as client:
        yield client


def _headers() -> dict[str, str]:
    return {"X-API-Key": get_settings().api_key}


def test_runs_router_serves_a_v4_trace_tree() -> None:
    root = {**_root("abc", "2026-11-17T10:00:00.000Z"), "id": "root"}
    child = {
        "id": "kid",
        "traceId": "abc",
        "type": "GENERATION",
        "name": "llm",
        "startTime": "2026-11-17T10:00:01.000Z",
        "parentObservationId": "root",
        "isRootObservation": False,
    }
    with _runs_client(_V2Upstream([root, child])) as client:
        response = client.get("/langfuse/traces/abc", headers=_headers())

    assert response.status_code == 200
    body = response.json()
    assert body["trace"]["id"] == "abc"
    assert [node["id"] for node in body["tree"]] == ["root"]
    assert [node["id"] for node in body["tree"][0]["children"]] == ["kid"]


def test_runs_router_maps_a_missing_v4_trace_to_not_ready() -> None:
    with _runs_client(_V2Upstream([])) as client:
        response = client.get("/langfuse/traces/abc", headers=_headers())

    assert response.status_code == 404
    assert response.json()["detail"] == "trace has no observations yet"


def test_runs_router_maps_root_not_yet_readable_to_not_ready() -> None:
    # Observations landed but no root observation is readable yet: the v4
    # equivalent of "observations exist, trace row 404s".
    orphan = {
        "id": "kid",
        "traceId": "abc",
        "type": "SPAN",
        "name": "x",
        "startTime": "2026-11-17T10:00:01.000Z",
        "parentObservationId": "root",
        "isRootObservation": False,
    }
    with _runs_client(_V2Upstream([orphan])) as client:
        response = client.get("/langfuse/traces/abc", headers=_headers())

    assert response.status_code == 404
    assert response.json()["detail"] == "trace has no observations yet"



class _UnboundedRoots(_V2Upstream):
    def _observations(self, params: httpx.QueryParams) -> httpx.Response:
        row = _root("abc", "2026-11-17T10:00:00.000Z")
        return httpx.Response(200, json={"data": [row], "meta": {"cursor": "next"}})


@pytest.mark.parametrize("method", ["get_trace", "get_observations"])
def test_resource_reads_refuse_to_return_a_truncated_trace(method: str) -> None:
    upstream = _UnboundedRoots([])
    client = _client(upstream)
    with pytest.raises(LangfuseReadLimitExceeded):
        asyncio.run(getattr(client, method)("abc"))


@pytest.mark.parametrize("method,suffix", [("get", ""), ("post", "/eval-case")])
def test_runs_router_reports_incomplete_observations_as_upstream_failure(
    method: str, suffix: str
) -> None:
    with _runs_client(_UnboundedRoots([])) as client:
        response = getattr(client, method)(f"/langfuse/traces/abc{suffix}", headers=_headers())
    assert response.status_code == 502
    assert response.json()["detail"] == "trace exceeds read limit"
