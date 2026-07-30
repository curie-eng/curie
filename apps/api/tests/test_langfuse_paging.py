"""LangfuseClient pagination: the per-request page size must never exceed 100.

Langfuse's public list API caps `limit` at 100 and returns 400 for anything
larger. These tests drive the real client against a mock httpx transport that
records every request, so a regression that sends limit=500 (the live 400->500
bug) fails here.
"""

import asyncio

import httpx
from curie_api.config import Settings
from curie_api.langfuse import LangfuseClient


class _RecordingBackend:
    """Serves paginated /api/public/traces and records every request's params."""

    def __init__(self, total: int) -> None:
        self._total = total
        self.requests: list[httpx.QueryParams] = []

    def handle(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request.url.params)
        limit = int(request.url.params.get("limit", "0"))
        page = int(request.url.params.get("page", "1"))
        total_pages = (self._total + limit - 1) // limit if limit else 1
        start = (page - 1) * limit
        data = [
            {"id": str(i), "name": f"curie-run:agent-A-thread-{i}"}
            for i in range(start, min(start + limit, self._total))
        ]
        return httpx.Response(200, json={"data": data, "meta": {"totalPages": total_pages}})


def _client(backend: _RecordingBackend) -> LangfuseClient:
    transport = httpx.MockTransport(backend.handle)
    http = httpx.AsyncClient(transport=transport)
    return LangfuseClient(Settings(), http)


def _limits(backend: _RecordingBackend) -> list[int]:
    return [int(p.get("limit", "0")) for p in backend.requests]


def test_unfiltered_list_never_requests_more_than_100() -> None:
    backend = _RecordingBackend(total=250)
    client = _client(backend)

    traces = asyncio.run(client.list_traces(limit=200))

    assert len(traces) == 200
    assert backend.requests, "expected at least one request"
    assert all(limit <= 100 for limit in _limits(backend))


def test_agent_filtered_scan_never_requests_more_than_100() -> None:
    # The agent-filtered path scans up to _TRACE_SCAN_LIMIT (500) traces; it must
    # page at 100, not ask Langfuse for 500 in one shot (the live 400 bug).
    backend = _RecordingBackend(total=600)
    client = _client(backend)

    asyncio.run(client.list_traces(limit=5, name_contains="agent-A"))

    assert all(limit <= 100 for limit in _limits(backend))
    assert max(_limits(backend)) == 100


def test_scan_stops_at_the_trace_scan_cap() -> None:
    backend = _RecordingBackend(total=10_000)
    client = _client(backend)

    asyncio.run(client.list_traces(limit=5, name_contains="agent-A"))

    # 500-item cap at 100 per page -> at most 5 pages, never the whole 10k.
    assert len(backend.requests) == 5
