"""Trace proxy when observations exist but the Langfuse trace row does not.

Drives the real runs router and the real LangfuseClient. The upstream is an
httpx.MockTransport, so no compose stack or network is involved.
"""

from collections.abc import Callable, Iterator
from contextlib import contextmanager
from typing import Any

import httpx
from curie_api.config import get_settings
from curie_api.deps import get_langfuse
from curie_api.langfuse import LangfuseClient
from curie_api.routers.runs import router
from fastapi import FastAPI
from fastapi.testclient import TestClient

_TRACE_ID = "abc"
_NOT_READY = "trace has no observations yet"

# Upstream shape is Langfuse's public observations list (data[]) and the public
# trace GET. A 404 on the trace row while observations already exist is the race
# this test freezes.
_OBSERVATIONS: dict[str, Any] = {
    "data": [{"id": "obs-1", "type": "SPAN", "name": "agent.run", "startTime": "1"}],
    "meta": {"totalPages": 1},
}


def _handler(trace_status: int) -> Callable[[httpx.Request], httpx.Response]:
    trace_path = f"/api/public/traces/{_TRACE_ID}"

    def handle(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if (
            request.method == "GET"
            and path.endswith("/api/public/observations")
            and request.url.params.get("traceId") == _TRACE_ID
        ):
            return httpx.Response(200, json=_OBSERVATIONS)
        if request.method == "GET" and path == trace_path:
            return httpx.Response(trace_status, json={"message": "trace missing"})
        return httpx.Response(500, json={"message": "unexpected upstream request"})

    return handle


def _api_key_headers() -> dict[str, str]:
    return {"X-API-Key": get_settings().api_key}


@contextmanager
def _client(trace_status: int) -> Iterator[TestClient]:
    upstream = httpx.AsyncClient(transport=httpx.MockTransport(_handler(trace_status)))
    app = FastAPI()
    app.include_router(router)

    def _langfuse() -> LangfuseClient:
        return LangfuseClient(get_settings(), upstream)

    app.dependency_overrides[get_langfuse] = _langfuse
    with TestClient(app, raise_server_exceptions=False) as client:
        yield client


def test_proxy_maps_upstream_trace_404_to_not_ready() -> None:
    with _client(404) as client:
        response = client.get(f"/langfuse/traces/{_TRACE_ID}", headers=_api_key_headers())
    assert response.status_code == 404
    assert response.json()["detail"] == _NOT_READY


def test_proxy_does_not_hide_upstream_trace_5xx() -> None:
    with _client(503) as client:
        response = client.get(f"/langfuse/traces/{_TRACE_ID}", headers=_api_key_headers())
    assert response.status_code == 500
    assert response.status_code != 404


def test_promote_maps_upstream_trace_404_to_not_ready() -> None:
    with _client(404) as client:
        response = client.post(
            f"/langfuse/traces/{_TRACE_ID}/eval-case", headers=_api_key_headers()
        )
    assert response.status_code == 404
    assert response.json()["detail"] == _NOT_READY
