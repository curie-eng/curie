"""#3776: ``BindingResolver.close_turn_memory`` tells the API a turn has ended.

The resolver mints the per-turn memory write credential (ADR 0188), so it also
retires it: a POST to the API's internal ``/v1/internal/memory/closed-turns``
with the worker token every other ``/v1/internal`` call uses. It never raises:
a failed close must not fail the turn, since the credential still expires at
the turn's deadline. An older API without the route answers 404; that is
logged once, not as an error.

Runs the real method on a bare resolver (``__new__`` plus ``_config``, as
``test_memory_credential.py`` does) against a local HTTP server.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import socket
import uuid
from collections.abc import AsyncIterator
from typing import Any

import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer
from curie_worker.binding import BindingResolver
from curie_worker.config import WorkerConfig

_AGENT = uuid.UUID("11111111-1111-4111-8111-111111111111")
_WORKER_TOKEN = "test-worker-token-3776"
_PATH = "/v1/internal/memory/closed-turns"


def _resolver(api_url: str) -> BindingResolver:
    resolver = BindingResolver.__new__(BindingResolver)
    resolver._config = WorkerConfig(  # type: ignore[attr-defined]
        api_base_url=api_url, internal_worker_token=_WORKER_TOKEN
    )
    return resolver


@contextlib.asynccontextmanager
async def _api(status: int) -> AsyncIterator[tuple[str, list[dict[str, Any]]]]:
    seen: list[dict[str, Any]] = []

    async def handler(request: web.Request) -> web.Response:
        seen.append(
            {
                "path": request.path,
                "method": request.method,
                "headers": dict(request.headers),
                "body": await request.json(),
            }
        )
        return web.Response(status=status)

    app = web.Application()
    app.router.add_route("*", "/{tail:.*}", handler)
    server = TestServer(app)
    await server.start_server()
    try:
        yield f"http://127.0.0.1:{server.port}", seen
    finally:
        await server.close()


def test_close_posts_the_turn_with_the_worker_token() -> None:
    async def go() -> None:
        async with _api(204) as (url, seen):
            await _resolver(url).close_turn_memory(_AGENT, "evt-1#abc")

        assert len(seen) == 1, seen
        call = seen[0]
        assert (call["method"], call["path"]) == ("POST", _PATH)
        assert call["headers"].get("X-Curie-Worker-Token") == _WORKER_TOKEN
        assert call["body"] == {"agent_id": str(_AGENT), "turn": "evt-1#abc"}
        # The worker token only: never the platform key.
        assert "X-API-Key" not in call["headers"]

    asyncio.run(go())


def test_an_old_api_404_is_logged_once_and_not_raised(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.DEBUG)

    async def go() -> None:
        async with _api(404) as (url, seen):
            resolver = _resolver(url)
            await resolver.close_turn_memory(_AGENT, "evt-1#a")
            await resolver.close_turn_memory(_AGENT, "evt-2#b")
        assert len(seen) == 2

    asyncio.run(go())
    mentions = [r for r in caplog.records if "404" in r.getMessage() or "closed" in r.getMessage()]
    assert len(mentions) == 1, [r.getMessage() for r in caplog.records]
    assert mentions[0].levelno < logging.ERROR


@pytest.mark.parametrize("status", [500, 401])
def test_an_api_error_is_not_raised(status: int, caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.DEBUG)

    async def go() -> None:
        async with _api(status) as (url, seen):
            await _resolver(url).close_turn_memory(_AGENT, "evt-1#a")
        assert len(seen) == 1

    asyncio.run(go())
    assert any(r.levelno >= logging.WARNING for r in caplog.records), caplog.text


def test_an_unreachable_api_is_not_raised(caplog: pytest.LogCaptureFixture) -> None:
    probe = socket.socket()
    probe.bind(("127.0.0.1", 0))
    port = probe.getsockname()[1]
    probe.close()
    caplog.set_level(logging.DEBUG)

    asyncio.run(_resolver(f"http://127.0.0.1:{port}").close_turn_memory(_AGENT, "evt-1#a"))

    assert any(r.levelno >= logging.WARNING for r in caplog.records), caplog.text
