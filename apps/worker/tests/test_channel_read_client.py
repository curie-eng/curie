"""ADR 0100 (#2877): ``BindingResolver.channel_read_context`` mints a turn's capability.

The worker is the sole issuer of the ``chr`` channel read capability. It asks
the API's internal ``/v1/internal/channel-read/context`` route with the worker
token every other ``/v1/internal`` call uses: ``mode="open"`` at turn open with
the attempt's owner, ``mode="steer"`` to renew the live logical turn with no
owner. A 409 ``channel_read.grant_absent`` raises ``ChannelReadGrantAbsent`` so
the kernel can cache it; any other failure returns None and the turn runs
without a capability. An older API without the route answers 404, which is
logged once. The token never reaches a log line.

Runs the real method on a bare resolver (``__new__`` plus ``_config``, as
``binding/test_memory_turn_close_client.py`` does) against a local HTTP server
playing the API.
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
_DEPLOYMENT = uuid.UUID("22222222-2222-4222-8222-222222222222")
_WORKER_TOKEN = "test-worker-token-2877"
_PATH = "/v1/internal/channel-read/context"
_OWNER = "0123456789abcdef0123456789abcdef"
_TOKEN = "chr.eyJ0dXJuIjoiYWJjIn0.SECRET-SIGNATURE-2877-do-not-log"
_TURN_KEY = "5f1c0de5f1c0de5f1c0de5f1c0de5f1c"


def _resolver(api_url: str) -> BindingResolver:
    resolver = BindingResolver.__new__(BindingResolver)
    resolver._config = WorkerConfig(  # type: ignore[attr-defined]
        api_base_url=api_url, internal_worker_token=_WORKER_TOKEN
    )
    return resolver


def _ok_body(generation: int = 1) -> dict[str, Any]:
    return {
        "token": _TOKEN,
        "generation": generation,
        "expires_at": 1_900_000_000,
        "turn_key": _TURN_KEY,
    }


@contextlib.asynccontextmanager
async def _api(
    status: int, body: object | None = None
) -> AsyncIterator[tuple[str, list[dict[str, Any]]]]:
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
        if body is None:
            return web.Response(status=status)
        return web.json_response(body, status=status, headers={"Cache-Control": "no-store"})

    app = web.Application()
    app.router.add_route("*", "/{tail:.*}", handler)
    server = TestServer(app)
    await server.start_server()
    try:
        yield f"http://127.0.0.1:{server.port}", seen
    finally:
        await server.close()


async def _open(resolver: BindingResolver, **overrides: Any) -> Any:
    kwargs: dict[str, Any] = {
        "agent_id": _AGENT,
        "deployment_id": _DEPLOYMENT,
        "event_id": "evt-2877-1",
        "mode": "open",
        "owner": _OWNER,
        "default": ("slack", "C0EXAMPLE1"),
        "ttl_s": 120,
    }
    kwargs.update(overrides)
    return await resolver.channel_read_context(**kwargs)  # type: ignore[attr-defined]


def _refusal(code: str) -> dict[str, Any]:
    return {"detail": {"code": code, "message": "refused"}}


def test_open_posts_the_context_with_the_worker_token() -> None:
    async def go() -> None:
        async with _api(200, _ok_body()) as (url, seen):
            mint = await _open(_resolver(url))

        assert len(seen) == 1, seen
        call = seen[0]
        assert (call["method"], call["path"]) == ("POST", _PATH)
        assert call["headers"].get("X-Curie-Worker-Token") == _WORKER_TOKEN
        # The worker token only: never the platform key.
        assert "X-API-Key" not in call["headers"]
        assert call["body"] == {
            "agent_id": str(_AGENT),
            "deployment_id": str(_DEPLOYMENT),
            "event_id": "evt-2877-1",
            "mode": "open",
            "owner": _OWNER,
            "default_channel": {"kind": "slack", "address": "C0EXAMPLE1"},
            "ttl_s": 120,
        }
        assert mint is not None
        assert mint.token == _TOKEN
        assert mint.generation == 1
        assert mint.expires_at == 1_900_000_000
        assert mint.turn_key == _TURN_KEY

    asyncio.run(go())


def test_steer_sends_no_owner_and_a_null_default() -> None:
    # A steer keeps the opener's owner server side; the route refuses one
    # sent with it. A targetless turn's default is null.
    async def go() -> None:
        async with _api(200, _ok_body(generation=2)) as (url, seen):
            mint = await _open(_resolver(url), mode="steer", owner=None, default=None)

        assert len(seen) == 1, seen
        body = seen[0]["body"]
        assert body["mode"] == "steer"
        assert "owner" not in body
        assert body["default_channel"] is None
        assert mint is not None and mint.generation == 2

    asyncio.run(go())


def test_grant_absent_raises() -> None:
    from curie_worker.binding import ChannelReadGrantAbsent

    async def go() -> None:
        async with _api(409, _refusal("channel_read.grant_absent")) as (url, seen):
            with pytest.raises(ChannelReadGrantAbsent):
                await _open(_resolver(url))
        assert len(seen) == 1

    asyncio.run(go())


@pytest.mark.parametrize(
    "code",
    [
        "channel_read.turn_inactive",
        "channel_read.turn_expired",
        "channel_read.deployment_inactive",
        "channel_read.turn_unresolvable",
    ],
)
def test_other_refusals_return_none(code: str) -> None:
    async def go() -> None:
        async with _api(409, _refusal(code)) as (url, seen):
            assert await _open(_resolver(url)) is None
        assert len(seen) == 1

    asyncio.run(go())


@pytest.mark.parametrize("status", [500, 503, 401])
def test_an_api_error_returns_none_and_warns(status: int, caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.DEBUG)

    async def go() -> None:
        async with _api(status, {"detail": "nope"}) as (url, seen):
            assert await _open(_resolver(url)) is None
        assert len(seen) == 1

    asyncio.run(go())
    worker = [r for r in caplog.records if r.name.startswith("curie_worker")]
    assert any(r.levelno >= logging.WARNING for r in worker), caplog.text


def test_a_malformed_success_body_returns_none() -> None:
    async def go() -> None:
        async with _api(200, {"generation": 1}) as (url, _seen):
            assert await _open(_resolver(url)) is None

    asyncio.run(go())


def test_an_old_api_404_is_logged_once_and_returns_none(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG)

    async def go() -> None:
        async with _api(404) as (url, seen):
            resolver = _resolver(url)
            assert await _open(resolver, event_id="evt-a") is None
            assert await _open(resolver, event_id="evt-b") is None
        assert len(seen) == 2

    asyncio.run(go())
    # Only the worker's own lines: the test server's access log also says 404.
    worker = [r for r in caplog.records if r.name.startswith("curie_worker")]
    mentions = [r for r in worker if "404" in r.getMessage() or "channel" in r.getMessage()]
    assert len(mentions) == 1, [(r.name, r.getMessage()) for r in caplog.records]
    assert mentions[0].levelno < logging.ERROR


def test_an_unreachable_api_returns_none(caplog: pytest.LogCaptureFixture) -> None:
    probe = socket.socket()
    probe.bind(("127.0.0.1", 0))
    port = probe.getsockname()[1]
    probe.close()
    caplog.set_level(logging.DEBUG)

    assert asyncio.run(_open(_resolver(f"http://127.0.0.1:{port}"))) is None

    assert any(r.levelno >= logging.WARNING for r in caplog.records), caplog.text


def test_the_token_never_reaches_a_log_line(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.DEBUG)

    async def go() -> None:
        async with _api(200, _ok_body()) as (url, _seen):
            resolver = _resolver(url)
            assert await _open(resolver) is not None
        # A refusal body that echoes the token is not logged either.
        echoed = {"detail": {"code": "channel_read.unavailable", "message": _TOKEN}}
        async with _api(503, echoed) as (url, _seen):
            assert await _open(_resolver(url)) is None

    asyncio.run(go())
    assert caplog.records, "nothing was logged"
    assert "SECRET-SIGNATURE" not in caplog.text
    for record in caplog.records:
        assert "SECRET-SIGNATURE" not in record.getMessage(), record.name
