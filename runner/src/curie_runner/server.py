"""aiohttp server exposing the ACI channel over HTTP.

Productizes the prototype's aiohttp ``/run`` into the ACI session channel:

- ``GET  /healthz``      liveness (always ok once the process is up)
- ``GET  /status``       session status: done / idle-awaiting-input /
                         classified-failure, plus readiness and turn state
- ``POST /v1/event``     open a turn: body is an ACI ``event`` frame; the
                         response streams outbound NDJSON, ending in a final
- ``POST /v1/steer``     inject a follow-up ACI ``event`` frame into the live
                         turn (same frame type as ``/v1/event``); 409 when no turn
                         is active (the finish-race boundary F1 owns), so the
                         caller falls back to a fresh ``/v1/event``
- ``POST /v1/interrupt`` hard-stop the live turn: body is an ACI ``interrupt``
                         frame; the open turn's final is reclassified to idle
- ``POST /v1/reset``     discard the conversation and start a fresh model
                         session (eval isolation, #550); 409 while a turn is
                         active. Not an ACI wire frame -- a runner control route,
                         like /status and /healthz, so it takes no body

One turn consumes the SDK generator at a time (enforced by the runner's turn
lock); steer and interrupt are side-channel injections whose output surfaces on
the open ``/v1/event`` stream, exactly as the PT-2 steering proof showed.
"""

from __future__ import annotations

import contextlib
import hmac
import inspect
from collections.abc import Awaitable, Callable
from typing import cast

from aci_protocol import Event, Interrupt, parse_inbound
from aiohttp import web
from aiohttp.typedefs import Handler, Middleware
from curie_telemetry import TRACEPARENT_STREAM_FIELD, extract_trace_context

from .session import SessionRunner
from .workspace_snapshot import WorkspaceSnapshot, WorkspaceSnapshotError

_NDJSON = "application/x-ndjson"

# The three ACI POST routes that drive a turn; gated when a token is configured.
# /healthz and /status stay open so the chart readinessProbe (no auth header)
# keeps working.
_GATED_PATHS = frozenset({"/v1/event", "/v1/steer", "/v1/interrupt", "/v1/reset", "/v1/snapshot"})

# Typed app key so aiohttp resolves the runner without the string-key warning.
RUNNER: web.AppKey[SessionRunner] = web.AppKey("runner", SessionRunner)
Snapshotter = Callable[[], WorkspaceSnapshot | Awaitable[WorkspaceSnapshot]]
SNAPSHOTTER: web.AppKey[object] = web.AppKey("snapshotter", object)


def _auth_middleware(token: str) -> Middleware:
    """Require ``Authorization: Bearer <token>`` on the gated ACI POST routes.

    Runs before body parsing so an authenticated call keeps the route's existing
    400/409 semantics unchanged. The presented token is compared with the
    configured one via ``hmac.compare_digest`` (no timing oracle).
    """

    # The configured token is invariant for the process, so encode it once here
    # rather than on every gated request.
    token_bytes = token.encode("utf-8")

    @web.middleware
    async def middleware(request: web.Request, handler: Handler) -> web.StreamResponse:
        if request.method == "POST" and request.path in _GATED_PATHS:
            header = request.headers.get("Authorization", "")
            scheme = "Bearer "
            if not header.startswith(scheme):
                return web.json_response({"error": "missing bearer token"}, status=401)
            presented = header[len(scheme) :]
            # Compare UTF-8 bytes: hmac.compare_digest raises TypeError on a
            # non-ASCII str, which aiohttp would surface as a 500 instead of a
            # 401. Bytes keep a crafted non-ASCII token a clean 401.
            if not hmac.compare_digest(presented.encode("utf-8"), token_bytes):
                return web.json_response({"error": "invalid token"}, status=401)
        return await handler(request)

    return middleware


def create_app(
    runner: SessionRunner,
    token: str | None = None,
    snapshotter: Snapshotter | None = None,
) -> web.Application:
    """Build the aiohttp application bound to a started SessionRunner.

    When ``token`` is set, the three ACI POST routes require a matching bearer
    token; when it is ``None`` the app is a pass-through (CLI, fake-model CI, and
    pre-token sandboxes stay unauthenticated).
    """

    # A falsy token (None or empty string) means no enforcement: an empty token
    # would make ``Bearer `` with an empty value compare-equal, so treat it as
    # pass-through rather than an unusable enforce-on state.
    middlewares = [_auth_middleware(token)] if token else []
    app = web.Application(middlewares=middlewares)
    app[RUNNER] = runner
    app[SNAPSHOTTER] = snapshotter
    app.add_routes(
        [
            web.get("/healthz", _healthz),
            web.get("/status", _status),
            web.post("/v1/event", _event),
            web.post("/v1/steer", _steer),
            web.post("/v1/interrupt", _interrupt),
            web.post("/v1/reset", _reset),
            web.post("/v1/snapshot", _snapshot),
        ]
    )
    app.on_cleanup.append(_on_cleanup)
    return app


async def _on_cleanup(app: web.Application) -> None:
    await app[RUNNER].close()


async def _healthz(_request: web.Request) -> web.Response:
    return web.json_response({"ok": True})


async def _status(request: web.Request) -> web.Response:
    runner: SessionRunner = request.app[RUNNER]
    return web.json_response(
        {
            "status": runner.status.value,
            "ready": runner.ready,
            "turn_active": runner.turn_active,
        }
    )


async def _snapshot(request: web.Request) -> web.Response:
    """Capture the managed checkout for the bearer-authenticated worker."""

    snapshotter = cast("Snapshotter | None", request.app[SNAPSHOTTER])
    if snapshotter is None:
        return web.json_response(
            {
                "error": (
                    "this session has no managed repository workspace; deploy with "
                    "workspace support before requesting publication"
                )
            },
            status=409,
        )
    try:
        result = snapshotter()
        captured = await result if inspect.isawaitable(result) else result
    except WorkspaceSnapshotError as exc:
        return web.json_response({"error": str(exc)}, status=422)
    return web.json_response(captured.to_json())


def _parse(body: object) -> Event | Interrupt:
    # parse_inbound validates against the frozen InboundMessage union; the
    # runtime type is always Event | Interrupt though the signature is Any.
    return cast("Event | Interrupt", parse_inbound(cast("dict[str, object]", body)))


async def _event(request: web.Request) -> web.StreamResponse:
    runner: SessionRunner = request.app[RUNNER]
    try:
        frame = _parse(await request.json())
    except Exception as exc:  # noqa: BLE001 - map any decode/validation error to 400
        return web.json_response({"error": f"invalid event frame: {exc}"}, status=400)
    if not isinstance(frame, Event):
        return web.json_response(
            {"error": "expected an event frame; use /v1/interrupt for interrupts"},
            status=400,
        )

    response = web.StreamResponse(status=200, headers={"Content-Type": _NDJSON})
    await response.prepare(request)
    # aclosing guarantees the generator is finalized on THIS driving task. On a
    # client disconnect, response.write() raises from this frame; without
    # aclosing the suspended generator would instead be closed later by the
    # asyncgen GC on a different task, releasing the turn lock cross-task (see
    # SessionRunner._turn_lock). aclosing keeps the teardown -- and the turn
    # interrupt in run_turn's finally -- on the task that opened it.
    carrier: dict[str, str] = {}
    traceparent = request.headers.get(TRACEPARENT_STREAM_FIELD)
    if traceparent is not None:
        carrier[TRACEPARENT_STREAM_FIELD] = traceparent
    parent = extract_trace_context(carrier)
    async with contextlib.aclosing(runner.run_turn(frame, parent=parent)) as stream:
        async for line in stream:
            await response.write(line.encode("utf-8"))
    await response.write_eof()
    return response


async def _steer(request: web.Request) -> web.Response:
    runner: SessionRunner = request.app[RUNNER]
    try:
        frame = _parse(await request.json())
    except Exception as exc:  # noqa: BLE001
        return web.json_response({"error": f"invalid steer frame: {exc}"}, status=400)
    if not isinstance(frame, Event):
        return web.json_response({"error": "expected an event frame"}, status=400)

    delivered = await runner.steer(frame.text)
    if not delivered:
        return web.json_response(
            {"error": "no active turn to steer; open a new /v1/event"}, status=409
        )
    return web.json_response({"ok": True})


async def _interrupt(request: web.Request) -> web.Response:
    runner: SessionRunner = request.app[RUNNER]
    try:
        frame = _parse(await request.json())
    except Exception as exc:  # noqa: BLE001
        return web.json_response({"error": f"invalid interrupt frame: {exc}"}, status=400)
    if not isinstance(frame, Interrupt):
        return web.json_response({"error": "expected an interrupt frame"}, status=400)

    await runner.interrupt(frame.reason)
    return web.json_response({"ok": True})


async def _reset(request: web.Request) -> web.Response:
    runner: SessionRunner = request.app[RUNNER]
    # Refuse to reset a session mid-turn: tearing the SDK session down under a
    # live turn would strand the open /v1/event stream. 409 mirrors the steer
    # finish-race boundary -- the caller resets once the turn has completed.
    if runner.turn_active:
        return web.json_response({"error": "cannot reset while a turn is active"}, status=409)
    await runner.reset()
    return web.json_response({"ok": True})
