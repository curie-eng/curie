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
- ``POST /v1/timeout``   mark the exact open turn as timed out and stop it; the
                         opaque epoch is carried only in a response/request header
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
import secrets
from collections.abc import Awaitable, Callable, Mapping
from types import MappingProxyType
from typing import TypedDict, cast

from aci_protocol import Event, Interrupt, parse_inbound
from aiohttp import web
from aiohttp.typedefs import Handler, Middleware
from curie_telemetry import TRACEPARENT_STREAM_FIELD, extract_trace_context

from .session import SessionRunner
from .workspace_snapshot import WorkspaceSnapshot, WorkspaceSnapshotError

_NDJSON = "application/x-ndjson"
_TURN_EPOCH_HEADER = "X-Curie-Turn-Epoch"
_TURN_EPOCH_MIN_LENGTH = 32
_TURN_EPOCH_MAX_LENGTH = 256

# Authenticated control routes. /healthz and the probe-oriented /status stay
# open so chart probes keep working; the worker reads replacement authority
# from /v1/status with the per-claim bearer token.
_GATED_PATHS = frozenset(
    {
        "/v1/event",
        "/v1/steer",
        "/v1/interrupt",
        "/v1/timeout",
        "/v1/reset",
        "/v1/snapshot",
        "/v1/status",
    }
)

# Typed app key so aiohttp resolves the runner without the string-key warning.
RUNNER: web.AppKey[SessionRunner] = web.AppKey("runner", SessionRunner)
Snapshotter = Callable[[], WorkspaceSnapshot | Awaitable[WorkspaceSnapshot]]
SNAPSHOTTER: web.AppKey[object] = web.AppKey("snapshotter", object)
STATUS_ATTESTATION: web.AppKey[object] = web.AppKey("status_attestation", object)


class _StatusAttestation(TypedDict):
    session_id: str
    sandbox_id: str
    managed_workspace: bool
    cwd: str | None


_STATUS_ATTESTATION_ATTR = "_curie_control_status_attestation"


def bind_status_attestation(
    runner: SessionRunner,
    *,
    session_id: str,
    sandbox_id: str,
    cwd: str | None,
) -> SessionRunner:
    """Bind credential-free boot facts for the authenticated worker status."""

    attestation: _StatusAttestation = {
        "session_id": session_id,
        "sandbox_id": sandbox_id,
        "managed_workspace": cwd is not None,
        "cwd": cwd,
    }
    setattr(runner, _STATUS_ATTESTATION_ATTR, MappingProxyType(attestation))
    return runner


def _bound_status_attestation(runner: SessionRunner) -> Mapping[str, object] | None:
    value = getattr(runner, _STATUS_ATTESTATION_ATTR, None)
    return cast("Mapping[str, object]", value) if isinstance(value, Mapping) else None


def _auth_middleware(token: str) -> Middleware:
    """Require ``Authorization: Bearer <token>`` on the gated control routes.

    Runs before body parsing so an authenticated call keeps the route's existing
    400/409 semantics unchanged. The presented token is compared with the
    configured one via ``hmac.compare_digest`` (no timing oracle).
    """

    # The configured token is invariant for the process, so encode it once here
    # rather than on every gated request.
    token_bytes = token.encode("utf-8")

    @web.middleware
    async def middleware(request: web.Request, handler: Handler) -> web.StreamResponse:
        if request.path in _GATED_PATHS:
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

    When ``token`` is set, the runner control routes require a matching bearer
    token; when it is ``None`` the app is a pass-through (CLI, fake-model CI,
    and pre-token sandboxes stay unauthenticated).
    """

    # A falsy token (None or empty string) means no enforcement: an empty token
    # would make ``Bearer `` with an empty value compare-equal, so treat it as
    # pass-through rather than an unusable enforce-on state.
    middlewares = [_auth_middleware(token)] if token else []
    app = web.Application(middlewares=middlewares)
    app[RUNNER] = runner
    app[SNAPSHOTTER] = snapshotter
    # An identity-bearing response exists only when middleware above enforces a
    # non-empty bearer. Legacy/tokenless apps keep both status routes probe-only.
    app[STATUS_ATTESTATION] = _bound_status_attestation(runner) if token else None
    app.add_routes(
        [
            web.get("/healthz", _healthz),
            web.get("/status", _status),
            web.get("/v1/status", _status),
            web.post("/v1/event", _event),
            web.post("/v1/steer", _steer),
            web.post("/v1/interrupt", _interrupt),
            web.post("/v1/timeout", _timeout),
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
    body: dict[str, object] = {
        "status": runner.status.value,
        "ready": runner.ready,
        "turn_active": runner.turn_active,
        "history_durable": runner.history_durable,
    }
    if request.path == "/v1/status":
        attestation = cast("Mapping[str, object] | None", request.app[STATUS_ATTESTATION])
        if attestation is not None:
            body.update(attestation)
    return web.json_response(body)


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

    turn_epoch = secrets.token_urlsafe(32)
    response = web.StreamResponse(
        status=200,
        headers={"Content-Type": _NDJSON, _TURN_EPOCH_HEADER: turn_epoch},
    )
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
    async with contextlib.aclosing(
        runner.run_turn(frame, parent=parent, turn_epoch=turn_epoch)
    ) as stream:
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


def _valid_turn_epoch(epoch: str | None) -> bool:
    """Whether an epoch header has the bounded opaque token shape we mint."""

    return (
        epoch is not None
        and _TURN_EPOCH_MIN_LENGTH <= len(epoch) <= _TURN_EPOCH_MAX_LENGTH
        and epoch.isascii()
        and all(character.isalnum() or character in "-_" for character in epoch)
    )


async def _timeout(request: web.Request) -> web.Response:
    """Stop only the currently open turn named by its private response epoch."""

    runner: SessionRunner = request.app[RUNNER]
    turn_epoch = request.headers.get(_TURN_EPOCH_HEADER)
    if not _valid_turn_epoch(turn_epoch):
        return web.json_response({"error": "invalid turn epoch"}, status=400)
    assert turn_epoch is not None
    if not await runner.timeout(turn_epoch):
        return web.json_response({"error": "turn epoch is not active"}, status=409)
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
