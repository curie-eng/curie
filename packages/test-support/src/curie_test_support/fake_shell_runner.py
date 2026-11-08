"""A stand-in runner child for shell tests: the ACI shape, nothing more.

Run as ``python -m curie_test_support.fake_shell_runner`` by a supervisor.

It enforces the claim bearer like the real runner, streams NDJSON turns,
records what the supervisor did to it under ``FAKE_RECORD_DIR`` (a path the
test owns, outside the claim tree), leaves files in every place a claim can
write, and starts a descendant in its own process group so teardown has
something to find.
"""

from __future__ import annotations

import asyncio
import hmac
import json
import os
import subprocess
import sys
from pathlib import Path

from aiohttp import web

TOKEN = os.environ["CURIE_RUNNER_TOKEN"].encode()
RECORD = Path(os.environ["FAKE_RECORD_DIR"])
STATE = {"turn_active": False, "durable": os.environ.get("FAKE_DURABLE", "1") == "1"}
# A held turn writes its first line, then waits here until /v1/release.
RELEASE = asyncio.Event()


def record(name: str, value: str) -> None:
    (RECORD / name).write_text(value)


@web.middleware
async def auth(request: web.Request, handler):  # type: ignore[no-untyped-def]
    if request.path != "/healthz":
        presented = request.headers.get("Authorization", "").removeprefix("Bearer ").encode()
        if not hmac.compare_digest(presented, TOKEN):
            return web.json_response({"error": "invalid token"}, status=401)
    return await handler(request)


async def healthz(_request: web.Request) -> web.Response:
    return web.json_response({"ok": True})


async def status(_request: web.Request) -> web.Response:
    raw = os.environ.get("FAKE_STATUS_BODY")
    if raw is not None:
        # A hostile or broken child answers anything at all.
        return web.Response(body=raw.encode(), content_type="application/json")
    return web.json_response(
        {"turn_active": STATE["turn_active"], "history_durable": STATE["durable"]}
    )


async def flood(request: web.Request) -> web.StreamResponse:
    """Stream without end and without a turn: only retirement may cut it."""

    response = web.StreamResponse()
    await response.prepare(request)
    chunk = b"x" * 65536
    while True:
        await response.write(chunk)


async def event(request: web.Request) -> web.StreamResponse:
    body = await request.json()
    lines = int(body.get("lines", 3))
    hold = bool(body.get("hold", False))
    response = web.StreamResponse(headers={"X-Curie-Turn-Epoch": "e" * 32})
    response.content_type = "application/x-ndjson"
    await response.prepare(request)
    STATE["turn_active"] = True
    try:
        for index in range(lines):
            line = {
                "i": index,
                "echo": request.headers.get("X-Test-Echo"),
                "q": request.query_string,
            }
            await response.write(json.dumps(line).encode() + b"\n")
            if hold and index == 0:
                await RELEASE.wait()
        await response.write_eof()
    except (asyncio.CancelledError, ConnectionResetError):
        record("cancelled", body.get("tag", "turn"))
        raise
    finally:
        STATE["turn_active"] = body.get("hang", False)
    return response


async def release(_request: web.Request) -> web.Response:
    RELEASE.set()
    return web.json_response({"released": True})


async def crash(_request: web.Request) -> web.Response:
    os._exit(3)


def main() -> None:
    RECORD.mkdir(parents=True, exist_ok=True)
    record("env.json", json.dumps(dict(os.environ)))
    record("cwd", os.getcwd())
    for directory in (os.environ["HOME"], os.environ["TMPDIR"], os.environ.get("FAKE_SCRUB_DIR")):
        if directory:
            Path(directory, "left-by-claim").write_text("x")
            # Links out of the claim's tree: cleanup must remove the links and
            # never follow them.
            Path(directory, "escape-dir").symlink_to("/")
            Path(directory, "escape-file").symlink_to("/etc/hostname")
    # A descendant in the child's own group, as a tool subprocess would be.
    grandchild = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(600)"])  # noqa: S603
    record("grandchild", str(grandchild.pid))
    record("pid", str(os.getpid()))
    app = web.Application(middlewares=[auth], handler_args={})
    app.add_routes(
        [
            web.get("/healthz", healthz),
            web.get("/v1/status", status),
            web.post("/v1/event", event),
            web.post("/v1/release", release),
            web.get("/v1/flood", flood),
            web.post("/v1/crash", crash),
        ]
    )
    web.run_app(
        app,
        host="127.0.0.1",
        port=int(os.environ["CURIE_RUNNER_PORT"]),
        print=None,
        handler_cancellation=True,
    )


if __name__ == "__main__":
    main()
