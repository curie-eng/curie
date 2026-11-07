"""A connector process for the container acceptance test."""

import asyncio
import signal
import sys
from pathlib import Path

from aiohttp import web


async def main() -> None:
    mode = sys.argv[1] if len(sys.argv) > 1 else "loopback"
    stopped = asyncio.Event()
    asyncio.get_running_loop().add_signal_handler(signal.SIGTERM, stopped.set)
    executions = 0

    async def call(request: web.Request) -> web.Response:
        nonlocal executions
        payload = await request.json()
        if payload.get("method") == "tools/call":
            executions += 1
            Path("/tmp/executions").write_text(str(executions))
        return web.json_response({"executions": executions})

    async def upload(request: web.Request) -> web.Response:
        body = await request.read()
        Path("/tmp/upload").write_bytes(body)
        return web.json_response({"uploaded": len(body)})

    runners = []
    for port, path, handler in [(8000, "/mcp", call), (8001, "/v1/upload", upload)]:
        app = web.Application()
        app.router.add_post(path, handler)
        runner = web.AppRunner(app)
        runners.append(runner)
        await runner.setup()
        public = (mode == "public" and port == 8000) or (mode == "upload-public" and port == 8001)
        await web.TCPSite(runner, "0.0.0.0" if public else "127.0.0.1", port).start()
    Path("/tmp/ready").touch()
    if mode in {"late-public", "late-upload-public"}:
        while not Path("/tmp/rebind").exists() and not stopped.is_set():
            try:
                await asyncio.wait_for(stopped.wait(), timeout=0.05)
            except TimeoutError:
                pass
        index = 0 if mode == "late-public" else 1
        await runners[index].cleanup()
        app = web.Application()
        app.router.add_post("/mcp" if index == 0 else "/v1/upload", call if index == 0 else upload)
        runner = web.AppRunner(app)
        runners[index] = runner
        await runner.setup()
        await web.TCPSite(runner, "0.0.0.0", 8000 if index == 0 else 8001).start()
        Path("/tmp/rebound").touch()
    await stopped.wait()
    for runner in runners:
        await runner.cleanup()
    Path("/tmp/terminated").write_text("SIGTERM")


asyncio.run(main())
