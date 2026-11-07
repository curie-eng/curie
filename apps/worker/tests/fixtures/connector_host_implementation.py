"""A connector process for the container acceptance test."""

import asyncio
import signal
from pathlib import Path

from aiohttp import web


async def main() -> None:
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
        await web.TCPSite(runner, "127.0.0.1", port).start()
    Path("/tmp/ready").touch()
    await stopped.wait()
    for runner in runners:
        await runner.cleanup()
    Path("/tmp/terminated").write_text("SIGTERM")


asyncio.run(main())
