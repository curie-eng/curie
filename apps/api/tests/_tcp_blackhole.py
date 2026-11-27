"""Real TCP endpoints that stop answering, for the connection bound tests (#5995).

``Blackhole`` relays to a real server and can freeze every flow it already
carries: bytes in either direction are read and dropped while both sockets
stay open, which is what a dead network path looks like to a client that has
no bound of its own. Flows accepted after a freeze relay normally, so a fresh
connection through the same address works. ``silent_listener`` accepts
connections and never answers at all. ``within`` turns a missing bound into a
test failure instead of a hang.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator

import pytest


@contextlib.asynccontextmanager
async def within(seconds: float, what: str) -> AsyncIterator[None]:
    """Fail, rather than hang, when the body outlives ``seconds``."""
    deadline = asyncio.timeout(seconds)
    try:
        async with deadline:
            yield
    except TimeoutError:
        if deadline.expired():
            pytest.fail(f"{what} was not bounded within {seconds}s")
        raise


class _Flow:
    def __init__(self) -> None:
        self.frozen = False


class Blackhole:
    """A loopback relay to ``host:port`` whose existing flows can be frozen."""

    def __init__(self, host: str, port: int) -> None:
        self._target = (host, port)
        self._flows: list[_Flow] = []
        self._writers: list[asyncio.StreamWriter] = []
        self._tasks: set[asyncio.Task[None]] = set()
        self._server: asyncio.Server | None = None
        self.port = 0

    async def __aenter__(self) -> Blackhole:
        self._server = await asyncio.start_server(self._accept, "127.0.0.1", 0)
        self.port = self._server.sockets[0].getsockname()[1]
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.close()

    def freeze(self) -> None:
        """Stop relaying on every current flow; sockets stay open."""
        for flow in self._flows:
            flow.frozen = True

    async def close(self) -> None:
        if self._server is not None:
            self._server.close()
        for writer in self._writers:
            writer.transport.abort()
        for task in list(self._tasks):
            task.cancel()
        for task in list(self._tasks):
            with contextlib.suppress(asyncio.CancelledError, OSError):
                await task
        if self._server is not None:
            await self._server.wait_closed()
            self._server = None

    async def _accept(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        flow = _Flow()
        self._flows.append(flow)
        self._writers.append(writer)
        upstream_reader, upstream_writer = await asyncio.open_connection(*self._target)
        self._writers.append(upstream_writer)
        pumps = [
            asyncio.create_task(self._pump(flow, reader, upstream_writer)),
            asyncio.create_task(self._pump(flow, upstream_reader, writer)),
        ]
        self._tasks.update(pumps)
        try:
            await asyncio.wait(pumps, return_when=asyncio.FIRST_COMPLETED)
        finally:
            for pump in pumps:
                pump.cancel()
            upstream_writer.transport.abort()
            writer.transport.abort()
            self._tasks.difference_update(pumps)

    @staticmethod
    async def _pump(
        flow: _Flow, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        while data := await reader.read(65536):
            if flow.frozen:
                continue
            writer.write(data)
            await writer.drain()


class Listener:
    def __init__(self) -> None:
        self.port = 0
        # Set once the first client connection has been accepted.
        self.accepted = asyncio.Event()


@contextlib.asynccontextmanager
async def silent_listener() -> AsyncIterator[Listener]:
    """A loopback port that accepts connections and never sends a byte."""
    listener = Listener()
    writers: list[asyncio.StreamWriter] = []
    tasks: set[asyncio.Task[None]] = set()

    async def swallow(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        writers.append(writer)
        listener.accepted.set()
        task = asyncio.current_task()
        assert task is not None
        tasks.add(task)
        try:
            while await reader.read(65536):
                pass
        finally:
            tasks.discard(task)

    server = await asyncio.start_server(swallow, "127.0.0.1", 0)
    listener.port = server.sockets[0].getsockname()[1]
    try:
        yield listener
    finally:
        server.close()
        for writer in writers:
            writer.transport.abort()
        for task in list(tasks):
            task.cancel()
        await server.wait_closed()
