"""Genuine owned RESP reply delay, @spec PROTECTED-HOOK-SOURCE-2."""

from __future__ import annotations

import asyncio
from typing import Any


async def frame(reader: asyncio.StreamReader) -> tuple[bytes, Any]:
    """@spec PROTECTED-HOOK-SOURCE-2."""
    line = await reader.readuntil(b"\r\n")
    tag, value = line[:1], line[1:-2]
    if tag == b"$":
        size = int(value)
        if size < 0:
            return line, None
        if size > 16 * 1024 * 1024:
            raise ValueError("owned relay frame limit")
        body = await reader.readexactly(size + 2)
        return line + body, body[:-2]
    if tag in {b"*", b"%", b"~", b">"}:
        size = int(value)
        if size < 0:
            return line, None
        if size > 1000:
            raise ValueError("owned relay array limit")
        if tag == b"%":
            size *= 2
        raw, values = line, []
        for _ in range(size):
            item, decoded = await frame(reader)
            raw += item
            values.append(decoded)
        return raw, values
    if tag in {b"+", b"-", b":", b"_", b"#", b",", b"("}:
        return line, value
    raise ValueError("owned relay unsupported frame")


class GenuineReplyRelay:
    """@spec PROTECTED-HOOK-SOURCE-2."""

    def __init__(self, host: str, port: int, phase: str) -> None:
        """@spec PROTECTED-HOOK-SOURCE-2."""
        self.host, self.port, self.phase = host, port, phase
        self.seen, self.release = asyncio.Event(), asyncio.Event()
        self.server: asyncio.Server | None = None
        self.tasks: set[asyncio.Task[Any]] = set()
        self.writers: set[asyncio.StreamWriter] = set()
        self.held = False

    async def start(self) -> int:
        """@spec PROTECTED-HOOK-SOURCE-2."""
        self.server = await asyncio.start_server(self.connection, "127.0.0.1", 0)
        return int(self.server.sockets[0].getsockname()[1])

    def matches(self, command: Any, response: bytes) -> bool:
        """@spec PROTECTED-HOOK-SOURCE-2."""
        if not isinstance(command, list) or len(command) < 2:
            return False
        if self.phase == "claim":
            return command[0].upper() == b"SET" and command[1].startswith(b"curie:hook:delivery:")
        if command[0].upper() != b"EVAL" or len(command) < 4:
            return False
        if self.phase == "quota":
            return command[3].startswith(b"curie:hook:backlog:")
        return command[3].startswith(b"curie:hook:delivery:") and response.startswith(b"-WRONGTYPE")

    async def connection(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        """@spec PROTECTED-HOOK-SOURCE-2."""
        task = asyncio.current_task()
        assert task is not None
        self.tasks.add(task)
        upstream = None
        self.writers.add(writer)
        try:
            upstream_reader, upstream = await asyncio.open_connection(self.host, self.port)
            self.writers.add(upstream)
            while True:
                raw, command = await frame(reader)
                upstream.write(raw)
                await upstream.drain()
                response, _ = await frame(upstream_reader)
                if not self.held and self.matches(command, response):
                    self.held = True
                    self.seen.set()
                    await self.release.wait()
                writer.write(response)
                await writer.drain()
        except (asyncio.IncompleteReadError, ConnectionError):
            pass
        finally:
            for current in (writer, upstream):
                if current is not None:
                    self.writers.discard(current)
                    current.close()
                    try:
                        await current.wait_closed()
                    except ConnectionError:
                        pass
            self.tasks.discard(task)

    async def close(self) -> None:
        """@spec PROTECTED-HOOK-SOURCE-2."""
        self.release.set()
        if self.server is not None:
            self.server.close()
            await self.server.wait_closed()
        for writer in list(self.writers):
            writer.close()
        tasks = list(self.tasks)
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
