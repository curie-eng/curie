"""Actual COMMIT response loss only, @spec PROTECTED-HOOK-SOURCE-10.

Listener is owned loopback; upstream is the exact disposable database backing.
Authentication bytes pass unchanged and are never recorded or printed.
"""

import asyncio
import contextlib
import struct


class CommitResponseLossRelay:
    """@spec PROTECTED-HOOK-SOURCE-10."""

    def __init__(self, host: str, port: int) -> None:
        """@spec PROTECTED-HOOK-SOURCE-10."""
        self.host, self.port = host, port
        self.armed = False
        self.dropped = asyncio.Event()
        self.tasks = set()
        self.writers = set()
        self.errors = []
        self.ssl_rejected = 0
        self.server = None

    async def startup(self, reader, writer):
        """@spec PROTECTED-HOOK-SOURCE-10."""
        length_raw = await reader.readexactly(4)
        length = struct.unpack("!I", length_raw)[0]
        if not 8 <= length <= 65536:
            raise RuntimeError("invalid_startup_length")
        packet = length_raw + await reader.readexactly(length - 4)
        writer.write(packet)
        await writer.drain()
        return length == 8 and packet[4:] == struct.pack("!I", 80877103)

    async def handle(self, client_read, client_write):
        """@spec PROTECTED-HOOK-SOURCE-10."""
        owner = asyncio.current_task()
        self.tasks.add(owner)
        self.writers.add(client_write)
        server_write = None
        children = []
        try:
            server_read, server_write = await asyncio.open_connection(self.host, self.port)
            self.writers.add(server_write)
            ssl_request = await self.startup(client_read, server_write)
            if ssl_request:
                answer = await server_read.readexactly(1)
                client_write.write(answer)
                await client_write.drain()
                if answer != b"N":
                    raise RuntimeError("encrypted_protocol_deferred")
                self.ssl_rejected += 1
                if await self.startup(client_read, server_write):
                    raise RuntimeError("unexpected_repeated_ssl")

            async def upstream():
                """@spec PROTECTED-HOOK-SOURCE-10."""
                while chunk := await client_read.read(65536):
                    server_write.write(chunk)
                    await server_write.drain()

            async def downstream():
                """@spec PROTECTED-HOOK-SOURCE-10."""
                while True:
                    kind = await server_read.readexactly(1)
                    length_raw = await server_read.readexactly(4)
                    length = struct.unpack("!I", length_raw)[0]
                    if not 4 <= length <= 16 * 1024 * 1024:
                        raise RuntimeError("invalid_backend_length")
                    payload = await server_read.readexactly(length - 4)
                    if self.armed and kind == b"C" and payload == b"COMMIT\x00":
                        self.armed = False
                        self.dropped.set()
                        client_write.transport.abort()
                        server_write.transport.abort()
                        return
                    client_write.write(kind + length_raw + payload)
                    await client_write.drain()

            children = [asyncio.create_task(upstream()), asyncio.create_task(downstream())]
            done, _ = await asyncio.wait(children, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                task.result()
        except (asyncio.IncompleteReadError, ConnectionError):
            pass
        except asyncio.CancelledError:
            raise
        except Exception as error:  # noqa: BLE001 - report the class only to withhold transport credentials
            self.errors.append(type(error).__name__)
        finally:
            for task in children:
                task.cancel()
            await asyncio.gather(*children, return_exceptions=True)
            for writer in (client_write, server_write):
                if writer is not None:
                    writer.close()
                    with contextlib.suppress(Exception):
                        await writer.wait_closed()
                    self.writers.discard(writer)
            self.tasks.discard(owner)

    async def close(self):
        """@spec PROTECTED-HOOK-SOURCE-10."""
        if self.server is not None:
            self.server.close()
            await self.server.wait_closed()
        for writer in tuple(self.writers):
            writer.transport.abort()
        for task in tuple(self.tasks):
            task.cancel()
        await asyncio.gather(*tuple(self.tasks), return_exceptions=True)

    async def start(self) -> int:
        """@spec PROTECTED-HOOK-SOURCE-10."""
        self.server = await asyncio.start_server(self.handle, "127.0.0.1", 0)
        return self.server.sockets[0].getsockname()[1]

    def arm(self) -> None:
        """@spec PROTECTED-HOOK-SOURCE-10."""
        assert not self.armed and not self.dropped.is_set()
        self.armed = True
