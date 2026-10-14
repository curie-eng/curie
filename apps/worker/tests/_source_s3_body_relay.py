"""Genuine owned S3 bytes fault fixture, @spec PROTECTED-HOOK-SOURCE-2."""

from __future__ import annotations

import socket
import socketserver
import threading
from urllib.parse import urlsplit


class S3BodyRelay:
    """@spec PROTECTED-HOOK-SOURCE-2."""

    def __init__(self, endpoint: str, object_path: str, *, hold: bool) -> None:
        """@spec PROTECTED-HOOK-SOURCE-2."""
        upstream = urlsplit(endpoint)
        assert upstream.scheme == "http" and upstream.hostname
        self.upstream = (upstream.hostname, upstream.port or 80)
        self.object_path = object_path.encode("ascii")
        self.hold = hold
        self.body_held = threading.Event()
        self.release = threading.Event()
        self.errors: list[str] = []
        self.requests = 0
        self.server = None
        self.thread = None

    def start(self) -> str:
        """@spec PROTECTED-HOOK-SOURCE-2."""
        relay = self

        class Handler(socketserver.BaseRequestHandler):
            """@spec PROTECTED-HOOK-SOURCE-2."""

            def handle(self) -> None:
                """@spec PROTECTED-HOOK-SOURCE-2."""
                try:
                    self.request.settimeout(5)
                    request, tail = relay._headers(self.request)
                    assert tail == b""
                    assert (
                        request.split(b"\r\n", 1)[0] == b"GET " + relay.object_path + b" HTTP/1.1"
                    )
                    with socket.create_connection(relay.upstream, timeout=5) as upstream:
                        upstream.sendall(request)
                        headers, buffered = relay._headers(upstream)
                        assert headers.split(b"\r\n", 1)[0].split()[1] == b"200"
                        fields = {}
                        for line in headers.split(b"\r\n")[1:]:
                            if b":" in line:
                                key, value = line.split(b":", 1)
                                fields[key.lower()] = value.strip()
                        length = int(fields[b"content-length"])
                        assert 0 < length <= 1024 * 1024
                        relay.requests += 1
                        self.request.sendall(headers)
                        relay.body_held.set()
                        if relay.hold:
                            assert relay.release.wait(30), "owned body release deadline"
                        body = buffered
                        while len(body) < length:
                            chunk = upstream.recv(min(65536, length - len(body)))
                            assert chunk, "genuine upstream body ended early"
                            body += chunk
                        assert len(body) == length
                        self.request.sendall(body)
                except (BrokenPipeError, ConnectionResetError):
                    # The exclusively owned fatal child can close before body release.
                    pass
                # record every relay failure without leaking response data.
                except Exception as error:  # noqa: BLE001
                    relay.errors.append(type(error).__name__)

        self.server = socketserver.ThreadingTCPServer(("127.0.0.1", 0), Handler)
        self.server.daemon_threads = False
        self.server.block_on_close = True
        self.thread = threading.Thread(
            target=self.server.serve_forever, kwargs={"poll_interval": 0.05}
        )
        self.thread.start()
        return "http://127.0.0.1:" + str(self.server.server_address[1])

    @staticmethod
    def _headers(connection: socket.socket) -> tuple[bytes, bytes]:
        """@spec PROTECTED-HOOK-SOURCE-2."""
        data = b""
        while b"\r\n\r\n" not in data:
            chunk = connection.recv(4096)
            assert chunk and len(data) + len(chunk) <= 65536
            data += chunk
        header, body = data.split(b"\r\n\r\n", 1)
        return header + b"\r\n\r\n", body

    def close(self) -> None:
        """@spec PROTECTED-HOOK-SOURCE-2."""
        self.release.set()
        if self.server is not None:
            self.server.shutdown()
            self.server.server_close()
        if self.thread is not None:
            self.thread.join(timeout=3)
            assert not self.thread.is_alive(), "owned relay server did not join"
