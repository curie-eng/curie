"""Shared pieces of the connection hold bound tests (#5995).

`Blackhole` is a TCP relay to a real server whose open flows can be cut, so a
test can put a real Postgres or Valkey behind a dead network flow. The
`connection_bounds` and `tcp_blackhole` fixtures in conftest use these.
"""

from __future__ import annotations

import contextlib
import socket
import threading

# Every awaited step that the bounds under test should end is itself wrapped in
# this outer deadline, so a regression fails the test instead of hanging it.
OUTER_DEADLINE_S = 15.0

# Every setting `connection_bounds` (conftest) clears before a test sets its own.
BOUND_ENV = (
    "CURIE_API_DB_LOCK_TIMEOUT_S",
    "CURIE_API_DB_IDLE_IN_TRANSACTION_TIMEOUT_S",
    "CURIE_API_DB_COMMAND_TIMEOUT_S",
    "CURIE_API_VALKEY_SOCKET_TIMEOUT_S",
    "CURIE_API_VALKEY_HEALTH_CHECK_INTERVAL_S",
)


class Blackhole:
    """A TCP relay to a real server whose open flows can be cut silently.

    `cut` stops forwarding in both directions on every flow open at that
    moment and keeps both sockets open, so neither end learns the flow is
    gone. Flows accepted afterwards forward normally, as a reconnect through
    a recovered network would. `close` shuts every socket down, which ends
    the server side sessions too, and joins every relay thread.
    """

    def __init__(self, host: str, port: int) -> None:
        self._upstream = (host, port)
        self._listener = socket.create_server(("127.0.0.1", 0))
        # A short accept timeout lets the accept loop notice `close`.
        self._listener.settimeout(0.1)
        self.port: int = self._listener.getsockname()[1]
        self._sockets: list[socket.socket] = []
        self._dead: list[threading.Event] = []
        # Client-to-server bytes per flow, in accept order.
        self.sent: list[bytearray] = []
        self._closed = threading.Event()
        self._threads = [threading.Thread(target=self._accept, daemon=True)]
        self._threads[0].start()

    def _accept(self) -> None:
        while not self._closed.is_set():
            try:
                client, _ = self._listener.accept()
            except TimeoutError:
                continue
            except OSError:
                return
            client.settimeout(None)
            upstream = socket.create_connection(self._upstream)
            dead, sent = threading.Event(), bytearray()
            self._sockets += [client, upstream]
            self._dead.append(dead)
            self.sent.append(sent)
            for args in ((client, upstream, dead, sent), (upstream, client, dead, None)):
                thread = threading.Thread(target=self._pump, args=args, daemon=True)
                self._threads.append(thread)
                thread.start()

    @staticmethod
    def _pump(
        source: socket.socket, sink: socket.socket, dead: threading.Event, sent: bytearray | None
    ) -> None:
        while True:
            try:
                data = source.recv(65536)
            except OSError:
                return
            if not data:
                # Pass the end of stream on, unless the flow is cut.
                if not dead.is_set():
                    with contextlib.suppress(OSError):
                        sink.shutdown(socket.SHUT_WR)
                return
            if dead.is_set():
                continue
            if sent is not None:
                sent.extend(data)
            try:
                sink.sendall(data)
            except OSError:
                return

    def cut(self) -> int:
        for dead in self._dead:
            dead.set()
        return len(self._dead)

    def close(self) -> None:
        self._closed.set()
        for sock in self._sockets:
            with contextlib.suppress(OSError):
                sock.shutdown(socket.SHUT_RDWR)
        for thread in self._threads:
            thread.join(timeout=5)
        for sock in [self._listener, *self._sockets]:
            sock.close()
        assert not [thread for thread in self._threads if thread.is_alive()]
