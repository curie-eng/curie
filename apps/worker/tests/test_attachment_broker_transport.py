"""Real channel transport has bounded error reads and incremental slow reads."""

from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest
from curie_worker import attachments


def test_error_body_is_bounded_and_slow_body_yields_before_eof() -> None:
    release = threading.Event()

    class Provider(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802 -- stdlib contract
            self.send_response(500 if self.path == "/error" else 200)
            self.send_header("Content-Length", str(2 * 1024 * 1024 if self.path == "/error" else 2))
            self.end_headers()
            if self.path == "/error":
                try:
                    self.wfile.write(b"x" * (2 * 1024 * 1024))
                except (BrokenPipeError, ConnectionResetError):
                    pass
            else:
                self.wfile.write(b"a")
                self.wfile.flush()
                release.wait(3)
                with __import__("contextlib").suppress(BrokenPipeError):
                    self.wfile.write(b"b")

        def log_message(self, format: str, *args: object) -> None:
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Provider)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    base = f"http://127.0.0.1:{server.server_port}"
    try:
        error = attachments._slack_transport(
            method="GET", url=base + "/error", headers={}, chunk_bytes=1024 * 1024
        )
        assert error.status == 500
        assert len(b"".join(error.chunks)) <= 65537
        response = attachments._slack_transport(
            method="GET", url=base + "/slow", headers={}, chunk_bytes=1024 * 1024
        )
        with ThreadPoolExecutor(max_workers=1) as pool:
            first = pool.submit(next, response.chunks)
            try:
                assert first.result(timeout=1) == b"a"
            finally:
                release.set()
            assert b"".join(response.chunks) == b"b"
    finally:
        release.set()
        server.shutdown()
        server.server_close()
        thread.join()


def test_cancel_scope_interrupts_owned_stalled_socket() -> None:
    import time

    import pytest

    first = threading.Event()
    release = threading.Event()

    class Provider(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            self.send_response(200)
            self.send_header("Content-Length", "2")
            self.end_headers()
            self.wfile.write(b"a")
            self.wfile.flush()
            release.wait(3)

        def log_message(self, format: str, *args: object) -> None:
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Provider)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    scope = attachments.AttachmentReadScope(deadline_epoch=time.time() + 60)

    def read() -> None:
        with scope:
            response = attachments._slack_transport(
                method="GET",
                url=f"http://127.0.0.1:{server.server_port}",
                headers={},
                chunk_bytes=1024 * 1024,
            )
            assert next(response.chunks) == b"a"
            first.set()
            list(response.chunks)

    try:
        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(read)
            try:
                assert first.wait(1)
                scope.cancel()
                with pytest.raises((OSError, RuntimeError)):
                    future.result(timeout=1)
            finally:
                release.set()
    finally:
        release.set()
        server.shutdown()
        server.server_close()
        thread.join()


@pytest.mark.parametrize("stage", ["status", "headers", "tls"])
@pytest.mark.parametrize("stop", ["cancel", "deadline"])
def test_scope_interrupts_dripping_response_head(stage: str, stop: str) -> None:
    import socketserver
    import time

    started, release = threading.Event(), threading.Event()

    class Provider(socketserver.BaseRequestHandler):
        def handle(self) -> None:
            self.request.recv(8192)
            if stage == "tls":
                started.set()
                release.wait(3)
                return
            prefix = b"HTTP/1.1 200" if stage == "status" else b"HTTP/1.1 200 OK\r\nX-Slow: "
            try:
                self.request.sendall(prefix)
                started.set()
                while not release.wait(0.02):
                    self.request.sendall(b"x")
            except OSError:
                pass

    server = socketserver.ThreadingTCPServer(("127.0.0.1", 0), Provider)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    scope = attachments.AttachmentReadScope(
        deadline_epoch=time.time() + (0.35 if stop == "deadline" else 60)
    )

    def read() -> None:
        with scope:
            attachments._slack_transport(
                method="GET",
                url=f"{'https' if stage == 'tls' else 'http'}://127.0.0.1:{server.server_address[1]}",
                headers={},
                chunk_bytes=1024 * 1024,
            )

    try:
        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(read)
            try:
                assert started.wait(1)
                if stop == "cancel":
                    scope.cancel()
                done = threading.Event()
                future.add_done_callback(lambda _: done.set())
                assert done.wait(1), (
                    "owned header read remained blocked after cancellation/deadline"
                )
                assert future.exception() is not None
            finally:
                release.set()
    finally:
        release.set()
        server.shutdown()
        server.server_close()
        thread.join()
