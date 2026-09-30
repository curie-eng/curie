"""Real HTTP regression for the workspace's urllib3 streaming dependency."""

from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread

import pytest
import urllib3
from urllib3.exceptions import ProtocolError

# Upstream GHSA-vxq7-64xx-v4gw identifies both streaming entry points and the
# 65536-byte chunk-size-line limit introduced in 2.8.0:
# https://github.com/advisories/GHSA-vxq7-64xx-v4gw
# https://github.com/urllib3/urllib3/releases/tag/2.8.0
# https://github.com/urllib3/urllib3/blob/2.8.0/src/urllib3/response.py
# A finite, otherwise-valid chunk extension proves rejection at that bound
# without sending an unbounded response or measuring machine-dependent memory.
CHUNK_LINE_LIMIT = 65536


def _chunk_line(length: int) -> bytes:
    prefix = b"4;acme="
    return prefix + b"x" * (length - len(prefix) - 2) + b"\r\n"


@pytest.fixture
def chunked_response(request: pytest.FixtureRequest) -> Iterator[urllib3.HTTPResponse]:
    body = request.param + b"acme\r\n0\r\n\r\n"
    server_errors: list[Exception] = []

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *_args: object) -> None:
            pass

        def do_GET(self) -> None:
            try:
                self.send_response(200)
                self.send_header("Transfer-Encoding", "chunked")
                self.send_header("Connection", "close")
                self.end_headers()
                self.wfile.write(body)
                self.wfile.flush()
            except (BrokenPipeError, ConnectionResetError):
                # The fixed client can close as soon as the oversized line is
                # rejected. This is expected external-server cancellation.
                pass
            except Exception as exc:
                server_errors.append(exc)

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01})
    pool = urllib3.PoolManager(timeout=urllib3.Timeout(connect=2, read=2), retries=False)
    response: urllib3.HTTPResponse | None = None
    started = False
    try:
        # Cleanup owns the bound socket before the server starts. Connection or
        # read prerequisite failures happen in fixture setup, never count as red.
        thread.start()
        started = True
        response = pool.request(
            "GET", f"http://127.0.0.1:{server.server_port}/", preload_content=False
        )
        if response.status != 200 or not response.chunked:
            raise RuntimeError("loopback fixture did not produce a chunked HTTP response")
        yield response
    finally:
        if response is not None:
            response.close()
        pool.clear()
        if started:
            server.shutdown()
        server.server_close()
        if started:
            thread.join(timeout=3)
            if thread.is_alive():
                raise RuntimeError("owned loopback server thread did not stop")
        if server.socket.fileno() != -1:
            raise RuntimeError("owned loopback listener was not closed")
        if server_errors:
            raise RuntimeError("loopback response fixture failed") from server_errors[0]


@pytest.mark.parametrize("reader", ["stream", "read_chunked"])
@pytest.mark.parametrize(
    "chunked_response", [_chunk_line(CHUNK_LINE_LIMIT + 1)], indirect=True, ids=["oversized"]
)
def test_streamed_chunk_size_line_is_bounded(chunked_response, reader: str) -> None:
    yielded = []
    with pytest.raises(ProtocolError, match="chunk size line exceeded maximum allowed length"):
        yielded.extend(getattr(chunked_response, reader)(amt=2))
    assert yielded == []
    assert chunked_response.closed


@pytest.mark.parametrize("reader", ["stream", "read_chunked"])
@pytest.mark.parametrize(
    "chunked_response",
    [b"4;acme=fixture\r\n", _chunk_line(CHUNK_LINE_LIMIT)],
    indirect=True,
    ids=["short-extension", "exact-boundary"],
)
def test_valid_chunked_stream_including_boundary_still_reads_exact_body(
    chunked_response, reader: str
) -> None:
    assert b"".join(getattr(chunked_response, reader)(amt=2)) == b"acme"
    assert chunked_response.closed
