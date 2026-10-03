"""#3823: releasing a sandbox claim reports the boot credential to the API.

The substrate calls ``BindingResolver.release_boot_credential_sync``. It never
raises. A 404 from an older API is logged once.
"""

from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from curie_worker.binding import BindingResolver
from curie_worker.config import WorkerConfig

_AGENT = "11111111-1111-4111-8111-111111111111"
_CRED = "ab" * 16
_TOKEN = "test-worker-token-3823"


class _Seen:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []
        self.status = 204


def _server(seen: _Seen) -> tuple[ThreadingHTTPServer, str]:
    class Handler(BaseHTTPRequestHandler):
        def do_POST(self) -> None:  # noqa: N802
            length = int(self.headers.get("Content-Length", "0"))
            raw = self.rfile.read(length)
            seen.calls.append(
                {
                    "path": self.path,
                    "token": self.headers.get("X-Curie-Worker-Token"),
                    "api_key": self.headers.get("X-API-Key"),
                    "body": json.loads(raw),
                }
            )
            self.send_response(seen.status)
            self.end_headers()

        def log_message(self, fmt: str, *args: object) -> None:
            del fmt, args

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    host, port = server.server_address[:2]
    return server, f"http://{host}:{port}"


def _resolver(url: str) -> BindingResolver:
    resolver = BindingResolver.__new__(BindingResolver)
    resolver._config = WorkerConfig(  # type: ignore[attr-defined]
        api_base_url=url, internal_worker_token=_TOKEN
    )
    return resolver


def test_release_posts_the_credential_with_the_worker_token() -> None:
    seen = _Seen()
    server, url = _server(seen)
    try:
        _resolver(url).release_boot_credential_sync(_AGENT, _CRED)
    finally:
        server.shutdown()
    assert len(seen.calls) == 1
    call = seen.calls[0]
    assert call["path"] == "/v1/internal/state/released-credentials"
    assert call["token"] == _TOKEN
    assert call["api_key"] is None
    assert call["body"] == {"agent_id": _AGENT, "credential": _CRED}


def test_a_missing_route_does_not_raise() -> None:
    seen = _Seen()
    seen.status = 404
    server, url = _server(seen)
    resolver = _resolver(url)
    try:
        resolver.release_boot_credential_sync(_AGENT, _CRED)
        resolver.release_boot_credential_sync(_AGENT, _CRED)
    finally:
        server.shutdown()
    assert len(seen.calls) == 2
