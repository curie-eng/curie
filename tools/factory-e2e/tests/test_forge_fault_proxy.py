"""The forge fault proxy the publication recovery proof injects through (#5429).

The pure pieces are checked directly. The served proxy is checked over real
TLS on loopback against a recording upstream: pass-through of method, path,
query, body and headers; an armed fault answering exactly its count without
reaching the upstream; and the next call passing again.
"""

from __future__ import annotations

import http.client
import json
import shutil
import ssl
import threading
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

import forge_fault_proxy as proxy
import pytest

PULLS = r"/repos/[^/]+/[^/]+/pulls"


def test_forwarded_path_keeps_the_rest_and_query_under_the_prefix() -> None:
    assert proxy.forwarded_path("/api/v3", "/api/v3/repos/a/b/pulls?state=all") == (
        "/repos/a/b/pulls?state=all"
    )
    assert proxy.forwarded_path("/api/v3", "/api/v3") == "/"
    assert proxy.forwarded_path("", "/repos/a/b") == "/repos/a/b"
    assert proxy.forwarded_path("/api/v3", "/api/v30/repos") is None
    assert proxy.forwarded_path("/api/v3", "/repos/a/b") is None


def test_take_fault_consumes_in_arming_order_and_only_on_a_full_match() -> None:
    rules: list[dict[str, Any]] = [
        {"method": "POST", "path": PULLS, "status": 502, "remaining": 1},
        {"method": "POST", "path": PULLS, "status": 503, "remaining": 1},
    ]
    assert proxy.take_fault(rules, "GET", "/repos/a/b/pulls") is None
    assert proxy.take_fault(rules, "POST", "/repos/a/b/pulls/1") is None
    assert proxy.take_fault(rules, "post", "/repos/a/b/pulls") == 502
    assert proxy.take_fault(rules, "POST", "/repos/a/b/pulls") == 503
    assert proxy.take_fault(rules, "POST", "/repos/a/b/pulls") is None
    assert [(r["remaining"], r["answered"]) for r in rules] == [(0, 1), (0, 1)]


def test_forward_headers_drops_hop_by_hop_and_recomputed_headers() -> None:
    kept = proxy.forward_headers(
        {
            "Authorization": "token x",
            "Accept": "application/json",
            "Host": "proxy",
            "Connection": "keep-alive",
            "Transfer-Encoding": "chunked",
            "Content-Length": "3",
            "Accept-Encoding": "gzip",
        }
    )
    assert kept == {"Authorization": "token x", "Accept": "application/json"}


def test_arm_refuses_a_rule_that_could_not_mean_a_transport_failure(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="server error"):
        proxy.arm(tmp_path, method="POST", path=PULLS, status=404, count=1)
    with pytest.raises(ValueError, match="positive"):
        proxy.arm(tmp_path, method="POST", path=PULLS, status=502, count=0)
    with pytest.raises(ValueError, match="method"):
        proxy.arm(tmp_path, method="TRACE", path=PULLS, status=502, count=1)
    assert proxy.armed(tmp_path) == []


class _Recorder(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    seen: list[dict[str, Any]] = []

    def log_message(self, format: str, *args: Any) -> None:  # noqa: A002 - stdlib name
        return

    def _handle(self) -> None:
        length = int(self.headers.get("Content-Length") or 0)
        body = self.rfile.read(length) if length else b""
        self.seen.append(
            {
                "method": self.command,
                "path": self.path,
                "body": body.decode(),
                "authorization": self.headers.get("Authorization"),
                "host": self.headers.get("Host"),
            }
        )
        answer = json.dumps({"number": 7}).encode()
        self.send_response(201 if self.command == "POST" else 200)
        self.send_header("Content-Type", "application/json")
        self.send_header("X-Upstream", "yes")
        self.send_header("Content-Length", str(len(answer)))
        self.end_headers()
        self.wfile.write(answer)

    do_GET = _handle
    do_POST = _handle


@pytest.fixture
def served(tmp_path: Path) -> Iterator[tuple[Path, int, ssl.SSLContext]]:
    if shutil.which("openssl") is None:
        pytest.fail("openssl is required to serve the proxy over TLS")
    _Recorder.seen = []
    upstream = ThreadingHTTPServer(("127.0.0.1", 0), _Recorder)
    state = tmp_path / "state"
    ca, certificate, key = proxy.make_certificates(state, "127.0.0.1")
    handler = proxy.build_handler(
        state=state,
        prefix="/api/v3",
        upstream=f"http://127.0.0.1:{upstream.server_address[1]}/base",
        upstream_context=None,
    )
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.minimum_version = ssl.TLSVersion.TLSv1_2
    context.load_cert_chain(certificate, key)
    server.socket = context.wrap_socket(server.socket, server_side=True)
    threads = [threading.Thread(target=s.serve_forever, daemon=True) for s in (upstream, server)]
    for thread in threads:
        thread.start()
    try:
        yield state, server.server_address[1], ssl.create_default_context(cafile=str(ca))
    finally:
        for s in (server, upstream):
            s.shutdown()
            s.server_close()


def _call(
    port: int, trust: ssl.SSLContext, method: str, path: str, body: str = ""
) -> tuple[int, dict[str, str], bytes]:
    connection = http.client.HTTPSConnection("127.0.0.1", port, context=trust, timeout=10)
    try:
        connection.request(
            method,
            path,
            body=body.encode() or None,
            headers={"Authorization": "Bearer secret", "Content-Type": "application/json"},
        )
        response = connection.getresponse()
        return response.status, dict(response.getheaders()), response.read()
    finally:
        connection.close()


def test_the_served_proxy_passes_through_and_fails_exactly_the_armed_calls(
    served: tuple[Path, int, ssl.SSLContext],
) -> None:
    state, port, trust = served
    status, headers, body = _call(port, trust, "GET", "/api/v3/repos/a/b/pulls?state=all")
    assert (status, json.loads(body), headers.get("X-Upstream")) == (200, {"number": 7}, "yes")
    assert _Recorder.seen[-1]["path"] == "/base/repos/a/b/pulls?state=all"
    assert _Recorder.seen[-1]["authorization"] == "Bearer secret"
    assert _Recorder.seen[-1]["host"] != f"127.0.0.1:{port}"

    proxy.arm(state, method="POST", path=PULLS, status=502, count=2)
    before = len(_Recorder.seen)
    for _ in range(2):
        status, _headers, body = _call(port, trust, "POST", "/api/v3/repos/a/b/pulls", "{}")
        assert status == 502 and json.loads(body)["message"] == "Server Error"
    assert len(_Recorder.seen) == before, "an injected failure reached the upstream"
    assert [(r["remaining"], r["answered"]) for r in proxy.armed(state)] == [(0, 2)]

    status, _headers, body = _call(port, trust, "POST", "/api/v3/repos/a/b/pulls", '{"t":1}')
    assert status == 201 and json.loads(body) == {"number": 7}
    assert _Recorder.seen[-1] == {
        "method": "POST",
        "path": "/base/repos/a/b/pulls",
        "body": '{"t":1}',
        "authorization": "Bearer secret",
        "host": _Recorder.seen[-1]["host"],
    }

    status, _headers, _body = _call(port, trust, "GET", "/elsewhere")
    assert status == 404
