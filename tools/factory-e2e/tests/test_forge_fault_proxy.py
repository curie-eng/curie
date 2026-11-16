"""The forge fault proxy the publication recovery proof injects through (#5429).

The pure pieces are checked directly. The served proxy is checked over real
TLS on loopback against a recording upstream: pass-through of method, path,
query, body (plain and chunked) and headers; the upstream origin rewritten to
the proxy's own in JSON bodies and ``Location`` but not in binary bodies; an
armed fault answering exactly its count without reaching the upstream; and
the next call passing again.
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

PULLS = r"/api/v3/repos/[^/]+/[^/]+/pulls"


def test_origin_of_keeps_scheme_host_and_port_and_refuses_anything_more() -> None:
    assert proxy.origin_of("https://forge.example:8443") == "https://forge.example:8443"
    assert proxy.origin_of("http://127.0.0.1:9/") == "http://127.0.0.1:9"
    for bad in ("ftp://forge.example", "https://forge.example/api/v3", "https://u@forge.example"):
        with pytest.raises(ValueError):
            proxy.origin_of(bad)


def test_upstream_url_keeps_the_whole_path_and_query() -> None:
    assert proxy.upstream_url("https://stub:8443", "/api/v3/repos/a/b/pulls?state=all") == (
        "https://stub:8443/api/v3/repos/a/b/pulls?state=all"
    )
    assert proxy.upstream_url(
        "https://stub:8443", "/a/b.git/info/refs?service=git-upload-pack"
    ) == ("https://stub:8443/a/b.git/info/refs?service=git-upload-pack")
    with pytest.raises(ValueError):
        proxy.upstream_url("https://stub:8443", "https://elsewhere/")


def test_only_json_and_text_bodies_are_rewritten() -> None:
    assert proxy.rewrites_body("application/json; charset=utf-8")
    assert proxy.rewrites_body("application/vnd.github+json")
    assert proxy.rewrites_body("text/plain")
    assert not proxy.rewrites_body("application/x-git-upload-pack-result")
    assert not proxy.rewrites_body(None)
    # The shape GitHub answers a pull request create with (docs.github.com REST
    # "Create a pull request", response 201): html_url under the HTML host.
    created = b'{"number":1,"html_url":"https://stub:8443/acme/bot/pull/1"}'
    assert proxy.rewrite_origin(created, "https://stub:8443", "https://proxy:8443") == (
        b'{"number":1,"html_url":"https://proxy:8443/acme/bot/pull/1"}'
    )


def test_take_fault_consumes_in_arming_order_and_only_on_a_full_match() -> None:
    rules: list[dict[str, Any]] = [
        {"method": "POST", "path": PULLS, "status": 502, "remaining": 1},
        {"method": "POST", "path": PULLS, "status": 503, "remaining": 1},
    ]
    assert proxy.take_fault(rules, "GET", "/api/v3/repos/a/b/pulls") is None
    assert proxy.take_fault(rules, "POST", "/api/v3/repos/a/b/pulls/1") is None
    assert proxy.take_fault(rules, "post", "/api/v3/repos/a/b/pulls") == 502
    assert proxy.take_fault(rules, "POST", "/api/v3/repos/a/b/pulls") == 503
    assert proxy.take_fault(rules, "POST", "/api/v3/repos/a/b/pulls") is None
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
    # Liveness: a valid rule is stored as armed.
    assert proxy.arm(tmp_path, method="post", path=PULLS, status=503, count=1)["method"] == "POST"
    assert [rule["status"] for rule in proxy.armed(tmp_path)] == [503]


class _Recorder(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    seen: list[dict[str, Any]] = []
    origin = ""

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
        if self.path.endswith("/git-upload-pack"):
            answer, kind = f"PACK {self.origin}".encode(), "application/x-git-upload-pack-result"
        else:
            pull = f"{self.origin}/a/b/pull/7"
            answer, kind = json.dumps({"number": 7, "html_url": pull}).encode(), "application/json"
        self.send_response(201 if self.command == "POST" else 200)
        self.send_header("Content-Type", kind)
        self.send_header("X-Upstream", "yes")
        self.send_header("Location", f"{self.origin}/a/b/pull/7")
        self.send_header("Content-Length", str(len(answer)))
        self.end_headers()
        self.wfile.write(answer)

    do_GET = _handle
    do_POST = _handle


@pytest.fixture
def served(tmp_path: Path) -> Iterator[tuple[Path, int, ssl.SSLContext, str]]:
    if shutil.which("openssl") is None:
        pytest.fail("openssl is required to serve the proxy over TLS")
    _Recorder.seen = []
    upstream = ThreadingHTTPServer(("127.0.0.1", 0), _Recorder)
    _Recorder.origin = f"http://127.0.0.1:{upstream.server_address[1]}"
    state = tmp_path / "state"
    ca, certificate, key = proxy.make_certificates(state, "127.0.0.1")
    server = ThreadingHTTPServer(("127.0.0.1", 0), BaseHTTPRequestHandler)
    public = f"https://127.0.0.1:{server.server_address[1]}"
    server.RequestHandlerClass = proxy.build_handler(
        state=state, upstream=_Recorder.origin, public=public, upstream_context=None
    )
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.minimum_version = ssl.TLSVersion.TLSv1_2
    context.load_cert_chain(certificate, key)
    server.socket = context.wrap_socket(server.socket, server_side=True)
    threads = [threading.Thread(target=s.serve_forever, daemon=True) for s in (upstream, server)]
    for thread in threads:
        thread.start()
    try:
        yield state, server.server_address[1], ssl.create_default_context(cafile=str(ca)), public
    finally:
        for s in (server, upstream):
            s.shutdown()
            s.server_close()


def _call(
    port: int,
    trust: ssl.SSLContext,
    method: str,
    path: str,
    body: str = "",
    *,
    chunked: bool = False,
) -> tuple[int, dict[str, str], bytes]:
    connection = http.client.HTTPSConnection("127.0.0.1", port, context=trust, timeout=10)
    headers = {"Authorization": "Bearer secret", "Content-Type": "application/json"}
    try:
        if chunked:
            connection.putrequest(method, path)
            for name, value in {**headers, "Transfer-Encoding": "chunked"}.items():
                connection.putheader(name, value)
            connection.endheaders()
            half = len(body) // 2
            for piece in (body[:half], body[half:]):
                connection.send(f"{len(piece.encode()):x}\r\n".encode() + piece.encode() + b"\r\n")
            connection.send(b"0\r\n\r\n")
        else:
            connection.request(method, path, body=body.encode() or None, headers=headers)
        response = connection.getresponse()
        return response.status, dict(response.getheaders()), response.read()
    finally:
        connection.close()


def test_the_served_proxy_passes_through_and_fails_exactly_the_armed_calls(
    served: tuple[Path, int, ssl.SSLContext, str],
) -> None:
    state, port, trust, public = served
    status, headers, body = _call(port, trust, "GET", "/api/v3/repos/a/b/pulls?state=all")
    assert status == 200 and headers.get("X-Upstream") == "yes"
    # The forge's own URLs come back under the proxy, so Curie's host checks hold.
    assert json.loads(body) == {"number": 7, "html_url": f"{public}/a/b/pull/7"}
    assert headers.get("Location") == f"{public}/a/b/pull/7"
    assert _Recorder.seen[-1]["path"] == "/api/v3/repos/a/b/pulls?state=all"
    assert _Recorder.seen[-1]["authorization"] == "Bearer secret"
    assert _Recorder.seen[-1]["host"] != f"127.0.0.1:{port}"

    # Git smart HTTP passes through, its binary answer untouched.
    status, _headers, body = _call(port, trust, "POST", "/a/b.git/git-upload-pack", "want")
    assert (status, body) == (201, f"PACK {_Recorder.origin}".encode())
    assert _Recorder.seen[-1]["body"] == "want"

    proxy.arm(state, method="POST", path=PULLS, status=502, count=2)
    before = len(_Recorder.seen)
    for _ in range(2):
        status, _headers, body = _call(port, trust, "POST", "/api/v3/repos/a/b/pulls", "{}")
        assert status == 502 and json.loads(body)["message"] == "Server Error"
    assert len(_Recorder.seen) == before, "an injected failure reached the upstream"
    assert [(r["remaining"], r["answered"]) for r in proxy.armed(state)] == [(0, 2)]

    status, _headers, body = _call(
        port, trust, "POST", "/api/v3/repos/a/b/pulls", '{"title":"chunked"}', chunked=True
    )
    assert status == 201 and json.loads(body)["number"] == 7
    assert _Recorder.seen[-1] == {
        "method": "POST",
        "path": "/api/v3/repos/a/b/pulls",
        "body": '{"title":"chunked"}',
        "authorization": "Bearer secret",
        "host": _Recorder.seen[-1]["host"],
    }
