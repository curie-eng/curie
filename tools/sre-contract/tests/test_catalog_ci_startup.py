"""Startup readiness for catalog_ci: TCP open is not application-ready.

Handshake shape from installed mcp==2.1.1
mcp/client/streamable_http.py:_prepare_headers (accept + content-type)
and initialize POST from the 2025-06-18 streamable HTTP transport:
https://modelcontextprotocol.io/specification/2025-06-18/basic/transports
"""

from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "tools/sre-contract"))
import catalog_ci  # noqa: E402


def _surface_tools():
    surface = json.loads((ROOT / "examples/sre-bot/supported-surface.json").read_text())
    return {
        name: [
            {
                "name": tool,
                "inputSchema": {"type": "object"},
                "annotations": {"readOnlyHint": decision == "allow"},
            }
            for tool, decision in spec["tools"].items()
        ]
        for name, spec in surface["connectors"].items()
    }


class MCPHandler(BaseHTTPRequestHandler):
    """External HTTP MCP fixture. Same initialize/tools/list shape as test_contract."""

    tools = []
    calls = None

    def log_message(self, *args):
        pass

    def do_GET(self):
        self.send_response(405)
        self.end_headers()

    def do_DELETE(self):
        self.send_response(204)
        self.end_headers()

    def do_POST(self):
        payload = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        if self.calls is not None:
            self.calls.append(payload.get("method"))
        if "id" not in payload:
            self.send_response(202)
            self.end_headers()
            return
        result = (
            {
                "protocolVersion": payload["params"]["protocolVersion"],
                "capabilities": {"tools": {}},
                "serverInfo": {"name": "fixture", "version": "1"},
            }
            if payload["method"] == "initialize"
            else {"tools": self.tools}
        )
        body = json.dumps({"jsonrpc": "2.0", "id": payload["id"], "result": result}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


class StatusHandler(BaseHTTPRequestHandler):
    status = 500
    body = b'{"error":"fixture"}'

    def log_message(self, *args):
        pass

    def do_POST(self):
        self.rfile.read(int(self.headers.get("Content-Length", "0") or 0))
        self.send_response(self.status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(self.body)))
        self.end_headers()
        self.wfile.write(self.body)

    def do_GET(self):
        self.send_response(405)
        self.end_headers()

    def do_DELETE(self):
        self.send_response(204)
        self.end_headers()


def _http(handler):
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    url = f"http://127.0.0.1:{server.server_port}/mcp"
    return server, thread, url


def _close_http(server, thread):
    server.shutdown()
    thread.join(timeout=2)
    server.server_close()


class TcpThenHttp:
    """Accept TCP immediately; speak HTTP only after `delay` seconds.

    Models a published Docker port that accepts before the Python app serves.
    """

    def __init__(self, delay, tools, drain=False):
        self.delay = delay
        self.tools = tools
        self.drain = drain
        self.calls = []
        self._sock = socket.socket()
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._sock.bind(("127.0.0.1", 0))
        self._sock.listen(16)
        self.port = self._sock.getsockname()[1]
        self.url = f"http://127.0.0.1:{self.port}/mcp"
        self._stop = threading.Event()
        self._http = None
        self._thread = threading.Thread(target=self._run, daemon=True)

    def start(self):
        self._thread.start()
        return self

    def close(self):
        self._stop.set()
        try:
            socket.create_connection(("127.0.0.1", self.port), timeout=0.2).close()
        except OSError:
            pass
        if self._http is not None:
            self._http.shutdown()
        self._sock.close()
        self._thread.join(timeout=2)

    def _run(self):
        deadline = time.monotonic() + self.delay
        self._sock.settimeout(0.05)
        while time.monotonic() < deadline and not self._stop.is_set():
            try:
                conn, _ = self._sock.accept()
            except TimeoutError:
                continue
            except OSError:
                return
            if self.drain:
                try:
                    conn.recv(4096)
                except OSError:
                    pass
            conn.close()
        if self._stop.is_set():
            return
        tools = self.tools
        calls = self.calls

        class Handler(MCPHandler):
            pass

        Handler.tools = tools
        Handler.calls = calls

        class Reused(ThreadingHTTPServer):
            def server_bind(self):
                pass

            def server_activate(self):
                pass

        http = Reused(("127.0.0.1", self.port), Handler, bind_and_activate=False)
        http.socket = self._sock
        self._http = http
        http.serve_forever()


class BannerServer:
    def __init__(self, payload=b"SSH-2.0-OpenSSH_fixture\r\n"):
        self._sock = socket.socket()
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._sock.bind(("127.0.0.1", 0))
        self._sock.listen(4)
        self.port = self._sock.getsockname()[1]
        self.url = f"http://127.0.0.1:{self.port}/mcp"
        self._stop = threading.Event()
        self._payload = payload
        self._thread = threading.Thread(target=self._run, daemon=True)

    def start(self):
        self._thread.start()
        return self

    def close(self):
        self._stop.set()
        try:
            socket.create_connection(("127.0.0.1", self.port), timeout=0.2).close()
        except OSError:
            pass
        self._sock.close()
        self._thread.join(timeout=2)

    def _run(self):
        self._sock.settimeout(0.2)
        while not self._stop.is_set():
            try:
                conn, _ = self._sock.accept()
            except TimeoutError:
                continue
            except OSError:
                return
            try:
                conn.sendall(self._payload)
            except OSError:
                pass
            conn.close()


def test_wait_until_application_ready_ignores_tcp_until_http():
    delay = 0.4
    server = TcpThenHttp(delay, _surface_tools()["self-upgrade"]).start()
    try:
        with socket.create_connection(("127.0.0.1", server.port), timeout=1):
            pass
        start = time.monotonic()
        catalog_ci.wait_until_application_ready(server.url, timeout=5)
        elapsed = time.monotonic() - start
        assert elapsed >= delay
        catalog_ci.wait_until_application_ready(server.url, timeout=2)
    finally:
        server.close()


def test_wait_wrong_protocol_is_nonretryable():
    server = BannerServer().start()
    try:
        start = time.monotonic()
        with pytest.raises(catalog_ci.StartupNonretryable):
            catalog_ci.wait_until_application_ready(server.url, timeout=5)
        assert time.monotonic() - start < 2
    finally:
        server.close()


@pytest.mark.parametrize("status", [401, 500])
def test_wait_completed_http_is_ready_for_checker(status):
    class Handler(StatusHandler):
        pass

    Handler.status = status
    server, thread, url = _http(Handler)
    try:
        start = time.monotonic()
        catalog_ci.wait_until_application_ready(url, timeout=5)
        assert time.monotonic() - start < 2
    finally:
        _close_http(server, thread)


def test_wait_fin_hold_close_is_unreadiness_not_wrong_protocol():
    # Accept, read the initialize POST, close without HTTP (clean FIN).
    # CPython RemoteDisconnected subclasses BadStatusLine with line=="''".
    delay = 0.4
    server = TcpThenHttp(delay, _surface_tools()["self-upgrade"], drain=True).start()
    try:
        start = time.monotonic()
        catalog_ci.wait_until_application_ready(server.url, timeout=5)
        assert time.monotonic() - start >= delay
    finally:
        server.close()


def test_wait_transient_unreadiness_then_healthy():
    server = TcpThenHttp(0.3, _surface_tools()["tempo"]).start()
    try:
        catalog_ci.wait_until_application_ready(server.url, timeout=5)
    finally:
        server.close()


def test_wait_timeout_preserves_connector_startup():
    server = TcpThenHttp(30, []).start()
    try:
        with pytest.raises(TimeoutError, match="connector startup"):
            catalog_ci.wait_until_application_ready(server.url, timeout=0.6)
    finally:
        server.close()


def test_ci_driver_waits_for_application_then_invokes_checker_once(tmp_path):
    tools = _surface_tools()
    # Delay the first catalog_ci endpoint so TCP-only wait reaches the checker
    # while HTTP is still absent. A later connector can hide the race.
    delayed = TcpThenHttp(5.0, tools["kubernetes"]).start()
    servers = []
    ports = {"kubernetes": delayed.port}
    try:
        for name in ("self-upgrade", "grafana", "tempo"):

            class Handler(MCPHandler):
                pass

            Handler.tools = tools[name]
            Handler.calls = []
            server, thread, _url = _http(Handler)
            servers.append((server, thread, Handler))
            ports[name] = server.server_port

        executable = tmp_path / "docker"
        log = tmp_path / "docker.jsonl"
        executable.write_text(
            """#!/usr/bin/env python3
import json, os, sys
with open(os.environ["DOCKER_TEST_LOG"], "a") as f:
    f.write(json.dumps(sys.argv[1:]) + "\\n")
if sys.argv[1] == "port":
    ports = json.loads(os.environ["DOCKER_TEST_PORTS"])
    name = sys.argv[2]
    for connector, port in ports.items():
        if name.endswith("-" + connector):
            print(f"127.0.0.1:{port}")
            raise SystemExit(0)
    raise SystemExit(1)
raise SystemExit(0)
"""
        )
        executable.chmod(0o755)
        env = dict(
            os.environ,
            PATH=f"{tmp_path}:{os.environ['PATH']}",
            DOCKER_TEST_LOG=str(log),
            DOCKER_TEST_PORTS=json.dumps(ports),
            PYTHONPATH=str(ROOT / "packages/plugin-format/src"),
        )
        result = subprocess.run(
            [sys.executable, str(ROOT / "tools/sre-contract/catalog_ci.py")],
            env=env,
            capture_output=True,
            text=True,
            timeout=30,
        )
        assert result.returncode == 0, result.stdout + result.stderr
        payload = json.loads(result.stdout)
        assert payload["status"] == "pass"
        assert payload["mode"] == "live-catalog"
        lists = [method for method in delayed.calls if method == "tools/list"]
        # check.py live_catalogs plus assert-gates-are-live-tools; not retried.
        assert lists == ["tools/list", "tools/list"]
        commands = [json.loads(line) for line in log.read_text().splitlines()]
        assert any(cmd[0] == "run" for cmd in commands)
        assert any(cmd[:2] == ["rm", "-f"] for cmd in commands)
    finally:
        delayed.close()
        for server, thread, _handler in servers:
            _close_http(server, thread)
