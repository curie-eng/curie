"""The AgentMail client dials only addresses the pod's egress policy admits (#2731).

The chart's NetworkPolicy admits a fixed CIDR snapshot of AgentMail's CloudFront
edges, but the edge DNS returns rotates. When DNS hands the adapter a rotated
edge, the cluster rejects the dial with ECONNREFUSED and discovery dies silently.
These tests drive a REAL socket: a local HTTP server stands in for the admitted
edge, and only name resolution is steered, so the connection path under test is
the one the adapter actually uses.
"""

from __future__ import annotations

import ipaddress
import json
import socket
import threading
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

import pytest
from curie_mail_adapter.config import MailAdapterConfig

HOST = "agentmail.invalid"
ROTATED_EDGE = "203.0.113.9"
LIST_BODY = {"messages": [], "count": 0}


class _ListHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        payload = json.dumps(LIST_BODY).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def do_POST(self) -> None:
        self.do_GET()

    def log_message(self, format: str, *args: Any) -> None:
        return


@pytest.fixture
def edge() -> Iterator[int]:
    server = ThreadingHTTPServer(("127.0.0.1", 0), _ListHandler)
    server.daemon_threads = True
    threading.Thread(target=server.serve_forever, daemon=True).start()
    yield server.server_address[1]
    server.shutdown()
    server.server_close()


class _Dns:
    """Steer resolution of HOST only, and record every address a socket dials."""

    def __init__(self, monkeypatch: pytest.MonkeyPatch, answers: list[str]) -> None:
        self.answers = answers
        self.dialed: list[str] = []
        real_getaddrinfo = socket.getaddrinfo
        real_connect = socket.socket.connect

        def fake_getaddrinfo(host: Any, port: Any, *args: Any, **kwargs: Any) -> Any:
            if host == HOST:
                port_number = int(port) if port is not None else 0
                return [
                    (socket.AF_INET, socket.SOCK_STREAM, 6, "", (address, port_number))
                    for address in self.answers
                ]
            return real_getaddrinfo(host, port, *args, **kwargs)

        dialed = self.dialed

        def recording_connect(sock: socket.socket, address: Any) -> Any:
            if isinstance(address, tuple):
                dialed.append(str(address[0]))
            return real_connect(sock, address)

        # A dial to the unroutable rotated edge would otherwise hang for the full
        # production timeout before failing; bound it so a regression fails fast.
        from curie_mail_adapter import agentmail

        monkeypatch.setattr(agentmail, "HTTP_TIMEOUT_SECONDS", 2.0)
        monkeypatch.setattr(socket, "getaddrinfo", fake_getaddrinfo)
        monkeypatch.setattr(socket.socket, "connect", recording_connect)


def _config(tmp_path: Path, port: int, cidrs: list[str], host: str = HOST) -> MailAdapterConfig:
    return MailAdapterConfig(
        agentmail_api_key="am-key",
        agentmail_inbox="inbox@agentmail.to",
        agentmail_base_url=f"http://{host}:{port}/v0",
        api_base_url="http://127.0.0.1:1",
        channel_token="chn",
        egress_secret="egr",
        allowed_senders=("*",),
        state_path=str(tmp_path / "state.sqlite3"),
        agentmail_egress_cidrs=cidrs,
    )


# -- config ------------------------------------------------------------------


def test_egress_cidrs_parse_from_env_comma_separated(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(
        "CURIE_MAIL_AGENTMAIL_EGRESS_CIDRS", " 18.160.41.105/32 ,10.0.0.0/8,, 2001:db8::/32 "
    )
    config = MailAdapterConfig()
    assert list(config.agentmail_egress_cidrs) == [
        ipaddress.ip_network("18.160.41.105/32"),
        ipaddress.ip_network("10.0.0.0/8"),
        ipaddress.ip_network("2001:db8::/32"),
    ]
    for network in config.agentmail_egress_cidrs:
        assert isinstance(network, (ipaddress.IPv4Network, ipaddress.IPv6Network))


@pytest.mark.parametrize("raw", [None, "", "  ", ","])
def test_egress_cidrs_default_empty(monkeypatch: pytest.MonkeyPatch, raw: str | None) -> None:
    if raw is None:
        monkeypatch.delenv("CURIE_MAIL_AGENTMAIL_EGRESS_CIDRS", raising=False)
    else:
        monkeypatch.setenv("CURIE_MAIL_AGENTMAIL_EGRESS_CIDRS", raw)
    assert list(MailAdapterConfig().agentmail_egress_cidrs) == []


def test_invalid_egress_cidr_is_a_validation_error(monkeypatch: pytest.MonkeyPatch) -> None:
    from pydantic import ValidationError

    monkeypatch.setenv("CURIE_MAIL_AGENTMAIL_EGRESS_CIDRS", "18.160.41.105/32,not-a-cidr")
    with pytest.raises(ValidationError):
        MailAdapterConfig()


# -- pinned dialing over a real socket -----------------------------------------


def test_rotated_edge_is_never_dialed_admitted_address_is(
    edge: int, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from curie_mail_adapter.agentmail import AgentMailClient

    dns = _Dns(monkeypatch, [ROTATED_EDGE])
    client = AgentMailClient(_config(tmp_path, edge, ["127.0.0.1/32"]))

    status, body = client.list_messages(20)

    assert status == 200, f"the pinned dial did not reach the admitted edge: {body!r}"
    assert body == LIST_BODY
    assert ROTATED_EDGE not in dns.dialed, f"a non-admitted edge was dialed: {dns.dialed}"
    assert "127.0.0.1" in dns.dialed


def test_admitted_address_is_preferred_over_non_admitted(
    edge: int, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from curie_mail_adapter.agentmail import AgentMailClient

    dns = _Dns(monkeypatch, [ROTATED_EDGE, "127.0.0.1"])
    client = AgentMailClient(_config(tmp_path, edge, ["127.0.0.0/8"]))

    status, body = client.list_messages(20)

    assert status == 200, body
    assert ROTATED_EDGE not in dns.dialed, f"a non-admitted edge was dialed: {dns.dialed}"


def test_without_cidrs_resolution_is_used_unchanged(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """No pinning configured: the client dials whatever DNS says, as before.

    DNS answers 127.0.0.1 on a closed port, so the unpinned dial fails fast with
    status 0, while the server that WOULD answer is never reachable by the name.
    """
    from curie_mail_adapter.agentmail import AgentMailClient

    closed = socket.socket()
    closed.bind(("127.0.0.1", 0))
    closed_port = closed.getsockname()[1]
    closed.close()

    dns = _Dns(monkeypatch, ["127.0.0.1"])
    config = _config(tmp_path, closed_port, [])
    assert list(config.agentmail_egress_cidrs) == []
    status, _ = AgentMailClient(config).list_messages(20)

    assert status == 0
    assert dns.dialed == ["127.0.0.1"]


def test_no_admitted_address_reports_status_zero(
    edge: int, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from curie_mail_adapter.agentmail import AgentMailClient

    dns = _Dns(monkeypatch, [ROTATED_EDGE])
    client = AgentMailClient(_config(tmp_path, edge, ["10.0.0.0/8"]))

    status, body = client.list_messages(20)

    assert status == 0
    assert isinstance(body, dict) and "admitted" in str(body.get("error", "")), body
    assert ROTATED_EDGE not in dns.dialed


def test_module_request_is_not_pinned(edge: int, monkeypatch: pytest.MonkeyPatch) -> None:
    """Platform API posts go through the module opener, never the pinned one."""
    from curie_mail_adapter import agentmail

    monkeypatch.setenv("CURIE_MAIL_AGENTMAIL_EGRESS_CIDRS", "10.0.0.0/8")
    dns = _Dns(monkeypatch, ["127.0.0.1"])
    result = agentmail.request("POST", f"http://{HOST}:{edge}/v1/ingress", {"x": 1})

    assert result.status == 200, result.body
    assert dns.dialed == ["127.0.0.1"]
