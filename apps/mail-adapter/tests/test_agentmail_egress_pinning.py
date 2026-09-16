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
import ssl
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


def _config(
    tmp_path: Path, port: int, cidrs: list[str], host: str = HOST, scheme: str = "http"
) -> MailAdapterConfig:
    return MailAdapterConfig(
        agentmail_api_key="am-key",
        agentmail_inbox="inbox@agentmail.to",
        agentmail_base_url=f"{scheme}://{host}:{port}/v0",
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


# -- pinned dialing over real TLS ----------------------------------------------


def _write_tls_material(tmp_path: Path, cert_hostname: str) -> tuple[Path, Path, Path]:
    """A throwaway CA and a server certificate for ``cert_hostname``, both PEM."""
    import datetime

    x509 = pytest.importorskip("cryptography.x509", reason="real-TLS pins need cryptography")
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import ec
    from cryptography.x509.oid import NameOID

    now = datetime.datetime.now(datetime.UTC)

    def name(common: str) -> Any:
        return x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, common)])

    ca_key = ec.generate_private_key(ec.SECP256R1())
    ca_cert = (
        x509.CertificateBuilder()
        .subject_name(name("curie test CA"))
        .issuer_name(name("curie test CA"))
        .public_key(ca_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(minutes=5))
        .not_valid_after(now + datetime.timedelta(hours=1))
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .add_extension(
            x509.KeyUsage(
                digital_signature=True,
                content_commitment=False,
                key_encipherment=False,
                data_encipherment=False,
                key_agreement=False,
                key_cert_sign=True,
                crl_sign=True,
                encipher_only=False,
                decipher_only=False,
            ),
            critical=True,
        )
        .add_extension(
            x509.SubjectKeyIdentifier.from_public_key(ca_key.public_key()), critical=False
        )
        .sign(ca_key, hashes.SHA256())
    )
    server_key = ec.generate_private_key(ec.SECP256R1())
    server_cert = (
        x509.CertificateBuilder()
        .subject_name(name(cert_hostname))
        .issuer_name(ca_cert.subject)
        .public_key(server_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(minutes=5))
        .not_valid_after(now + datetime.timedelta(hours=1))
        .add_extension(x509.SubjectAlternativeName([x509.DNSName(cert_hostname)]), critical=False)
        .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
        .add_extension(
            x509.ExtendedKeyUsage([x509.oid.ExtendedKeyUsageOID.SERVER_AUTH]), critical=False
        )
        .add_extension(
            x509.AuthorityKeyIdentifier.from_issuer_public_key(ca_key.public_key()),
            critical=False,
        )
        .sign(ca_key, hashes.SHA256())
    )
    ca_path = tmp_path / "ca.pem"
    cert_path = tmp_path / "server.pem"
    key_path = tmp_path / "server.key"
    ca_path.write_bytes(ca_cert.public_bytes(serialization.Encoding.PEM))
    cert_path.write_bytes(server_cert.public_bytes(serialization.Encoding.PEM))
    key_path.write_bytes(
        server_key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
    )
    return ca_path, cert_path, key_path


class _TlsEdge:
    """A real HTTPS server on 127.0.0.1 that records the SNI name each client sends."""

    def __init__(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, cert_hostname: str) -> None:
        ca_path, cert_path, key_path = _write_tls_material(tmp_path, cert_hostname)
        self.sni: list[str | None] = []
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.minimum_version = ssl.TLSVersion.TLSv1_2
        context.load_cert_chain(cert_path, key_path)

        def record_sni(_sock: Any, server_name: str | None, _context: Any) -> None:
            self.sni.append(server_name)

        context.sni_callback = record_sni
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), _ListHandler)
        self.server.daemon_threads = True
        self.server.socket = context.wrap_socket(self.server.socket, server_side=True)
        self.port = self.server.server_address[1]
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        # The client trusts the throwaway CA through the stdlib default context, which
        # keeps CERT_REQUIRED and check_hostname on: nothing in agentmail is replaced.
        monkeypatch.setenv("SSL_CERT_FILE", str(ca_path))
        monkeypatch.delenv("SSL_CERT_DIR", raising=False)

    def close(self) -> None:
        self.server.shutdown()
        self.server.server_close()


@pytest.fixture
def tls_edge_factory(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[Any]:
    edges: list[_TlsEdge] = []

    def make(cert_hostname: str) -> _TlsEdge:
        edge = _TlsEdge(tmp_path, monkeypatch, cert_hostname)
        edges.append(edge)
        return edge

    yield make
    for edge in edges:
        edge.close()


def test_https_dial_is_pinned_and_keeps_sni_and_verification(
    tls_edge_factory: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The production path: HTTPS with DNS answers wholly outside the policy."""
    from curie_mail_adapter.agentmail import AgentMailClient

    assert ssl.create_default_context().verify_mode == ssl.CERT_REQUIRED
    tls = tls_edge_factory(HOST)
    dns = _Dns(monkeypatch, [ROTATED_EDGE])
    client = AgentMailClient(_config(tmp_path, tls.port, ["127.0.0.1/32"], scheme="https"))

    status, body = client.list_messages(20)

    assert status == 200, f"the pinned HTTPS dial did not reach the admitted edge: {body!r}"
    assert body == LIST_BODY
    assert tls.sni == [HOST], f"SNI did not carry the URL hostname: {tls.sni}"
    assert ROTATED_EDGE not in dns.dialed, f"a non-admitted edge was dialed: {dns.dialed}"
    assert "127.0.0.1" in dns.dialed


def test_https_pinned_dial_still_rejects_a_certificate_for_another_host(
    tls_edge_factory: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from curie_mail_adapter.agentmail import AgentMailClient

    tls = tls_edge_factory("other.invalid")
    dns = _Dns(monkeypatch, [ROTATED_EDGE])
    client = AgentMailClient(_config(tmp_path, tls.port, ["127.0.0.1/32"], scheme="https"))

    status, body = client.list_messages(20)

    assert status == 0, f"hostname verification was not enforced: {body!r}"
    assert ROTATED_EDGE not in dns.dialed
    assert dns.dialed == ["127.0.0.1"]
