"""The one faked party in the resolver suites: an external HTTPS provider.

ADR 0239 "Consequences" 5 lets automated provider fixtures stand in for
external providers while every internal store stays real. This server is that
fixture: aiohttp over TLS on 127.0.0.1 with a certificate for ``localhost``
issued by a throwaway test CA (generated here with ``cryptography``; the CA
file is what an SDK or refresh source trusts). It records every request's
method, path, Host, every Authorization header value and the client's peer
port, and per path it can answer normally, fail, redirect, issue a token, or
block inside the handler until the test releases it.

Nothing here imports the resolver, so ``conftest.py`` can load it while the
production package does not exist yet.
"""

from __future__ import annotations

import asyncio
import datetime as dt
import ipaddress
import json
import secrets
import ssl
import threading
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from aiohttp import web
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID

# Bounds every wait the provider makes, so a hang fails instead of stalling.
GATE_WAIT_S = 30.0

ALLOWED_PATH = "/v1/items"
REDIRECT_PATH = "/v1/moved"
REDIRECT_TARGET_PATH = "/v1/moved-target"
TOKEN_PATH = "/oauth/token"


@dataclass(frozen=True)
class ProviderRequest:
    method: str
    path: str
    host: str
    authorization: tuple[str, ...]
    peer_port: int
    body: bytes


@dataclass
class Gate:
    """Parks every request to one path inside the handler until released."""

    entered: threading.Event = field(default_factory=threading.Event)
    released: threading.Event = field(default_factory=threading.Event)

    def wait_entered(self, timeout: float = GATE_WAIT_S) -> None:
        assert self.entered.wait(timeout), "the provider was never called"

    def release(self) -> None:
        self.released.set()


def _issue_certificates(directory: Path) -> tuple[Path, Path, Path]:
    """A CA, and a leaf for localhost and 127.0.0.1 signed by it."""

    now = dt.datetime.now(dt.UTC)
    ca_key = ec.generate_private_key(ec.SECP256R1())
    ca_name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "curie resolver test CA")])
    ca_ski = x509.SubjectKeyIdentifier.from_public_key(ca_key.public_key())
    ca = (
        x509.CertificateBuilder()
        .subject_name(ca_name)
        .issuer_name(ca_name)
        .public_key(ca_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - dt.timedelta(minutes=5))
        .not_valid_after(now + dt.timedelta(hours=2))
        .add_extension(x509.BasicConstraints(ca=True, path_length=0), critical=True)
        .add_extension(
            x509.KeyUsage(
                digital_signature=False,
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
        .add_extension(ca_ski, critical=False)
        .sign(ca_key, hashes.SHA256())
    )

    leaf_key = ec.generate_private_key(ec.SECP256R1())
    leaf = (
        x509.CertificateBuilder()
        .subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "localhost")]))
        .issuer_name(ca_name)
        .public_key(leaf_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - dt.timedelta(minutes=5))
        .not_valid_after(now + dt.timedelta(hours=2))
        .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
        .add_extension(
            x509.KeyUsage(
                digital_signature=True,
                content_commitment=False,
                key_encipherment=False,
                data_encipherment=False,
                key_agreement=False,
                key_cert_sign=False,
                crl_sign=False,
                encipher_only=False,
                decipher_only=False,
            ),
            critical=True,
        )
        .add_extension(x509.ExtendedKeyUsage([ExtendedKeyUsageOID.SERVER_AUTH]), critical=False)
        .add_extension(
            x509.SubjectAlternativeName(
                [x509.DNSName("localhost"), x509.IPAddress(ipaddress.ip_address("127.0.0.1"))]
            ),
            critical=False,
        )
        .add_extension(
            x509.SubjectKeyIdentifier.from_public_key(leaf_key.public_key()), critical=False
        )
        .add_extension(
            x509.AuthorityKeyIdentifier.from_issuer_subject_key_identifier(ca_ski), critical=False
        )
        .sign(ca_key, hashes.SHA256())
    )

    ca_file = directory / "ca.pem"
    cert_file = directory / "leaf.pem"
    key_file = directory / "leaf.key"
    ca_file.write_bytes(ca.public_bytes(serialization.Encoding.PEM))
    cert_file.write_bytes(
        leaf.public_bytes(serialization.Encoding.PEM) + ca.public_bytes(serialization.Encoding.PEM)
    )
    key_file.write_bytes(
        leaf_key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
    )
    key_file.chmod(0o600)
    return ca_file, cert_file, key_file


class FakeProvider:
    """An HTTPS provider on its own thread and event loop."""

    def __init__(self, directory: Path) -> None:
        directory.mkdir(parents=True, exist_ok=True)
        self.ca_file, cert_file, key_file = _issue_certificates(directory)
        self._server_ssl = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
        self._server_ssl.load_cert_chain(cert_file, key_file)
        self._lock = threading.Lock()
        self._requests: list[ProviderRequest] = []
        self._gates: dict[str, Gate] = {}
        self._failing: set[str] = set()
        self.issued: list[str] = []
        self._hooks: dict[str, Callable[[], None]] = {}
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=self._loop.run_forever, daemon=True)
        self._thread.start()
        self._runner: web.AppRunner | None = None
        self.port = self._wait(self._start())
        self.origin = f"https://localhost:{self.port}"
        self.upper_origin = f"https://LOCALHOST:{self.port}"
        self.undeclared_origin = f"https://127.0.0.1:{self.port}"

    def _wait(self, coroutine: Any, timeout: float = GATE_WAIT_S) -> Any:
        return asyncio.run_coroutine_threadsafe(coroutine, self._loop).result(timeout)

    async def _start(self) -> int:
        app = web.Application()
        app.router.add_route("*", "/{tail:.*}", self._handle)
        # No access log: the provider must not be a place a token could leak to.
        self._runner = web.AppRunner(app, access_log=None)
        await self._runner.setup()
        site = web.TCPSite(self._runner, "127.0.0.1", 0, ssl_context=self._server_ssl)
        await site.start()
        addresses = self._runner.addresses
        assert addresses, "the provider bound no address"
        return int(addresses[0][1])

    async def _handle(self, request: web.Request) -> web.Response:
        body = await request.read()
        peer = request.transport.get_extra_info("peername") if request.transport else None
        record = ProviderRequest(
            method=request.method,
            path=request.path,
            host=request.headers.get("Host", ""),
            authorization=tuple(request.headers.getall("Authorization", [])),
            peer_port=int(peer[1]) if peer else -1,
            body=body,
        )
        with self._lock:
            self._requests.append(record)
            gate = self._gates.get(request.path)
            failing = request.path in self._failing
            hook = self._hooks.get(request.path)
        if gate is not None:
            gate.entered.set()
            released = await asyncio.get_running_loop().run_in_executor(
                None, gate.released.wait, GATE_WAIT_S
            )
            if not released:
                return web.json_response({"error": "gate_timeout"}, status=504)
        if hook is not None:
            await asyncio.get_running_loop().run_in_executor(None, hook)
        if failing:
            return web.json_response({"error": "server_error"}, status=500)
        if request.path == REDIRECT_PATH:
            return web.Response(
                status=302,
                headers={"Location": f"https://localhost:{self.port}{REDIRECT_TARGET_PATH}"},
            )
        if request.path == TOKEN_PATH:
            token = f"provider-issued-{secrets.token_hex(12)}"
            with self._lock:
                self.issued.append(token)
            return web.Response(
                status=200,
                body=json.dumps(
                    {"access_token": token, "token_type": "bearer", "expires_in": 3600}
                ).encode(),
                content_type="application/json",
            )
        return web.json_response({"ok": True, "path": request.path})

    # --- controls ------------------------------------------------------------

    def block(self, path: str) -> Gate:
        gate = Gate()
        with self._lock:
            self._gates[path] = gate
        return gate

    def unblock(self, path: str) -> None:
        with self._lock:
            gate = self._gates.pop(path, None)
        if gate is not None:
            gate.release()

    def fail(self, path: str, failing: bool = True) -> None:
        with self._lock:
            if failing:
                self._failing.add(path)
            else:
                self._failing.discard(path)

    def on_request(self, path: str, hook: Callable[[], None] | None) -> None:
        """Run ``hook`` (blocking, off the loop) inside the handler, before answering."""

        with self._lock:
            if hook is None:
                self._hooks.pop(path, None)
            else:
                self._hooks[path] = hook

    @property
    def requests(self) -> list[ProviderRequest]:
        with self._lock:
            return list(self._requests)

    def calls(self, path: str) -> list[ProviderRequest]:
        return [request for request in self.requests if request.path == path]

    def client_ssl(self) -> ssl.SSLContext:
        return ssl.create_default_context(cafile=str(self.ca_file))

    def close(self) -> None:
        with self._lock:
            gates = list(self._gates.values())
        for gate in gates:
            gate.release()
        try:
            if self._runner is not None:
                self._wait(self._runner.cleanup())
        finally:
            self._loop.call_soon_threadsafe(self._loop.stop)
            self._thread.join(timeout=10)
            if not self._thread.is_alive():
                self._loop.close()
