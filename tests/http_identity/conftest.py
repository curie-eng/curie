"""Owned, synthetic transport fixtures. No API application/DB fixture is imported.

@spec HTTP-IDENTITY-TRANSPORT-1 HTTP-IDENTITY-TRANSPORT-2
@spec HTTP-IDENTITY-TRANSPORT-3 HTTP-IDENTITY-TRANSPORT-4 HTTP-IDENTITY-TRANSPORT-5
"""

from __future__ import annotations

import asyncio
import contextlib
import copy
import importlib
import ipaddress
import json
import ssl
import threading
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any

import pytest

VECTORS = json.loads(
    (Path(__file__).resolve().parents[1] / "vectors/http-identity-v1.json").read_text()
)
TOKEN = "Synthetic.Transport.Secret+/=works".replace("=works", "==")
MARKER = "SyntheticProviderBodyMarker"
Handler = Callable[[asyncio.StreamReader, asyncio.StreamWriter], Awaitable[None]]


def transport_api() -> ModuleType:
    """@spec HTTP-IDENTITY-TRANSPORT-1: absent code is an execution assertion."""
    name = "curie_api.identity.http_transport"
    try:
        module = importlib.import_module(name)
    except ModuleNotFoundError as exc:
        assert exc.name != name, "HTTP identity transport is not implemented"
        raise
    for symbol in (
        "LookupTarget",
        "ProviderBearer",
        "HttpIdentityClient",
        "HttpIdentityObservation",
        "CloseReport",
        "IdentityTransportInternalError",
        "IdentityShutdownIncomplete",
        "IdentityClientClosed",
    ):
        assert hasattr(module, symbol), f"HTTP identity transport entry point missing: {symbol}"
    return module


def facts_api() -> ModuleType:
    """@spec HTTP-IDENTITY-TRANSPORT-1: consume the normative T1 public API."""
    return importlib.import_module("curie_api.schemas.http_identity")


def request() -> Any:
    """@spec HTTP-IDENTITY-TRANSPORT-1."""
    return facts_api().LookupRequestV1.model_validate(copy.deepcopy(VECTORS["request"]))


def limits(**changes: int) -> Any:
    """@spec HTTP-IDENTITY-TRANSPORT-1."""
    return facts_api().FreshnessLimits.model_validate(
        {
            "max_identity_age_ms": 60_000,
            "max_membership_age_ms": 60_000,
            "max_clock_skew_ms": 0,
            **changes,
        }
    )


def response(*, outcome: str = "resolved") -> bytes:
    """@spec HTTP-IDENTITY-TRANSPORT-2: fresh synthetic facts, never a live identity."""
    index = {"resolved": 0, "inactive": 3, "unavailable": 11, "unresolved": 7}[outcome]
    body = copy.deepcopy(VECTORS["cases"][index]["response"])
    stamp = (datetime.now(UTC) - timedelta(milliseconds=20)).isoformat()
    if body["identity_observation"]:
        body["identity_observation"]["as_of"] = stamp
    if body["membership"]:
        body["membership"]["observation"]["as_of"] = stamp
    return json.dumps(body, separators=(",", ":")).encode()


def target(module: ModuleType, endpoint: str, **changes: Any) -> Any:
    """@spec HTTP-IDENTITY-TRANSPORT-1."""
    captured = request()
    return module.LookupTarget(
        **{
            "endpoint": endpoint,
            "tenant_id": captured.scope.tenant_id,
            "expected_issuer": captured.scope.expected_issuer,
            "authority_version": captured.authority_version,
            **changes,
        }
    )


def refusal(value: Any, code: str) -> None:
    """@spec HTTP-IDENTITY-TRANSPORT-2: no raw provider information escapes."""
    assert isinstance(value, facts_api().FactsRefusal)
    assert value.model_dump(mode="json") == {"code": code}
    assert not hasattr(value, "facts") and not hasattr(value, "response_bytes")


def report_fields(report: Any) -> tuple[Any, ...]:
    """@spec HTTP-IDENTITY-TRANSPORT-4: compare the contract, not a model's equality method."""
    return tuple(
        getattr(report, field)
        for field in (
            "state",
            "pending_exchanges",
            "pending_parsers",
            "worker_joined",
            "close_failed",
        )
    )


def wire(
    body: bytes = b"",
    *,
    status: bytes = b"200 OK",
    headers: list[tuple[bytes, bytes]] | None = None,
    framing: str = "length",
) -> bytes:
    """@spec HTTP-IDENTITY-TRANSPORT-2: literal wire framing, no HTTP client fake."""
    fields = [(b"Content-Type", b"application/json")] if headers is None else list(headers)
    if framing == "length":
        fields.append((b"Content-Length", str(len(body)).encode()))
    elif framing == "chunked":
        fields.append((b"Transfer-Encoding", b"chunked"))
        body = (
            b"".join(
                f"{len(part):x}\r\n".encode() + part + b"\r\n"
                for part in (body[:32_000], body[32_000:])
                if part
            )
            + b"0\r\n\r\n"
        )
    else:
        assert framing == "eof"
        fields.append((b"Connection", b"close"))
    return (
        b"HTTP/1.1 "
        + status
        + b"\r\n"
        + b"".join(key + b": " + value + b"\r\n" for key, value in fields)
        + b"\r\n"
        + body
    )


async def read_request(reader: asyncio.StreamReader) -> tuple[bytes, dict[str, str], bytes]:
    """@spec HTTP-IDENTITY-TRANSPORT-1: observe the actual received request."""
    raw = await reader.readuntil(b"\r\n\r\n")
    first, *lines = raw[:-4].split(b"\r\n")
    headers = {}
    for line in lines:
        key, value = line.split(b":", 1)
        headers[key.decode().lower()] = value.decode().strip()
    body = await reader.readexactly(int(headers.get("content-length", "0")))
    return first, headers, body


class Listener:
    """@spec HTTP-IDENTITY-TRANSPORT-1: one fixture-owned loopback listener."""

    def __init__(self) -> None:
        self.server: asyncio.Server | None = None
        self.tls = False
        self.connections = 0
        self.requests: list[tuple[bytes, dict[str, str], bytes]] = []
        self.arrived = asyncio.Event()

    @property
    def url(self) -> str:
        """@spec HTTP-IDENTITY-TRANSPORT-1."""
        assert self.server is not None
        port = self.server.sockets[0].getsockname()[1]
        scheme = "https" if self.tls else "http"
        return f"{scheme}://127.0.0.1:{port}/lookup"


class Rig:
    """@spec HTTP-IDENTITY-TRANSPORT-4: register ownership before startup."""

    def __init__(self, root: Path) -> None:
        self.root = root
        self.listeners: list[Listener] = []
        self.handlers: set[asyncio.Task[Any]] = set()
        self.writers: set[asyncio.StreamWriter] = set()
        self.clients: list[Any] = []
        self.tasks: list[asyncio.Task[Any]] = []
        self.releases: list[threading.Event | asyncio.Event] = []
        self.errors: list[BaseException] = []
        self.thread_ids = {thread.ident for thread in threading.enumerate()}

    def event(self, *, thread: bool = False) -> Any:
        """@spec HTTP-IDENTITY-TRANSPORT-4: teardown releases every artificial stall."""
        event = threading.Event() if thread else asyncio.Event()
        self.releases.append(event)
        return event

    def client(self, module: ModuleType, **kwargs: Any) -> Any:
        """@spec HTTP-IDENTITY-TRANSPORT-4."""
        client = module.HttpIdentityClient(**kwargs)
        self.clients.append(client)
        return client

    def task(self, awaitable: Awaitable[Any]) -> asyncio.Task[Any]:
        """@spec HTTP-IDENTITY-TRANSPORT-4."""
        task = asyncio.create_task(awaitable)
        self.tasks.append(task)
        return task

    async def listen(
        self,
        handler: Handler | None = None,
        *,
        payload: bytes | None = None,
        tls: ssl.SSLContext | None = None,
        parse_request: bool = True,
    ) -> Listener:
        """@spec HTTP-IDENTITY-TRANSPORT-1 HTTP-IDENTITY-TRANSPORT-4."""
        listener = Listener()
        listener.tls = tls is not None
        self.listeners.append(listener)

        async def connected(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            """@spec HTTP-IDENTITY-TRANSPORT-4."""
            task = asyncio.current_task()
            assert task is not None
            self.handlers.add(task)
            self.writers.add(writer)
            listener.connections += 1
            listener.arrived.set()
            try:
                if parse_request:
                    listener.requests.append(await read_request(reader))
                if handler is not None:
                    await handler(reader, writer)
                else:
                    writer.write(payload if payload is not None else wire(response()))
                    await writer.drain()
            except (ConnectionError, asyncio.IncompleteReadError):
                pass  # The peer is allowed to abandon its own exchange.
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 -- re-raised by fixture, not discarded.
                self.errors.append(exc)
            finally:
                writer.close()
                try:
                    with contextlib.suppress(ConnectionError, ssl.SSLError):
                        await writer.wait_closed()
                finally:
                    self.writers.discard(writer)
                    self.handlers.discard(task)

        listener.server = await asyncio.start_server(
            connected, "127.0.0.1", 0, ssl=tls, ssl_handshake_timeout=1 if tls else None
        )
        return listener

    def certificates(self, *, wrong_host: bool = False) -> tuple[ssl.SSLContext, ssl.SSLContext]:
        """@spec HTTP-IDENTITY-TRANSPORT-1: ephemeral private CA; hostname verification stays on."""
        from cryptography import x509
        from cryptography.hazmat.primitives import hashes, serialization
        from cryptography.hazmat.primitives.asymmetric import rsa
        from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID

        now = datetime.now(UTC)
        ca_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "Synthetic transport test CA")])
        ca = (
            x509.CertificateBuilder()
            .subject_name(name)
            .issuer_name(name)
            .public_key(ca_key.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(now - timedelta(minutes=1))
            .not_valid_after(now + timedelta(days=1))
            .add_extension(x509.BasicConstraints(ca=True, path_length=0), critical=True)
            .add_extension(x509.SubjectKeyIdentifier.from_public_key(ca_key.public_key()), False)
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
            .sign(ca_key, hashes.SHA256())
        )
        key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        host = "127.0.0.2" if wrong_host else "127.0.0.1"
        cert = (
            x509.CertificateBuilder()
            .subject_name(
                x509.Name(
                    [
                        x509.NameAttribute(NameOID.COMMON_NAME, "Synthetic transport listener"),
                    ]
                )
            )
            .issuer_name(name)
            .public_key(key.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(now - timedelta(minutes=1))
            .not_valid_after(now + timedelta(days=1))
            .add_extension(
                x509.SubjectAlternativeName(
                    [
                        x509.IPAddress(ipaddress.ip_address(host)),
                    ]
                ),
                critical=False,
            )
            .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
            .add_extension(
                x509.AuthorityKeyIdentifier.from_issuer_public_key(ca_key.public_key()), False
            )
            .add_extension(x509.ExtendedKeyUsage([ExtendedKeyUsageOID.SERVER_AUTH]), False)
            .sign(ca_key, hashes.SHA256())
        )
        ca_path, cert_path, key_path = [
            self.root / name for name in ("ca.pem", "cert.pem", "key.pem")
        ]
        ca_path.write_bytes(ca.public_bytes(serialization.Encoding.PEM))
        cert_path.write_bytes(cert.public_bytes(serialization.Encoding.PEM))
        key_path.write_bytes(
            key.private_bytes(
                serialization.Encoding.PEM,
                serialization.PrivateFormat.PKCS8,
                serialization.NoEncryption(),
            )
        )
        key_path.chmod(0o600)
        server = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        server.load_cert_chain(cert_path, key_path)
        client = ssl.create_default_context(cafile=str(ca_path))
        assert client.verify_mode == ssl.CERT_REQUIRED and client.check_hostname
        return server, client

    async def cleanup(self) -> None:
        """@spec HTTP-IDENTITY-TRANSPORT-4: eventual drain is distinct from result timing."""
        for release in self.releases:
            release.set()
        for task in self.tasks:
            if not task.done():
                task.cancel()
        if self.tasks:
            await asyncio.gather(*self.tasks, return_exceptions=True)
        try:
            for client in self.clients:
                async with asyncio.timeout(3):
                    while True:
                        report = await client.aclose()
                        if report.state == "closed":
                            assert report.pending_exchanges == report.pending_parsers == 0
                            assert report.worker_joined and not report.close_failed
                            break
                        await asyncio.sleep(0.01)
        finally:
            # A product shutdown defect must not skip fixture listener cleanup.
            for listener in self.listeners:
                if listener.server is not None:
                    listener.server.close()
                    await listener.server.wait_closed()
                    assert not listener.server.is_serving()
            for writer in tuple(self.writers):
                writer.close()
            for task in tuple(self.handlers):
                task.cancel()
            if self.handlers:
                await asyncio.gather(*tuple(self.handlers), return_exceptions=True)
        assert not self.handlers and not self.writers
        assert not self.errors, "owned listener handler failed"
        # A closed report must correspond to actual thread and async-task drain.
        for _ in range(50):
            new_threads = [
                t for t in threading.enumerate() if t.ident not in self.thread_ids and t.is_alive()
            ]
            if not new_threads:
                break
            await asyncio.sleep(0.01)
        assert not new_threads, "transport reported closed while an owned thread survived"
        await asyncio.sleep(0)
        pending = [
            t for t in asyncio.all_tasks() if t is not asyncio.current_task() and not t.done()
        ]
        assert not pending, "transport or fixture leaked an asynchronous task"


@pytest.fixture
def support() -> SimpleNamespace:
    """@spec HTTP-IDENTITY-TRANSPORT-1: no production import until the test body."""
    return SimpleNamespace(
        api=transport_api,
        facts=facts_api,
        request=request,
        limits=limits,
        response=response,
        target=target,
        refusal=refusal,
        report=report_fields,
        wire=wire,
        token=TOKEN,
        marker=MARKER,
    )


@pytest.fixture
def run_transport(tmp_path: Path) -> Callable[[Callable[[Rig], Awaitable[None]]], None]:
    """@spec HTTP-IDENTITY-TRANSPORT-4: cleanup is installed before any owned resource starts."""

    def run(scenario: Callable[[Rig], Awaitable[None]]) -> None:
        """@spec HTTP-IDENTITY-TRANSPORT-4."""

        async def guarded() -> None:
            """@spec HTTP-IDENTITY-TRANSPORT-4."""
            rig = Rig(tmp_path)
            try:
                async with asyncio.timeout(14):
                    await scenario(rig)
            finally:
                await rig.cleanup()

        asyncio.run(guarded())

    return run
