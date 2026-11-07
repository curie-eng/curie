"""Run real static-pool shell supervisors in tests.

``make_shell_tls`` mints a throwaway CA and a server certificate for the
loopback names, so the worker side verifies certificates and host names
exactly as it does against a deployment. ``SupervisorProcess`` starts
``python -m curie_shell_supervisor`` as its own process (so a crash, a signal
and process-group teardown behave as they do in a shell) and waits for its
health endpoint.
"""

from __future__ import annotations

import datetime
import http.client
import io
import ipaddress
import json
import os
import select
import signal
import socket
import ssl
import subprocess
import sys
import tarfile
import threading
import time
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

CONTROL_TOKEN = "control-" + "c" * 40
FAKE_RUNNER_COMMAND = f"{sys.executable} -m curie_test_support.fake_shell_runner"


def claim_archive(
    claim: str,
    generation: int,
    env: Mapping[str, str],
    *,
    files: Mapping[str, bytes] | None = None,
    workspace: bool = False,
    attachments: str = "none",
    manifest: Mapping[str, object] | None = None,
) -> bytes:
    """A staging archive shaped like the worker's (``StaticPoolSandboxClient``)."""

    body = dict(
        manifest
        or {
            "v": 1,
            "claim": claim,
            "generation": generation,
            "env": dict(env),
            "executor_secret_names": None,
            "withheld_env": [],
            "attachments": attachments,
            "workspace": workspace,
        }
    )
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w") as tar:
        entries = {"claim.json": json.dumps(body).encode(), **(files or {})}
        for name, data in entries.items():
            info = tarfile.TarInfo(name)
            info.size = len(data)
            info.mode = 0o644
            tar.addfile(info, io.BytesIO(data))
    return buffer.getvalue()


@dataclass(frozen=True)
class ShellTls:
    ca: Path
    cert: Path
    key: Path


def make_shell_tls(directory: Path, host: str = "localhost") -> ShellTls:
    """A CA and a certificate for ``host`` and 127.0.0.1, signed by it."""

    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import ec
    from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID

    now = datetime.datetime.now(datetime.UTC)
    ca_key = ec.generate_private_key(ec.SECP256R1())
    ca_name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "test shell CA")])
    ca = (
        x509.CertificateBuilder()
        .subject_name(ca_name)
        .issuer_name(ca_name)
        .public_key(ca_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(minutes=5))
        .not_valid_after(now + datetime.timedelta(days=1))
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
        .add_extension(x509.SubjectKeyIdentifier.from_public_key(ca_key.public_key()), False)
        .sign(ca_key, hashes.SHA256())
    )
    key = ec.generate_private_key(ec.SECP256R1())
    cert = (
        x509.CertificateBuilder()
        .subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, host)]))
        .issuer_name(ca_name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(minutes=5))
        .not_valid_after(now + datetime.timedelta(days=1))
        .add_extension(
            x509.SubjectAlternativeName(
                [x509.DNSName(host), x509.IPAddress(ipaddress.ip_address("127.0.0.1"))]
            ),
            critical=False,
        )
        .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
        .add_extension(x509.ExtendedKeyUsage([ExtendedKeyUsageOID.SERVER_AUTH]), False)
        .add_extension(
            x509.AuthorityKeyIdentifier.from_issuer_public_key(ca_key.public_key()), False
        )
        .add_extension(x509.SubjectKeyIdentifier.from_public_key(key.public_key()), False)
        .sign(ca_key, hashes.SHA256())
    )
    directory.mkdir(parents=True, exist_ok=True)
    paths = ShellTls(directory / "ca.pem", directory / "cert.pem", directory / "key.pem")
    paths.ca.write_bytes(ca.public_bytes(serialization.Encoding.PEM))
    paths.cert.write_bytes(cert.public_bytes(serialization.Encoding.PEM))
    paths.key.write_bytes(
        key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
    )
    return paths


def wait_until(predicate: Callable[[], bool], timeout: float, message: str) -> None:
    """Return as soon as ``predicate`` holds; fail naming ``message`` after ``timeout``.

    For state another process owns (a supervisor, its runner child, Valkey),
    where no notification exists to wait on. It rechecks every 20 ms, so a
    test waits only as long as the other process takes.
    """
    deadline = time.monotonic() + timeout
    pause = threading.Event()
    while not predicate():
        if time.monotonic() >= deadline:
            raise AssertionError(f"timed out after {timeout}s waiting for: {message}")
        pause.wait(0.02)


def wait_gone(pid: int, timeout: float) -> bool:
    """Wait for process ``pid`` to exit, on its pidfd; a zombie counts as gone."""
    try:
        descriptor = os.pidfd_open(pid)
    except ProcessLookupError:
        return True
    try:
        poller = select.poll()
        poller.register(descriptor, select.POLLIN)
        return bool(poller.poll(int(timeout * 1000)))
    finally:
        os.close(descriptor)


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def client_context(tls: ShellTls) -> ssl.SSLContext:
    return ssl.create_default_context(cafile=str(tls.ca))


@dataclass
class SupervisorProcess:
    """One real supervisor process serving one shell slot on loopback."""

    slot: str
    tls: ShellTls
    root: Path
    runner_command: str
    control_token: str = CONTROL_TOKEN
    env: dict[str, str] = field(default_factory=dict)
    host: str = "localhost"
    port: int = field(default_factory=free_port)
    child_port: int = field(default_factory=free_port)
    process: subprocess.Popen[bytes] | None = None

    @property
    def state_root(self) -> Path:
        return self.root / "state"

    @property
    def workspace_link(self) -> Path:
        return self.root / "links" / "workspace"

    @property
    def attachments_link(self) -> Path:
        return self.root / "links" / "attachments"

    @property
    def scrub_dir(self) -> Path:
        return self.root / "scrub"

    @property
    def log_path(self) -> Path:
        return self.root / "supervisor.log"

    def start(self, timeout: float = 20.0) -> SupervisorProcess:
        for path in (self.state_root, self.workspace_link.parent, self.scrub_dir):
            path.mkdir(parents=True, exist_ok=True)
        env = {
            "PATH": os.environ.get("PATH", os.defpath),
            "OTEL_SDK_DISABLED": "true",
            "CURIE_SUPERVISOR_SLOT": self.slot,
            "CURIE_SUPERVISOR_CONTROL_TOKEN": self.control_token,
            "CURIE_SUPERVISOR_TLS_CERT_FILE": str(self.tls.cert),
            "CURIE_SUPERVISOR_TLS_KEY_FILE": str(self.tls.key),
            "CURIE_SUPERVISOR_LISTEN_HOST": "127.0.0.1",
            "CURIE_SUPERVISOR_PORT": str(self.port),
            "CURIE_SUPERVISOR_CHILD_PORT": str(self.child_port),
            "CURIE_SUPERVISOR_STATE_ROOT": str(self.state_root),
            "CURIE_SUPERVISOR_WORKSPACE_PATH": str(self.workspace_link),
            "CURIE_SUPERVISOR_ATTACHMENTS_PATH": str(self.attachments_link),
            "CURIE_SUPERVISOR_SCRUB_PATHS": str(self.scrub_dir),
            "CURIE_SUPERVISOR_RUNNER_COMMAND": self.runner_command,
            "CURIE_SUPERVISOR_SETTLE_TIMEOUT_SECONDS": "3",
            "CURIE_SUPERVISOR_STOP_GRACE_SECONDS": "3",
            **self.env,
        }
        log = self.log_path.open("ab")
        self.process = subprocess.Popen(  # noqa: S603 - fixed argv
            [sys.executable, "-m", "curie_shell_supervisor"],
            env=env,
            stdout=log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        log.close()
        process = self.process

        def answered() -> bool:
            if process.poll() is not None:
                raise RuntimeError(f"supervisor exited: {self.log_path.read_text()[-2000:]}")
            try:
                status, _ = self.request("GET", "/_curie/supervisor/healthz", token=None)
            except OSError:
                return False
            return status in (200, 503)

        wait_until(answered, timeout, f"supervisor health: {self.log_path}")
        return self

    def request(
        self,
        method: str,
        path: str,
        *,
        token: str | None,
        body: bytes | Iterable[bytes] | None = None,
        headers: dict[str, str] | None = None,
        timeout: float = 30.0,
    ) -> tuple[int, Any]:
        connection = http.client.HTTPSConnection(
            self.host, self.port, timeout=timeout, context=client_context(self.tls)
        )
        sent = dict(headers or {})
        if token is not None:
            sent["Authorization"] = f"Bearer {token}"
        try:
            connection.request(method, path, body=body, headers=sent)
            response = connection.getresponse()
            data = response.read()
        finally:
            connection.close()
        try:
            return response.status, json.loads(data) if data else None
        except ValueError:
            return response.status, data

    def shell(self) -> dict[str, Any]:
        status, body = self.request("GET", "/_curie/supervisor/v1/shell", token=self.control_token)
        assert status == 200, body
        assert isinstance(body, dict)
        return body

    def stop(self) -> None:
        process = self.process
        if process is None or process.poll() is not None:
            return
        process.send_signal(signal.SIGTERM)
        try:
            process.wait(timeout=15)
        except subprocess.TimeoutExpired:
            os.killpg(process.pid, signal.SIGKILL)
            process.wait(timeout=5)
