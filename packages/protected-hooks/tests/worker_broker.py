"""Disposable TLS Valkey and owned worker principal, @spec PROTECTED-HOOK-LANE-3
PROTECTED-HOOK-LANE-6 PROTECTED-HOOK-LANE-7.

The worker role realisation (S1 of the protected worker lane) needs a pinned Valkey
whose only standing users are the default user (disabled) and a provisioner, so the
worker principal under test is installed by the test provisioner from the product's
``worker_acl_rules("worker")`` and nothing else. The container is owned by this module
(name prefix ``curie-check-pworker-``), never the shared compose stack, and only that
exact container is removed. Fixture ids are placeholders; no real deployment data.
"""

from __future__ import annotations

import os
import secrets
import shutil
import socket
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID
from redis import Redis
from redis import asyncio as aioredis
from redis.backoff import NoBackoff
from redis.retry import Retry

from . import admission_broker as helpers

FixtureSecret = helpers.FixtureSecret
fail_safely = helpers.fail_safely

# The worker's own key family and the exact broker names the lane uses.
LANE = "protected:lane"
WORKER_PREFIX = LANE + ":worker"
SANDBOX_PREFIX = LANE + ":sandbox"
DEAD_LETTER = LANE + ":runs:dead"
STREAM = "curie:runs"
GROUP = "protected-lane"
CONSUMER = "protected-consumer-1"
CONTROL = "protected:control:selection:33333333-3333-4333-8333-333333333333"
BINDING_EVENT = "hook-11111111-1111-4111-8111-111111111111-incident-0123456789abcdef"
BINDING = "protected:admission:binding:" + BINDING_EVENT

IMAGE = "valkey/valkey:8.1.10-alpine"


@pytest.fixture(scope="module")
def pworker_service(tmp_path_factory):
    """Real TLS, disabled default, only a provisioner, owned cleanup,
    @spec PROTECTED-HOOK-LANE-3."""
    if shutil.which("docker") is None:
        fail_safely("Docker is required for actual TLS worker role verification")
    fixture = helpers.AdmissionBroker()
    fixture.private = tmp_path_factory.mktemp("protected-worker-tls")
    fixture.private.chmod(0o700)
    fixture.cid = None
    fixture.admin = None
    cidfile = fixture.private / "container.cid"
    owner = "curie-check-pworker-" + uuid4().hex[:12]
    try:
        config = fixture.private / "docker"
        config.mkdir(mode=0o700)
        fixture.env = {key: FixtureSecret(value) for key, value in os.environ.items()}
        fixture.env["DOCKER_CONFIG"] = str(config)
        fixture.env.pop("DOCKER_AUTH_CONFIG", None)
        fixture.env.pop("DOCKER_CONTEXT", None)
        fixture.docker("ps", "--format", "{{.ID}}")
        fixture.passwords = {}
        provisioner_password = FixtureSecret(secrets.token_hex(24))
        fixture.ca_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        now = datetime.now(UTC)
        subject = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "fixture-ca")])
        fixture.ca_cert = (
            x509.CertificateBuilder()
            .subject_name(subject)
            .issuer_name(subject)
            .public_key(fixture.ca_key.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(now - timedelta(minutes=1))
            .not_valid_after(now + timedelta(days=1))
            .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
            .add_extension(
                x509.SubjectKeyIdentifier.from_public_key(fixture.ca_key.public_key()), False
            )
            .add_extension(
                x509.AuthorityKeyIdentifier.from_issuer_public_key(fixture.ca_key.public_key()),
                False,
            )
            .add_extension(
                x509.KeyUsage(True, False, False, False, False, True, True, False, False), True
            )
            .sign(fixture.ca_key, hashes.SHA256())
        )
        fixture.ca_pem = FixtureSecret(
            fixture.ca_cert.public_bytes(serialization.Encoding.PEM).decode()
        )
        fixture.pin = helpers.certificate(fixture.private, fixture.ca_key, fixture.ca_cert)
        ca_path = fixture.private / "ca.crt"
        helpers.write_private(ca_path, fixture.ca_pem)
        helpers.write_private(
            fixture.private / "users.acl",
            "user default off\nuser provisioner on >" + provisioner_password + " ~* &* +@all\n",
        )
        for _attempt in range(5):
            with socket.socket() as probe:
                probe.bind(("127.0.0.1", 0))
                fixture.port = probe.getsockname()[1]
            started = fixture.docker(
                "run",
                "-d",
                "--name",
                owner,
                "--cidfile",
                str(cidfile),
                "--label",
                "curie.test.owner=" + owner,
                "--user",
                f"{os.getuid()}:{os.getgid()}",
                "-p",
                f"127.0.0.1:{fixture.port}:6379",
                "-v",
                str(fixture.private) + ":/tls:ro",
                IMAGE,
                "valkey-server",
                "--port",
                "0",
                "--tls-port",
                "6379",
                "--tls-cert-file",
                "/tls/server.crt",
                "--tls-key-file",
                "/tls/server.key",
                "--tls-ca-cert-file",
                "/tls/ca.crt",
                "--tls-auth-clients",
                "no",
                "--aclfile",
                "/tls/users.acl",
                "--save",
                "",
                "--appendonly",
                "no",
                allow_port_collision=True,
            )
            if started is not None:
                break
            fixture.remove_owned(owner, cidfile)
        else:
            fail_safely("owned TLS fixture could not reserve a stable loopback port")
        fixture.cid = cidfile.read_text().strip()
        fixture.admin = Redis(
            host="127.0.0.1",
            port=fixture.port,
            username="provisioner",
            password=provisioner_password,
            ssl=True,
            ssl_ca_certs=str(ca_path),
            ssl_cert_reqs="required",
            ssl_check_hostname=True,
            protocol=3,
            socket_timeout=2,
            socket_connect_timeout=2,
            retry=Retry(NoBackoff(), 0),
        )
        fixture.ready()
        assert fixture.command("INFO", "server")["valkey_version"] == "8.1.10"
        yield fixture
    finally:
        try:
            if fixture.admin is not None:
                fixture.admin.close()
        finally:
            try:
                fixture.remove_owned(owner, cidfile)
            finally:
                for path in fixture.private.iterdir():
                    if path.is_file():
                        path.unlink()


def worker_acl():
    """The product recipe module; absence is an assertion, @spec PROTECTED-HOOK-LANE-3."""
    import importlib
    import importlib.util

    assert importlib.util.find_spec("curie_protected_hooks.worker_acl") is not None, (
        "worker ACL recipe module absent"
    )
    return importlib.import_module("curie_protected_hooks.worker_acl")


def worker_transport():
    """The product transport module; absence is an assertion, @spec PROTECTED-HOOK-LANE-3."""
    import importlib
    import importlib.util

    assert importlib.util.find_spec("curie_protected_hooks.worker_transport") is not None, (
        "worker transport module absent"
    )
    return importlib.import_module("curie_protected_hooks.worker_transport")


class WorkerPrincipal:
    """One owned worker principal installed from the product recipe, @spec PROTECTED-HOOK-LANE-3."""

    def __init__(self, broker, role="worker", rules=None):
        """@spec PROTECTED-HOOK-LANE-3."""
        self.broker = broker
        self.username = role + "-" + secrets.token_hex(6)
        self.password = FixtureSecret(secrets.token_hex(24))
        self.role = role
        self.rules = rules

    def __repr__(self):
        """@spec PROTECTED-HOOK-LANE-3."""
        return "<fixture-worker-principal>"

    def install(self):
        """Only the provisioner installs a product recipe, @spec PROTECTED-HOOK-LANE-3."""
        rules = self.rules if self.rules is not None else worker_acl().worker_acl_rules(self.role)
        self.broker.command(
            "ACL", "SETUSER", self.username, "reset", "on", ">" + self.password, *rules
        )

    def remove(self):
        """@spec PROTECTED-HOOK-LANE-3."""
        users = [
            u.decode() if isinstance(u, bytes) else u for u in self.broker.command("ACL", "USERS")
        ]
        if self.username in users:
            self.broker.command("CLIENT", "KILL", "USER", self.username, "SKIPME", "yes")
            self.broker.command("ACL", "DELUSER", self.username)

    def credential(self, password=None):
        """@spec PROTECTED-HOOK-LANE-3."""
        return worker_transport().WorkerCredential(self.username, password or self.password)

    def _kwargs(self, **extra):
        return dict(
            host="127.0.0.1",
            port=self.broker.port,
            username=self.username,
            password=self.password,
            ssl=True,
            ssl_ca_certs=str(self.broker.private / "ca.crt"),
            ssl_cert_reqs="required",
            ssl_check_hostname=True,
            socket_timeout=3,
            socket_connect_timeout=2,
            retry=Retry(NoBackoff(), 0),
            **extra,
        )

    def raw(self, **extra):
        """A plain TLS client as this principal, for recipe measurement only."""
        return Redis(**self._kwargs(**extra))

    def araw(self, **extra):
        """A plain async TLS client as this principal, for recipe measurement only."""
        return aioredis.Redis(**self._kwargs(**extra))

    def sessions(self):
        """@spec PROTECTED-HOOK-LANE-3."""
        raw = self.broker.command("CLIENT", "LIST")
        name = "user=" + self.username
        return [
            dict(item.split("=", 1) for item in line.split())
            for line in raw.decode().splitlines()
            if name in line.split()
        ]

    def denials(self):
        """The ACL LOG entries recorded for this principal."""
        return [
            entry
            for entry in self.broker.command("ACL", "LOG", 200)
            if entry[b"username"] == self.username.encode()
        ]


@pytest.fixture
def owned_worker(pworker_service):
    """Fresh keys, a clean denial log and one owned worker principal."""
    pworker_service.command("FLUSHDB")
    pworker_service.command("ACL", "LOG", "RESET")
    principal = WorkerPrincipal(pworker_service)
    principal.install()
    try:
        yield principal
    finally:
        principal.remove()
        pworker_service.command("FLUSHDB")


def lane_config(broker, **overrides):
    """The worker configuration the lane will use on the private broker.

    Stream stays the admission-written ``curie:runs``; every key the worker owns lives
    under ``protected:lane:``, including the graveyard, the lock/marker/lease prefix
    and the affinity prefix. @spec PROTECTED-HOOK-LANE-3 PROTECTED-HOOK-LANE-7.
    """
    from curie_worker.config import WorkerConfig

    base = dict(
        valkey_host="127.0.0.1",
        valkey_port=broker.port,
        valkey_password="unused-fixture",
        stream=STREAM,
        consumer_group=GROUP,
        consumer_name=CONSUMER,
        key_prefix=WORKER_PREFIX,
        dead_letter_stream=DEAD_LETTER,
        slack_edit_min_interval_s=0.0,
        max_attempts=3,
        retry_backoff_base_s=0.001,
        retry_backoff_max_s=0.01,
        lock_ttl_ms=5000,
        lock_acquire_timeout_s=5.0,
        reclaim_min_idle_ms=50,
        reclaim_interval_s=0.05,
        read_block_ms=100,
        delivery_budget_s=60.0,
        delivery_lease_ttl_s=1.0,
        delivery_lease_heartbeat_s=0.3,
        runner_total_timeout_s=30.0,
    )
    base.update(overrides)
    return WorkerConfig(**base)


def connections(broker):
    """Connections the broker has ever accepted, @spec PROTECTED-HOOK-LANE-3."""
    return int(broker.command("INFO", "stats")["total_connections_received"])


def command_count(broker, command):
    """Executed plus rejected calls of one command, @spec PROTECTED-HOOK-LANE-3."""
    stats = broker.command("INFO", "commandstats").get("cmdstat_" + command, {})
    return stats.get("calls", 0) + stats.get("rejected_calls", 0)


def wrong_ca():
    """An unrelated self-signed CA, @spec PROTECTED-HOOK-LANE-2/3."""
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    now = datetime.now(UTC)
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "wrong-ca")])
    cert = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(minutes=1))
        .not_valid_after(now + timedelta(days=1))
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), True)
        .sign(key, hashes.SHA256())
    )
    return FixtureSecret(cert.public_bytes(serialization.Encoding.PEM).decode())
