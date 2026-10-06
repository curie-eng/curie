"""Authenticated source writer transport, @spec PROTECTED-HOOK-SOURCE-6/7 PROTECTED-HOOK-LANE-3.

Every case runs against the owned disposable TLS Valkey of
``admission_broker.py`` (default user disabled, exact container ownership)
with distinct named ``source_writer`` and ``control_reader`` principals
installed from the closed recipes. Faults are owned ACL changes, client kills
and budgets only.
"""

from __future__ import annotations

import inspect
import json
import secrets
import socket
import time
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID
from curie_protected_hooks import broker_transport as transport
from curie_protected_hooks.broker_metadata import BrokerMetadataUnavailable, metadata_acl_rules
from curie_protected_hooks.source_fence import SourceFence, SourceFenceConflict
from redis import Redis
from redis.backoff import NoBackoff
from redis.exceptions import RedisError, ResponseError
from redis.retry import Retry

from . import admission_broker as broker_helpers

admission_service = broker_helpers.admission_service
FixtureSecret = broker_helpers.FixtureSecret

AGENT = "11111111-1111-4111-8111-111111111111"
HOOK = "incident"
SOURCE = f"protected:source:{AGENT}:{HOOK}"
CONTROL = "protected:control:runtime:manifest"
FINGERPRINT = "e" * 64


class Principals:
    """Owned writer and reader principals on the owned broker, @spec PROTECTED-HOOK-LANE-3."""

    def __init__(self, broker):
        """@spec PROTECTED-HOOK-LANE-3."""
        self.broker = broker
        self.writer = "writer-" + secrets.token_hex(6)
        self.reader = "reader-" + secrets.token_hex(6)
        self.writer_password = FixtureSecret(secrets.token_hex(24))
        self.reader_password = FixtureSecret(secrets.token_hex(24))

    def __repr__(self):
        """@spec PROTECTED-HOOK-LANE-3."""
        return "<fixture-principals>"

    def install(self):
        """@spec PROTECTED-HOOK-LANE-3."""
        for user, password, role in (
            (self.writer, self.writer_password, "source_writer"),
            (self.reader, self.reader_password, "control_reader"),
        ):
            self.broker.command(
                "ACL", "SETUSER", user, "reset", "on", ">" + password, *metadata_acl_rules(role)
            )

    def remove(self):
        """@spec PROTECTED-HOOK-LANE-3."""
        users = [
            u.decode() if isinstance(u, bytes) else u for u in self.broker.command("ACL", "USERS")
        ]
        for user in (self.writer, self.reader):
            if user in users:
                self.broker.command("CLIENT", "KILL", "USER", user, "SKIPME", "yes")
                self.broker.command("ACL", "DELUSER", user)

    def credential(self, password=None):
        """@spec PROTECTED-HOOK-SOURCE-6."""
        return transport.SourceWriterCredential(self.writer, password or self.writer_password)

    def connect(self, manifest=None, ca=None, password=None):
        """Only the public writer factory, @spec PROTECTED-HOOK-SOURCE-6."""
        return transport.AuthenticatedSourceWriter.connect(
            manifest or self.broker.manifest(), self.credential(password), ca or self.broker.ca_pem
        )

    def raw(self, role):
        """A plain TLS client as one principal, @spec PROTECTED-HOOK-LANE-3."""
        user, password = (
            (self.writer, self.writer_password)
            if role == "writer"
            else (self.reader, self.reader_password)
        )
        ca = self.broker.private / "ca.crt"
        return Redis(
            host="127.0.0.1",
            port=self.broker.port,
            username=user,
            password=password,
            ssl=True,
            ssl_ca_certs=str(ca),
            ssl_cert_reqs="required",
            ssl_check_hostname=True,
            protocol=3,
            socket_timeout=2,
            socket_connect_timeout=2,
            retry=Retry(NoBackoff(), 0),
        )

    def sessions(self, user=None):
        """@spec PROTECTED-HOOK-LANE-3."""
        raw = self.broker.command("CLIENT", "LIST")
        name = "user=" + (user or self.writer)
        return [line for line in raw.decode().splitlines() if name in line.split()]


@pytest.fixture
def owned(admission_service):
    """Fresh keys, denial log and principals, @spec PROTECTED-HOOK-SOURCE-6."""
    admission_service.command("FLUSHDB")
    admission_service.command("ACL", "LOG", "RESET")
    principals = Principals(admission_service)
    principals.install()
    try:
        yield principals
    finally:
        principals.remove()
        admission_service.command("FLUSHDB")


def connections(broker):
    """@spec PROTECTED-HOOK-SOURCE-6."""
    return int(broker.command("INFO", "stats")["total_connections_received"])


def command_count(broker, command):
    """Executed plus rejected calls of one command, @spec PROTECTED-HOOK-LANE-3."""
    stats = broker.command("INFO", "commandstats").get("cmdstat_" + command, {})
    return stats.get("calls", 0) + stats.get("rejected_calls", 0)


def assert_safe(error, owned):
    """Exact safe error, no credential, endpoint or certificate, @spec PROTECTED-HOOK-SOURCE-6."""
    assert type(error) is BrokerMetadataUnavailable
    assert str(error) == "Broker metadata unavailable"
    assert error.__cause__ is None
    assert error.__context__ is None or error.__suppress_context__
    broker = owned.broker
    for value in (
        owned.writer,
        owned.writer_password,
        broker.ca_pem.splitlines()[1],
        "127.0.0.1",
        str(broker.port),
        broker.pin,
    ):
        assert value not in repr(error) and value not in str(error)


def wrong_ca():
    """@spec PROTECTED-HOOK-LANE-2/3."""
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


def test_closed_surface_and_redacted_credential():
    """Sibling class: reserve, both publications, close, @spec PROTECTED-HOOK-SOURCE-6."""
    credential = transport.SourceWriterCredential("unique-writer", "unique-password")
    assert "unique-writer" not in repr(credential) and "unique-password" not in repr(credential)
    assert not hasattr(credential, "__dict__")
    with pytest.raises((AttributeError, TypeError)):
        credential.password = "changed"
    public = {name for name in dir(transport.AuthenticatedSourceWriter) if not name.startswith("_")}
    assert public == {
        "connect",
        "reserve_and_revoke",
        "publish_ordinary",
        "publish_protected",
        "close",
    }
    assert list(inspect.signature(transport.AuthenticatedSourceWriter.connect).parameters) == [
        "manifest",
        "credential",
        "ca_pem",
    ]
    assert not issubclass(
        transport.AuthenticatedSourceWriter, transport.AuthenticatedMetadataReader
    )
    with pytest.raises((TypeError, BrokerMetadataUnavailable)):
        transport.AuthenticatedSourceWriter()


@pytest.mark.parametrize(
    "username,password",
    [("", "x"), ("default", "x"), (None, "x"), ("writer", ""), ("writer", None)],
)
def test_invalid_writer_credentials_are_safe(username, password):
    """@spec PROTECTED-HOOK-SOURCE-6."""
    with pytest.raises(BrokerMetadataUnavailable):
        transport.SourceWriterCredential(username, password)


def test_connect_sends_no_info_and_reserves_publishes_and_republishes(owned):
    """The writer role has no INFO: connecting succeeds without one; fence effects succeed.

    @spec PROTECTED-HOOK-SOURCE-6 @spec PROTECTED-HOOK-SOURCE-7 @spec PROTECTED-HOOK-LANE-3.
    """
    broker = owned.broker
    assert broker.command("ACL", "DRYRUN", owned.writer, "INFO", "server") != b"OK"
    writer = owned.connect()
    try:
        # A sent INFO would be denied and logged; none is, and the session is authenticated.
        assert broker.command("ACL", "LOG") == []
        operation = str(uuid4())
        assert writer.reserve_and_revoke(AGENT, HOOK, 0, operation, 4) == 5
        assert writer.publish_ordinary(AGENT, HOOK, 5, operation, FINGERPRINT) is True
        assert writer.publish_ordinary(AGENT, HOOK, 5, operation, FINGERPRINT) is True
        assert json.loads(broker.command("GET", SOURCE)) == {
            "floor": "5",
            "operation_id": operation,
            "active": {
                "generation": "5",
                "operation_id": operation,
                "mode": "ordinary",
                "policy_fingerprint": FINGERPRINT,
            },
        }
        assert broker.command("ACL", "LOG") == [], "the writer attempted a denied command"
        assert len(owned.sessions()) == 1
        assert "127.0.0.1" not in repr(writer) and owned.writer_password not in repr(writer)
    finally:
        writer.close()


def test_floor_or_operation_mismatch_refuses(owned):
    """@spec PROTECTED-HOOK-SOURCE-6 @spec PROTECTED-HOOK-SOURCE-7."""
    broker = owned.broker
    writer = owned.connect()
    try:
        operation = str(uuid4())
        assert writer.reserve_and_revoke(AGENT, HOOK, 0, operation, 0) == 1
        with pytest.raises(SourceFenceConflict):
            writer.reserve_and_revoke(AGENT, HOOK, 0, str(uuid4()), 0)
        assert writer.publish_ordinary(AGENT, HOOK, 1, str(uuid4()), FINGERPRINT) is False
        assert writer.publish_ordinary(AGENT, HOOK, 2, operation, FINGERPRINT) is False
        assert json.loads(broker.command("GET", SOURCE))["active"] is None
    finally:
        writer.close()


@pytest.mark.parametrize("case", ["ca", "hostname", "pin"])
def test_tls_refusals_precede_authentication(owned, case):
    """Chain, name and SPKI pin are checked before HELLO AUTH, @spec PROTECTED-HOOK-SOURCE-6."""
    broker = owned.broker
    manifest, ca = broker.manifest(), broker.ca_pem
    if case == "ca":
        ca = wrong_ca()
    elif case == "hostname":
        manifest = broker.manifest(
            endpoint={"host": "localhost", "port": broker.port}, tls_server_name="localhost"
        )
    else:
        manifest = broker.manifest(tls_spki_sha256="0" * 64)
    bad = FixtureSecret("wrong-fixture-password")
    hellos = command_count(broker, "hello")
    with pytest.raises(BrokerMetadataUnavailable) as caught:
        owned.connect(manifest, ca, bad)
    assert_safe(caught.value, owned)
    assert broker.command("ACL", "LOG") == []
    assert command_count(broker, "hello") == hellos
    with pytest.raises(BrokerMetadataUnavailable):
        owned.connect(password=bad)
    entries = broker.command("ACL", "LOG")
    assert entries and all(entry[b"reason"] == b"auth" for entry in entries)


def test_failed_named_authentication_never_tries_default(owned):
    """A default-only password cannot authenticate the writer, @spec PROTECTED-HOOK-SOURCE-6."""
    broker = owned.broker
    default_secret = FixtureSecret(secrets.token_hex(24))
    broker.command("ACL", "SETUSER", "default", "on", ">" + default_secret, "~*", "+@all")
    try:
        before = {name: command_count(broker, name) for name in ("hello", "auth")}
        with pytest.raises(BrokerMetadataUnavailable) as caught:
            owned.connect(password=default_secret)
        assert command_count(broker, "hello") - before["hello"] == 1
        assert command_count(broker, "auth") - before["auth"] == 0
        assert_safe(caught.value, owned)
        entries = broker.command("ACL", "LOG")
        assert entries and all(entry[b"username"] == owned.writer.encode() for entry in entries)
        assert owned.sessions() == [] and owned.sessions("default") == []
    finally:
        broker.command("ACL", "SETUSER", "default", "reset", "off")


def test_a_killed_connection_is_never_reopened(owned):
    """Connection loss leaves the writer permanently unusable, @spec PROTECTED-HOOK-SOURCE-6."""
    broker = owned.broker
    writer = owned.connect()
    try:
        assert broker.command("CLIENT", "KILL", "USER", owned.writer, "SKIPME", "yes") == 1
        accepted = connections(broker)
        for _ in range(3):
            with pytest.raises(BrokerMetadataUnavailable) as caught:
                writer.reserve_and_revoke(AGENT, HOOK, 0, str(uuid4()), 0)
            assert_safe(caught.value, owned)
            with pytest.raises(BrokerMetadataUnavailable):
                writer.publish_ordinary(AGENT, HOOK, 1, str(uuid4()), FINGERPRINT)
        assert connections(broker) == accepted, "the writer opened a second connection"
        assert owned.sessions() == []
        assert broker.command("GET", SOURCE) is None
    finally:
        writer.close()


def test_an_expired_budget_refuses_the_next_call(owned):
    """The reader budget watchdog bounds the writer too, @spec PROTECTED-HOOK-SOURCE-6."""
    broker = owned.broker
    with transport.metadata_reader_budget(0.4):
        writer = owned.connect()
    try:
        time.sleep(0.6)
        accepted = connections(broker)
        with pytest.raises(BrokerMetadataUnavailable) as caught:
            writer.reserve_and_revoke(AGENT, HOOK, 0, str(uuid4()), 0)
        assert_safe(caught.value, owned)
        assert connections(broker) == accepted
        assert broker.command("GET", SOURCE) is None
    finally:
        writer.close()


def test_close_is_terminal(owned):
    """@spec PROTECTED-HOOK-SOURCE-6."""
    writer = owned.connect()
    writer.close()
    writer.close()
    with pytest.raises(BrokerMetadataUnavailable):
        writer.reserve_and_revoke(AGENT, HOOK, 0, str(uuid4()), 0)
    with pytest.raises(BrokerMetadataUnavailable):
        writer.publish_ordinary(AGENT, HOOK, 1, str(uuid4()), FINGERPRINT)
    deadline = time.monotonic() + 2
    while owned.sessions():
        assert time.monotonic() < deadline, "closed writer session still listed"
        time.sleep(0.02)


@pytest.mark.parametrize("kind", ["manifest", "credential", "empty-ca", "text-ca", "tuple"])
def test_invalid_inputs_precede_network(owned, monkeypatch, kind):
    """@spec PROTECTED-HOOK-SOURCE-6."""
    broker = owned.broker
    manifest, credential, ca = broker.manifest(), owned.credential(), broker.ca_pem
    if kind == "manifest":
        manifest = manifest.as_dict()
    elif kind == "credential":
        credential = transport.MetadataReaderCredential(owned.writer, owned.writer_password)
    elif kind == "empty-ca":
        ca = ""
    elif kind == "text-ca":
        ca = FixtureSecret(broker.ca_pem + "trailing text")
    else:
        manifest = broker.manifest(tls_server_name="localhost")
    attempts = []

    def forbidden(*args, **kwargs):
        """@spec PROTECTED-HOOK-SOURCE-6."""
        attempts.append(True)
        raise AssertionError("invalid input attempted network I/O")

    monkeypatch.setattr(socket.socket, "connect", forbidden)
    monkeypatch.setattr(socket, "getaddrinfo", forbidden)
    with pytest.raises(BrokerMetadataUnavailable) as caught:
        transport.AuthenticatedSourceWriter.connect(manifest, credential, ca)
    assert_safe(caught.value, owned)
    assert attempts == []


def test_writer_and_reader_cannot_do_each_others_work(owned):
    """The broker ACL confines each principal, @spec PROTECTED-HOOK-SOURCE-6/LANE-3.

    The writer cannot read or write control keys, write outside
    ``protected:source:*`` (directly or from a script) or run INFO or TIME; the
    reader cannot reserve or publish.
    """
    broker = owned.broker
    broker.command("SET", CONTROL, "control-value")
    writer, reader = owned.raw("writer"), owned.raw("reader")
    try:
        for command in (
            ("GET", CONTROL),
            ("SET", CONTROL, "x"),
            ("SET", "protected:admission:intent:x", "x"),
            ("SET", "ordinary:key", "x"),
            ("INFO", "server"),
            ("TIME",),
            ("EVAL", "return redis.call('SET', KEYS[1], 'x')", "1", CONTROL),
            ("EVAL", "return redis.call('GET', KEYS[1])", "1", CONTROL),
        ):
            with pytest.raises(ResponseError):
                writer.execute_command(*command)
        operation = str(uuid4())
        with pytest.raises(RedisError):
            SourceFence(reader).reserve_and_revoke(AGENT, HOOK, 0, operation, 0)
        assert broker.command("GET", SOURCE) is None
        broker.command(
            "SET", SOURCE, json.dumps({"floor": "1", "operation_id": operation, "active": None})
        )
        with pytest.raises(RedisError):
            SourceFence(reader).publish_ordinary(AGENT, HOOK, 1, operation, FINGERPRINT)
        with pytest.raises(ResponseError):
            reader.execute_command("SET", SOURCE, "x")
        assert json.loads(broker.command("GET", SOURCE))["active"] is None
        assert broker.command("GET", CONTROL) == b"control-value"
    finally:
        writer.close()
        reader.close()
