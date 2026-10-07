"""Authenticated enqueue transport and its measured recipe, @spec PROTECTED-HOOK-LANE-3
PROTECTED-HOOK-SOURCE-6 PROTECTED-HOOK-ADMISSION-6.

``AuthenticatedEnqueueClient`` is the LANE-3 enqueue role's transport beside
the metadata reader and the source writer: the same input validation, TLS, CA,
hostname, SPKI pin, RESP3 HELLO AUTH, two second timeouts, disabled retries,
redaction and budget watchdog (``metadata_reader_budget``), the live
``INFO server`` run_id verified on connection, and never a second connection.
It exports the source and control reads and the observation the shared
evaluation uses, a presence check of one delivery's private intent key
(``intent_present``), the admission facade bound to its connection, and
``close``; never a raw client or generic command.

Every case runs against the owned disposable TLS Valkey of
``admission_broker.py`` (default user disabled, exact container ownership) with
a distinct owned principal installed from ``admission_acl_rules("enqueue")``
and removed after the test. Faults are owned ACL changes, client kills,
budgets and owned loopback listeners only.
"""

from __future__ import annotations

import importlib
import importlib.util
import inspect
import json
import secrets
import socket
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID
from curie_protected_hooks.admission_acl import admission_acl_rules
from curie_protected_hooks.admission_records import DeliveryIdentity, delivery_digest
from curie_protected_hooks.broker_metadata import BrokerMetadataUnavailable
from redis import Redis
from redis.backoff import NoBackoff
from redis.connection import Connection
from redis.exceptions import NoPermissionError, RedisError, ResponseError
from redis.retry import Retry

from . import admission_broker as broker_helpers

admission_service = broker_helpers.admission_service
FixtureSecret = broker_helpers.FixtureSecret

AGENT = "11111111-1111-4111-8111-111111111111"
HOOK = "incident"
SOURCE = f"protected:source:{AGENT}:{HOOK}"
CONTROL = "protected:control:selection:33333333-3333-4333-8333-333333333333"
QUOTA = "protected:admission:quota"
STREAM = "curie:runs"
EVIDENCE = Path(__file__).resolve().parents[3] / "docs/adr/evidence/0191-protected-hooks/README.md"


def transport():
    """The transport module, @spec PROTECTED-HOOK-LANE-3."""
    return importlib.import_module("curie_protected_hooks.broker_transport")


def enqueue_types():
    """Missing product names are assertions in the test body, @spec PROTECTED-HOOK-LANE-3."""
    module = transport()
    assert hasattr(module, "EnqueueCredential"), "enqueue credential absent"
    assert hasattr(module, "AuthenticatedEnqueueClient"), "authenticated enqueue client absent"
    return module.EnqueueCredential, module.AuthenticatedEnqueueClient


class Principal:
    """One owned enqueue principal on the owned broker, @spec PROTECTED-HOOK-LANE-3."""

    def __init__(self, broker):
        """@spec PROTECTED-HOOK-LANE-3."""
        self.broker = broker
        self.username = "enqueue-" + secrets.token_hex(6)
        self.password = FixtureSecret(secrets.token_hex(24))

    def __repr__(self):
        """@spec PROTECTED-HOOK-LANE-3."""
        return "<fixture-enqueue-principal>"

    def install(self):
        """Only the provisioner installs the product recipe, @spec PROTECTED-HOOK-ADMISSION-6."""
        self.broker.command(
            "ACL",
            "SETUSER",
            self.username,
            "reset",
            "on",
            ">" + self.password,
            *admission_acl_rules("enqueue"),
        )

    def remove(self):
        """@spec PROTECTED-HOOK-LANE-3."""
        users = [
            u.decode() if isinstance(u, bytes) else u for u in self.broker.command("ACL", "USERS")
        ]
        if self.username in users:
            self.broker.command("CLIENT", "KILL", "USER", self.username, "SKIPME", "yes")
            self.broker.command("ACL", "DELUSER", self.username)

    def restore(self):
        """Reinstall the product recipe after an owned ACL fault, @spec PROTECTED-HOOK-LANE-3."""
        self.install()

    def credential(self, password=None):
        """@spec PROTECTED-HOOK-LANE-3."""
        credential_type, _ = enqueue_types()
        return credential_type(self.username, password or self.password)

    def connect(self, manifest=None, ca=None, password=None):
        """Only the public enqueue factory, @spec PROTECTED-HOOK-LANE-3."""
        _, client_type = enqueue_types()
        return client_type.connect(
            manifest or self.broker.manifest(), self.credential(password), ca or self.broker.ca_pem
        )

    def raw(self):
        """A plain TLS client as this principal, for recipe measurement only.

        @spec PROTECTED-HOOK-ADMISSION-6.
        """
        return Redis(
            host="127.0.0.1",
            port=self.broker.port,
            username=self.username,
            password=self.password,
            ssl=True,
            ssl_ca_certs=str(self.broker.private / "ca.crt"),
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
        name = "user=" + (user or self.username)
        return [
            dict(item.split("=", 1) for item in line.split())
            for line in raw.decode().splitlines()
            if name in line.split()
        ]


@pytest.fixture
def owned(admission_service):
    """Fresh keys, denial log and an owned principal, @spec PROTECTED-HOOK-LANE-3."""
    admission_service.command("FLUSHDB")
    admission_service.command("ACL", "LOG", "RESET")
    principal = Principal(admission_service)
    principal.install()
    try:
        yield principal
    finally:
        principal.remove()
        admission_service.command("FLUSHDB")


def connections(broker):
    """@spec PROTECTED-HOOK-LANE-3."""
    return int(broker.command("INFO", "stats")["total_connections_received"])


def command_count(broker, command):
    """Executed plus rejected calls of one command, @spec PROTECTED-HOOK-LANE-3."""
    stats = broker.command("INFO", "commandstats").get("cmdstat_" + command, {})
    return stats.get("calls", 0) + stats.get("rejected_calls", 0)


def assert_safe(error, owned):
    """Exact safe error, no credential, endpoint or certificate, @spec PROTECTED-HOOK-LANE-3."""
    assert type(error) is BrokerMetadataUnavailable
    assert str(error) == "Broker metadata unavailable"
    assert error.__cause__ is None
    assert error.__context__ is None or error.__suppress_context__
    broker = owned.broker
    for value in (
        owned.username,
        owned.password,
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


def identity(delivery="delivery/example"):
    """@spec PROTECTED-HOOK-ADMISSION-2."""
    return DeliveryIdentity(agent_id=AGENT, hook=HOOK, delivery_id=delivery)


def intent_key(delivery="delivery/example"):
    """@spec PROTECTED-HOOK-ADMISSION-2 PROTECTED-HOOK-SOURCE-8."""
    return "protected:admission:intent:" + delivery_digest(identity(delivery))


# -- credential and closed surface ---------------------------------------------------------


def test_enqueue_credential_is_frozen_slotted_and_redacted():
    """A sibling of the reader and writer credentials, @spec PROTECTED-HOOK-SOURCE-6 LANE-3."""
    credential_type, _ = enqueue_types()
    module = transport()
    credential = credential_type("unique-enqueue", "unique-password")
    assert (credential.username, credential.password) == ("unique-enqueue", "unique-password")
    for text in (repr(credential), str(credential)):
        assert "unique-enqueue" not in text and "unique-password" not in text
    assert not hasattr(credential, "__dict__")
    with pytest.raises((AttributeError, TypeError)):
        credential.password = "changed"
    assert credential_type is not module.MetadataReaderCredential
    assert credential_type is not module.SourceWriterCredential


@pytest.mark.parametrize(
    "username,password",
    [("", "x"), ("default", "x"), (None, "x"), (7, "x"), ("enqueue", ""), ("enqueue", None)],
)
def test_invalid_enqueue_credentials_are_safe(username, password):
    """@spec PROTECTED-HOOK-SOURCE-6 @spec PROTECTED-HOOK-LANE-3."""
    credential_type, _ = enqueue_types()
    with pytest.raises(BrokerMetadataUnavailable):
        credential_type(username, password)


ALLOWED_PUBLIC = {
    "connect",
    "read_source",
    "read_control",
    "observe",
    "intent_present",
    "read_binding",
    "admission",
    "close",
}
REQUIRED_PUBLIC = ALLOWED_PUBLIC - {"admission"}


def test_closed_surface_exports_no_raw_client_or_generic_command():
    """Only the named operations; no raw client, connection, eval or execute.

    The admission facade binding (``admission``) may join with the facade
    constructor change; nothing else may. @spec PROTECTED-HOOK-LANE-3.
    """
    module = transport()
    _, client_type = enqueue_types()
    public = {name for name in dir(client_type) if not name.startswith("_")}
    assert REQUIRED_PUBLIC <= public <= ALLOWED_PUBLIC
    assert list(inspect.signature(client_type.connect).parameters) == [
        "manifest",
        "credential",
        "ca_pem",
    ]
    for unrelated in (module.AuthenticatedMetadataReader, module.AuthenticatedSourceWriter):
        assert not issubclass(client_type, unrelated)
    assert "__slots__" in vars(client_type)
    with pytest.raises((TypeError, BrokerMetadataUnavailable)):
        client_type()


def test_a_connected_client_holds_nothing_public_that_is_a_raw_client(owned):
    """No exported attribute of a live instance is a Redis client or connection.

    @spec PROTECTED-HOOK-LANE-3.
    """
    client = owned.connect()
    try:
        for name in dir(client):
            if name.startswith("_"):
                continue
            value = getattr(client, name)
            assert not isinstance(value, (Redis, Connection)), name
            assert callable(value), name
        assert not hasattr(client, "__dict__")
        text = repr(client) + str(client)
        for secret in (owned.username, owned.password, "127.0.0.1", str(owned.broker.port)):
            assert secret not in text
    finally:
        client.close()


# -- connection identity --------------------------------------------------------------


def test_reads_observation_and_intent_presence_on_one_retained_session(owned):
    """Source, control, observation and the intent check share one RESP3 session.

    No denied command is ever sent, so the principal's denial log stays empty.
    @spec PROTECTED-HOOK-LANE-3 @spec PROTECTED-HOOK-SOURCE-8 @spec PROTECTED-HOOK-SOURCE-9.
    """
    broker = owned.broker
    operation = "22222222-2222-4222-8222-222222222222"
    broker.command(
        "SET", SOURCE, json.dumps({"floor": "3", "operation_id": operation, "active": None})
    )
    broker.command("SET", CONTROL, "control-bytes")
    broker.command("SET", intent_key("delivery/present"), "intent-bytes")
    client = owned.connect()
    try:
        initial = owned.sessions()
        assert len(initial) == 1 and initial[0]["resp"] == "3"
        assert client.read_source(AGENT, HOOK) == {
            "floor": 3,
            "operation_id": operation,
            "active": None,
        }
        assert client.read_control(CONTROL) == b"control-bytes"
        assert client.read_control("protected:control:absent") is None
        before = broker.command("TIME")
        observation = client.observe()
        after = broker.command("TIME")
        assert observation.run_id == broker.command("INFO", "server")["run_id"]
        assert int(before[0]) * 1000 + int(before[1]) // 1000 <= observation.now_ms
        assert observation.now_ms <= int(after[0]) * 1000 + int(after[1]) // 1000
        assert client.intent_present(identity("delivery/present")) is True
        assert client.intent_present(identity("delivery/absent")) is False
        assert [session["id"] for session in owned.sessions()] == [initial[0]["id"]]
        assert broker.command("ACL", "LOG") == []
        assert broker.command("GET", intent_key("delivery/present")) == b"intent-bytes"
    finally:
        client.close()


@pytest.mark.parametrize("case", ["ca", "hostname", "pin"])
def test_tls_refusals_precede_authentication(owned, case):
    """Chain, name and SPKI pin are checked before HELLO AUTH, @spec PROTECTED-HOOK-LANE-3."""
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


def test_named_authentication_never_falls_back_to_default(owned):
    """A default-only password cannot authenticate the enqueue principal.

    @spec PROTECTED-HOOK-LANE-3.
    """
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
        assert entries and all(entry[b"username"] == owned.username.encode() for entry in entries)
        assert owned.sessions() == [] and owned.sessions("default") == []
    finally:
        broker.command("ACL", "SETUSER", "default", "reset", "off")


def test_a_wrong_run_id_refuses_before_any_application_command(owned):
    """TLS and authentication cannot stand in for the live broker epoch.

    @spec PROTECTED-HOOK-LANE-2 @spec PROTECTED-HOOK-LANE-3.
    """
    broker = owned.broker
    broker.command("ACL", "SETUSER", owned.username, "-get", "-eval", "-time")
    try:
        with pytest.raises(BrokerMetadataUnavailable) as caught:
            owned.connect(broker.manifest(run_id="0" * 40))
        assert_safe(caught.value, owned)
        assert broker.command("ACL", "LOG") == []
        deadline = time.monotonic() + 2
        while owned.sessions():
            assert time.monotonic() < deadline, "a refused enqueue session stayed open"
            time.sleep(0.02)
    finally:
        owned.restore()


BINDING_EVENT = "hook-11111111-1111-4111-8111-111111111111-incident-0123456789abcdef"


def test_read_binding_returns_the_exact_envelope_bytes_on_the_retained_session(owned):
    """``read_binding(event_id)`` reads one event's binding: bytes or ``None``.

    The nomination route resolves a protected event through it (AUTOMATED-
    REMEDIATION-6); it is a plain GET of ``protected:admission:binding:<event_id>``
    on the one retained session, with no denied command and no write.
    @spec AUTOMATED-REMEDIATION-6 @spec PROTECTED-HOOK-LANE-3.
    """
    broker = owned.broker
    key = "protected:admission:binding:" + BINDING_EVENT
    broker.command("SET", key, b'{"schema_version":1}')
    client = owned.connect()
    try:
        initial = owned.sessions()
        assert client.read_binding(BINDING_EVENT) == b'{"schema_version":1}'
        assert client.read_binding("hook-absent-event") is None
        assert [session["id"] for session in owned.sessions()] == [initial[0]["id"]]
        assert broker.command("ACL", "LOG") == []
        assert broker.command("GET", key) == b'{"schema_version":1}'
        assert broker.command("PTTL", key) == -1
    finally:
        client.close()


@pytest.mark.parametrize(
    "event", ["", " leading-space", "-leading-dash", "a" * 257, "with space", "x\n", None, 7, b"x"]
)
def test_read_binding_refuses_an_invalid_event_id_before_any_command(owned, event):
    """Only an opaque_ref event id names a binding key, @spec AUTOMATED-REMEDIATION-6.

    @spec PROTECTED-HOOK-LANE-3.
    """
    broker = owned.broker
    client = owned.connect()
    broker.command("ACL", "SETUSER", owned.username, "-get")
    try:
        before = command_count(broker, "get")
        with pytest.raises(BrokerMetadataUnavailable) as caught:
            client.read_binding(event)
        assert_safe(caught.value, owned)
        assert command_count(broker, "get") == before
        assert broker.command("ACL", "LOG") == []
    finally:
        client.close()
        owned.restore()


def test_read_binding_after_loss_or_close_is_the_safe_error(owned):
    """@spec AUTOMATED-REMEDIATION-6 @spec PROTECTED-HOOK-LANE-3."""
    broker = owned.broker
    client = owned.connect()
    try:
        assert broker.command("CLIENT", "KILL", "USER", owned.username, "SKIPME", "yes") == 1
        accepted = connections(broker)
        with pytest.raises(BrokerMetadataUnavailable) as caught:
            client.read_binding(BINDING_EVENT)
        assert_safe(caught.value, owned)
        assert connections(broker) == accepted
    finally:
        client.close()
    with pytest.raises(BrokerMetadataUnavailable):
        client.read_binding(BINDING_EVENT)


def test_a_killed_connection_is_never_reopened(owned):
    """Connection loss leaves the client permanently unusable, @spec PROTECTED-HOOK-LANE-3."""
    broker = owned.broker
    broker.command("SET", CONTROL, "control-bytes")
    client = owned.connect()
    try:
        assert broker.command("CLIENT", "KILL", "USER", owned.username, "SKIPME", "yes") == 1
        accepted = connections(broker)
        for _ in range(3):
            for operation in (
                lambda: client.read_control(CONTROL),
                lambda: client.read_source(AGENT, HOOK),
                client.observe,
                lambda: client.intent_present(identity()),
            ):
                with pytest.raises(BrokerMetadataUnavailable) as caught:
                    operation()
                assert_safe(caught.value, owned)
        assert connections(broker) == accepted, "the enqueue client opened a second connection"
        assert owned.sessions() == []
    finally:
        client.close()


def test_an_expired_budget_refuses_the_next_call_without_reconnecting(owned):
    """The shared budget watchdog bounds the enqueue client, @spec PROTECTED-HOOK-LANE-3.

    @spec PROTECTED-HOOK-SOURCE-2 (one five second budget per delivery).
    """
    broker = owned.broker
    broker.command("SET", CONTROL, "control-bytes")
    with transport().metadata_reader_budget(0.4):
        client = owned.connect()
    try:
        time.sleep(0.6)
        accepted = connections(broker)
        with pytest.raises(BrokerMetadataUnavailable) as caught:
            client.read_control(CONTROL)
        assert_safe(caught.value, owned)
        with pytest.raises(BrokerMetadataUnavailable):
            client.intent_present(identity())
        assert connections(broker) == accepted
    finally:
        client.close()


def test_a_stalled_broker_is_bounded_by_the_budget(owned):
    """A listener never completing TLS cannot outlast the budget, @spec PROTECTED-HOOK-LANE-3."""
    broker = owned.broker
    with socket.socket() as stalled:
        stalled.bind(("127.0.0.1", 0))
        stalled.listen(4)
        manifest = broker.manifest(endpoint={"host": "127.0.0.1", "port": stalled.getsockname()[1]})
        started = time.monotonic()
        with transport().metadata_reader_budget(0.3):
            with pytest.raises(BrokerMetadataUnavailable) as caught:
                owned.connect(manifest)
        elapsed = time.monotonic() - started
    assert_safe(caught.value, owned)
    assert elapsed < 1.5, f"the enqueue client outlived its budget: {elapsed:.2f}s"


def test_close_is_terminal(owned):
    """@spec PROTECTED-HOOK-LANE-3."""
    client = owned.connect()
    client.close()
    client.close()
    for operation in (
        lambda: client.read_control(CONTROL),
        lambda: client.read_source(AGENT, HOOK),
        client.observe,
        lambda: client.intent_present(identity()),
    ):
        with pytest.raises(BrokerMetadataUnavailable):
            operation()
    deadline = time.monotonic() + 2
    while owned.sessions():
        assert time.monotonic() < deadline, "closed enqueue session still listed"
        time.sleep(0.02)


@pytest.mark.parametrize(
    "kind", ["manifest", "reader-credential", "writer-credential", "empty-ca", "text-ca", "tuple"]
)
def test_invalid_inputs_precede_network(owned, monkeypatch, kind):
    """@spec PROTECTED-HOOK-LANE-3."""
    broker = owned.broker
    module = transport()
    manifest, credential, ca = broker.manifest(), owned.credential(), broker.ca_pem
    if kind == "manifest":
        manifest = manifest.as_dict()
    elif kind == "reader-credential":
        credential = module.MetadataReaderCredential(owned.username, owned.password)
    elif kind == "writer-credential":
        credential = module.SourceWriterCredential(owned.username, owned.password)
    elif kind == "empty-ca":
        ca = ""
    elif kind == "text-ca":
        ca = FixtureSecret(broker.ca_pem + "trailing text")
    else:
        manifest = broker.manifest(tls_server_name="localhost")
    attempts = []

    def forbidden(*args, **kwargs):
        """Network tripwire, @spec PROTECTED-HOOK-LANE-3."""
        attempts.append(True)
        raise AssertionError("invalid input attempted network I/O")

    _, client_type = enqueue_types()
    monkeypatch.setattr(socket.socket, "connect", forbidden)
    monkeypatch.setattr(socket, "getaddrinfo", forbidden)
    with pytest.raises(BrokerMetadataUnavailable) as caught:
        client_type.connect(manifest, credential, ca)
    assert_safe(caught.value, owned)
    assert attempts == []


def test_invalid_coordinates_refuse_before_any_command(owned):
    """Malformed keys and identities send nothing, @spec PROTECTED-HOOK-LANE-3."""
    broker = owned.broker
    client = owned.connect()
    broker.command("ACL", "SETUSER", owned.username, "-info", "-get")
    try:
        for operation in (
            lambda: client.read_control("protected:control:"),
            lambda: client.read_control("ordinary:key"),
            lambda: client.read_control("protected:admission:quota"),
            lambda: client.read_source("bad-agent", HOOK),
            lambda: client.intent_present({"agent_id": AGENT, "hook": HOOK}),
            lambda: client.intent_present("delivery/example"),
        ):
            with pytest.raises((BrokerMetadataUnavailable, ValueError)):
                operation()
        assert broker.command("ACL", "LOG") == []
    finally:
        client.close()
        owned.restore()


# -- the measured enqueue recipe ------------------------------------------------------------

# @spec PROTECTED-HOOK-ADMISSION-6: the recipe changes from the base only by ZRANGE on the quota.
BASE_RECIPE = (
    "-@all",
    "resetkeys",
    "resetchannels",
    "clearselectors",
    "+auth",
    "+hello",
    "+ping",
    "+client|setname",
    "+client|setinfo",
    "%R~protected:source:*",
    "%R~protected:control:*",
    "+get",
    "+type",
    "+info|server",
    "+time",
    "+eval",
    "(+get +type +set +del %RW~protected:admission:intent:* %RW~protected:admission:state:* "
    "%RW~protected:admission:commit:* %RW~protected:admission:recovery:* "
    "%RW~protected:admission:binding:*)",
    None,  # the quota selector, compared as a token set below
    "(+type +xinfo|stream +xrange +xadd %RW~curie:runs)",
    "(+eval %RW~protected:source:* %RW~protected:control:* %RW~protected:admission:intent:* "
    "%RW~protected:admission:state:* %RW~protected:admission:commit:* "
    "%RW~protected:admission:recovery:* %RW~protected:admission:binding:* "
    "%RW~protected:admission:quota %RW~curie:runs)",
)
QUOTA_SELECTOR = {"+type", "+zadd", "+zcard", "+zrem", "+zscore", "+zrange", "%RW~" + QUOTA}


def test_the_recipe_adds_zrange_on_exactly_the_quota_selector():
    """ZRANGE joins the quota selector, nothing else changes, @spec PROTECTED-HOOK-ADMISSION-6."""
    rules = admission_acl_rules("enqueue")
    assert len(rules) == len(BASE_RECIPE)
    for actual, expected in zip(rules, BASE_RECIPE, strict=True):
        if expected is None:
            assert actual.startswith("(") and actual.endswith(")")
            assert set(actual[1:-1].split()) == QUOTA_SELECTOR
            assert len(actual[1:-1].split()) == len(QUOTA_SELECTOR)
        else:
            assert actual == expected
    zrange = [rule for rule in rules if "+zrange" in rule]
    assert len(zrange) == 1, "ZRANGE is granted outside the quota selector"


def test_the_zrange_measurement_is_recorded_before_the_recipe_changes():
    """The ADR 0191 evidence record holds the measured recipe, command and outcome.

    @spec PROTECTED-HOOK-ADMISSION-6.
    """
    text = EVIDENCE.read_text(encoding="utf-8")
    paragraphs = [part for part in text.split("\n\n") if "ZRANGE" in part]
    assert paragraphs, "no measured ZRANGE observation in the ADR 0191 evidence record"
    assert any(QUOTA in part for part in paragraphs), "the ZRANGE record names no quota key"


# (command) the enqueue principal may run directly, each on an absent or seeded key.
PERMITTED = [
    ("GET", SOURCE),
    ("GET", CONTROL),
    ("TYPE", SOURCE),
    ("INFO", "server"),
    ("TIME",),
    ("PING",),
    ("GET", "protected:admission:intent:x"),
    ("SET", "protected:admission:intent:x", "v"),
    ("DEL", "protected:admission:intent:x"),
    ("SET", "protected:admission:state:x", "v"),
    ("SET", "protected:admission:commit:x", "v"),
    ("SET", "protected:admission:recovery:x", "v"),
    ("SET", "protected:admission:binding:x", "v"),
    ("ZADD", QUOTA, 1, "member"),
    ("ZCARD", QUOTA),
    ("ZSCORE", QUOTA, "member"),
    ("ZRANGE", QUOTA, 0, -1),
    ("ZRANGE", QUOTA, 0, 63, "WITHSCORES"),
    ("ZRANGE", QUOTA, "-inf", "+inf", "BYSCORE"),
    ("ZREM", QUOTA, "member"),
    ("XADD", STREAM, "*", "payload", "v"),
    ("XRANGE", STREAM, "-", "+"),
    ("XINFO", "STREAM", STREAM),
]


@pytest.mark.parametrize("command", PERMITTED, ids=[" ".join(map(str, c)) for c in PERMITTED])
def test_the_enqueue_principal_may_run_exactly_its_commands(owned, command):
    """Measured on the pinned Valkey, including ZRANGE on the quota key.

    @spec PROTECTED-HOOK-ADMISSION-6 @spec PROTECTED-HOOK-LANE-3.
    """
    broker = owned.broker
    broker.command("SET", SOURCE, "source-bytes")
    broker.command("SET", CONTROL, "control-bytes")
    broker.command("ZADD", QUOTA, 1, "member")
    broker.command("XADD", STREAM, "*", "payload", "seed")
    client = owned.raw()
    try:
        client.execute_command(*command)
    finally:
        client.close()
    assert broker.command("ACL", "LOG") == []


def test_zrange_reads_quota_members_in_score_order(owned):
    """The ``preparing`` read: quota members in score order, @spec PROTECTED-HOOK-ADMISSION-5/6."""
    broker = owned.broker
    for score, member in ((30, "c"), (10, "a"), (20, "b")):
        broker.command("ZADD", QUOTA, score, member)
    client = owned.raw()
    try:
        assert client.execute_command("ZRANGE", QUOTA, 0, 63) == [b"a", b"b", b"c"]
        assert client.eval("return redis.call('ZRANGE',KEYS[1],0,-1)", 1, QUOTA) == [
            b"a",
            b"b",
            b"c",
        ]
    finally:
        client.close()


ZRANGE_ELSEWHERE = [
    "protected:admission:intent:x",
    "protected:admission:binding:x",
    "protected:admission:quotas",
    "protected:admission:quota:other",
    STREAM,
    SOURCE,
    CONTROL,
    "curie:ordinary",
]


@pytest.mark.parametrize("key", ZRANGE_ELSEWHERE)
@pytest.mark.parametrize("inside_script", [False, True])
def test_zrange_refuses_on_every_other_key(owned, key, inside_script):
    """ZRANGE is granted on the quota key only, @spec PROTECTED-HOOK-ADMISSION-6."""
    broker = owned.broker
    before = broker.command("ACL", "LOG")
    client = owned.raw()
    try:
        if inside_script:
            with pytest.raises(ResponseError, match="(?i)(permission|noperm)"):
                client.eval("return redis.call('ZRANGE',KEYS[1],0,-1)", 1, key)
        else:
            with pytest.raises(NoPermissionError):
                client.execute_command("ZRANGE", key, 0, -1)
    finally:
        client.close()
    assert broker.command("ACL", "LOG") != before


DENIED = [
    # Source authority and control or evidence records are never written.
    ("SET", SOURCE, "forged"),
    ("DEL", SOURCE),
    ("SET", CONTROL, "forged"),
    ("SET", "protected:control:manifest:x", "forged"),
    ("SET", "protected:control:qualification:x:1", "forged"),
    ("SET", "protected:control:readiness:x:1", "forged"),
    ("DEL", "protected:control:readiness:x:1"),
    # Consumption belongs to the protected worker.
    ("XREADGROUP", "GROUP", "group", "consumer", "STREAMS", STREAM, ">"),
    ("XREAD", "STREAMS", STREAM, "0"),
    ("XACK", STREAM, "group", "1-0"),
    ("XAUTOCLAIM", STREAM, "group", "consumer", 0, "0-0"),
    ("XCLAIM", STREAM, "group", "consumer", 0, "1-0"),
    ("XGROUP", "CREATE", STREAM, "group", "0"),
    ("XDEL", STREAM, "1-0"),
    ("XTRIM", STREAM, "MAXLEN", 0),
    ("XPENDING", STREAM, "group"),
    # Other quota writes and reads beyond the recipe.
    ("ZPOPMIN", QUOTA),
    ("ZREMRANGEBYRANK", QUOTA, 0, -1),
    ("ZRANGESTORE", "protected:admission:binding:x", QUOTA, 0, -1),
    ("DEL", QUOTA),
    ("DEL", STREAM),
    # Administration.
    ("ACL", "LIST"),
    ("ACL", "WHOAMI"),
    ("CONFIG", "GET", "*"),
    ("INFO", "clients"),
    ("CLIENT", "LIST"),
    ("CLIENT", "KILL", "USER", "provisioner"),
    ("KEYS", "*"),
    ("SCAN", 0),
    ("FLUSHDB",),
    ("SCRIPT", "FLUSH"),
    ("FUNCTION", "LIST"),
    ("PUBLISH", "channel/example", "value"),
    ("GET", "curie:ordinary"),
    ("SET", "curie:ordinary", "forged"),
]


@pytest.mark.parametrize("command", DENIED, ids=[" ".join(map(str, c)) for c in DENIED])
def test_the_enqueue_principal_refuses_control_consume_and_administration(owned, command):
    """Writes to source or control and evidence, consumption and administration all refuse.

    @spec PROTECTED-HOOK-ADMISSION-6 @spec PROTECTED-HOOK-LANE-3.
    """
    broker = owned.broker
    broker.command("SET", SOURCE, "source-bytes")
    broker.command("SET", CONTROL, "control-bytes")
    broker.command("ZADD", QUOTA, 1, "member")
    broker.command("XADD", STREAM, "*", "payload", "seed")
    before = {key: broker.command("DUMP", key) for key in (SOURCE, CONTROL, QUOTA, STREAM)}
    client = owned.raw()
    try:
        with pytest.raises(RedisError):
            client.execute_command(*command)
    finally:
        client.close()
    assert {key: broker.command("DUMP", key) for key in (SOURCE, CONTROL, QUOTA, STREAM)} == before
