"""Actual authenticated TLS transport, @spec PROTECTED-HOOK-LANE-2/3/SOURCE-6."""

from __future__ import annotations

import hashlib
import importlib
import importlib.util
import inspect
import ipaddress
import json
import os
import secrets
import shutil
import socket
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID
from curie_protected_hooks.authority_records import parse_manifest
from curie_protected_hooks.broker_metadata import BrokerMetadataUnavailable, metadata_acl_rules
from redis import Redis
from redis.backoff import NoBackoff
from redis.exceptions import RedisError
from redis.retry import Retry

AGENT = "11111111-1111-4111-8111-111111111111"
HOOK = "incident"
CONTROL = "protected:control:runtime:manifest"
SOURCE = f"protected:source:{AGENT}:{HOOK}"


class FixtureSecret(str):
    """Keep disposable secrets out of assertion locals, @spec PROTECTED-HOOK-LANE-3."""

    def __repr__(self):
        """@spec PROTECTED-HOOK-LANE-3."""
        return "<fixture-secret>"


def fail_safely(message):
    """No third party exception locals, @spec PROTECTED-HOOK-LANE-3."""
    raise pytest.fail.Exception(message, pytrace=False) from None


def transport():
    """Missing module is a behavioral red, @spec PROTECTED-HOOK-LANE-2/3."""
    name = "curie_protected_hooks.broker_transport"
    assert importlib.util.find_spec(name) is not None, "authenticated metadata transport absent"
    return importlib.import_module(name)


def write_private(path, payload):
    """Suppress secret-bearing file errors, @spec PROTECTED-HOOK-LANE-3."""
    try:
        if isinstance(payload, bytes):
            path.write_bytes(payload)
        else:
            path.write_text(payload)
        path.chmod(0o600)
    except OSError:
        fail_safely("owned TLS private file operation failed")


def certificate(private, ca_key, ca_cert, name="127.0.0.1", basename="server"):
    """Independent ephemeral leaf, @spec PROTECTED-HOOK-LANE-2/3."""
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    now = datetime.now(UTC)
    san = x509.IPAddress(ipaddress.ip_address(name))
    cert = (
        x509.CertificateBuilder()
        .subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "fixture")]))
        .issuer_name(ca_cert.subject)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(minutes=1))
        .not_valid_after(now + timedelta(days=1))
        .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
        .add_extension(x509.SubjectKeyIdentifier.from_public_key(key.public_key()), False)
        .add_extension(
            x509.AuthorityKeyIdentifier.from_issuer_public_key(ca_key.public_key()), False
        )
        .add_extension(x509.ExtendedKeyUsage([ExtendedKeyUsageOID.SERVER_AUTH]), False)
        .add_extension(x509.SubjectAlternativeName([san]), critical=False)
        .sign(ca_key, hashes.SHA256())
    )
    for filename, payload in (
        (basename + ".crt", cert.public_bytes(serialization.Encoding.PEM)),
        (
            basename + ".key",
            key.private_bytes(
                serialization.Encoding.PEM,
                serialization.PrivateFormat.PKCS8,
                serialization.NoEncryption(),
            ),
        ),
    ):
        path = private / filename
        write_private(path, payload)
    spki = key.public_key().public_bytes(
        serialization.Encoding.DER, serialization.PublicFormat.SubjectPublicKeyInfo
    )
    return hashlib.sha256(spki).hexdigest()


class TLSBroker:
    """Private disposable fixture ownership, @spec PROTECTED-HOOK-LANE-2/3."""

    def __repr__(self):
        """@spec PROTECTED-HOOK-LANE-3."""
        return "<owned-tls-broker>"

    def docker(self, *args, allow_port_collision=False):
        """Anonymous owned daemon operations, @spec PROTECTED-HOOK-LANE-3."""
        try:
            result = subprocess.run(
                ["docker", *args], env=self.env, capture_output=True, text=True, timeout=60
            )
        except (OSError, subprocess.TimeoutExpired):
            fail_safely("owned TLS Docker operation failed")
        if result.returncode:
            if (
                allow_port_collision
                and args[0] == "run"
                and any(
                    marker in result.stderr.lower()
                    for marker in ("address already in use", "port is already allocated")
                )
            ):
                return None
            fail_safely("owned TLS Docker operation refused")
        return result.stdout

    def remove_owned(self, owner, cidfile):
        """Validate each failed or finished resource, @spec PROTECTED-HOOK-LANE-3."""
        owned = cidfile.read_text().strip() if cidfile.exists() else self.cid
        if owned:
            assert len(owned) == 64 and all(char in "0123456789abcdef" for char in owned)
            identity = json.loads(self.docker("inspect", owned))[0]
            assert identity["Id"] == owned
            assert identity["Config"]["Labels"]["curie.test.owner"] == owner
            self.docker("rm", "-f", owned)
            assert owned not in self.docker("ps", "-aq", "--no-trunc").splitlines()
        cidfile.unlink(missing_ok=True)
        self.cid = None

    def command(self, *args):
        """Sanitize provisioning failures, @spec PROTECTED-HOOK-LANE-3."""
        try:
            return self.admin.execute_command(*args)
        except RedisError:
            fail_safely("owned TLS provisioner operation failed")

    def ready(self):
        """Actual pinned TLS broker readiness, @spec PROTECTED-HOOK-LANE-2/3."""
        self.admin.close()
        for attempt in range(60):
            try:
                self.admin.ping()
                return
            except RedisError as error:
                if attempt == 59:
                    fail_safely("owned TLS broker did not become ready: " + type(error).__name__)
                time.sleep(0.1)

    def restart(self):
        """Owned epoch or certificate replacement, @spec PROTECTED-HOOK-LANE-2/3."""
        self.docker("restart", self.cid)
        binding = FixtureSecret(self.docker("port", self.cid, "6379/tcp").strip())
        assert binding == FixtureSecret(f"127.0.0.1:{self.port}")
        self.ready()

    def replace_leaf(self):
        """Actual live TLS context replacement, @spec PROTECTED-HOOK-LANE-2/3."""
        old_run_id = self.command("INFO", "server")["run_id"]
        self.pin = certificate(self.private, self.ca_key, self.ca_cert, basename="rotated")
        self.command(
            "CONFIG", "SET", "tls-cert-file", "/tls/rotated.crt", "tls-key-file", "/tls/rotated.key"
        )
        self.ready()
        assert self.command("INFO", "server")["run_id"] == old_run_id

    def manifest(self, **changes):
        """Independent closed manifest oracle, @spec PROTECTED-HOOK-LANE-2."""
        identity = {
            "instance_id": "11111111-1111-4111-8111-111111111111",
            "endpoint": {"host": "127.0.0.1", "port": self.port},
            "tls_server_name": "127.0.0.1",
            "tls_spki_sha256": self.pin,
            "run_id": self.command("INFO", "server")["run_id"],
            "database": 0,
        }
        identity.update(changes)
        value = {
            "schema_version": 1,
            "runtime_id": "33333333-3333-4333-8333-333333333333",
            "runtime_generation": "1",
            "broker_identity": identity,
            "worker_image_digest": "sha256:" + "d" * 64,
            "runner_image_digest": "sha256:" + "e" * 64,
            "bundle_digest": {"sha256": "f" * 64, "object_identity": "bundle/example"},
            "execution_config_digest": "1" * 64,
            "qualification_id": "44444444-4444-4444-8444-444444444444",
            "substrate": {
                "kind": "docker",
                "authority_domain_id": "55555555-5555-4555-8555-555555555555",
                "launch_identity": "launch/example",
                "launch_config_sha256": "2" * 64,
            },
            "guard_identity": {
                "control_id": "22222222-2222-4222-8222-222222222222",
                "revision": "1",
                "config_sha256": "c" * 64,
            },
            "credential_refs": {
                role: {"id": "credential/example-" + role, "generation": "1"}
                for role in ("enqueue", "worker", "verifier")
            },
        }
        return parse_manifest(json.dumps(value).encode())

    def connect(self, module, manifest=None):
        """Use only the public reader factory, @spec PROTECTED-HOOK-LANE-2/3."""
        credential = module.MetadataReaderCredential("control_reader", self.reader_password)
        return module.AuthenticatedMetadataReader.connect(
            manifest or self.manifest(), credential, self.ca_pem
        )

    def reader_sessions(self):
        """Observe retained socket without product internals, @spec PROTECTED-HOOK-LANE-3."""
        raw = self.command("CLIENT", "LIST")
        return [
            dict(item.split("=", 1) for item in line.split())
            for line in raw.decode().splitlines()
            if "user=control_reader" in line.split()
        ]


@pytest.fixture(scope="module")
def tls_broker(tmp_path_factory):
    """Real TLS, disabled default, owned cleanup, @spec PROTECTED-HOOK-LANE-2/3."""
    if shutil.which("docker") is None:
        fail_safely("Docker is required for actual TLS transport verification")
    fixture = TLSBroker()
    fixture.private = tmp_path_factory.mktemp("authenticated-metadata-tls")
    fixture.private.chmod(0o700)
    fixture.cid = None
    fixture.admin = None
    cidfile = fixture.private / "container.cid"
    owner = "curie-tls-test-" + uuid4().hex
    try:
        config = fixture.private / "docker"
        config.mkdir(mode=0o700)
        fixture.env = {key: FixtureSecret(value) for key, value in os.environ.items()}
        fixture.env["DOCKER_CONFIG"] = str(config)
        fixture.env.pop("DOCKER_AUTH_CONFIG", None)
        fixture.env.pop("DOCKER_CONTEXT", None)
        fixture.docker("ps", "--format", "{{.ID}}")
        fixture.reader_password = FixtureSecret(secrets.token_hex(24))
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
        fixture.pin = certificate(fixture.private, fixture.ca_key, fixture.ca_cert)
        ca_path = fixture.private / "ca.crt"
        write_private(ca_path, fixture.ca_pem)
        acl = fixture.private / "users.acl"
        write_private(
            acl,
            "user default off\nuser provisioner on >"
            + provisioner_password
            + " ~* &* +@all\n"
            + "user control_reader on >"
            + fixture.reader_password
            + " "
            + " ".join(metadata_acl_rules("control_reader"))
            + "\n",
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
                "valkey/valkey:8.1.10-alpine",
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
        binding = FixtureSecret(fixture.docker("port", fixture.cid, "6379/tcp").strip())
        assert binding == FixtureSecret(f"127.0.0.1:{fixture.port}")
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


@pytest.fixture
def broker(tls_broker):
    """Fresh owned metadata and denial log, @spec PROTECTED-HOOK-LANE-3/SOURCE-6."""
    tls_broker.command("FLUSHDB")
    tls_broker.command("ACL", "LOG", "RESET")
    yield tls_broker
    tls_broker.command("CLIENT", "KILL", "USER", "control_reader", "SKIPME", "yes")


def assert_safe(error, broker):
    """Exact public safe error oracle, @spec PROTECTED-HOOK-LANE-3."""
    assert type(error) is BrokerMetadataUnavailable
    assert str(error) == "Broker metadata unavailable"
    assert error.__cause__ is None
    assert error.__context__ is None or error.__suppress_context__
    for value in (broker.reader_password, broker.ca_pem, "127.0.0.1", str(broker.port)):
        assert value not in repr(error)


def test_credential_and_closed_surface():
    """Explicit immutable secret and operation API, @spec PROTECTED-HOOK-LANE-2/3."""
    module = transport()
    credential = module.MetadataReaderCredential("unique-reader", "unique-password")
    assert "unique-reader" not in repr(credential) and "unique-password" not in repr(credential)
    assert not hasattr(credential, "__dict__")
    with pytest.raises((AttributeError, TypeError)):
        credential.password = "changed"
    assert list(inspect.signature(module.AuthenticatedMetadataReader.connect).parameters) == [
        "manifest",
        "credential",
        "ca_pem",
    ]
    public = {name for name in dir(module.AuthenticatedMetadataReader) if not name.startswith("_")}
    assert public == {"connect", "read_source", "read_control", "observe", "close"}
    with pytest.raises((TypeError, BrokerMetadataUnavailable)):
        module.AuthenticatedMetadataReader()


def test_real_reads_observe_retained_and_serialized_connection(broker):
    """Actual metadata, clock, one retained socket, @spec PROTECTED-HOOK-LANE-2/3/SOURCE-6."""
    module = transport()
    broker.command("SET", CONTROL, "metadata")
    reader = broker.connect(module)
    try:
        initial = broker.reader_sessions()
        assert len(initial) == 1 and initial[0]["resp"] == "3"
        assert reader.read_source(AGENT, HOOK) == {"floor": 0, "operation_id": None, "active": None}
        assert reader.read_control(CONTROL) == b"metadata"
        assert reader.read_control("protected:control:absent") is None
        broker.command(
            "SET",
            SOURCE,
            json.dumps(
                {
                    "floor": "1",
                    "operation_id": "22222222-2222-4222-8222-222222222222",
                    "active": None,
                }
            ),
        )
        assert reader.read_source(AGENT, HOOK)["floor"] == 1
        before = broker.command("TIME")
        observation = reader.observe()
        after = broker.command("TIME")
        assert observation.run_id == broker.command("INFO", "server")["run_id"]
        assert int(before[0]) * 1000 + int(before[1]) // 1000 <= observation.now_ms
        assert observation.now_ms <= int(after[0]) * 1000 + int(after[1]) // 1000
        with ThreadPoolExecutor(max_workers=4) as pool:
            assert (
                list(pool.map(lambda _: reader.read_control(CONTROL), range(16)))
                == [b"metadata"] * 16
            )
        assert [session["id"] for session in broker.reader_sessions()] == [initial[0]["id"]]
        assert "127.0.0.1" not in repr(reader) and broker.reader_password not in repr(reader)
        for command in (("SET", CONTROL, "x"), ("ACL", "LIST"), ("INFO", "clients")):
            assert broker.command("ACL", "DRYRUN", "control_reader", *command) != b"OK"
    finally:
        reader.close()


@pytest.mark.parametrize("case", ["ca", "hostname", "pin"])
def test_tls_refusals_precede_authentication(broker, case):
    """Actual chain/name/pin before HELLO AUTH, @spec PROTECTED-HOOK-LANE-2/3."""
    module = transport()
    manifest = broker.manifest()
    ca = broker.ca_pem
    if case == "ca":
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
        ca = FixtureSecret(cert.public_bytes(serialization.Encoding.PEM).decode())
    elif case == "hostname":
        manifest = broker.manifest(
            endpoint={"host": "localhost", "port": broker.port}, tls_server_name="localhost"
        )
    else:
        manifest = broker.manifest(tls_spki_sha256="0" * 64)
    bad = module.MetadataReaderCredential("control_reader", FixtureSecret("wrong-fixture-password"))
    with pytest.raises(BrokerMetadataUnavailable) as caught:
        module.AuthenticatedMetadataReader.connect(manifest, bad, ca)
    assert_safe(caught.value, broker)
    assert broker.command("ACL", "LOG") == []
    with pytest.raises(BrokerMetadataUnavailable):
        module.AuthenticatedMetadataReader.connect(broker.manifest(), bad, broker.ca_pem)
    entries = broker.command("ACL", "LOG")
    assert entries and all(entry[b"reason"] == b"auth" for entry in entries)


def test_named_auth_has_no_default_fallback(broker):
    """A default-only password cannot authenticate reader, @spec PROTECTED-HOOK-LANE-3."""
    module = transport()
    default_secret = FixtureSecret(secrets.token_hex(24))
    broker.command(
        "ACL",
        "SETUSER",
        "default",
        "on",
        FixtureSecret(">" + default_secret),
        "~*",
        "+@all",
        "-info",
    )
    try:
        credential = module.MetadataReaderCredential("control_reader", default_secret)
        manifest = broker.manifest()
        before = broker.command("INFO", "commandstats")
        with pytest.raises(BrokerMetadataUnavailable) as caught:
            module.AuthenticatedMetadataReader.connect(manifest, credential, broker.ca_pem)
        after = broker.command("INFO", "commandstats")
        for command, expected in (("hello", 1), ("auth", 0)):
            key = "cmdstat_" + command
            first = before.get(key, {})
            last = after.get(key, {})
            # failed_calls is a subset of calls; rejected_calls counts unexecuted attempts.
            assert (
                last.get("calls", 0)
                + last.get("rejected_calls", 0)
                - first.get("calls", 0)
                - first.get("rejected_calls", 0)
            ) == expected
        assert_safe(caught.value, broker)
        entries = broker.command("ACL", "LOG")
        assert entries and all(entry[b"username"] == b"control_reader" for entry in entries)
        assert broker.reader_sessions() == []
    finally:
        broker.command("ACL", "SETUSER", "default", "reset")


def test_wrong_run_id_refuses_eagerly_before_application_commands(broker):
    """TLS/auth success cannot replace broker identity, @spec PROTECTED-HOOK-LANE-2/3."""
    module = transport()
    broker.command("ACL", "SETUSER", "control_reader", "-get", "-eval")
    try:
        with pytest.raises(BrokerMetadataUnavailable) as caught:
            broker.connect(module, broker.manifest(run_id="0" * 40))
        assert_safe(caught.value, broker)
        assert broker.command("ACL", "LOG") == []
        assert broker.reader_sessions() == []
    finally:
        broker.command("ACL", "SETUSER", "control_reader", *metadata_acl_rules("control_reader"))


def test_retained_identity_failure_blocks_metadata_and_observation(broker):
    """Every operation requires live identity first, @spec PROTECTED-HOOK-LANE-2/3/SOURCE-6."""
    module = transport()
    reader = broker.connect(module)
    broker.command("ACL", "SETUSER", "control_reader", "-info", "-get", "-eval", "-time")
    try:
        for operation in (
            lambda: reader.read_control(CONTROL),
            lambda: reader.read_source(AGENT, HOOK),
            reader.observe,
        ):
            broker.command("ACL", "LOG", "RESET")
            with pytest.raises(BrokerMetadataUnavailable) as caught:
                operation()
            assert_safe(caught.value, broker)
            entries = broker.command("ACL", "LOG")
            assert entries and all(
                entry[b"object"] in (b"info", b"info|server") for entry in entries
            )
    finally:
        reader.close()
        broker.command("ACL", "SETUSER", "control_reader", *metadata_acl_rules("control_reader"))


def test_forced_reconnect_repeats_authorized_identity(broker):
    """A fresh socket reauthenticates the same tuple, @spec PROTECTED-HOOK-LANE-2/3."""
    module = transport()
    reader = broker.connect(module)
    try:
        first = broker.reader_sessions()[0]["id"]
        assert broker.command("CLIENT", "KILL", "USER", "control_reader", "SKIPME", "yes") == 1
        # Zero retries may surface the first broken socket; the next operation must reconnect.
        try:
            reader.observe()
        except BrokerMetadataUnavailable:
            pass
        assert reader.observe().run_id == broker.manifest().as_dict()["broker_identity"]["run_id"]
        sessions = broker.reader_sessions()
        assert len(sessions) == 1 and sessions[0]["id"] != first and sessions[0]["resp"] == "3"
    finally:
        reader.close()


@pytest.mark.parametrize("replace_leaf", [False, True])
def test_restart_or_leaf_change_refuses_old_reader(broker, replace_leaf):
    """Every reconnect binds pin and broker epoch, @spec PROTECTED-HOOK-LANE-2/3."""
    module = transport()
    reader = broker.connect(module)
    old = broker.manifest()
    try:
        if replace_leaf:
            broker.replace_leaf()
            assert broker.command("CLIENT", "KILL", "USER", "control_reader", "SKIPME", "yes") == 1
        else:
            broker.restart()
            broker.command("SET", CONTROL, "restored-metadata")
        changed = (
            broker.manifest().as_dict()["broker_identity"]["run_id"]
            != old.as_dict()["broker_identity"]["run_id"]
        )
        assert changed is (not replace_leaf)
        broker.command("ACL", "SETUSER", "control_reader", "-get", "-eval", "-time")
        for _ in range(2):
            with pytest.raises(BrokerMetadataUnavailable):
                reader.read_control(CONTROL)
        assert broker.command("ACL", "LOG") == []
        if replace_leaf:
            assert broker.reader_sessions() == []
    finally:
        reader.close()
        broker.command("ACL", "SETUSER", "control_reader", *metadata_acl_rules("control_reader"))


@pytest.mark.parametrize(
    "kind", ["manifest", "credential", "empty-ca", "text-ca", "key-ca", "tuple"]
)
def test_factory_invalid_inputs_precede_network(broker, monkeypatch, kind):
    """Malformed public inputs require no socket, @spec PROTECTED-HOOK-LANE-2/3."""
    module = transport()
    manifest = broker.manifest()
    credential = module.MetadataReaderCredential("control_reader", broker.reader_password)
    ca = broker.ca_pem
    if kind == "manifest":
        manifest = manifest.as_dict()
    elif kind == "credential":
        credential = {"username": "control_reader", "password": broker.reader_password}
    elif kind == "empty-ca":
        ca = ""
    elif kind == "text-ca":
        ca = FixtureSecret(broker.ca_pem + "trailing text")
    elif kind == "key-ca":
        ca = FixtureSecret(broker.ca_pem + (broker.private / "server.key").read_text())
    else:
        manifest = broker.manifest(tls_server_name="localhost")
    attempts = []

    def forbidden(*args, **kwargs):
        """Network tripwire, @spec PROTECTED-HOOK-LANE-2/3."""
        attempts.append(True)
        raise AssertionError("invalid input attempted network I/O")

    monkeypatch.setattr(socket.socket, "connect", forbidden)
    monkeypatch.setattr(socket, "getaddrinfo", forbidden)
    with pytest.raises(BrokerMetadataUnavailable) as caught:
        module.AuthenticatedMetadataReader.connect(manifest, credential, ca)
    assert_safe(caught.value, broker)
    assert attempts == []


@pytest.mark.parametrize(
    "username,password",
    [("", "x"), ("default", "x"), (None, "x"), ("reader", ""), ("reader", None)],
)
def test_invalid_credentials_are_safe(username, password):
    """Credential validation uses existing refusal, @spec PROTECTED-HOOK-LANE-2/3."""
    module = transport()
    with pytest.raises(BrokerMetadataUnavailable):
        module.MetadataReaderCredential(username, password)


def test_invalid_coordinates_precede_identity_and_close_is_terminal(broker):
    """Validation before INFO and terminal close, @spec PROTECTED-HOOK-LANE-3/SOURCE-6."""
    module = transport()
    reader = broker.connect(module)
    broker.command("ACL", "SETUSER", "control_reader", "-info")
    try:
        for operation in (
            lambda: reader.read_control("protected:control:"),
            lambda: reader.read_control("ordinary:key"),
            lambda: reader.read_source("bad-agent", HOOK),
            lambda: reader.read_source(AGENT, "bad hook"),
        ):
            with pytest.raises(BrokerMetadataUnavailable) as caught:
                operation()
            assert_safe(caught.value, broker)
        assert broker.command("ACL", "LOG") == []
        reader.close()
        reader.close()
        assert broker.reader_sessions() == []
        for operation in (
            lambda: reader.read_control(CONTROL),
            lambda: reader.read_source(AGENT, HOOK),
            reader.observe,
        ):
            with pytest.raises(BrokerMetadataUnavailable):
                operation()
        assert broker.command("ACL", "LOG") == []
        assert broker.reader_sessions() == []
    finally:
        reader.close()
        broker.command("ACL", "SETUSER", "control_reader", *metadata_acl_rules("control_reader"))


def test_broker_type_and_source_corruption_refusals_are_safe(broker):
    """Untrusted broker data cannot leak through readers, @spec PROTECTED-HOOK-LANE-3/SOURCE-6."""
    module = transport()
    reader = broker.connect(module)
    try:
        broker.command("LPUSH", CONTROL, "private-value")
        with pytest.raises(BrokerMetadataUnavailable) as caught:
            reader.read_control(CONTROL)
        assert_safe(caught.value, broker)
        for value in ("not-json", '{"floor":"1","operation_id":null,"active":null}'):
            broker.command("SET", SOURCE, value)
            with pytest.raises(BrokerMetadataUnavailable) as caught:
                reader.read_source(AGENT, HOOK)
            assert_safe(caught.value, broker)
        broker.command("DEL", SOURCE)
        broker.command("LPUSH", SOURCE, "private-value")
        with pytest.raises(BrokerMetadataUnavailable) as caught:
            reader.read_source(AGENT, HOOK)
        assert_safe(caught.value, broker)
    finally:
        reader.close()
